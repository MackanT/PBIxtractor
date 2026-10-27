"""Sign-in and a small REST helper for Microsoft cloud APIs (Fabric, Azure DevOps).

    credential = get_credential()            # the active account (signs in when there is none)
    client = RestClient("https://api.fabric.microsoft.com/v1", FABRIC_SCOPE, credential)
    status, headers, body = client.request("GET", "workspaces")

Sign-in: Microsoft's sign-in page in the browser (MSAL, with its account picker). After a
sign-in the user's tenants are listed and each becomes an account entry "<user> · <tenant>";
one entry is active and every API call uses it (`pbixtractor account use`, or the web UI). The
entries are kept in ~/.pbixtractor/accounts.json (no secrets), the tokens in an MSAL token
cache next to it: encrypted by the OS where it can (Windows DPAPI, macOS keychain, Linux
keyring), else a plain file readable only by the user. One login gives tokens for every API
(the refresh token is multi-resource), so Fabric and DevOps do not ask twice. A service
principal in the environment replaces all of this (servers, containers: never a browser).
"""

import base64
import contextlib
import email.utils
import json
import logging
import os
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Callable, Optional

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
DEVOPS_SCOPE = "499b84ac-1321-427f-aa17-267ca6975798/.default"  # Azure DevOps resource id
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
ARM_SCOPE = "https://management.azure.com/.default"  # lists the user's tenants

AUTH_DIR = Path.home() / ".pbixtractor"
ACCOUNTS_FILE = "accounts.json"  # entries + the active key (no secrets)
TOKEN_CACHE_FILE = "msal_token_cache.bin"  # tokens (with the Linux keyring: only its marker)
LOGIN_TIMEOUT = 180  # seconds the browser sign-in may take
CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"  # azure-identity's developer sign-on id
AUTHORITY_HOST = "https://login.microsoftonline.com"
TENANTS_URL = "https://management.azure.com/tenants?api-version=2022-12-01"

logger = logging.getLogger("pbixtractor")


class ApiError(RuntimeError):
    """An API call failed (the message includes the API's error code and text)."""


# ============================================================================
# Sign-in
# ============================================================================


@dataclass(frozen=True)
class Account:
    """One user in one tenant: an entry of the account list."""

    username: str  # e.g. marcus@contoso.com
    home_account_id: str  # MSAL "<oid>.<home tenant id>"
    tenant_id: str
    tenant_name: str = ""  # display name; "" when unknown
    home: bool = False  # the user's home tenant

    @property
    def key(self) -> str:
        """Identifies the entry (the user and the tenant)."""
        return f"{self.home_account_id}|{self.tenant_id}"

    @property
    def label(self) -> str:
        """The entry's name: "<user> · <tenant name>" (the tenant id when the name is unknown)."""
        return f"{self.username} · {self.tenant_name or self.tenant_id}"


_store_lock = threading.RLock()  # accounts.json read-modify-write
# One sign-in at a time (web UI tabs share the process): held around every browser sign-in, for
# all of add_account (sign-in, tenant listing, writing the entries) and while a user's tokens
# are deleted (so a sign-in writing fresh tokens for that user is never cut short)
_login_lock = threading.RLock()
_sign_ins = 0  # sign-ins going on in this process (sign_in_in_progress); nested ones count too
_sign_ins_lock = threading.Lock()
_apps_lock = threading.Lock()
_caches: dict[Path, tuple] = {}  # token cache file -> (PersistedTokenCache, encrypted)
_apps: dict[tuple, object] = {}  # (token cache file, authority) -> PublicClientApplication
_service_principal: Optional[tuple] = None  # (AZURE_* values, its credential)
_service_principal_lock = threading.Lock()
_browser_login_allowed = True
_READ_ATTEMPTS = 6  # accounts.json held by another process (replacing it, a virus scanner):
_READ_RETRY_SECONDS = 0.1  # retried for about half a second, then an error
_REPLACE_ATTEMPTS = 5  # Windows: os.replace fails while another process reads accounts.json
_REPLACE_RETRY_SECONDS = 0.05
TENANTS_PAGE_TIMEOUT = 15  # seconds for one page of the tenant listing (one attempt, no retry)
TENANTS_DEADLINE = 30  # seconds for the whole listing: a throttled tenant never holds a sign-in

_INTERACTION_ERRORS = {"interaction_required", "login_required", "consent_required", "invalid_grant"}
_SERVICE_PRINCIPAL_HELP = (
    "set a service principal in the environment: AZURE_TENANT_ID, AZURE_CLIENT_ID and "
    "AZURE_CLIENT_SECRET (or AZURE_CLIENT_CERTIFICATE_PATH)"
)


def service_principal_configured() -> bool:
    """A service principal is set in the environment (the variables azure-identity reads)."""
    has_id = os.environ.get("AZURE_CLIENT_ID") and os.environ.get("AZURE_TENANT_ID")
    has_proof = os.environ.get("AZURE_CLIENT_SECRET") or os.environ.get(
        "AZURE_CLIENT_CERTIFICATE_PATH"
    )
    return bool(has_id and has_proof)


def allow_browser_login(allowed: bool) -> None:
    """
    Allow or forbid the browser sign-in (forbidden when embedded in a server app: there a
    browser would open on the server, so only a service principal can sign in).

    Args:
        allowed: False makes get_credential() and add_account() raise ApiError
    """
    global _browser_login_allowed
    _browser_login_allowed = bool(allowed)


def _require_browser() -> None:
    if not _browser_login_allowed:
        raise ApiError(f"No browser sign-in here - {_SERVICE_PRINCIPAL_HELP}.")


def sign_in_in_progress() -> bool:
    """
    True while a sign-in of this process is going on, in any tab or thread (a run's too): the
    whole of add_account / ensure_account (the browser, listing the tenants, writing the
    entries) and every browser sign-in a credential's token needs. False once the last ends.
    """
    return _sign_ins > 0


@contextlib.contextmanager
def _sign_in_running():
    """Count a sign-in for sign_in_in_progress() while the block runs (on every exit path)."""
    global _sign_ins
    with _sign_ins_lock:
        _sign_ins += 1
    try:
        yield
    finally:
        with _sign_ins_lock:
            _sign_ins -= 1


# ---------------------------------------------------------------------------
# The account list (~/.pbixtractor/accounts.json)
# ---------------------------------------------------------------------------


def _sorted(entries) -> list[Account]:
    """By user name, the home tenant first, then by tenant name."""
    return sorted(
        entries,
        key=lambda a: (
            a.username.casefold(),
            not a.home,
            (a.tenant_name or a.tenant_id).casefold(),
            a.tenant_id,
        ),
    )


def _retried(action: Callable, attempts: int, pause: float, retried: tuple):
    """
    action(), tried again after `pause` seconds when it raises one of `retried` - at most
    `attempts` times in all; the last attempt's error propagates.
    """
    for attempt in range(attempts):
        try:
            return action()
        except retried:
            if attempt >= attempts - 1:
                raise
            time.sleep(pause)
    raise AssertionError("attempts must be at least 1")  # pragma: no cover


def _damaged(path: Path, reason: str) -> ApiError:
    return ApiError(f"The account list {path} is damaged ({reason}) - delete it and sign in again")


def _read_store(wait: bool = True) -> tuple[list[Account], Optional[str]]:
    """
    (entries in list order, active key) from accounts.json; nothing when it is missing.

    Args:
        wait: Retry a file that cannot be read for about half a second (False: one attempt,
            no sleeping - a poll on the web UI's event loop)

    Raises:
        ApiError: The file exists but cannot be read (still after the retries) or is damaged:
            not JSON, "accounts" not a list, or a non-empty list without one valid entry -
            never "no accounts", which would let a writer wipe the entries or a credential
            open a sign-in
    """
    path = AUTH_DIR / ACCOUNTS_FILE

    def read() -> Optional[bytes]:
        try:
            return path.read_bytes()
        except FileNotFoundError:
            return None

    try:  # e.g. Windows: another process is replacing it right now
        attempts = _READ_ATTEMPTS if wait else 1
        content = _retried(read, attempts, _READ_RETRY_SECONDS, (OSError,))
    except OSError as error:
        raise ApiError(f"Cannot read the account list {path}: {error}") from None
    if content is None:
        return [], None
    try:
        data = json.loads(content.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("not a JSON object")
    except ValueError as error:  # also UnicodeDecodeError
        raise _damaged(path, str(error)) from None
    items = data.get("accounts")
    if items is None:
        items = []
    if not isinstance(items, list):
        raise _damaged(path, "the accounts are not a list")
    entries = []
    for item in items:
        try:
            entries.append(
                Account(
                    username=str(item["username"]),
                    home_account_id=str(item["home_account_id"]),
                    tenant_id=str(item["tenant_id"]),
                    tenant_name=str(item.get("tenant_name") or ""),
                    home=bool(item.get("home")),
                )
            )
        except (KeyError, TypeError, AttributeError):
            continue
    if items and not entries:  # an empty list is no accounts (removing the last entry)
        raise _damaged(path, "no valid account in the list")
    active = data.get("active")
    return _sorted(entries), active if isinstance(active, str) else None


def _write_store(entries: list[Account], active: Optional[str]) -> None:
    """Replace accounts.json (atomically: a reader never sees half a file)."""
    AUTH_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    data = {"version": 1, "active": active, "accounts": [asdict(a) for a in _sorted(entries)]}
    handle, temp = tempfile.mkstemp(dir=AUTH_DIR, prefix="accounts.", suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2, ensure_ascii=False)
        _retried(  # Windows: another process is reading it right now
            lambda: os.replace(temp, AUTH_DIR / ACCOUNTS_FILE),
            _REPLACE_ATTEMPTS,
            _REPLACE_RETRY_SECONDS,
            (PermissionError,),
        )
    except BaseException:
        Path(temp).unlink(missing_ok=True)
        raise


def account_snapshot(wait: bool = True) -> tuple[list[Account], Optional[Account]]:
    """
    The entries and the active one, from one read of accounts.json (never a mix of two
    versions of the file, which another tab or the command line may replace at any time).

    Args:
        wait: Retry a file that cannot be read for about half a second (False: one attempt,
            no sleeping, the ApiError at once - for a poll on the web UI's event loop)

    Returns:
        (entries by user, the home tenant first, then by tenant name; the active entry or None)

    Raises:
        ApiError: accounts.json exists but cannot be read or is damaged (missing: no entries)
    """
    entries, active = _read_store(wait)
    return entries, next((a for a in entries if a.key == active), None)


def accounts() -> list[Account]:
    """The remembered entries: by user, the home tenant first, then by tenant name (ApiError:
    the account list cannot be read, see account_snapshot)."""
    return account_snapshot()[0]


def active_account() -> Optional[Account]:
    """The entry every sign-in uses (None: the next sign-in opens the browser; ApiError: the
    account list cannot be read, see account_snapshot)."""
    return account_snapshot()[1]


def accounts_changed_since(stamp: Optional[tuple]) -> tuple[bool, Optional[tuple]]:
    """
    Whether accounts.json may have changed since `stamp`, without reading it: a cheap check
    for a poll, which then calls account_snapshot() only when it did.

        changed, stamp = accounts_changed_since(None)       # None: never read - True
        changed, stamp = accounts_changed_since(stamp)      # every few seconds

    Args:
        stamp: The stamp an earlier call returned, or None

    Returns:
        (changed, the new stamp): the file's modification time, size and file id (a writer
        replaces the file, so the id changes too); a missing file has a stamp of its own. When
        the file cannot even be looked at, (True, None): read it (that raises the error).
    """
    path = AUTH_DIR / ACCOUNTS_FILE
    try:
        info = path.stat()
        new = (str(path), info.st_mtime_ns, info.st_size, info.st_ino)
    except FileNotFoundError:
        new = (str(path), None, None, None)
    except OSError:
        return True, None
    return stamp is None or new != stamp, new


def use_account(key: str) -> Account:
    """
    Make an entry the active one (for the whole app: every tab and the command line).

    Args:
        key: Account.key of a remembered entry

    Raises:
        ValueError: No entry with that key
        ApiError: The account list cannot be read (nothing is written then)
    """
    with _store_lock:
        entries, _ = account_snapshot()
        account = next((a for a in entries if a.key == key), None)
        if account is None:
            raise ValueError(f"No account {key}")
        _write_store(entries, key)
        return account


def find_account(text: str, exact: bool = False) -> Account:
    """
    The entry a user means on the command line.

    Args:
        text: 1-based number in the list, a key, or (case-insensitive) a user name, tenant
            name, tenant id or label - exactly, else (unless `exact`) as part of it
        exact: Only a number, a key or an exact match (for removing: never a guess)

    Raises:
        ValueError: No entry or several match (the message names them)
    """
    entries = accounts()
    text = text.strip()
    if not entries:
        raise ValueError("No accounts - run `pbixtractor account add`")
    if text.isdigit():
        number = int(text)
        if 1 <= number <= len(entries):
            return entries[number - 1]
        raise ValueError(f"No account number {number}: the list has {len(entries)}")
    for account in entries:
        if account.key == text:
            return account
    wanted = text.casefold()

    def fields(account: Account) -> list[str]:
        values = (account.username, account.tenant_name, account.tenant_id, account.label)
        return [value.casefold() for value in values if value]

    matches = [a for a in entries if wanted in fields(a)]
    if not matches and not exact:
        matches = [a for a in entries if any(wanted in value for value in fields(a))]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        if exact:
            raise ValueError(
                f"No account matches '{text}' exactly - give its number, user, tenant name or "
                "tenant id (see `pbixtractor account list`)"
            )
        raise ValueError(f"No account matches '{text}' (see `pbixtractor account list`)")
    names = "; ".join(f"{entries.index(a) + 1}. {a.label}" for a in matches)
    raise ValueError(f"'{text}' matches several accounts: {names} - give the number")


def _next_active(removed: Account, remaining: list[Account]) -> Optional[Account]:
    """After the active entry is removed: the same user's home tenant, that user's other
    entries, then the first remaining entry (never another person while the user has one)."""
    same_user = [a for a in remaining if a.home_account_id == removed.home_account_id]
    home = next((a for a in same_user if a.home), None)
    return home or next(iter(same_user), None) or next(iter(remaining), None)


def remove_account(key: str) -> None:
    """
    Forget an entry. When the active entry goes, the same user's home tenant becomes active,
    else that user's other entry, else the first remaining one (none left: no active entry).
    The user's last entry also deletes that user's tokens from the token cache (offline: no
    sign-in service is asked) - after any sign-in of this process going on meanwhile, and only
    if the user still has no entry then (a sign-in adding them again keeps its fresh tokens).

    Args:
        key: Account.key of a remembered entry

    Raises:
        ValueError: No entry with that key
        ApiError: The account list cannot be read (nothing is removed then), or the entry was
            removed but the user's tokens could not be deleted (the message says so)
    """
    with _store_lock:
        entries, active = account_snapshot()
        account = next((a for a in entries if a.key == key), None)
        if account is None:
            raise ValueError(f"No account {key}")
        remaining = [a for a in entries if a.key != key]
        if active is not None and active.key == key:
            active = _next_active(account, remaining)
        _write_store(remaining, active.key if active else None)
    if any(a.home_account_id == account.home_account_id for a in remaining):
        return
    with _login_lock:  # a sign-in writing fresh tokens for this user ends first
        try:
            _delete_unlisted_tokens(account.home_account_id)
        except Exception as error:  # cache file locked / unreadable, keyring refused
            raise ApiError(
                f"Removed {account.label}, but could not delete the tokens of "
                f"{account.username} from {AUTH_DIR / TOKEN_CACHE_FILE}: {error}"
            ) from None


# ---------------------------------------------------------------------------
# Token cache and MSAL
# ---------------------------------------------------------------------------


def _encrypted_persistence(path: Path):
    """OS-encrypted token storage; raises where the OS (or its Python bindings) cannot."""
    from msal_extensions import build_encrypted_persistence

    return build_encrypted_persistence(str(path))


def _persistence(path: Path):
    """Encrypted storage when possible, else a plain file readable only by the user (0600)."""
    from msal_extensions import FilePersistence

    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        return _encrypted_persistence(path)
    except Exception:  # e.g. Linux without working keyring bindings (PyGObject / libsecret)
        pass
    if not path.exists():
        os.close(os.open(path, os.O_WRONLY | os.O_CREAT, 0o600))
    if os.name == "posix":
        os.chmod(path, 0o600)
    return FilePersistence(str(path))


def _token_cache():
    """(the persisted MSAL token cache in AUTH_DIR, whether it is encrypted)."""
    from msal_extensions import PersistedTokenCache

    path = AUTH_DIR / TOKEN_CACHE_FILE
    with _apps_lock:
        if path not in _caches:
            persistence = _persistence(path)
            _caches[path] = (PersistedTokenCache(persistence), bool(persistence.is_encrypted))
        return _caches[path]


def token_cache_encrypted() -> bool:
    """False: the tokens are in a plain file readable only by the user (no OS encryption)."""
    return _token_cache()[1]


def _delete_tokens(home_account_id: str) -> None:
    """Delete a user's records (account, refresh, access and ID tokens) straight from the
    persisted token cache: no MSAL app, so no sign-in metadata from the network."""
    cache = _token_cache()[0]
    kinds = cache.CredentialType
    removers = (
        (kinds.ACCESS_TOKEN, cache.remove_at),
        (kinds.REFRESH_TOKEN, cache.remove_rt),
        (kinds.ID_TOKEN, cache.remove_idt),
        (kinds.ACCOUNT, cache.remove_account),
    )
    for kind, remove in removers:
        for record in list(cache.search(kind, query={"home_account_id": home_account_id})):
            remove(record)


def _delete_unlisted_tokens(home_account_id: str) -> bool:
    """Delete a user's tokens unless the user has an entry (looked up under the store lock).
    Callers hold _login_lock, so no sign-in of this process writes tokens meanwhile.
    Returns whether the tokens were deleted."""
    with _store_lock:
        if any(a.home_account_id == home_account_id for a in accounts()):
            return False
        _delete_tokens(home_account_id)
        return True


def _drop_new_tokens(home_account_id: str, who: str) -> None:
    """After a sign-in that is not kept: delete the user's tokens unless that user has an
    entry (_delete_unlisted_tokens; the caller holds _login_lock). A failure to delete them
    (account list or token cache unreadable) is logged as a warning: the tokens stay."""
    try:
        _delete_unlisted_tokens(home_account_id)
    except Exception as error:
        logger.warning(
            f"Could not delete the tokens of {who} from {AUTH_DIR / TOKEN_CACHE_FILE}: {error}"
        )


def _new_app(authority: str, token_cache):
    """An MSAL public client for one tenant authority (tests replace this)."""
    import msal

    return msal.PublicClientApplication(CLIENT_ID, authority=authority, token_cache=token_cache)


def _app(tenant: Optional[str]):
    """The MSAL app for a tenant (None: any organisation), one per tenant, all one cache."""
    tenant = urllib.parse.quote((tenant or "").strip() or "organizations", safe=".-_")
    authority = f"{AUTHORITY_HOST}/{tenant}"
    cache = _token_cache()[0]
    key = (AUTH_DIR / TOKEN_CACHE_FILE, authority.lower())
    with _apps_lock:
        app = _apps.get(key)
    if app is None:
        try:
            app = _new_app(authority, cache)  # reads the tenant's sign-in metadata (network)
        except ApiError:
            raise
        except Exception as error:
            raise ApiError(f"Sign-in failed: cannot use {authority}: {error}") from None
        with _apps_lock:
            app = _apps.setdefault(key, app)
    return app


def _msal_error(result: Optional[dict]) -> str:
    """The reason in an MSAL error result (first line: the rest is trace/correlation ids)."""
    if not result:
        return "no answer from the sign-in"
    text = str(result.get("error_description") or result.get("error") or "no token received")
    return text.strip().splitlines()[0]


def _sign_in(app, scopes, prompt=None, login_hint=None, claims=None, timeout=LOGIN_TIMEOUT) -> dict:
    """One browser sign-in (never two at once; sign_in_in_progress() meanwhile); MSAL's
    result with an access token."""
    from msal import BrowserInteractionTimeoutError

    _require_browser()
    with _sign_in_running(), _login_lock:
        try:
            result = app.acquire_token_interactive(
                list(scopes),
                prompt=prompt,
                login_hint=login_hint,
                claims_challenge=claims,
                timeout=timeout,
            )
        except BrowserInteractionTimeoutError:
            raise ApiError(f"Sign-in failed: not completed within {timeout:g} s") from None
        except ApiError:
            raise
        except Exception as error:
            raise ApiError(f"Sign-in failed: {error}") from None
    if not result or "access_token" not in result:
        raise ApiError(f"Sign-in failed: {_msal_error(result)}")
    return result


def _home_account_id(result: dict, app) -> str:
    """MSAL's "<oid>.<home tenant id>" of the user a sign-in result belongs to."""
    info = result.get("client_info")
    if isinstance(info, str) and info:
        try:
            decoded = json.loads(base64.urlsafe_b64decode(info + "=" * (-len(info) % 4)))
            return f"{decoded['uid']}.{decoded['utid']}"
        except (ValueError, KeyError, TypeError):
            pass
    claims = result.get("id_token_claims") or {}
    username = claims.get("preferred_username")
    for cached in app.get_accounts(username=username) if username else []:
        if cached.get("home_account_id"):
            return cached["home_account_id"]
    return f"{claims.get('oid', '')}.{claims.get('tid', '')}"


class _FixedToken:
    """A credential for one access token already in hand."""

    def __init__(self, token: str):
        from azure.core.credentials import AccessToken

        self._token = AccessToken(token, int(time.time()) + 600)

    def get_token(self, *scopes, **kwargs):
        return self._token


def _tenants_page(url: str, access_token: str, timeout: float) -> dict:
    """One page of the tenant listing: one attempt (no 429/5xx retries, no Retry-After wait)."""
    request = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    )
    endpoint = url.split("?")[0]
    try:
        with _OPENER.open(request, timeout=timeout) as response:
            content = response.read()
    except urllib.error.HTTPError as error:
        raise ApiError(error_text(error.code, url, error.read())) from None
    except (urllib.error.URLError, OSError) as error:  # incl. timeouts, resets
        raise ApiError(f"Cannot reach {endpoint}: {getattr(error, 'reason', error)}") from None
    try:
        body = json.loads(content) if content else {}
    except ValueError:
        raise ApiError(f"Unexpected non-JSON answer from {endpoint}") from None
    if not isinstance(body, dict):
        raise ApiError(f"Unexpected answer from {endpoint}")
    return body


def _list_tenants(access_token: str) -> list[dict]:
    """
    The tenants the signed-in user belongs to (Azure management API, as `az login` does).
    Each page gets one attempt of at most TENANTS_PAGE_TIMEOUT seconds and the whole listing
    TENANTS_DEADLINE seconds: a throttled or slow listing fails fast (the sign-in then keeps
    the user's entries) instead of holding "Waiting for sign-in" for minutes. Only https
    pages on management.azure.com are followed.

    Raises:
        ApiError: A page failed, or the listing took longer than TENANTS_DEADLINE
    """
    deadline = time.monotonic() + TENANTS_DEADLINE
    tenants: list[dict] = []
    url: Optional[str] = TENANTS_URL
    for _ in range(50):  # pages
        left = deadline - time.monotonic()
        if left <= 0:
            raise ApiError(f"Listing the tenants took longer than {TENANTS_DEADLINE:g} s")
        body = _tenants_page(url, access_token, min(TENANTS_PAGE_TIMEOUT, left))
        tenants.extend(body.get("value") or [])
        url = body.get("nextLink")
        if not isinstance(url, str) or not url:
            break
        parts = urllib.parse.urlsplit(url)
        if parts.scheme != "https" or parts.hostname != "management.azure.com":
            break
    return tenants


def _management_token(app, home_account_id: str) -> str:
    """An Azure management token for the user just signed in, from the token cache (silently:
    a tenant that blocks the management API never opens the browser for it)."""
    cached = _cached_account(app, home_account_id)
    if cached is None:
        raise ApiError("the sign-in left no account in the token cache")
    result = app.acquire_token_silent_with_error([ARM_SCOPE], cached)
    if not result or "access_token" not in result:
        raise ApiError(f"no Azure management token: {_msal_error(result)}")
    return result["access_token"]


def add_account(
    tenant: Optional[str] = None,
    timeout: float = LOGIN_TIMEOUT,
    on_warning: Optional[Callable[[str], None]] = None,
) -> list[Account]:
    """
    Sign in through Microsoft's sign-in page (with its account picker) and add every tenant of
    that user as an entry. Signing in as a user already in the list refreshes their entries
    (tenants no longer listed disappear). When the tenants cannot be listed, the user's entries
    are kept and only the tenant signed in to is added (or updated).

    Args:
        tenant: Entra tenant (id or domain) to sign in to and make active (default: the user's
            home tenant)
        timeout: Seconds to wait for the sign-in in the browser
        on_warning: Called with the text when the tenants could not be listed (also logged)

    Returns:
        That user's entries, in list order

    Raises:
        ApiError: The sign-in was cancelled, timed out or refused (or no browser is allowed)
    """
    tenant = (tenant or "").strip() or None
    return _add_account(tenant, timeout, on_warning, activate_signed_in=tenant is not None)


def _add_account(
    tenant: Optional[str],
    timeout: float,
    on_warning: Optional[Callable[[str], None]],
    activate_signed_in: bool,
) -> list[Account]:
    """add_account(); `activate_signed_in` False makes the user's home tenant active even after
    signing in at `tenant` (added as an entry also when the tenants cannot be listed)."""
    _require_browser()
    # The whole add: a caller waiting for the lock then sees the new entries, and every tab
    # sees a sign-in going on until the entries are written
    with _sign_in_running(), _login_lock:
        account_snapshot()  # an account list that cannot be read: fail before the browser opens
        app = _app(tenant)
        result = _sign_in(app, [FABRIC_SCOPE], prompt="select_account", timeout=timeout)
        claims = result.get("id_token_claims") or {}
        username = str(
            claims.get("preferred_username") or claims.get("upn") or claims.get("name") or "?"
        )
        home_account_id = _home_account_id(result, app)
        try:
            added, home_added, listing_error = _keep_signed_in(
                app, tenant, activate_signed_in, claims, username, home_account_id
            )
        except Exception:  # not kept (e.g. the list cannot be read or written): no orphan tokens
            _drop_new_tokens(home_account_id, username)
            raise
    if listing_error is not None:
        which = "the tenant signed in to and the home tenant were" if home_added else (
            "the tenant signed in to was"
        )
        warning = (
            f"Could not list the tenants of {username} ({listing_error}): only {which} added "
            "or updated - add other tenants by their tenant id or domain"
        )
        try:
            if on_warning is not None:
                on_warning(warning)
        finally:
            logger.warning(warning)
    return added


def _keep_signed_in(
    app,
    tenant: Optional[str],
    activate_signed_in: bool,
    claims: dict,
    username: str,
    home_account_id: str,
) -> tuple[list[Account], bool, Optional[Exception]]:
    """_add_account after the browser sign-in: list the user's tenants and write their
    entries. Returns (the user's entries, whether the unlisted home tenant was added, the
    listing error or None). The caller holds _login_lock."""
    home_tenant_id = home_account_id.partition(".")[2]
    home_tenant = home_tenant_id.casefold()
    signed_in = str(claims.get("tid") or tenant or home_tenant_id)

    def entry(tenant_id: str, tenant_name: str = "") -> Account:
        home = tenant_id.casefold() == home_tenant
        return Account(username, home_account_id, tenant_id, tenant_name, home=home)

    listing_error = None
    try:
        listed = _list_tenants(_management_token(app, home_account_id))
    except Exception as error:  # ApiError mostly: no access to the management API
        listed, listing_error = None, error

    with _store_lock:  # read again: another process may have changed the list meanwhile
        existing, _ = account_snapshot()
        others = [a for a in existing if a.home_account_id != home_account_id]
        if listed is None:  # keep what is known: a failed listing never deletes entries
            mine = {
                a.tenant_id.casefold(): replace(a, username=username)
                for a in existing
                if a.home_account_id == home_account_id
            }
        else:  # the listing replaces the user's entries (tenants no longer listed go)
            mine = {}
            for item in listed:
                tenant_id = str(item.get("tenantId") or "").strip()
                if tenant_id:
                    name = str(item.get("displayName") or "")
                    mine[tenant_id.casefold()] = entry(tenant_id, name)
        key = signed_in.casefold()
        if listed is None or key not in mine:
            known = mine.get(key)
            mine[key] = entry(signed_in, known.tenant_name if known else "")
        home_added = False
        if not activate_signed_in and home_tenant and home_tenant not in mine:
            mine[home_tenant] = entry(home_tenant_id)  # unlisted: the home tenant still
            home_added = True  # becomes the active entry (its name is unknown then)
        added = _sorted(mine.values())
        active = mine[key]
        if not activate_signed_in:
            active = next((a for a in added if a.home), active)
        _write_store(others + added, active.key)
    return added, home_added, listing_error


def _ensure_account(tenant: Optional[str], timeout: float) -> Account:
    """The active entry; with none, one sign-in (at `tenant`, the home tenant made active)."""
    account = active_account()
    if account is not None:
        return account
    _require_browser()
    # Another caller may be signing in right now: then wait for it and use its account
    with _sign_in_running(), _login_lock:
        account = active_account()
        if account is None:
            _add_account(tenant, timeout, None, activate_signed_in=False)
            account = active_account()
    if account is None:
        raise ApiError("Sign-in failed: no account is active")
    return account


def ensure_account(timeout: float = LOGIN_TIMEOUT) -> Account:
    """
    The active entry; with none, one browser sign-in (add_account) whose home tenant becomes
    active. Several callers at once open one sign-in: the others wait and get its account.

    Args:
        timeout: Seconds to wait for the sign-in in the browser

    Raises:
        ApiError: The sign-in was cancelled, timed out or refused (or no browser is allowed)
    """
    return _ensure_account(None, timeout)


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


def _bound_account(tenant_id: Optional[str]) -> Optional[Account]:
    """The active entry, or the active user in `tenant_id` (also when that is not listed)."""
    entries, active = account_snapshot()
    if active is None or not tenant_id or not tenant_id.strip():
        return active
    wanted = tenant_id.strip()
    for account in entries:
        if account.home_account_id == active.home_account_id and (
            account.tenant_id.casefold() == wanted.casefold()
        ):
            return account
    return replace(active, tenant_id=wanted, tenant_name="", home=False)


def _cached_account(app, home_account_id: str) -> Optional[dict]:
    """The user's account record in MSAL's token cache (None: no tokens, sign in again)."""
    return next(
        (c for c in app.get_accounts() if c.get("home_account_id") == home_account_id), None
    )


class _AccountCredential:
    """
    Tokens for one account entry: the one active when the credential is created, kept for its
    lifetime (a switch affects only credentials created after it). With no active entry the
    first get_token() signs in (ensure_account; with a tenant: there, the home tenant made
    active) and uses the active user in that tenant. An account list that cannot be read is
    an error of get_token() (ApiError), never a sign-in. Safe to use from several threads.
    """

    def __init__(self, tenant_id: Optional[str] = None):
        self._tenant_id = tenant_id
        self._lock = threading.Lock()
        try:
            self._account = _bound_account(tenant_id)
        except ApiError:  # unreadable account list: get_token() reads it again (and raises)
            self._account = None

    def _bound(self) -> Account:
        with self._lock:
            if self._account is None:
                _ensure_account(self._tenant_id, LOGIN_TIMEOUT)
                self._account = _bound_account(self._tenant_id)
                if self._account is None:
                    raise ApiError("Sign-in failed: no account is active")
            return self._account

    def _silent(self, app, account: Account, scopes, claims) -> Optional[dict]:
        try:
            cached = _cached_account(app, account.home_account_id)
            if cached is None:
                return None
            return app.acquire_token_silent_with_error(
                list(scopes), cached, claims_challenge=claims
            )
        except Exception as error:  # network, cache that cannot be read
            raise ApiError(f"Sign-in failed: {error}") from None

    def get_token(self, *scopes, claims: Optional[str] = None, **kwargs):
        """
        An access token for the scopes: from the cache or the refresh token, else (when MSAL
        asks for interaction, e.g. a tenant with its own MFA) the browser for that tenant.

        Returns:
            azure.core.credentials.AccessToken

        Raises:
            ApiError: The sign-in failed
        """
        from azure.core.credentials import AccessToken

        account = self._bound()
        app = _app(account.tenant_id)
        result = self._silent(app, account, scopes, claims)
        if not result or "access_token" not in result:
            if result and result.get("error") not in _INTERACTION_ERRORS:
                raise ApiError(f"Sign-in failed: {_msal_error(result)}")
            result = self._interactive(app, account, scopes, claims)
        expires_in = int(result.get("expires_in") or 0)
        return AccessToken(result["access_token"], int(time.time()) + expires_in)

    def _interactive(self, app, account: Account, scopes, claims) -> dict:
        """The browser sign-in a token needs (sign_in_in_progress() meanwhile)."""
        with _sign_in_running():
            waited = not _login_lock.acquire(blocking=False)
            if waited:
                _login_lock.acquire()
            try:
                if waited:  # the other sign-in may have been this user's: try the cache again
                    result = self._silent(app, account, scopes, claims)
                    if result and "access_token" in result:
                        return result
                result = _sign_in(app, scopes, login_hint=account.username, claims=claims)
                signed_in = _home_account_id(result, app)
                if signed_in != account.home_account_id:
                    self._refuse_other_user(result, signed_in, account)  # raises ApiError
                return result
            finally:
                _login_lock.release()

    @staticmethod
    def _refuse_other_user(result: dict, signed_in: str, account: Account) -> None:
        """Signed in as someone else (another account picked): MSAL has cached that user's
        tokens - delete them unless that user has an entry - and raise ApiError."""
        other = (result.get("id_token_claims") or {}).get("preferred_username") or signed_in
        _drop_new_tokens(signed_in, other)  # the caller holds _login_lock
        raise ApiError(
            f"Sign-in failed: signed in as {other}, but the account in use is "
            f"{account.username} - sign in as that user, or add {other} with "
            "`pbixtractor account add`"
        )


def _credential(tenant_id: Optional[str]):
    if service_principal_configured():
        from azure.identity import EnvironmentCredential

        return EnvironmentCredential()
    _require_browser()
    return _AccountCredential(tenant_id)


def default_credential(tenant_id: Optional[str] = None):
    """
    A token credential:

    - a service principal from the environment (AZURE_CLIENT_ID + AZURE_TENANT_ID +
      AZURE_CLIENT_SECRET or AZURE_CLIENT_CERTIFICATE_PATH): for servers and containers, where
      no one can complete a browser login - then never a browser;
    - else the account entry active now, kept for the credential's lifetime (a client keeps
      its account; clients made after a switch use the new one). With no entry the first
      token opens Microsoft's sign-in page (at `tenant_id` when given) and the user's home
      tenant becomes the active entry; the credential uses that user in `tenant_id`. An
      account list that cannot be read makes get_token() raise ApiError (no sign-in).

    A new credential on every call (get_credential() shares a service principal's).

    Args:
        tenant_id: Entra tenant (id or domain) for the active user (default: the active
            entry's; a service principal uses AZURE_TENANT_ID)

    Raises:
        ApiError: No service principal and the browser sign-in is not allowed (embedded)
    """
    return _credential(tenant_id)


def get_credential(tenant_id: Optional[str] = None):
    """
    default_credential(tenant_id) - looked up when called, so a replaced
    azure_auth.default_credential is used - except that a service principal's credential is
    made once per process (for the same AZURE_* values) and shared, so its tokens are reused.
    """
    global _service_principal
    if not service_principal_configured():
        return default_credential(tenant_id)
    values = tuple(sorted(item for item in os.environ.items() if item[0].startswith("AZURE_")))
    with _service_principal_lock:
        if _service_principal is None or _service_principal[0] != values:
            _service_principal = (values, default_credential(tenant_id))
        return _service_principal[1]


# ============================================================================
# REST
# ============================================================================


class RestClient:
    """JSON REST calls with a bearer token (or a PAT), retries when throttled."""

    def __init__(
        self,
        api: str,
        scope: str,
        credential=None,
        pat: Optional[str] = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        """
        Args:
            api: API root; relative URLs are joined to it
            scope: Token scope for the credential
            credential: Object with get_token(scope) (default: get_credential())
            pat: Personal access token (Basic auth) instead of a token credential
            sleep: Wait function (tests pass a no-op)
        """
        self._api = api.rstrip("/")
        self._scope = scope
        self._credential = credential
        self._pat = pat
        self._sleep = sleep
        self._token = None
        self._credential_lock = threading.Lock()

    def _resolve_credential(self):
        """
        The client's credential, made on first use when none was given: get_credential() is
        looked up when called (so a replaced azure_auth.get_credential is used). Sub-clients
        get this one, so all calls of a client use one account.
        """
        with self._credential_lock:
            if self._credential is None:
                self._credential = get_credential()
            return self._credential

    def _auth_header(self) -> str:
        if self._pat:
            return "Basic " + base64.b64encode(f":{self._pat}".encode()).decode()
        credential = self._resolve_credential()
        if self._token is None or self._token.expires_on - time.time() < 120:
            try:
                self._token = credential.get_token(self._scope)
            except ApiError:
                raise
            except Exception as error:  # login cancelled or timed out, service principal refused
                raise ApiError(f"Sign-in failed: {error}") from None
        return f"Bearer {self._token.token}"

    def request(self, method: str, url: str, body: Optional[dict] = None, raw: bool = False):
        """
        One API call. Retries throttling (429), server errors (5xx) and dropped connections.

        Args:
            method: HTTP method
            url: Absolute URL, or a path relative to the API root
            body: JSON body
            raw: Return the body as bytes instead of parsed JSON

        Returns:
            (status, headers, body) - body is parsed JSON (None if empty) or bytes with raw

        Raises:
            ApiError: For every failure (HTTP error, network, sign-in, unexpected answer)
        """
        if not url.startswith("http"):
            url = f"{self._api}/{url.lstrip('/')}"
        parts = urllib.parse.urlsplit(url)
        if parts.scheme != "https" and parts.hostname not in ("127.0.0.1", "localhost"):
            raise ApiError(f"Refusing to send credentials over {parts.scheme or 'no scheme'}: {url}")
        data = json.dumps(body).encode() if body is not None else None
        for attempt in range(MAX_ATTEMPTS):
            last_attempt = attempt == MAX_ATTEMPTS - 1
            headers = {"Authorization": self._auth_header(), "Content-Type": "application/json"}
            req = urllib.request.Request(url, data=data, method=method, headers=headers)
            try:
                with _OPENER.open(req, timeout=300) as response:
                    content = response.read()
                    response_headers = dict(response.headers)
                    content_type = response.headers.get("Content-Type", "")
                    if response.status == 203 or (content and "html" in content_type):
                        # e.g. Azure DevOps answers an expired or wrong token with a sign-in page
                        raise ApiError(
                            f"Unexpected sign-in page from {url.split('?')[0]} (HTTP "
                            f"{response.status}) - check the sign-in / access token."
                        )
                    if raw:
                        return response.status, response_headers, content
                    if content and "json" not in content_type:
                        raise ApiError(
                            f"Unexpected non-JSON answer from {url.split('?')[0]} (HTTP "
                            f"{response.status}) - check the sign-in / access token."
                        )
                    return response.status, response_headers, (json.loads(content) if content else None)
            except urllib.error.HTTPError as error:
                content = error.read()
                if error.code in RETRY_STATUSES and not last_attempt:
                    self._sleep(_retry_after(error.headers.get("Retry-After"), 2**attempt))
                    continue
                raise ApiError(error_text(error.code, url, content)) from None
            except (urllib.error.URLError, OSError) as error:  # incl. timeouts, resets
                if not last_attempt:
                    self._sleep(2**attempt)
                    continue
                reason = getattr(error, "reason", error)
                raise ApiError(f"Cannot reach {url.split('?')[0]}: {reason}") from None
        raise ApiError(f"Still failing after {MAX_ATTEMPTS} attempts: {url.split('?')[0]}")


MAX_ATTEMPTS = 5
RETRY_STATUSES = {429, 500, 502, 503, 504}


def _retry_after(value: Optional[str], default: float) -> float:
    """Seconds to wait from a Retry-After header (seconds or an HTTP date), at most 2 minutes."""
    if value:
        try:
            return min(max(float(value), 0), 120)
        except ValueError:
            try:
                delta = email.utils.parsedate_to_datetime(value).timestamp() - time.time()
                return min(max(delta, 0), 120)
            except (TypeError, ValueError):
                pass
    return default


class _NoCrossHostAuthRedirect(urllib.request.HTTPRedirectHandler):
    """Follow redirects, but never carry the Authorization header to another host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new_request = super().redirect_request(req, fp, code, msg, headers, newurl)
        old_host = urllib.parse.urlsplit(req.full_url).netloc.lower()
        if new_request is not None and urllib.parse.urlsplit(newurl).netloc.lower() != old_host:
            new_request.remove_header("Authorization")
        return new_request


_OPENER = urllib.request.build_opener(_NoCrossHostAuthRedirect)


def _error_detail(body: dict) -> str:
    """The reason in a JSON error body: Fabric, Azure DevOps or Power BI format."""
    if isinstance(body.get("error"), dict):  # Power BI: {"error": {"code", "message"?, "pbi.error"}}
        error = body["error"]
        details = [
            str((d.get("detail") or {}).get("value", ""))
            for d in (error.get("pbi.error") or {}).get("details", [])
        ]
        return ": ".join(
            part for part in (error.get("code"), error.get("message"), " ".join(filter(None, details))) if part
        )
    code = body.get("errorCode") or body.get("typeKey") or ""
    return f"{code}: {body.get('message', '')}".strip(": ")


def error_text(code: int, url: str, content: bytes) -> str:
    """Readable error message from an HTTP error response (reason first, then the URL)."""
    try:
        detail = _error_detail(json.loads(content))
    except (ValueError, AttributeError):
        detail = content.decode("utf-8", errors="replace")[:300]
    hint = ""
    if code in (401, 403):
        hint = " - check that you have access (Fabric getDefinition needs Contributor or higher)."
    endpoint = url.split("?")[0]
    return f"HTTP {code}: {detail or '(no details)'}{hint} [{endpoint}]"
