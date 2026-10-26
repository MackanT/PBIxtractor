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
_login_lock = threading.RLock()  # one browser sign-in at a time (web UI tabs share the process)
_apps_lock = threading.Lock()
_caches: dict[Path, tuple] = {}  # token cache file -> (PersistedTokenCache, encrypted)
_apps: dict[tuple, object] = {}  # (token cache file, authority) -> PublicClientApplication
_browser_login_allowed = True
_reported: set[tuple] = set()  # account list read errors already logged

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


def _read_store() -> tuple[list[Account], Optional[str]]:
    """(entries in list order, active key) from accounts.json; nothing when it is missing."""
    path = AUTH_DIR / ACCOUNTS_FILE
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return [], None
    except (OSError, ValueError) as error:
        if (path, str(error)) not in _reported:  # read every few seconds by the web UI
            _reported.add((path, str(error)))
            logger.warning(f"Cannot read the account list {path}: {error}")
        return [], None
    if not isinstance(data, dict):
        return [], None
    entries = []
    for item in data.get("accounts") or []:
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
        for attempt in range(5):
            try:
                os.replace(temp, AUTH_DIR / ACCOUNTS_FILE)
                break
            except PermissionError:  # Windows: another process is reading it right now
                if attempt == 4:
                    raise
                time.sleep(0.05)
    except BaseException:
        Path(temp).unlink(missing_ok=True)
        raise


def accounts() -> list[Account]:
    """The remembered entries: by user, the home tenant first, then by tenant name."""
    return _read_store()[0]


def active_account() -> Optional[Account]:
    """The entry every sign-in uses (None: the next sign-in opens the browser)."""
    entries, active = _read_store()
    return next((a for a in entries if a.key == active), None)


def use_account(key: str) -> Account:
    """
    Make an entry the active one (for the whole app: every tab and the command line).

    Args:
        key: Account.key of a remembered entry

    Raises:
        ValueError: No entry with that key
    """
    with _store_lock:
        entries, _ = _read_store()
        account = next((a for a in entries if a.key == key), None)
        if account is None:
            raise ValueError(f"No account {key}")
        _write_store(entries, key)
        return account


def find_account(text: str) -> Account:
    """
    The entry a user means on the command line.

    Args:
        text: 1-based number in the list, a key, or (case-insensitive) a user name, tenant
            name or tenant id - exactly, else as part of it

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
    if not matches:
        matches = [a for a in entries if any(wanted in value for value in fields(a))]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(f"No account matches '{text}' (see `pbixtractor account list`)")
    names = "; ".join(f"{entries.index(a) + 1}. {a.label}" for a in matches)
    raise ValueError(f"'{text}' matches several accounts: {names} - give the number")


def remove_account(key: str) -> None:
    """
    Forget an entry. The user's last entry also deletes that user's tokens; when the active
    entry goes, the first remaining one becomes active (none left: no active entry).

    Args:
        key: Account.key of a remembered entry

    Raises:
        ValueError: No entry with that key
    """
    with _store_lock:
        entries, active = _read_store()
        account = next((a for a in entries if a.key == key), None)
        if account is None:
            raise ValueError(f"No account {key}")
        remaining = [a for a in entries if a.key != key]
        if active == key:
            active = remaining[0].key if remaining else None
        _write_store(remaining, active)
    if not any(a.home_account_id == account.home_account_id for a in remaining):
        try:
            app = _app(None)
            for cached in app.get_accounts():
                if cached.get("home_account_id") == account.home_account_id:
                    app.remove_account(cached)
        except Exception as error:  # e.g. offline: the sign-in service cannot be reached
            logger.warning(f"Could not delete the tokens of {account.username}: {error}")


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
    """One browser sign-in (never two at once); MSAL's result with an access token."""
    from msal import BrowserInteractionTimeoutError

    _require_browser()
    with _login_lock:
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


def _list_tenants(access_token: str) -> list[dict]:
    """The tenants the signed-in user belongs to (Azure management API, as `az login` does)."""
    client = RestClient("https://management.azure.com", ARM_SCOPE, credential=_FixedToken(access_token))
    tenants: list[dict] = []
    url: Optional[str] = TENANTS_URL
    for _ in range(50):  # pages
        _, _, body = client.request("GET", url)
        tenants.extend((body or {}).get("value") or [])
        url = (body or {}).get("nextLink")
        if not url or urllib.parse.urlsplit(url).hostname != "management.azure.com":
            break
    return tenants


def add_account(tenant: Optional[str] = None, timeout: float = LOGIN_TIMEOUT) -> list[Account]:
    """
    Sign in through Microsoft's sign-in page (with its account picker) and add every tenant of
    that user as an entry. Signing in as a user already in the list refreshes their entries
    (tenants no longer listed disappear).

    Args:
        tenant: Entra tenant (id or domain) to sign in to and make active (default: the user's
            home tenant)
        timeout: Seconds to wait for the sign-in in the browser

    Returns:
        That user's entries, in list order

    Raises:
        ApiError: The sign-in was cancelled, timed out or refused (or no browser is allowed)
    """
    _require_browser()
    tenant = (tenant or "").strip() or None
    app = _app(tenant)
    result = _sign_in(app, [ARM_SCOPE], prompt="select_account", timeout=timeout)
    claims = result.get("id_token_claims") or {}
    username = str(claims.get("preferred_username") or claims.get("upn") or claims.get("name") or "?")
    home_account_id = _home_account_id(result, app)
    home_tenant = home_account_id.partition(".")[2].casefold()
    signed_in = str(claims.get("tid") or tenant or home_tenant)

    try:
        listed = _list_tenants(result["access_token"])
    except Exception as error:  # ApiError mostly: no access to the management API
        logger.warning(
            f"Could not list the tenants of {username} ({error}): added only the tenant signed "
            "in to - add others by their tenant id or domain"
        )
        listed = []
    entries: dict[str, Account] = {}
    for item in listed:
        tenant_id = str(item.get("tenantId") or "").strip()
        if tenant_id:
            entries[tenant_id.casefold()] = Account(
                username,
                home_account_id,
                tenant_id,
                str(item.get("displayName") or ""),
                home=tenant_id.casefold() == home_tenant,
            )
    if signed_in.casefold() not in entries:
        entries[signed_in.casefold()] = Account(
            username, home_account_id, signed_in, home=signed_in.casefold() == home_tenant
        )
    added = _sorted(entries.values())
    if tenant:
        active = entries[signed_in.casefold()]
    else:
        active = next((a for a in added if a.home), entries[signed_in.casefold()])

    with _store_lock:
        existing, _ = _read_store()
        kept = [a for a in existing if a.home_account_id != home_account_id]
        _write_store(kept + added, active.key)
    return added


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


def _bound_account(tenant_id: Optional[str]) -> Optional[Account]:
    """The active entry, or the active user in `tenant_id` (also when that is not listed)."""
    active = active_account()
    if active is None or not tenant_id or not tenant_id.strip():
        return active
    wanted = tenant_id.strip()
    for account in accounts():
        if account.home_account_id == active.home_account_id and (
            account.tenant_id.casefold() == wanted.casefold()
        ):
            return account
    return replace(active, tenant_id=wanted, tenant_name="", home=False)


def _cached_account(app, account: Account) -> Optional[dict]:
    """The user's account record in MSAL's token cache (None: no tokens, sign in again)."""
    return next(
        (c for c in app.get_accounts() if c.get("home_account_id") == account.home_account_id),
        None,
    )


class _AccountCredential:
    """
    Tokens for one account entry: the one active when the credential is created, kept for its
    lifetime (a switch affects only credentials created after it). With no active entry the
    first get_token() signs in and uses the new active entry. Safe to use from several threads.
    """

    def __init__(self, tenant_id: Optional[str] = None):
        self._tenant_id = tenant_id
        self._lock = threading.Lock()
        self._account = _bound_account(tenant_id)

    def _bound(self) -> Account:
        with self._lock:
            if self._account is None:
                with _login_lock:  # another thread may be signing in right now
                    if active_account() is None:
                        add_account(self._tenant_id)
                self._account = _bound_account(self._tenant_id)
                if self._account is None:
                    raise ApiError("Sign-in failed: no account is active")
            return self._account

    def _silent(self, app, account: Account, scopes, claims) -> Optional[dict]:
        try:
            cached = _cached_account(app, account)
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
        waited = not _login_lock.acquire(blocking=False)
        if waited:
            _login_lock.acquire()
        try:
            if waited:  # the other sign-in may have been this user's: try the cache again
                result = self._silent(app, account, scopes, claims)
                if result and "access_token" in result:
                    return result
            result = _sign_in(app, scopes, login_hint=account.username, claims=claims)
        finally:
            _login_lock.release()
        signed_in = _home_account_id(result, app)
        if signed_in != account.home_account_id:
            other = (result.get("id_token_claims") or {}).get("preferred_username") or signed_in
            raise ApiError(
                f"Sign-in failed: signed in as {other}, but the account in use is "
                f"{account.username} - sign in as that user, or add {other} with "
                "`pbixtractor account add`"
            )
        return result


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
    - else the active account entry (Microsoft's sign-in page when there is none yet).

    Args:
        tenant_id: Entra tenant (id or domain) for the active user (default: the active
            entry's; a service principal uses AZURE_TENANT_ID)

    Raises:
        ApiError: No service principal and the browser sign-in is not allowed (embedded)
    """
    return _credential(tenant_id)


def get_credential(tenant_id: Optional[str] = None):
    """
    The credential for the account active now (see default_credential). A client keeps its
    account for its lifetime; clients created after a switch use the new one.

    Args:
        tenant_id: Entra tenant (id or domain) for the active user (default: the active entry's)

    Raises:
        ApiError: No service principal and the browser sign-in is not allowed (embedded)
    """
    return _credential(tenant_id)


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

    def _auth_header(self) -> str:
        if self._pat:
            return "Basic " + base64.b64encode(f":{self._pat}".encode()).decode()
        if self._credential is None:
            self._credential = get_credential()
        if self._token is None or self._token.expires_on - time.time() < 120:
            try:
                self._token = self._credential.get_token(self._scope)
            except ApiError:
                raise
            except Exception as error:  # azure-identity: login cancelled/timed out, az expired
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
