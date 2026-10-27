"""Tests for the sign-in accounts (azure_auth.py) and `pbixtractor account`, with a fake MSAL."""

import base64
import io
import json
import os
import stat
import threading
import time
import urllib.error
from pathlib import Path

import pytest

from pbixtractor import azure_auth
from pbixtractor.azure_auth import ARM_SCOPE, FABRIC_SCOPE, Account, ApiError
from pbixtractor.azure_auth import _list_tenants as real_list_tenants

# The real ones, imported at collection time: conftest replaces both during tests
from pbixtractor.azure_auth import default_credential as real_default_credential
from pbixtractor.azure_auth import get_credential as real_get_credential
from pbixtractor.cli import main

CONTOSO = "aaaaaaaa-0000-0000-0000-000000000001"
FABRIKAM = "aaaaaaaa-0000-0000-0000-000000000002"
NORTHWIND = "aaaaaaaa-0000-0000-0000-000000000003"
NAMES = {CONTOSO: "Contoso", FABRIKAM: "Fabrikam", NORTHWIND: "Northwind"}
DOMAINS = {"fabrikam.com": FABRIKAM}


class FakeCloud:
    """Microsoft's sign-in as far as the tests need it: users, their tenants and their tokens."""

    def __init__(self):
        self.users = {}  # home_account_id -> {"username", "oid", "tenants"}
        self.picker = None  # home_account_id the account picker returns
        self.signed_in = set()  # home_account_ids with tokens in the cache
        self.interactive = []  # the interactive sign-ins (tenant, prompt, login_hint, ...)
        self.interactive_result = None  # replaces the sign-in result (e.g. an error)
        self.interactive_delay = 0.0
        self.during = []  # sign_in_in_progress() inside each interactive sign-in
        self.silent = []  # the silent token requests (tenant, scopes)
        self.silent_errors = {}  # tenant -> MSAL error result for silent calls
        self.scope_errors = {}  # scope -> MSAL error result for silent calls (e.g. blocked ARM)
        self.listing_fails = False
        self.on_listing = None  # called while the tenants are listed (to act meanwhile)
        self.sign_in_as = None  # the user the browser signs in as, whatever the login hint
        self.authorities = []

    def add_user(self, name, home, *guest_of):
        oid = f"oid-{name}"
        home_account_id = f"{oid}.{home}"
        self.users[home_account_id] = {
            "username": f"{name}@{NAMES[home].lower()}.com",
            "oid": oid,
            "tenants": [home, *guest_of],
        }
        return home_account_id

    def token(self, home_account_id, tenant, scopes):
        user = self.users[home_account_id]
        oid, home = home_account_id.split(".")
        client_info = base64.urlsafe_b64encode(json.dumps({"uid": oid, "utid": home}).encode())
        return {
            "access_token": f"{user['username']}|{tenant}|{' '.join(scopes)}",
            "expires_in": 3600,
            "client_info": client_info.decode().rstrip("="),
            "id_token_claims": {"preferred_username": user["username"], "tid": tenant, "oid": oid},
        }

    def list_tenants(self, access_token):
        assert access_token.endswith(f"|{ARM_SCOPE}"), "the listing needs a management token"
        if self.on_listing is not None:
            self.on_listing()
        if self.listing_fails:
            raise ApiError("HTTP 403: AuthorizationFailed [https://management.azure.com/tenants]")
        username = access_token.split("|")[0]
        user = next(u for u in self.users.values() if u["username"] == username)
        return [{"tenantId": t, "displayName": NAMES[t]} for t in user["tenants"]]


class FakeApp:
    """An msal.PublicClientApplication for one authority, backed by the FakeCloud. A sign-in
    also writes the user's records into the real (persisted) token cache, as MSAL does."""

    def __init__(self, cloud, authority, token_cache):
        assert token_cache is not None
        self.cloud = cloud
        self.token_cache = token_cache
        self.tenant = authority.rsplit("/", 1)[1]
        cloud.authorities.append(authority)

    def _tenant_id(self, home_account_id):
        if self.tenant == "organizations":
            return home_account_id.split(".")[1]
        return DOMAINS.get(self.tenant, self.tenant)

    def get_accounts(self, username=None):
        return [
            {"home_account_id": key, "username": user["username"], "environment": "login"}
            for key, user in self.cloud.users.items()
            if key in self.cloud.signed_in
            and (username is None or user["username"].lower() == username.lower())
        ]

    def acquire_token_interactive(self, scopes, prompt=None, login_hint=None, timeout=None, **kw):
        cloud = self.cloud
        cloud.interactive.append(
            {"tenant": self.tenant, "prompt": prompt, "login_hint": login_hint, "timeout": timeout,
             "scopes": list(scopes)}
        )
        cloud.during.append(azure_auth.sign_in_in_progress())
        time.sleep(cloud.interactive_delay)
        if cloud.interactive_result is not None:
            result = cloud.interactive_result
            if isinstance(result, Exception):
                raise result
            return result
        if cloud.sign_in_as:  # another account picked in the browser
            key = cloud.sign_in_as
        elif login_hint:
            key = next(k for k, u in cloud.users.items() if u["username"] == login_hint)
        else:
            key = cloud.picker
        tenant = self._tenant_id(key)
        cloud.signed_in.add(key)
        cloud.silent_errors.pop(tenant, None)  # e.g. the tenant's MFA is now done
        result = cloud.token(key, tenant, scopes)
        self.token_cache.add({
            "client_id": azure_auth.CLIENT_ID,
            "scope": list(scopes),
            "token_endpoint": f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
            "response": dict(result, refresh_token=f"rt-{key}", id_token="fake.id.token"),
            "grant_type": "authorization_code",
        })
        return result

    def acquire_token_silent_with_error(self, scopes, account, claims_challenge=None, **kw):
        key = account["home_account_id"]
        self.cloud.silent.append({"tenant": self.tenant, "scopes": list(scopes)})
        if key not in self.cloud.signed_in:
            return None
        tenant = self._tenant_id(key)
        if tenant in self.cloud.silent_errors:
            return self.cloud.silent_errors[tenant]
        for scope in scopes:
            if scope in self.cloud.scope_errors:
                return self.cloud.scope_errors[scope]
        return self.cloud.token(key, tenant, scopes)

    def remove_account(self, account):
        raise AssertionError("tokens are deleted from the token cache, not through an MSAL app")


@pytest.fixture(autouse=True)
def real_default_credential_restored(monkeypatch, no_real_sign_in):
    """get_credential() goes through azure_auth.default_credential, which conftest replaces:
    these tests use the real one, and no service principal credential kept by another test."""
    monkeypatch.setattr(azure_auth, "default_credential", real_default_credential)
    monkeypatch.setattr(azure_auth, "_service_principal", None)


@pytest.fixture
def cloud(monkeypatch):
    fake = FakeCloud()
    monkeypatch.setattr(azure_auth, "_new_app", lambda authority, cache: FakeApp(fake, authority, cache))
    monkeypatch.setattr(azure_auth, "_list_tenants", fake.list_tenants)
    return fake


@pytest.fixture
def marcus(cloud):
    """A user in Contoso who is a guest in two client tenants; the picker returns him."""
    cloud.picker = cloud.add_user("marcus", CONTOSO, FABRIKAM, NORTHWIND)
    return cloud.picker


def _tenant_of(token) -> str:
    return token.token.split("|")[1]


def _user_of(token) -> str:
    return token.token.split("|")[0]


def _by_tenant(tenant_id) -> Account:
    return next(a for a in azure_auth.accounts() if a.tenant_id == tenant_id)


def _entry(home_account_id, tenant_id) -> Account:
    return next(a for a in azure_auth.accounts()
                if a.home_account_id == home_account_id and a.tenant_id == tenant_id)


def _token_records(home_account_id) -> list[int]:
    """How many account, refresh, access and ID token records the user has in the token cache."""
    cache = azure_auth._token_cache()[0]
    kinds = cache.CredentialType
    return [len(list(cache.search(kind, query={"home_account_id": home_account_id})))
            for kind in (kinds.ACCOUNT, kinds.REFRESH_TOKEN, kinds.ACCESS_TOKEN, kinds.ID_TOKEN)]


class HeldAccountList:
    """accounts.json held by another process (replacing it, a virus scanner): reading it
    raises PermissionError for the next `hold(times)` reads (default: until released)."""

    def __init__(self, monkeypatch):
        real_read = Path.read_bytes
        self.reads = 0  # attempts to read accounts.json
        self._refuse = 0.0

        def read_bytes(path):
            if path.name == azure_auth.ACCOUNTS_FILE:
                self.reads += 1
                if self._refuse > 0:
                    self._refuse -= 1
                    raise PermissionError(13, "Permission denied", str(path))
            return real_read(path)

        monkeypatch.setattr(Path, "read_bytes", read_bytes)
        monkeypatch.setattr(azure_auth, "_READ_RETRY_SECONDS", 0.001)

    def hold(self, times: float = float("inf")) -> None:
        self.reads, self._refuse = 0, times

    def release(self) -> None:
        self._refuse = 0


HELD = r"^Cannot read the account list .*accounts\.json: \[Errno 13\] Permission denied"
MFA = {"error": "interaction_required", "error_description": "MFA"}


def _threaded(target, *args) -> tuple[threading.Thread, list]:
    """A started thread running target(*args); the list gets what it raised."""
    errors: list = []

    def run():
        try:
            target(*args)
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, errors


def _eventually(condition, seconds=5.0) -> bool:
    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


# ---------------------------------------------------------------------------
# Signing in and the account list
# ---------------------------------------------------------------------------


def test_no_azure_cli_in_the_chain(monkeypatch, cloud, marcus):
    import shutil

    import azure.identity

    class NoCli:
        def __init__(self, *args, **kwargs):
            raise AssertionError("the Azure CLI must not be used")

    monkeypatch.setattr(shutil, "which", lambda name: f"/usr/bin/{name}")  # `az` is installed
    monkeypatch.setattr(azure.identity, "AzureCliCredential", NoCli)
    azure_auth.add_account()
    for make in (real_get_credential, real_default_credential):
        credential = make()
        assert not isinstance(credential, azure.identity.ChainedTokenCredential)
        assert _user_of(credential.get_token(FABRIC_SCOPE)) == "marcus@contoso.com"
    assert "AzureCli" not in open(azure_auth.__file__, encoding="utf-8").read()


def test_a_service_principal_still_comes_first(monkeypatch, cloud):
    from azure.identity import EnvironmentCredential

    for name, value in (("AZURE_CLIENT_ID", "x"), ("AZURE_CLIENT_SECRET", "y"),
                        ("AZURE_TENANT_ID", CONTOSO)):
        monkeypatch.setenv(name, value)
    assert isinstance(real_get_credential(), EnvironmentCredential)
    assert isinstance(real_default_credential(FABRIKAM), EnvironmentCredential)
    assert cloud.interactive == []


def test_get_credential_keeps_one_service_principal_default_credential_does_not(monkeypatch):
    for name, value in (("AZURE_CLIENT_ID", "x"), ("AZURE_CLIENT_SECRET", "kept"),
                        ("AZURE_TENANT_ID", CONTOSO)):
        monkeypatch.setenv(name, value)
    shared = real_get_credential()
    assert real_get_credential() is shared  # its token cache is reused (no new token per client)
    assert real_get_credential(FABRIKAM) is shared  # a service principal has its one tenant
    assert real_default_credential() is not shared
    assert real_default_credential() is not real_default_credential()

    monkeypatch.setenv("AZURE_CLIENT_SECRET", "rotated")  # other values: another credential
    rotated = real_get_credential()
    assert rotated is not shared and real_get_credential() is rotated


def test_get_credential_goes_through_default_credential(monkeypatch):
    made = []

    def replaced(tenant_id=None):
        made.append(tenant_id)
        return object()

    monkeypatch.setattr(azure_auth, "default_credential", replaced)
    first = real_get_credential(FABRIKAM)
    assert made == [FABRIKAM]  # a replaced default_credential is what get_credential returns
    assert real_get_credential() is not first  # accounts: one per call (bound when made)

    for name, value in (("AZURE_CLIENT_ID", "x"), ("AZURE_CLIENT_SECRET", "y"),
                        ("AZURE_TENANT_ID", CONTOSO)):
        monkeypatch.setenv(name, value)
    shared = real_get_credential()
    assert real_get_credential() is shared and real_get_credential(FABRIKAM) is shared
    assert made == [FABRIKAM, None, None]  # a service principal's: made once per process
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "rotated")
    assert real_get_credential() is not shared and len(made) == 4


def test_adding_an_account_lists_every_tenant_and_makes_home_active(cloud, marcus):
    added = azure_auth.add_account()

    # The browser asks for Fabric (a tenant may block Azure management); the management token
    # for the tenant listing comes silently from the cache
    assert cloud.interactive == [
        {"tenant": "organizations", "prompt": "select_account", "login_hint": None,
         "timeout": azure_auth.LOGIN_TIMEOUT, "scopes": [FABRIC_SCOPE]}
    ]
    assert cloud.silent == [{"tenant": "organizations", "scopes": [ARM_SCOPE]}]
    assert [a.label for a in added] == [
        "marcus@contoso.com · Contoso",  # home first, then by tenant name
        "marcus@contoso.com · Fabrikam",
        "marcus@contoso.com · Northwind",
    ]
    assert azure_auth.accounts() == added
    assert [a.home for a in added] == [True, False, False]
    assert added[0].home_account_id == marcus
    assert added[0].key == f"{marcus}|{CONTOSO}"
    assert azure_auth.active_account() == added[0]

    stored = json.loads((azure_auth.AUTH_DIR / "accounts.json").read_text(encoding="utf-8"))
    assert stored["active"] == added[0].key
    assert {key for entry in stored["accounts"] for key in entry} == {
        "username", "home_account_id", "tenant_id", "tenant_name", "home"
    }  # no tokens in the account list
    assert "|" not in json.dumps(stored["accounts"])  # (the fake's tokens contain "|")


def test_adding_with_a_tenant_signs_in_there_and_makes_it_active(cloud, marcus):
    azure_auth.add_account(FABRIKAM)
    assert cloud.interactive[-1]["tenant"] == FABRIKAM
    assert azure_auth.active_account() == _by_tenant(FABRIKAM)
    assert len(azure_auth.accounts()) == 3

    azure_auth.add_account("fabrikam.com")  # a domain: the token tells the tenant id
    assert cloud.interactive[-1]["tenant"] == "fabrikam.com"
    assert azure_auth.active_account().tenant_id == FABRIKAM


def test_adding_the_same_user_again_refreshes_their_entries(cloud, marcus):
    cloud.picker = cloud.add_user("anna", NORTHWIND)
    azure_auth.add_account()
    cloud.picker = marcus
    azure_auth.add_account()
    assert len(azure_auth.accounts()) == 4

    cloud.users[marcus]["tenants"].remove(NORTHWIND)  # no longer a guest there
    added = azure_auth.add_account()
    assert [a.tenant_id for a in added] == [CONTOSO, FABRIKAM]
    assert [a.label for a in azure_auth.accounts()] == [
        "anna@northwind.com · Northwind",
        "marcus@contoso.com · Contoso",
        "marcus@contoso.com · Fabrikam",
    ]


def test_when_tenants_cannot_be_listed_the_users_entries_are_kept(cloud, marcus, caplog):
    cloud.listing_fails = True
    warnings = []
    added = azure_auth.add_account(on_warning=warnings.append)
    assert added == [Account("marcus@contoso.com", marcus, CONTOSO, "", home=True)]
    assert added[0].label == f"marcus@contoso.com · {CONTOSO}"
    assert "Could not list the tenants of marcus@contoso.com" in caplog.text  # still logged
    assert len(warnings) == 1
    assert warnings[0].startswith("Could not list the tenants of marcus@contoso.com (HTTP 403")
    assert warnings[0].endswith("add other tenants by their tenant id or domain")

    added = azure_auth.add_account(NORTHWIND)  # the way in then: a typed tenant
    assert [a.tenant_id for a in added] == [CONTOSO, NORTHWIND]  # added, Contoso kept
    assert azure_auth.accounts() == added
    assert azure_auth.active_account().tenant_id == NORTHWIND

    cloud.listing_fails = False
    azure_auth.add_account()  # listed: every tenant, with its name
    names = ["Contoso", "Fabrikam", "Northwind"]
    assert [a.tenant_name for a in azure_auth.accounts()] == names

    cloud.listing_fails = True  # fails again: nothing deleted, the names kept
    warnings.clear()
    added = azure_auth.add_account(FABRIKAM, on_warning=warnings.append)
    assert [a.tenant_name for a in added] == names and len(warnings) == 1
    assert azure_auth.active_account() == _by_tenant(FABRIKAM)


def test_a_tenant_that_blocks_azure_management_can_still_sign_in(cloud, marcus):
    azure_auth.add_account()
    blocked = {"error": "interaction_required",
               "error_description": "AADSTS53003: Access has been blocked by Conditional Access "
                                    "policies.\r\nTrace ID: 4"}
    cloud.scope_errors[ARM_SCOPE] = blocked
    warnings = []
    added = azure_auth.add_account(FABRIKAM, on_warning=warnings.append)

    # No browser for the management API: a failed silent token is a failed listing
    assert [i["scopes"] for i in cloud.interactive] == [[FABRIC_SCOPE], [FABRIC_SCOPE]]
    assert "AADSTS53003: Access has been blocked by Conditional Access policies." in warnings[0]
    assert [a.tenant_name for a in added] == ["Contoso", "Fabrikam", "Northwind"]  # kept
    assert azure_auth.active_account() == _by_tenant(FABRIKAM)
    assert _tenant_of(real_get_credential().get_token(FABRIC_SCOPE)) == FABRIKAM


class FakeOpener:
    """azure_auth._OPENER for the tenant listing: url -> JSON body, or an exception to raise."""

    def __init__(self, answers):
        self.answers = answers
        self.calls = []  # (url, Authorization header, timeout)

    def open(self, request, timeout=None):
        self.calls.append((request.full_url, request.get_header("Authorization"), timeout))
        answer = self.answers[request.full_url]
        if isinstance(answer, Exception):
            raise answer
        return FakeResponse(json.dumps(answer).encode())


class FakeResponse(io.BytesIO):  # a urllib response as far as _list_tenants needs one
    status = 200


def _sleeps(monkeypatch) -> list:
    """The seconds anything in azure_auth sleeps (without sleeping)."""
    slept: list = []
    monkeypatch.setattr(azure_auth.time, "sleep", lambda seconds: slept.append(seconds))
    return slept


def test_tenants_are_listed_page_by_page(monkeypatch):
    opener = FakeOpener({
        azure_auth.TENANTS_URL: {"value": [{"tenantId": CONTOSO}],
                                 "nextLink": "https://management.azure.com/tenants?page=2"},
        "https://management.azure.com/tenants?page=2": {
            "value": [{"tenantId": FABRIKAM}], "nextLink": "https://elsewhere.example.com/x"},
        "https://management.azure.com/tenants?page=3": {"value": [{"tenantId": NORTHWIND}]},
    })
    monkeypatch.setattr(azure_auth, "_OPENER", opener)
    tenants = azure_auth._list_tenants("arm-token")
    assert [t["tenantId"] for t in tenants] == [CONTOSO, FABRIKAM]
    # A nextLink to another host is not followed; each page one attempt with a short timeout
    assert [url for url, _, _ in opener.calls] == [azure_auth.TENANTS_URL,
                                                   "https://management.azure.com/tenants?page=2"]
    assert all(auth == "Bearer arm-token" for _, auth, _ in opener.calls)
    assert all(0 < timeout <= azure_auth.TENANTS_PAGE_TIMEOUT for _, _, timeout in opener.calls)

    # Nor one over http
    opener.answers[azure_auth.TENANTS_URL]["nextLink"] = "http://management.azure.com/tenants?page=3"
    opener.calls.clear()
    assert [t["tenantId"] for t in azure_auth._list_tenants("arm-token")] == [CONTOSO]
    assert len(opener.calls) == 1


def test_a_throttled_tenant_listing_fails_at_once(monkeypatch, cloud, marcus):
    monkeypatch.setattr(azure_auth, "_list_tenants", real_list_tenants)
    throttled = urllib.error.HTTPError(
        azure_auth.TENANTS_URL, 429, "Too Many Requests", {"Retry-After": "120"},
        io.BytesIO(b'{"error": {"code": "TooManyRequests", "message": "Slow down"}}'),
    )
    opener = FakeOpener({azure_auth.TENANTS_URL: throttled})
    monkeypatch.setattr(azure_auth, "_OPENER", opener)
    slept = _sleeps(monkeypatch)
    warnings = []
    added = azure_auth.add_account(on_warning=warnings.append)
    assert len(opener.calls) == 1 and not [s for s in slept if s]  # no retry, no Retry-After
    assert "(HTTP 429: TooManyRequests: Slow down" in warnings[0]  # the listing-failed path
    assert added == [Account("marcus@contoso.com", marcus, CONTOSO, "", home=True)]
    assert azure_auth.active_account() == added[0]

    # The whole listing has a deadline too
    monkeypatch.setattr(azure_auth, "TENANTS_DEADLINE", 0)
    with pytest.raises(ApiError, match="Listing the tenants took longer than 0 s"):
        real_list_tenants("arm-token")


def test_sign_in_errors(cloud, marcus):
    from msal import BrowserInteractionTimeoutError

    cloud.interactive_result = BrowserInteractionTimeoutError("User did not complete the flow")
    with pytest.raises(ApiError, match="Sign-in failed: not completed within 180 s"):
        azure_auth.add_account()
    cloud.interactive_result = {"error": "access_denied",
                                "error_description": "AADSTS65004: User declined\r\nTrace ID: 1"}
    with pytest.raises(ApiError, match=r"Sign-in failed: AADSTS65004: User declined$"):
        azure_auth.add_account()
    assert azure_auth.accounts() == []


def test_a_sign_in_is_in_progress_for_the_whole_add_and_every_interactive_token(
    monkeypatch, cloud, marcus
):
    from msal import BrowserInteractionTimeoutError

    seen = []
    cloud.on_listing = lambda: seen.append(("listing", azure_auth.sign_in_in_progress()))
    real_write = azure_auth._write_store

    def write(entries, active):
        seen.append(("writing", azure_auth.sign_in_in_progress()))
        real_write(entries, active)

    monkeypatch.setattr(azure_auth, "_write_store", write)
    assert azure_auth.sign_in_in_progress() is False
    azure_auth.add_account()  # the browser, listing the tenants, writing the entries
    assert cloud.during == [True] and seen == [("listing", True), ("writing", True)]
    assert azure_auth.sign_in_in_progress() is False

    # Every way out ends it: the browser times out or fails, the listing fails, writing fails
    cloud.interactive_result = BrowserInteractionTimeoutError("User did not complete the flow")
    with pytest.raises(ApiError, match="not completed within"):
        azure_auth.add_account()
    cloud.interactive_result = RuntimeError("no browser could be started")
    with pytest.raises(ApiError, match="Sign-in failed: no browser could be started"):
        azure_auth.add_account()
    assert azure_auth.sign_in_in_progress() is False
    cloud.interactive_result = None
    cloud.listing_fails = True
    azure_auth.add_account()
    assert azure_auth.sign_in_in_progress() is False
    cloud.listing_fails = False

    def disk_full(entries, active):
        raise OSError("No space left on device")

    monkeypatch.setattr(azure_auth, "_write_store", disk_full)
    with pytest.raises(OSError, match="No space left"):
        azure_auth.add_account()
    assert azure_auth.sign_in_in_progress() is False
    monkeypatch.setattr(azure_auth, "_write_store", write)

    # ensure_account (nested: ensure -> add -> browser): until its entries are written
    for account in azure_auth.accounts():
        azure_auth.remove_account(account.key)
    seen.clear()
    azure_auth.ensure_account()
    assert seen == [("listing", True), ("writing", True)]
    assert azure_auth.sign_in_in_progress() is False

    # A token that needs interaction (e.g. a tenant's own MFA), from a run's thread
    cloud.silent_errors[CONTOSO] = dict(MFA)
    cloud.interactive_delay = 0.3
    during = len(cloud.during)
    thread, errors = _threaded(lambda: real_get_credential().get_token(FABRIC_SCOPE))
    assert _eventually(azure_auth.sign_in_in_progress)
    thread.join(10)
    assert errors == [] and cloud.during[during:] == [True]
    assert azure_auth.sign_in_in_progress() is False

    # Two at once (a run's token, an "Add account" in another tab): until the last one ends
    cloud.silent_errors[CONTOSO] = dict(MFA)
    after_token = []

    def token():
        real_get_credential().get_token(FABRIC_SCOPE)
        after_token.append(azure_auth.sign_in_in_progress())

    during = len(cloud.during)
    first, first_errors = _threaded(token)
    assert _eventually(lambda: len(cloud.during) > during)  # its browser is open
    second, second_errors = _threaded(azure_auth.add_account)  # waits for that one
    first.join(10)
    assert first_errors == [] and after_token == [True]  # the second one is still going
    second.join(10)
    assert second_errors == [] and azure_auth.sign_in_in_progress() is False


def test_finding_and_using_an_entry(cloud, marcus):
    cloud.picker = cloud.add_user("anna", NORTHWIND)
    azure_auth.add_account()
    cloud.picker = marcus
    azure_auth.add_account()
    entries = azure_auth.accounts()  # anna · Northwind, marcus · Contoso / Fabrikam / Northwind

    assert azure_auth.find_account("2") == entries[1]
    assert azure_auth.find_account(entries[3].key) == entries[3]
    assert azure_auth.find_account("fabrikam") == entries[2]  # part of a tenant name
    assert azure_auth.find_account("ANNA@northwind.com") == entries[0]
    assert azure_auth.find_account(FABRIKAM) == entries[2]
    assert azure_auth.find_account("marcus@contoso.com · Northwind") == entries[3]
    with pytest.raises(ValueError, match=r"'northwind' matches several accounts: 1\. .*; 4\. "):
        azure_auth.find_account("northwind")
    with pytest.raises(ValueError, match="No account matches 'nobody'"):
        azure_auth.find_account("nobody")
    with pytest.raises(ValueError, match="No account number 5"):
        azure_auth.find_account("5")

    # exact (for removing): a number, a key or a whole user / tenant name / tenant id / label
    assert azure_auth.find_account("3", exact=True) == entries[2]
    assert azure_auth.find_account(entries[1].key, exact=True) == entries[1]
    assert azure_auth.find_account("FABRIKAM", exact=True) == entries[2]
    assert azure_auth.find_account("anna@northwind.com", exact=True) == entries[0]
    assert azure_auth.find_account(f" {FABRIKAM} ", exact=True) == entries[2]
    assert azure_auth.find_account("marcus@contoso.com · northwind", exact=True) == entries[3]
    for part in ("fabrik", "anna", "aaaaaaaa-0000"):
        with pytest.raises(ValueError, match=f"No account matches '{part}' exactly"):
            azure_auth.find_account(part, exact=True)
    with pytest.raises(ValueError, match="'Northwind' matches several accounts"):
        azure_auth.find_account("Northwind", exact=True)

    assert azure_auth.use_account(entries[2].key) == entries[2]
    assert azure_auth.active_account() == entries[2]
    with pytest.raises(ValueError, match="No account"):
        azure_auth.use_account("unknown|key")


def test_removing_the_active_entry_stays_with_the_same_user(cloud, marcus):
    anna = cloud.picker = cloud.add_user("anna", NORTHWIND, FABRIKAM)
    azure_auth.add_account()
    cloud.picker = marcus
    azure_auth.add_account()  # anna · Northwind / Fabrikam first, then marcus's three
    contoso, fabrikam, northwind = (_entry(marcus, t) for t in (CONTOSO, FABRIKAM, NORTHWIND))

    azure_auth.remove_account(fabrikam.key)  # not the active one: the active entry stays
    assert azure_auth.active_account() == contoso
    azure_auth.add_account()
    azure_auth.use_account(northwind.key)

    azure_auth.remove_account(northwind.key)  # his home tenant, not the list's first (anna)
    assert azure_auth.active_account() == contoso
    azure_auth.remove_account(contoso.key)  # without his home tenant: his other entry
    assert azure_auth.active_account() == fabrikam
    azure_auth.remove_account(fabrikam.key)  # his last one: the first remaining entry
    assert azure_auth.active_account() == azure_auth.accounts()[0] == _entry(anna, NORTHWIND)

    for account in azure_auth.accounts():
        azure_auth.remove_account(account.key)
    assert azure_auth.accounts() == [] and azure_auth.active_account() is None
    with pytest.raises(ValueError, match="No account"):
        azure_auth.remove_account(contoso.key)


def test_a_users_last_entry_deletes_their_tokens_offline(monkeypatch, cloud, marcus):
    anna = cloud.picker = cloud.add_user("anna", NORTHWIND)
    azure_auth.add_account()
    cloud.picker = marcus
    azure_auth.add_account()
    assert all(_token_records(marcus)) and all(_token_records(anna))

    def offline(tenant):
        raise AssertionError("removing tokens must not need an MSAL app (sign-in metadata)")

    monkeypatch.setattr(azure_auth, "_app", offline)
    azure_auth.remove_account(_entry(marcus, CONTOSO).key)
    azure_auth.remove_account(_entry(marcus, FABRIKAM).key)
    assert all(_token_records(marcus))  # he still has an entry: his tokens stay

    azure_auth.remove_account(_entry(marcus, NORTHWIND).key)  # his last entry: his tokens go
    assert _token_records(marcus) == [0, 0, 0, 0] and all(_token_records(anna))
    stored = (azure_auth.AUTH_DIR / azure_auth.TOKEN_CACHE_FILE).read_text(encoding="utf-8")
    assert marcus not in stored and anna in stored  # also in the file (other processes)


def test_tokens_that_cannot_be_deleted_are_reported(monkeypatch, cloud, marcus):
    azure_auth.add_account()
    cache = azure_auth._token_cache()[0]

    def locked(record):
        raise OSError("the token cache is locked")

    monkeypatch.setattr(cache, "remove_rt", locked)
    *others, last = azure_auth.accounts()
    for account in others:
        azure_auth.remove_account(account.key)
    with pytest.raises(ApiError, match=r"^Removed marcus@contoso\.com · Northwind, but could not "
                                       r"delete the tokens of marcus@contoso\.com from .*"
                                       r"msal_token_cache\.bin: the token cache is locked$"):
        azure_auth.remove_account(last.key)
    assert azure_auth.accounts() == [] and azure_auth.active_account() is None  # entry gone


def test_removing_a_users_last_entry_during_their_sign_in_keeps_its_tokens(cloud, marcus):
    azure_auth.add_account()
    *others, last = azure_auth.accounts()
    for account in others:
        azure_auth.remove_account(account.key)
    listing, finish = threading.Event(), threading.Event()

    def slow_listing():  # signed in again: the fresh tokens are cached, the tenants not listed
        listing.set()
        assert finish.wait(10)

    cloud.on_listing = slow_listing
    adding, adding_errors = _threaded(azure_auth.add_account)
    assert listing.wait(10)
    removing, removing_errors = _threaded(azure_auth.remove_account, last.key)
    try:
        assert _eventually(lambda: azure_auth.accounts() == [])  # the entry goes at once,
        assert removing.is_alive()  # its tokens wait for the sign-in to end
    finally:
        finish.set()
    adding.join(10)
    removing.join(10)
    assert adding_errors == [] and removing_errors == []
    assert len(azure_auth.accounts()) == 3  # the sign-in added him again
    assert all(_token_records(marcus))  # with its fresh tokens

    cloud.on_listing = None  # no sign-in going on: his last entry deletes them at once
    for account in azure_auth.accounts():
        azure_auth.remove_account(account.key)
    assert _token_records(marcus) == [0, 0, 0, 0]


def test_the_old_auth_record_is_left_alone(cloud, marcus):
    record = azure_auth.AUTH_DIR / "auth_record.json"
    record.write_text('{"username": "old"}', encoding="utf-8")
    azure_auth.add_account()
    azure_auth.remove_account(azure_auth.accounts()[0].key)
    assert record.read_text(encoding="utf-8") == '{"username": "old"}'
    assert not hasattr(azure_auth, "AUTH_RECORD")


def test_a_missing_account_list_is_no_accounts(cloud):
    assert not (azure_auth.AUTH_DIR / azure_auth.ACCOUNTS_FILE).exists()
    assert azure_auth.account_snapshot() == ([], None)
    assert azure_auth.accounts() == [] and azure_auth.active_account() is None


def test_an_account_list_that_cannot_be_read_is_an_error_never_no_accounts(
    monkeypatch, cloud, marcus
):
    azure_auth.add_account()
    entries, active = azure_auth.account_snapshot()
    path = azure_auth.AUTH_DIR / azure_auth.ACCOUNTS_FILE
    with open(path, "rb") as file:
        before = file.read()
    held = HeldAccountList(monkeypatch)

    held.hold(2)  # held for a moment: retried
    assert azure_auth.account_snapshot() == (entries, active) and held.reads == 3

    held.hold()  # still held after the retries: an error
    for call in (azure_auth.account_snapshot, azure_auth.accounts, azure_auth.active_account,
                 lambda: azure_auth.find_account("1")):
        held.hold()
        with pytest.raises(ApiError, match=HELD):
            call()
        assert held.reads == azure_auth._READ_ATTEMPTS

    # Nothing is written then (an empty list would wipe the entries), no browser opens
    def no_write(entries, active):
        raise AssertionError("accounts.json must not be written")

    monkeypatch.setattr(azure_auth, "_write_store", no_write)
    for call in (lambda: azure_auth.use_account(entries[1].key),
                 lambda: azure_auth.remove_account(entries[0].key),
                 azure_auth.add_account, azure_auth.ensure_account):
        with pytest.raises(ApiError, match=HELD):
            call()
    assert len(cloud.interactive) == 1  # only the first add
    assert all(_token_records(marcus))

    # A credential raises instead of signing in, also one made while the list was held
    credential = real_get_credential()
    with pytest.raises(ApiError, match=HELD):
        credential.get_token(FABRIC_SCOPE)
    assert len(cloud.interactive) == 1 and azure_auth.sign_in_in_progress() is False
    held.release()
    assert _tenant_of(credential.get_token(FABRIC_SCOPE)) == CONTOSO  # readable again: works
    with open(path, "rb") as file:
        assert file.read() == before

    # Held from the moment the sign-in lists the tenants: the entries are not written either
    cloud.on_listing = held.hold
    with pytest.raises(ApiError, match=HELD):
        azure_auth.add_account(FABRIKAM)
    assert len(cloud.interactive) == 2 and azure_auth.sign_in_in_progress() is False
    held.release()
    assert azure_auth.account_snapshot() == (entries, active)


def test_a_damaged_account_list_is_an_error_that_says_what_to_do(cloud, marcus):
    path = azure_auth.AUTH_DIR / azure_auth.ACCOUNTS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    damaged = (r"^The account list .*accounts\.json is damaged \(.+\) - delete it and sign "
               r"in again$")
    for content in (b'{"active": "x", "accounts": [', b"[]", b"\xff\xfe{\x00"):
        path.write_bytes(content)
        for call in (azure_auth.accounts, azure_auth.active_account, azure_auth.add_account,
                     lambda: real_get_credential().get_token(FABRIC_SCOPE),
                     lambda: azure_auth.use_account("x")):
            with pytest.raises(ApiError, match=damaged):
                call()
        assert path.read_bytes() == content  # never replaced by an empty list
    assert cloud.interactive == []

    path.unlink()  # the way out: delete it and sign in again
    azure_auth.add_account()
    assert len(azure_auth.accounts()) == 3


@pytest.mark.parametrize(
    "accounts_value", [5, True, "x", [{"username": "a"}, 3, None]],
    ids=["number", "true", "text", "no-valid-entry"],
)
def test_accounts_that_are_not_a_list_of_entries_are_damaged(cloud, marcus, accounts_value):
    path = azure_auth.AUTH_DIR / azure_auth.ACCOUNTS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps({"active": None, "accounts": accounts_value}).encode()
    path.write_bytes(content)
    damaged = r"^The account list .*accounts\.json is damaged \(.+\) - delete it and sign in again$"
    for call in (azure_auth.account_snapshot, lambda: azure_auth.account_snapshot(wait=False),
                 azure_auth.add_account):
        with pytest.raises(ApiError, match=damaged):
            call()
    assert path.read_bytes() == content and cloud.interactive == []  # never overwritten


def test_an_empty_or_partly_valid_account_list(cloud, marcus):
    path = azure_auth.AUTH_DIR / azure_auth.ACCOUNTS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    for data in ({"accounts": []}, {"accounts": None}, {}):  # no accounts
        path.write_text(json.dumps(data), encoding="utf-8")
        assert azure_auth.account_snapshot() == ([], None)
    valid = {"username": "marcus@contoso.com", "home_account_id": marcus, "tenant_id": CONTOSO,
             "tenant_name": "Contoso", "home": True}
    key = f"{marcus}|{CONTOSO}"
    path.write_text(json.dumps({"active": key, "accounts": [3, {"username": "x"}, valid]}),
                    encoding="utf-8")
    entry = Account("marcus@contoso.com", marcus, CONTOSO, "Contoso", home=True)
    assert azure_auth.account_snapshot() == ([entry], entry)  # the invalid ones skipped


def test_cli_account_list_on_a_damaged_account_list(cloud, marcus, capsys):
    path = azure_auth.AUTH_DIR / azure_auth.ACCOUNTS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"accounts": 5}', encoding="utf-8")
    code, out, err = _run(capsys, "list")
    assert code == 1 and out == ""
    assert err.startswith("ERROR: The account list ") and "delete it and sign in again" in err
    assert "Traceback" not in err


def test_a_poll_reads_an_account_list_that_cannot_be_read_once_without_sleeping(
    monkeypatch, cloud, marcus
):
    azure_auth.add_account()
    held = HeldAccountList(monkeypatch)
    slept = _sleeps(monkeypatch)
    held.hold()
    with pytest.raises(ApiError, match=HELD):
        azure_auth.account_snapshot(wait=False)
    assert held.reads == 1 and slept == []
    held.hold()
    with pytest.raises(ApiError, match=HELD):
        azure_auth.account_snapshot()  # the default still retries
    assert held.reads == azure_auth._READ_ATTEMPTS and len(slept) == azure_auth._READ_ATTEMPTS - 1
    held.release()
    assert len(azure_auth.account_snapshot(wait=False)[0]) == 3


def test_a_sign_in_that_cannot_be_kept_leaves_no_new_tokens(monkeypatch, cloud, marcus, caplog):
    azure_auth.add_account()
    anna = cloud.add_user("anna", NORTHWIND)
    cloud.picker = anna
    real_write = azure_auth._write_store

    def disk_full(entries, active):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(azure_auth, "_write_store", disk_full)
    with pytest.raises(OSError, match="No space left on device"):  # the original error
        azure_auth.add_account()
    assert _token_records(anna) == [0, 0, 0, 0]  # she has no entry: her tokens are deleted
    assert all(_token_records(marcus)) and azure_auth.sign_in_in_progress() is False

    cloud.picker = marcus  # someone with entries keeps their tokens
    with pytest.raises(OSError, match="No space left on device"):
        azure_auth.add_account()
    assert all(_token_records(marcus)) and len(azure_auth.accounts()) == 3

    # The tokens cannot be deleted either: a warning, still the original error
    cloud.picker = anna

    def locked(home_account_id):
        raise OSError("the token cache is locked")

    monkeypatch.setattr(azure_auth, "_delete_unlisted_tokens", locked)
    with pytest.raises(OSError, match="No space left on device"):
        azure_auth.add_account()
    assert "Could not delete the tokens of anna@northwind.com" in caplog.text
    monkeypatch.setattr(azure_auth, "_write_store", real_write)


def test_a_poll_sees_a_changed_account_list_without_reading_it(monkeypatch, cloud, marcus):
    changed, missing = azure_auth.accounts_changed_since(None)
    assert changed is True  # never read
    assert azure_auth.accounts_changed_since(missing) == (False, missing)  # still missing
    azure_auth.add_account()
    changed, stamp = azure_auth.accounts_changed_since(missing)
    assert changed is True and stamp != missing

    def not_read(*args):
        raise AssertionError("the account list must not be read")

    real_read_store, real_read_bytes = azure_auth._read_store, Path.read_bytes
    monkeypatch.setattr(azure_auth, "_read_store", not_read)
    monkeypatch.setattr(Path, "read_bytes", not_read)
    assert azure_auth.accounts_changed_since(stamp) == (False, stamp)
    monkeypatch.setattr(azure_auth, "_read_store", real_read_store)
    monkeypatch.setattr(Path, "read_bytes", real_read_bytes)

    azure_auth.use_account(_by_tenant(FABRIKAM).key)  # same size: a new file all the same
    changed, newer = azure_auth.accounts_changed_since(stamp)
    assert changed is True and newer not in (stamp, missing)
    assert azure_auth.accounts_changed_since(newer) == (False, newer)

    (azure_auth.AUTH_DIR / azure_auth.ACCOUNTS_FILE).unlink()
    assert azure_auth.accounts_changed_since(newer) == (True, missing)

    real_stat = Path.stat

    def refused(path, *args, **kwargs):
        if path.name == azure_auth.ACCOUNTS_FILE:
            raise PermissionError(13, "Permission denied", str(path))
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", refused)  # cannot even look: read it (that tells why)
    assert azure_auth.accounts_changed_since(missing) == (True, None)


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


def test_a_credential_keeps_its_account_after_a_switch(cloud, marcus):
    from azure.core.credentials import AccessToken

    azure_auth.add_account()
    before = real_get_credential()
    azure_auth.use_account(_by_tenant(FABRIKAM).key)
    after = real_get_credential()

    token = before.get_token(FABRIC_SCOPE)
    assert isinstance(token, AccessToken)
    assert token.expires_on > time.time() + 3000
    assert token.token == f"marcus@contoso.com|{CONTOSO}|{FABRIC_SCOPE}"
    assert _tenant_of(after.get_token(FABRIC_SCOPE)) == FABRIKAM
    assert _tenant_of(before.get_token(FABRIC_SCOPE)) == CONTOSO  # still bound to Contoso
    assert len(cloud.interactive) == 1  # only the first sign-in


def test_a_tenant_override_uses_the_active_user_there(cloud, marcus):
    cloud.picker = cloud.add_user("anna", NORTHWIND, FABRIKAM)
    azure_auth.add_account()
    cloud.picker = marcus
    azure_auth.add_account()  # marcus · Contoso active

    assert real_get_credential(FABRIKAM).get_token(FABRIC_SCOPE).token.startswith(
        f"marcus@contoso.com|{FABRIKAM}|"
    )
    token = real_get_credential("other.example.com").get_token(FABRIC_SCOPE)  # not listed
    assert token.token.startswith("marcus@contoso.com|other.example.com|")
    assert cloud.authorities[-1] == "https://login.microsoftonline.com/other.example.com"

    azure_auth.use_account(next(a for a in azure_auth.accounts() if a.username.startswith("anna")).key)
    assert _user_of(real_get_credential(FABRIKAM).get_token(FABRIC_SCOPE)) == "anna@northwind.com"
    assert azure_auth.active_account().tenant_id == NORTHWIND  # the override changes nothing
    assert len(cloud.interactive) == 2


def test_the_first_token_without_an_account_signs_in(cloud, marcus):
    credential = real_get_credential()
    assert cloud.interactive == []  # nothing happens until a token is needed
    assert _tenant_of(credential.get_token(FABRIC_SCOPE)) == CONTOSO
    assert cloud.interactive[0]["prompt"] == "select_account"
    assert len(azure_auth.accounts()) == 3
    assert azure_auth.active_account().tenant_id == CONTOSO


def test_the_first_token_with_a_tenant_signs_in_there_and_makes_home_active(cloud, marcus):
    for make in (real_get_credential, real_default_credential):
        for account in azure_auth.accounts():
            azure_auth.remove_account(account.key)
        cloud.interactive.clear()

        assert _tenant_of(make(FABRIKAM).get_token(FABRIC_SCOPE)) == FABRIKAM  # this one: there
        assert [i["tenant"] for i in cloud.interactive] == [FABRIKAM]
        assert len(azure_auth.accounts()) == 3
        assert azure_auth.active_account().tenant_id == CONTOSO  # the app: the home tenant
        assert _tenant_of(make().get_token(FABRIC_SCOPE)) == CONTOSO
        assert len(cloud.interactive) == 1

    azure_auth.add_account(FABRIKAM)  # asked for explicitly: that tenant becomes active
    assert azure_auth.active_account().tenant_id == FABRIKAM


def test_a_guest_tenant_without_a_tenant_listing_still_makes_home_active(cloud, marcus, caplog):
    cloud.listing_fails = True
    home = Account("marcus@contoso.com", marcus, CONTOSO, "", home=True)
    for make in (real_get_credential, real_default_credential):
        for account in azure_auth.accounts():
            azure_auth.remove_account(account.key)
        cloud.interactive.clear()

        assert _tenant_of(make(FABRIKAM).get_token(FABRIC_SCOPE)) == FABRIKAM  # this one: there
        assert [i["tenant"] for i in cloud.interactive] == [FABRIKAM]
        assert azure_auth.accounts() == [home, Account("marcus@contoso.com", marcus, FABRIKAM)]
        assert azure_auth.active_account() == home  # the app: the home tenant, name unknown
        assert _tenant_of(make().get_token(FABRIC_SCOPE)) == CONTOSO
        assert len(cloud.interactive) == 1
    assert ("only the tenant signed in to and the home tenant were added or updated"
            in caplog.text)

    for account in azure_auth.accounts():  # asked for explicitly: that tenant, active
        azure_auth.remove_account(account.key)
    azure_auth.add_account(FABRIKAM)
    assert azure_auth.active_account().tenant_id == FABRIKAM


def test_ensure_account_opens_one_sign_in_for_several_callers(cloud, marcus):
    cloud.interactive_delay = 0.2
    found = []
    threads = [threading.Thread(target=lambda: found.append(azure_auth.ensure_account()))
               for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    assert len(cloud.interactive) == 1
    assert found == [_by_tenant(CONTOSO)] * 2 and azure_auth.active_account() == found[0]
    assert azure_auth.ensure_account() == found[0] and len(cloud.interactive) == 1

    # A caller arriving during an "Add account" gets that account instead of a second browser
    for account in azure_auth.accounts():
        azure_auth.remove_account(account.key)
    adding = threading.Thread(target=lambda: azure_auth.add_account(FABRIKAM))
    adding.start()
    assert _eventually(lambda: len(cloud.interactive) == 2)
    assert azure_auth.ensure_account() == _by_tenant(FABRIKAM)
    adding.join(10)
    assert len(cloud.interactive) == 2


def test_ensure_account_fails_like_a_sign_in(cloud, marcus):
    cloud.interactive_result = {"error": "access_denied", "error_description": "User cancelled"}
    with pytest.raises(ApiError, match="^Sign-in failed: User cancelled$"):
        azure_auth.ensure_account()
    azure_auth.allow_browser_login(False)
    with pytest.raises(ApiError, match="AZURE_CLIENT_ID"):
        azure_auth.ensure_account()
    assert len(cloud.interactive) == 1


def test_one_read_of_the_account_list_per_answer(monkeypatch, cloud, marcus):
    azure_auth.add_account()
    azure_auth.use_account(_by_tenant(FABRIKAM).key)
    real_read = azure_auth._read_store
    reads = []

    def counted(wait=True):
        reads.append(1)
        return real_read(wait)

    monkeypatch.setattr(azure_auth, "_read_store", counted)
    entries, active = azure_auth.account_snapshot()
    assert len(reads) == 1 and active in entries and active == _by_tenant(FABRIKAM)
    for read in (azure_auth.accounts, azure_auth.active_account,
                 lambda: azure_auth._bound_account(NORTHWIND), lambda: azure_auth.find_account("2"),
                 real_get_credential):
        reads.clear()
        read()
        assert len(reads) == 1, read

    # Another process empties the file between two reads: an answer never mixes them
    northwind = _entry(marcus, NORTHWIND)
    versions = iter([real_read(), ([], None)])
    monkeypatch.setattr(azure_auth, "_read_store", lambda wait=True: next(versions))
    assert azure_auth._bound_account(NORTHWIND) == northwind


def test_two_threads_without_an_account_sign_in_once(cloud, marcus):
    cloud.interactive_delay = 0.2
    credentials = [real_get_credential(), real_get_credential()]
    tokens = []
    threads = [threading.Thread(target=lambda c=c: tokens.append(c.get_token(FABRIC_SCOPE)))
               for c in credentials]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    assert len(tokens) == 2 and len(cloud.interactive) == 1


def test_interaction_only_when_msal_asks_for_it(cloud, marcus):
    azure_auth.add_account()
    azure_auth.use_account(_by_tenant(FABRIKAM).key)
    mfa = {"error": "invalid_grant", "suberror": "basic_action",
           "error_description": "AADSTS50076: you must use multi-factor authentication\r\nTrace ID: 2"}
    cloud.silent_errors[FABRIKAM] = mfa
    credential = real_get_credential()
    assert _tenant_of(credential.get_token(FABRIC_SCOPE)) == FABRIKAM
    assert cloud.interactive[-1] == {"tenant": FABRIKAM, "prompt": None,
                                     "login_hint": "marcus@contoso.com",
                                     "timeout": azure_auth.LOGIN_TIMEOUT, "scopes": [FABRIC_SCOPE]}
    credential.get_token(FABRIC_SCOPE)  # MFA done: silent again
    assert len(cloud.interactive) == 2

    cloud.silent_errors[FABRIKAM] = {"error": "invalid_client",
                                     "error_description": "AADSTS7000218: bad request\r\nTrace ID: 3"}
    with pytest.raises(ApiError, match=r"^Sign-in failed: AADSTS7000218: bad request$"):
        credential.get_token(FABRIC_SCOPE)
    assert len(cloud.interactive) == 2  # no browser for an error a sign-in cannot fix

    cloud.silent_errors.clear()
    cloud.signed_in.clear()  # tokens gone from the cache: sign in again as that user
    assert _tenant_of(credential.get_token(FABRIC_SCOPE)) == FABRIKAM
    assert cloud.interactive[-1]["login_hint"] == "marcus@contoso.com"


def test_signing_in_as_someone_else_is_refused_and_leaves_no_tokens(cloud, marcus):
    azure_auth.add_account()
    anna = cloud.add_user("anna", NORTHWIND)
    cloud.silent_errors[CONTOSO] = dict(MFA)
    cloud.sign_in_as = anna  # another account picked in the browser: MSAL caches her tokens
    with pytest.raises(ApiError, match="signed in as anna@northwind.com, but the account in use "
                                       "is marcus@contoso.com"):
        real_get_credential().get_token(FABRIC_SCOPE)
    assert _token_records(anna) == [0, 0, 0, 0]  # she has no entry: her tokens are deleted
    assert all(_token_records(marcus)) and azure_auth.sign_in_in_progress() is False

    # Someone with an entry keeps their tokens
    cloud.sign_in_as, cloud.picker = None, anna
    azure_auth.add_account()
    azure_auth.use_account(_entry(marcus, CONTOSO).key)
    cloud.silent_errors[CONTOSO] = dict(MFA)
    cloud.sign_in_as = anna
    with pytest.raises(ApiError, match="signed in as anna@northwind.com"):
        real_get_credential().get_token(FABRIC_SCOPE)
    assert all(_token_records(anna)) and all(_token_records(marcus))


def test_embedded_never_opens_a_browser(monkeypatch, cloud, marcus):
    azure_auth.add_account()
    azure_auth.allow_browser_login(False)
    for call in (real_get_credential, real_default_credential, azure_auth.add_account):
        with pytest.raises(ApiError) as error:
            call()
        for name in ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET",
                     "AZURE_CLIENT_CERTIFICATE_PATH"):
            assert name in str(error.value)
    assert len(cloud.interactive) == 1

    for name, value in (("AZURE_CLIENT_ID", "x"), ("AZURE_CLIENT_SECRET", "y"),
                        ("AZURE_TENANT_ID", CONTOSO)):
        monkeypatch.setenv(name, value)
    from azure.identity import EnvironmentCredential

    assert isinstance(real_get_credential(), EnvironmentCredential)
    azure_auth.allow_browser_login(True)


# ---------------------------------------------------------------------------
# Token storage
# ---------------------------------------------------------------------------


def test_without_os_encryption_the_tokens_are_a_file_only_the_user_can_read(monkeypatch, tmp_path):
    path = tmp_path / "auth" / "msal_token_cache.bin"
    monkeypatch.setattr(azure_auth, "AUTH_DIR", path.parent)
    assert azure_auth.token_cache_encrypted() is False  # conftest: no keyring in tests
    assert path.is_file()
    persistence = azure_auth._persistence(path)
    persistence.save('{"AccessToken": {}}')
    assert persistence.load() == '{"AccessToken": {}}'
    if os.name == "posix":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        path.chmod(0o644)  # an older, readable file is tightened
        azure_auth._persistence(path)
        assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_os_encryption_is_used_when_it_works(monkeypatch, tmp_path):
    from msal_extensions import FilePersistence

    class Encrypted(FilePersistence):
        is_encrypted = True

    monkeypatch.setattr(azure_auth, "AUTH_DIR", tmp_path)
    monkeypatch.setattr(azure_auth, "_encrypted_persistence", lambda path: Encrypted(str(path)))
    assert azure_auth.token_cache_encrypted() is True


# ---------------------------------------------------------------------------
# pbixtractor account ...
# ---------------------------------------------------------------------------


def _run(capsys, *args):
    with pytest.raises(SystemExit) as exit_info:
        main(["account", *args])
    out = capsys.readouterr()
    return exit_info.value.code, out.out, out.err


def test_cli_account_commands(cloud, marcus, capsys):
    assert _run(capsys, "list") == (0, "No accounts - run `pbixtractor account add`\n", "")

    code, out, _ = _run(capsys, "add")
    assert code == 0
    assert "* 1. marcus@contoso.com · Contoso\n  2. marcus@contoso.com · Fabrikam\n" in out
    assert "Tokens stored unencrypted, readable only by your user" in out

    code, out, _ = _run(capsys, "use", "fabrikam")
    assert code == 0 and "Now using marcus@contoso.com · Fabrikam" in out
    assert "  1. marcus@contoso.com · Contoso\n* 2. marcus@contoso.com · Fabrikam\n" in out

    code, out, err = _run(capsys, "use", "marcus")
    assert code == 1 and err.startswith("ERROR: 'marcus' matches several accounts")
    code, out, err = _run(capsys, "remove", "9")
    assert code == 1 and err.startswith("ERROR: No account number 9")
    code, out, err = _run(capsys, "remove", "fabrik")  # part of a name: never removes
    assert code == 1 and err.startswith("ERROR: No account matches 'fabrik' exactly")
    assert len(azure_auth.accounts()) == 3

    code, out, _ = _run(capsys, "remove", "FABRIKAM")  # the whole tenant name, any case
    assert code == 0 and "Removed marcus@contoso.com · Fabrikam" in out
    assert "* 1. marcus@contoso.com · Contoso\n  2. marcus@contoso.com · Northwind\n" in out

    code, out, _ = _run(capsys, "add", "--tenant", FABRIKAM)
    assert code == 0 and "* 2. marcus@contoso.com · Fabrikam" in out
    assert cloud.interactive[-1]["tenant"] == FABRIKAM


def test_cli_account_add_prints_a_listing_problem_once(cloud, marcus, capsys, caplog):
    cloud.listing_fails = True
    code, out, err = _run(capsys, "add")
    assert code == 0
    assert err.startswith("WARNING: Could not list the tenants of marcus@contoso.com (HTTP 403")
    assert err.count("Could not list") == 1
    assert "Could not list" not in caplog.text  # printed instead of logged a second time
    assert f"* 1. marcus@contoso.com · {CONTOSO}\n" in out

    cloud.listing_fails = False
    code, out, err = _run(capsys, "add")
    assert code == 0 and err == "" and "  3. marcus@contoso.com · Northwind\n" in out


def test_cli_account_remove_says_when_the_tokens_stay(monkeypatch, cloud, marcus, capsys):
    azure_auth.add_account(NORTHWIND)
    for account in azure_auth.accounts()[:2]:
        azure_auth.remove_account(account.key)

    def locked(record):
        raise OSError("the token cache is locked")

    monkeypatch.setattr(azure_auth._token_cache()[0], "remove_rt", locked)
    code, _, err = _run(capsys, "remove", "1")
    assert code == 1
    assert err.startswith("ERROR: Removed marcus@contoso.com · Northwind, but could not delete "
                          "the tokens of marcus@contoso.com")
    assert azure_auth.accounts() == []


def test_cli_account_add_failure_and_service_principal_note(monkeypatch, cloud, marcus, capsys):
    cloud.interactive_result = {"error": "access_denied", "error_description": "User cancelled"}
    code, _, err = _run(capsys, "add")
    assert code == 1 and err == "ERROR: Sign-in failed: User cancelled\n"

    for name, value in (("AZURE_CLIENT_ID", "x"), ("AZURE_CLIENT_SECRET", "y"),
                        ("AZURE_TENANT_ID", CONTOSO)):
        monkeypatch.setenv(name, value)
    code, out, _ = _run(capsys, "list")
    assert code == 0 and "service principal from the environment" in out
