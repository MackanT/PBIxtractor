"""Sign-in and a small REST helper for Microsoft cloud APIs (Fabric, Azure DevOps).

    credential = get_credential()            # one sign-in for every API, remembered
    client = RestClient("https://api.fabric.microsoft.com/v1", FABRIC_SCOPE, credential)
    status, headers, body = client.request("GET", "workspaces")

Sign-in: the Azure CLI's login when `az` is installed and logged in, otherwise an interactive
browser login. The token cache is encrypted by the OS (Windows DPAPI); only the non-secret
account record is kept in ~/.pbixtractor/. One login gives tokens for every API (the refresh
token is multi-resource), so Fabric and DevOps do not ask twice.
"""

import base64
import json
import shutil
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Optional

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
DEVOPS_SCOPE = "499b84ac-1321-427f-aa17-267ca6975798/.default"  # Azure DevOps resource id
AUTH_RECORD = Path.home() / ".pbixtractor" / "auth_record.json"


class ApiError(RuntimeError):
    """An API call failed (the message includes the API's error code and text)."""


# ============================================================================
# Sign-in
# ============================================================================


def default_credential(tenant_id: Optional[str] = None):
    """
    A token credential: Azure CLI login if `az` is available, otherwise an interactive browser
    login that is remembered (OS-encrypted token cache + account record).

    Args:
        tenant_id: Entra tenant (default: the account's home tenant)
    """
    from azure.identity import (
        AuthenticationRecord,
        AzureCliCredential,
        ChainedTokenCredential,
        InteractiveBrowserCredential,
        TokenCachePersistenceOptions,
    )

    record = None
    if AUTH_RECORD.is_file():
        try:
            record = AuthenticationRecord.deserialize(AUTH_RECORD.read_text(encoding="utf-8"))
        except (ValueError, KeyError):
            record = None
    browser = _RememberingCredential(
        InteractiveBrowserCredential(
            tenant_id=tenant_id,
            cache_persistence_options=TokenCachePersistenceOptions(name="pbixtractor"),
            authentication_record=record,
        ),
        needs_login=record is None,
    )
    if shutil.which("az"):
        return ChainedTokenCredential(AzureCliCredential(tenant_id=tenant_id), browser)
    return browser


class _RememberingCredential:
    """Browser credential that saves the (non-secret) account record after the first login."""

    def __init__(self, credential, needs_login: bool):
        self._credential = credential
        self._needs_login = needs_login
        self._lock = threading.Lock()  # the web UI may ask for tokens from several threads

    def get_token(self, *scopes, **kwargs):
        with self._lock:
            if self._needs_login:
                record = self._credential.authenticate(scopes=list(scopes))
                AUTH_RECORD.parent.mkdir(parents=True, exist_ok=True)
                AUTH_RECORD.write_text(record.serialize(), encoding="utf-8")
                self._needs_login = False
        return self._credential.get_token(*scopes, **kwargs)


_credentials: dict[Optional[str], object] = {}


def get_credential(tenant_id: Optional[str] = None):
    """The shared credential for a tenant (created once per process)."""
    if tenant_id not in _credentials:
        _credentials[tenant_id] = default_credential(tenant_id)
    return _credentials[tenant_id]


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
            self._token = self._credential.get_token(self._scope)
        return f"Bearer {self._token.token}"

    def request(self, method: str, url: str, body: Optional[dict] = None, raw: bool = False):
        """
        One API call.

        Args:
            method: HTTP method
            url: Absolute URL, or a path relative to the API root
            body: JSON body
            raw: Return the body as bytes instead of parsed JSON

        Returns:
            (status, headers, body) - body is parsed JSON (None if empty) or bytes with raw
        """
        if not url.startswith("http"):
            url = f"{self._api}/{url.lstrip('/')}"
        data = json.dumps(body).encode() if body is not None else None
        for attempt in range(5):
            headers = {"Authorization": self._auth_header(), "Content-Type": "application/json"}
            req = urllib.request.Request(url, data=data, method=method, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=300) as response:
                    content = response.read()
                    response_headers = dict(response.headers)
                    if raw:
                        return response.status, response_headers, content
                    if content and "json" not in response.headers.get("Content-Type", ""):
                        # e.g. Azure DevOps answers a failed PAT login with an HTML page (203)
                        raise ApiError(
                            f"Unexpected non-JSON answer from {url} (HTTP {response.status}) - "
                            "check the sign-in / access token."
                        )
                    return response.status, response_headers, (json.loads(content) if content else None)
            except urllib.error.HTTPError as error:
                content = error.read()
                if error.code == 429 and attempt < 4:
                    self._sleep(float(error.headers.get("Retry-After") or 10))
                    continue
                raise ApiError(error_text(error.code, url, content)) from None
            except urllib.error.URLError as error:
                raise ApiError(f"Cannot reach {url}: {error.reason}") from None
        raise ApiError(f"Still throttled after 5 attempts: {url}")


def error_text(code: int, url: str, content: bytes) -> str:
    """Readable error message from an HTTP error response (Fabric and DevOps formats)."""
    try:
        body = json.loads(content)
        detail = f"{body.get('errorCode') or body.get('typeKey', '')}: {body.get('message', '')}"
        detail = detail.strip(": ")
    except (ValueError, AttributeError):
        detail = content.decode("utf-8", errors="replace")[:300]
    hint = ""
    if code in (401, 403):
        hint = " - check that you have access (Fabric getDefinition needs Contributor or higher)."
    return f"HTTP {code} for {url}: {detail}{hint}"
