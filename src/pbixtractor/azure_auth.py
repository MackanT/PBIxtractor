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
import email.utils
import json
import os
import shutil
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Callable, Optional

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
DEVOPS_SCOPE = "499b84ac-1321-427f-aa17-267ca6975798/.default"  # Azure DevOps resource id
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
AUTH_RECORD = Path.home() / ".pbixtractor" / "auth_record.json"


class ApiError(RuntimeError):
    """An API call failed (the message includes the API's error code and text)."""


# ============================================================================
# Sign-in
# ============================================================================


def service_principal_configured() -> bool:
    """A service principal is set in the environment (the variables azure-identity reads)."""
    has_id = os.environ.get("AZURE_CLIENT_ID") and os.environ.get("AZURE_TENANT_ID")
    has_proof = os.environ.get("AZURE_CLIENT_SECRET") or os.environ.get(
        "AZURE_CLIENT_CERTIFICATE_PATH"
    )
    return bool(has_id and has_proof)


def default_credential(tenant_id: Optional[str] = None):
    """
    A token credential:

    - a service principal from the environment (AZURE_CLIENT_ID + AZURE_TENANT_ID +
      AZURE_CLIENT_SECRET or AZURE_CLIENT_CERTIFICATE_PATH): for servers and containers, where
      no one can complete a browser login - then never a browser;
    - else the Azure CLI login if `az` is available, otherwise an interactive browser login that
      is remembered (OS-encrypted token cache + account record).

    Args:
        tenant_id: Entra tenant (default: the account's home tenant; a service principal
            uses AZURE_TENANT_ID)
    """
    from azure.identity import (
        AuthenticationRecord,
        AzureCliCredential,
        ChainedTokenCredential,
        EnvironmentCredential,
        InteractiveBrowserCredential,
        TokenCachePersistenceOptions,
    )

    if service_principal_configured():
        return EnvironmentCredential()

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
_credentials_lock = threading.Lock()  # two web UI tabs must not start two browser logins


def get_credential(tenant_id: Optional[str] = None):
    """The shared credential for a tenant (created once per process)."""
    with _credentials_lock:
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
