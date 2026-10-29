"""Test-wide safety: tests never touch the developer's state or real Microsoft cloud APIs."""

import os
import socket
import tempfile

# NiceGUI fixes its storage folder when first imported, and user_simulation's reset deletes
# it: point it at a throwaway folder before any test module imports nicegui, so the tests
# never wipe the web UI's remembered choices in <repo>/.nicegui.
os.environ["NICEGUI_STORAGE_PATH"] = tempfile.mkdtemp(prefix="pbixtractor_nicegui_")

import msal  # noqa: E402
import pytest  # noqa: E402

from pbixtractor import azure_auth, bpa_custom  # noqa: E402

_real_create_connection = socket.create_connection


def _local_only_connection(address, *args, **kwargs):
    """Every HTTP client connects through here: only local test servers are allowed."""
    host = address[0]
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise RuntimeError(f"Network access is disabled in tests: {host}")
    return _real_create_connection(address, *args, **kwargs)


@pytest.fixture(autouse=True)
def no_real_sign_in(monkeypatch, tmp_path_factory):
    """No real credential, no personal access token and no network beyond 127.0.0.1. The
    account list, token cache and custom rule file live in a temp folder (never ~/.pbixtractor
    or the OS keyring), and no MSAL app or browser sign-in can start (tests pass fake MSAL apps)."""

    def refuse(*args, **kwargs):
        raise azure_auth.ApiError("Real sign-in is disabled in tests")

    def no_keyring(path):
        raise RuntimeError("OS-encrypted token storage is disabled in tests")

    monkeypatch.setattr(azure_auth, "get_credential", refuse)
    monkeypatch.setattr(azure_auth, "default_credential", refuse)
    auth_dir = tmp_path_factory.mktemp("pbixtractor_auth")
    monkeypatch.setattr(azure_auth, "AUTH_DIR", auth_dir)
    # The user's custom model quality rules: never the real ~/.pbixtractor/bpa_rules.yaml
    monkeypatch.setattr(bpa_custom, "USER_RULES_PATH", auth_dir / "bpa_rules.yaml")
    monkeypatch.setattr(azure_auth, "_new_app", refuse)
    monkeypatch.setattr(azure_auth, "_encrypted_persistence", no_keyring)
    monkeypatch.setattr(azure_auth, "_browser_login_allowed", True)
    monkeypatch.setattr(msal.PublicClientApplication, "acquire_token_interactive", refuse)
    for name in ("AZURE_DEVOPS_PAT", "AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_CLIENT_SECRET",
                 "AZURE_CLIENT_CERTIFICATE_PATH"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(socket, "create_connection", _local_only_connection)
