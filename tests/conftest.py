"""Test-wide safety: tests never touch the developer's state or real Microsoft cloud APIs."""

import os
import socket
import tempfile

# NiceGUI fixes its storage folder when first imported, and user_simulation's reset deletes
# it: point it at a throwaway folder before any test module imports nicegui, so the tests
# never wipe the web UI's remembered choices in <repo>/.nicegui.
os.environ["NICEGUI_STORAGE_PATH"] = tempfile.mkdtemp(prefix="pbixtractor_nicegui_")

import pytest  # noqa: E402

from pbixtractor import azure_auth  # noqa: E402

_real_create_connection = socket.create_connection


def _local_only_connection(address, *args, **kwargs):
    """Every HTTP client connects through here: only local test servers are allowed."""
    host = address[0]
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise RuntimeError(f"Network access is disabled in tests: {host}")
    return _real_create_connection(address, *args, **kwargs)


@pytest.fixture(autouse=True)
def no_real_sign_in(monkeypatch):
    """No real credential, no personal access token and no network beyond 127.0.0.1."""

    def refuse(tenant_id=None):
        raise azure_auth.ApiError("Real sign-in is disabled in tests")

    monkeypatch.setattr(azure_auth, "get_credential", refuse)
    monkeypatch.setattr(azure_auth, "default_credential", refuse)
    monkeypatch.delenv("AZURE_DEVOPS_PAT", raising=False)
    monkeypatch.setattr(socket, "create_connection", _local_only_connection)
