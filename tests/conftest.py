"""Test-wide safety: tests never sign in to or call real Microsoft cloud APIs."""

import pytest

from pbixtractor import azure_auth


@pytest.fixture(autouse=True)
def no_real_sign_in(monkeypatch):
    """Any attempt to get a real credential fails (tests pass fake clients or credentials)."""

    def refuse(tenant_id=None):
        raise azure_auth.ApiError("Real sign-in is disabled in tests")

    monkeypatch.setattr(azure_auth, "get_credential", refuse)
    monkeypatch.setattr(azure_auth, "default_credential", refuse)
