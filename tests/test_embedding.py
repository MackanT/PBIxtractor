"""PBIxtractor embedded in another NiceGUI app (web_ui.register + build_page), the way the
data-platform web UI uses it: a host with its own header, theme and storage keys."""

import asyncio
import json

import pytest

pytest.importorskip("nicegui")

from nicegui import app, ui  # noqa: E402
from nicegui.testing.user_simulation import user_simulation  # noqa: E402

import pbixtractor.web_config as web_config  # noqa: E402
import pbixtractor.web_ui as web_ui  # noqa: E402

from .sample_layout import write_sample_pbix  # noqa: E402
from .test_semantic_model import BIM  # noqa: E402
from .test_web_ui import _element, _wait_for  # noqa: E402


@pytest.fixture
def host(tmp_path, monkeypatch):
    """A host app page: its own header, PBIxtractor's page inside it."""
    write_sample_pbix(tmp_path / "Sample.pbix")
    (tmp_path / "Sample.bim").write_text(json.dumps(BIM), encoding="utf-8")
    monkeypatch.setattr(web_ui, "find_tabular_editor", lambda: None)
    monkeypatch.setattr(web_ui, "find_local_instances", lambda: pytest.fail("scanned the server"))
    monkeypatch.chdir(tmp_path)
    # Module-level registries of served folders: keep this test's runs out of the others
    monkeypatch.setattr(web_ui, "_OUTPUT_DIRS", {})
    monkeypatch.setitem(web_ui._CATALOG, "dir", None)
    yield tmp_path
    web_config.configure()  # back to stand-alone for the other tests


def _any(user, marker: str):
    """An element by marker, also when hidden (user.find only sees visible ones)."""
    return next(e for e in user.client.elements.values() if marker in e._markers)


def _host_root() -> None:
    with ui.header():
        ui.label("Host app").mark("host_header")
    with ui.column().classes("w-full"):
        web_ui.build_page()


def _simulate(tmp_path, scenario) -> None:
    async def run() -> None:
        async with user_simulation(_host_root) as user:
            web_ui.register(prefix="/pbx", output_root=tmp_path / "out", storage_prefix="pbx.")
            await scenario(user)

    asyncio.run(run())


def test_embedded_page_has_no_shell_and_no_server_machine_parts(host):
    async def scenario(user):
        await user.open("/")
        await user.should_see("Create documentation")
        await user.should_see(marker="host_header")
        await user.should_not_see(marker="page-title")  # the host draws its own section header
        # No shell of our own: the host has the header / navigation
        for marker in ("nav_document", "nav_expand", "dark_toggle"):
            await user.should_not_see(marker=marker)
        # Nothing that acts on the server's machine
        assert not _any(user, "report_row").visible and not _any(user, "model_row").visible
        assert not _any(user, "output").visible
        await user.should_not_see(marker="te_folder")
        await user.should_see("Upload")
        routes = {getattr(r, "path", "") for r in app.routes}
        assert "/pbx/files/{token}/{name}" in routes and "/files/{token}/{name}" not in routes
        assert "/pbixtractor-fonts" not in routes  # the host serves its own fonts

    _simulate(host, scenario)


def test_embedded_run_uses_the_prefix_output_root_and_storage_prefix(host):
    report_bytes = (host / "Sample.pbix").read_bytes()
    model_bytes = (host / "Sample.bim").read_bytes()
    out = host / "out"

    async def scenario(user):
        await user.open("/")
        upload = _element(user, "upload")
        await upload.handle_uploads([ui.upload.SmallFileUpload("Dropped.bim", "", model_bytes)])
        await upload.handle_uploads([ui.upload.SmallFileUpload("Dropped.pbix", "", report_bytes)])
        await _wait_for(lambda: _any(user, "model").value == str(out / "_uploads" / "Dropped.bim"))
        _element(user, "catalog_switch").value = True
        user.find("Create documentation").click()
        await user.should_see("Lineage", retries=100)

        assert (out / "Dropped" / "Dropped.xlsx").is_file()  # below output_root
        assert (out / "_catalog" / "catalog.html").is_file()
        token = next(t for t, folder in web_ui._OUTPUT_DIRS.items() if folder == out / "Dropped")
        response = await user.http_client.get(f"/pbx/files/{token}/Dropped_lineage.html")
        assert response.status_code == 200 and "<svg" in response.text.lower()
        assert (await user.http_client.get("/pbx/catalog/catalog.html")).status_code == 200
        await user.should_see(marker="page_catalog")  # no rail: the page links the catalog

        # PBIxtractor's choices sit under its prefix, next to the host's own keys
        assert app.storage.general.get("pbx.catalog_enabled") is True
        assert "catalog_enabled" not in app.storage.general
        assert "pbx.catalog_dir" not in app.storage.general  # a server path is never kept

    _simulate(host, scenario)


def test_prefixed_choices_only_see_their_own_keys():
    store = {"host.theme": "dark", "pbx.devops_org": "contoso"}
    choices = web_config._PrefixedChoices(store, "pbx.")
    assert dict(choices) == {"devops_org": "contoso"}
    choices["fabric_workspace"] = "ws1"
    assert store["pbx.fabric_workspace"] == "ws1" and "fabric_workspace" not in store
    assert choices.get("theme") is None


def test_embedded_page_in_a_host_that_switches_sections_client_side(host):
    """data-platform renders sections with ui.sub_pages: the page is built after the first load."""

    def overview() -> None:
        ui.label("Host overview")

    def power_bi() -> None:
        web_ui.build_page()

    def shell() -> None:
        with ui.header():
            ui.button("Power BI", on_click=lambda: ui.navigate.to("/powerbi")).mark("to_powerbi")
        ui.sub_pages({"/": overview, "/powerbi": power_bi})

    async def run() -> None:
        async with user_simulation() as user:
            ui.page("/")(shell)
            ui.page("/powerbi")(shell)
            web_ui.register(prefix="/pbx", output_root=host / "out", storage_prefix="pbx.")
            await user.open("/")
            await user.should_see("Host overview")
            user.find(marker="to_powerbi").click()
            await user.should_see("Create documentation")
            await user.open("/powerbi")  # a deep link / refresh lands on the section too
            await user.should_see("Create documentation")

    asyncio.run(run())


def test_the_file_routes_follow_the_hosts_access_check(host, tmp_path):
    from fastapi import HTTPException

    catalog = tmp_path / "cat"
    catalog.mkdir()
    (catalog / "catalog.html").write_text("<html></html>")
    web_ui._CATALOG["dir"] = catalog
    allowed = {"value": False}
    web_config.configure(embedded=True, access=lambda: allowed["value"])
    for serve in (lambda: web_ui._serve_catalog_file("catalog.html"),
                  lambda: web_ui._serve_output_file("any", "x.html")):
        with pytest.raises(HTTPException) as refused:
            serve()
        assert refused.value.status_code == 403
    allowed["value"] = True
    assert web_ui._serve_catalog_file("catalog.html").status_code == 200

    def broken():
        raise RuntimeError("no session")

    web_config.configure(embedded=True, access=broken)  # fails closed
    with pytest.raises(HTTPException):
        web_ui._serve_catalog_file("catalog.html")
