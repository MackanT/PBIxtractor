"""Tests for the NiceGUI web UI (web_ui.py), using NiceGUI's simulated user (no browser)."""

import asyncio
import json
from pathlib import Path

import pytest

pytest.importorskip("nicegui")

from fastapi import HTTPException  # noqa: E402
from nicegui import ui  # noqa: E402
from nicegui.testing.user_simulation import user_simulation  # noqa: E402

import pbixtractor.web_sources as web_sources  # noqa: E402
import pbixtractor.web_ui as web_ui  # noqa: E402

from .sample_layout import write_sample_pbix  # noqa: E402
from .test_semantic_model import BIM  # noqa: E402


@pytest.fixture
def sample(tmp_path, monkeypatch):
    """Sample.pbix + Sample.bim; no Tabular Editor and no Power BI Desktop."""
    write_sample_pbix(tmp_path / "Sample.pbix")
    (tmp_path / "Sample.bim").write_text(json.dumps(BIM), encoding="utf-8")
    monkeypatch.setattr(web_ui, "find_tabular_editor", lambda: None)
    monkeypatch.setattr(web_ui, "find_local_instances", lambda: [])
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _simulate(scenario) -> None:
    async def run() -> None:
        async with user_simulation(web_ui.index) as user:
            web_ui.register_routes()
            await scenario(user)

    asyncio.run(run())


def test_page_renders(sample):
    async def scenario(user):
        await user.open("/")
        await user.should_see("Create documentation")
        await user.should_see("Tabular Editor 2 not found", retries=1)

    _simulate(scenario)


def test_report_path_fills_model_and_output(sample):
    async def scenario(user):
        await user.open("/")
        user.find(marker="report").type(str(sample / "Sample.pbix"))
        model = next(iter(user.find(marker="model").elements))
        output = next(iter(user.find(marker="output").elements))
        assert model.value == str(sample / "Sample.bim")
        assert output.value == str(sample / "output" / "Sample")

    _simulate(scenario)


def test_changing_the_report_replaces_the_found_model_but_not_a_chosen_one(sample):
    other = sample / "other"
    other.mkdir()
    (other / "Other.pbix").write_bytes((sample / "Sample.pbix").read_bytes())
    (other / "Other.bim").write_text((sample / "Sample.bim").read_text(encoding="utf-8"), encoding="utf-8")

    async def scenario(user):
        await user.open("/")
        report = next(iter(user.find(marker="report").elements))
        model = next(iter(user.find(marker="model").elements))
        report.value = str(sample / "Sample.pbix")
        assert model.value == str(sample / "Sample.bim")
        report.value = str(other / "Other.pbix")  # the audit found Sample.bim kept here
        assert model.value == str(other / "Other.bim")
        model.value = str(sample / "Sample.bim")  # chosen by the user: kept
        report.value = str(sample / "Sample.pbix")
        assert model.value == str(sample / "Sample.bim")

    _simulate(scenario)


def test_run_shows_results_and_serves_lineage(sample):
    async def scenario(user):
        await user.open("/")
        user.find(marker="report").type(str(sample / "Sample.pbix"))
        user.find("Create documentation").click()
        for _ in range(300):  # the extraction runs in a background thread
            if user._sees(None, ui.tabs, None, None) or user._sees("Failed", None, None, None):
                break
            await asyncio.sleep(0.1)
        await user.should_see("Lineage")
        await user.should_see("Unused measures")
        assert (sample / "output" / "Sample" / "Sample.xlsx").is_file()

        # The lineage iframe is served through the /files route
        token, folder = next(iter(web_ui._OUTPUT_DIRS.items()))
        assert folder == sample / "output" / "Sample"
        response = await user.http_client.get(f"/files/{token}/Sample_lineage.html")
        assert response.status_code == 200 and "<svg" in response.text.lower()
        # Only files inside the run's folder
        assert (await user.http_client.get("/files/unknown/Sample.xlsx")).status_code == 404
        assert (await user.http_client.get(f"/files/{token}/nope.txt")).status_code == 404
        with pytest.raises(HTTPException):
            web_ui._serve_output_file(token, "../Sample.pbix")

    _simulate(scenario)


def test_catalog_route_serves_only_catalog_files(tmp_path, monkeypatch):
    catalog = tmp_path / "catalog"
    (catalog / "models" / "k").mkdir(parents=True)
    (catalog / "catalog.html").write_text("<html></html>")
    (catalog / "models" / "k" / "x_lineage.html").write_text("<html></html>")
    (catalog / "secret.txt").write_text("not for the browser")
    (catalog / "notes").mkdir()
    (catalog / "notes" / "id_rsa").write_text("key")
    monkeypatch.setitem(web_ui._CATALOG, "dir", catalog)

    page = web_ui._serve_catalog_file("catalog.html")
    assert "sandbox" in page.headers["content-security-policy"]  # runs in an opaque origin
    assert web_ui._serve_catalog_file("models/k/x_lineage.html").status_code == 200
    for path in ("secret.txt", "notes/id_rsa", "models/../secret.txt", "../catalog/secret.txt"):
        with pytest.raises(HTTPException):
            web_ui._serve_catalog_file(path)


def test_unknown_hosts_are_refused():
    """DNS rebinding: a page on another host name pointing at 127.0.0.1 gets nothing."""
    import httpx
    from fastapi import FastAPI

    server = FastAPI()
    server.get("/")(lambda: {"ok": True})
    original, web_ui.app = web_ui.app, server
    try:
        web_ui.add_host_check(8081)
    finally:
        web_ui.app = original

    async def status(host: str) -> int:
        transport = httpx.ASGITransport(app=server)
        async with httpx.AsyncClient(transport=transport, base_url="http://x") as client:
            return (await client.get("/", headers={"host": host})).status_code

    assert asyncio.run(status("127.0.0.1:8081")) == 200
    assert asyncio.run(status("localhost:8081")) == 200
    assert asyncio.run(status("evil.example:8081")) == 421


def test_tabular_editor_folder_can_be_set(sample):
    exe = sample / "TE2" / "TabularEditor.exe"
    exe.parent.mkdir()
    exe.write_bytes(b"")

    async def scenario(user):
        await user.open("/")
        user.find(marker="te_folder").type(str(sample / "wrong"))
        user.find("Save").click()
        await user.should_see("No TabularEditor.exe in that folder.")

        user.find(marker="te_folder").clear().type(str(exe.parent))
        user.find("Save").click()
        locations = sample / "Input" / "TabularEditorLocations.txt"
        assert locations.read_text().splitlines() == [str(exe.parent)]

    _simulate(scenario)


def test_missing_report_warns(sample):
    async def scenario(user):
        await user.open("/")
        user.find(marker="report").type(str(sample / "Nope.pbix"))
        user.find("Create documentation").click()
        await user.should_see("Choose an existing report first.")

    _simulate(scenario)


def test_dropped_files_are_copied_and_filled_in(sample):
    report_bytes = (sample / "Sample.pbix").read_bytes()
    model_bytes = (sample / "Sample.bim").read_bytes()

    async def scenario(user):
        await user.open("/")
        upload = user.find(marker="upload").elements.pop()
        # The model and the report of one drop may arrive in any order
        await upload.handle_uploads([ui.upload.SmallFileUpload("Dropped.bim", "", model_bytes)])
        await upload.handle_uploads([ui.upload.SmallFileUpload("Dropped.pbix", "", report_bytes)])
        uploads = sample / "output" / "_uploads"
        await _wait_for(lambda: _value(user, "model") == str(uploads / "Dropped.bim"))
        assert (uploads / "Dropped.pbix").read_bytes() == report_bytes
        assert user.find(marker="report").elements.pop().value == str(uploads / "Dropped.pbix")
        assert user.find(marker="model").elements.pop().value == str(uploads / "Dropped.bim")
        assert user.find(marker="output").elements.pop().value.endswith("Dropped")

        await upload.handle_uploads([ui.upload.SmallFileUpload("notes.txt", "", b"x")])
        await user.should_see("drop a .pbix report or a .bim model")

    _simulate(scenario)


def _element(user, marker: str):
    return next(iter(user.find(marker=marker).elements))


def _value(user, marker: str):
    return _element(user, marker).value


def _options(user, marker: str) -> dict:
    return _element(user, marker).options


async def _wait_for(condition, timeout: float = 10.0) -> None:
    """Poll until an (async) handler has done its work - no fixed sleeps that are too short
    on a slow machine."""
    for _ in range(int(timeout / 0.05)):
        if condition():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")


async def _choose(user, marker: str, value, until=None) -> None:
    """Set a select/input like a user would; `until`: what its handler does (waited for)."""
    _element(user, marker).value = value
    await _wait_for(until or (lambda: True))


def test_document_a_report_from_devops(sample, monkeypatch):
    from .test_devops import FakeDevOps, _repo_files

    client = FakeDevOps(_repo_files(sample))
    monkeypatch.setattr(web_sources, "make_devops_client", lambda org: client)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "devops")
        await _choose(user, "devops_org", "contoso")
        user.find(marker="devops_load").click()
        await _wait_for(lambda: _options(user, "devops_project"))
        await _choose(user, "devops_project", "BI Team", until=lambda: _options(user, "devops_repo"))
        # the repository's default branch is preselected, and its reports listed
        await _choose(user, "devops_repo", "reports", until=lambda: _value(user, "devops_branch"))
        assert _value(user, "devops_branch") == "main"
        await _wait_for(lambda: _options(user, "devops_report"))
        await _choose(
            user, "devops_report", "/Reports/Sample.Report",
            until=lambda: len(_options(user, "devops_version")) > 1,
        )
        assert "a1b2c3d4" in list(_options(user, "devops_version").values())[1]  # commit history
        await _choose(user, "devops_version", "a1b2c3d4e5f6")
        assert user.find(marker="output").elements.pop().value.endswith("Sample")

        user.find("Create documentation").click()
        await user.should_see("Lineage", retries=100)
        assert (sample / "output" / "Sample" / "Sample.xlsx").is_file()
        zips = [q for p, q in client.calls if q.get("$format") == "zip"]
        assert zips[0]["versionDescriptor.version"] == "a1b2c3d4e5f6"

    _simulate(scenario)


def test_document_a_report_from_fabric(sample, monkeypatch):
    from .test_fabric import REPORT_ID, WS_SALES, FakeFabric, _report_parts

    client = FakeFabric(
        _report_parts(sample / "a", "Sales WS"),
        other_report_parts=_report_parts(sample / "b", "Sales WS"),
    )
    monkeypatch.setattr(web_sources, "make_fabric_client", lambda: client)
    monkeypatch.setattr("pbixtractor.service_stats.make_client", lambda: client)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        user.find("Create documentation").click()
        await user.should_see("Choose a report first.")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: _options(user, "fabric_workspace"))
        await _choose(user, "fabric_workspace", WS_SALES, until=lambda: _options(user, "fabric_report"))
        await _choose(user, "fabric_report", REPORT_ID)
        await _choose(user, "fabric_all_reports", True)  # model mode
        user.find("Create documentation").click()
        await user.should_see("Lineage", retries=100)
        # Named after the model; both reports' pages; statistics from the service
        document = json.loads(
            (sample / "output" / "Sample Model" / "Sample Model.json").read_text(encoding="utf-8")
        )
        pages = [page["name"] for page in document["report"]["pages"]]
        assert "Sample Detail › Sales" in pages
        sales = next(t for t in document["model"]["tables"] if t["name"] == "Sales")
        assert sales["rows"] == 1000

    _simulate(scenario)


def test_browser_opens_only_without_a_reconnecting_tab(monkeypatch):
    opened = []
    monkeypatch.setattr(web_ui, "BROWSER_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(web_ui.webbrowser, "open", opened.append)

    async def scenario() -> None:
        reconnected = asyncio.Event()
        reconnected.set()
        await web_ui._open_browser_unless_reconnected("http://x/", reconnected)
        assert opened == []
        await web_ui._open_browser_unless_reconnected("http://x/", asyncio.Event())
        assert opened == ["http://x/"]

    asyncio.run(scenario())


def test_tests_never_use_the_real_nicegui_storage():
    """user_simulation deletes NiceGUI's storage folder; conftest points it at a temp folder so
    running the tests does not wipe the web UI's remembered choices in the repo."""
    from nicegui.storage import Storage

    repo = Path(__file__).resolve().parent.parent
    assert repo not in Storage.path.parents and Storage.path != repo / ".nicegui"


def test_log_count_counts_messages_not_lines():
    logs = "WARNING: first\nWARNING: Tabular Editor failed:\n  details\n  more\nERROR: last\n"
    assert web_ui._log_count(logs) == 3
    assert web_ui._log_count("") == 0


def test_bookmark_filter_text():
    captured = {"captures": ["Data", "Display"], "filters": [{"text": "Sales: T[C] = 1"},
                                                             {"text": "All pages: T[D] = 2 (changed)"}]}
    assert web_ui._bookmark_filter_text(captured) == "Sales: T[C] = 1\nAll pages: T[D] = 2 (changed)"
    assert web_ui._bookmark_filter_text({"captures": ["Display"], "filters": []}) == (
        "(does not capture data)"
    )


def test_theme_fonts_are_served_and_the_band_follows_the_accent(sample):
    from pbixtractor import theme

    async def scenario(user):
        await user.open("/")
        await user.should_see("Document a report")
        for name in ("familjen-grotesk-latin.woff2", "source-sans-3-latin.woff2"):
            response = await user.http_client.get(f"{theme.FONTS_URL}/{name}")
            assert response.status_code == 200 and response.content[:4] == b"wOF2"

    _simulate(scenario)
    # A white-label accent re-derives the header/rail ground; nothing hard-codes the default
    assert theme.darken("#3f7d5a", 0.55) in theme.theme_css()
    assert theme.darken("#aa3366", 0.55) in theme.theme_css("#aa3366")
    assert "#3f7d5a" not in theme.theme_css("#aa3366").replace(theme.OK_HEX, "")
