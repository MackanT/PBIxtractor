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
    # The catalog in use is module state: another test's catalog must not show up (its rail
    # entries would, e.g. "Lineage across models" - and "Lineage" is what tests wait for)
    monkeypatch.setitem(web_ui._CATALOG, "dir", None)
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
        # The sample model has an RLS role: the page says the model is protected
        await user.should_see(marker="rls_notice")
        await user.should_see("RLS roles")
        await user.should_see("Security")

        # The lineage iframe is served through the /files route
        token = next(t for t, f in web_ui._OUTPUT_DIRS.items() if f == sample / "output" / "Sample")
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
    (catalog / "lineage.html").write_text("<html></html>")  # the lineage across models
    assert web_ui._serve_catalog_file("lineage.html").status_code == 200
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


def _any_element(user, marker: str):
    """An element by marker, also while it is hidden (user.find only sees visible ones)."""
    return next(e for e in list(user.client.elements.values()) if marker in e._markers)


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


def _drop(upload, *files):
    """Drop files one by one, as a browser hands them over."""
    async def run():
        for name, content in files:
            await upload.handle_uploads([ui.upload.SmallFileUpload(name, "", content)])
    return run()


def test_dropped_reports_on_one_model_are_documented_together(sample):
    report, model = (sample / "Sample.pbix").read_bytes(), (sample / "Sample.bim").read_bytes()
    uploads = sample / "output" / "_uploads"

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"),
                    ("First.pbix", report), ("Second.pbix", report), ("Shared.bim", model))
        await _wait_for(lambda: _value(user, "model") == str(uploads / "Shared.bim"))
        assert _value(user, "report") == str(uploads / "First.pbix")
        await user.should_see(marker="together")
        user.find("Create documentation").click()
        await user.should_see("Lineage", retries=100)
        # One documentation, named after the model, with both reports
        doc = json.loads((sample / "output" / "Shared" / "Shared.json").read_text(encoding="utf-8"))
        assert [r["name"] for r in doc["reports"]] == ["First", "Second"]
        await user.should_see("Reports")  # the stat tile, shown for several reports

    _simulate(scenario)


async def _batch_done(user) -> None:
    """Wait for a batch to finish (its summary appears)."""
    await _wait_for(lambda: user._sees(None, None, "batch_summary", None), timeout=60)


def test_dropped_reports_on_different_models_are_documented_separately(sample):
    report, model = (sample / "Sample.pbix").read_bytes(), (sample / "Sample.bim").read_bytes()
    from pbixtractor.catalog import list_entries

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"), ("First.pbix", report), ("First.bim", model),
                    ("Second.pbix", report), ("Second.bim", model))
        await user.should_see("2 models: each is documented separately")  # before the run
        user.find("Create documentation").click()
        await _batch_done(user)
        await user.should_see("2 of 2 models documented")
        # Each report with the model named like it, both in the catalog
        assert (sample / "output" / "First" / "First.xlsx").is_file()
        assert (sample / "output" / "Second" / "Second.xlsx").is_file()
        assert sorted(e["name"] for e in list_entries(sample / "output" / "_catalog")) == ["First", "Second"]
        await user.should_see(marker="batch_catalog")
        await user.should_see(marker="batch_cross_lineage")  # both models in one lineage viewer
        # A model's Details: its full result, as after a single run
        user.find(marker="batch_details").click()
        await user.should_see("Unused measures")

    _simulate(scenario)


def test_dropped_reports_without_any_model_are_refused(sample):
    report = (sample / "Sample.pbix").read_bytes()  # no model of its own, no .bim dropped

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"), ("First.pbix", report), ("Second.pbix", report))
        await user.should_see("No model found for these reports")  # said before the run
        user.find("Create documentation").click()
        await user.should_see("Nothing to document")
        await user.should_not_see(marker="batch_results")
        assert not (sample / "output" / "First").exists()

    _simulate(scenario)


def test_a_whole_fabric_workspace_is_documented_model_by_model(sample, monkeypatch):
    from pbixtractor.catalog import list_entries

    from .test_batch import WS_REPORTS, BatchFabric

    client = BatchFabric(sample / "parts")
    monkeypatch.setattr(web_sources, "make_fabric_client", lambda: client)
    monkeypatch.setattr("pbixtractor.service_stats.make_client", lambda: client)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        await _choose(user, "fabric_scope", "workspace")
        user.find(marker="fabric_plan_list").click()
        await user.should_see("Choose one or more workspaces first")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: _options(user, "fabric_workspaces"))
        await _choose(user, "fabric_workspaces", [WS_REPORTS["id"]])
        user.find(marker="fabric_plan_list").click()
        await _wait_for(lambda: len(_any_element(user, "fabric_plan_table").rows) == 2)
        # What cannot be documented is listed with the reason
        assert {e.text for e in user.find(marker="fabric_plan_skipped").elements} == {"Cannot be documented (3)"}
        await user.should_see("Paginated report - not supported")
        # Nothing is ticked at first: nothing runs
        user.find("Create documentation").click()
        await user.should_see("List the models and tick the ones to document first.")
        user.find(marker="fabric_plan_all").click()
        await user.should_see("2 of 2 models selected")
        user.find("Create documentation").click()
        await _batch_done(user)
        # Two models documented (one per model, with all its reports)
        await user.should_see("2 of 2 models documented")
        doc = json.loads(
            (sample / "output" / "Sales Model" / "Sales Model.json").read_text(encoding="utf-8")
        )
        assert [r["name"] for r in doc["reports"]] == ["Sales Detail", "Sales Overview"]
        sales = next(t for t in doc["model"]["tables"] if t["name"] == "Sales")
        assert sales["rows"] == 1000  # statistics from the service, as for a single report
        assert {e["name"] for e in list_entries(sample / "output" / "_catalog")} == {"Finance", "Sales Model"}

    _simulate(scenario)


def test_a_later_drop_starts_over(sample, monkeypatch):
    monkeypatch.setattr(web_ui, "DROP_SECONDS", -1)  # every file arrives "later"
    report = (sample / "Sample.pbix").read_bytes()
    uploads = sample / "output" / "_uploads"

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"), ("First.pbix", report), ("Second.pbix", report))
        await _wait_for(lambda: _value(user, "report") == str(uploads / "Second.pbix"))
        await user.should_not_see(marker="together")

    _simulate(scenario)


def test_an_uploaded_live_connected_pbix_gets_its_model_from_fabric(sample, monkeypatch):
    import zipfile

    from .test_fabric import MODEL_ID, WS_MODELS, FakeFabric, _report_parts

    with zipfile.ZipFile(sample / "Sample.pbix", "a") as archive:
        archive.writestr("Connections", json.dumps({"Version": 3, "Connections": [{
            "ConnectionType": "pbiServiceLive", "PbiModelDatabaseName": MODEL_ID,
            "ConnectionString": "Data Source=pbiazure://api.powerbi.com"}]}))
    client = FakeFabric(_report_parts(sample / "x", "Shared Models"), model_workspace=WS_MODELS)
    monkeypatch.setattr(web_sources, "make_fabric_client", lambda: client)
    monkeypatch.setattr("pbixtractor.service_stats.make_client", lambda: client)

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"), ("Thin.pbix", (sample / "Sample.pbix").read_bytes()))
        await _wait_for(lambda: _value(user, "report").endswith("Thin.pbix"))
        assert _value(user, "model") == ""  # no model of its own
        user.find("Create documentation").click()
        await user.should_see("Lineage", retries=100)
        doc = json.loads((sample / "output" / "Thin" / "Thin.json").read_text(encoding="utf-8"))
        assert next(t for t in doc["model"]["tables"] if t["name"] == "Sales")["rows"] == 1000
        assert (sample / "output" / "_fabric" / "Shared Models" / "Sample Model.SemanticModel").is_dir()

    _simulate(scenario)


def test_a_devops_report_on_a_published_model_gets_it_from_fabric(sample, monkeypatch):
    from .test_devops import FakeDevOps, _bound_to_service, _repo_files
    from .test_fabric import MODEL_ID, WS_SALES, FakeFabric, _report_parts

    files = _bound_to_service(_repo_files(sample, bound_by_path=False), "/Reports/Sample.Report", MODEL_ID)
    devops_client = FakeDevOps(files)
    fabric_client = FakeFabric(_report_parts(sample / "x", "Sales WS"), model_workspace=WS_SALES)
    monkeypatch.setattr(web_sources, "make_devops_client", lambda org: devops_client)
    monkeypatch.setattr(web_sources, "make_fabric_client", lambda: fabric_client)
    monkeypatch.setattr("pbixtractor.service_stats.make_client", lambda: fabric_client)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "devops")
        await _choose(user, "devops_org", "contoso")
        user.find(marker="devops_load").click()
        await _wait_for(lambda: _options(user, "devops_project"))
        await _choose(user, "devops_project", "BI Team", until=lambda: _options(user, "devops_repo"))
        await _choose(user, "devops_repo", "reports", until=lambda: _value(user, "devops_branch"))
        await _wait_for(lambda: _options(user, "devops_report"))
        await _choose(user, "devops_report", "/Reports/Sample.Report")
        user.find("Create documentation").click()
        await user.should_see("Lineage", retries=100)
        doc = json.loads((sample / "output" / "Sample" / "Sample.json").read_text(encoding="utf-8"))
        assert next(t for t in doc["model"]["tables"] if t["name"] == "Sales")["rows"] == 1000

    _simulate(scenario)


def test_dropped_pbix_files_with_their_own_models_are_flagged_at_once(sample):
    import io
    import zipfile

    buffer = io.BytesIO((sample / "Sample.pbix").read_bytes())
    with zipfile.ZipFile(buffer, "a") as archive:
        archive.writestr("DataModel", b"\x00")  # a model of its own (never read here)
    report = buffer.getvalue()

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"), ("First.pbix", report), ("Second.pbix", report))
        # Before any click: the page says these are separate models, documented separately
        await user.should_see(marker="separate_models")
        await user.should_see("2 models: each is documented separately")
        await user.should_see("Dropped:")
        await user.should_not_see("Documented together:")

    _simulate(scenario)


def test_the_page_says_where_the_model_comes_from(sample):
    import io
    import zipfile

    def with_entry(name: str, content) -> bytes:
        buffer = io.BytesIO((sample / "Sample.pbix").read_bytes())
        with zipfile.ZipFile(buffer, "a") as archive:
            archive.writestr(name, content)
        return buffer.getvalue()

    thick = with_entry("DataModel", b"\x00")
    thin = with_entry("Connections", json.dumps({"Connections": [{
        "ConnectionType": "pbiServiceLive", "PbiModelDatabaseName": "44444444-4444-4444-4444-444444444444",
        "ConnectionString": "Data Source=pbiazure://api.powerbi.com"}]}))

    async def scenario(user):
        await user.open("/")
        upload = _element(user, "upload")
        await _drop(upload, ("Thick.pbix", thick))
        await user.should_see("Model: inside the report")
        _element(user, "selected")  # the report shows as a chip
        web_ui.DROP_SECONDS, saved = -1, web_ui.DROP_SECONDS  # the next file is a new drop
        try:
            await _drop(upload, ("Thin.pbix", thin))
            await user.should_see("downloaded from Fabric when you run")
            await _drop(upload, ("Plain.pbix", (sample / "Sample.pbix").read_bytes()))
            await user.should_see("Model: not found")
        finally:
            web_ui.DROP_SECONDS = saved
        # Removing the last report starts over
        chip = next(e for e in user.client.elements.values() if isinstance(e, ui.chip))
        chip.value = False
        await _wait_for(lambda: _value(user, "report") == "")
        await user.should_not_see(marker="selected")

    _simulate(scenario)


def test_only_the_ticked_models_of_a_devops_repository_are_documented(sample, monkeypatch):
    from .test_batch import BatchFabric, _devops_repo
    from .test_devops import FakeDevOps

    devops_client = FakeDevOps(_devops_repo(sample / "repo"))
    monkeypatch.setattr(web_sources, "make_devops_client", lambda org: devops_client)
    monkeypatch.setattr(web_sources, "make_fabric_client", lambda: BatchFabric(sample / "parts"))

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "devops")
        await _choose(user, "devops_scope", "repository")
        await _choose(user, "devops_org", "contoso")
        user.find(marker="devops_load").click()
        await _wait_for(lambda: _options(user, "devops_project"))
        await _choose(user, "devops_project", "BI Team", until=lambda: _options(user, "devops_repo"))
        await _choose(user, "devops_repo", "reports", until=lambda: _value(user, "devops_branch"))
        user.find(marker="devops_plan_list").click()
        await _wait_for(lambda: len(_any_element(user, "devops_plan_table").rows) == 3)
        await user.should_see("0 of 3 models selected")
        # "Select all" ticks what the filter shows
        await _choose(user, "devops_plan_filter", "other")
        user.find(marker="devops_plan_all").click()
        await user.should_see("1 of 3 models selected")
        user.find("Create documentation").click()
        await _batch_done(user)
        await user.should_see("1 of 1 model documented")
        assert (sample / "output" / "Other" / "Other.xlsx").is_file()
        assert not (sample / "output" / "Sample").exists()
        # Another scope (here: a folder): the listed models no longer apply
        await _choose(user, "devops_folder", "/Reports")
        await _wait_for(lambda: not _any_element(user, "devops_plan_table").rows)

    _simulate(scenario)
