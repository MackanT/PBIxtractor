"""Tests for the NiceGUI web UI (web_ui.py), using NiceGUI's simulated user (no browser)."""

import asyncio
import json

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
        await asyncio.sleep(0.3)  # let the handlers finish
        uploads = sample / "output" / "_uploads"
        assert (uploads / "Dropped.pbix").read_bytes() == report_bytes
        assert user.find(marker="report").elements.pop().value == str(uploads / "Dropped.pbix")
        assert user.find(marker="model").elements.pop().value == str(uploads / "Dropped.bim")
        assert user.find(marker="output").elements.pop().value.endswith("Dropped")

        await upload.handle_uploads([ui.upload.SmallFileUpload("notes.txt", "", b"x")])
        await user.should_see("drop a .pbix report or a .bim model")

    _simulate(scenario)


async def _choose(user, marker: str, value) -> None:
    """Set a select/input like a user would and let its (async) handler finish."""
    user.find(marker=marker).elements.pop().value = value
    await asyncio.sleep(0.3)


def test_document_a_report_from_devops(sample, monkeypatch):
    from .test_devops import FakeDevOps, _repo_files

    client = FakeDevOps(_repo_files(sample))
    monkeypatch.setattr(web_sources, "make_devops_client", lambda org: client)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "devops")
        await _choose(user, "devops_org", "contoso")
        user.find(marker="devops_load").click()
        await asyncio.sleep(0.3)
        await _choose(user, "devops_project", "BI Team")
        await _choose(user, "devops_repo", "reports")
        branch = user.find(marker="devops_branch").elements.pop()
        assert branch.value == "main"  # the repository's default branch is preselected
        await asyncio.sleep(0.3)
        await _choose(user, "devops_report", "/Reports/Sample.Report")
        version = user.find(marker="devops_version").elements.pop()
        assert "a1b2c3d4" in list(version.options.values())[1]  # commit history listed
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
        await asyncio.sleep(0.3)
        await _choose(user, "fabric_workspace", WS_SALES)
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


def test_log_count_counts_messages_not_lines():
    logs = "WARNING: first\nWARNING: Tabular Editor failed:\n  details\n  more\nERROR: last\n"
    assert web_ui._log_count(logs) == 3
    assert web_ui._log_count("") == 0
