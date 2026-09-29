"""Tests for the NiceGUI web UI (web_ui.py), using NiceGUI's simulated user (no browser)."""

import asyncio
import json

import pytest

pytest.importorskip("nicegui")

from fastapi import HTTPException  # noqa: E402
from nicegui import ui  # noqa: E402
from nicegui.testing.user_simulation import user_simulation  # noqa: E402

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
