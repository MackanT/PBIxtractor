"""Tests for documenting many models at once (batch.py): planning, running, the CLI."""

import json
import zipfile
from pathlib import Path

import pytest

from pbixtractor.batch import (
    BatchJob,
    BatchSettings,
    Prepared,
    plan_fabric,
    plan_files,
    run_batch,
    summary,
)
from pbixtractor.catalog import list_entries
from pbixtractor.cli import main
from pbixtractor.fabric import FabricClient, FabricError, fetch_model_reports

from .sample_layout import write_sample_pbix
from .test_fabric import SERVICE_ROWS, _Credential, _model_parts, _report_parts
from .test_semantic_model import BIM

WS_REPORTS = {"id": "aaaaaaaa-0000-0000-0000-000000000001", "displayName": "Reports WS"}
WS_OTHER = {"id": "aaaaaaaa-0000-0000-0000-000000000002", "displayName": "Other WS"}
WS_MODELS = {"id": "aaaaaaaa-0000-0000-0000-000000000003", "displayName": "Shared Models"}
SALES = {"id": "bbbbbbbb-0000-0000-0000-000000000001", "displayName": "Sales Model"}  # in WS_MODELS
FINANCE = {"id": "bbbbbbbb-0000-0000-0000-000000000002", "displayName": "Finance"}  # in WS_REPORTS
LONELY = {"id": "bbbbbbbb-0000-0000-0000-000000000003", "displayName": "No Reports"}  # in WS_REPORTS
HIDDEN = "bbbbbbbb-0000-0000-0000-000000000004"  # a model in a workspace you cannot open


def _report(report_id, name, model=None, model_ws=None, kind="PowerBIReport"):
    report = {"id": report_id, "name": name, "reportType": kind}
    if model:
        report["datasetId"] = model
    if model_ws:
        report["datasetWorkspaceId"] = model_ws
    return report


REPORTS = {
    WS_REPORTS["id"]: [
        _report("r1", "Sales Overview", SALES["id"], WS_MODELS["id"]),
        _report("r2", "Sales Detail", SALES["id"], WS_MODELS["id"]),
        _report("r3", "Budget", FINANCE["id"], WS_REPORTS["id"]),
        _report("r4", "Invoices", kind="PaginatedReport"),
        _report("r5", "Secret", HIDDEN),
    ],
    WS_OTHER["id"]: [_report("r6", "Sales Nordics", SALES["id"])],  # no datasetWorkspaceId
}
MODELS = {WS_REPORTS["id"]: [FINANCE, LONELY], WS_MODELS["id"]: [SALES], WS_OTHER["id"]: []}


class BatchFabric(FabricClient):
    """Three workspaces; getDefinition answers at once (no long-running operation)."""

    def __init__(self, tmp_path: Path, failing=()):
        super().__init__(credential=_Credential(), sleep=lambda seconds: None)
        self.report_parts = _report_parts(tmp_path, "Shared Models")
        self.failing = set(failing)
        self.downloads = []

    def workspaces(self):
        return [WS_REPORTS, WS_OTHER, WS_MODELS]

    def powerbi_reports(self, workspace_id):
        return REPORTS.get(workspace_id, [])

    def semantic_models(self, workspace_id):
        return MODELS.get(workspace_id, [])

    def execute_query(self, workspace_id, model_id, dax):
        return next(rows for marker, rows in SERVICE_ROWS.items() if marker in dax)

    def get_definition(self, workspace_id, item_id, kind, format=None, progress=None):
        self.downloads.append((kind, item_id))
        if item_id in self.failing:
            raise FabricError("HTTP 403: InsufficientPrivileges")
        return self.report_parts if kind == "reports" else _model_parts()


SETTINGS = dict(tabular_editor_analysis=False, service_statistics=False)


@pytest.fixture
def files(tmp_path):
    """A.pbix + A.bim and B.pbix + B.bim: two reports, each on its own model."""
    for name in ("A", "B"):
        write_sample_pbix(tmp_path / f"{name}.pbix")
        (tmp_path / f"{name}.bim").write_text(json.dumps(BIM), encoding="utf-8")
    return tmp_path


def _live(path: Path, model_id: str) -> Path:
    """A live-connected .pbix: no model of its own, a Connections entry naming the published one."""
    write_sample_pbix(path)
    with zipfile.ZipFile(path, "a") as archive:
        archive.writestr("Connections", json.dumps({"Connections": [{
            "ConnectionType": "pbiServiceLive", "PbiModelDatabaseName": model_id,
            "ConnectionString": "Data Source=pbiazure://api.powerbi.com"}]}))
    return path


# ---------------------------------------------------------------- planning: files


def test_reports_on_different_models_become_one_job_each(files):
    jobs = plan_files([files / "A.pbix", files / "B.pbix"], [files / "A.bim", files / "B.bim"])
    assert [(j.model, j.reports, j.skip_reason) for j in jobs] == [("A", ["A"], ""), ("B", ["B"], "")]


def test_one_model_given_is_used_for_all_reports(files):
    (jobs,) = plan_files([files / "A.pbix", files / "B.pbix"], [files / "A.bim"])
    assert (jobs.model, jobs.reports, jobs.name) == ("A", ["A", "B"], "A")
    prepared = jobs.prepare(lambda text: None)
    assert prepared.report_paths == [files / "A.pbix", files / "B.pbix"]
    assert prepared.model_path == files / "A.bim"


def test_reports_without_a_model_and_models_without_a_report_are_skipped(files):
    write_sample_pbix(files / "C.pbix")  # no C.bim
    (files / "Spare.bim").write_text(json.dumps(BIM), encoding="utf-8")
    jobs = plan_files([files / "A.pbix", files / "C.pbix"], [files / "A.bim", files / "Spare.bim"])
    assert [(j.model, bool(j.prepare), j.skip_reason) for j in jobs] == [
        ("A", True, ""),
        ("C", False, "No model found - add C.bim with it"),
        ("Spare", False, "No report on this model was given"),
    ]


def test_live_connected_reports_are_grouped_by_their_published_model(tmp_path):
    reports = [_live(tmp_path / "One.pbix", SALES["id"]), _live(tmp_path / "Two.pbix", SALES["id"])]
    (job,) = plan_files(reports, make_client=lambda: BatchFabric(tmp_path), output_root=tmp_path / "out")
    assert job.reports == ["One", "Two"] and job.source == "Fabric (published model)"
    prepared = job.prepare(lambda text: None)
    assert prepared.model == prepared.name == "Sales Model"
    assert prepared.model_path == tmp_path / "out" / "_fabric" / "Shared Models" / "Sales Model.SemanticModel"
    assert prepared.service_model.model_id == SALES["id"]
    # Without a way to reach Fabric, it cannot run
    (offline,) = plan_files(reports[:1])
    assert offline.skip_reason and offline.prepare is None


# ---------------------------------------------------------------- planning: Fabric


def test_a_workspace_plan_groups_reports_by_model(tmp_path):
    jobs = plan_fabric(BatchFabric(tmp_path), ["Reports WS"], output_root=tmp_path)
    runnable = [(j.model, j.reports, j.source) for j in jobs if not j.skip_reason]
    assert runnable == [
        ("Finance", ["Budget"], "Fabric · Reports WS"),
        ("Sales Model", ["Sales Detail", "Sales Overview"], "Fabric · Shared Models"),
    ]
    skipped = {j.model: j.skip_reason for j in jobs if j.skip_reason}
    assert skipped == {
        "Invoices": "Paginated report - not supported",
        "No Reports": "No report on this model in the chosen workspaces",
        f"Model {HIDDEN}": "Its semantic model is in a workspace you cannot open",
    }
    # Runnable first, then the skipped ones
    assert [bool(j.skip_reason) for j in jobs] == [False, False, True, True, True]


def test_all_workspaces_completes_a_model_with_its_reports_elsewhere(tmp_path):
    jobs = plan_fabric(BatchFabric(tmp_path), ["Reports WS"], all_workspaces=True, output_root=tmp_path)
    sales = next(j for j in jobs if j.model == "Sales Model")
    assert sales.reports == ["Sales Detail", "Sales Overview", "Sales Nordics"]


def test_an_unknown_workspace_is_an_error(tmp_path):
    with pytest.raises(FabricError, match="No workspace 'Nope'"):
        plan_fabric(BatchFabric(tmp_path), ["Nope"])


def test_fetch_model_reports_downloads_the_model_once_and_lists_failed_reports(tmp_path):
    client = BatchFabric(tmp_path / "parts", failing={"r2"})
    reports = [(WS_REPORTS, {"id": "r1", "displayName": "Sales Overview"}),
               (WS_REPORTS, {"id": "r2", "displayName": "Sales Detail"})]
    fetched = fetch_model_reports(client, WS_MODELS, SALES, reports, tmp_path / "dl")
    assert client.downloads == [("semanticModels", SALES["id"]), ("reports", "r1"), ("reports", "r2")]
    assert [f.name for f in fetched.report_folders] == ["Sales Overview.Report"]
    (skipped,) = fetched.skipped
    assert skipped.startswith("Sales Detail (Reports WS): HTTP 403: InsufficientPrivileges")
    # Bound to the downloaded model, and where it came from (statistics, catalog identity)
    pbir = json.loads((fetched.report_folder / "definition.pbir").read_text(encoding="utf-8"))
    assert pbir["datasetReference"] == {"byPath": {"path": "../Sales Model.SemanticModel"}}
    source = json.loads((tmp_path / "dl" / "Sales Overview.fabric_source.json").read_text(encoding="utf-8"))
    assert source["semantic_model"]["id"] == SALES["id"]
    with pytest.raises(FabricError, match="None of the reports"):
        fetch_model_reports(BatchFabric(tmp_path / "p2", failing={"r1", "r2"}), WS_MODELS, SALES,
                            reports, tmp_path / "dl2")


# ---------------------------------------------------------------- running


def test_run_batch_documents_each_model_into_one_catalog(files):
    jobs = plan_files([files / "A.pbix", files / "B.pbix"], [files / "A.bim", files / "B.bim"])

    def broken(say):
        raise FabricError("HTTP 403: no access")

    jobs.insert(1, BatchJob("Broken", ["Broken"], "Fabric", broken))
    seen = []
    outcomes = run_batch(
        jobs,
        BatchSettings(files / "out", files / "catalog", **SETTINGS),
        on_outcome=lambda index, outcome: seen.append((index, outcome.status)),
    )
    assert [(o.job.model, o.ok) for o in outcomes] == [("A", True), ("Broken", False), ("B", True)]
    assert outcomes[1].message == "Download failed: HTTP 403: no access"  # and the batch went on
    assert (files / "out" / "A" / "A.xlsx").is_file() and (files / "out" / "B" / "B.xlsx").is_file()
    assert sorted(e["name"] for e in list_entries(files / "catalog")) == ["A", "B"]
    assert seen[0] == (0, "running") and (2, "running") in seen
    assert summary(outcomes).startswith("2 of 3 models documented")


def test_a_stopped_batch_skips_the_rest(files):
    jobs = plan_files([files / "A.pbix", files / "B.pbix"], [files / "A.bim", files / "B.bim"])
    done = []
    outcomes = run_batch(
        jobs,
        BatchSettings(files / "out", files / "catalog", **SETTINGS),
        on_outcome=lambda index, outcome: done.append(outcome.status),
        should_stop=lambda: "success" in done or "warnings" in done,
    )
    assert outcomes[0].ok
    assert (outcomes[1].status, outcomes[1].message) == ("skipped", "Stopped before this model")


def test_same_names_get_their_own_folders(files):
    def job(qualifier):
        return BatchJob("Sales", ["Sales"], "Fabric", lambda say: Prepared([files / "A.pbix"], files / "A.bim"),
                        qualifier=qualifier)

    outcomes = run_batch([job("WS 1"), job("WS 2"), job("")], BatchSettings(files / "out", files / "cat", **SETTINGS))
    assert [o.options.output_dir.name for o in outcomes] == ["Sales", "Sales (WS 2)", "Sales (2)"]


def test_a_fabric_workspace_is_documented_model_by_model(tmp_path):
    client = BatchFabric(tmp_path / "parts")
    jobs = plan_fabric(client, ["Reports WS"], output_root=tmp_path / "out")
    outcomes = run_batch(jobs, BatchSettings(tmp_path / "out", tmp_path / "catalog", **SETTINGS))
    assert [(o.job.model, o.ok) for o in outcomes if o.status != "skipped"] == [
        ("Finance", True), ("Sales Model", True)
    ]
    # Model mode for the two reports on Sales Model: named after the model, both reports in it
    doc = json.loads((tmp_path / "out" / "Sales Model" / "Sales Model.json").read_text(encoding="utf-8"))
    assert [r["name"] for r in doc["reports"]] == ["Sales Detail", "Sales Overview"]
    entries = {e["name"]: e for e in list_entries(tmp_path / "catalog")}
    assert set(entries) == {"Finance", "Sales Model"}
    assert entries["Sales Model"]["key"] == f"fabric-{SALES['id']}"


# ---------------------------------------------------------------- CLI


def test_cli_catalog_add_files(files, capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["catalog", "add", str(files / "catalog"), str(files / "A.pbix"), str(files / "B.pbix"),
              str(files / "A.bim"), str(files / "B.bim"), "-o", str(files / "out"),
              "--no-tabular-editor", "--no-service-statistics"])
    assert exit_info.value.code == 0
    printed = capsys.readouterr().out
    assert "2 models to document" in printed and "2 of 2 models documented" in printed
    assert len(list_entries(files / "catalog")) == 2


def test_cli_catalog_add_fabric_workspace(tmp_path, monkeypatch, capsys):
    import pbixtractor.cli as cli

    client = BatchFabric(tmp_path / "parts")
    monkeypatch.setattr(cli, "_fabric_client", lambda args: client)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as exit_info:
        main(["catalog", "add", "catalog", "--fabric-workspace", "Reports WS",
              "--no-tabular-editor", "--no-service-statistics", "-q"])
    assert exit_info.value.code == 0
    printed = capsys.readouterr().out
    assert "2 of 2 models documented" in printed and "3 skipped" in printed
    assert (tmp_path / "output" / "Budget" / "Budget.xlsx").is_file()  # one report: its name


def test_cli_catalog_add_needs_files_or_a_workspace(tmp_path, capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["catalog", "add", str(tmp_path / "catalog")])
    assert exit_info.value.code == 2
