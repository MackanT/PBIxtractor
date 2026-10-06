"""Web UI panels for reports in a Fabric workspace or an Azure DevOps repository.

Each panel lets the user pick a report step by step (sign-in happens on the first call) and
downloads it as a local PBIP project when the documentation is created:

    panel = FabricPanel(on_choose=lambda name: ...)   # builds the UI in the current container
    panel.ready()                                     # a report is chosen
    report_folder, model_path = panel.fetch(progress) # blocking: call via run.io_bound

Or, scope "Whole workspaces" / "Whole repository" (panel.is_batch): "List models" shows every
semantic model found (PlanPicker); the ticked ones are documented (panel.selected_jobs(),
batch.run_batch).

API calls block (and the first one may open a browser sign-in), so they run in threads.
"""

import logging
import queue
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from nicegui import run, ui

from . import batch, devops, fabric
from .azure_auth import ApiError
from .pipeline import model_name
from .service_stats import ServiceModel
from .web_config import output_root, stored_choices

logger = logging.getLogger("pbixtractor")


@dataclass
class Fetched:
    """What a panel downloaded: the report(s) and their model."""

    report_folder: Path
    model_path: Path
    extra_reports: list[Path] = field(default_factory=list)  # model mode: the other reports
    skipped: list[str] = field(default_factory=list)  # reports that could not be downloaded
    # A published model fetched for a report bound to the service (statistics come from it)
    service_model: Optional[ServiceModel] = None

    @property
    def model_mode(self) -> bool:
        return bool(self.extra_reports) or bool(self.skipped)

    @property
    def name(self) -> str:
        """Output name: the model's in model mode, else the report's."""
        if self.model_mode:
            return model_name(self.model_path)
        return self.report_folder.name.removesuffix(".Report")


MODEL_MODE_HELP = (
    "Document every report on this semantic model together: pages become '<report> › <page>' "
    "and 'unused' then means unused by all of them."
)


def make_fabric_client():
    """The Fabric client (tests replace this)."""
    return fabric.FabricClient()


def make_devops_client(org: str):
    """The Azure DevOps client (tests replace this)."""
    return devops.DevOpsClient(org)


async def _call(function, *args, select: Optional[ui.select] = None):
    """Run a blocking API call in a thread; show errors as a notification (returns None)."""
    if select is not None:
        select.props("loading")
    try:
        return await run.io_bound(function, *args)
    except (ApiError, ValueError, OSError) as error:
        ui.notify(str(error), type="negative", multi_line=True, timeout=15000)
        return None
    except Exception as error:  # UI boundary: never let a click end without any message
        logger.exception(f"Unexpected error: {error}")
        ui.notify(f"Unexpected error: {error}", type="negative", multi_line=True, timeout=15000)
        return None
    finally:
        if select is not None:
            select.props(remove="loading")


def _set_options(select: ui.select, options: dict, value=None) -> None:
    select.set_options(options, value=value if value in options else None)


PLAN_COLUMNS = [
    {"name": "model", "label": "Semantic model", "field": "model", "align": "left", "sortable": True},
    {"name": "reports", "label": "Reports", "field": "reports", "align": "left",
     "style": "white-space: normal"},
    {"name": "where", "label": "Where", "field": "where", "align": "left", "sortable": True},
]


class PlanPicker:
    """
    The semantic models found (in workspaces / a repository), to tick the ones to document.
    Nothing is ticked at first: only what is chosen is documented.

        picker = PlanPicker(plan=lambda progress: batch.plan_fabric(...), check=lambda: "")
        picker.selected_jobs()   # the ticked models' BatchJobs
        picker.clear()           # the scope changed: list again
    """

    def __init__(
        self,
        plan: Callable[[Callable[[str], None]], list[batch.BatchJob]],
        check: Callable[[], str],
        marker: str,
    ):
        """
        Args:
            plan: Blocking: lists the models (batch.plan_fabric / batch.plan_devops)
            check: "" when the models can be listed, else what to choose first
            marker: Prefix of the elements' markers ("fabric" -> "fabric_plan_list", ...)
        """
        self._plan = plan
        self._check = check
        self.jobs: list[batch.BatchJob] = []
        with ui.row().classes("w-full items-center no-wrap gap-2"):
            self.list_button = (
                ui.button("List models", icon="checklist", on_click=self.refresh)
                .props("outline no-caps")
                .mark(f"{marker}_plan_list")
            )
            self.status = ui.label("").classes("text-sm text-grey-7").mark(f"{marker}_plan_status")
        with ui.column().classes("w-full gap-1") as self.box:
            with ui.row().classes("w-full items-center no-wrap gap-2"):
                self.filter = (
                    ui.input(placeholder="Filter models and reports")
                    .props("dense clearable")
                    .classes("grow")
                    .mark(f"{marker}_plan_filter")
                )
                ui.button("Select all", on_click=self.select_all).props("flat dense no-caps").tooltip(
                    "Every model shown (matching the filter)"
                ).mark(f"{marker}_plan_all")
                ui.button("Select none", on_click=self.select_none).props("flat dense no-caps").mark(
                    f"{marker}_plan_none"
                )
            self.table = (
                ui.table(
                    columns=PLAN_COLUMNS,
                    rows=[],
                    row_key="id",
                    selection="multiple",
                    pagination={"rowsPerPage": 0},
                    on_select=lambda _: self._show_count(),
                )
                .props("dense flat bordered hide-bottom")
                .classes("w-full")
                .style("max-height: 24rem")
                .mark(f"{marker}_plan_table")
            )
            self.filter.bind_value_to(self.table, "filter")
            self.skipped = ui.expansion("").props("dense").classes("w-full text-sm").mark(f"{marker}_plan_skipped")
            with self.skipped:
                self.skipped_list = ui.column().classes("gap-0")
        self.box.visible = False

    async def refresh(self) -> None:
        """List the models (in a thread, with progress), then show them - none ticked."""
        problem = self._check()
        if problem:
            ui.notify(problem, type="warning")
            return
        messages: queue.Queue = queue.Queue()

        def show_progress() -> None:
            while not messages.empty():
                self.status.text = messages.get_nowait()

        timer = ui.timer(0.2, show_progress)
        self.list_button.props("loading")
        jobs = None
        try:
            jobs = await run.io_bound(self._plan, messages.put)
        except (ApiError, ValueError, OSError) as error:
            ui.notify(f"Listing failed: {error}", type="negative", multi_line=True, timeout=15000)
        except Exception as error:  # UI boundary: never let a click end without any message
            logger.exception(f"Listing failed unexpectedly: {error}")
            ui.notify(f"Listing failed: {error}", type="negative", multi_line=True, timeout=15000)
        finally:
            timer.cancel()
            self.list_button.props(remove="loading")
        if jobs is None:
            self.status.text = ""
            return
        self.show(jobs)

    def show(self, jobs: list[batch.BatchJob]) -> None:
        self.jobs = jobs
        self.table.rows = [
            {
                "id": index,
                "model": job.model,
                "reports": ", ".join(job.reports) or "-",
                "where": job.source.partition(" · ")[2] or job.source,
            }
            for index, job in enumerate(jobs)
            if job.prepare and not job.skip_reason
        ]
        self.table.selected = []
        self.table.update()
        unusable = [job for job in jobs if job.skip_reason or not job.prepare]
        self.skipped.text = f"Cannot be documented ({len(unusable)})"
        self.skipped.visible = bool(unusable)
        self.skipped_list.clear()
        with self.skipped_list:
            for job in unusable:
                reports = f" ({', '.join(job.reports)})" if job.reports and job.reports != [job.model] else ""
                ui.label(f"{job.model}{reports}: {job.skip_reason or 'nothing to document'}").classes(
                    "text-sm text-grey-7"
                )
        self.box.visible = True
        self._show_count()

    def _show_count(self) -> None:
        total, chosen = len(self.table.rows), len(self.table.selected)
        self.status.text = (
            f"{chosen} of {total} model{'s' if total != 1 else ''} selected" if total else "No models found"
        )

    def select_all(self) -> None:
        text = (self.filter.value or "").strip().lower()
        chosen = {row["id"] for row in self.table.selected}
        shown = [
            row for row in self.table.rows
            if not text or text in f"{row['model']} {row['reports']} {row['where']}".lower()
        ]
        self.table.selected = list(self.table.selected) + [r for r in shown if r["id"] not in chosen]
        self._show_count()

    def select_none(self) -> None:
        self.table.selected = []
        self._show_count()

    def selected_jobs(self) -> list[batch.BatchJob]:
        return [self.jobs[row["id"]] for row in self.table.selected if row["id"] < len(self.jobs)]

    def clear(self) -> None:
        """The scope changed: what was listed no longer applies."""
        self.jobs = []
        self.table.rows = []
        self.table.selected = []
        self.table.update()
        self.box.visible = False
        self.status.text = ""


class FabricPanel:
    """Workspace -> report; the report's semantic model is downloaded with it. Or whole
    workspaces: the models used by their reports are listed, the ticked ones documented
    (one documentation each, batch.py)."""

    def __init__(self, on_choose: Callable[[str], None]):
        self._on_choose = on_choose
        self._client = None
        self._workspaces: dict[str, str] = {}
        ui.label(
            "Downloads the report and its semantic model with the Fabric REST API. Needs "
            "Contributor (or higher) access; reports with an encrypting sensitivity label cannot "
            "be downloaded - use Azure DevOps for those."
        ).classes("text-caption text-grey-7")
        self.scope = (
            ui.toggle({"report": "One report", "workspace": "Whole workspaces"}, value="report")
            .props("dense unelevated")
            .mark("fabric_scope")
        )
        with ui.row().classes("w-full items-center no-wrap gap-2"):
            self.workspace = (
                ui.select({}, label="Workspace", with_input=True, on_change=self._workspace_changed)
                .classes("grow")
                .bind_visibility_from(self.scope, "value", value="report")
                .mark("fabric_workspace")
            )
            self.workspaces = (
                ui.select({}, label="Workspaces", multiple=True, with_input=True, value=[])
                .props("use-chips")
                .classes("grow")
                .bind_visibility_from(self.scope, "value", value="workspace")
                .mark("fabric_workspaces")
            )
            ui.button("Sign in / load", icon="login", on_click=self.load_workspaces).props(
                "flat"
            ).mark("fabric_load")
        with ui.column().classes("w-full gap-2") as one_report:
            self.report = (
                ui.select({}, label="Report", with_input=True, on_change=self._report_changed)
                .classes("w-full")
                .mark("fabric_report")
            )
            with ui.row().classes("items-center gap-4"):
                self.all_reports = (
                    ui.switch("All reports on its semantic model").tooltip(MODEL_MODE_HELP).mark(
                        "fabric_all_reports"
                    )
                )
                self.all_workspaces = (
                    ui.switch("Search all my workspaces (slower)")
                    .tooltip("Default: only the report's and the model's workspace")
                    .bind_visibility_from(self.all_reports, "value")
                    .mark("fabric_all_workspaces")
                )
        one_report.bind_visibility_from(self.scope, "value", value="report")
        with ui.column().classes("w-full gap-1") as whole:
            ui.label(
                "List the semantic models used by the reports in these workspaces, then tick the "
                "ones to document - one documentation per model, with all its reports, added to "
                "the catalog."
            ).classes("text-caption text-grey-7")
            self.batch_all_workspaces = (
                ui.switch("Also find their reports in my other workspaces (slower)")
                .tooltip(
                    "A model's reports in workspaces you did not choose are documented with it "
                    "too, so 'unused' is complete"
                )
                .mark("fabric_batch_all_workspaces")
            )
            self.picker = PlanPicker(
                self.plan,
                lambda: "" if self.workspaces.value else "Choose one or more workspaces first (Sign in / load).",
                "fabric",
            )
        whole.bind_visibility_from(self.scope, "value", value="workspace")
        self.workspaces.on_value_change(lambda _: self.picker.clear())
        self.batch_all_workspaces.on_value_change(lambda _: self.picker.clear())

    @property
    def is_batch(self) -> bool:
        """Whole workspaces: selected_jobs() + batch.run_batch() instead of fetch()."""
        return self.scope.value == "workspace"

    def selected_jobs(self) -> list[batch.BatchJob]:
        """The models ticked in the list (whole workspaces)."""
        return self.picker.selected_jobs()

    def _get_client(self):
        if self._client is None:
            self._client = make_fabric_client()
        return self._client

    async def load_workspaces(self) -> None:
        items = await _call(lambda: self._get_client().workspaces(), select=self.workspace)
        if items is None:
            return
        self._workspaces = {
            w["id"]: w["displayName"] for w in sorted(items, key=lambda w: w["displayName"].lower())
        }
        _set_options(self.workspace, self._workspaces, stored_choices().get("fabric_workspace"))
        self.workspaces.set_options(
            self._workspaces, value=[w for w in (self.workspaces.value or []) if w in self._workspaces]
        )
        if not items:
            ui.notify("No workspaces found for this account.", type="warning")

    async def _workspace_changed(self) -> None:
        _set_options(self.report, {})
        if not self.workspace.value:
            return
        workspace = self.workspace.value
        stored_choices()["fabric_workspace"] = workspace
        items = await _call(self._get_client().reports, workspace, select=self.report)
        if items is not None and self.workspace.value == workspace:  # not changed meanwhile
            _set_options(
                self.report,
                {r["id"]: r["displayName"] for r in sorted(items, key=lambda r: r["displayName"].lower())},
            )

    def _report_changed(self) -> None:
        if self.report.value:
            self._on_choose(self.report.options[self.report.value])

    def ready(self) -> bool:
        if self.is_batch:
            return bool(self.selected_jobs())
        return bool(self.workspace.value and self.report.value)

    def plan(self, progress: Callable[[str], None]) -> list[batch.BatchJob]:
        """Blocking: list the chosen workspaces' reports and models, one job per model."""
        return batch.plan_fabric(
            self._get_client(),
            list(self.workspaces.value),
            self.batch_all_workspaces.value,
            output_root(),
            progress,
        )

    def fetch(self, progress: Callable[[str], None]) -> Fetched:
        workspace = self._workspaces.get(self.workspace.value, self.workspace.value)
        fetched = fabric.fetch_report(
            self._get_client(),
            self.workspace.value,
            self.report.value,
            output_root() / "_fabric" / fabric.safe_name(workspace),
            progress=progress,
            all_reports=self.all_reports.value,
            all_workspaces=self.all_workspaces.value,
        )
        return Fetched(
            fetched.report_folder, fetched.model_path, fetched.report_folders[1:], fetched.skipped
        )


class DevOpsPanel:
    """Organisation -> project -> repository -> branch -> report -> version (commit). Or the
    whole repository (optionally one folder): its models are listed, the ticked ones
    documented (one documentation each, batch.py)."""

    def __init__(self, on_choose: Callable[[str], None]):
        self._on_choose = on_choose
        self._client = None
        self._default_branches: dict[str, str] = {}
        ui.label(
            "Reads a PBIP project (report + the semantic model it references) from a git "
            "repository, at the latest version of a branch or at an older commit. Same sign-in "
            "as Fabric, or a personal access token in AZURE_DEVOPS_PAT."
        ).classes("text-caption text-grey-7")
        self.scope = (
            ui.toggle({"report": "One report", "repository": "Whole repository"}, value="report")
            .props("dense unelevated")
            .mark("devops_scope")
        )
        with ui.row().classes("w-full items-center no-wrap gap-2"):
            self.org = (
                ui.input(
                    "Organisation",
                    placeholder="myorg or https://dev.azure.com/myorg",
                    value=stored_choices().get("devops_org", ""),
                )
                .classes("grow")
                .mark("devops_org")
            )
            ui.button("Sign in / load", icon="login", on_click=self.load_projects).props(
                "flat"
            ).mark("devops_load")
        with ui.row().classes("w-full no-wrap gap-2"):
            self.project = (
                ui.select({}, label="Project", with_input=True, on_change=self._project_changed)
                .classes("w-1/2")
                .mark("devops_project")
            )
            self.repo = (
                ui.select({}, label="Repository", with_input=True, on_change=self._repo_changed)
                .classes("w-1/2")
                .mark("devops_repo")
            )
        with ui.row().classes("w-full no-wrap gap-2"):
            self.branch = (
                ui.select({}, label="Branch", with_input=True, on_change=self._branch_changed)
                .classes("w-1/3")
                .mark("devops_branch")
            )
            self.report = (
                ui.select({}, label="Report", with_input=True, on_change=self._report_changed)
                .classes("w-2/3")
                .bind_visibility_from(self.scope, "value", value="report")
                .mark("devops_report")
            )
            self.folder = (
                ui.input("Only this folder (optional)", placeholder="/Reports/Finance")
                .classes("w-2/3")
                .bind_visibility_from(self.scope, "value", value="repository")
                .mark("devops_folder")
            )
        with ui.column().classes("w-full gap-2") as one_report:
            self.version = (
                ui.select({"": "Latest on the branch"}, label="Version", value="")
                .classes("w-full")
                .mark("devops_version")
            )
            self.all_reports = (
                ui.switch("All reports on its semantic model")
                .tooltip(MODEL_MODE_HELP + " (reports in this repository at the chosen version)")
                .mark("devops_all_reports")
            )
        one_report.bind_visibility_from(self.scope, "value", value="report")
        with ui.column().classes("w-full gap-1") as whole:
            ui.label(
                "List the semantic models used by the reports in this repository (latest on the "
                "branch), then tick the ones to document - one documentation per model, with all "
                "its reports, added to the catalog."
            ).classes("text-caption text-grey-7")
            self.picker = PlanPicker(
                self.plan,
                lambda: "" if self.project.value and self.repo.value and self.branch.value
                else "Choose the project, repository and branch first (Sign in / load).",
                "devops",
            )
        whole.bind_visibility_from(self.scope, "value", value="repository")
        for element in (self.project, self.repo, self.branch, self.folder):
            element.on_value_change(lambda _: self.picker.clear())

    @property
    def is_batch(self) -> bool:
        """Whole repository: selected_jobs() + batch.run_batch() instead of fetch()."""
        return self.scope.value == "repository"

    def selected_jobs(self) -> list[batch.BatchJob]:
        """The models ticked in the list (whole repository)."""
        return self.picker.selected_jobs()

    def plan(self, progress: Callable[[str], None]) -> list[batch.BatchJob]:
        """Blocking: list the repository's reports (and their models), one job per model."""
        return batch.plan_devops(
            self._get_client(),
            self.project.value,
            self.repo.value,
            self.branch.value,
            "branch",
            (self.folder.value or "").strip(),
            output_root(),
            make_fabric_client,  # reports bound to a published model
            progress,
        )

    def _get_client(self):
        org = devops.normalize_org(self.org.value or "")
        if self._client is None or self._client.org != org:
            self._client = make_devops_client(org)
        return self._client

    async def load_projects(self) -> None:
        if not (self.org.value or "").strip():
            ui.notify("Enter the organisation first.", type="warning")
            return
        names = await _call(lambda: self._get_client().projects(), select=self.project)
        if names is None:
            return
        stored_choices()["devops_org"] = self.org.value.strip()
        _set_options(self.project, {n: n for n in names}, stored_choices().get("devops_project"))

    async def _project_changed(self) -> None:
        for select in (self.repo, self.branch, self.report):
            _set_options(select, {})
        if not self.project.value:
            return
        asked = self._selection(1)
        stored_choices()["devops_project"] = self.project.value
        repos = await _call(self._get_client().repositories, self.project.value, select=self.repo)
        if repos is not None and self._selection(1) == asked:  # not changed meanwhile
            self._default_branches = {r["name"]: r["defaultBranch"] for r in repos}
            _set_options(self.repo, {r["name"]: r["name"] for r in repos}, stored_choices().get("devops_repo"))

    async def _repo_changed(self) -> None:
        _set_options(self.branch, {})
        _set_options(self.report, {})
        if not self.repo.value:
            return
        asked = self._selection(2)
        stored_choices()["devops_repo"] = self.repo.value
        names = await _call(self._get_client().branches, self.project.value, self.repo.value, select=self.branch)
        if names and self._selection(2) == asked:
            default = self._default_branches.get(self.repo.value)
            _set_options(self.branch, {n: n for n in names}, default if default in names else names[0])

    async def _branch_changed(self) -> None:
        _set_options(self.report, {})
        if not self.branch.value:
            return
        asked = self._selection(3)
        paths = await _call(
            self._get_client().find_reports,
            self.project.value,
            self.repo.value,
            self.branch.value,
            select=self.report,
        )
        if paths is not None and self._selection(3) == asked:
            _set_options(self.report, {p: p.lstrip("/") for p in paths})
            if not paths:
                ui.notify("No PBIP reports (*.Report/definition.pbir) on this branch.", type="warning")

    async def _report_changed(self) -> None:
        self.version.set_options({"": f"Latest on {self.branch.value}"}, value="")
        if not self.report.value:
            return
        self._on_choose(self.report.value.rsplit("/", 1)[-1].removesuffix(".Report"))
        asked = self._selection(4)
        commits = await _call(
            self._get_client().commits,
            self.project.value,
            self.repo.value,
            self.report.value,
            self.branch.value,
            select=self.version,
        )
        if commits and self._selection(4) == asked:
            options = {"": f"Latest on {self.branch.value}"}
            options.update(
                {c["id"]: f"{c['short']} · {c['date']} · {c['author']}: {c['comment']}" for c in commits}
            )
            self.version.set_options(options, value="")

    def _selection(self, depth: int) -> tuple:
        """The first `depth` choices (project, repo, branch, report): a slow answer is only
        applied if they did not change while it was on its way."""
        return (self.project.value, self.repo.value, self.branch.value, self.report.value)[:depth]

    def ready(self) -> bool:
        if self.is_batch:
            return bool(self.selected_jobs())
        return bool(self.project.value and self.repo.value and self.branch.value and self.report.value)

    def fetch(self, progress: Callable[[str], None]) -> Fetched:
        """Blocking download; a report bound to a published model gets it from Fabric (the
        same sign-in)."""
        connected: list[fabric.FetchedModel] = []

        def connected_model(pbir: dict) -> Path:
            model_id, workspace = fabric.model_reference(pbir)
            connected.append(
                fabric.fetch_connected_model(
                    make_fabric_client(), model_id, workspace, output_root() / "_fabric", progress
                )
            )
            return connected[-1].model_path

        commit = self.version.value or ""
        version, version_type = (commit, "commit") if commit else (self.branch.value, "branch")
        fetched = devops.fetch_report(
            self._get_client(),
            self.project.value,
            self.repo.value,
            self.report.value,
            # default_destination() is output/_devops/...: keep the layout, below output_root()
            output_root()
            / devops.default_destination(self.project.value, self.repo.value, version).relative_to(
                "output"
            ),
            version,
            version_type,
            progress=progress,
            all_reports=self.all_reports.value,
            connected_model=connected_model,
        )
        return Fetched(
            fetched.report_folder,
            fetched.model_path,
            fetched.report_folders[1:],
            fetched.skipped,
            service_model=connected_service_model(connected),
        )


def connected_service_model(connected: list) -> Optional[ServiceModel]:
    """The published model of a service-bound report, for statistics (None if there was none)."""
    if not connected:
        return None
    model = connected[-1]
    return ServiceModel(model.workspace_id, model.model_id, model.model)
