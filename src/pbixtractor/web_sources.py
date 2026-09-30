"""Web UI panels for reports in a Fabric workspace or an Azure DevOps repository.

Each panel lets the user pick a report step by step (sign-in happens on the first call) and
downloads it as a local PBIP project when the documentation is created:

    panel = FabricPanel(on_choose=lambda name: ...)   # builds the UI in the current container
    panel.ready()                                     # a report is chosen
    report_folder, model_path = panel.fetch(progress) # blocking: call via run.io_bound

API calls block (and the first one may open a browser sign-in), so they run in threads.
"""

from pathlib import Path
from typing import Callable, Optional

from nicegui import app, run, ui

from . import devops, fabric
from .azure_auth import ApiError


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
    finally:
        if select is not None:
            select.props(remove="loading")


def _set_options(select: ui.select, options: dict, value=None) -> None:
    select.set_options(options, value=value if value in options else None)


def _storage() -> dict:
    """Remembered choices (server-side JSON in .nicegui/); empty if storage is unavailable."""
    try:
        return app.storage.general
    except (RuntimeError, AttributeError):
        return {}


class FabricPanel:
    """Workspace -> report; the report's semantic model is downloaded with it."""

    def __init__(self, on_choose: Callable[[str], None]):
        self._on_choose = on_choose
        self._client = None
        self._workspaces: dict[str, str] = {}
        ui.label(
            "Downloads the report and its semantic model with the Fabric REST API. Needs "
            "Contributor (or higher) access; reports with an encrypting sensitivity label cannot "
            "be downloaded - use Azure DevOps for those."
        ).classes("text-caption text-grey-7")
        with ui.row().classes("w-full items-center no-wrap gap-2"):
            self.workspace = (
                ui.select({}, label="Workspace", with_input=True, on_change=self._workspace_changed)
                .classes("grow")
                .mark("fabric_workspace")
            )
            ui.button("Sign in / load", icon="login", on_click=self.load_workspaces).props(
                "flat no-caps"
            ).mark("fabric_load")
        self.report = (
            ui.select({}, label="Report", with_input=True, on_change=self._report_changed)
            .classes("w-full")
            .mark("fabric_report")
        )

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
        _set_options(self.workspace, self._workspaces, _storage().get("fabric_workspace"))
        if not items:
            ui.notify("No workspaces found for this account.", type="warning")

    async def _workspace_changed(self) -> None:
        _set_options(self.report, {})
        if not self.workspace.value:
            return
        _storage()["fabric_workspace"] = self.workspace.value
        items = await _call(self._get_client().reports, self.workspace.value, select=self.report)
        if items is not None:
            _set_options(
                self.report,
                {r["id"]: r["displayName"] for r in sorted(items, key=lambda r: r["displayName"].lower())},
            )

    def _report_changed(self) -> None:
        if self.report.value:
            self._on_choose(self.report.options[self.report.value])

    def ready(self) -> bool:
        return bool(self.workspace.value and self.report.value)

    def fetch(self, progress: Callable[[str], None]) -> tuple[Path, Path]:
        workspace = self._workspaces.get(self.workspace.value, self.workspace.value)
        fetched = fabric.fetch_report(
            self._get_client(),
            self.workspace.value,
            self.report.value,
            Path.cwd() / "output" / "_fabric" / fabric.safe_name(workspace),
            progress=progress,
        )
        return fetched.report_folder, fetched.model_path


class DevOpsPanel:
    """Organisation -> project -> repository -> branch -> report -> version (commit)."""

    def __init__(self, on_choose: Callable[[str], None]):
        self._on_choose = on_choose
        self._client = None
        self._default_branches: dict[str, str] = {}
        ui.label(
            "Reads a PBIP project (report + the semantic model it references) from a git "
            "repository, at the latest version of a branch or at an older commit. Same sign-in "
            "as Fabric, or a personal access token in AZURE_DEVOPS_PAT."
        ).classes("text-caption text-grey-7")
        with ui.row().classes("w-full items-center no-wrap gap-2"):
            self.org = (
                ui.input(
                    "Organisation",
                    placeholder="myorg or https://dev.azure.com/myorg",
                    value=_storage().get("devops_org", ""),
                )
                .classes("grow")
                .mark("devops_org")
            )
            ui.button("Sign in / load", icon="login", on_click=self.load_projects).props(
                "flat no-caps"
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
                .mark("devops_report")
            )
        self.version = (
            ui.select({"": "Latest on the branch"}, label="Version", value="")
            .classes("w-full")
            .mark("devops_version")
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
        _storage()["devops_org"] = self.org.value.strip()
        _set_options(self.project, {n: n for n in names}, _storage().get("devops_project"))

    async def _project_changed(self) -> None:
        for select in (self.repo, self.branch, self.report):
            _set_options(select, {})
        if not self.project.value:
            return
        _storage()["devops_project"] = self.project.value
        repos = await _call(self._get_client().repositories, self.project.value, select=self.repo)
        if repos is not None:
            self._default_branches = {r["name"]: r["defaultBranch"] for r in repos}
            _set_options(self.repo, {r["name"]: r["name"] for r in repos}, _storage().get("devops_repo"))

    async def _repo_changed(self) -> None:
        _set_options(self.branch, {})
        _set_options(self.report, {})
        if not self.repo.value:
            return
        _storage()["devops_repo"] = self.repo.value
        names = await _call(self._get_client().branches, self.project.value, self.repo.value, select=self.branch)
        if names:
            default = self._default_branches.get(self.repo.value)
            _set_options(self.branch, {n: n for n in names}, default if default in names else names[0])

    async def _branch_changed(self) -> None:
        _set_options(self.report, {})
        if not self.branch.value:
            return
        paths = await _call(
            self._get_client().find_reports,
            self.project.value,
            self.repo.value,
            self.branch.value,
            select=self.report,
        )
        if paths is not None:
            _set_options(self.report, {p: p.lstrip("/") for p in paths})
            if not paths:
                ui.notify("No PBIP reports (*.Report/definition.pbir) on this branch.", type="warning")

    async def _report_changed(self) -> None:
        self.version.set_options({"": f"Latest on {self.branch.value}"}, value="")
        if not self.report.value:
            return
        self._on_choose(self.report.value.rsplit("/", 1)[-1].removesuffix(".Report"))
        commits = await _call(
            self._get_client().commits,
            self.project.value,
            self.repo.value,
            self.report.value,
            self.branch.value,
            select=self.version,
        )
        if commits:
            options = {"": f"Latest on {self.branch.value}"}
            options.update(
                {c["id"]: f"{c['short']} · {c['date']} · {c['author']}: {c['comment']}" for c in commits}
            )
            self.version.set_options(options, value="")

    def ready(self) -> bool:
        return bool(self.project.value and self.repo.value and self.branch.value and self.report.value)

    def fetch(self, progress: Callable[[str], None]) -> tuple[Path, Path]:
        commit = self.version.value or ""
        version, version_type = (commit, "commit") if commit else (self.branch.value, "branch")
        fetched = devops.fetch_report(
            self._get_client(),
            self.project.value,
            self.repo.value,
            self.report.value,
            Path.cwd() / devops.default_destination(self.project.value, self.repo.value, version),
            version,
            version_type,
            progress=progress,
        )
        return fetched.report_folder, fetched.model_path
