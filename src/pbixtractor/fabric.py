"""Download reports and semantic models from a Fabric / Power BI workspace.

The definitions are written as a PBIP project, so the normal local pipeline documents them:

    client = FabricClient()                         # signs in (browser, once; then cached)
    fetched = fetch_report(client, "Sales WS", "Sales", Path("output/_fabric/Sales WS"))
    fetched.report_folder   # .../Sales.Report          (PBIR or PBIR-Legacy)
    fetched.model_path      # .../<Model>.SemanticModel (TMDL)

Uses the Fabric REST API (getDefinition, a long-running operation). The signed-in user needs
read AND write permission on the report and model (Contributor or higher in the workspace) -
getDefinition requires it. Nothing is changed in the workspace. getDefinition is blocked for
items with an encrypting sensitivity label (labels without encryption are fine).

Sign-in: see azure_auth.py.
"""

import base64
import json
import logging
import os
import re
import shutil
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from .azure_auth import FABRIC_SCOPE, POWERBI_SCOPE, ApiError, RestClient

logger = logging.getLogger("pbixtractor")

API = "https://api.fabric.microsoft.com/v1"
POWERBI_API = "https://api.powerbi.com/v1.0/myorg"
_GUID = re.compile(r"^[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")

FabricError = ApiError  # kept as the name callers catch


def _column_name(key: str) -> str:
    """executeQueries column key -> plain name: "[Rows]" -> "Rows", "T[Col]" -> "Col"."""
    return key[key.index("[") + 1 : -1] if key.endswith("]") and "[" in key else key


# ============================================================================
# REST client
# ============================================================================


class FabricClient(RestClient):
    """Minimal Fabric REST client: list workspaces/items and download item definitions."""

    def __init__(self, credential=None, api: str = API, sleep: Callable[[float], None] = time.sleep):
        """
        Args:
            credential: Object with get_token(scope) (default: azure_auth.get_credential())
            api: API root (tests point this elsewhere)
            sleep: Wait function used while polling (tests pass a no-op)
        """
        super().__init__(api, FABRIC_SCOPE, credential=credential, sleep=sleep)
        self._powerbi: Optional[RestClient] = None

    @property
    def powerbi(self) -> RestClient:
        """The Power BI REST API (same sign-in): report -> model bindings and DAX queries."""
        if self._powerbi is None:
            self._powerbi = RestClient(
                POWERBI_API, POWERBI_SCOPE, credential=self._credential, sleep=self._sleep
            )
        return self._powerbi

    def powerbi_reports(self, workspace_id: str) -> list[dict]:
        """Reports of a workspace with their model: [{"id", "name", "datasetId", ...}]."""
        _, _, body = self.powerbi.request("GET", f"groups/{workspace_id}/reports")
        return body.get("value", [])

    def execute_query(self, workspace_id: str, model_id: str, dax: str) -> list[dict]:
        """
        Run one DAX query on a semantic model in the service (read-only).

        Returns:
            Rows as dicts; column names without brackets ("Table[Col]" -> "Col")
        """
        _, _, body = self.powerbi.request(
            "POST",
            f"groups/{workspace_id}/datasets/{model_id}/executeQueries",
            {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}},
        )
        result = (body.get("results") or [{}])[0]
        if result.get("error"):
            raise ApiError(f"DAX query failed: {result['error'].get('message', result['error'])}")
        rows = ((result.get("tables") or [{}])[0]).get("rows", [])
        return [{_column_name(key): value for key, value in row.items()} for row in rows]

    def _list(self, path: str) -> list[dict]:
        """GET a paged list (follows continuationUri)."""
        items, url = [], path
        while url:
            _, _, body = self.request("GET", url)
            items += body.get("value", [])
            url = body.get("continuationUri")
        return items

    def workspaces(self) -> list[dict]:
        """Workspaces the user can access: [{"id", "displayName", "type", ...}]."""
        return self._list("workspaces")

    def reports(self, workspace_id: str) -> list[dict]:
        return self._list(f"workspaces/{workspace_id}/reports")

    def semantic_models(self, workspace_id: str) -> list[dict]:
        return self._list(f"workspaces/{workspace_id}/semanticModels")

    def get_definition(
        self,
        workspace_id: str,
        item_id: str,
        kind: str,
        format: Optional[str] = None,
        progress: Optional[Callable[[float], None]] = None,
    ) -> list[dict]:
        """
        Download an item definition (long-running operation: polled until done).

        Args:
            workspace_id: Workspace id
            item_id: Report or semantic model id
            kind: "reports" or "semanticModels"
            format: Optional definition format (reports: PBIR / PBIR-Legacy; models: TMDL /
                TMSL)
            progress: Called with the seconds waited so far while Fabric prepares it

        Returns:
            Definition parts: [{"path", "payload" (base64), "payloadType"}]
        """
        url = f"workspaces/{workspace_id}/{kind}/{item_id}/getDefinition"
        if format:
            url += f"?format={urllib.parse.quote(format)}"
        status, headers, body = self.request("POST", url)
        if status == 202:
            body = self._wait_for_operation(headers, progress=progress)
        return (body or {}).get("definition", {}).get("parts", [])

    def _wait_for_operation(
        self, headers: dict, timeout: float = 600, progress: Optional[Callable[[float], None]] = None
    ) -> dict:
        headers = {k.lower(): v for k, v in headers.items()}
        operation = headers.get("x-ms-operation-id")
        location = headers.get("location") or f"{self._api}/operations/{operation}"
        started = time.monotonic()
        deadline = started + timeout
        while True:
            # Fabric suggests the wait (Retry-After, often 20-30 s); poll at least every 5 s so
            # small items finish sooner and the user sees progress
            self._sleep(min(float(headers.get("retry-after") or 2), 5))
            if progress:
                progress(time.monotonic() - started)
            _, headers, state = self.request("GET", location)
            headers = {k.lower(): v for k, v in headers.items()}
            status = (state or {}).get("status")
            if status == "Succeeded":
                break
            if status in ("Failed", "Undefined"):
                error = (state or {}).get("error") or {}
                raise FabricError(
                    f"getDefinition failed: {error.get('errorCode', '')} {error.get('message', '')}"
                )
            if time.monotonic() > deadline:
                raise FabricError(f"getDefinition did not finish within {timeout:.0f}s")
        result_url = headers.get("location") or f"{location.rstrip('/')}/result"
        _, _, body = self.request("GET", result_url)
        return body


# ============================================================================
# Finding items
# ============================================================================


def _find(items: list[dict], name_or_id: str, what: str) -> dict:
    """An item by id or (case-insensitive) display name."""
    if _GUID.match(name_or_id):
        for item in items:
            if item["id"].lower() == name_or_id.lower():
                return item
    matches = [i for i in items if i.get("displayName", "").lower() == name_or_id.lower()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise FabricError(f"Several {what}s are called '{name_or_id}' - use the id instead.")
    names = ", ".join(sorted(i.get("displayName", "") for i in items)[:20])
    raise FabricError(f"No {what} '{name_or_id}'. Available: {names or '(none)'}")


def find_workspace(client: FabricClient, name_or_id: str) -> dict:
    return _find(client.workspaces(), name_or_id, "workspace")


def split_fabric_path(value: str) -> tuple[str, str]:
    """ "Workspace/Report" -> ("Workspace", "Report") (split at the last "/")."""
    workspace, _, report = value.rpartition("/")
    if not workspace or not report:
        raise ValueError(f"Expected WORKSPACE/REPORT, got '{value}'")
    return workspace, report


def model_reference(pbir: dict) -> tuple[Optional[str], Optional[str]]:
    """
    The semantic model a service report is bound to, from its definition.pbir.

    Returns:
        (model id, workspace name from the connection string); either may be None
    """
    by_connection = (pbir.get("datasetReference") or {}).get("byConnection") or {}
    text = by_connection.get("connectionString") or ""
    model_id = by_connection.get("pbiModelDatabaseName")
    match = re.search(r"semanticmodelid=([0-9a-fA-F-]{36})", text, re.IGNORECASE)
    if match:
        model_id = match.group(1)
    workspace = re.search(r"powerbi://[^;]*/myorg/([^;\"]+)", text, re.IGNORECASE)
    return model_id, (urllib.parse.unquote(workspace.group(1)).strip() if workspace else None)


# ============================================================================
# Fetching
# ============================================================================


@dataclass
class FetchedReport:
    workspace: str
    report: str
    model: str
    report_folder: Path  # <report>.Report (the report asked for)
    model_path: Path  # <model>.SemanticModel
    model_id: str = ""
    model_workspace_id: str = ""
    # With all_reports: every downloaded report on the model (the asked-for one first)
    report_folders: list[Path] = field(default_factory=list)
    # Reports on the model that could not be downloaded: "<name> (<workspace>): <reason>"
    skipped: list[str] = field(default_factory=list)


def safe_name(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]+', "_", name).strip(" .") or "item"


def write_parts(parts: list[dict], folder: Path) -> None:
    """Write definition parts (base64 payloads) under a folder, replacing its content."""
    if folder.exists():
        shutil.rmtree(folder)
    for part in parts:
        target = (folder / part["path"]).resolve()
        if folder.resolve() not in target.parents:
            raise FabricError(f"Unexpected part path outside the item folder: {part['path']}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(base64.b64decode(part.get("payload", "")))


def _download_report(client: FabricClient, ws: dict, rep: dict, folder: Path, say) -> dict:
    """Download a report definition into folder; returns its definition.pbir content."""
    say(f"Downloading report {rep['displayName']}")
    try:
        parts = client.get_definition(
            ws["id"],
            rep["id"],
            "reports",
            progress=lambda s: say(f"Report {rep['displayName']}: Fabric is preparing it ({s:.0f} s)"),
        )
    except FabricError as error:
        raise FabricError(
            f"{error} (Reports with an encrypting sensitivity label cannot be downloaded from "
            "Fabric; use a git-synced copy, e.g. Azure DevOps, instead.)"
        ) from None
    write_parts(parts, folder)
    pbir_path = folder / "definition.pbir"
    return json.loads(pbir_path.read_bytes().decode("utf-8-sig")) if pbir_path.is_file() else {}


def _bind_to_local_model(report_folder: Path, pbir: dict, model_folder: Path) -> None:
    """Point the report at the local model, as in a PBIP project saved by Power BI Desktop."""
    relative = Path(os.path.relpath(model_folder, report_folder)).as_posix()
    pbir["datasetReference"] = {"byPath": {"path": relative}}
    (report_folder / "definition.pbir").write_text(json.dumps(pbir, indent=2), encoding="utf-8")


def _write_source(report_folder: Path, ws: dict, rep: dict, model_ws: dict, model: dict) -> None:
    """<report>.fabric_source.json next to the report: where it came from (used for statistics)."""
    (report_folder.parent / f"{report_folder.stem}.fabric_source.json").write_text(
        json.dumps(
            {
                "workspace": {"id": ws["id"], "name": ws["displayName"]},
                "report": {"id": rep["id"], "name": rep["displayName"]},
                "semantic_model": {
                    "id": model["id"],
                    "name": model["displayName"],
                    "workspace": model_ws["displayName"],
                    "workspace_id": model_ws["id"],
                },
                "fetched": time.strftime("%Y-%m-%d %H:%M:%S"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def reports_on_model(
    client: FabricClient, model_id: str, workspaces: list[dict], say=None
) -> list[tuple[dict, dict]]:
    """
    Reports bound to a semantic model, in the given workspaces.

    Returns:
        [(workspace, report as {"id", "displayName"})]; workspaces that cannot be read are skipped
    """
    found = []
    for ws in workspaces:
        if say:
            say(f"Looking for reports on the model in {ws['displayName']}")
        try:
            reports = client.powerbi_reports(ws["id"])
        except FabricError as error:
            logger.debug(f"Cannot list reports in {ws['displayName']}: {error}")
            continue
        for report in reports:
            if (report.get("datasetId") or "").lower() == model_id.lower():
                found.append((ws, {"id": report["id"], "displayName": report["name"]}))
    return found


def fetch_report(
    client: FabricClient,
    workspace: str,
    report: str,
    destination: Path,
    progress: Optional[Callable[[str], None]] = None,
    all_reports: bool = False,
    all_workspaces: bool = False,
) -> FetchedReport:
    """
    Download a report and its semantic model as a PBIP project.

    Args:
        client: FabricClient
        workspace: Workspace name or id
        report: Report name or id
        destination: Folder for <report>.Report and <model>.SemanticModel
        progress: Called with a short status text per step
        all_reports: Also download every other report bound to the same model (model mode)
        all_workspaces: With all_reports, search every accessible workspace, not only the
            report's and the model's

    Returns:
        FetchedReport with the local report folder(s) and model folder
    """
    say = progress or (lambda text: None)
    destination = Path(destination)
    say(f"Finding workspace {workspace}")
    workspaces = client.workspaces()
    ws = _find(workspaces, workspace, "workspace")
    say(f"Finding report {report}")
    rep = _find(client.reports(ws["id"]), report, "report")

    report_folder = destination / f"{safe_name(rep['displayName'])}.Report"
    pbir = _download_report(client, ws, rep, report_folder, say)
    model_id, model_ws_name = model_reference(pbir)
    if not model_id:
        raise FabricError(
            f"Cannot tell which semantic model {rep['displayName']} uses (definition.pbir has "
            "no semanticmodelid)."
        )

    say("Finding the semantic model")
    model_ws, model = ws, None
    try:
        model = _find(client.semantic_models(ws["id"]), model_id, "semantic model")
    except FabricError:
        if model_ws_name:
            model_ws = _find(workspaces, model_ws_name, "workspace")
            model = _find(client.semantic_models(model_ws["id"]), model_id, "semantic model")
    if model is None:
        raise FabricError(
            f"Semantic model {model_id} of {rep['displayName']} is not in workspace "
            f"{ws['displayName']} and its workspace is unknown."
        )

    say(f"Downloading semantic model {model['displayName']}")
    model_parts = client.get_definition(
        model_ws["id"],
        model["id"],
        "semanticModels",
        "TMDL",
        progress=lambda s: say(f"Semantic model {model['displayName']}: Fabric is preparing it ({s:.0f} s)"),
    )
    model_folder = destination / f"{safe_name(model['displayName'])}.SemanticModel"
    write_parts(model_parts, model_folder)
    _bind_to_local_model(report_folder, pbir, model_folder)
    _write_source(report_folder, ws, rep, model_ws, model)

    fetched = FetchedReport(
        workspace=ws["displayName"],
        report=rep["displayName"],
        model=model["displayName"],
        report_folder=report_folder,
        model_path=model_folder,
        model_id=model["id"],
        model_workspace_id=model_ws["id"],
        report_folders=[report_folder],
    )
    if not all_reports:
        return fetched

    search = workspaces if all_workspaces else list({w["id"]: w for w in (ws, model_ws)}.values())
    used_names = {report_folder.name.lower()}
    for other_ws, other in reports_on_model(client, model["id"], search, say):
        if other["id"] == rep["id"]:
            continue
        name = safe_name(other["displayName"])
        if f"{name}.report".lower() in used_names:  # same name in another workspace
            name = safe_name(f"{other['displayName']} ({other_ws['displayName']})")
        used_names.add(f"{name}.report".lower())
        folder = destination / f"{name}.Report"
        try:
            other_pbir = _download_report(client, other_ws, other, folder, say)
        except FabricError as error:
            fetched.skipped.append(f"{other['displayName']} ({other_ws['displayName']}): {error}")
            logger.warning(f"Report {other['displayName']} on the model could not be downloaded: {error}")
            continue
        _bind_to_local_model(folder, other_pbir, model_folder)
        _write_source(folder, other_ws, other, model_ws, model)
        fetched.report_folders.append(folder)
    logger.debug(
        f"Fetched {len(fetched.report_folders)} report(s) and {model['displayName']} to {destination}"
    )
    return fetched
