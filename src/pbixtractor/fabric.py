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
import re
import shutil
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from .azure_auth import FABRIC_SCOPE, ApiError, RestClient

logger = logging.getLogger("pbixtractor")

API = "https://api.fabric.microsoft.com/v1"
_GUID = re.compile(r"^[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")

FabricError = ApiError  # kept as the name callers catch


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
        self, workspace_id: str, item_id: str, kind: str, format: Optional[str] = None
    ) -> list[dict]:
        """
        Download an item definition (long-running operation: polled until done).

        Args:
            workspace_id: Workspace id
            item_id: Report or semantic model id
            kind: "reports" or "semanticModels"
            format: Optional definition format (reports: PBIR / PBIR-Legacy; models: TMDL /
                TMSL)

        Returns:
            Definition parts: [{"path", "payload" (base64), "payloadType"}]
        """
        url = f"workspaces/{workspace_id}/{kind}/{item_id}/getDefinition"
        if format:
            url += f"?format={urllib.parse.quote(format)}"
        status, headers, body = self.request("POST", url)
        if status == 202:
            body = self._wait_for_operation(headers)
        return (body or {}).get("definition", {}).get("parts", [])

    def _wait_for_operation(self, headers: dict, timeout: float = 600) -> dict:
        headers = {k.lower(): v for k, v in headers.items()}
        operation = headers.get("x-ms-operation-id")
        location = headers.get("location") or f"{self._api}/operations/{operation}"
        deadline = time.monotonic() + timeout
        while True:
            self._sleep(float(headers.get("retry-after") or 2))
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
    report_folder: Path  # <report>.Report
    model_path: Path  # <model>.SemanticModel


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


def fetch_report(
    client: FabricClient,
    workspace: str,
    report: str,
    destination: Path,
    progress: Optional[Callable[[str], None]] = None,
) -> FetchedReport:
    """
    Download a report and its semantic model as a PBIP project.

    Args:
        client: FabricClient
        workspace: Workspace name or id
        report: Report name or id
        destination: Folder for <report>.Report and <model>.SemanticModel
        progress: Called with a short status text per step

    Returns:
        FetchedReport with the local report folder and model folder
    """
    say = progress or (lambda text: None)
    say(f"Finding workspace {workspace}")
    workspaces = client.workspaces()
    ws = _find(workspaces, workspace, "workspace")
    say(f"Finding report {report}")
    rep = _find(client.reports(ws["id"]), report, "report")

    say(f"Downloading report {rep['displayName']}")
    try:
        report_parts = client.get_definition(ws["id"], rep["id"], "reports")
    except FabricError as error:
        raise FabricError(
            f"{error} (Reports with an encrypting sensitivity label cannot be downloaded from "
            "Fabric; use a git-synced copy, e.g. Azure DevOps, instead.)"
        ) from None
    report_folder = Path(destination) / f"{safe_name(rep['displayName'])}.Report"
    write_parts(report_parts, report_folder)

    pbir_path = report_folder / "definition.pbir"
    pbir = json.loads(pbir_path.read_bytes().decode("utf-8-sig")) if pbir_path.is_file() else {}
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
    model_parts = client.get_definition(model_ws["id"], model["id"], "semanticModels", "TMDL")
    model_folder = Path(destination) / f"{safe_name(model['displayName'])}.SemanticModel"
    write_parts(model_parts, model_folder)

    # Point the report at the local model, as in a PBIP project saved by Power BI Desktop
    pbir["datasetReference"] = {"byPath": {"path": f"../{model_folder.name}"}}
    pbir_path.write_text(json.dumps(pbir, indent=2), encoding="utf-8")
    (Path(destination) / "fabric_source.json").write_text(
        json.dumps(
            {
                "workspace": {"id": ws["id"], "name": ws["displayName"]},
                "report": {"id": rep["id"], "name": rep["displayName"]},
                "semantic_model": {
                    "id": model["id"],
                    "name": model["displayName"],
                    "workspace": model_ws["displayName"],
                },
                "fetched": time.strftime("%Y-%m-%d %H:%M:%S"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    logger.debug(f"Fetched {rep['displayName']} and {model['displayName']} to {destination}")
    return FetchedReport(
        workspace=ws["displayName"],
        report=rep["displayName"],
        model=model["displayName"],
        report_folder=report_folder,
        model_path=model_folder,
    )
