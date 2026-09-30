"""Tests for downloading reports from Fabric (fabric.py) against a fake Fabric API."""

import base64
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from pbixtractor.cli import main
from pbixtractor.fabric import (
    FabricClient,
    FabricError,
    fetch_report,
    model_reference,
    split_fabric_path,
    write_parts,
)

from .sample_pbir import write_sample_pbip
from .sample_tmdl import SAMPLE_TMDL

WS_SALES = "11111111-1111-1111-1111-111111111111"
WS_MODELS = "22222222-2222-2222-2222-222222222222"
REPORT_ID = "33333333-3333-3333-3333-333333333333"
MODEL_ID = "44444444-4444-4444-4444-444444444444"


class _Token:
    token = "fake-token"
    expires_on = time.time() + 3600


class _Credential:
    def get_token(self, *scopes, **kwargs):
        return _Token()


def _part(path: str, content: bytes) -> dict:
    return {"path": path, "payload": base64.b64encode(content).decode(), "payloadType": "InlineBase64"}


def _report_parts(tmp_path: Path, model_workspace: str) -> list[dict]:
    write_sample_pbip(tmp_path / "src", "Sample")
    folder = tmp_path / "src" / "Sample.Report"
    parts = [_part(p.relative_to(folder).as_posix(), p.read_bytes()) for p in folder.rglob("*") if p.is_file()]
    pbir = {
        "version": "4.0",
        "datasetReference": {
            "byConnection": {
                "connectionString": f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{model_workspace};"
                f"Initial Catalog=Sample Model;semanticmodelid={MODEL_ID}"
            }
        },
    }
    return parts + [_part("definition.pbir", json.dumps(pbir).encode())]


def _model_parts() -> list[dict]:
    parts = [_part(f"definition/{name}", text.encode()) for name, text in SAMPLE_TMDL.items()]
    return parts + [_part("definition.pbism", b'{"version": "4.0"}')]


class FakeFabric(FabricClient):
    """Answers the API calls fetch_report makes; getDefinition is a long-running operation."""

    def __init__(self, report_parts, model_workspace=WS_SALES):
        super().__init__(credential=_Credential(), sleep=lambda seconds: None)
        self.report_parts = report_parts
        self.model_workspace = model_workspace
        self.calls = []
        self.polls = 0

    def request(self, method, url, body=None):
        self.calls.append((method, url))
        if url == "workspaces":
            # paged: the second page comes from continuationUri
            return 200, {}, {
                "value": [{"id": WS_SALES, "displayName": "Sales WS"}],
                "continuationUri": "workspaces?page=2",
            }
        if url == "workspaces?page=2":
            return 200, {}, {"value": [{"id": WS_MODELS, "displayName": "Shared Models"}]}
        if url == f"workspaces/{WS_SALES}/reports":
            return 200, {}, {"value": [{"id": REPORT_ID, "displayName": "Sample"}]}
        if url.endswith("/semanticModels"):
            models = [{"id": MODEL_ID, "displayName": "Sample Model"}]
            return 200, {}, {"value": models if self.model_workspace in url else []}
        if url.endswith("/getDefinition") or "/getDefinition?" in url:
            kind = "report" if "/reports/" in url else "model"
            return 202, {"x-ms-operation-id": kind, "Retry-After": "1",
                         "Location": f"https://fake/operations/{kind}"}, None
        if url.startswith("https://fake/operations/") and not url.endswith("/result"):
            self.polls += 1
            if self.polls % 2:  # first poll: still running
                return 200, {}, {"status": "Running"}
            return 200, {"Location": f"{url}/result"}, {"status": "Succeeded"}
        if url == "https://fake/operations/report/result":
            return 200, {}, {"definition": {"parts": self.report_parts}}
        if url == "https://fake/operations/model/result":
            return 200, {}, {"definition": {"parts": _model_parts()}}
        raise AssertionError(f"unexpected call {method} {url}")


def test_split_fabric_path():
    assert split_fabric_path("Sales WS/Sample") == ("Sales WS", "Sample")
    assert split_fabric_path("A/B/Report") == ("A/B", "Report")
    with pytest.raises(ValueError):
        split_fabric_path("no-slash")


def test_model_reference():
    pbir = {"datasetReference": {"byConnection": {
        "connectionString": "Data Source=powerbi://api.powerbi.com/v1.0/myorg/My%20WS;"
                            f"Initial Catalog=M;semanticmodelid={MODEL_ID}"}}}
    assert model_reference(pbir) == (MODEL_ID, "My WS")
    legacy = {"datasetReference": {"byConnection": {"pbiModelDatabaseName": MODEL_ID}}}
    assert model_reference(legacy) == (MODEL_ID, None)
    assert model_reference({}) == (None, None)


def test_write_parts_rejects_paths_outside_the_folder(tmp_path):
    with pytest.raises(FabricError, match="outside"):
        write_parts([_part("../evil.txt", b"x")], tmp_path / "Item.Report")


def test_fetch_report_writes_a_pbip_project(tmp_path):
    client = FakeFabric(_report_parts(tmp_path, "Sales WS"))
    steps = []
    fetched = fetch_report(client, "sales ws", "Sample", tmp_path / "out", progress=steps.append)

    assert fetched.report_folder == tmp_path / "out" / "Sample.Report"
    assert fetched.model_path == tmp_path / "out" / "Sample Model.SemanticModel"
    assert (fetched.model_path / "definition" / "model.tmdl").is_file()
    pbir = json.loads((fetched.report_folder / "definition.pbir").read_text(encoding="utf-8"))
    assert pbir["datasetReference"] == {"byPath": {"path": "../Sample Model.SemanticModel"}}
    source = json.loads((tmp_path / "out" / "fabric_source.json").read_text(encoding="utf-8"))
    assert source["semantic_model"]["id"] == MODEL_ID
    assert client.polls == 4  # two polls per getDefinition
    assert any("TMDL" in url for _, url in client.calls)
    assert steps[0].startswith("Finding workspace")


def test_model_in_another_workspace(tmp_path):
    client = FakeFabric(_report_parts(tmp_path, "Shared Models"), model_workspace=WS_MODELS)
    fetched = fetch_report(client, WS_SALES, REPORT_ID, tmp_path / "out")  # ids work too
    assert fetched.model == "Sample Model"
    assert ("POST", f"workspaces/{WS_MODELS}/semanticModels/{MODEL_ID}/getDefinition?format=TMDL") in client.calls


def test_unknown_report_lists_what_exists(tmp_path):
    client = FakeFabric(_report_parts(tmp_path, "Sales WS"))
    with pytest.raises(FabricError, match="No report 'Nope'. Available: Sample"):
        fetch_report(client, "Sales WS", "Nope", tmp_path / "out")


def test_cli_extract_from_fabric(tmp_path, monkeypatch, capsys):
    client = FakeFabric(_report_parts(tmp_path, "Sales WS"))
    monkeypatch.setattr("pbixtractor.cli._fabric_client", lambda args: client)
    output = tmp_path / "doc"
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", "--fabric", "Sales WS/Sample", "-o", str(output), "--no-tabular-editor", "-q"])
    assert exit_info.value.code == 0, capsys.readouterr().err
    assert (output / "Sample.xlsx").is_file()
    assert (output / "source" / "Sample.Report" / "definition.pbir").is_file()


def test_cli_extract_needs_report_or_fabric(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["extract"])
    assert exit_info.value.code == 2
    assert "--fabric" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# The HTTP layer, against a local server
# ---------------------------------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 (http.server API)
        if self.path == "/v1/workspaces":
            assert self.headers["Authorization"] == "Bearer fake-token"
            body = json.dumps({"value": [{"id": WS_SALES, "displayName": "Sales WS"}]}).encode()
            self.send_response(200)
        else:
            body = json.dumps({"errorCode": "InsufficientPrivileges", "message": "Nope"}).encode()
            self.send_response(403)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def server():
    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_port}/v1"
    httpd.shutdown()


def test_http_requests_and_errors(server):
    client = FabricClient(_Credential(), api=server)
    assert client.workspaces() == [{"id": WS_SALES, "displayName": "Sales WS"}]
    with pytest.raises(FabricError, match="HTTP 403.*InsufficientPrivileges: Nope.*Contributor"):
        client.reports(WS_SALES)
