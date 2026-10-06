"""Tests for downloading reports from Fabric (fabric.py) against a fake Fabric API."""

import base64
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from pbixtractor.azure_auth import error_text
from pbixtractor.cli import main
from pbixtractor.fabric import (
    FabricClient,
    FabricError,
    fetch_report,
    model_reference,
    split_fabric_path,
    write_parts,
)
from pbixtractor.service_stats import ServiceModel, read_service_statistics, row_count_query

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


OTHER_REPORT_ID = "55555555-5555-5555-5555-555555555555"

# executeQueries answers for the sample model (plain DAX only: the API rejects INFO/DMV)
SERVICE_ROWS = {
    "COUNTROWS(": [{"Table": "Sales", "Rows": 1000}, {"Table": "Dates", "Rows": 365}],
    "COLUMNSTATISTICS(": [
        {"Table Name": "Sales", "Column Name": "Amount", "Cardinality": 321},
        {"Table Name": "Sales", "Column Name": "RowNumber-2662979B", "Cardinality": 1000},
    ],
}


class FakeFabric(FabricClient):
    """Answers the API calls fetch_report makes; getDefinition is a long-running operation."""

    def __init__(self, report_parts, model_workspace=WS_SALES, other_report_parts=None):
        super().__init__(credential=_Credential(), sleep=lambda seconds: None)
        self.report_parts = report_parts
        self.other_report_parts = other_report_parts  # a second report on the same model
        self.model_workspace = model_workspace
        self.calls = []
        self.polls = 0
        self.queries = []

    def powerbi_reports(self, workspace_id):
        self.calls.append(("GET", f"powerbi/{workspace_id}/reports"))
        if workspace_id != WS_SALES:
            return []
        reports = [{"id": REPORT_ID, "name": "Sample", "datasetId": MODEL_ID},
                   {"id": "66666666-6666-6666-6666-666666666666", "name": "Other model",
                    "datasetId": "77777777-7777-7777-7777-777777777777"}]
        if self.other_report_parts:
            reports.append({"id": OTHER_REPORT_ID, "name": "Sample Detail", "datasetId": MODEL_ID})
        return reports

    def execute_query(self, workspace_id, model_id, dax):
        self.queries.append((workspace_id, model_id, dax))
        if "INFO." in dax:
            raise FabricError("HTTP 400: DatasetExecuteQueriesError: INFO functions are not supported")
        for marker, rows in SERVICE_ROWS.items():
            if marker in dax:
                return rows
        raise FabricError(f"unexpected query {dax}")

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
        if url == f"workspaces/{WS_SALES}/reports/{OTHER_REPORT_ID}/getDefinition":
            if self.other_report_parts == "fail":
                raise FabricError("HTTP 403: InsufficientPrivileges")
            return 200, {}, {"definition": {"parts": self.other_report_parts}}
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
    source = json.loads((tmp_path / "out" / "Sample.fabric_source.json").read_text(encoding="utf-8"))
    assert source["semantic_model"]["id"] == MODEL_ID
    assert source["semantic_model"]["workspace_id"] == WS_SALES
    assert (fetched.model_id, fetched.model_workspace_id) == (MODEL_ID, WS_SALES)
    assert fetched.report_folders == [fetched.report_folder]
    assert client.polls == 4  # two polls per getDefinition
    assert any("TMDL" in url for _, url in client.calls)
    assert steps[0].startswith("Finding workspace")
    # While Fabric prepares a definition, the waiting time is reported
    assert any(s.startswith("Report Sample: Fabric is preparing it (") for s in steps)
    assert any(s.startswith("Semantic model Sample Model: Fabric is preparing it (") for s in steps)


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


def test_fetch_all_reports_on_the_model(tmp_path):
    client = FakeFabric(
        _report_parts(tmp_path / "a", "Sales WS"),
        other_report_parts=_report_parts(tmp_path / "b", "Sales WS"),
    )
    fetched = fetch_report(client, "Sales WS", "Sample", tmp_path / "out", all_reports=True)
    assert [f.name for f in fetched.report_folders] == ["Sample.Report", "Sample Detail.Report"]
    detail = fetched.report_folders[1]
    pbir = json.loads((detail / "definition.pbir").read_text(encoding="utf-8"))
    assert pbir["datasetReference"] == {"byPath": {"path": "../Sample Model.SemanticModel"}}
    assert (tmp_path / "out" / "Sample Detail.fabric_source.json").is_file()
    # Only the report's (= the model's) workspace is searched by default
    assert [u for _, u in client.calls if u.startswith("powerbi/")] == [f"powerbi/{WS_SALES}/reports"]


def test_report_that_cannot_be_downloaded_is_skipped(tmp_path):
    client = FakeFabric(_report_parts(tmp_path, "Sales WS"), other_report_parts="fail")
    fetched = fetch_report(
        client, "Sales WS", "Sample", tmp_path / "out", all_reports=True, all_workspaces=True
    )
    assert len(fetched.report_folders) == 1
    assert fetched.skipped[0].startswith("Sample Detail (Sales WS): HTTP 403")
    assert f"powerbi/{WS_MODELS}/reports" in [u for _, u in client.calls]  # every workspace


def test_workspace_that_cannot_be_searched_is_reported(tmp_path):
    client = FakeFabric(_report_parts(tmp_path, "Sales WS"))

    def refuse(workspace_id):
        raise FabricError("HTTP 401: TokenExpired")

    client.powerbi_reports = refuse
    fetched = fetch_report(client, "Sales WS", "Sample", tmp_path / "out", all_reports=True)
    assert fetched.skipped == ["workspace Sales WS could not be searched: HTTP 401: TokenExpired"]


def test_service_statistics(tmp_path):
    client = FakeFabric([])
    stats = read_service_statistics(ServiceModel(WS_SALES, MODEL_ID), ["Sales", "Dates"], client=client)
    assert stats.table_rows == {"Sales": 1000, "Dates": 365}
    assert stats.columns[("Sales", "Amount")].distinct_values == 321
    assert ("Sales", "RowNumber-2662979B") not in stats.columns
    assert not stats.has_sizes and stats.measure_types == {}  # not available from the service
    assert stats.tables == {"Sales", "Dates"} and stats.errors == {}
    assert all(q[:2] == (WS_SALES, MODEL_ID) for q in client.queries)
    assert not any("INFO." in q[2] for q in client.queries)


def test_row_count_query_escapes_names():
    assert row_count_query(["Sales"]) == 'EVALUATE ROW("Table", "Sales", "Rows", COUNTROWS(\'Sales\'))'
    query = row_count_query(["Bob's \"Sales\"", "Dates"])
    assert query.startswith("EVALUATE UNION(")
    assert "COUNTROWS('Bob''s \"Sales\"')" in query and '"Bob\'s ""Sales"""' in query


def test_power_bi_error_details_are_shown():
    body = json.dumps({"error": {"code": "DatasetExecuteQueriesError", "pbi.error": {"details": [
        {"code": "DetailsMessage", "detail": {"type": 1, "value": "Query (1, 10) Failed to resolve name"}}
    ]}}}).encode()
    text = error_text(400, "https://api.powerbi.com/v1.0/myorg/groups/x/executeQueries?a=1", body)
    assert text.startswith("HTTP 400: DatasetExecuteQueriesError: Query (1, 10) Failed to resolve")
    assert text.endswith("[https://api.powerbi.com/v1.0/myorg/groups/x/executeQueries]")


def test_execute_query_parses_rows_and_errors():
    class PowerBI:
        def __init__(self, body):
            self.body = body

        def request(self, method, url, body=None, raw=False):
            assert url.endswith(f"groups/{WS_SALES}/datasets/{MODEL_ID}/executeQueries")
            assert body["queries"][0]["query"] == "EVALUATE x"
            return 200, {}, self.body

    client = FabricClient(_Credential())
    client._powerbi = PowerBI({"results": [{"tables": [{"rows": [{"[Rows]": 5, "Sales[Amount]": 1}]}]}]})
    assert client.execute_query(WS_SALES, MODEL_ID, "EVALUATE x") == [{"Rows": 5, "Amount": 1}]
    client._powerbi = PowerBI({"results": [{"error": {"message": "INFO needs write permission"}}]})
    with pytest.raises(FabricError, match="INFO needs write permission"):
        client.execute_query(WS_SALES, MODEL_ID, "EVALUATE x")


def test_cli_extract_from_fabric_reads_service_statistics(tmp_path, monkeypatch, capsys):
    client = FakeFabric(
        _report_parts(tmp_path / "a", "Sales WS"),
        other_report_parts=_report_parts(tmp_path / "b", "Sales WS"),
    )
    monkeypatch.setattr("pbixtractor.cli._fabric_client", lambda args: client)
    monkeypatch.setattr("pbixtractor.service_stats.make_client", lambda: client)
    output = tmp_path / "doc"
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", "--fabric", "Sales WS/Sample", "--all-reports", "-o", str(output),
              "--no-tabular-editor", "-q"])
    assert exit_info.value.code == 0, capsys.readouterr().err
    document = json.loads((output / "Sample Model.json").read_text(encoding="utf-8"))
    sales = next(t for t in document["model"]["tables"] if t["name"] == "Sales")
    assert sales["rows"] == 1000  # from the service
    pages = [page["name"] for page in document["report"]["pages"]]
    assert "Sample › Sales" in pages and "Sample Detail › Sales" in pages


def test_cli_extract_needs_report_or_fabric(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["extract"])
    assert exit_info.value.code == 2
    assert "--fabric" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# The HTTP layer, against a local server
# ---------------------------------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    calls: dict = {}

    def _send(self, status, body, content_type="application/json", **headers):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        for name, value in headers.items():
            self.send_header(name.replace("_", "-"), value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 (http.server API)
        _Handler.calls[self.path] = _Handler.calls.get(self.path, 0) + 1
        if self.path == "/v1/flaky":  # 503 once, then fine
            if _Handler.calls[self.path] == 1:
                return self._send(503, b"{}", Retry_After="Wed, 21 Oct 2015 07:28:00 GMT")
            return self._send(200, b'{"ok": true}')
        if self.path == "/v1/signin":
            return self._send(203, b"<html>Sign in</html>", "text/html")
        if self.path == "/v1/redirect":  # to another host (localhost vs 127.0.0.1)
            port = self.server.server_port
            return self._send(302, b"", Location=f"http://localhost:{port}/v1/landing")
        if self.path == "/v1/landing":
            _Handler.calls["landing_auth"] = self.headers.get("Authorization")
            return self._send(200, b"{}")
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


def test_http_retries_sign_in_pages_and_redirects(server):
    waits = []
    client = FabricClient(_Credential(), api=server, sleep=waits.append)
    # A 503 is retried; its Retry-After is an HTTP date in the past -> no long wait
    assert client.request("GET", "flaky")[2] == {"ok": True}
    assert waits == [0]
    # An HTML sign-in page (DevOps answers an expired token so) is an error, also for raw
    with pytest.raises(FabricError, match="sign-in page"):
        client.request("GET", "signin", raw=True)
    # The token is never carried to another host on a redirect
    client.request("GET", "redirect")
    assert _Handler.calls["landing_auth"] is None


def test_credentials_are_never_sent_over_plain_http():
    client = FabricClient(_Credential(), api="http://api.example.com/v1")
    with pytest.raises(FabricError, match="Refusing to send credentials over http"):
        client.request("GET", "workspaces")


def test_sign_in_errors_become_api_errors():
    class Failing:
        def get_token(self, *scopes):
            raise RuntimeError("User cancelled the login")

    with pytest.raises(FabricError, match="Sign-in failed: User cancelled"):
        FabricClient(Failing(), api="https://api.fabric.microsoft.com/v1").request("GET", "x")


# Imported at collection time: conftest replaces azure_auth.default_credential during tests
from pbixtractor.azure_auth import default_credential as _real_default_credential  # noqa: E402


def test_a_service_principal_in_the_environment_never_opens_a_browser(monkeypatch):
    from azure.identity import EnvironmentCredential

    from pbixtractor import azure_auth

    for name in ("AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_CLIENT_SECRET"):
        monkeypatch.setenv(name, "x" if name != "AZURE_TENANT_ID" else "00000000-0000-0000-0000-000000000000")
    assert azure_auth.service_principal_configured()
    assert isinstance(_real_default_credential(), EnvironmentCredential)  # no browser fallback

    monkeypatch.delenv("AZURE_CLIENT_SECRET")
    assert not azure_auth.service_principal_configured()  # id without a secret: not enough


def test_a_published_model_is_found_by_id_and_downloaded(tmp_path):
    """A live-connected report only knows its model's id: search the workspaces, download it."""
    from pbixtractor.fabric import fetch_connected_model
    from pbixtractor.semantic_model import read_model

    client = FakeFabric(_report_parts(tmp_path, "Shared Models"), model_workspace=WS_MODELS)
    steps = []
    fetched = fetch_connected_model(client, MODEL_ID, None, tmp_path / "_fabric", steps.append)
    assert fetched.model_path == tmp_path / "_fabric" / "Shared Models" / "Sample Model.SemanticModel"
    assert (fetched.workspace_id, fetched.model_id, fetched.model) == (WS_MODELS, MODEL_ID, "Sample Model")
    assert read_model(fetched.model_path).tables
    assert any("Shared Models" in step for step in steps)

    client.calls.clear()  # a named workspace (from the connection string) is searched first
    fetch_connected_model(client, MODEL_ID, "Shared Models", tmp_path / "again")
    assert [u for _, u in client.calls if u.endswith("/semanticModels")][0] == (
        f"workspaces/{WS_MODELS}/semanticModels"
    )
    with pytest.raises(FabricError, match="not in any workspace you can open"):
        fetch_connected_model(client, "99999999-9999-9999-9999-999999999999", None, tmp_path / "x")


def test_cli_documents_a_live_connected_pbix_with_its_published_model(tmp_path, monkeypatch, capsys):
    import zipfile

    from .sample_layout import write_sample_pbix

    report = tmp_path / "Thin.pbix"
    write_sample_pbix(report)
    with zipfile.ZipFile(report, "a") as archive:
        archive.writestr("Connections", json.dumps({"Version": 3, "Connections": [{
            "ConnectionType": "pbiServiceLive", "PbiModelDatabaseName": MODEL_ID,
            "ConnectionString": "Data Source=pbiazure://api.powerbi.com;Initial Catalog=x"}]}))
    client = FakeFabric(_report_parts(tmp_path, "Shared Models"), model_workspace=WS_MODELS)
    monkeypatch.setattr("pbixtractor.cli._fabric_client", lambda args: client)
    monkeypatch.setattr("pbixtractor.service_stats.make_client", lambda: client)
    output = tmp_path / "doc"
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", str(report), "-o", str(output), "--no-tabular-editor", "-q"])
    assert exit_info.value.code == 0, capsys.readouterr().err
    assert (output / "source" / "_fabric" / "Shared Models" / "Sample Model.SemanticModel").is_dir()
    document = json.loads((output / "Thin.json").read_text(encoding="utf-8"))
    sales = next(t for t in document["model"]["tables"] if t["name"] == "Sales")
    assert sales["rows"] == 1000  # the published model's row counts, from the service
