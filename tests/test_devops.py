"""Tests for reading PBIP reports from Azure DevOps (devops.py) against a fake DevOps API."""

import io
import json
import urllib.parse
import zipfile
from pathlib import Path

import pytest

from pbixtractor.azure_auth import ApiError
from pbixtractor.cli import main
from pbixtractor.devops import (
    DevOpsClient,
    extract_folder,
    fetch_model_reports,
    fetch_report,
    normalize_org,
    parse_devops_url,
    parse_version,
    report_folder_path,
    safe_repo_path,
)

from .sample_pbir import write_sample_pbip
from .sample_tmdl import SAMPLE_TMDL


def _repo_files(tmp_path: Path, bound_by_path: bool = True) -> dict[str, bytes]:
    """Repository content: /Reports/Sample.Report + /Reports/Sample.SemanticModel."""
    write_sample_pbip(tmp_path / "src", "Sample")
    report = tmp_path / "src" / "Sample.Report"
    files = {
        f"/Reports/Sample.Report/{p.relative_to(report).as_posix()}": p.read_bytes()
        for p in report.rglob("*")
        if p.is_file()
    }
    reference = (
        {"byPath": {"path": "../Sample.SemanticModel"}}
        if bound_by_path
        else {"byConnection": {"connectionString": "Data Source=powerbi://..."}}
    )
    files["/Reports/Sample.Report/definition.pbir"] = json.dumps(
        {"version": "4.0", "datasetReference": reference}
    ).encode()
    for name, text in SAMPLE_TMDL.items():
        files[f"/Reports/Sample.SemanticModel/definition/{name}"] = text.encode()
    files["/README.md"] = b"# repo"
    return files


def _with_sibling_reports(files: dict[str, bytes]) -> dict[str, bytes]:
    """Add a second report on the same model and one on another model."""
    extra = {}
    for path, content in files.items():
        if path.startswith("/Reports/Sample.Report/"):
            extra[path.replace("/Reports/Sample.Report/", "/Detail/Sample Detail.Report/")] = content
            extra[path.replace("/Reports/Sample.Report/", "/Other/Other.Report/")] = content
    extra["/Detail/Sample Detail.Report/definition.pbir"] = json.dumps(
        {"datasetReference": {"byPath": {"path": "../../Reports/Sample.SemanticModel"}}}
    ).encode()
    extra["/Other/Other.Report/definition.pbir"] = json.dumps(
        {"datasetReference": {"byPath": {"path": "../Other.SemanticModel"}}}
    ).encode()
    return {**files, **extra}


class FakeDevOps(DevOpsClient):
    """Answers the calls the client makes; folder downloads are zips like DevOps returns."""

    def __init__(self, files: dict[str, bytes], prefix_folder: bool = True):
        super().__init__("https://dev.azure.com/contoso", credential=object(), pat="unused")
        self.repo_files = files
        self.prefix_folder = prefix_folder
        self.calls = []

    def request(self, method, url, body=None, raw=False):
        parts = urllib.parse.urlsplit(url)
        query = {k: v[0] for k, v in urllib.parse.parse_qs(parts.query).items()}
        path = urllib.parse.unquote(parts.path)
        self.calls.append((path, query))
        assert query["api-version"] == "7.1"
        if path == "/contoso/_apis/projects":
            return 200, {}, {"value": [{"name": "Finance"}, {"name": "BI Team"}]}
        if path == "/contoso/BI Team/_apis/git/repositories":
            return 200, {}, {"value": [{"id": "r1", "name": "reports", "defaultBranch": "refs/heads/main"}]}
        if path.endswith("/refs"):
            return 200, {}, {"value": [{"name": "refs/heads/main"}, {"name": "refs/heads/dev"}]}
        if path.endswith("/commits"):
            assert query["searchCriteria.itemPath"] == "/Reports/Sample.Report"
            return 200, {}, {"value": [{"commitId": "a1b2c3d4e5f6", "comment": "Fix KPI\n\nmore",
                                        "author": {"name": "Ada", "date": "2026-09-01T10:00:00Z"}}]}
        if path.endswith("/items") and query.get("$format") == "zip":
            folder = query["path"]
            name = folder.rsplit("/", 1)[-1]
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w") as archive:
                for file, content in self.repo_files.items():
                    if file.startswith(folder + "/"):
                        relative = file[len(folder) + 1 :]
                        archive.writestr(f"{name}/{relative}" if self.prefix_folder else relative, content)
            return 200, {}, buffer.getvalue()
        if path.endswith("/items") and "path" in query:  # one file
            return 200, {}, self.repo_files[query["path"]]
        if path.endswith("/items"):
            return 200, {}, {"value": [{"path": f, "isFolder": False} for f in self.repo_files]
                                      + [{"path": "/Reports", "isFolder": True}]}
        raise AssertionError(f"unexpected call {method} {url}")


@pytest.mark.parametrize(
    "value, expected",
    [
        ("contoso", "https://dev.azure.com/contoso"),
        ("https://dev.azure.com/contoso/BI%20Team/_git/reports", "https://dev.azure.com/contoso"),
        ("https://contoso.visualstudio.com/", "https://contoso.visualstudio.com"),
    ],
)
def test_normalize_org(value, expected):
    assert normalize_org(value) == expected


def test_parse_devops_url():
    location = parse_devops_url(
        "https://dev.azure.com/contoso/BI%20Team/_git/reports"
        "?path=/Reports/Sample.Report/definition/report.json&version=GCa1b2c3d&_a=contents"
    )
    assert (location.org, location.project, location.repo) == (
        "https://dev.azure.com/contoso", "BI Team", "reports")
    assert location.path == "/Reports/Sample.Report"
    assert (location.version, location.version_type) == ("a1b2c3d", "commit")

    old_style = parse_devops_url("https://contoso.visualstudio.com/Proj/_git/Repo?path=/Sales.pbip")
    assert (old_style.org, old_style.path, old_style.version) == (
        "https://contoso.visualstudio.com", "/Sales.Report", "")
    with pytest.raises(ValueError, match="_git"):
        parse_devops_url("https://dev.azure.com/contoso/Proj")
    with pytest.raises(ValueError, match="PBIP"):
        report_folder_path("/Reports/readme.md")


def test_parse_short_devops_url_without_project():
    location = parse_devops_url("https://dev.azure.com/contoso/_git/Reports?path=/Sales.Report")
    assert (location.org, location.project, location.repo) == (
        "https://dev.azure.com/contoso", "Reports", "Reports")
    old_style = parse_devops_url("https://contoso.visualstudio.com/_git/Reports?path=/Sales.Report")
    assert (old_style.project, old_style.repo) == ("Reports", "Reports")


@pytest.mark.parametrize(
    "org",
    ["http://contoso.visualstudio.com", "https://evil.example/contoso", "https://dev.azure.com.evil.example"],
)
def test_tokens_are_only_sent_to_azure_devops(org):
    with pytest.raises(ApiError, match="Not an Azure DevOps address"):
        DevOpsClient(org, credential=object())
    # dev.azure.com links are always rewritten to https
    assert DevOpsClient("http://dev.azure.com/contoso", credential=object()).org.startswith("https://")


def test_extra_devops_hosts_can_be_allowed(monkeypatch):
    monkeypatch.setenv("PBIXTRACTOR_DEVOPS_HOSTS", "tfs.contoso.local")
    assert DevOpsClient("https://tfs.contoso.local", credential=object()).org == "https://tfs.contoso.local"


def test_parse_version():
    assert parse_version("main") == ("main", "branch")
    assert parse_version("tag:v1.0") == ("v1.0", "tag")
    assert parse_version("commit:a1b2") == ("a1b2", "commit")
    assert parse_version("feature:x") == ("feature:x", "branch")


def test_listing(tmp_path):
    client = FakeDevOps(_repo_files(tmp_path))
    assert client.projects() == ["BI Team", "Finance"]
    assert client.repositories("BI Team")[0]["defaultBranch"] == "main"
    assert client.branches("BI Team", "reports") == ["dev", "main"]
    assert client.find_reports("BI Team", "reports", "main") == ["/Reports/Sample.Report"]
    commits = client.commits("BI Team", "reports", "/Reports/Sample.Report", "main")
    assert commits == [{"id": "a1b2c3d4e5f6", "short": "a1b2c3d4", "comment": "Fix KPI",
                        "author": "Ada", "date": "2026-09-01"}]


@pytest.mark.parametrize("prefix_folder", [True, False])
def test_fetch_report_at_a_commit(tmp_path, prefix_folder):
    client = FakeDevOps(_repo_files(tmp_path), prefix_folder=prefix_folder)
    fetched = fetch_report(
        client, "BI Team", "reports", "/Reports/Sample.pbip", tmp_path / "out",
        version="a1b2c3d", version_type="commit",
    )
    assert fetched.report_folder == (tmp_path / "out" / "Reports" / "Sample.Report").resolve()
    assert (fetched.model_path / "definition" / "model.tmdl").is_file()
    assert (fetched.report_folder / "definition" / "pages" / "pages.json").is_file()
    zips = [q for p, q in client.calls if q.get("$format") == "zip"]
    assert all(q["versionDescriptor.version"] == "a1b2c3d" for q in zips)
    assert all(q["versionDescriptor.versionType"] == "commit" for q in zips)
    source = json.loads((fetched.report_folder.parent / "Sample.devops_source.json").read_text())
    assert source["semantic_model"] == "/Reports/Sample.SemanticModel"


def test_fetch_uses_default_branch(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = FakeDevOps(_repo_files(tmp_path))
    fetched = fetch_report(client, "BI Team", "reports", "/Reports/Sample.Report")
    assert fetched.version == "main"
    assert Path("output/_devops/BI Team/reports/main/Reports/Sample.Report").is_dir()


def test_fetch_all_reports_on_the_model(tmp_path):
    client = FakeDevOps(_with_sibling_reports(_repo_files(tmp_path)))
    fetched = fetch_report(
        client, "BI Team", "reports", "/Reports/Sample.Report", tmp_path / "out", "main",
        all_reports=True,
    )
    names = [folder.name for folder in fetched.report_folders]
    assert names == ["Sample.Report", "Sample Detail.Report"]  # Other.Report uses another model
    assert (tmp_path / "out" / "Detail" / "Sample Detail.Report" / "definition.pbir").is_file()
    assert fetched.skipped == []


def test_cli_extract_all_reports_from_devops(tmp_path, monkeypatch, capsys):
    client = FakeDevOps(_with_sibling_reports(_repo_files(tmp_path)))
    monkeypatch.setattr("pbixtractor.cli._devops_client", lambda args, org: client)
    output = tmp_path / "doc"
    url = "https://dev.azure.com/contoso/BI%20Team/_git/reports?path=/Reports/Sample.Report"
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", "--devops", url, "--all-reports", "-o", str(output),
              "--no-tabular-editor", "-q"])
    assert exit_info.value.code == 0, capsys.readouterr().err
    document = json.loads((output / "Sample.json").read_text(encoding="utf-8"))  # model name
    pages = [page["name"] for page in document["report"]["pages"]]
    assert "Sample › Sales" in pages and "Sample Detail › Sales" in pages


def test_report_bound_to_service_model(tmp_path):
    client = FakeDevOps(_repo_files(tmp_path, bound_by_path=False))
    with pytest.raises(ApiError, match="Fabric workspace instead"):
        fetch_report(client, "BI Team", "reports", "/Reports/Sample.Report", tmp_path / "out", "main")


@pytest.mark.parametrize(
    "path", ["/a/../b", "..", "/a\\..\\b", "/C:/Windows", "/a//b", "/./a", ""]
)
def test_unsafe_repository_paths_are_refused(path):
    with pytest.raises(ApiError, match="Unsafe path"):
        safe_repo_path(path)


def test_malicious_model_reference_cannot_touch_other_folders(tmp_path):
    """A definition.pbir byPath with ..\\ segments must never delete or write outside the
    download folder (the audit reproduced wiping an arbitrary folder)."""
    victim = tmp_path / "victim"
    victim.mkdir()
    (victim / "keep.txt").write_text("precious")
    files = _repo_files(tmp_path)
    for reference in ("..\\..\\..\\victim", "../../../../victim"):
        files["/Reports/Sample.Report/definition.pbir"] = json.dumps(
            {"datasetReference": {"byPath": {"path": reference}}}
        ).encode()
        client = FakeDevOps(files)
        with pytest.raises(ApiError):
            fetch_report(client, "BI Team", "reports", "/Reports/Sample.Report", tmp_path / "out", "main")
        assert (victim / "keep.txt").read_text() == "precious"


def test_non_zip_download_gives_a_clear_error(tmp_path):
    with pytest.raises(ApiError, match="not a zip archive.*access token"):
        extract_folder(b"<html>Sign in</html>", "/Reports/Sample.Report", tmp_path)


def test_extract_folder_rejects_escaping_paths(tmp_path):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("../../evil.txt", b"x")
    with pytest.raises(ApiError, match="Unexpected path"):
        extract_folder(buffer.getvalue(), "/Reports/Sample.Report", tmp_path)


def test_cli_extract_from_devops(tmp_path, monkeypatch, capsys):
    client = FakeDevOps(_repo_files(tmp_path))
    monkeypatch.setattr("pbixtractor.cli._devops_client", lambda args, org: client)
    output = tmp_path / "doc"
    url = "https://dev.azure.com/contoso/BI%20Team/_git/reports?path=/Reports/Sample.Report&version=GBmain"
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", "--devops", url, "--ref", "tag:v1", "-o", str(output),
              "--no-tabular-editor", "-q"])
    assert exit_info.value.code == 0, capsys.readouterr().err
    assert (output / "Sample.xlsx").is_file()
    zips = [q for p, q in client.calls if q.get("$format") == "zip"]
    assert zips[0]["versionDescriptor.versionType"] == "tag"  # --ref wins over the URL


def test_cli_devops_list(tmp_path, monkeypatch, capsys):
    client = FakeDevOps(_repo_files(tmp_path))
    monkeypatch.setattr("pbixtractor.cli._devops_client", lambda args, org: client)
    with pytest.raises(SystemExit):
        main(["devops", "list", "contoso", "BI Team", "reports"])
    out = capsys.readouterr().out
    assert "Branches: dev, main" in out and "/Reports/Sample.Report" in out
    with pytest.raises(SystemExit):
        main(["devops", "list", "contoso", "BI Team", "reports", "--history", "/Reports/Sample.Report"])
    assert "a1b2c3d4  2026-09-01  Ada: Fix KPI" in capsys.readouterr().out


def _bound_to_service(files: dict, report: str, model_id: str) -> dict:
    files[f"{report}/definition.pbir"] = json.dumps({"version": "4.0", "datasetReference": {"byConnection": {
        "connectionString": "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Sales%20WS;"
                            f"semanticmodelid={model_id}"}}}).encode()
    return files


def test_a_report_on_a_published_model_gets_it_from_the_service(tmp_path):
    """No model in git: the connected_model hook (Fabric in the app) supplies it, and model mode
    finds the other reports on the same published model."""
    from pbixtractor.semantic_model import read_model

    model_id, other_model = "44444444-4444-4444-4444-444444444444", "99999999-9999-9999-9999-999999999999"
    files = _repo_files(tmp_path, bound_by_path=False)
    for path, content in list(files.items()):
        if path.startswith("/Reports/Sample.Report/"):
            files[path.replace("/Reports/Sample.Report/", "/Detail/Detail.Report/")] = content
            files[path.replace("/Reports/Sample.Report/", "/Other/Other.Report/")] = content
    for report, model in (("/Reports/Sample.Report", model_id), ("/Detail/Detail.Report", model_id),
                          ("/Other/Other.Report", other_model)):
        _bound_to_service(files, report, model)

    model_folder = tmp_path / "service" / "Sample Model.SemanticModel"
    asked = []

    def connected_model(pbir: dict) -> Path:
        asked.append(pbir)
        for name, text in SAMPLE_TMDL.items():
            target = model_folder / "definition" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        return model_folder

    fetched = fetch_report(FakeDevOps(files), "BI Team", "reports", "/Reports/Sample.Report",
                           tmp_path / "out", "main", all_reports=True, connected_model=connected_model)
    assert fetched.model_path == model_folder and len(asked) == 1
    assert sorted(f.name for f in fetched.report_folders) == ["Detail.Report", "Sample.Report"]
    assert read_model(fetched.model_path).tables


def test_parse_a_repository_url():
    whole = parse_devops_url("https://dev.azure.com/contoso/BI%20Team/_git/reports", report=False)
    assert (whole.project, whole.repo, whole.path, whole.version) == ("BI Team", "reports", "", "")
    folder = parse_devops_url(
        "https://dev.azure.com/contoso/BI%20Team/_git/reports?path=/Reports/Finance&version=GBdev",
        report=False,
    )
    assert (folder.path, folder.version, folder.version_type) == ("/Reports/Finance", "dev", "branch")
    with pytest.raises(ApiError, match="Unsafe"):
        parse_devops_url("https://dev.azure.com/c/P/_git/R?path=/../x", report=False)


def test_fetch_model_reports_downloads_the_reports_and_their_model(tmp_path):
    class Failing(FakeDevOps):
        def download_folder(self, project, repo, path, version="", version_type="branch"):
            if path == "/Other/Other.Report":
                raise ApiError("HTTP 404: not found")
            return super().download_folder(project, repo, path, version, version_type)

    client = Failing(_with_sibling_reports(_repo_files(tmp_path)))
    fetched = fetch_model_reports(
        client, "BI Team", "reports", "/Reports/Sample.SemanticModel",
        ["/Reports/Sample.Report", "/Detail/Sample Detail.Report", "/Other/Other.Report"],
        tmp_path / "dl", "main",
    )
    assert [f.name for f in fetched.report_folders] == ["Sample.Report", "Sample Detail.Report"]
    assert fetched.model_path == tmp_path / "dl" / "Reports" / "Sample.SemanticModel"
    assert fetched.skipped == ["/Other/Other.Report: HTTP 404: not found"]
    source = json.loads((tmp_path / "dl" / "Detail" / "Sample Detail.devops_source.json").read_text(encoding="utf-8"))
    assert source["semantic_model"] == "/Reports/Sample.SemanticModel" and source["version"] == "main"
    with pytest.raises(ApiError, match="published semantic model"):
        fetch_model_reports(client, "BI Team", "reports", "service:abc", ["/Reports/Sample.Report"],
                            tmp_path / "dl2", "main")
