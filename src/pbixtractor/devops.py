"""Read Power BI projects (PBIP) from an Azure DevOps git repository, at any version.

    client = DevOpsClient("myorg")                        # Entra sign-in (or AZURE_DEVOPS_PAT)
    client.find_reports("Project", "Repo", "main")        # ["/Reports/Sales.Report", ...]
    fetched = fetch_report(client, "Project", "Repo", "/Reports/Sales.Report",
                           Path("output/_devops/..."), version="a1b2c3d", version_type="commit")
    fetched.report_folder, fetched.model_path             # local PBIP folders

    location = parse_devops_url("https://dev.azure.com/org/Proj/_git/Repo?path=/Sales.Report&version=GBmain")

Only the report folder and the semantic model folder it references (definition.pbir byPath)
are downloaded (as zip archives), not the whole repository. Reports bound to a model in the
service (byConnection) have no model in git - use the Fabric source for those. Files in git
are never encrypted, so this also works for reports with an encrypting sensitivity label.

Sign-in: see azure_auth.py (same login as Fabric). A personal access token in the
AZURE_DEVOPS_PAT environment variable (scope Code: Read) is used instead when set.
"""

import io
import json
import os
import posixpath
import re
import shutil
import time
import urllib.parse
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from .azure_auth import DEVOPS_SCOPE, ApiError, RestClient

API_VERSION = "7.1"
VERSION_TYPES = ("branch", "tag", "commit")
_URL_VERSION_PREFIX = {"GB": "branch", "GT": "tag", "GC": "commit"}


def normalize_org(org: str) -> str:
    """ "myorg", "https://dev.azure.com/myorg/..." or "https://myorg.visualstudio.com" -> base URL."""
    org = org.strip().rstrip("/")
    if not org.startswith("http"):
        return f"https://dev.azure.com/{org}"
    parts = urllib.parse.urlsplit(org)
    if parts.netloc.lower() == "dev.azure.com":
        name = parts.path.strip("/").split("/")[0]
        return f"https://dev.azure.com/{name}"
    return f"{parts.scheme}://{parts.netloc}"


@dataclass
class DevOpsLocation:
    """A report in a repository: organisation, project, repo, report folder and version."""

    org: str
    project: str
    repo: str
    path: str  # report folder in the repo, e.g. "/Reports/Sales.Report"
    version: str = ""  # branch / tag / commit; "" = the repository's default branch
    version_type: str = "branch"


def report_folder_path(path: str) -> str:
    """The <name>.Report folder for a path to a .pbip file or to anything inside the folder."""
    path = "/" + path.strip("/")
    if path.lower().endswith(".pbip"):
        return path[: -len(".pbip")] + ".Report"
    parts = path.split("/")
    for index, part in enumerate(parts):
        if part.lower().endswith(".report"):
            return "/".join(parts[: index + 1])
    raise ValueError(f"Not a PBIP report path (expected a .pbip file or <name>.Report folder): {path}")


def parse_devops_url(url: str) -> DevOpsLocation:
    """
    Parse a report URL as copied from the Azure DevOps web UI.

    Args:
        url: e.g. https://dev.azure.com/org/Project/_git/Repo?path=/Sales.Report&version=GBmain
            (also https://org.visualstudio.com/Project/_git/Repo?...); version GB = branch,
            GT = tag, GC = commit

    Returns:
        DevOpsLocation (path normalised to the .Report folder)
    """
    parts = urllib.parse.urlsplit(url.strip())
    segments = [urllib.parse.unquote(s) for s in parts.path.strip("/").split("/")]
    if "_git" not in segments:
        raise ValueError(f"Not an Azure DevOps repository URL (no /_git/): {url}")
    git = segments.index("_git")
    if parts.netloc.lower() == "dev.azure.com":
        org, project = f"https://dev.azure.com/{segments[0]}", segments[git - 1]
    else:
        org, project = f"{parts.scheme}://{parts.netloc}", segments[git - 1]
    repo = segments[git + 1] if len(segments) > git + 1 else ""
    query = urllib.parse.parse_qs(parts.query)
    path = query.get("path", [""])[0]
    if not repo or not path:
        raise ValueError(f"The URL must point to a report inside a repository (?path=...): {url}")
    version, version_type = query.get("version", [""])[0], "branch"
    if version[:2] in _URL_VERSION_PREFIX:
        version_type, version = _URL_VERSION_PREFIX[version[:2]], version[2:]
    return DevOpsLocation(org, project, repo, report_folder_path(path), version, version_type)


def parse_version(value: str) -> tuple[str, str]:
    """ "main" -> branch, "tag:v1.2" -> tag, "commit:a1b2c3" -> commit."""
    kind, _, name = value.partition(":")
    if name and kind in VERSION_TYPES:
        return name, kind
    return value, "branch"


class DevOpsClient(RestClient):
    """Azure DevOps git REST calls needed to find and download PBIP reports."""

    def __init__(
        self,
        org: str,
        credential=None,
        pat: Optional[str] = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        """
        Args:
            org: Organisation name or URL
            credential: Token credential (default: azure_auth.get_credential())
            pat: Personal access token (default: AZURE_DEVOPS_PAT environment variable)
            sleep: Wait function (tests pass a no-op)
        """
        self.org = normalize_org(org)
        super().__init__(
            self.org,
            DEVOPS_SCOPE,
            credential=credential,
            pat=pat if pat is not None else os.environ.get("AZURE_DEVOPS_PAT") or None,
            sleep=sleep,
        )

    def _url(self, endpoint: str, **params) -> str:
        """URL of an API endpoint; "__" in parameter names becomes "." (versionDescriptor.x)."""
        params = {k.replace("__", "."): v for k, v in params.items() if v is not None}
        params["api-version"] = API_VERSION
        query = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        return f"{self.org}/{endpoint}?{query}"

    def _repo(self, project: str, repo: str) -> str:
        return f"{urllib.parse.quote(project)}/_apis/git/repositories/{urllib.parse.quote(repo)}"

    @staticmethod
    def _version(version: str, version_type: str) -> dict:
        if not version:
            return {}
        return {"versionDescriptor__version": version, "versionDescriptor__versionType": version_type}

    def projects(self) -> list[str]:
        names, token = [], None
        while True:
            _, headers, body = self.request(
                "GET", self._url("_apis/projects", **{"$top": 500, "continuationToken": token})
            )
            names += [p["name"] for p in body.get("value", [])]
            token = {k.lower(): v for k, v in headers.items()}.get("x-ms-continuationtoken")
            if not token:
                return sorted(names, key=str.lower)

    def repositories(self, project: str) -> list[dict]:
        """[{"name", "defaultBranch" ("main"), ...}] sorted by name."""
        _, _, body = self.request(
            "GET", self._url(f"{urllib.parse.quote(project)}/_apis/git/repositories")
        )
        repos = body.get("value", [])
        for repo in repos:
            repo["defaultBranch"] = (repo.get("defaultBranch") or "").removeprefix("refs/heads/")
        return sorted(repos, key=lambda r: r["name"].lower())

    def default_branch(self, project: str, repo: str) -> str:
        for item in self.repositories(project):
            if item["name"].lower() == repo.lower() or item.get("id") == repo:
                return item["defaultBranch"]
        raise ApiError(f"No repository '{repo}' in project '{project}'.")

    def branches(self, project: str, repo: str) -> list[str]:
        _, _, body = self.request("GET", self._url(f"{self._repo(project, repo)}/refs", filter="heads/"))
        return sorted(
            (r["name"].removeprefix("refs/heads/") for r in body.get("value", [])), key=str.lower
        )

    def files(self, project: str, repo: str, version: str = "", version_type: str = "branch") -> list[str]:
        """Every file path in the repository at a version."""
        _, _, body = self.request(
            "GET",
            self._url(
                f"{self._repo(project, repo)}/items",
                scopePath="/",
                recursionLevel="Full",
                **self._version(version, version_type),
            ),
        )
        return [i["path"] for i in body.get("value", []) if not i.get("isFolder")]

    def find_reports(
        self, project: str, repo: str, version: str = "", version_type: str = "branch"
    ) -> list[str]:
        """Report folders (<name>.Report with a definition.pbir) in the repository."""
        return sorted(
            {
                path.rsplit("/", 1)[0]
                for path in self.files(project, repo, version, version_type)
                if path.lower().endswith(".report/definition.pbir")
            },
            key=str.lower,
        )

    def commits(self, project: str, repo: str, path: str, branch: str = "", top: int = 30) -> list[dict]:
        """
        Recent commits that changed a path: [{"id", "short", "comment", "author", "date"}].
        """
        _, _, body = self.request(
            "GET",
            self._url(
                f"{self._repo(project, repo)}/commits",
                searchCriteria__itemPath=path,
                searchCriteria__itemVersion__version=branch or None,
                **{"searchCriteria.$top": top},
            ),
        )
        return [
            {
                "id": c["commitId"],
                "short": c["commitId"][:8],
                "comment": (c.get("comment") or "").splitlines()[0] if c.get("comment") else "",
                "author": (c.get("author") or {}).get("name", ""),
                "date": (c.get("author") or {}).get("date", "")[:10],
            }
            for c in body.get("value", [])
        ]

    def download_folder(
        self, project: str, repo: str, path: str, version: str = "", version_type: str = "branch"
    ) -> bytes:
        """A folder of the repository as a zip archive."""
        _, _, content = self.request(
            "GET",
            self._url(
                f"{self._repo(project, repo)}/items",
                path=path,
                download="true",
                **{"$format": "zip"},
                **self._version(version, version_type),
            ),
            raw=True,
        )
        return content


def extract_folder(archive: bytes, repo_path: str, destination: Path) -> Path:
    """
    Unpack a folder zip from DevOps to <destination>/<repo path>.

    The archive may hold paths relative to the folder or including it (and its parents); both
    are handled. Returns the local folder.
    """
    target = (Path(destination) / repo_path.strip("/")).resolve()
    name = repo_path.rstrip("/").rsplit("/", 1)[-1]
    if target.exists():
        shutil.rmtree(target)  # an earlier download: files deleted in git must not linger
    with zipfile.ZipFile(io.BytesIO(archive)) as zip_file:
        for entry in zip_file.infolist():
            if entry.is_dir():
                continue
            parts = entry.filename.replace("\\", "/").strip("/").split("/")
            if name in parts[:-1]:
                parts = parts[parts.index(name) + 1 :]
            local = (target / Path(*parts)).resolve()
            if target not in local.parents:
                raise ApiError(f"Unexpected path in the downloaded archive: {entry.filename}")
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(zip_file.read(entry))
    return target


@dataclass
class DevOpsFetched:
    project: str
    repo: str
    version: str  # branch / tag / commit that was read
    report_folder: Path  # local <name>.Report
    model_path: Path  # local <model>.SemanticModel


def safe_name(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]+', "_", name).strip(" .") or "item"


def fetch_report(
    client: DevOpsClient,
    project: str,
    repo: str,
    report_path: str,
    destination: Optional[Path] = None,
    version: str = "",
    version_type: str = "branch",
    progress: Optional[Callable[[str], None]] = None,
) -> DevOpsFetched:
    """
    Download a PBIP report folder and the semantic model it references.

    Args:
        client: DevOpsClient
        project, repo: Where the report lives
        report_path: .Report folder (or .pbip file / a file inside the folder)
        destination: Local root; repository paths are kept below it (so the report's relative
            reference to its model still works). Default: default_destination()
        version, version_type: Branch / tag / commit ("" = the default branch)
        progress: Called with a short status text per step

    Returns:
        DevOpsFetched with the local report and model folders
    """
    say = progress or (lambda text: None)
    report_path = report_folder_path(report_path)
    if not version:
        say(f"Finding the default branch of {repo}")
        version, version_type = client.default_branch(project, repo), "branch"
    destination = Path(destination or default_destination(project, repo, version))
    say(f"Downloading {report_path} ({version_type} {version})")
    report_folder = extract_folder(
        client.download_folder(project, repo, report_path, version, version_type),
        report_path,
        destination,
    )

    pbir = json.loads((report_folder / "definition.pbir").read_bytes().decode("utf-8-sig"))
    reference = (pbir.get("datasetReference") or {}).get("byPath") or {}
    if not reference.get("path"):
        raise ApiError(
            f"{report_path} is bound to a semantic model in the Power BI service (not stored in "
            "the repository) - document it from the Fabric workspace instead."
        )
    model_path = posixpath.normpath(posixpath.join(report_path, reference["path"]))
    say(f"Downloading {model_path}")
    model_folder = extract_folder(
        client.download_folder(project, repo, model_path, version, version_type),
        model_path,
        destination,
    )
    (report_folder.parent / f"{report_folder.stem}.devops_source.json").write_text(
        json.dumps(
            {
                "organization": client.org,
                "project": project,
                "repository": repo,
                "report": report_path,
                "semantic_model": model_path,
                "version": version,
                "version_type": version_type,
                "fetched": time.strftime("%Y-%m-%d %H:%M:%S"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return DevOpsFetched(project, repo, version, report_folder, model_folder)


def default_destination(project: str, repo: str, version: str) -> Path:
    """output/_devops/<project>/<repo>/<version> (CWD-relative)."""
    return Path("output") / "_devops" / safe_name(project) / safe_name(repo) / safe_name(version or "default")
