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
import shutil
import time
import urllib.parse
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from .azure_auth import DEVOPS_SCOPE, ApiError, RestClient
from .utils import safe_name  # noqa: F401 (also used as fabric/devops.safe_name)

API_VERSION = "7.1"
VERSION_TYPES = ("branch", "tag", "commit")
MAX_ARCHIVE_BYTES = 2_000_000_000  # unpacked size limit of one downloaded folder (zip bombs)
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


def check_devops_host(base_url: str) -> None:
    """
    Only Azure DevOps receives the sign-in token or PAT: https and dev.azure.com /
    *.visualstudio.com, plus hosts listed in PBIXTRACTOR_DEVOPS_HOSTS (comma-separated, for
    Azure DevOps Server on your own domain). A pasted link to any other host is refused, so
    a phishing URL cannot collect the token.

    Raises:
        ApiError: For any other host or scheme
    """
    parts = urllib.parse.urlsplit(base_url)
    host = (parts.hostname or "").lower()
    extra = {h.strip().lower() for h in os.environ.get("PBIXTRACTOR_DEVOPS_HOSTS", "").split(",") if h.strip()}
    if parts.scheme == "https" and (
        host == "dev.azure.com" or host.endswith(".visualstudio.com") or host in extra
    ):
        return
    raise ApiError(
        f"Not an Azure DevOps address: {base_url} (allowed: https://dev.azure.com, "
        "https://<org>.visualstudio.com, or hosts in PBIXTRACTOR_DEVOPS_HOSTS)"
    )


@dataclass
class DevOpsLocation:
    """A report in a repository: organisation, project, repo, report folder and version."""

    org: str
    project: str
    repo: str
    path: str  # report folder in the repo, e.g. "/Reports/Sales.Report"
    version: str = ""  # branch / tag / commit; "" = the repository's default branch
    version_type: str = "branch"


def safe_repo_path(path: str) -> str:
    """
    A repository path that cannot leave the local download folder: "/"-separated, no "..",
    "." or empty segments, no drive letters or other ":" / "\\" characters.

    Paths come from the repository (definition.pbir byPath, folder names), so anyone with
    commit access controls them - they must never reach the file system unchecked.

    Raises:
        ApiError: For an unsafe path
    """
    parts = path.strip("/").split("/")
    if not path.strip("/") or any(
        part in ("", ".", "..") or any(c in part for c in '\\:\x00') for part in parts
    ):
        raise ApiError(f"Unsafe path in the repository, refusing to use it: {path!r}")
    return "/" + "/".join(parts)


def _resolve_repo_path(base: str, relative: str) -> str:
    """A repository path relative to another one ("../X.SemanticModel"), checked with safe_repo_path."""
    if "\\" in relative:
        raise ApiError(f"Unsafe path in the repository, refusing to use it: {relative!r}")
    # Resolve segment by segment: posixpath.normpath would silently clamp "/../.." at the root
    parts = [] if relative.startswith("/") else base.strip("/").split("/")
    for part in relative.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                raise ApiError(f"Path points outside the repository: {relative!r}")
            parts.pop()
        else:
            parts.append(part)
    return safe_repo_path("/".join(parts))


def report_folder_path(path: str) -> str:
    """The <name>.Report folder for a path to a .pbip file or to anything inside the folder."""
    path = safe_repo_path(path)
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
    on_dev_azure = parts.netloc.lower() == "dev.azure.com"
    org = f"https://dev.azure.com/{segments[0]}" if on_dev_azure else f"{parts.scheme}://{parts.netloc}"
    repo = segments[git + 1] if len(segments) > git + 1 else ""
    # Short form without a project (…/org/_git/Repo): the repository has the project's name
    project = segments[git - 1] if git > (1 if on_dev_azure else 0) else repo
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
        check_devops_host(self.org)
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

    def read_file(
        self, project: str, repo: str, path: str, version: str = "", version_type: str = "branch"
    ) -> bytes:
        """The content of one file of the repository."""
        _, _, content = self.request(
            "GET",
            self._url(
                f"{self._repo(project, repo)}/items",
                path=path,
                download="true",
                **self._version(version, version_type),
            ),
            raw=True,
        )
        return content

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

    Raises:
        ApiError: For an unsafe repository path or archive entry, a corrupt/non-zip download
            or an archive larger than MAX_ARCHIVE_BYTES unpacked
    """
    repo_path = safe_repo_path(repo_path)
    root = Path(destination).resolve()
    target = (root / repo_path.strip("/")).resolve()
    if root not in target.parents:  # checked BEFORE anything is deleted
        raise ApiError(f"Path points outside the download folder: {repo_path!r}")
    name = repo_path.rstrip("/").rsplit("/", 1)[-1]
    try:
        zip_file = zipfile.ZipFile(io.BytesIO(archive))
    except zipfile.BadZipFile:
        raise ApiError(
            f"The download of {repo_path} is not a zip archive - check the sign-in / access "
            "token (Azure DevOps answers an expired token with a sign-in page)."
        ) from None
    with zip_file:
        entries = [entry for entry in zip_file.infolist() if not entry.is_dir()]
        if sum(entry.file_size for entry in entries) > MAX_ARCHIVE_BYTES:
            raise ApiError(f"The download of {repo_path} is larger than {MAX_ARCHIVE_BYTES:,} bytes unpacked")
        if target.exists():
            shutil.rmtree(target)  # an earlier download: files deleted in git must not linger
        for entry in entries:
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
    # With all_reports: every downloaded report on the model (the asked-for one first)
    report_folders: list[Path] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


def _model_reference(pbir: bytes, report_path: str) -> Optional[str]:
    """Repository path of the model a definition.pbir points to (None if byConnection)."""
    reference = (json.loads(pbir.decode("utf-8-sig")).get("datasetReference") or {}).get("byPath")
    if not (reference or {}).get("path"):
        return None
    return _resolve_repo_path(report_path, reference["path"])


def fetch_report(
    client: DevOpsClient,
    project: str,
    repo: str,
    report_path: str,
    destination: Optional[Path] = None,
    version: str = "",
    version_type: str = "branch",
    progress: Optional[Callable[[str], None]] = None,
    all_reports: bool = False,
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
        all_reports: Also download every other report in the repository whose
            definition.pbir points to the same model folder (model mode)

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

    model_path = _model_reference((report_folder / "definition.pbir").read_bytes(), report_path)
    if model_path is None:
        raise ApiError(
            f"{report_path} is bound to a semantic model in the Power BI service (not stored in "
            "the repository) - document it from the Fabric workspace instead."
        )
    say(f"Downloading {model_path}")
    model_folder = extract_folder(
        client.download_folder(project, repo, model_path, version, version_type),
        model_path,
        destination,
    )

    def write_source(folder: Path, path: str) -> None:
        (folder.parent / f"{folder.stem}.devops_source.json").write_text(
            json.dumps(
                {
                    "organization": client.org,
                    "project": project,
                    "repository": repo,
                    "report": path,
                    "semantic_model": model_path,
                    "version": version,
                    "version_type": version_type,
                    "fetched": time.strftime("%Y-%m-%d %H:%M:%S"),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    write_source(report_folder, report_path)
    fetched = DevOpsFetched(
        project, repo, version, report_folder, model_folder, report_folders=[report_folder]
    )
    if not all_reports:
        return fetched

    say("Looking for other reports on the same model")
    for other in client.find_reports(project, repo, version, version_type):
        if other.lower() == report_path.lower():
            continue
        try:
            pbir = client.read_file(project, repo, f"{other}/definition.pbir", version, version_type)
            if (_model_reference(pbir, other) or "").lower() != model_path.lower():
                continue
            say(f"Downloading {other}")
            folder = extract_folder(
                client.download_folder(project, repo, other, version, version_type), other, destination
            )
        except (ApiError, ValueError) as error:
            fetched.skipped.append(f"{other}: {error}")
            continue
        write_source(folder, other)
        fetched.report_folders.append(folder)
    return fetched


def default_destination(project: str, repo: str, version: str) -> Path:
    """output/_devops/<project>/<repo>/<version> (CWD-relative)."""
    return Path("output") / "_devops" / safe_name(project) / safe_name(repo) / safe_name(version or "default")
