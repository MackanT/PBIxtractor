#!/usr/bin/env python3
"""Command-line interface for PBIxtractor.

    pbixtractor                          # web UI (NiceGUI) in the browser
    pbixtractor web --port 8081          # same, with options
    pbixtractor extract Sales.pbix       # document a report from the command line
"""

import argparse
import sys
from pathlib import Path
from typing import Callable, Optional

from . import __version__

EXAMPLES = """
Examples:
  pbixtractor                                     Start the web UI
  pbixtractor extract C:/Reports/Sales.pbix       Document a report (model: Sales.bim next to it)
  pbixtractor extract Sales.pbix --model Model.bim -o out/Sales --no-tabular-editor
  pbixtractor extract C:/Reports/Sales.pbip       PBIP project (model.bim or TMDL, found via
                                                  the report's definition.pbir)
  pbixtractor extract Sales.pbix --model Sales.SemanticModel/definition     TMDL model
  pbixtractor extract --fabric "Sales WS/Sales"   Download from a Fabric workspace, then document
  pbixtractor fabric list                         Workspaces you can access
  pbixtractor fabric list "Sales WS"              Reports and semantic models in a workspace
  pbixtractor fabric fetch "Sales WS/Sales" -o C:/pbip   Only download (as a PBIP project)
  pbixtractor extract --devops "https://dev.azure.com/org/Proj/_git/Repo?path=/Sales.Report"
                                                  Document a report from Azure DevOps (URL as
                                                  copied from the browser; add --ref)
  pbixtractor devops list myorg                   Projects (then: myorg Proj, myorg Proj Repo)
  pbixtractor devops fetch "<url>" --ref commit:a1b2c3d   Download an older version
  pbixtractor catalog add output/_catalog --fabric-workspace "Sales WS"
                                                  Document every model of a workspace into
                                                  one searchable catalog
  pbixtractor catalog add output/_catalog A.pbix B.pbix C.pbix   One documentation per model
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pbixtractor",
        description="PBIxtractor - Power BI documentation tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLES,
    )
    parser.add_argument("--version", action="version", version=f"PBIxtractor {__version__}")
    commands = parser.add_subparsers(dest="command")

    extract = commands.add_parser(
        "extract",
        help="document a report",
        description="Write Excel workbooks, JSON and the lineage viewer for a report.",
    )
    extract.add_argument(
        "report", type=Path, nargs="?", help=".pbix, .pbip or <name>.Report folder"
    )
    extract.add_argument(
        "--fabric",
        metavar="WORKSPACE/REPORT",
        help="download the report and its semantic model from a Fabric workspace first "
        "(names or ids; needs Contributor access)",
    )
    extract.add_argument(
        "--devops",
        metavar="URL",
        help="download the report (and the model it references) from an Azure DevOps repository "
        "first; the URL of the .Report folder or .pbip file as shown in the browser",
    )
    extract.add_argument(
        "--ref",
        "--version",  # the old name; "pbixtractor --version" is the program's version
        dest="repo_version",
        metavar="REF",
        help="with --devops: branch name, tag:<name> or commit:<id> (default: the URL's "
        "version, else the default branch)",
    )
    extract.add_argument(
        "--all-reports",
        action="store_true",
        help="model mode: document every report on the same semantic model together (with "
        "--fabric / --devops; pages become '<report> › <page>', 'unused' = unused by all)",
    )
    extract.add_argument(
        "--all-workspaces",
        action="store_true",
        help="with --fabric --all-reports: look for reports in every workspace you can access "
        "(default: the report's and the model's workspace)",
    )
    extract.add_argument(
        "--also",
        type=Path,
        nargs="+",
        metavar="REPORT",
        help="model mode for local files: more reports on the same model to document together",
    )
    extract.add_argument(
        "--no-service-statistics",
        action="store_true",
        help="do not read row counts / distinct values from the Power BI service for Fabric models",
    )
    extract.add_argument(
        "--catalog",
        type=Path,
        metavar="FOLDER",
        help="also add the result to this catalog folder (one searchable catalog.html for many "
        "models; documenting the same model again replaces its entry)",
    )
    extract.add_argument("--tenant", help="Entra tenant id for the Fabric / DevOps sign-in")
    extract.add_argument(
        "--model",
        type=Path,
        help=".bim file, TMDL folder (or its model.tmdl), or <name>.SemanticModel folder "
        "(default: <name>.bim next to the report, or the PBIP project's semantic model)",
    )
    extract.add_argument(
        "-o", "--output", type=Path, help="output folder (default: output/<name>)"
    )
    extract.add_argument("--name", help="base name of the output files (default: report name)")
    extract.add_argument(
        "--no-tabular-editor",
        action="store_true",
        help="skip the Tabular Editor analysis (BPA, exact dependencies, live statistics)",
    )
    extract.add_argument(
        "--tabular-editor-tsv",
        action="store_true",
        help="use Tabular Editor's TSV export (formats DAX via daxformatter.com - sends DAX online)",
    )
    extract.add_argument("--description-tag", help="delimiter of descriptions embedded in DAX")
    extract.add_argument("--no-log-file", action="store_true", help="do not write logs/*.txt")
    extract.add_argument("-q", "--quiet", action="store_true", help="only print the result")

    fabric = commands.add_parser(
        "fabric",
        help="list or download Fabric workspace items",
        description="Browse Fabric workspaces and download reports as PBIP projects.",
    )
    fabric_commands = fabric.add_subparsers(dest="fabric_command", required=True)
    fabric_list = fabric_commands.add_parser("list", help="list workspaces, or a workspace's items")
    fabric_list.add_argument("workspace", nargs="?", help="workspace name or id")
    fabric_fetch = fabric_commands.add_parser("fetch", help="download a report and its model")
    fabric_fetch.add_argument("path", metavar="WORKSPACE/REPORT", help="names or ids")
    fabric_fetch.add_argument(
        "-o",
        "--output",
        type=Path,
        help="folder for the PBIP project (default: output/_fabric/<workspace>)",
    )
    devops = commands.add_parser(
        "devops",
        help="list or download PBIP reports in Azure DevOps repositories",
        description="Browse Azure DevOps repositories and download PBIP reports at any version. "
        "Sign-in: same as Fabric, or a personal access token in AZURE_DEVOPS_PAT.",
    )
    devops_commands = devops.add_subparsers(dest="devops_command", required=True)
    devops_list = devops_commands.add_parser(
        "list", help="projects; a project's repositories; or a repository's branches and reports"
    )
    devops_list.add_argument("org", help="organisation name or URL")
    devops_list.add_argument("project", nargs="?")
    devops_list.add_argument("repo", nargs="?")
    devops_list.add_argument(
        "--history", metavar="REPORT_PATH", help="list the commits that changed this report"
    )
    devops_fetch = devops_commands.add_parser("fetch", help="download a report and its model")
    devops_fetch.add_argument("url", help="URL of the .Report folder or .pbip file")
    devops_fetch.add_argument(
        "--ref", "--version", dest="repo_version", metavar="REF",
        help="branch, tag:<name> or commit:<id>",
    )
    devops_fetch.add_argument(
        "-o", "--output", type=Path, help="local root (default: output/_devops/<project>/<repo>/<version>)"
    )
    for sub in (fabric_list, fabric_fetch, devops_list, devops_fetch):
        sub.add_argument("--tenant", help="Entra tenant id for the sign-in")

    catalog = commands.add_parser(
        "catalog",
        help="list, remove or rebuild catalog entries",
        description="Manage a catalog folder (add models with: extract ... --catalog FOLDER).",
    )
    catalog_commands = catalog.add_subparsers(dest="catalog_command", required=True)
    catalog_add = catalog_commands.add_parser(
        "add",
        help="document many models at once: whole Fabric workspaces, or several report files",
        description="Document every semantic model used by the given reports (or by the reports "
        "in whole Fabric workspaces) - one documentation per model, all added to the catalog. "
        "Reports on the same model are documented together.",
    )
    catalog_list = catalog_commands.add_parser("list", help="the models in a catalog")
    catalog_remove = catalog_commands.add_parser("remove", help="remove a model (by its key)")
    catalog_rebuild = catalog_commands.add_parser("rebuild", help="regenerate catalog.html")
    for sub in (catalog_add, catalog_list, catalog_remove, catalog_rebuild):
        sub.add_argument("folder", type=Path, help="the catalog folder")
    catalog_remove.add_argument("key", help="entry key, as shown by 'catalog list'")
    catalog_add.add_argument(
        "files",
        type=Path,
        nargs="*",
        metavar="FILE",
        help=".pbix / .pbip / .Report reports, and .bim models (one .bim: used for all the "
        "reports; several: each report uses the one named like it)",
    )
    catalog_add.add_argument(
        "--fabric-workspace",
        action="append",
        metavar="WORKSPACE",
        help="document every model used by a report in this workspace (name or id; repeat for "
        "more workspaces). Needs Contributor access to download",
    )
    catalog_add.add_argument(
        "--all-workspaces",
        action="store_true",
        help="with --fabric-workspace: also find the models' reports in every other workspace "
        "you can access (slower; makes 'unused' complete)",
    )
    catalog_add.add_argument(
        "-o", "--output", type=Path, help="root of the models' output folders (default: output)"
    )
    catalog_add.add_argument("--no-tabular-editor", action="store_true",
                             help="skip the Tabular Editor analysis (faster)")
    catalog_add.add_argument("--no-service-statistics", action="store_true",
                             help="do not read row counts / distinct values from the Power BI service")
    catalog_add.add_argument("--no-log-file", action="store_true", help="do not write logs/*.txt")
    catalog_add.add_argument("--tenant", help="Entra tenant id for the Fabric sign-in")
    catalog_add.add_argument("-q", "--quiet", action="store_true", help="only print the result")

    web = commands.add_parser("web", help="start the web UI (default)")
    web.add_argument("--port", type=int, default=8081, help="port (default: 8081)")
    web.add_argument("--no-browser", action="store_true", help="do not open a browser tab")
    return parser


def _fabric_client(args: argparse.Namespace):
    from .azure_auth import get_credential
    from .fabric import FabricClient

    return FabricClient(get_credential(getattr(args, "tenant", None)))


def _fetch(args: argparse.Namespace, path: str, destination: Optional[Path]):
    """Download WORKSPACE/REPORT; returns the FetchedReport (raises FabricError/ValueError)."""
    from .fabric import fetch_report, safe_name, split_fabric_path

    workspace, report = split_fabric_path(path)
    destination = destination or Path("output") / "_fabric" / safe_name(workspace)
    return fetch_report(
        _fabric_client(args),
        workspace,
        report,
        destination,
        progress=None if getattr(args, "quiet", False) else lambda text: print(f"  {text}"),
        all_reports=getattr(args, "all_reports", False),
        all_workspaces=getattr(args, "all_workspaces", False),
    )


def run_fabric(args: argparse.Namespace) -> int:
    """Handle `pbixtractor fabric list|fetch`. Returns the process exit code."""
    from .fabric import FabricError, find_workspace

    try:
        if args.fabric_command == "fetch":
            fetched = _fetch(args, args.path, args.output)
            print(f"Report: {fetched.report_folder}\nModel:  {fetched.model_path}")
            return 0
        client = _fabric_client(args)
        if not args.workspace:
            for ws in sorted(client.workspaces(), key=lambda w: w["displayName"].lower()):
                print(f"{ws['id']}  {ws['displayName']}")
            return 0
        ws = find_workspace(client, args.workspace)
        for label, items in (
            ("Reports", client.reports(ws["id"])),
            ("Semantic models", client.semantic_models(ws["id"])),
        ):
            print(f"{label} in {ws['displayName']}:")
            for item in sorted(items, key=lambda i: i["displayName"].lower()):
                print(f"  {item['id']}  {item['displayName']}")
        return 0
    except (FabricError, ValueError, OSError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


def _devops_client(args: argparse.Namespace, org: str):
    from .azure_auth import get_credential
    from .devops import DevOpsClient

    return DevOpsClient(org, get_credential(getattr(args, "tenant", None)))


def _connected_models(args: argparse.Namespace, root: Path, fetched: list) -> Callable[[dict], Path]:
    """A devops.fetch_report connected_model hook: gets a published model via Fabric (sign-in)
    into <root>/<workspace>/, and keeps the FetchedModel in `fetched` (for statistics)."""
    from .fabric import fetch_connected_model, model_reference

    def get(pbir: dict) -> Path:
        model_id, workspace = model_reference(pbir)
        model = fetch_connected_model(_fabric_client(args), model_id, workspace, root, _progress(args))
        fetched.append(model)
        return model.model_path

    return get


def _progress(args: argparse.Namespace):
    return None if getattr(args, "quiet", False) else lambda text: print(f"  {text}")


def _devops_fetch(
    args: argparse.Namespace, url: str, destination: Optional[Path], connected: Optional[list] = None
):
    """Download the report at a DevOps URL (raises ApiError/ValueError). A report bound to a
    published model gets that model from Fabric; `connected` receives its FetchedModel."""
    from .devops import fetch_report, parse_devops_url, parse_version

    location = parse_devops_url(url)
    version, version_type = location.version, location.version_type
    if getattr(args, "repo_version", None):
        version, version_type = parse_version(args.repo_version)
    return fetch_report(
        _devops_client(args, location.org),
        location.project,
        location.repo,
        location.path,
        destination,
        version,
        version_type,
        progress=_progress(args),
        all_reports=getattr(args, "all_reports", False),
        connected_model=_connected_models(
            args, (destination or Path("output")) / "_fabric", connected if connected is not None else []
        ),
    )


def run_devops(args: argparse.Namespace) -> int:
    """Handle `pbixtractor devops list|fetch`. Returns the process exit code."""
    from .azure_auth import ApiError

    try:
        if args.devops_command == "fetch":
            fetched = _devops_fetch(args, args.url, args.output)
            print(f"Version: {fetched.version}")
            print(f"Report:  {fetched.report_folder}\nModel:   {fetched.model_path}")
            return 0
        client = _devops_client(args, args.org)
        if not args.project:
            for name in client.projects():
                print(name)
        elif not args.repo:
            for repo in client.repositories(args.project):
                print(f"{repo['name']}  (default branch: {repo['defaultBranch'] or '-'})")
        elif args.history:
            for commit in client.commits(args.project, args.repo, args.history):
                print(f"{commit['short']}  {commit['date']}  {commit['author']}: {commit['comment']}")
        else:
            print("Branches: " + ", ".join(client.branches(args.project, args.repo)))
            print("Reports on the default branch:")
            for path in client.find_reports(args.project, args.repo):
                print(f"  {path}")
        return 0
    except (ApiError, ValueError, OSError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


def run_catalog_add(args: argparse.Namespace) -> int:
    """Handle `pbixtractor catalog add`: plan, then document every model (exit 1 if any failed)."""
    from .azure_auth import ApiError
    from .batch import BatchSettings, plan_fabric, plan_files, run_batch, summary

    if bool(args.files) == bool(args.fabric_workspace):
        print("Give report files or --fabric-workspace (not both).", file=sys.stderr)
        return 2
    output_root = args.output or Path("output")
    say = _progress(args)
    try:
        if args.fabric_workspace:
            jobs = plan_fabric(
                _fabric_client(args), args.fabric_workspace, args.all_workspaces, output_root, say
            )
        else:
            models = [f for f in args.files if f.suffix.lower() == ".bim"]
            reports = [f for f in args.files if f.suffix.lower() != ".bim"]
            missing = [str(f) for f in args.files if not f.exists()]
            if missing or not reports:
                print(f"Not found: {', '.join(missing)}" if missing else "No report files given.",
                      file=sys.stderr)
                return 2
            jobs = plan_files(reports, models, make_client=lambda: _fabric_client(args),
                              output_root=output_root)
    except (ApiError, ValueError, OSError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    if not args.quiet:
        runnable = sum(1 for job in jobs if not job.skip_reason)
        print(f"{runnable} model{'s' if runnable != 1 else ''} to document:")
        for job in jobs:
            reports = ", ".join(job.reports) or "-"
            print(f"  {job.model} [{job.source}] - reports: {reports}"
                  + (f" - skipped: {job.skip_reason}" if job.skip_reason else ""))

    last = {"step": None}

    def progress(step: str, fraction: float) -> None:
        if not args.quiet and step != last["step"]:
            last["step"] = step
            print(f"[{fraction:4.0%}] {step}")

    outcomes = run_batch(
        jobs,
        BatchSettings(
            output_root=output_root,
            catalog_dir=args.folder,
            tabular_editor_analysis=not args.no_tabular_editor,
            service_statistics=not args.no_service_statistics,
            write_log_file=not args.no_log_file,
        ),
        progress=progress,
    )
    print(f"\n{summary(outcomes)}. Catalog: {Path(args.folder) / 'catalog.html'}")
    for outcome in outcomes:
        detail = str(outcome.options.output_dir) if outcome.ok and outcome.options else outcome.message
        print(f"  {outcome.status:8} {outcome.job.model}: {detail}")
    return 1 if any(o.status == "error" for o in outcomes) else 0


def run_catalog(args: argparse.Namespace) -> int:
    """Handle `pbixtractor catalog add|list|remove|rebuild`. Returns the process exit code."""
    from .catalog import list_entries, rebuild_catalog, remove_from_catalog

    if args.catalog_command == "add":
        return run_catalog_add(args)
    if args.catalog_command == "list":
        entries = list_entries(args.folder)
        for entry in entries:
            reports = ", ".join(r["name"] for r in entry["reports"] if r["name"]) or "-"
            print(f"{entry['key']}\n  {entry['name']} ({entry['documented']}) - reports: {reports}")
            print(f"  {entry['source']['label']}")
        if not entries:
            print(f"No entries in {args.folder}")
        return 0
    if args.catalog_command == "remove":
        if not remove_from_catalog(args.folder, args.key):
            print(f"ERROR: no entry {args.key} in {args.folder}", file=sys.stderr)
            return 1
        print(f"Removed {args.key}")
        return 0
    print(f"Catalog written: {rebuild_catalog(args.folder)}")
    return 0


def run_extract(args: argparse.Namespace) -> int:
    """Handle `pbixtractor extract`. Returns the process exit code."""
    from .azure_auth import ApiError
    from .fabric import FetchedModel, fetch_connected_model
    from .pbix_model import live_connection
    from .pipeline import (
        ExtractionOptions,
        find_model_for_report,
        model_name,
        report_name,
        run_extraction,
    )
    from .service_stats import ServiceModel

    connected: list[FetchedModel] = []  # a published model fetched for a live-connected report

    if sum(bool(x) for x in (args.report, args.fabric, args.devops)) != 1:
        print(
            "Give one of: a report path, --fabric WORKSPACE/REPORT or --devops URL.",
            file=sys.stderr,
        )
        return 2
    if args.fabric or args.devops:
        if not args.quiet:
            print(f"Downloading {args.fabric or args.devops}")
        source = args.output / "source" if args.output else None
        try:
            if args.fabric:
                fetched = _fetch(args, args.fabric, source)
            else:
                fetched = _devops_fetch(args, args.devops, source, connected)
        except (ApiError, ValueError, OSError) as error:
            print(f"ERROR: {error}", file=sys.stderr)
            return 1
        args.report = fetched.report_folder
        args.model = args.model or fetched.model_path
        extra_reports = fetched.report_folders[1:]
        not_included = list(fetched.skipped)  # reported as warnings of the run
        if args.all_reports and not args.name:
            args.name = model_name(fetched.model_path)
    else:
        if args.all_reports:
            print("--all-reports works with --fabric / --devops; for local files use --also.",
                  file=sys.stderr)
            return 2
        extra_reports, not_included = list(args.also or []), []
        if extra_reports and not args.name:
            model_path = args.model or find_model_for_report(args.report)
            args.name = model_name(model_path) if model_path else None

    model = args.model or find_model_for_report(args.report)
    reference = live_connection(args.report) if model is None else None
    if reference is not None:
        # A live-connected .pbix: its model is published in the service - get it from there
        if not args.quiet:
            print(f"{Path(args.report).name} uses a published semantic model: getting it from Fabric")
        root = (args.output / "source" if args.output else Path("output")) / "_fabric"
        try:
            connected.append(fetch_connected_model(
                _fabric_client(args), reference.model_id, reference.workspace, root, _progress(args)
            ))
        except (ApiError, ValueError, OSError) as error:
            print(f"ERROR: {error}", file=sys.stderr)
            return 1
        model = connected[-1].model_path
    if model is None:
        print(
            f"No model found for {args.report}: a .pbix normally carries its own; else expected "
            f"{report_name(args.report)}.bim next to it (or a PBIP .SemanticModel folder with "
            "model.bim or TMDL). Pass it with --model.",
            file=sys.stderr,
        )
        return 2

    name = args.name or report_name(args.report)
    options = ExtractionOptions(
        report_path=args.report,
        model_path=model,
        output_dir=args.output or Path("output") / name,
        name=name,
        tabular_editor_analysis=not args.no_tabular_editor,
        tabular_editor_tsv=args.tabular_editor_tsv,
        write_log_file=not args.no_log_file,
        extra_reports=extra_reports,
        not_included=not_included,
        service_statistics=not args.no_service_statistics,
        catalog_dir=args.catalog,
        service_model=(
            ServiceModel(connected[-1].workspace_id, connected[-1].model_id, connected[-1].model)
            if connected
            else None
        ),
    )
    if args.description_tag:
        options.description_tag = args.description_tag

    def progress(step: str, fraction: float) -> None:
        if not args.quiet:
            print(f"[{fraction:4.0%}] {step}")

    result = run_extraction(options, progress=progress)

    if result.status == "error":
        print(f"ERROR: {result.message}", file=sys.stderr)
        return 1
    outcome = "Done" if result.status == "success" else "Done with warnings"
    print(f"{outcome} in {result.seconds}s: {result.message}")
    for kind, path in result.files.items():
        print(f"  {kind:14} {path}")
    if result.status == "warnings" and not args.quiet:
        warnings = result.logs.strip().splitlines()
        print(f"\n{len(warnings)} warning(s):")
        for line in warnings[:20]:
            print(f"  {line}")
        if len(warnings) > 20:
            print(f"  ... see {result.files.get('log', 'the log file')}")
    return 0


def main(argv: list[str] | None = None) -> None:
    """Main entry point for the `pbixtractor` command."""
    args = build_parser().parse_args(argv)

    if args.command == "extract":
        sys.exit(run_extract(args))
    if args.command == "fabric":
        sys.exit(run_fabric(args))
    if args.command == "devops":
        sys.exit(run_devops(args))
    if args.command == "catalog":
        sys.exit(run_catalog(args))

    from .web_ui import start

    start(
        port=getattr(args, "port", 8081),
        open_browser=not getattr(args, "no_browser", False),
    )


if __name__ == "__main__":
    main()
