#!/usr/bin/env python3
"""Command-line interface for PBIxtractor.

    pbixtractor                          # web UI (NiceGUI) in the browser
    pbixtractor web --port 8081          # same, with options
    pbixtractor extract Sales.pbix       # document a report from the command line
"""

import argparse
import sys
from pathlib import Path

from . import __version__

EXAMPLES = """
Examples:
  pbixtractor                                     Start the web UI
  pbixtractor extract C:/Reports/Sales.pbix       Document a report (model: Sales.bim next to it)
  pbixtractor extract Sales.pbix --model Model.bim -o out/Sales --no-tabular-editor
  pbixtractor extract C:/Reports/Sales.pbip       PBIP project (model from Sales.SemanticModel)
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
    extract.add_argument("report", type=Path, help=".pbix, .pbip or <name>.Report folder")
    extract.add_argument(
        "--model",
        type=Path,
        help=".bim file or folder with model.bim (default: <name>.bim or <name>.SemanticModel)",
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

    web = commands.add_parser("web", help="start the web UI (default)")
    web.add_argument("--port", type=int, default=8081, help="port (default: 8081)")
    web.add_argument("--no-browser", action="store_true", help="do not open a browser tab")
    return parser


def run_extract(args: argparse.Namespace) -> int:
    """Handle `pbixtractor extract`. Returns the process exit code."""
    from .pipeline import ExtractionOptions, find_model_for_report, report_name, run_extraction

    model = args.model or find_model_for_report(args.report)
    if model is None:
        print(
            f"No model found for {args.report}: expected {report_name(args.report)}.bim next to "
            "it (or a PBIP .SemanticModel folder). Pass it with --model.",
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

    from .web_ui import start

    start(
        port=getattr(args, "port", 8081),
        open_browser=not getattr(args, "no_browser", False),
    )


if __name__ == "__main__":
    main()
