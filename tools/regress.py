"""Regression check: document real reports and compare the workbooks with a saved baseline.

    python tools/regress.py --save-baseline     # before a change: run and keep the output
    python tools/regress.py                     # after the change: run and compare

The reports come from Input/regression.json (gitignored - real reports may be client data):

    {"reports": [
        {"name": "AdventureWorks",
         "report": "C:/Users/me/Downloads/Adventure Works DW 2020.pbix",
         "model": "C:/Users/me/Downloads/Model.bim"}
    ]}

"model" is optional (found like `pbixtractor extract` does). Output goes to
output/_regress/<name>/, the baseline to output/_baseline/<name>/ (both gitignored).
Output is deterministic except the relationship graph PNG, which is not compared.
Exit code 0 when every workbook is identical.
"""

import argparse
import json
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from compare_xlsx import compare  # noqa: E402

from pbixtractor.pipeline import (  # noqa: E402
    ExtractionOptions,
    find_model_for_report,
    run_extraction,
)

DEFAULT_CONFIG = REPO / "Input" / "regression.json"
RUN_DIR = REPO / "output" / "_regress"
BASELINE_DIR = REPO / "output" / "_baseline"

EXAMPLE_CONFIG = """{"reports": [
    {"name": "AdventureWorks",
     "report": "C:/Users/me/Downloads/Adventure Works DW 2020.pbix",
     "model": "C:/Users/me/Downloads/Model.bim"}
]}"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="reports to run")
    parser.add_argument(
        "--save-baseline", action="store_true", help="store this run as the new baseline"
    )
    parser.add_argument(
        "--no-tabular-editor", action="store_true", help="skip BPA/dependencies/live statistics"
    )
    parser.add_argument("--only", help="run only the report with this name")
    args = parser.parse_args()

    if not args.config.is_file():
        print(f"No config at {args.config}. Create it like this:\n\n{EXAMPLE_CONFIG}")
        return 2
    reports = json.loads(args.config.read_text(encoding="utf-8"))["reports"]
    if args.only:
        reports = [r for r in reports if r["name"] == args.only]

    failed = False
    for entry in reports:
        name, report = entry["name"], Path(entry["report"])
        model = Path(entry["model"]) if entry.get("model") else find_model_for_report(report)
        if model is None:
            print(f"{name}: no model found for {report}")
            failed = True
            continue
        out = RUN_DIR / name
        shutil.rmtree(out, ignore_errors=True)
        result = run_extraction(
            ExtractionOptions(
                report_path=report,
                model_path=model,
                output_dir=out,
                name=name,
                tabular_editor_analysis=not args.no_tabular_editor,
                write_log_file=False,
            )
        )
        print(f"{name}: {result.status} in {result.seconds}s")
        if not result.ok:
            print(f"  {result.message}")
            failed = True
            continue

        baseline = BASELINE_DIR / name
        workbooks = [result.files["workbook"], result.files["data_workbook"]]
        if args.save_baseline:
            baseline.mkdir(parents=True, exist_ok=True)
            for workbook in workbooks:
                shutil.copy2(workbook, baseline / workbook.name)
            print(f"  baseline saved to {baseline}")
            continue
        for workbook in workbooks:
            reference = baseline / workbook.name
            if not reference.is_file():
                print(f"  no baseline for {workbook.name} (run with --save-baseline first)")
                failed = True
                continue
            problems = compare(reference, workbook)
            print(f"  {'IDENTICAL' if not problems else f'{len(problems)} differences'}: "
                  f"{workbook.name}")
            for problem in problems[:25]:
                print(f"    {problem}")
            failed |= bool(problems)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
