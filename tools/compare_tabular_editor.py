"""Compare the Python BPA and DAX dependencies with Tabular Editor 2's on one model.

    python tools/compare_tabular_editor.py MODEL [--tabular-editor PATH] [-o OUT.xlsx]

MODEL is a .bim file, a TMDL / .SemanticModel folder or a .pbix with its own model (handed to
Tabular Editor as a temporary <name>_model.bim, as the pipeline does). Python side: bpa.run_bpa
and dax_parser.model_dependencies; Tabular Editor side: run_best_practice_analyzer and
export_dependencies. Both get the model only (no statistics), so the rules that need row counts
or cardinality are "not checked" in Python and listed separately.

Prints a summary and writes every difference to compare_tabular_editor.xlsx (sheets summary,
bpa differences, dependency differences, object types). A developer check: documentation runs
never use Tabular Editor 2. It needs Windows, or Wine on Linux.
Exit code 0 when both ran (differences or not), 1 when Tabular Editor failed, 2 when the model
or Tabular Editor was not found.

Tabular Editor 2 under Wine (Linux), for this tool and the `tabular_editor` tests:
    1. Install Wine and Mono for it, e.g. `sudo apt install wine64 wine-mono` (or download the
       wine-mono .msi and run `wine msiexec /i wine-mono-<version>-x86.msi`).
    2. Unpack TabularEditor.Portable.zip of release 2.29 (or newer 2.x; github.com/TabularEditor/
       TabularEditor/releases) into a folder, e.g. ~/te2.
    3. Write a wrapper script named TabularEditor.exe that runs it, and make it executable:
           #!/bin/sh
           exec wine "$HOME/te2/TabularEditor.exe" "$@"
       Put it in a folder listed in Input/TabularEditorLocations.txt (so find_tabular_editor()
       finds it, also for the tests), or pass it with --tabular-editor.
    The model path is passed as a Linux path; Wine maps it through its Z: drive.
"""

import argparse
import json
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

import xlsxwriter  # noqa: E402

from pbixtractor.bpa import BpaViolation, load_rules, run_bpa  # noqa: E402
from pbixtractor.dax_parser import (  # noqa: E402
    Dependency,
    drop_redundant_table_refs,
    model_dependencies,
)
from pbixtractor.pbix_model import read_pbix_model  # noqa: E402
from pbixtractor.semantic_model import model_source, parse_model, read_model  # noqa: E402
from pbixtractor.tabular_editor import (  # noqa: E402
    export_dependencies,
    find_tabular_editor,
    run_best_practice_analyzer,
)

PYTHON = "Python"
TABULAR_EDITOR = "Tabular Editor"
DEPENDENCY_FIELDS = (
    "source_type",
    "source_table",
    "source_name",
    "target_type",
    "target_table",
    "target_name",
)


def _norm(*values: str) -> tuple[str, ...]:
    """Comparison key: trimmed, case-insensitive."""
    return tuple(str(v or "").strip().casefold() for v in values)


def _bpa_key(v: BpaViolation) -> tuple[str, ...]:
    return _norm(v.rule, v.object_type, v.object_name)


def _dependency_key(d: Dependency) -> tuple[str, ...]:
    return _norm(*(getattr(d, f) for f in DEPENDENCY_FIELDS))


# ============================================================================
# Comparison
# ============================================================================


def compare_bpa(python: list[BpaViolation], te: list[BpaViolation], not_checked: list[str]):
    """
    Findings per rule on both sides.

    Args:
        python: Python findings (run_bpa)
        te: Tabular Editor findings (run_best_practice_analyzer)
        not_checked: Names of the rules Python did not check

    Returns:
        (rules [(name, python, tabular editor, both)] that differ, largest difference first,
        totals (python, tabular editor, both), difference rows [side, rule, severity, type, name],
        not checked [(name, tabular editor findings)])
    """
    sides = {PYTHON: {_bpa_key(v): v for v in python}, TABULAR_EDITOR: {_bpa_key(v): v for v in te}}
    skipped = {_norm(name)[0]: name for name in not_checked}
    names: dict[str, str] = {}
    per_rule: dict[str, dict[str, set]] = defaultdict(lambda: {PYTHON: set(), TABULAR_EDITOR: set()})
    for side, found in sides.items():
        for key, v in found.items():
            names.setdefault(key[0], v.rule.strip())
            per_rule[key[0]][side].add(key)

    rules = []
    for rule, found in per_rule.items():
        if rule in skipped:
            continue
        n, m = len(found[PYTHON]), len(found[TABULAR_EDITOR])
        k = len(found[PYTHON] & found[TABULAR_EDITOR])
        if n != k or m != k:
            rules.append((names[rule], n, m, k))
    rules.sort(key=lambda r: (-(r[1] - r[3] + r[2] - r[3]), r[0].casefold()))

    py_keys, te_keys = set(sides[PYTHON]), set(sides[TABULAR_EDITOR])
    totals = (len(py_keys), len(te_keys), len(py_keys & te_keys))
    differences = []
    for side, keys in ((PYTHON, py_keys - te_keys), (TABULAR_EDITOR, te_keys - py_keys)):
        for key in sorted(keys):
            v = sides[side][key]
            differences.append([side, v.rule, v.severity, v.object_type, v.object_name])
    unchecked = [
        (name, len(per_rule[rule][TABULAR_EDITOR]) if rule in per_rule else 0)
        for rule, name in skipped.items()
    ]
    return rules, totals, differences, unchecked


def compare_dependencies(python: list[Dependency], te: list[Dependency]):
    """
    Dependencies on both sides, after drop_redundant_table_refs.

    Returns:
        ((agreeing, only in Python, only in Tabular Editor), difference rows [side, six fields])
    """
    sides = {
        PYTHON: {_dependency_key(d): d for d in drop_redundant_table_refs(python)},
        TABULAR_EDITOR: {_dependency_key(d): d for d in drop_redundant_table_refs(te)},
    }
    py_keys, te_keys = set(sides[PYTHON]), set(sides[TABULAR_EDITOR])
    differences = []
    for side, keys in ((PYTHON, py_keys - te_keys), (TABULAR_EDITOR, te_keys - py_keys)):
        for key in sorted(keys):
            d = sides[side][key]
            differences.append([side] + [getattr(d, f) for f in DEPENDENCY_FIELDS])
    totals = (len(py_keys & te_keys), len(py_keys - te_keys), len(te_keys - py_keys))
    return totals, differences


def object_types(
    py_bpa: list[BpaViolation],
    te_bpa: list[BpaViolation],
    py_deps: list[Dependency],
    te_deps: list[Dependency],
) -> list[list[str]]:
    """
    Every object type string either side uses, exactly as written.

    Returns:
        Rows [object type, used by, found in, Python example, Tabular Editor example]
    """
    seen: dict[str, dict] = {}

    def add(object_type: str, side: str, where: str, example: str):
        entry = seen.setdefault(object_type, {"where": set(), PYTHON: "", TABULAR_EDITOR: ""})
        entry["where"].add(where)
        entry[side] = entry[side] or example

    for side, bpa, deps in ((PYTHON, py_bpa, py_deps), (TABULAR_EDITOR, te_bpa, te_deps)):
        for v in bpa:
            add(v.object_type, side, "BPA", v.object_name)
        for d in deps:
            add(d.source_type, side, "dependencies", d.source_ref)
            add(d.target_type, side, "dependencies", d.target_ref)

    rows = []
    for object_type in sorted(seen, key=str.casefold):
        entry = seen[object_type]
        used = [s for s in (PYTHON, TABULAR_EDITOR) if entry[s]]
        rows.append([
            object_type,
            "both" if len(used) == 2 else used[0],
            ", ".join(sorted(entry["where"])),
            entry[PYTHON],
            entry[TABULAR_EDITOR],
        ])  # fmt: skip
    return rows


# ============================================================================
# Output
# ============================================================================


def summary_rows(bpa_rules, bpa_totals, unchecked, te_errors, dep_totals) -> list[list]:
    """The printed numbers as rows [section, item, Python, Tabular Editor, both]."""
    rows = [["BPA total", "findings", *bpa_totals]]
    rows += [["BPA rule", name, n, m, k] for name, n, m, k in bpa_rules]
    rows += [["Not checked by Python", name, "", m, ""] for name, m in unchecked]
    rows += [["Tabular Editor rule error", error, "", "", ""] for error in te_errors]
    agree, only_python, only_te = dep_totals
    rows += [
        ["Dependencies", "agreeing", "", "", agree],
        ["Dependencies", "only in Python", only_python, "", ""],
        ["Dependencies", "only in Tabular Editor", "", only_te, ""],
    ]
    return rows


def print_summary(bpa_rules, bpa_totals, unchecked, te_errors, dep_totals) -> None:
    """Print the comparison."""
    print("Best Practice Analyzer")
    if bpa_rules:
        print("  Rules with different findings (largest difference first):")
        for name, n, m, k in bpa_rules:
            print(f"    {name}: Python {n} / Tabular Editor {m} / both {k}")
    else:
        print("  Every rule has the same findings on both sides.")
    n, m, k = bpa_totals
    print(f"  Total: Python {n} / Tabular Editor {m} / both {k}")
    if unchecked:
        print("  Not checked by Python (Tabular Editor's findings for them):")
        for name, m in unchecked:
            print(f"    {name}: Tabular Editor {m}")
    if te_errors:
        print("  Tabular Editor rule errors:")
        for error in te_errors:
            print(f"    {error}")
    agree, only_python, only_te = dep_totals
    print("Dependencies")
    print(f"  Agreeing: {agree}")
    print(f"  Only in Python: {only_python}")
    print(f"  Only in Tabular Editor: {only_te}")


def write_workbook(path: Path, summary, bpa_differences, dependency_differences, types) -> None:
    """Write the four sheets."""
    sheets = [
        ("summary", ["Section", "Item", "Python", "Tabular Editor", "Both"], summary),
        ("bpa differences", ["Side", "Rule", "Severity", "Object Type", "Object Name"],
         bpa_differences),
        ("dependency differences", ["Side", "Source Type", "Source Table", "Source Name",
                                    "Target Type", "Target Table", "Target Name"],
         dependency_differences),
        ("object types", ["Object Type", "Used By", "Found In", "Python Example",
                          "Tabular Editor Example"], types),
    ]  # fmt: skip
    workbook = xlsxwriter.Workbook(
        str(path), {"strings_to_formulas": False, "strings_to_urls": False}
    )
    bold = workbook.add_format({"bold": True})
    try:
        for name, header, rows in sheets:
            sheet = workbook.add_worksheet(name)
            sheet.write_row(0, 0, header, bold)
            for i, row in enumerate(rows, start=1):
                sheet.write_row(i, 0, row)
            sheet.freeze_panes(1, 0)
            sheet.autofilter(0, 0, max(len(rows), 1), len(header) - 1)
            for col, title in enumerate(header):
                width = max([len(str(title))] + [len(str(r[col])) for r in rows])
                sheet.set_column(col, col, min(max(width + 2, 10), 80))
    finally:
        workbook.close()


# ============================================================================
# Main
# ============================================================================


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("model", type=Path, help=".bim, TMDL / .SemanticModel folder or .pbix")
    parser.add_argument(
        "--tabular-editor", type=Path, help="TabularEditor.exe (default: find_tabular_editor())"
    )
    parser.add_argument(
        "-o", "--output", type=Path, default=Path("compare_tabular_editor.xlsx"),
        help="workbook to write (default: compare_tabular_editor.xlsx here)",
    )  # fmt: skip
    args = parser.parse_args(argv)

    exe = args.tabular_editor or find_tabular_editor()
    if exe is None or not Path(exe).is_file():
        where = f" at {exe}" if exe else " (pass --tabular-editor PATH to TabularEditor.exe)"
        print(f"ERROR: Tabular Editor 2 not found{where}", file=sys.stderr)
        return 2
    try:
        source = model_source(args.model)
    except (OSError, ValueError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="pbixtractor_compare_") as temp:
        try:
            if source.suffix.lower() == ".pbix":
                # Tabular Editor reads a .bim, not a .pbix (as in pipeline.run_extraction)
                pbix = read_pbix_model(source)
                model = parse_model(pbix.database)
                te_input = Path(temp) / f"{source.stem}_model.bim"
                te_input.write_text(
                    json.dumps(pbix.database, indent=2, ensure_ascii=False), encoding="utf-8"
                )
            else:
                model = read_model(source)
                te_input = source
        except (OSError, ValueError) as e:
            print(f"ERROR: Could not read the model {args.model}: {e}", file=sys.stderr)
            return 2

        py_bpa, not_checked = run_bpa(model)
        py_deps = model_dependencies(model)
        try:
            te_bpa, te_errors = run_best_practice_analyzer(Path(exe), te_input)
            te_deps = export_dependencies(Path(exe), te_input)
        except (RuntimeError, OSError, subprocess.TimeoutExpired) as e:
            print(f"ERROR: Tabular Editor failed: {e}", file=sys.stderr)
            return 1

    rule_names = {rule.id: rule.name for rule in load_rules()}
    unchecked_names = [rule_names.get(rule_id, rule_id) for rule_id, _ in not_checked]
    bpa_rules, bpa_totals, bpa_differences, unchecked = compare_bpa(
        py_bpa, te_bpa, unchecked_names
    )
    dep_totals, dependency_differences = compare_dependencies(py_deps, te_deps)
    types = object_types(py_bpa, te_bpa, py_deps, drop_redundant_table_refs(te_deps))

    print(f"Model: {args.model}")
    print_summary(bpa_rules, bpa_totals, unchecked, te_errors, dep_totals)
    summary = summary_rows(bpa_rules, bpa_totals, unchecked, te_errors, dep_totals)
    write_workbook(args.output, summary, bpa_differences, dependency_differences, types)
    print(f"Written: {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
