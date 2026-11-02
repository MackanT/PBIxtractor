"""Compare the BPA findings and DAX dependencies of two data workbooks (<name>_data.xlsx).

    python tools/compare_quality.py OLD_data.xlsx NEW_data.xlsx

Meant for an old run made with Tabular Editor 2 (its "model quality" and "dependencies" sheets
hold Tabular Editor's Best Practice Analyzer and DependsOn) against a new run of the same model
made with the Python analysis. Only those two sheets are compared, so the intended changes
elsewhere (formatted DAX, new columns) do not hide the result. Rows are matched case-insensitively
with whitespace trimmed. Exit code 0 when both sheets agree, 1 when they differ.
"""

import re
import sys
import zipfile
from collections import Counter
from pathlib import Path
from xml.etree import ElementTree

_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"


def _cell_text(cell, shared: list[str]) -> str:
    kind = cell.get("t")
    if kind == "inlineStr":
        return "".join(t.text or "" for t in cell.iterfind(".//m:t", _NS))
    value = cell.find("m:v", _NS)
    if value is None or value.text is None:
        return ""
    return shared[int(value.text)] if kind == "s" else value.text


def read_sheet(path: Path, name: str) -> list[list[str]] | None:
    """The rows of one sheet as text (None when the workbook has no such sheet)."""
    with zipfile.ZipFile(path) as z:
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            root = ElementTree.fromstring(z.read("xl/sharedStrings.xml"))
            for item in root.iterfind("m:si", _NS):
                shared.append("".join(t.text or "" for t in item.iterfind(".//m:t", _NS)))
        workbook = ElementTree.fromstring(z.read("xl/workbook.xml"))
        rels = ElementTree.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        targets = {r.get("Id"): r.get("Target") for r in rels}
        for sheet in workbook.iterfind("m:sheets/m:sheet", _NS):
            if sheet.get("name", "").casefold() != name.casefold():
                continue
            target = targets[sheet.get(_REL)].lstrip("/")
            target = target if target.startswith("xl/") else f"xl/{target}"
            root = ElementTree.fromstring(z.read(target))
            rows = []
            for row in root.iterfind("m:sheetData/m:row", _NS):
                values: dict[int, str] = {}
                for cell in row.iterfind("m:c", _NS):
                    letters = re.match(r"[A-Z]+", cell.get("r", "A")).group(0)
                    column = 0
                    for letter in letters:
                        column = column * 26 + ord(letter) - 64
                    values[column - 1] = _cell_text(cell, shared)
                width = max(values, default=-1) + 1
                rows.append([values.get(i, "") for i in range(width)])
            return rows
    return None


def _records(rows: list[list[str]] | None, columns: list[str]) -> Counter | None:
    """Rows as a multiset of tuples of the named columns (header row first)."""
    if not rows:
        return None
    header = [h.strip().casefold() for h in rows[0]]
    try:
        index = [header.index(c.casefold()) for c in columns]
    except ValueError:
        return None
    return Counter(
        tuple(" ".join((row[i] if i < len(row) else "").split()).casefold() for i in index)
        for row in rows[1:]
        if any(cell.strip() for cell in row)
    )


BPA_COLUMNS = ["Rule", "Object Type", "Object"]
DEPENDENCY_COLUMNS = ["MeasureName", "Dependent", "Object Type", "Dependent Type"]


def compare(old: Path, new: Path, out=print) -> bool:
    """Print the differences; True when both sheets agree."""
    same = True
    old_bpa = _records(read_sheet(old, "model quality"), BPA_COLUMNS)
    new_bpa = _records(read_sheet(new, "model quality"), BPA_COLUMNS)
    out("Best Practice Analyzer (model quality sheet)")
    if old_bpa is None or new_bpa is None:
        which = "old" if old_bpa is None else "new"
        out(f"  The {which} workbook has no model quality sheet (BPA did not run there).")
        same = False
    else:
        only_old, only_new = old_bpa - new_bpa, new_bpa - old_bpa
        rules = sorted({key[0] for key in only_old | only_new})
        for rule in rules:
            gone = sum(n for key, n in only_old.items() if key[0] == rule)
            added = sum(n for key, n in only_new.items() if key[0] == rule)
            out(f"  {rule}: only old {gone} / only new {added}")
            for key in sorted(k for k in only_old if k[0] == rule)[:10]:
                out(f"      old: {key[1]} {key[2]}")
            for key in sorted(k for k in only_new if k[0] == rule)[:10]:
                out(f"      new: {key[1]} {key[2]}")
        both = sum((old_bpa & new_bpa).values())
        out(f"  Total: old {sum(old_bpa.values())} / new {sum(new_bpa.values())} / both {both}")
        if not rules:
            out("  Every rule has the same findings in both workbooks.")
        same = same and not rules

    old_deps = _records(read_sheet(old, "dependencies"), DEPENDENCY_COLUMNS)
    new_deps = _records(read_sheet(new, "dependencies"), DEPENDENCY_COLUMNS)
    out("Dependencies (dependencies sheet)")
    if old_deps is None or new_deps is None:
        which = "old" if old_deps is None else "new"
        out(f"  The {which} workbook has no dependencies sheet.")
        return False
    only_old, only_new = old_deps - new_deps, new_deps - old_deps
    out(f"  Agreeing: {sum((old_deps & new_deps).values())}")
    out(f"  Only in old: {sum(only_old.values())}")
    for key in sorted(only_old)[:20]:
        out(f"      {key[0]} -> {key[1]} ({key[2]} -> {key[3]})")
    out(f"  Only in new: {sum(only_new.values())}")
    for key in sorted(only_new)[:20]:
        out(f"      {key[0]} -> {key[1]} ({key[2]} -> {key[3]})")
    return same and not only_old and not only_new


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__.strip().splitlines()[2].strip())
        return 2
    old, new = (Path(a) for a in argv)
    for path in (old, new):
        if not path.is_file():
            print(f"Not found: {path}")
            return 2
    return 0 if compare(old, new) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
