"""tools/compare_quality.py: BPA findings and dependencies of two data workbooks."""

import importlib.util
from pathlib import Path

import xlsxwriter

_TOOL = Path(__file__).resolve().parents[1] / "tools" / "compare_quality.py"
_spec = importlib.util.spec_from_file_location("compare_quality", _TOOL)
compare_quality = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(compare_quality)

BPA_HEADER = ["Severity", "Category", "Rule", "Object Type", "Object"]
DEP_HEADER = ["MeasureName", "Dependent", "Object Type", "Dependent Type"]


def _workbook(path: Path, bpa: list[list[str]] | None, deps: list[list[str]]) -> Path:
    workbook = xlsxwriter.Workbook(str(path))
    workbook.add_worksheet("pages")
    if bpa is not None:
        sheet = workbook.add_worksheet("model quality")
        for r, row in enumerate([BPA_HEADER, *bpa]):
            for c, value in enumerate(row):
                sheet.write(r, c, value)
    sheet = workbook.add_worksheet("dependencies")
    for r, row in enumerate([DEP_HEADER, *deps]):
        for c, value in enumerate(row):
            sheet.write(r, c, value)
    workbook.close()
    return path


FINDING = [
    "High",
    "Formatting",
    "[Formatting] Do not summarize numeric columns",
    "Column",
    "'Sales'[Amount]",
]
DEP = ["[Total]", "'Sales'[Amount]", "Measure", "Column"]


def test_same_findings_and_dependencies_agree(tmp_path):
    old = _workbook(tmp_path / "old.xlsx", [FINDING], [DEP])
    # Case and spacing differences still match
    new = _workbook(
        tmp_path / "new.xlsx",
        [[*FINDING[:4], "'sales'[amount] "]],
        [["[TOTAL]", "'Sales'[Amount]", "Measure", "Column"]],
    )
    lines = []
    assert compare_quality.compare(old, new, lines.append)
    assert "  Every rule has the same findings in both workbooks." in lines
    assert compare_quality.main([str(old), str(new)]) == 0


def test_differences_are_listed(tmp_path):
    other = [
        "Low",
        "Maintenance",
        "[Maintenance] Visible objects with no description",
        "Measure",
        "[Total]",
    ]
    old = _workbook(tmp_path / "old.xlsx", [FINDING, other], [DEP])
    new = _workbook(
        tmp_path / "new.xlsx", [FINDING], [DEP, ["[Total]", "'Sales'", "Measure", "Table"]]
    )
    lines = []
    assert not compare_quality.compare(old, new, lines.append)
    text = "\n".join(lines)
    assert "[maintenance] visible objects with no description: only old 1 / only new 0" in text
    assert "Only in new: 1" in text
    assert compare_quality.main([str(old), str(new)]) == 1


def test_missing_model_quality_sheet(tmp_path):
    old = _workbook(tmp_path / "old.xlsx", None, [DEP])
    new = _workbook(tmp_path / "new.xlsx", [FINDING], [DEP])
    lines = []
    assert not compare_quality.compare(old, new, lines.append)
    assert any("old workbook has no model quality sheet" in line for line in lines)


def test_bad_arguments(tmp_path):
    assert compare_quality.main([]) == 2
    assert compare_quality.main([str(tmp_path / "a.xlsx"), str(tmp_path / "b.xlsx")]) == 2
