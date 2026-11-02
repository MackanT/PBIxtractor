"""Tests for tools/compare_tabular_editor.py (Tabular Editor replaced by fakes)."""

import importlib.util
import json
import zipfile
from pathlib import Path

import pytest

from pbixtractor.bpa import run_bpa
from pbixtractor.dax_parser import model_dependencies
from pbixtractor.pbix_model import PbixModel
from pbixtractor.semantic_model import parse_model
from pbixtractor.tabular_editor import BpaViolation, Dependency

from .test_documentation import _xlsx_rows
from .test_semantic_model import BIM

TOOL = Path(__file__).resolve().parent.parent / "tools" / "compare_tabular_editor.py"

TE_ONLY_FINDING = BpaViolation(
    "Calculation Item", "'Calc'[Item]", "[Made up] Only Tabular Editor finds this", "", "High"
)
TE_ONLY_DEPENDENCY = Dependency("CalculationItem", "Calc", "Item", "Measure", "Sales", "Dynamic")


@pytest.fixture
def tool():
    spec = importlib.util.spec_from_file_location("compare_tabular_editor", TOOL)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def fake_te(tool, monkeypatch, tmp_path):
    """Tabular Editor that agrees with Python except one dropped and one extra item per side."""
    exe = tmp_path / "TabularEditor.exe"
    exe.write_text("fake")
    model = parse_model(BIM)
    python_bpa, _ = run_bpa(model)
    python_deps = model_dependencies(model)
    calls = {"inputs": []}

    def bpa(exe_path, bim_path, *args, **kwargs):
        calls["inputs"].append(Path(bim_path))
        calls["bim"] = Path(bim_path).read_text(encoding="utf-8")
        # Python-only: the first finding; the rest agree, in other case and with spaces
        same = [
            BpaViolation(v.object_type.upper(), f" {v.object_name} ", v.rule.lower(), "", v.severity)
            for v in python_bpa[1:]
        ]
        return same + [TE_ONLY_FINDING], ["Some rule: could not evaluate"]

    def deps(exe_path, bim_path, *args, **kwargs):
        calls["inputs"].append(Path(bim_path))
        # Python-only: the first dependency; a redundant table ref is dropped on both sides
        first = python_deps[2]  # a column target: its table entry is redundant
        redundant = Dependency(first.source_type, first.source_table, first.source_name,
                               "Table", first.target_table, first.target_table)  # fmt: skip
        return python_deps[1:] + [redundant, TE_ONLY_DEPENDENCY]

    monkeypatch.setattr(tool, "find_tabular_editor", lambda: exe)
    monkeypatch.setattr(tool, "run_best_practice_analyzer", bpa)
    monkeypatch.setattr(tool, "export_dependencies", deps)
    calls["python_bpa"], calls["python_deps"] = python_bpa, python_deps
    return calls


def test_differences_are_printed_and_written(tool, fake_te, tmp_path, capsys):
    bim = tmp_path / "Sample.bim"
    bim.write_text(json.dumps(BIM), encoding="utf-8")
    out = tmp_path / "out.xlsx"

    assert tool.main([str(bim), "-o", str(out)]) == 0
    printed = capsys.readouterr().out
    first = fake_te["python_bpa"][0]
    total = len(fake_te["python_bpa"])
    assert f"{first.rule}: Python " in printed
    assert "[Made up] Only Tabular Editor finds this: Python 0 / Tabular Editor 1 / both 0" in printed
    assert f"Total: Python {total} / Tabular Editor {total} / both {total - 1}" in printed
    assert "Not checked by Python" in printed and "Split date and time" in printed
    assert "Some rule: could not evaluate" in printed
    deps = len(fake_te["python_deps"])
    assert f"Agreeing: {deps - 1}" in printed
    assert "Only in Python: 1" in printed and "Only in Tabular Editor: 1" in printed
    assert fake_te["inputs"] == [bim, bim]

    assert _xlsx_rows(out, "bpa differences")[1:] == [
        ["Python", first.rule, first.severity, first.object_type, first.object_name],
        ["Tabular Editor", TE_ONLY_FINDING.rule, "High", "Calculation Item", "'Calc'[Item]"],
    ]
    dep = fake_te["python_deps"][0]
    assert _xlsx_rows(out, "dependency differences")[1:] == [
        ["Python", dep.source_type, dep.source_table, dep.source_name,
         dep.target_type, dep.target_table, dep.target_name],
        ["Tabular Editor", "CalculationItem", "Calc", "Item", "Measure", "Sales", "Dynamic"],
    ]  # fmt: skip
    summary = _xlsx_rows(out, "summary")
    assert summary[0] == ["Section", "Item", "Python", "Tabular Editor", "Both"]
    assert ["BPA total", "findings", str(total), str(total), str(total - 1)] in summary
    assert ["Dependencies", "agreeing", "", "", str(deps - 1)] in summary

    types = {row[0]: row for row in _xlsx_rows(out, "object types")[1:]}
    assert types["Calculation Item"][1:] == ["Tabular Editor", "BPA", "", "'Calc'[Item]"]
    assert types["CalculationItem"][1] == "Tabular Editor"
    assert types["Measure"][1] == "both"
    assert types["Relationship"][1] == "Python"  # Tabular Editor upper-cased it here
    assert types["RELATIONSHIP"][1] == "Tabular Editor"


def test_tabular_editor_not_found(tool, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(tool, "find_tabular_editor", lambda: None)
    bim = tmp_path / "Sample.bim"
    bim.write_text(json.dumps(BIM), encoding="utf-8")
    assert tool.main([str(bim), "-o", str(tmp_path / "out.xlsx")]) == 2
    assert "Tabular Editor 2 not found" in capsys.readouterr().err
    assert not (tmp_path / "out.xlsx").exists()
    missing = tmp_path / "missing.exe"
    assert tool.main([str(bim), "--tabular-editor", str(missing)]) == 2


def test_a_pbix_is_handed_over_as_a_temporary_bim(tool, fake_te, monkeypatch, tmp_path):
    report = tmp_path / "Sample.pbix"
    with zipfile.ZipFile(report, "w") as archive:
        archive.writestr("DataModel", b"\x00")  # read_pbix_model is replaced below
    monkeypatch.setattr(tool, "read_pbix_model", lambda path: PbixModel(BIM, None))

    assert tool.main([str(report), "-o", str(tmp_path / "out.xlsx")]) == 0
    bim = fake_te["inputs"][0]
    assert bim.name == "Sample_model.bim" and bim.parent != tmp_path
    assert json.loads(fake_te["bim"]) == BIM
    assert not bim.exists()  # a temp file, removed afterwards
