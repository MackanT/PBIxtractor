"""Tests for the Tabular Editor 2 integration (Best Practice Analyzer)."""

import json

import pytest

from pbixtractor.model_sheets import bpa_summary_rows
from pbixtractor.tabular_editor import (
    DEFAULT_BPA_RULES,
    add_tabular_editor_location,
    drop_redundant_table_refs,
    export_dependencies,
    find_tabular_editor,
    load_bpa_rules,
    parse_bpa_results,
    parse_dependencies,
    run_best_practice_analyzer,
)

from .test_semantic_model import BIM


# Runs the real Tabular Editor 2 (slow); leave these out with: pytest -m "not tabular_editor"
def _needs_tabular_editor(test):
    test = pytest.mark.skipif(find_tabular_editor() is None, reason="Tabular Editor 2 not installed")(test)
    return pytest.mark.tabular_editor(test)


FLOAT_RULE = "[Performance] Do not use floating point data types"
FK_RULE = "[Formatting] Hide foreign keys"

# TSV written by the BPA script (RuleName, ObjectType, ObjectName, Error)
BPA_TSV = (
    "RuleName\tObjectType\tObjectName\tError\n"
    f"{FLOAT_RULE}\tColumn\t'Sales'[Amount]\t\n"
    f"{FK_RULE}\tColumn\t'Sales'[Date Key]\t\n"
    f"{FK_RULE}\tColumn\t'Sales'[Region Key]\t\n"
    "[Performance] Minimize Power Query transformations\tPartition (M - Import)\tSales-1234\t\n"
    "Some custom rule\tModel\tModel\t\n"
    "Broken rule\t\t\tSyntax error in expression\n"
)


@pytest.fixture(scope="module")
def rules():
    return load_bpa_rules()


def test_bundled_rules_load(rules):
    assert DEFAULT_BPA_RULES.is_file()
    assert len(rules) > 50
    assert rules[FLOAT_RULE]["Category"] == "Performance"


def test_parse_bpa_results(rules):
    violations, errors = parse_bpa_results(BPA_TSV, rules)
    assert len(violations) == 5
    assert errors == ["Broken rule: Syntax error in expression"]

    by_object = {v.object_name: v for v in violations}
    amount = by_object["'Sales'[Amount]"]
    assert (amount.object_type, amount.rule, amount.category) == (
        "Column",
        FLOAT_RULE,
        "Performance",
    )
    assert amount.severity in ("Low", "Medium", "High")
    assert by_object["Sales-1234"].object_type == "Partition (M - Import)"

    # Rules not in the rules file are kept, without category/severity, and sort last
    custom = violations[-1]
    assert (custom.object_type, custom.object_name, custom.rule) == (
        "Model",
        "Model",
        "Some custom rule",
    )
    assert custom.category == "" and custom.severity == ""


def test_bpa_summary_counts_findings_per_rule(rules):
    rows = bpa_summary_rows(parse_bpa_results(BPA_TSV, rules)[0])
    counts = {row[2]: row[3] for row in rows}
    assert counts[FK_RULE] == 2
    assert counts[FLOAT_RULE] == 1


def test_find_tabular_editor_from_locations_file(tmp_path):
    exe = tmp_path / "Tools" / "Tabular Editor" / "TabularEditor.exe"
    exe.parent.mkdir(parents=True)
    exe.write_bytes(b"")
    locations = tmp_path / "TabularEditorLocations.txt"
    locations.write_text(f"{tmp_path / 'Tools'}\n")
    assert find_tabular_editor(locations) == exe


def test_add_tabular_editor_location(tmp_path):
    exe = tmp_path / "TE2" / "TabularEditor.exe"
    exe.parent.mkdir()
    exe.write_bytes(b"")
    locations = tmp_path / "Input" / "TabularEditorLocations.txt"

    assert add_tabular_editor_location(tmp_path / "Nothing here", locations) is None
    assert not locations.exists()  # nothing saved for a wrong folder

    assert add_tabular_editor_location(f'"{exe.parent}"', locations) == exe  # quotes from Explorer
    assert add_tabular_editor_location(exe.parent, locations) == exe  # no duplicates
    assert locations.read_text().splitlines() == [str(exe.parent)]
    assert find_tabular_editor(locations) == exe


@_needs_tabular_editor
def test_run_best_practice_analyzer_on_sample_model(tmp_path):
    # Direct Lake partitions and dynamic format strings need compatibility level 1604
    bim = tmp_path / "Model.bim"
    bim.write_text(json.dumps({**BIM, "compatibilityLevel": 1604}), encoding="utf-8")

    violations, errors = run_best_practice_analyzer(find_tabular_editor(), bim)
    assert errors == []
    found = {(v.object_type, v.object_name, v.rule) for v in violations}

    # The sample's decimal -> integer many-to-many relationship is deliberately bad
    relationship = "'Sales'[Amount] <--> 'Dates'[Year Number]"
    assert (
        "Relationship",
        relationship,
        "[Error Prevention] Relationship columns should be of the same data type",
    ) in found
    assert ("Column", "'Dates'[Date Key]", FK_RULE) in found
    assert all(v.category and v.severity for v in violations)  # all from the bundled rules


@_needs_tabular_editor
def test_run_best_practice_analyzer_reports_load_errors(tmp_path):
    bim = tmp_path / "Broken.bim"
    bim.write_text("{not json", encoding="utf-8")
    with pytest.raises(RuntimeError, match="script failed"):
        run_best_practice_analyzer(find_tabular_editor(), bim)


DEPENDENCY_TSV = (
    "SourceType\tSourceTable\tSourceName\tTargetType\tTargetTable\tTargetName\n"
    "Measure\tSales\tTotal Amount\tTable\tSales\tSales\n"
    "Measure\tSales\tTotal Amount\tColumn\tSales\tAmount\n"
    "Measure\tSales\tRows\tTable\tSales\tSales\n"
    "Measure\tSales\tDynamic\tMeasure\tSales\tTotal Amount\n"
    "TablePermission\tSales\tNordics\tColumn\tSales\tAmount\n"
)


def test_parse_dependencies_and_refs():
    dependencies = parse_dependencies(DEPENDENCY_TSV)
    assert len(dependencies) == 5
    refs = [(d.source_ref, d.target_ref) for d in dependencies]
    assert ("Sales[Total Amount]", "'Sales'") in refs
    assert ("Sales[Dynamic]", "Sales[Total Amount]") in refs
    assert ("RLS: Nordics (Sales)", "Sales[Amount]") in refs


def test_drop_redundant_table_refs():
    kept = drop_redundant_table_refs(parse_dependencies(DEPENDENCY_TSV))
    refs = [(d.source_ref, d.target_ref) for d in kept]
    # SUM ( Sales[Amount] ) also lists 'Sales' -> dropped; COUNTROWS ( Sales ) keeps it
    assert ("Sales[Total Amount]", "'Sales'") not in refs
    assert ("Sales[Rows]", "'Sales'") in refs
    assert len(kept) == 4


@_needs_tabular_editor
def test_export_dependencies_on_sample_model(tmp_path):
    bim = tmp_path / "Model.bim"
    bim.write_text(json.dumps({**BIM, "compatibilityLevel": 1604}), encoding="utf-8")

    dependencies = drop_redundant_table_refs(export_dependencies(find_tabular_editor(), bim))
    found = {(d.source_type, d.source_ref, d.target_type, d.target_ref) for d in dependencies}
    assert found == {
        ("Measure", "Sales[Total Amount]", "Column", "Sales[Amount]"),
        ("Measure", "Sales[Dynamic]", "Measure", "Sales[Total Amount]"),
        ("Column", "Sales[Is Big]", "Column", "Sales[Amount]"),  # calculated column
        ("TablePermission", "RLS: Nordics (Sales)", "Column", "Sales[Amount]"),
    }
