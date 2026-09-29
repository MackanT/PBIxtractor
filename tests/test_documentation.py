"""Tests for the analysis stage (documentation.py), the DAX helpers and the full pipeline."""

import json
import logging
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import pytest

import pbixtractor.extractor as extractor
from pbixtractor.constants import REPORT_COLUMNS
from pbixtractor.dax import highlight_dax, text_dependencies
from pbixtractor.documentation import (
    OBJECT_COLUMNS,
    build_documentation,
    build_objects,
    parse_tsv_object_name,
    resolve_hierarchy_columns,
)
from pbixtractor.semantic_model import model_to_dataset, parse_model
from pbixtractor.tabular_editor import parse_dependencies

from .sample_layout import write_sample_pbix
from .test_semantic_model import BIM

LOGGER = logging.getLogger("test")


@pytest.fixture
def model():
    return parse_model(BIM)


@pytest.fixture(scope="module")
def report():
    """Sample report extraction: (items, filters, report definition)."""
    folder = Path(tempfile.mkdtemp())
    write_sample_pbix(folder / "Sample.pbix")
    report_extractor = extractor.ReportExtractor(str(folder), "Sample.pbix")
    report_extractor.extract()
    return report_extractor.result, report_extractor.filters, report_extractor.report


def _documentation(model, report, **kwargs):
    items, filters, definition = report
    return build_documentation(
        report_items=items,
        report_filters=filters,
        model=model,
        dataset=model_to_dataset(model),
        report_name="Sample",
        description_tag="////",
        visual_mapper=extractor.visual_mapper,
        visual_types=extractor.visual_type_list,
        logger=LOGGER,
        report=definition,
        **kwargs,
    )


@pytest.mark.parametrize(
    "name, expected",
    [
        ("Model.T.Sales", ("Table", "Sales", "")),
        ("Model.T.Sales.C.Amount", ("Column", "Sales", "Amount")),
        ("Model.T.Sales.M.Total Amount", ("Measure", "Sales", "Total Amount")),
        ("Model.T.Dates.H.Date Hierarchy", ("Hierarchy", "Dates", "Date Hierarchy")),
        ("Model.T.Sales.P.Sales-1234", ("Table", "Sales", "")),
    ],
)
def test_parse_tsv_object_name(name, expected):
    assert parse_tsv_object_name(name) == expected


def test_text_dependencies():
    deps = text_dependencies("SUMX ( Sales, Sales[Amount] * [Rate] ) + [Total]")
    assert deps[0] == "Sales[Amount]"
    assert sorted(deps[1:]) == ["[Rate]", "[Total]"]


def test_highlight_dax_keeps_all_text():
    formats = {
        k: k for k in ("comment", "quote", "var", "varname", "measure", "function", "return")
    }
    formats["para"] = [f"para{i}" for i in range(15)]
    segments = highlight_dax(
        'VAR x = SUM ( Sales[Amount] ) // total\nRETURN IF ( x > 0, "yes" )', formats, ["SUM", "IF"]
    )
    text = "".join(s for s in segments if s not in formats.values() and s not in formats["para"])
    for word in ("VAR", "x", "SUM", "Sales[Amount]", "// ", "total", "RETURN", "IF", '"'):
        assert word in text
    assert "function" in segments and "var" in segments and "comment" in segments


def test_build_objects_description_tag_and_order(model):
    dataset = model_to_dataset(model)
    dataset.loc[dataset["Object"] == "Model.T.Sales.M.Total Amount", "Expression"] = (
        "//// Sum of sales ////\nSUM ( Sales[Amount] )"
    )
    dataset.loc[dataset["Object"] == "Model.T.Sales.M.Total Amount", "Description"] = float("nan")

    objects = build_objects(dataset, ["Sales", "Dates"], "Sample", "////")
    assert list(objects.columns) == OBJECT_COLUMNS
    # Columns and measures only, in model order
    expected = [
        name
        for name, obj in zip(dataset["Name"], dataset["Object"])
        if ".C." in obj or ".M." in obj
    ]
    assert list(objects["Name"]) == expected and expected[0] == "Date Key"
    types = dict(zip(objects["Name"], objects["Type"]))
    assert types["Amount"] == "Column"
    assert types["Is Big"] == "Calculated Column"
    assert types["Total Amount"] == "Measure"
    total = objects[objects["Name"] == "Total Amount"].iloc[0]
    assert total["Description"] == "Sum of sales"
    assert total["Definition"] == "SUM ( Sales[Amount] )"


def test_unused_with_exact_dependencies(model, report):
    dependencies = parse_dependencies(
        "SourceType\tSourceTable\tSourceName\tTargetType\tTargetTable\tTargetName\n"
        "Measure\tSales\tTotal Amount\tColumn\tSales\tAmount\n"
        "Measure\tSales\tDynamic\tMeasure\tSales\tTotal Amount\n"
    )
    documentation = _documentation(model, report, exact_dependencies=dependencies)
    # Relationship keys, sort-by and hierarchy columns are used by the model itself
    assert ("Dates", "Month Number") not in documentation.unused_columns
    assert ("Sales", "Is Big") in documentation.unused_columns
    # Total Amount is used by Dynamic; Dynamic by nothing
    assert documentation.unused_measures == [("Sales", "Dynamic")]
    assert documentation.depends_on("Sales", "Dynamic") == ["Sales[Total Amount]"]


def test_unused_with_text_matching(model, report):
    documentation = _documentation(model, report)
    assert documentation.depends_on("Sales", "Dynamic") is None
    assert documentation.unused_measures == [("Sales", "Dynamic")]


def test_page_items(model, report):
    documentation = _documentation(model, report)
    items = documentation.pages["Sales"]
    kinds = [item.item_type for item in items]
    # Visuals first, then slicers, filters, buttons, groups
    assert kinds == sorted(kinds, key=["Visual", "Slicer", "Filter", "Button", "Group"].index)

    table = next(i for i in items if i.id == "tbl1")
    assert table.visual_filters == [("Dates[Year]", "> 2019 and < 2030")]
    page_filter = next(i for i in items if i.item_type == "Filter")
    assert (page_filter.filter_field, page_filter.filter_condition) == ("Warehouses[Code]", "<> D1")
    button = next(i for i in items if i.id == "btn1")
    assert button.first_row["Name"] == "Panel Open"


def test_run_cmd_end_to_end(tmp_path, monkeypatch):
    """The whole pipeline on the sample report + model, without Tabular Editor."""
    write_sample_pbix(tmp_path / "Sample.pbix")
    (tmp_path / "Sample.bim").write_text(json.dumps(BIM), encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(extractor, "_PBIX_", ["Sample", str(tmp_path)])
    monkeypatch.setattr(extractor, "_BIM_", ["Sample", str(tmp_path)])
    monkeypatch.setattr(extractor, "SAVE_NAME", "Sample")
    monkeypatch.setattr(extractor, "RUN_TE_ANALYSIS", False)
    monkeypatch.setattr(extractor, "LOG_DATA", False)

    assert extractor.run_cmd() in ("Success", "Log")

    output = tmp_path / "output" / "Sample"
    assert (output / "Sample_Relationships.png").is_file()
    with zipfile.ZipFile(output / "Sample.xlsx") as main:
        workbook = main.read("xl/workbook.xml").decode()
    for sheet in ("Sample Common", "Sales", "Pages", "model tables", "model columns"):
        assert f'name="{sheet}"' in workbook
    assert 'name="Detail"' in workbook  # pages without visuals get a sheet too
    assert "model quality" not in workbook  # BPA not run
    with zipfile.ZipFile(output / "Sample.xlsx") as main:
        main_strings = main.read("xl/sharedStrings.xml").decode()
    assert "Depends On" in main_strings and "Dependants" not in main_strings
    assert "Calculated Column" in main_strings  # calculated columns are listed with their DAX
    assert "(no visuals, buttons or page filters on this page)" in main_strings

    # openpyxl is not a dependency, so inspect the workbook XML directly
    with zipfile.ZipFile(output / "Sample_data.xlsx") as data_book:
        names = data_book.read("xl/workbook.xml").decode()
        strings = data_book.read("xl/sharedStrings.xml").decode()
    for sheet in (
        "pages",
        "common",
        "relationships",
        "unused measures",
        "dependencies",
        "report pages",
        "bookmarks",
        "report filters",
    ):
        assert f'name="{sheet}"' in names
    assert "Sales[Dynamic]" in strings  # unused measure listed
    assert "Panel Open" in strings  # bookmark button target
    assert "Sync group: Year" in strings  # interactivity column

    # Conditions like "= False" must be text, not Excel formulas
    with zipfile.ZipFile(output / "Sample_data.xlsx") as data_book:
        sheets = [n for n in data_book.namelist() if n.startswith("xl/worksheets/sheet")]
        assert not any(b"<f>" in data_book.read(sheet) for sheet in sheets)
    assert "= False" in strings


def test_page_info_bookmarks_and_interactivity(model, report):
    documentation = _documentation(model, report)

    info = {p.name: p for p in documentation.page_info}
    assert info["Detail"].hidden and not info["Sales"].hidden  # empty pages are listed too
    assert info["Sales"].changed_interactions == 1
    assert info["Sales"].sync_groups == ["Year"]
    assert info["Sales"].broken_buttons == 1  # btn4 points to a deleted bookmark
    assert info["Detail"].broken_buttons == 0

    assert documentation.interactivity[("Sales", "slc1")] == [
        "Sync group: Year",
        "No effect on Table (tbl1)",
    ]
    assert documentation.interactivity[("Sales", "grp1")] == ["Hidden on page"]
    assert documentation.interactivity_text("Sales", "btn1") == ""

    bookmarks = {b.display_name: b for b in documentation.bookmarks}
    panel = bookmarks["Panel Open"]
    assert (panel.captures, panel.applies_to, panel.page) == (
        "Display, Current page",
        "2 selected visuals",
        "Sales",
    )
    assert panel.hidden_visuals == ["Table (tbl1) on Sales", "Panel (grp1) on Sales"]
    assert panel.used_by == ["Sales (btn1)"]

    nested = bookmarks["Nested"]
    assert nested.group == "Group"
    assert nested.used_by == ["Sales (shp2)"]  # the clickable shape
    assert nested.captures == "Data, Display, Current page"  # defaults: everything


def test_resolve_hierarchy_columns(model):
    report_info = pd.DataFrame(
        [
            # Level "Year" is backed by column "Year Number"; the report only knows the level
            ["P", "slicer", "v1", "Dates", "Year", "Date Hierarchy: Year", "Hierarchy"],
            ["P", "slicer", "v1", "Dates", "Month", "Date Hierarchy: Month", "Hierarchy"],
            ["P", "tableEx", "v2", "Dates", "Year", None, "Values"],  # not a hierarchy row
            ["P", "slicer", "v3", "Dates", "Week", "Unknown Hierarchy: Week", "Hierarchy"],
        ],
        columns=REPORT_COLUMNS,
    )
    resolved = resolve_hierarchy_columns(report_info, model)
    assert list(resolved["Name"]) == ["Year Number", "Month", "Year", "Week"]
