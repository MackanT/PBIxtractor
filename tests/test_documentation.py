"""Tests for the analysis stage (documentation.py), the DAX helpers and the full pipeline."""

import json
import logging
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import pytest

import pbixtractor.report_extractor as extractor
from pbixtractor.constants import REPORT_COLUMNS
from pbixtractor.dax import highlight_dax, text_dependencies
from pbixtractor.documentation import (
    OBJECT_COLUMNS,
    build_documentation,
    build_objects,
    parse_tsv_object_name,
    resolve_hierarchy_columns,
    split_embedded_description,
)
from pbixtractor.pipeline import ExtractionOptions, run_extraction
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


@pytest.mark.parametrize(
    "name, expected",
    [
        ("Model.T.dbo.Customer.C.Id", ("Column", "dbo.Customer", "Id")),
        ("Model.T.dbo.Customer.M.Cnt", ("Measure", "dbo.Customer", "Cnt")),
        ("Model.T.dbo.Customer", ("Table", "dbo.Customer", "")),
        ("Model.T.01. Sales.C.Amount", ("Column", "01. Sales", "Amount")),
        ("Model.T.dbo.C.x", ("Column", "dbo", "x")),  # the shorter table still resolves
        ("Model.T.dbo.Customer.P.dbo.Customer-1", ("Table", "dbo.Customer", "")),
    ],
)
def test_parse_tsv_object_name_with_dotted_table_names(name, expected):
    tables = ["dbo", "dbo.Customer", "01. Sales"]
    assert parse_tsv_object_name(name, tables) == expected


@pytest.mark.parametrize(
    "dax, description, definition",
    [
        ("//// Sum of sales ////\nSUM(Sales[Amount])", "Sum of sales", "\nSUM(Sales[Amount])"),
        # inside the DAX (seen in real models): only the comment is removed
        ("VAR y =\n    //// Year ////\n    SELECTEDVALUE(D[Y])", "Year", "VAR y =\n    \n    SELECTEDVALUE(D[Y])"),
        ("SUM(Sales[Amt]) //// note", "", "SUM(Sales[Amt]) //// note"),  # one tag: not a description
        ("VAR x = 1\n//////////////\nRETURN x", "", "VAR x = 1\n//////////////\nRETURN x"),
        ("SUM(x)", "", "SUM(x)"),
    ],
)
def test_split_embedded_description(dax, description, definition):
    assert split_embedded_description(dax, "////") == (description, definition)


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


def _plain(segments, formats):
    return "".join(s for s in segments if s not in formats.values() and s not in formats["para"])


def test_highlight_dax_keeps_names_that_look_like_placeholders():
    formats = {k: k for k in ("comment", "quote", "var", "varname", "measure", "function", "return")}
    formats["para"] = [f"para{i}" for i in range(15)]
    text = _plain(highlight_dax("VAR YYY = 1 RETURN YYY + XXX && AAA || ZZZ", formats, []), formats)
    assert "YYY" in text and "XXX" in text and "AAA" in text and "ZZZ" in text
    assert "\n" not in text and "\t" not in text  # no name was turned into a newline or tab
    assert "&& " in text and "|| " in text


def test_highlight_dax_survives_deep_nesting_and_a_trailing_var():
    formats = {k: k for k in ("comment", "quote", "var", "varname", "measure", "function", "return")}
    formats["para"] = [f"para{i}" for i in range(15)]
    deep = "(" * 20 + "[M]" + ")" * 20
    assert "M" in _plain(highlight_dax(deep, formats, []), formats)
    assert "var" in _plain(highlight_dax("SUM ( x ) // no var", formats, ["SUM"]), formats)


def test_text_dependencies_with_quoted_table_names():
    deps = text_dependencies("SUM ( 'Sales Data'[Amount] ) + 'Bob''s'[X]")
    assert sorted(deps) == ["Bob's[X]", "Sales Data[Amount]"]


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


def test_workbooks_end_to_end(tmp_path):
    """The whole pipeline on the sample report + model, without Tabular Editor."""
    write_sample_pbix(tmp_path / "Sample.pbix")
    (tmp_path / "Sample.bim").write_text(json.dumps(BIM), encoding="utf-8")

    output = tmp_path / "output" / "Sample"
    result = run_extraction(
        ExtractionOptions(
            report_path=tmp_path / "Sample.pbix",
            model_path=tmp_path / "Sample.bim",
            output_dir=output,
            tabular_editor_analysis=False,
            write_log_file=False,
        )
    )
    assert result.ok, result.message

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
    assert not any(b.broken for b in documentation.bookmarks)


def test_bookmark_on_deleted_page_is_broken(model, report, caplog):
    items, filters, definition = report
    original = definition.bookmark_details[0].page
    definition.bookmark_details[0].page = "ReportSectionGone"
    try:
        with caplog.at_level(logging.WARNING, logger="test"):
            documentation = _documentation(model, report)
    finally:
        definition.bookmark_details[0].page = original  # the fixture is module-scoped

    bookmark = documentation.bookmarks[0]
    assert bookmark.broken
    assert bookmark.page == "(missing page: ReportSectionGone)"
    assert "no longer exists: ReportSectionGone" in caplog.text
    assert not any(b.broken for b in documentation.bookmarks[1:])


def _in_filter(table: str, column: str, *values: str, negate: bool = False) -> dict:
    """A filter-pane entry: table[column] in values (or not in)."""
    condition = {
        "In": {
            "Expressions": [
                {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": column}}
            ],
            "Values": [[{"Literal": {"Value": v}}] for v in values],
        }
    }
    if negate:
        condition = {"Not": {"Expression": condition}}
    return {
        "filter": {
            "Version": 2,
            "From": [{"Name": "t", "Entity": table, "Type": 0}],
            "Where": [{"Condition": condition}],
        }
    }


def _slicer_objects(selection: dict) -> dict:
    return {"general": [{"properties": {"filter": selection}}]}


def test_bookmark_filter_and_slicer_state():
    from pbixtractor.documentation import build_bookmarks
    from pbixtractor.readers import PageDefinition, ReportDefinition, VisualDefinition, _bookmark

    warehouse = _in_filter("Warehouses", "Code", "'D1'", negate=True)
    report = ReportDefinition(
        format="legacy",
        pages=[
            PageDefinition(
                "S1",
                "Sales",
                visuals=[
                    VisualDefinition(
                        "slc1", "slicer", objects=_slicer_objects(_in_filter("Dates", "Year", "2023L"))
                    )
                ],
                filters=[warehouse],
            )
        ],
    )
    state = {
        "activeSection": "S1",
        "filters": {"byExpr": [_in_filter("Dates", "Is Future", "false")]},
        "sections": {
            "S1": {
                # the saved page filter again, and a pane card without a condition (left out)
                "filters": {"byExpr": [warehouse, {"name": "f", "type": "Categorical"}]},
                "visualContainers": {
                    "slc1": {"singleVisual": {"objects": {"merge": _slicer_objects(
                        _in_filter("Dates", "Year", "2024L"))}}},
                    "gone1": {"filters": {"byExpr": [_in_filter("Sales", "Channel", "'Web'")]}},
                },
            }
        },
    }
    report.bookmark_details = [
        _bookmark({"name": "B1", "displayName": "Year 2024", "explorationState": state}),
        _bookmark({"name": "B2", "displayName": "No data", "explorationState": state,
                   "options": {"suppressData": True}}),
        _bookmark({"name": "B3", "displayName": "Slicer only", "explorationState": state,
                   "options": {"targetVisualNames": ["slc1"]}}),
    ]
    report_info = pd.DataFrame(columns=REPORT_COLUMNS)

    b1, b2, b3 = build_bookmarks(report, report_info, {})
    assert [f.text for f in b1.filters] == [
        "All pages: Dates[Is Future] = False (changed)",  # the report has no such filter
        "Sales: Warehouses[Code] <> D1",  # same as the page's saved filter
        "slc1 on Sales, selection: Dates[Year] = 2024 (changed)",  # saved selection is 2023
        "gone1 on Sales, visual filter: Sales[Channel] = Web (page/visual no longer exists)",
    ]
    assert [f.level for f in b1.filters] == ["All Pages", "This Page", "Slicer", "Visual"]
    assert b2.filters == []  # "Data" not captured: its stored state is never applied
    # "Selected visuals": only those visuals' state (plus report and page filters)
    assert [f.level for f in b3.filters] == ["All Pages", "This Page", "Slicer"]


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
