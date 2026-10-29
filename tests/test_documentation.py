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


# ============================================================================
# "Model quality" column (BPA findings per object, main workbook only)
# ============================================================================


def _xlsx_rows(path, sheet: str) -> list[list[str]]:
    """A sheet's cell texts as rows (openpyxl is not a dependency: read the XML)."""
    import re
    import xml.etree.ElementTree as ET

    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rel_ns = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    with zipfile.ZipFile(path) as book:
        workbook = ET.fromstring(book.read("xl/workbook.xml"))
        rels = ET.fromstring(book.read("xl/_rels/workbook.xml.rels"))
        targets = {r.get("Id"): r.get("Target") for r in rels}
        rid = next(
            s.get(rel_ns) for s in workbook.iter(f"{{{ns['m']}}}sheet") if s.get("name") == sheet
        )
        strings = [
            "".join(t.text or "" for t in si.iter(f"{{{ns['m']}}}t"))
            for si in ET.fromstring(book.read("xl/sharedStrings.xml")).findall("m:si", ns)
        ]
        root = ET.fromstring(book.read("xl/" + targets[rid]))
    rows = []
    for row in root.iter(f"{{{ns['m']}}}row"):
        cells = {}
        for cell in row.findall("m:c", ns):
            column = re.match(r"[A-Z]+", cell.get("r")).group()
            index = 0
            for char in column:
                index = index * 26 + ord(char) - 64
            value = cell.find("m:v", ns)
            if value is not None:
                text = strings[int(value.text)] if cell.get("t") == "s" else value.text
                cells[index - 1] = text
        number = int(row.get("r")) - 1
        rows.extend([] for _ in range(number - len(rows) + 1))
        rows[number] = [cells.get(i, "") for i in range(max(cells, default=-1) + 1)]
    return rows


def _violation(object_type, object_name, rule, severity):
    from pbixtractor.tabular_editor import BpaViolation

    return BpaViolation(
        object_type=object_type, object_name=object_name, rule=rule, severity=severity
    )


ODD_TABLE = {
    "name": "O'Brien",
    "columns": [{"name": "Key", "dataType": "int64", "sourceColumn": "Key"}],
    "measures": [{"name": "Net]Total", "expression": "1"}],
}

VIOLATIONS = [
    _violation("Measure", "'Sales'[Total Amount]", "[Formatting] Provide format string", "Low"),
    _violation("Measure", "'sales'[TOTAL AMOUNT]", "[DAX] Use DIVIDE", "High"),  # case-insensitive
    _violation("Measure", "'Sales'[Total Amount]", "[DAX] Avoid IFERROR", "High"),
    _violation(
        "Calculated Column", "'Sales'[Is Big]", "[Performance] Avoid calculated columns", "Medium"
    ),
    _violation(
        "Column", "'Sales'[Amount]", "[Performance] Do not use floating point data types", "High"
    ),
    _violation("Table", "'Dates'", "[Metadata] Mark the date table", "Medium"),
    _violation(
        "Relationship",
        "'Sales'[Date Key] -> 'Dates'[Date Key]",
        "[Model] Relationship rule",
        "High",
    ),
    _violation("Measure", "'O''Brien'[Net]]Total]", "[Naming] Odd name", "Low"),
    _violation("Data Column", "'O''Brien'[Key]", "[Metadata] Hide keys", "Low"),
]


@pytest.mark.parametrize(
    "name, expected",
    [
        ("'Sales'[Amount]", ("Sales", "Amount")),
        ("Sales[Amount]", ("Sales", "Amount")),
        ("[Total]", (None, "Total")),
        ("'O''Brien'[Net]]Total]", ("O'Brien", "Net]Total")),
        ("'Dates'", (None, "Dates")),
        ("Dates", (None, "Dates")),
    ],
)
def test_parse_bpa_object_name(name, expected):
    from pbixtractor.model_sheets import parse_bpa_object_name

    assert parse_bpa_object_name(name) == expected


def test_bpa_by_object_sorts_and_skips_other_types():
    from pbixtractor.model_sheets import bpa_by_object, bpa_cell

    cells = bpa_by_object(VIOLATIONS)
    assert bpa_cell(cells, "SALES", "total amount") == (
        "High: [DAX] Avoid IFERROR\nHigh: [DAX] Use DIVIDE\nLow: [Formatting] Provide format string"
    )
    assert bpa_cell(cells, "Dates") == "Medium: [Metadata] Mark the date table"
    assert bpa_cell(cells, "O'Brien", "Net]Total") == "Low: [Naming] Odd name"
    assert bpa_cell(cells, "Sales") == ""  # no table finding
    assert not any("Relationship rule" in text for text in cells.values())


def _write_quality_workbooks(tmp_path, report, violations):
    from copy import deepcopy

    from pbixtractor.excel_report import write_data_workbook, write_main_workbook
    from pbixtractor.relationship_graph import save_relationship_graph

    bim = deepcopy(BIM)
    bim["model"]["tables"].append(ODD_TABLE)
    model = parse_model(bim)
    documentation = _documentation(model, report, bpa_violations=violations)
    main, data = tmp_path / "Sample.xlsx", tmp_path / "Sample_data.xlsx"
    graph = str(tmp_path / "graph.png")
    save_relationship_graph(documentation.relations, graph)
    write_main_workbook(str(main), documentation, graph, extractor.known_functions)
    write_data_workbook(str(data), documentation, graph, extractor.known_functions)
    return main, data


def _quality_by_row(rows: list[list[str]], header_row: int, *key_columns: int) -> dict:
    headers = rows[header_row]
    assert headers[-1] == "Model quality"
    column = len(headers) - 1
    return {
        tuple(row[i] for i in key_columns): (row[column] if len(row) > column else "")
        for row in rows[header_row + 1 :]
        if row and all(row[i] for i in key_columns)
    }


def test_model_quality_column_in_main_workbook(tmp_path, report):
    main, data = _write_quality_workbooks(tmp_path, report, VIOLATIONS)

    common = _xlsx_rows(main, "Sample Common")
    header_row = next(
        i for i, row in enumerate(common) if row[: len(OBJECT_COLUMNS)] == OBJECT_COLUMNS
    )
    assert len(common[header_row]) == len(OBJECT_COLUMNS) + 1  # added at the end
    name, table = OBJECT_COLUMNS.index("Name"), OBJECT_COLUMNS.index("Table")
    common_rows = common[header_row + 1 :]
    end = next((i for i, row in enumerate(common_rows) if not row), len(common_rows))
    quality = _quality_by_row(common[: header_row + 1 + end], header_row, table, name)
    assert quality[("Sales", "Total Amount")] == (
        "High: [DAX] Avoid IFERROR\nHigh: [DAX] Use DIVIDE\nLow: [Formatting] Provide format string"
    )
    assert quality[("Sales", "Is Big")] == "Medium: [Performance] Avoid calculated columns"
    assert quality[("O'Brien", "Net]Total")] == "Low: [Naming] Odd name"
    assert quality[("Sales", "Dynamic")] == ""

    tables = _quality_by_row(_xlsx_rows(main, "model tables"), 0, 0)
    assert tables == {
        ("Sales",): "",
        ("Dates",): "Medium: [Metadata] Mark the date table",
        ("O'Brien",): "",
    }
    columns = _quality_by_row(_xlsx_rows(main, "model columns"), 0, 0, 1)
    assert (
        columns[("Sales", "Amount")] == "High: [Performance] Do not use floating point data types"
    )
    assert columns[("Sales", "Is Big")] == "Medium: [Performance] Avoid calculated columns"
    assert columns[("O'Brien", "Key")] == "Low: [Metadata] Hide keys"
    assert columns[("Dates", "Month")] == ""

    with zipfile.ZipFile(main) as book:
        assert "Relationship rule" in book.read("xl/sharedStrings.xml").decode()  # quality sheet
    for sheet in ("Sample Common", "model tables", "model columns"):
        assert not any(
            "Relationship rule" in cell for row in _xlsx_rows(main, sheet) for cell in row
        )

    with zipfile.ZipFile(data) as book:
        assert "Model quality" not in book.read("xl/sharedStrings.xml").decode()


def test_model_quality_column_absent_without_bpa_and_empty_without_findings(tmp_path, report):
    main, _ = _write_quality_workbooks(tmp_path, report, None)
    for sheet in ("Sample Common", "model tables", "model columns"):
        assert not any("Model quality" in cell for row in _xlsx_rows(main, sheet) for cell in row)

    main, _ = _write_quality_workbooks(tmp_path, report, [])
    tables = _quality_by_row(_xlsx_rows(main, "model tables"), 0, 0)
    columns = _quality_by_row(_xlsx_rows(main, "model columns"), 0, 0, 1)
    assert tables and set(tables.values()) == {""}
    assert columns and set(columns.values()) == {""}
    common = _xlsx_rows(main, "Sample Common")
    assert any(row[len(OBJECT_COLUMNS) :] == ["Model quality"] for row in common)
