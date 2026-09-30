"""Tests for report extraction (visuals, buttons, filters) on a hand-built sample report.

The same sample report is tested in three formats: legacy .pbix (Report/Layout), PBIR .pbix
(Report/definition/) and a PBIP project folder. All must give identical results.
"""

import os
import re
import zipfile
from pathlib import Path

import pytest

from pbixtractor import ReportExtractor
from pbixtractor.extractors import ReportContext, clean_literal
from pbixtractor.readers import read_report
from pbixtractor.utils import excel_sheet_name

from .sample_layout import write_sample_pbix
from .sample_pbir import write_sample_pbip, write_sample_pbir_pbix

WRITERS = {
    "legacy": lambda folder: write_sample_pbix(folder / "Sample.pbix"),
    "pbir": lambda folder: write_sample_pbir_pbix(folder / "Sample.pbix"),
    "pbip": lambda folder: write_sample_pbip(folder),
}


@pytest.fixture(scope="module", params=list(WRITERS))
def sample_path(request, tmp_path_factory) -> Path:
    """The sample report written in each supported format."""
    return WRITERS[request.param](tmp_path_factory.mktemp(request.param))


@pytest.fixture(scope="module")
def extracted(sample_path):
    """Run ReportExtractor on the sample report and return (items, filters)."""
    extractor = ReportExtractor(str(sample_path.parent), sample_path.name)
    extractor.extract()
    return extractor.result, extractor.filters


def _rows(items, item_name):
    return [row for row in items if row[2] == item_name]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("'abc'", "abc"),
        ("'it''s'", "it's"),
        ("2019L", "2019"),
        ("-1L", "-1"),
        ("0D", "0"),
        ("1.5M", "1.5"),
        ("true", "True"),
        ("datetime'2024-01-01T00:00:00'", "2024-01-01T00:00:00"),
        (5, "5"),
    ],
)
def test_clean_literal(raw, expected):
    assert clean_literal(raw) == expected


def test_report_format_detected(sample_path):
    expected = "legacy" if "legacy" in str(sample_path) else "pbir"
    assert read_report(sample_path).format == expected


def test_report_context_includes_nested_bookmarks(sample_path):
    context = ReportContext.from_report(read_report(sample_path))
    assert context.page_names["ReportSectionB"] == "Detail"
    assert context.bookmark_names["Bookmark1"] == "Panel Open"
    assert context.bookmark_names["Bookmark2"] == "Nested"


def test_page_and_bookmark_details(sample_path):
    """Hidden pages, interactions, sync groups, hidden visuals and bookmarks in every format."""
    report = read_report(sample_path)
    sales, detail = report.pages
    assert (sales.hidden, detail.hidden) == (False, True)
    assert [(i.source, i.target, i.kind) for i in sales.interactions] == [("slc1", "tbl1", "None")]

    visuals = {v.name: v for v in sales.visuals}
    assert visuals["slc1"].sync_group == "Year"
    assert visuals["grp1"].hidden and not visuals["tbl1"].hidden

    bookmarks = {b.name: b for b in report.bookmark_details}
    panel = bookmarks["Bookmark1"]
    assert (panel.captures_data, panel.captures_display, panel.captures_page) == (
        False,
        True,
        True,
    )
    assert panel.page == "ReportSectionA"
    assert panel.target_visuals == ["tbl1", "grp1"]
    assert sorted(panel.hidden_visuals) == ["grp1", "tbl1"]
    assert bookmarks["Bookmark2"].group == "Group"


def test_fields_resolved_through_query_aliases(extracted):
    items, _ = extracted
    rows = _rows(items, "tbl1")
    assert ["Sales", "tableEx", "tbl1", "Sales", "Amount", None, "Values"] in rows
    # Stale Name "_Measures.Budget" must resolve to Budget[Budget OC]
    assert ["Sales", "tableEx", "tbl1", "Budget", "Budget OC", "Budget (OC)", "Values"] in rows


def test_formatting_measure_is_extracted(extracted):
    items, _ = extracted
    assert ["Sales", "tableEx", "tbl1", "_Measures", "Color Flag", None, "Formatting"] in items


def test_hierarchy_level(extracted):
    items, _ = extracted
    assert _rows(items, "slc1") == [
        ["Sales", "slicer", "slc1", "Dates", "Year Number", "Date Hierarchy: Year", "Hierarchy"]
    ]


def test_aggregation(extracted):
    items, _ = extracted
    assert ["Sales", "pivotTable", "mtx1", "Sales", "Qty", "Sum of Qty", "Values"] in items


def test_unbound_field_in_legacy_layout(tmp_path):
    # Legacy Select can hold fields not bound to a role (PBIR has no equivalent).
    # This previously crashed on self.log(); now reported as unknown.
    write_sample_pbix(tmp_path / "Sample.pbix")
    extractor = ReportExtractor(str(tmp_path), "Sample.pbix")
    extractor.extract()
    assert [
        "Sales",
        "pivotTable",
        "mtx1",
        "Sales",
        "Hidden",
        None,
        "UNKNOWN Data Type",
    ] in extractor.result


def test_buttons(extracted):
    items, _ = extracted
    assert _rows(items, "btn1") == [
        ["Sales", "actionButton", "btn1", "", "Panel Open", "Show panel", "Bookmark"]
    ]
    assert _rows(items, "btn2") == [
        ["Sales", "actionButton", "btn2", "", "Detail", "Go to detail", "PageNavigation"]
    ]
    assert _rows(items, "btn3") == [["Sales", "actionButton", "btn3", "", "", None, "No Action"]]
    assert _rows(items, "btn4") == [
        [
            "Sales",
            "actionButton",
            "btn4",
            "",
            "(missing bookmark: Bookmarkdeadbeef)",
            None,
            "Bookmark",
        ]
    ]


def test_group_and_skipped_shape(extracted):
    items, _ = extracted
    assert _rows(items, "grp1") == [["Sales", "Group", "grp1", "", "", "Filter Popup", "Group"]]
    assert _rows(items, "shp1") == []
    # Shapes with an action are documented as (bookmark in a group) buttons
    assert _rows(items, "shp2") == [
        ["Sales", "actionButton", "shp2", "", "Nested", None, "Bookmark"]
    ]


def test_filters(extracted):
    _, filters = extracted
    assert ["", "Is Future", "All Pages", "Dates", "Is Future", "=", "False"] in filters
    assert ["Sales", "Code", "This Page", "Warehouses", "Code", "<>", "D1"] in filters
    assert ["Sales", "tbl1", "Visual", "Dates", "Year", "", "> 2019 and < 2030"] in filters
    # The filter without a condition is skipped
    assert len(filters) == 3


def test_pbix_without_report_gives_clear_error(tmp_path):
    with zipfile.ZipFile(tmp_path / "Empty.pbix", "w") as zip_file:
        zip_file.writestr("DataModel", b"")
    with pytest.raises(ValueError, match="no Power BI report definition"):
        ReportExtractor(str(tmp_path), "Empty.pbix").extract()


def test_encrypted_pbix_gives_clear_error(tmp_path):
    # Purview-labelled files are wrapped in a .pfile container instead of a zip
    (tmp_path / "Labelled.pbix").write_bytes(b".pfile\x03\x00\x00\x00" + b"\x00" * 64)
    with pytest.raises(ValueError, match="encrypted by a sensitivity label"):
        read_report(tmp_path / "Labelled.pbix")


def test_encrypted_pbix_fails_the_run_without_a_traceback(tmp_path):
    from pbixtractor.pipeline import ExtractionOptions, run_extraction

    (tmp_path / "Labelled.pbix").write_bytes(b".pfile\x03\x00\x00\x00" + b"\x00" * 64)
    (tmp_path / "Labelled.bim").write_text('{"model": {}}', encoding="utf-8")
    result = run_extraction(ExtractionOptions(
        tmp_path / "Labelled.pbix", tmp_path / "Labelled.bim", tmp_path / "out",
        tabular_editor_analysis=False,
    ))
    assert result.status == "error" and "sensitivity label" in result.message
    assert "Traceback" not in result.logs


def test_non_zip_pbix_gives_clear_error(tmp_path):
    (tmp_path / "Broken.pbix").write_bytes(b"not a zip")
    with pytest.raises(ValueError, match="not a valid .pbix file"):
        read_report(tmp_path / "Broken.pbix")


def test_excel_sheet_name():
    used = set()
    assert excel_sheet_name("Sales/Region: [EU]", used) == "Sales_Region_ _EU_"
    long_name = "A very long page name that exceeds the limit"
    first = excel_sheet_name(long_name, used)
    second = excel_sheet_name(long_name, used)
    assert len(first) == 31 and len(second) <= 31
    assert first != second


# ----------------------------------------------------------------------------
# Optional: run against a real report kept outside the repo
#   $env:PBIXTRACTOR_SAMPLE_PBIX = "C:\path\to\Report.pbix"
# ----------------------------------------------------------------------------

SAMPLE_PBIX = os.environ.get("PBIXTRACTOR_SAMPLE_PBIX")


@pytest.mark.skipif(
    not SAMPLE_PBIX or not Path(SAMPLE_PBIX).is_file(),
    reason="Set PBIXTRACTOR_SAMPLE_PBIX to a local .pbix to run",
)
def test_real_report_smoke():
    path = Path(SAMPLE_PBIX)
    extractor = ReportExtractor(str(path.parent), path.name)
    extractor.extract()

    assert extractor.result, "no visual items extracted"
    assert not any(row[6] == "UNKNOWN Data Type" and not row[4] for row in extractor.result)
    # Buttons must be resolved to readable targets (or flagged as missing), not raw ids
    for row in extractor.result:
        if row[6] in ("Bookmark", "PageNavigation"):
            assert row[4] and not re.fullmatch(r"(Bookmark|ReportSection)[0-9a-f]*", row[4]), row
