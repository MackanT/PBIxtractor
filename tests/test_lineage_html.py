"""Tests for the interactive lineage viewer (lineage_html.py)."""

import json
import logging
import tempfile
from pathlib import Path

import pytest

import pbixtractor.report_extractor as extractor
from pbixtractor.documentation import build_documentation
from pbixtractor.json_report import documentation_to_dict
from pbixtractor.lineage_html import build_viewer_data, render_lineage_html, write_lineage_html
from pbixtractor.semantic_model import model_to_dataset, parse_model
from pbixtractor.tabular_editor import parse_dependencies

from .sample_layout import write_sample_pbix
from .test_semantic_model import BIM

DEPENDENCIES = parse_dependencies(
    "SourceType\tSourceTable\tSourceName\tTargetType\tTargetTable\tTargetName\n"
    "Measure\tSales\tTotal Amount\tColumn\tSales\tAmount\n"
    "Measure\tSales\tDynamic\tMeasure\tSales\tTotal Amount\n"
)


@pytest.fixture(scope="module")
def doc():
    folder = Path(tempfile.mkdtemp())
    write_sample_pbix(folder / "Sample.pbix")
    report = extractor.ReportExtractor(str(folder), "Sample.pbix")
    report.extract()
    model = parse_model(BIM)
    documentation = build_documentation(
        report_items=report.result,
        report_filters=report.filters,
        model=model,
        dataset=model_to_dataset(model),
        report_name="Sample </script> & Co",  # must not break the page
        description_tag="////",
        visual_mapper=extractor.visual_mapper,
        visual_types=extractor.visual_type_list,
        logger=logging.getLogger("test"),
        exact_dependencies=DEPENDENCIES,
        report=report.report,
    )
    return documentation_to_dict(documentation)


@pytest.fixture(scope="module")
def data(doc):
    return build_viewer_data(doc)


def test_edges_point_from_data_to_report(data):
    edges = {(s, t, kind) for s, t, kind in data["edges"]}
    assert ("source:gold.dim_date", "table:Dates", "loads_from") in edges
    assert ("table:Sales", "column:Sales[Amount]", "contains") in edges
    assert ("column:Sales[Amount]", "measure:Sales[Total Amount]", "depends_on") in edges
    assert ("measure:Sales[Total Amount]", "measure:Sales[Dynamic]", "depends_on") in edges
    assert ("column:Sales[Amount]", "visual:Sales/tbl1", "uses") in edges
    assert ("visual:Sales/tbl1", "page:Sales", "contains") in edges
    assert not any(kind == "relationship" for _, _, kind in edges)


def test_layers_follow_the_flow(data):
    layer = {n["id"]: n["layer"] for n in data["nodes"]}
    chain = [
        "source:gold.dim_date",
        "table:Dates",
        "column:Sales[Amount]",
        "measure:Sales[Total Amount]",
        "measure:Sales[Dynamic]",  # depends on Total Amount: one layer further
        "visual:Sales/tbl1",
        "page:Sales",
    ]
    layers = [layer[node] for node in chain]
    assert layers == sorted(layers) and len(set(layers)) == len(layers)


def test_labels_details_and_unused(data):
    nodes = {n["id"]: n for n in data["nodes"]}
    assert nodes["visual:Sales/btn1"]["label"] == "Button → Panel Open"
    assert nodes["visual:Sales/btn4"]["label"].startswith("⚠ ")  # deleted bookmark
    assert nodes["visual:Sales/slc1"]["label"].startswith("Slicer: ")
    assert nodes["visual:Sales/grp1"]["label"] == "Panel: Filter Popup"

    measure = nodes["measure:Sales[Total Amount]"]
    assert measure["details"]["Expression"] == "SUM ( Sales[Amount] )"
    assert nodes["measure:Sales[Dynamic]"]["unused"] is True
    assert "Unused" in nodes["measure:Sales[Dynamic]"]["details"]
    assert nodes["page:Detail"]["details"]["Hidden"] is True
    assert "Sync group: Year" in nodes["visual:Sales/slc1"]["details"]["Interactivity"]


def test_html_is_self_contained_and_safe(doc, tmp_path):
    html = render_lineage_html(doc)
    assert html.startswith("<!doctype html>")
    assert "http://" not in html.replace("http://www.w3.org/2000/svg", "")  # no external loads
    assert "https://" not in html
    # The report name contains "</script>": it must be escaped in the title and the data
    assert "<title>Sample &lt;/script&gt; &amp; Co - lineage</title>" in html
    assert html.count("</script>") == 3  # only the theme switch, data and code blocks close

    start = html.index('<script type="application/json" id="data">') + len(
        '<script type="application/json" id="data">'
    )
    payload = json.loads(html[start : html.index("</script>", start)])
    assert payload["report"] == "Sample </script> & Co"

    path = tmp_path / "lineage.html"
    write_lineage_html(path, doc)
    assert path.read_text(encoding="utf-8") == html


@pytest.mark.parametrize(
    "ref, expected",
    [
        ("Sales[Amount]", ("Sales", "Amount")),
        ("'Sales Data'[Amount]", ("Sales Data", "Amount")),
        ("Sales[Price [EUR]]]", ("Sales", "Price [EUR]")),
        ("'Params'", ("Params", "")),
        ("'Bob''s'[X]", ("Bob's", "X")),
    ],
)
def test_split_ref(ref, expected):
    from pbixtractor.json_report import _split_ref

    assert _split_ref(ref) == expected


def test_html_has_guide(doc):
    html = render_lineage_html(doc)
    assert 'id="help"' in html and 'id="guide"' in html
    assert "How to read the lineage view" in html


def test_page_follows_the_app_theme_and_embeds_its_fonts(doc):
    from pbixtractor.lineage_html import render_lineage_html

    html = render_lineage_html(doc)
    assert 'font-family: "Source Sans 3"' in html and "data:font/woff2;base64," in html
    assert ':root[data-theme="dark"]' in html  # set by ?theme= or the app's toggle message
    assert 'e.data && e.data.pbixtractorTheme' in html
    assert "#2563eb" not in html  # the old blue accent is gone
