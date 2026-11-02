"""Tests for the interactive lineage viewer (lineage_html.py)."""

import json
import logging
import tempfile
from pathlib import Path

import pytest

import pbixtractor.report_extractor as extractor
from pbixtractor.documentation import build_documentation
from pbixtractor.json_report import documentation_to_dict
from pbixtractor.lineage_html import (
    build_multi_viewer_data,
    build_viewer_data,
    render_lineage_html,
    render_multi_lineage_html,
    write_lineage_html,
)
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


def test_viewer_interactions_stay_fast(doc):
    """The page's script avoids the measured slow paths (string checks: no JS test runner)."""
    html = render_lineage_html(doc)
    script = html[html.rindex("<script>") :]
    draw = script[script.index("function draw(") : script.index("function nodeOf(")]
    # Fit, render and reveal use the layout's own coordinates, never a DOM measurement
    assert "getBBox" not in script and "consolidate()" not in script
    # Text hidden when too small or while the wheel zooms: one class on the SVG root
    assert "TEXT_MIN_SCALE = 0.5" in script
    assert 'svg.classList.toggle("small-text", state.scale < TEXT_MIN_SCALE)' in script
    assert 'svg.classList.add("zooming")' in script
    assert "svg.small-text .node text, svg.zooming .node text { display: none; }" in html
    # No list lookups in the lineage walk / grouping (a Set)
    assert "ids.indexOf(" not in script and "inView = new Set(ids)" in script
    # Debounced search over text and an order computed once; rows built lazily, one handler set
    assert "SEARCH_DELAY = 150" in script and "setTimeout(function () { state.query = value" in script
    assert "n.hay = (n.label" in script and "LIST_ORDER.sort(" in script
    assert "matches.sort(" not in script and "LIST_PAGE = 100" in script
    assert 'row.addEventListener("click"' not in script
    # Nodes: one string per drawing (escaped), no listeners per node; dimming through one class
    assert "addEventListener" not in draw and "innerHTML" in draw
    assert '.replace(/</g, "&lt;")' in script and '.replace(/"/g, "&quot;")' in script
    assert "#viewport.has-mark .node:not(.rel):not(.marked) { opacity: .25; }" in html
    assert 'document.querySelectorAll(".node")' not in script
    # The details panel is laid out on its own: new details never relayout the graph
    assert "position: absolute; inset: 0; contain: strict;" in html


def test_big_lineages_are_grouped_and_fit_frames_the_item(doc):
    """Grouping, expanding, the new Fit and the off-screen markers (string checks)."""
    html = render_lineage_html(doc)
    script = html[html.rindex("<script>") :]
    # More than 40 of a kind in a layer: one group per table / page; the 600-node cut is gone
    assert "GROUP_THRESHOLD = 40" in script
    assert "MAX_NODES" not in script and "shown)" not in script
    assert "(grouped: " in script
    # A group node is stacked, has a "+" and expands on double-click / Enter / "+"
    assert '<rect class="stack"' in script and '<g class="expand">' in script
    assert "function expand(gid)" in script and 'if (groupNodes[id]) expand(id); else select(id);' in script
    assert 'id="collapse"' in html and "Collapse groups" in html
    # Jumping to an item inside a collapsed group expands it (list, details, #link)
    assert "function jump(id)" in script and 'addEventListener("hashchange"' in script
    # Fit: the item and its direct neighbours unless the whole view is readable; never above 1
    fit = script[script.index("function fit(") : script.index("function frame(")]
    assert "whole >= TEXT_MIN_SCALE" in fit and "graph.near[ids[0]]" in fit
    assert "Math.min(1, " in fit
    # Off-screen markers: from layout coordinates, at most one update per animation frame
    assert 'id="offscreen"' in html and html.count('class="btn off"') == 4
    assert "requestAnimationFrame(updateMarkers)" in script
    markers = script[script.index("function hiddenBySide(") : script.index("function updateMarkers(")]
    assert "graph.xy[id]" in markers and "getBoundingClientRect" not in markers
    assert "#offscreen { position: absolute; inset: 0; z-index: 2; pointer-events: none; contain: strict; }" in html


def test_overview_bands_wrapping_and_labels(doc):
    """Source bands, wrapped layers and sized labels (string checks: no JS test runner)."""
    import re

    html = render_lineage_html(doc)
    script = html[html.rindex("<script>") :]

    def number(name):
        return float(re.search(name + r" = ([\d.]+)", script).group(1))

    # Boxes as wide as the name needs, up to ~320 px; 2 lines, then "…"
    assert number("NODE_MIN_W") == 210 and number("NODE_MAX_W") == 320
    assert "function wrapLabel(text, room, font, maxLines)" in script
    assert 'wrapLabel(n.label, NODE_MAX_W - pads, "l", 2)' in script
    assert 'truncate(t.sub, s.w - t.pads, "s")' in script  # the sub-line: one line, cut
    # Widths from a per-character table: never a DOM (or canvas) measurement per node
    assert "getBBox" not in script and "getComputedTextLength" not in script
    assert "measureText" not in script
    table = re.search(r'var CHAR_W = \(("[^;]*)\)\.split', script).group(1)
    assert len("".join(re.findall(r'"([^"]*)"', table)).split(",")) == 95  # " " .. "~"
    # Readable text: at the threshold zoom a label is 9-10 px on screen; the CSS matches
    assert 9 <= number("LABEL_FONT") * number("TEXT_MIN_SCALE") <= 10
    assert f".node text {{ fill: var(--text); font-size: {number('LABEL_FONT'):g}px;" in html
    assert f".node .type {{ fill: var(--muted); font-size: {number('SUB_FONT'):g}px; }}" in html
    assert "svg.small-text .band text, svg.zooming .band text { display: none; }" in html
    # Tall layers wrap into columns closer together than the layers are
    assert "function wrapColumns(blocks, gap, limit)" in script
    assert number("WRAP_GAP") < number("GAP_X")
    assert "(size().h - TOP_H - 20) / TEXT_MIN_SCALE" in script
    # Overview: tables in a band per source ("No source" last), one edge per source and band
    assert '<g id="bands"></g><g id="edges"></g>' in html
    assert '"No source · "' in script and 'byId[id].label + " · " + tables(' in script
    assert "draw(ov.ids, ov.up, ov.down, null, ov.unused, feedsNone(ov.top), ov.bands)" in script
    assert "bandEdges[key]" in script
    # Fit on a big overview: from the left at the readable zoom (the markers show the rest)
    assert "function fromLeft(box)" in script and "fromLeft(graph.bounds)" in script
    # Counts formatted without toLocaleString (its first call is slow)
    assert "toLocaleString(" not in script


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


def test_the_report_is_the_level_above_its_pages(data):
    nodes = {n["id"]: n for n in data["nodes"]}
    report = nodes["report:Sample"]
    assert report["type"] == "report" and report["layer"] > nodes["page:Sales"]["layer"]
    assert ("page:Sales", "report:Sample", "contains") in {tuple(e) for e in data["edges"]}
    assert nodes["page:Sales"]["group"] == "Sample"  # "up a level" from a page goes here
    assert report["details"]["Pages"] == 2 and report["details"]["File"] == "Sample.pbix"
    assert data["stats"]["report"] == 1  # one report: the overview still shows pages


def test_rls_is_shown(data):
    assert data["rls"] == ["Nordics"]
    sales = next(n for n in data["nodes"] if n["id"] == "table:Sales")
    assert sales["details"]["Row-level security"].startswith("Nordics: ")


def _entry(doc: dict, key: str, name: str) -> dict:
    from pbixtractor.catalog import build_entry

    identity = {"key": key, "kind": "file", "label": f"File · C:/x/{name}.bim"}
    entry = build_entry(doc, identity, name, {"lineage": f"models/{key}/{name}_lineage.html"})
    return {**entry, "documented": "2026-10-06", "rls": ["Nordics"]}


def test_lineage_across_models(doc):
    import copy

    other = copy.deepcopy(doc)  # the same model again, with one measure changed
    for table in other["model"]["tables"]:
        for measure in table["measures"]:
            if measure["name"] == "Dynamic":
                measure["expression"] = "[Total Amount] * 3"
    data = build_multi_viewer_data(
        [(_entry(doc, "a", "Alpha"), build_viewer_data(doc)), (_entry(other, "b", "Beta"), build_viewer_data(other))]
    )
    nodes = {n["id"]: n for n in data["nodes"]}
    assert data["multi"] and data["stats"]["model"] == 2
    # Each model keeps its own items; one source loaded by both is one item
    assert nodes["a|table:Dates"]["model"] == "Alpha" and nodes["b|table:Dates"]["model"] == "Beta"
    source = nodes["source|gold.dim_date"]
    assert source["shared"] == 2 and source["details"]["Models"] == "Alpha, Beta"
    assert ["source|gold.dim_date", "a|table:Dates", "loads_from"] in data["edges"]
    assert ["source|gold.dim_date", "b|table:Dates", "loads_from"] in data["edges"]
    # Model nodes, linked only for the overviews (not in the lineage edges)
    model = nodes["model|a"]
    assert (model["type"], model["group"], model["link"]) == ("model", "File", "models/a/Alpha_lineage.html")
    assert ["source|gold.dim_date", "model|a", "loads_from"] in data["model_edges"]
    report = next(n["id"] for n in data["nodes"] if n["type"] == "report" and n["m"] == "a")
    assert ["model|a", report, "contains"] in data["model_edges"]
    assert not any("model|" in e[0] or "model|" in e[1] for e in data["edges"])
    # The same measure / column / table elsewhere: identical, or what differs
    assert data["same"]["a|measure:Sales[Total Amount]"] == [["b|measure:Sales[Total Amount]", True, []]]
    assert data["same"]["a|measure:Sales[Dynamic]"] == [["b|measure:Sales[Dynamic]", False, ["DAX"]]]
    assert data["same"]["a|table:Dates"] == [["b|table:Dates", True, []]]
    assert data["differs"] == {"a|measure:Sales[Dynamic]": True, "b|measure:Sales[Dynamic]": True}
    # One layering: the same type in the same column for every model
    assert {n["layer"] for n in data["nodes"] if n["type"] == "report"} == {nodes[report]["layer"]}
    assert data["rls"] == ["Alpha", "Beta"]
    html = render_multi_lineage_html([(_entry(doc, "a", "Alpha"), build_viewer_data(doc))])
    assert "<title>Lineage across models</title>" in html and '"multi":true' in html
