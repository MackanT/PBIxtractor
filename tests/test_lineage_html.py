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
    assert "Math.min(FIT_MAX, " in fit
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
    assert "(size().h - topH - 2 * EDGE - 10) / TEXT_MIN_SCALE" in script
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


def _two_models(doc):
    """The sample as two models, Alpha and Beta; Beta's "Dynamic" measure has other DAX."""
    import copy

    other = copy.deepcopy(doc)
    for table in other["model"]["tables"]:
        for measure in table["measures"]:
            if measure["name"] == "Dynamic":
                measure["expression"] = "[Total Amount] * 3"
    return [(_entry(doc, "a", "Alpha"), build_viewer_data(doc)), (_entry(other, "b", "Beta"), build_viewer_data(other))]


def _unpacked(data: dict) -> dict:
    """The multi data as the template unpacks it: model key / name per node, edges by id, and
    the former "same" (id -> [[other id, identical?, what differs]]) and "differs" maps."""
    ids = [n["id"] for n in data["nodes"]]
    nodes = {}
    for n in data["nodes"]:
        model = data["models"][n["mi"]] if "mi" in n else {"key": "", "name": ""}
        nodes[n["id"]] = {**n, "m": model["key"], "model": model["name"], "unused": n.get("unused", False)}
    same, differs = {}, {}
    for group in data["same"]:
        apart = {(a, b): names for a, b, names in group["apart"]}
        apart.update({(b, a): names for (a, b), names in list(apart.items())})
        for i, mine in zip(group["nodes"], group["versions"]):
            same[ids[i]] = [
                [ids[j], other == mine, apart.get((mine, other), [])]
                for j, other in zip(group["nodes"], group["versions"])
                if j != i
            ]
            if len(set(group["versions"])) > 1:
                differs[ids[i]] = True
    return {
        "nodes": nodes,
        "edges": [[ids[a], ids[b], kind] for a, b, kind in data["edges"]],
        "model_edges": [[ids[a], ids[b], kind] for a, b, kind in data["model_edges"]],
        "same": same,
        "differs": differs,
    }


def _former_same(models: list[tuple[dict, dict]], known: set[str]) -> tuple[dict, dict]:
    """The "same" / "differs" maps as build_multi_viewer_data wrote them before ticket 05: one
    list per member repeating every other member."""
    from pbixtractor.compare import compare_entries, differences

    same, differs = {}, {}
    groups = compare_entries([entry for entry, _ in models])
    for kind in ("measure", "column", "table"):
        for group in groups[kind]:
            ids = [f"{m['entry']}|{kind}:{m['id']}" for m in group["members"]]
            for member, node_id in zip(group["members"], ids):
                if node_id not in known:
                    continue
                pairs = [
                    [other_id, not differences(member, other), differences(member, other)]
                    for other, other_id in zip(group["members"], ids)
                    if other is not member and other_id in known
                ]
                if pairs:
                    same[node_id] = pairs
                    if any(not pair[1] for pair in pairs):
                        differs[node_id] = True
    return same, differs


def test_lineage_across_models(doc):
    data = build_multi_viewer_data(_two_models(doc))
    unpacked = _unpacked(data)
    nodes = unpacked["nodes"]
    assert data["multi"] and data["stats"]["model"] == 2
    # Each model keeps its own items; one source loaded by both is one item
    assert nodes["a|table:Dates"]["model"] == "Alpha" and nodes["b|table:Dates"]["model"] == "Beta"
    source = nodes["source|gold.dim_date"]
    assert source["shared"] == 2 and source["details"]["Models"] == "Alpha, Beta"
    assert source["m"] == "" and "mi" not in data["nodes"][list(nodes).index(source["id"])]
    assert ["source|gold.dim_date", "a|table:Dates", "loads_from"] in unpacked["edges"]
    assert ["source|gold.dim_date", "b|table:Dates", "loads_from"] in unpacked["edges"]
    # Model nodes, linked only for the overviews (not in the lineage edges)
    model = nodes["model|a"]
    assert (model["type"], model["group"], model["link"]) == ("model", "File", "models/a/Alpha_lineage.html")
    assert ["source|gold.dim_date", "model|a", "loads_from"] in unpacked["model_edges"]
    report = next(n["id"] for n in nodes.values() if n["type"] == "report" and n["m"] == "a")
    assert ["model|a", report, "contains"] in unpacked["model_edges"]
    assert not any("model|" in e[0] or "model|" in e[1] for e in unpacked["edges"])
    # The same measure / column / table elsewhere: identical, or what differs
    assert unpacked["same"]["a|measure:Sales[Total Amount]"] == [["b|measure:Sales[Total Amount]", True, []]]
    assert unpacked["same"]["a|measure:Sales[Dynamic]"] == [["b|measure:Sales[Dynamic]", False, ["DAX"]]]
    assert unpacked["same"]["a|table:Dates"] == [["b|table:Dates", True, []]]
    assert unpacked["differs"] == {"a|measure:Sales[Dynamic]": True, "b|measure:Sales[Dynamic]": True}
    # One layering: the same type in the same column for every model
    assert {n["layer"] for n in nodes.values() if n["type"] == "report"} == {nodes[report]["layer"]}
    assert data["rls"] == ["Alpha", "Beta"]
    html = render_multi_lineage_html([(_entry(doc, "a", "Alpha"), build_viewer_data(doc))])
    assert "<title>Lineage across models</title>" in html and '"multi":true' in html


def test_lineage_across_models_compact_shape_loses_nothing(doc):
    """Ticket 05 (D15): models by index, edges by node index, one "same" group per item - and
    everything the former per-member data held is still there."""
    import copy

    models = _two_models(doc)
    third = copy.deepcopy(models[0])  # a third model: groups of three, two versions
    third[0]["key"], third[0]["name"] = "c", "Gamma"
    models.append(third)
    data = build_multi_viewer_data(models)
    assert data["models"] == [{"key": "a", "name": "Alpha"}, {"key": "b", "name": "Beta"}, {"key": "c", "name": "Gamma"}]
    assert not any("m" in n or "model" in n for n in data["nodes"])
    assert all(n.get("unused", True) is True for n in data["nodes"])  # false is left out
    assert all(isinstance(e[0], int) and isinstance(e[1], int) for e in data["edges"] + data["model_edges"])
    assert "differs" not in data  # derived from the groups' versions
    group = next(g for g in data["same"] if data["nodes"][g["nodes"][0]]["id"] == "a|measure:Sales[Dynamic]")
    assert (group["kind"], group["versions"], group["apart"]) == ("measure", [1, 2, 1], [[1, 2, ["DAX"]]])

    # Rebuilt the template's way, the former data comes back exactly
    unpacked = _unpacked(data)
    same, differs = _former_same(models, set(unpacked["nodes"]))
    assert unpacked["same"] == same and unpacked["differs"] == differs
    assert len(same) == sum(len(g["nodes"]) for g in data["same"])
    former_nodes = {
        f"{entry['key']}|{n['id']}": n for entry, viewer in models for n in viewer["nodes"] if n["type"] != "source"
    }
    for node_id, former in former_nodes.items():
        node = unpacked["nodes"][node_id]
        assert {k: node[k] for k in ("label", "group", "type", "unused", "details")} == {
            k: former[k] for k in ("label", "group", "type", "unused", "details")
        }
        assert node.get("dax_formatted") == former.get("dax_formatted")
        assert node["m"] == node_id.split("|")[0]
    former_edges = {
        (f"{entry['key']}|{a}", f"{entry['key']}|{b}", kind)
        for entry, viewer in models for a, b, kind in viewer["edges"] if not a.startswith("source:")
    }
    assert former_edges <= {tuple(e) for e in unpacked["edges"]}

    # The template reads that shape: unpacked once on load, the details ask sameAs()
    html = render_multi_lineage_html(models)
    for needle in ("DATA.models", "sameAs(n.id)", "byIds(DATA.edges)", "DATA.differs[id] = true"):
        assert needle in html


def test_lineage_across_models_renders_catalog_entries_from_before(doc):
    """lineage.json written before the formatted DAX existed (no dax_formatted) still works."""
    models = _two_models(doc)
    for _, viewer in models:
        for node in viewer["nodes"]:
            node.pop("dax_formatted", None)
    data = build_multi_viewer_data(models)
    assert not any("dax_formatted" in n for n in data["nodes"])
    assert _unpacked(data)["same"]["a|measure:Sales[Dynamic]"] == [["b|measure:Sales[Dynamic]", False, ["DAX"]]]
    assert '"multi":true' in render_multi_lineage_html(models)


def test_formatted_dax_is_an_extra_field(doc, data):
    """D13: measures and calculated columns carry dax_formatted next to the DAX as written."""
    import copy

    nodes = {n["id"]: n for n in data["nodes"]}
    # A calculated column written over several lines: formatted differently, original kept
    column = nodes["column:Sales[Is Big]"]
    assert column["details"]["Expression"].startswith("IF (\n")
    assert column["dax_formatted"] == "IF ( Sales[Amount] > 100, TRUE ( ) )"
    # Already formatted: no extra field
    assert "dax_formatted" not in nodes["measure:Sales[Total Amount]"]
    # Tables (also calculated ones), pages, visuals: never
    assert not any("dax_formatted" in n for n in data["nodes"] if n["type"] not in ("measure", "column"))

    other = copy.deepcopy(doc)
    for table in other["model"]["tables"]:
        for measure in table["measures"]:
            if measure["name"] == "Total Amount":
                measure["expression"] = "sum(Sales[Amount])"
    measure = next(n for n in build_viewer_data(other)["nodes"] if n["id"] == "measure:Sales[Total Amount]")
    assert measure["details"]["Expression"] == "sum(Sales[Amount])"  # as written
    assert measure["dax_formatted"] == "SUM ( Sales[Amount] )"


def test_formatted_dax_is_not_given_to_a_calculated_table():
    from pbixtractor.lineage_html import _formatted_dax

    details = {"Expression": "filter(Sales,Sales[Amount]>1)"}
    assert _formatted_dax("table", details) is None
    assert _formatted_dax("measure", details) == "FILTER ( Sales, Sales[Amount] > 1 )"
    assert _formatted_dax("measure", {}) is None


def test_panels_list_details_dax_guide_and_fit(doc):
    """Ticket 04: the list in view, grouped details, the DAX toggle, the Guide hint, Fit's cap and
    the narrow details overlay (string checks: no JS test runner)."""
    import re

    html = render_lineage_html(doc)
    script = html[html.rindex("<script>") :]
    # The list: "In this view (N)" with a "Whole model" switch, kept in memory only
    assert '<div id="listHead" hidden><span id="listTitle"></span><button class="linkbtn" id="listScope">' in html
    assert '"In this view (" + fmt(graph.itemCount) + ")"' in script
    assert 'MULTI ? "All models" : "Whole model"' in script
    assert 'var listScope = "view"' in script  # memory only: never stored
    assert "listScope" not in "".join(re.findall(r"[^\n]*Storage[^\n]*", script))
    assert "renderResults();  // \"In this view\": the drawn items and groups" in script
    # Compact chips with "Only unused" in the same row
    assert '<div class="chips" id="types"><label class="chip unused is-off"' in html
    assert ".chip { display: inline-flex; align-items: center; gap: 3px; padding: 0 4px; font-size: 11px;" in html
    # Details: per type, collapsible, no 200 cap, lazy rows
    assert "ids.slice(0, 200)" not in script and "SECTION_OPEN = 10" in script
    assert 'html("details", "sect")' in script and 'byType(pair[0], ids)' in script
    assert "function lazyLinks(dd, ids, text)" in script
    # Formatted DAX with its toggle (sessionStorage, try/catch)
    assert "n.dax_formatted && !daxAsWritten" in script
    assert '"Show as written"' in script and '"Show formatted"' in script
    assert 'DAX_KEY = "pbixtractor.lineage.daxAsWritten"' in script
    # The Guide: never by itself; a hint until opened once (localStorage, every access caught)
    assert 'var GUIDE_KEY = "pbixtractor.lineage.guideSeen";' in script
    assert "lineageGuideSeen" not in script
    assert script.count("showGuide(true)") == 2  # only the Guide button and the hint open it
    assert 'id="guideHint" hidden' in html and "New here?" in html
    for access in re.findall(r"[^\n]*localStorage\.[gs]etItem[^\n]*", script):
        assert access.strip().startswith("try {"), access
    # Fit: never beyond 0.75, in the visible area (the details overlay left out)
    assert re.search(r"var FIT_MAX = ([\d.]+);", script).group(1) == "0.75"
    assert "function areaW() { return size().w - covered(); }" in script
    assert "var NARROW_W = 1200, DETAILS_W = 320, TAB_W = 26" in script
    assert "@media (max-width: 1199px)" in html and "#app.details-collapsed #details { width: 26px; }" in html
    assert 'id="detailsToggle"' in html


def test_title_is_inserted_last(doc):
    """A report named like a placeholder keeps its name; nothing is put into the data."""
    named = {**doc, "report": {**doc["report"], "name": "__DATA__"}}
    html = render_lineage_html(named)
    data = build_viewer_data(named)
    assert data["report"] == "__DATA__"
    assert "<title>__DATA__ - lineage</title>" in html
    start = html.index('<script type="application/json" id="data">') + len(
        '<script type="application/json" id="data">'
    )
    payload = json.loads(html[start : html.index("</script>", start)])
    assert payload["report"] == data["report"]
    # A measure named "__TITLE__" stays as it is in the data
    measure = next(n for n in data["nodes"] if n["type"] == "measure")
    from pbixtractor.lineage_html import _render

    html = _render({**data, "nodes": [{**measure, "label": "__TITLE__"}]}, "__DATA__")
    assert "<title>__DATA__</title>" in html and '"label":"__TITLE__"' in html


def test_review_fixes(doc):
    """Ticket 06: the review's bugs and decisions D20-D28 (string checks: no JS test runner)."""
    import re

    html = render_lineage_html(doc)
    script = html[html.rindex("<script>") :]
    # Group ids from what they group, never from their position
    assert '"group:" + (whole ? "layer|" : "table|")' in script
    assert '"group:" + groups++' not in script
    # The list head follows the view before the cached rows return early
    render = script[script.index("function renderResults(") : script.index("function appendRows(")]
    assert render.index("head.hidden = !canView") < render.index("key === listKey")
    # A resize: debounced, laid out again only when the wrapping or narrow mode changes
    assert "resizeTimer = setTimeout(function () {" in script and "}, 150);" in script
    assert "graph.columnsAt(wrapLimit()) === graph.columns" in script
    assert "graph.narrow === NARROW.matches" in script
    # A malformed #link opens the overview
    assert "try { linked = decodeURIComponent(location.hash.slice(1)); } catch (e)" in script
    # The toolbar (+ status) height: estimated, then measured (start, resize, Panels); no constant
    assert "TOOLBAR_H" not in script and "TOP_H" not in script
    assert script.count("measure();") == 2 and "if (measure() && !moved" in script
    # panToward: "unset" is null, the right side uses the node's own width
    pan = script[script.index("function panToward(") : script.index("function zoomBy(")]
    assert "best === null" in pan and "(best + b.w) * s" in pan and "NODE_MAX_W" not in pan
    # The "Show more" observers let go of wiped rows
    assert "moreObserver.disconnect()" in script and "detailsObserver.disconnect()" in script
    # D20: DAX does not wrap, it scrolls; D22: "Unused" on every page; D23: SUB_FONT 18
    assert "white-space: pre; overflow-x: auto;" in html
    assert "pre-wrap; word-break: break-word; font: 12px" not in html
    assert '<input type="checkbox" id="unusedOnly"> Unused</label>' in html
    assert "Only unused" not in html
    assert re.search(r"SUB_FONT = (\d+)", script).group(1) == "18"
    # D24: markers in an edge strip; the graph is clipped to the area inside it
    assert "var EDGE = 32" in script
    assert '<clipPath id="frame"><rect id="frameRect"/></clipPath>' in html
    assert '<g clip-path="url(#frame)"><g id="viewport">' in html
    for name in ("fit(", "fromLeft(", "reveal(", "hiddenBySide("):
        name = "function " + name
        body = script[script.index(name) :]
        assert "inner()" in body[: body.index("\n  }\n")], name
    # D25: the hint is a real button; D26: the browser's dblclick, no timer
    assert '<button type="button" id="guideHint" hidden' in html
    assert "Date.now()" not in script and "lastClick" not in script
    assert script.count('addEventListener("dblclick", openClicked)') == 2
    assert "if (ev.detail >= 2) return;" in script
    # D28: the graph size computed from the window at startup, not measured
    assert "function computedSize()" in script and "window.innerWidth" in script
    assert "svg.clientWidth" not in script

