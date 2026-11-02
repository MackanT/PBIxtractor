"""Interactive lineage viewer: one self-contained, offline HTML file.

Shows how data flows from source views through tables, columns and measures to visuals and
pages. Select any item to see everything upstream (what it is built from) and downstream
(what uses it, i.e. what breaks if it changes).

    write_lineage_html("Report_lineage.html", json_report.documentation_to_dict(documentation))

Built from the JSON documentation (json_report.py), so it can also be regenerated from a saved
.json file. No external libraries or network access: plain JavaScript + SVG.
"""

import json
from html import escape
from pathlib import Path

from .compare import compare_entries, differences
from .dax_format import format_dax
from .design import PAGE_THEME_SCRIPT, page_css

# Flow direction for each lineage edge type: True = same as the JSON edge, False = reversed.
# The viewer's edges always point from "data" to "report": source -> table -> column ->
# measure -> visual -> page.
_FLOW = {
    "loads_from": False,  # table -> source      becomes  source -> table
    "contains": None,  # page -> visual (reverse) or table -> field (keep), see below
    "uses": False,  # visual -> field        becomes  field -> visual
    "filters": False,  # visual -> field        becomes  field -> visual
    "depends_on": False,  # measure -> dependency  becomes  dependency -> measure
}

TYPE_ORDER = ["source", "table", "column", "measure", "visual", "page", "report"]


def _measure_depths(nodes: dict, edges: list[dict]) -> dict[str, int]:
    """Longest chain of measure -> measure dependencies below each measure."""
    below = {}
    for edge in edges:
        if edge["type"] == "depends_on" and nodes.get(edge["target"], {}).get("type") == "measure":
            below.setdefault(edge["source"], []).append(edge["target"])

    depths: dict[str, int] = {}

    def depth(node_id: str, visiting: set) -> int:
        if node_id in depths:
            return depths[node_id]
        if node_id in visiting:  # circular dependency: stop
            return 0
        visiting.add(node_id)
        value = max((depth(t, visiting) + 1 for t in below.get(node_id, [])), default=0)
        visiting.discard(node_id)
        depths[node_id] = value
        return value

    for node_id, node in nodes.items():
        if node["type"] == "measure":
            depth(node_id, set())
    return depths


def _details(doc: dict) -> dict[str, dict]:
    """Per node id: the fields shown in the details panel."""
    details: dict[str, dict] = {}
    unused = set(doc["unused"]["columns"]) | set(doc["unused"]["measures"])

    rls: dict[str, list[str]] = {}  # table -> "Role: filter" lines
    for role in doc["model"].get("roles", []):
        for table, dax in (role.get("filters") or {}).items():
            rls.setdefault(table, []).append(f"{role['name']}: {' '.join((dax or '').split())}")

    for table in doc["model"]["tables"]:
        details[f"table:{table['name']}"] = {
            "Storage mode": table.get("storage_mode"),
            "Connector": table.get("connector"),
            "Source objects": ", ".join(table.get("source_objects") or []) or None,
            "Rows": table.get("rows"),
            "Size (MB)": round(table["size_bytes"] / 1e6, 2) if table.get("size_bytes") else None,
            "Hidden": table.get("hidden") or None,
            "Description": table.get("description"),
            "Row-level security": "\n".join(rls.get(table["name"], [])) or None,
        }
        for column in table["columns"]:
            ref = f"{table['name']}[{column['name']}]"
            details[f"column:{ref}"] = {
                "Table": table["name"],
                "Data type": column.get("data_type"),
                "Kind": column.get("kind"),
                "Source column": column.get("source_column"),
                "Expression": column.get("expression"),
                "Hidden": column.get("hidden") or None,
                "Distinct values": column.get("distinct_values"),
                "Size (KB)": (
                    round(column["size_bytes"] / 1000, 1) if column.get("size_bytes") else None
                ),
                "Description": column.get("description"),
                "Unused": "Yes - not used by any visual, filter or DAX" if ref in unused else None,
            }
        for measure in table["measures"]:
            ref = f"{table['name']}[{measure['name']}]"
            details[f"measure:{ref}"] = {
                "Table": table["name"],
                "Expression": measure.get("expression"),
                "Format": measure.get("format"),
                "Display folder": measure.get("display_folder"),
                "Data type": measure.get("data_type"),
                "Hidden": measure.get("hidden") or None,
                "Description": measure.get("description"),
                "Unused": "Yes - not used by any visual, filter or DAX" if ref in unused else None,
            }

    pages_by_name = {page["name"]: page for page in doc["report"]["pages"]}
    for report in doc.get("reports", []):
        pages = [pages_by_name[p] for p in report["pages"] if p in pages_by_name]
        details[f"report:{report['name']}"] = {
            "File": report.get("file"),
            "Pages": len(pages),
            "Visuals": sum(
                1 for p in pages for i in p["items"] if i["type"] in ("Visual", "Slicer")
            ),
            "Hidden pages": sum(1 for p in pages if p.get("hidden")) or None,
        }

    for page in doc["report"]["pages"]:
        details[f"page:{page['name']}"] = {
            "Hidden": page.get("hidden") or None,
            "Page type": page.get("page_type"),
            "Visuals": sum(1 for i in page["items"] if i["type"] in ("Visual", "Slicer")),
            "Sync groups": ", ".join(page.get("sync_groups") or []) or None,
        }
        for item in page["items"]:
            if item["type"] == "Filter":
                continue
            fields = [
                f"{f['role']}: {f['table']}[{f['name']}]"
                + (f" ({f['display_name']})" if f["display_name"] != f["name"] else "")
                for f in item.get("fields", [])
            ]
            details[f"visual:{page['name']}/{item['id']}"] = {
                "Page": page["name"],
                "Title": item.get("title"),
                "Type": f"{item['type']} - {item['visual_type']}",
                "Action": (f"{item['action']}: {item['target']}" if item.get("action") else None),
                "Label": item.get("label") or item.get("name"),
                "Fields": "\n".join(fields) or None,
                "Visual filters": "\n".join(
                    f"{f['field']} {f['condition']}" for f in item.get("filters", [])
                )
                or None,
                "Interactivity": "\n".join(item.get("interactivity", [])) or None,
            }

    return {
        node_id: {k: v for k, v in values.items() if v not in (None, "", [])}
        for node_id, values in details.items()
    }


def _visual_labels(doc: dict) -> dict[str, str]:
    """Readable visual labels: what the visual shows, e.g. "Slicer: Month Name"."""
    labels = {}
    for page in doc["report"]["pages"]:
        for item in page["items"]:
            if item["type"] == "Filter":
                continue
            kind = item["visual_type"]
            if item["type"] == "Button":
                target = item.get("target") or item.get("label") or item.get("action")
                label = f"{kind} → {target}" if target else kind
                if item.get("broken"):
                    label = f"⚠ {label}"
            elif item["type"] == "Group":
                label = f"{kind}: {item.get('name') or item['id']}"
            elif item.get("title") and not item["title"].startswith("fx:"):
                label = f"{kind}: {item['title']}"  # its title says best what it shows
            else:
                names = list(dict.fromkeys(f["display_name"] for f in item.get("fields", [])))
                shown = ", ".join(names[:2]) + (f" +{len(names) - 2}" if len(names) > 2 else "")
                label = f"{kind}: {shown}" if shown else kind
            labels[f"visual:{page['name']}/{item['id']}"] = label
    return labels


def _formatted_dax(node_type: str, details: dict) -> str | None:
    """The details panel's formatted DAX of a measure or calculated column (dax_format), only
    when it differs from the expression as written; "Expression" itself stays as written."""
    expression = details.get("Expression")
    if node_type not in ("measure", "column") or not isinstance(expression, str):
        return None
    formatted = format_dax(expression)
    return formatted if formatted != expression else None


def build_viewer_data(doc: dict) -> dict:
    """
    Nodes (with layer and details) and flow-direction edges for the viewer.

    Args:
        doc: JSON documentation from json_report.documentation_to_dict()

    Returns:
        {"report": name, "nodes": [...], "edges": [[source, target, type], ...], "stats": {...}}
    """
    lineage = doc["lineage"]
    nodes = {n["id"]: dict(n) for n in lineage["nodes"]}
    unused = {f"column:{r}" for r in doc["unused"]["columns"]}
    unused |= {f"measure:{r}" for r in doc["unused"]["measures"]}

    edges = []
    for edge in lineage["edges"]:
        kind = edge["type"]
        if kind == "relationship":
            continue  # model structure, not data lineage
        source, target = edge["source"], edge["target"]
        if kind == "contains":
            keep = nodes.get(source, {}).get("type") == "table"  # table -> field stays
        else:
            keep = _FLOW.get(kind, True)
        edges.append([source, target, kind] if keep else [target, source, kind])

    depths = _measure_depths(nodes, lineage["edges"])
    max_depth = max(depths.values(), default=0)
    layer_of = {
        "source": 0,
        "table": 1,
        "column": 2,
        "visual": 4 + max_depth,
        "page": 5 + max_depth,
        "report": 6 + max_depth,
    }
    details = _details(doc)
    visual_labels = _visual_labels(doc)

    viewer_nodes = []
    for node_id, node in nodes.items():
        node_type = node["type"]
        layer = 3 + depths.get(node_id, 0) if node_type == "measure" else layer_of[node_type]
        viewer_node = {
            "id": node_id,
            "type": node_type,
            "label": visual_labels.get(node_id, node["label"]),
            # a field's table, a visual's page, a page's report
            "group": node.get("table") or node.get("page") or node.get("report") or "",
            "layer": layer,
            "unused": node_id in unused,
            "details": details.get(node_id, {}),
        }
        formatted = _formatted_dax(node_type, viewer_node["details"])
        if formatted:
            viewer_node["dax_formatted"] = formatted
        viewer_nodes.append(viewer_node)

    counts = {t: sum(1 for n in viewer_nodes if n["type"] == t) for t in TYPE_ORDER}
    return {
        "report": doc["report"]["name"],
        "generator": doc.get("generator", ""),
        "exact": doc.get("dependencies_exact", False),
        # Row-level security roles: the page says the model is protected
        "rls": [role["name"] for role in doc["model"].get("roles", [])],
        "nodes": viewer_nodes,
        "edges": edges,
        "stats": counts,
    }


def _source_key(label: str) -> str:
    """Sources of different models are the same when their names are ("[dbo].[X]" = "dbo.x").
    Names are not server/database-qualified yet, so equal names in different databases match."""
    return " ".join(label.replace("[", "").replace("]", "").replace('"', "").lower().split())


def build_multi_viewer_data(models: list[tuple[dict, dict]]) -> dict:
    """
    One viewer data set over several models: the catalog's lineage across models.

    Every model's nodes keep their own ids behind "<catalog key>|"; a source with the same name
    in several models becomes one node ("source|<name>"), so lineage runs from it into each
    model. Model nodes ("model|<key>") and model_edges (source -> model -> report) are only
    for the overviews. same: measure/column/table id -> [[the same item in another model,
    identical?, what differs], ...] (compare.compare_entries); differs: the ids of items that
    are not the same in every model (marked "≠").

    Args:
        models: [(catalog entry, that model's build_viewer_data())]
    """
    # One layering for all: measures stay at 3 + their own depth, the deepest model decides
    # where visuals / pages / reports go
    deepest = max(
        (n["layer"] - 3 for _, data in models for n in data["nodes"] if n["type"] == "measure"), default=0
    )
    layer_of = {"source": 0, "table": 1, "column": 2, "visual": 4 + deepest, "page": 5 + deepest,
                "report": 6 + deepest}
    nodes: list[dict] = []
    edges: list[list[str]] = []
    model_edges: list[list[str]] = []
    shared: dict[str, dict] = {}
    rls = []
    for entry, data in models:
        key, name = entry["key"], entry["name"]
        model_id = f"model|{key}"
        local: dict[str, str] = {}
        sources, reports = set(), []
        for node in data["nodes"]:
            if node["type"] == "source":
                norm = _source_key(node["label"])
                merged = shared.setdefault(
                    norm,
                    {**node, "id": f"source|{norm}", "m": "", "model": "", "layer": 0,
                     "details": dict(node["details"]), "models": []},
                )
                if name not in merged["models"]:
                    merged["models"].append(name)
                local[node["id"]] = merged["id"]
                sources.add(merged["id"])
                continue
            copy = {**node, "id": f"{key}|{node['id']}", "m": key, "model": name}
            if node["type"] != "measure":
                copy["layer"] = layer_of.get(node["type"], node["layer"])
            local[node["id"]] = copy["id"]
            nodes.append(copy)
            if node["type"] == "report":
                reports.append(copy)
        edges += [[local[a], local[b], kind] for a, b, kind in data["edges"] if a in local and b in local]
        stats = data.get("stats", {})
        nodes.append({
            "id": model_id,
            "type": "model",
            "label": name,
            # where it comes from, short: "Fabric · <workspace>"; a file's path is in its details
            "group": "File" if entry["source"].get("kind") == "file" else entry["source"].get("label", ""),
            "m": key,
            "model": name,
            "layer": 1,
            "unused": False,
            "link": entry.get("links", {}).get("lineage"),
            "details": {k: v for k, v in {
                "Source": entry["source"].get("label"),
                "Reports": ", ".join(r["label"] for r in reports) or None,
                "Tables": stats.get("table"),
                "Measures": stats.get("measure"),
                "Visuals": stats.get("visual"),
                "Documented": entry.get("documented"),
                "Row-level security": ", ".join(entry.get("rls") or []) or None,
                "Not included": "\n".join(entry.get("not_included") or []) or None,
            }.items() if v not in (None, "", 0)},
        })
        model_edges += [[s, model_id, "loads_from"] for s in sorted(sources)]
        model_edges += [[model_id, r["id"], "contains"] for r in reports]
        if entry.get("rls"):
            rls.append(name)  # the roles are in the model's details

    for merged in shared.values():
        models_of = merged.pop("models")
        if len(models_of) > 1:
            merged["details"]["Models"] = ", ".join(models_of)
        merged["shared"] = len(models_of)
        nodes.append(merged)

    # The same measure / column / table in other models, and whether it is really the same
    known = {n["id"] for n in nodes}
    same: dict[str, list] = {}
    differs: dict[str, bool] = {}
    groups = compare_entries([entry for entry, _ in models])
    for kind in ("measure", "column", "table"):
        for group in groups[kind]:
            ids = [f"{m['entry']}|{kind}:{m['id']}" for m in group["members"]]
            for member, node_id in zip(group["members"], ids):
                if node_id not in known:
                    continue
                pairs = []
                for other, other_id in zip(group["members"], ids):
                    if other is member or other_id not in known:
                        continue
                    apart = differences(member, other)
                    pairs.append([other_id, not apart, apart])
                if pairs:
                    same[node_id] = pairs
                    if any(not pair[1] for pair in pairs):
                        differs[node_id] = True

    types = TYPE_ORDER + ["model"]
    return {
        "report": "Lineage across models",
        "multi": True,
        "generator": next((d.get("generator", "") for _, d in models), ""),
        "exact": all(d.get("exact") for _, d in models),
        "rls": rls,
        "nodes": nodes,
        "edges": edges,
        "model_edges": model_edges,
        "same": same,
        "differs": differs,
        "stats": {t: sum(1 for n in nodes if n["type"] == t) for t in types},
    }


def _render(data: dict, title: str) -> str:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    # No "<" at all inside the data block: neither "</script>" nor "<!--<script" (which puts
    # the HTML parser in an escaped state) can then end or swallow it. Still valid JSON.
    payload = payload.replace("<", "\\u003c")
    return (
        _TEMPLATE.replace("__TITLE__", escape(title))
        .replace("__THEME_SCRIPT__", PAGE_THEME_SCRIPT)
        .replace("__THEME__", page_css())
        .replace("__DATA__", payload)  # last: the data must not be searched for placeholders
    )


def render_lineage_html(doc: dict) -> str:
    """The complete HTML page for a JSON documentation dict."""
    data = build_viewer_data(doc)
    return _render(data, f"{data['report']} - lineage")


def render_multi_lineage_html(models: list[tuple[dict, dict]]) -> str:
    """The lineage-across-models page (see build_multi_viewer_data)."""
    return _render(build_multi_viewer_data(models), "Lineage across models")


def write_lineage_html(path: str | Path, doc: dict) -> None:
    """Write the lineage viewer next to the other output files."""
    Path(path).write_text(render_lineage_html(doc), encoding="utf-8")


_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
__THEME_SCRIPT__
<style>
__THEME__
* { box-sizing: border-box; }
html, body { margin: 0; height: 100%; background: var(--bg); color: var(--text);
  font-size: 13.5px; line-height: 1.4; }
#app { display: grid; grid-template-columns: 290px 1fr 360px; height: 100vh; }
aside, #details { background: var(--panel); border-right: 1px solid var(--border);
  display: flex; flex-direction: column; min-height: 0; }
#details { border-right: 0; border-left: 1px solid var(--border); }
header { padding: 14px 16px 10px; border-bottom: 1px solid var(--border); }
h1 { font-size: 17px; margin: 0 0 2px; }
h2 { font-size: 15px; margin: 0; word-break: break-word; }
.muted { color: var(--muted); }
.section { padding: 10px 16px; border-bottom: 1px solid var(--border); }
input[type=search] { width: 100%; padding: 7px 9px; border: 1px solid var(--border);
  border-radius: 8px; background: var(--bg); color: var(--text); font: inherit; }
/* The type chips and "Only unused": one compact row of small toggles (wraps to 2 lines at most
   at 290 px); the checkbox stays for keyboards and screen readers, the chip shows its state */
.chips { display: flex; flex-wrap: wrap; gap: 3px; margin-top: 7px; }
.chip { display: inline-flex; align-items: center; gap: 3px; padding: 0 4px; font-size: 11px;
  line-height: 18px; border: 1px solid var(--border); border-radius: 999px; cursor: pointer;
  user-select: none; position: relative; }
.chip input { position: absolute; opacity: 0; width: 1px; height: 1px; margin: 0; pointer-events: none; }
.chip:focus-within { outline: 2px solid var(--accent); outline-offset: 1px; }
.chip.is-off { opacity: .45; text-decoration: line-through; }
.chip.unused { border-style: dashed; }
.chip.unused.is-off { text-decoration: none; }
.chip.unused:not(.is-off) { border-color: var(--unused); color: var(--unused); }
.chip .dot { width: 7px; height: 7px; }
.dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; }
/* "In this view (N)" / "Whole model (N)" above the list, with the switch between them */
#listHead { display: flex; align-items: center; gap: 8px; padding: 6px 16px 2px; font-size: 12px; }
#listHead[hidden] { display: none; }
#listTitle { flex: 1; font-weight: 600; color: var(--muted); }
.linkbtn { background: none; border: 0; padding: 0; font: inherit; color: var(--accent); cursor: pointer; }
.linkbtn:hover { text-decoration: underline; }
.result .twisty { width: 12px; flex: none; color: var(--muted); text-align: center; }
.result.member { padding-left: 34px; }
.result.grouprow .name { font-weight: 600; }
#results { overflow: auto; flex: 1; padding: 4px 0; }
.result { padding: 5px 16px; cursor: pointer; display: flex; gap: 8px; align-items: baseline; }
.result:hover, .result.active { background: var(--bg); }
.result.marked { background: var(--bg); box-shadow: inset 3px 0 0 var(--attn); }
.node text.diff { fill: var(--attn); font-weight: 700; font-size: 22px; }
.result .diff, a .diff { color: var(--attn); font-weight: 700; }
.notice { margin: 10px 0 4px; padding: 6px 9px; border-radius: 8px; font-size: 12px;
  border-left: 3px solid var(--attn); background: var(--bg); }
.rls { margin-top: 8px; padding: 6px 9px; border-radius: 8px; font-size: 12px;
  border-left: 3px solid var(--attn); background: var(--bg); }
.result .name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; }
.result .group { color: var(--muted); font-size: 11px; white-space: nowrap; }
.result.more { color: var(--accent); }
main { position: relative; min-width: 0; min-height: 0; }
#toolbar { position: absolute; top: 10px; left: 10px; right: 10px; display: flex; gap: 8px;
  align-items: center; flex-wrap: wrap; z-index: 2; pointer-events: none; }
#toolbar > * { pointer-events: auto; }
.btn, select { background: var(--panel); color: var(--text); border: 1px solid var(--border);
  border-radius: 8px; padding: 5px 9px; font: inherit; cursor: pointer; box-shadow: var(--shadow); }
.btn:disabled { opacity: .45; cursor: default; }
#toolbar .group { display: inline-flex; }
#toolbar .group .btn { border-radius: 0; margin-left: -1px; }
#toolbar .group .btn:first-child { border-radius: 8px 0 0 8px; margin-left: 0; }
#toolbar .group .btn:last-child { border-radius: 0 8px 8px 0; }
#app.wide { grid-template-columns: 1fr !important; }
#app.wide aside, #app.wide #details { display: none !important; }
#status { background: var(--panel); border: 1px solid var(--border); border-radius: 8px;
  padding: 5px 9px; box-shadow: var(--shadow); }
svg { width: 100%; height: 100%; display: block; cursor: grab; }
svg.dragging { cursor: grabbing; }
.node rect { fill: var(--panel); stroke-width: 1.5; rx: 6; }
.node text { fill: var(--text); font-size: 19px; pointer-events: none; }
.node .type { fill: var(--muted); font-size: 16px; }
.node { cursor: pointer; }
.node.selected rect { stroke-width: 3; }
.node.marked rect.box { stroke: var(--attn); stroke-width: 3.5; }
/* A group of columns / measures / visuals: a stack of boxes and a "+" that expands it */
.node rect.stack { stroke-width: 1.2; }
.node .expand rect { fill: var(--bg); stroke: var(--border); stroke-width: 1; }
.node .expand text { fill: var(--accent); font-size: 22px; font-weight: 700; text-anchor: middle; }
/* The overview's source bands: a frame around a source's tables, "<source> · <n> tables" */
.band rect { fill: var(--panel); fill-opacity: .45; stroke: var(--border); stroke-width: 1.5; rx: 10; }
.band text { fill: var(--muted); font-size: 19px; font-weight: 600; }
.edge title { pointer-events: none; }
/* Off-screen markers: how many items lie beyond each edge of the graph area. Laid out on their
   own (like the details): a changed count never makes the browser lay out the graph again */
#offscreen { position: absolute; inset: 0; z-index: 2; pointer-events: none; contain: strict; }
.off { position: absolute; font-size: 12px; padding: 3px 9px; pointer-events: auto; visibility: hidden; }
.off.on { visibility: visible; }  /* shown / hidden without a new layout (cheaper than display) */
.off[data-side=left] { left: 8px; top: 50%; transform: translateY(-50%); }
.off[data-side=right] { right: 8px; top: 50%; transform: translateY(-50%); }
.off[data-side=top] { top: 96px; left: 50%; transform: translateX(-50%); }
.off[data-side=bottom] { bottom: 8px; left: 50%; transform: translateX(-50%); }
.btn.primary { background: var(--accent); color: var(--on-color); border-color: var(--accent); }
.node.unused rect { stroke-dasharray: 5 3; }
.edge { fill: none; stroke: var(--edge); stroke-width: 1.2; }
.edge.filters { stroke-dasharray: 4 3; }
.edge.hi { stroke: var(--edge-hi); stroke-width: 2.2; }
/* One class on the graph group dims everything unrelated: .has-hi (an item highlighted: its
   edges .hi) and .has-mark (an item marked: the highlighted item and its neighbours .rel) */
#viewport.has-hi .edge:not(.hi) { opacity: .15; }
#viewport.has-mark .node:not(.rel):not(.marked) { opacity: .25; }
/* Text too small to read (zoomed out) or while the wheel zooms: hidden, shapes stay */
svg.small-text .node text, svg.zooming .node text { display: none; }
svg.small-text .band text, svg.zooming .band text { display: none; }
#empty { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center;
  color: var(--muted); pointer-events: none; }
/* The details are laid out on their own (absolute + contain): new details then never make the
   browser lay out the whole graph again */
#details { position: relative; }
#details .body { overflow: auto; padding: 10px 16px 20px; position: absolute; inset: 0; contain: strict; }
.kv { margin: 0 0 10px; }
.kv dt { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .03em; }
.kv dd { margin: 2px 0 0; white-space: pre-wrap; word-break: break-word; }
pre { margin: 2px 0 0; padding: 8px; background: var(--bg); border: 1px solid var(--border);
  border-radius: 8px; white-space: pre-wrap; word-break: break-word; font: 12px/1.45 Consolas, monospace; }
.links a { color: var(--accent); cursor: pointer; display: block; padding: 1px 0; }
/* "Built from" / "Used by": one collapsible section per type, "Columns (42)" */
.grouped > dt { margin-bottom: 2px; }
.grouped .counts { text-transform: none; letter-spacing: 0; }
details.sect { margin: 2px 0; }
details.sect > summary { cursor: pointer; padding: 2px 0; font-weight: 600; list-style-position: inside; }
details.sect > .links { padding-left: 14px; }
.links a.more { color: var(--muted); }
.daxhead { display: flex; align-items: baseline; gap: 8px; }
.daxhead .linkbtn { font-size: 11px; text-transform: none; letter-spacing: 0; }
/* The Guide hint: until the Guide has been opened once */
#guideHint { font-size: 12px; color: var(--muted); background: var(--panel); border: 1px dashed var(--border);
  border-radius: 8px; padding: 4px 8px; cursor: pointer; box-shadow: var(--shadow); }
#guideHint[hidden] { display: none; }
#detailsToggle { display: none; }
.badge { display: inline-block; padding: 1px 7px; border-radius: 999px; font-size: 11px;
  color: var(--on-color); margin-right: 6px; }
#guide { position: fixed; inset: 0; z-index: 10; background: rgba(0,0,0,.45); display: none;
  align-items: flex-start; justify-content: center; overflow: auto; padding: 40px 16px; }
#guide.open { display: flex; }
#guide .card { background: var(--panel); border: 1px solid var(--border); border-radius: 12px;
  max-width: 760px; width: 100%; padding: 20px 24px; box-shadow: 0 10px 30px rgba(0,0,0,.25); }
#guide h2 { font-size: 17px; margin-bottom: 4px; }
#guide h3 { font-size: 13px; margin: 18px 0 6px; text-transform: uppercase; letter-spacing: .04em;
  color: var(--muted); }
#guide p, #guide li { margin: 4px 0; }
#guide ul { margin: 4px 0; padding-left: 18px; }
#guide .flow { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin: 10px 0 4px; }
#guide .flow span.step { display: inline-flex; align-items: center; gap: 5px; padding: 3px 9px;
  border: 1px solid var(--border); border-radius: 6px; }
#guide table { border-collapse: collapse; width: 100%; }
#guide td { padding: 5px 8px; border-top: 1px solid var(--border); vertical-align: top; }
#guide td:first-child { white-space: nowrap; font-weight: 600; }
#guide .sample { display: inline-block; width: 34px; height: 0; vertical-align: middle;
  border-top: 2px solid var(--edge); margin-right: 6px; }
#guide .sample.dashed { border-top-style: dashed; }
#guide .box { display: inline-block; width: 26px; height: 14px; vertical-align: middle;
  border: 1.5px solid var(--measure); border-radius: 4px; margin-right: 6px; }
#guide .box.unused { border: 1.5px dashed var(--unused); }
#guide .close { float: right; }
/* Narrow (e.g. the web UI's iframe): the details are an overlay on the right of the graph, at
   most DETAILS_W wide, with a handle that collapses it to a slim tab (TAB_W). Fit, reveal() and
   the off-screen markers leave its width out (graph area = graph width - overlay) */
@media (max-width: 1199px) { #app { grid-template-columns: 260px 1fr; }
  #details { position: fixed; right: 0; top: 0; bottom: 0; width: min(320px, calc(100vw - 40px));
    z-index: 5; box-shadow: var(--shadow); }
  #details .body { left: 26px; }
  #detailsToggle { display: flex; position: absolute; left: 0; top: 0; bottom: 0; width: 26px; z-index: 1;
    align-items: center; justify-content: center; padding: 0; border: 0; border-right: 1px solid var(--border);
    background: var(--panel); color: var(--muted); font: inherit; font-size: 12px; cursor: pointer; }
  #detailsToggle:hover { color: var(--text); background: var(--bg); }
  #detailsToggle span { writing-mode: vertical-rl; white-space: nowrap; }
  #app.details-collapsed #details { width: 26px; }
  #app.details-collapsed #details .body { display: none; } }
@media (max-width: 700px) { #app { grid-template-columns: 1fr; grid-template-rows: 45vh 55vh; }
  aside { border-right: 0; border-bottom: 1px solid var(--border); } }
</style>
</head>
<body>
<div id="app">
  <aside>
    <header>
      <h1 id="title"></h1>
      <div class="muted" id="summary"></div>
      <div class="rls" id="rls" hidden></div>
    </header>
    <div class="section">
      <input type="search" id="search" placeholder="Search tables, columns, measures, visuals, pages" aria-label="Search">
      <div class="chips" id="types"><label class="chip unused is-off" title="Only unused columns and measures"><input type="checkbox" id="unusedOnly"> Only unused</label></div>
      <select id="modelFilter" aria-label="Model" hidden style="margin-top:8px; width:100%"><option value="">All models</option></select>
    </div>
    <div id="listHead" hidden><span id="listTitle"></span><button class="linkbtn" id="listScope"></button></div>
    <div id="results" role="listbox"></div>
  </aside>
  <main>
    <div id="toolbar">
      <span class="group">
        <button class="btn" id="back" title="Back (Alt+Left)" aria-label="Back">←</button>
        <button class="btn" id="forward" title="Forward (Alt+Right)" aria-label="Forward">→</button>
      </span>
      <button class="btn" id="levelUp">⤴ Up a level</button>
      <button class="btn" id="overview" title="The whole system: sources → tables → pages">⌂ Overview</button>
      <button class="btn" id="everything" title="Every table of every model at once - can be large" hidden>☰ Show everything</button>
      <select id="direction" aria-label="Direction">
        <option value="both">Upstream + downstream</option>
        <option value="up">Upstream only (built from)</option>
        <option value="down">Downstream only (used by)</option>
      </select>
      <span class="group">
        <button class="btn" id="zoomOut" title="Zoom out" aria-label="Zoom out">−</button>
        <button class="btn" id="fit" title="Frame the shown item and its direct neighbours (the whole graph when it fits readably)">Fit</button>
        <button class="btn" id="zoomIn" title="Zoom in" aria-label="Zoom in">+</button>
      </span>
      <button class="btn" id="collapse" title="Collapse the expanded groups again" hidden>⊟ Collapse groups</button>
      <button class="btn" id="panels" title="Hide or show the side panels for a bigger graph">⇔ Panels</button>
      <span id="guideHint" hidden title="Open the guide: how to read this view">New here?</span>
      <button class="btn" id="help" title="How to read this view">? Guide</button>
      <span id="status"></span>
    </div>
    <svg id="canvas" aria-label="Lineage graph"><g id="viewport"><g id="bands"></g><g id="edges"></g><g id="nodes"></g></g></svg>
    <div id="empty">Select an item on the left to see its lineage.</div>
    <div id="offscreen">
      <button class="btn off" data-side="left"></button>
      <button class="btn off" data-side="right"></button>
      <button class="btn off" data-side="top"></button>
      <button class="btn off" data-side="bottom"></button>
    </div>
  </main>
  <section id="details"><button id="detailsToggle" aria-controls="detailsBody"></button><div class="body" id="detailsBody"></div></section>
</div>
<div id="guide" role="dialog" aria-modal="true" aria-labelledby="guideTitle">
  <div class="card">
    <button class="btn close" id="guideClose">Close</button>
    <h2 id="guideTitle">How to read the lineage view</h2>
    <p class="muted">Lineage shows where the numbers in the report come from, and what would be
      affected if something in the model changes.</p>
    <div id="guideMulti" hidden>
      <h3>Several models</h3>
      <ul>
        <li>The start view shows the <b>models</b>, their <b>reports</b>, and the sources that
          several models load (where models meet). <b>Double-click a model</b> for its own
          overview: sources → tables → pages (or reports).</li>
        <li><b>☰ Show everything</b>: every table of every model at once - can be large.</li>
        <li>A source with the same name in several models is one item: its lineage runs into every
          model that loads it.</li>
        <li>A measure's details list the <b>same measure</b> (same name) in other models, and
          whether its DAX is identical.</li>
        <li><b>⤴ Up a level</b> also goes from a table or report to its model, and from a model to
          the start view. The list on the left can be limited to one model.</li>
      </ul>
    </div>

    <h3>Data flows left to right</h3>
    <div class="flow" id="guideFlow"></div>
    <p>Each column in the graph is one step. Measures can take several columns: a measure built on
      other measures sits to the right of the measures it uses.</p>
    <table>
      <tr><td>Source</td><td>Where a table loads its data from: a database view/table, a Lakehouse
        table (Direct Lake) or another Power Query source.</td></tr>
      <tr><td>Table</td><td>A table in the semantic model.</td></tr>
      <tr><td>Column</td><td>A column in a model table (loaded from the source or calculated with DAX).</td></tr>
      <tr><td>Measure</td><td>A DAX calculation. Arrows into it are the columns and measures its DAX uses.</td></tr>
      <tr><td>Visual</td><td>A chart, table, card, slicer or button on a report page. Arrows into it
        are the fields it shows or is filtered by.</td></tr>
      <tr><td>Page</td><td>A report page and the visuals on it.</td></tr>
      <tr><td>Report</td><td>A Power BI report and its pages. Several reports appear when all
        reports on one semantic model are documented together.</td></tr>
    </table>

    <h3>Overview and moving around</h3>
    <ul>
      <li><b>⌂ Overview</b> (the start view): the whole system at a glance - sources → tables →
        pages (→ reports instead of pages, when several reports are documented together). A table
        feeds a page when a visual on the page uses its columns or measures (also through other
        measures). Tables that feed nothing have a dashed red border.</li>
      <li><b>← →</b>: back and forward through what you looked at (also Alt+← / Alt+→).</li>
      <li><b>⤴ Up a level</b>: from a column or measure to its table, from a visual to its page,
        from a page to its report, and from a table, report or source to the overview.</li>
      <li><b>− Fit +</b>: zoom out, frame the item and its direct neighbours (or the whole graph
        when it fits with readable names; never closer than 75 %), zoom in (the mouse wheel zooms too).</li>
      <li><b>12 more →</b> at an edge of the graph: that many items lie beyond it. Click to move
        the nearest of them into view.</li>
      <li><b>⇔ Panels</b>: hide the side panels for a bigger graph (click again to bring them back).</li>
    </ul>

    <h3>Selecting an item</h3>
    <ul>
      <li><b>Click</b> an item in the graph to mark it: its lines are highlighted, unrelated items
        fade and its details appear on the right - the graph stays as it is. Names are hidden
        while they would be too small to read (zoomed far out) and while the wheel zooms. Click empty space to unmark.</li>
      <li><b>Double-click</b> (or Enter, or "Open its lineage" on the right) to open the item's own
        lineage.</li>
      <li><b>Upstream</b> (to the left) = what the item is <b>built from</b>.</li>
      <li><b>Downstream</b> (to the right) = what <b>uses</b> it, i.e. what can break if it changes
        or is removed.</li>
      <li>The drop-down at the top switches between both directions, upstream only and downstream only.</li>
    </ul>

    <h3>Lines and borders</h3>
    <table>
      <tr><td><span class="sample"></span>Solid line</td><td>Uses / contains / depends on.</td></tr>
      <tr><td><span class="sample dashed"></span>Dashed line</td><td>The field is used as a
        <b>filter</b> on the visual (in its filter pane), not shown in it.</td></tr>
      <tr><td><span class="box"></span>Solid border</td><td>Normal item; the colour is its type.</td></tr>
      <tr><td><span class="box unused"></span>Dashed red border</td><td><b>Unused</b> column or
        measure: no visual, filter, DAX, relationship, sort-by or hierarchy uses it - a candidate
        for removal.</td></tr>
      <tr><td>Thick border</td><td>The item whose lineage is shown.</td></tr>
      <tr><td>Thick blue border</td><td>The marked item (single click).</td></tr>
    </table>

    <h3>The three panels</h3>
    <ul>
      <li><b>Left</b>: search, show/hide item types, and "Only unused" (columns and measures).
        With a lineage open the list shows <b>what is in this view</b> (a group's members under its
        ▸); <b>Whole model</b> lists everything. Click an item to mark it (its node is highlighted
        in the graph when shown, and its details appear on the right); double-click (or Enter) to
        open its lineage.</li>
      <li><b>Middle</b>: the graph. Drag to move, scroll to zoom, hover an item to highlight its
        lines; click to mark, double-click to open.</li>
      <li><b>Right</b>: details of the marked (or shown) item - DAX (formatted; "Show as written"
        shows it exactly as in the model), data type, storage mode, rows/size when the .pbix or the
        Power BI service had statistics - and "Built from" / "Used by" per type ("Used by: 42
        columns · 512 measures"), each type a section that opens with a click. Click an entry to
        mark it, double-click to open its lineage. On a narrow page the details lie over the graph:
        its handle collapses them to a tab.</li>
    </ul>

    <h3>Worth knowing</h3>
    <ul>
      <li><b>⚠</b> before a button: it points to a bookmark or page that no longer exists.</li>
      <li>"dependencies by text matching" at the top left: the DAX analysis failed, so
        dependencies are found by searching the DAX text and can be incomplete.</li>
      <li><b>Groups</b>: when a lineage has more than 40 columns, measures or visuals in one step,
        they are shown per table (visuals per page) as one stacked box, e.g. "Sales · 42 measures".
        Click it for its members, double-click (or Enter, or its <b>+</b>) to expand it;
        <b>⊟ Collapse groups</b> folds them again.</li>
      <li>Relationships between tables are not drawn here - see the relationships sheet in the workbook.</li>
    </ul>
  </div>
</div>
<script type="application/json" id="data">__DATA__</script>
<script>
(function () {
  "use strict";
  var DATA = JSON.parse(document.getElementById("data").textContent);
  // Several models (the catalog's lineage across models): model nodes, a start view per level
  var MULTI = !!DATA.multi;
  var TYPES = ["source", "table", "column", "measure", "visual", "page", "report"].concat(MULTI ? ["model"] : []);
  var TYPE_LABEL = { source: "Source", table: "Table", column: "Column", measure: "Measure",
                     visual: "Visual", page: "Page", report: "Report", model: "Model" };
  // The overview's top level: reports when several are documented together, else pages
  var TOP = MULTI || (DATA.stats.report || 0) > 1 ? "report" : "page";
  function plural(top) { return top === "report" ? "reports" : "pages"; }
  function feedsNone(top) { return top === "report" ? "feeds no report" : "feeds no report page"; }
  // Node boxes: as wide as the name needs (NODE_MIN_W..NODE_MAX_W); a longer name wraps onto a
  // 2nd line, only beyond 2 lines it ends in "…". Wrapped columns of one layer sit WRAP_GAP
  // apart (closer than GAP_X between layers), source bands BAND_PAD inside their frame.
  var NODE_MIN_W = 210, NODE_MAX_W = 320, GAP_X = 90, GAP_Y = 12, WRAP_GAP = 30;
  var BAND_PAD = 12, BAND_GAP = 24;
  // Font sizes (layout units): at TEXT_MIN_SCALE a label is LABEL_FONT * 0.5 = 9.5 px on screen
  var LABEL_FONT = 19, SUB_FONT = 16, LINE_H = 23, PAD_L = 14, PAD_R = 12;
  // In a lineage, a kind with more than GROUP_THRESHOLD items in one layer is shown as one group
  // per table (columns, measures) or page (visuals); several models: per model and table
  var GROUP_THRESHOLD = 40, GROUPED = { column: "columns", measure: "measures", visual: "visuals" };
  var TOOLBAR_H = 50;  // the toolbar covers the top of the graph area
  var TOP_H = 90;  // ... and with the status line below it: what Fit keeps clear
  // 1,234: counts only (whole numbers); without toLocaleString, whose first call is slow
  function fmt(n) { return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ","); }
  // Node text is hidden below this zoom (labels would be under ~9.5 px on screen)
  var TEXT_MIN_SCALE = 0.5, SEARCH_DELAY = 150, LIST_PAGE = 100;
  // Fit never zooms in beyond this (labels about 14 px on screen); the + button still can
  var FIT_MAX = 0.75;
  var compare = (window.Intl && Intl.Collator ? new Intl.Collator() : { compare: function (a, b) { return a.localeCompare(b); } }).compare;

  // ---------- text sizes ----------
  // Widths from a table of the page font's (Source Sans 3, embedded) advance widths per
  // character, in 1/100 em, for " " to "~" (other characters: 0.5 em); the semi-bold band labels
  // are about 4 % wider. Nothing is measured in the page (no canvas or DOM text measurement).
  var CHAR_W = ("20,29,43,50,50,82,61,25,30,30,42,50,25,31,25,35,50,50,50,50,50,50,50,50,50,50,25,25,50,50,50,42," +
    "85,54,59,57,61,53,49,62,65,26,48,58,49,73,65,66,57,66,57,53,54,64,51,79,51,48,54,30,35,30,50,50," +
    "54,50,55,46,55,50,29,50,54,25,25,49,25,83,55,54,55,55,35,42,34,54,47,72,45,47,42,30,24,30,50").split(",").map(Number);
  var FONT_PX = { l: LABEL_FONT / 100, s: SUB_FONT / 100, b: LABEL_FONT * 1.04 / 100 };
  function textWidth(text, font) {
    var w = 0;
    for (var i = 0; i < text.length; i++) { var c = text.charCodeAt(i) - 32; w += c >= 0 && c < 95 ? CHAR_W[c] : 50; }
    return w * FONT_PX[font];
  }
  // The longest start of text that fits room with "…" after it (whole text when it fits)
  function truncate(text, room, font) {
    if (textWidth(text, font) <= room) return text;
    var lo = 0, hi = text.length - 1;
    while (lo < hi) {
      var mid = Math.ceil((lo + hi) / 2);
      if (textWidth(text.slice(0, mid) + "…", font) <= room) lo = mid; else hi = mid - 1;
    }
    return text.slice(0, lo).replace(/\s+$/, "") + "…";
  }
  // A name on at most maxLines lines of room width: broken between words (inside a word only
  // when one word is too long); the last line ends in "…" when the rest does not fit
  function wrapLabel(text, room, font, maxLines) {
    var lines = [], rest = text;
    while (rest && lines.length < maxLines - 1 && textWidth(rest, font) > room) {
      var words = rest.split(" "), line = words[0], i = 1;
      while (i < words.length && textWidth(line + " " + words[i], font) <= room) line += " " + words[i++];
      if (textWidth(line, font) > room) line = truncate(line, room, font).slice(0, -1) || rest.charAt(0);
      lines.push(line);
      rest = rest.slice(line.length).replace(/^\s+/, "");
    }
    if (rest) lines.push(truncate(rest, room, font));
    return lines;
  }
  // A node's text and size before its column's width is known: need = the width its name and
  // sub-line ask for (NODE_MIN_W..NODE_MAX_W), lines = the name wrapped at NODE_MAX_W
  function nodeText(n) {
    if (n.sized) return n.sized;
    var extra = n.isGroup ? 34 : MULTI && DATA.differs[n.id] ? 24 : 0, sub;  // room for "+" / "≠"
    if (n.isGroup) sub = TYPE_LABEL[n.type] + " group" + (MULTI && n.model ? " · " + n.model : "");
    else {
      var place = MULTI && n.model && n.type !== "model" ? n.model + (n.group ? " / " + n.group : "") : n.group;
      sub = TYPE_LABEL[n.type] + (place ? " · " + place : "");
    }
    var pads = PAD_L + PAD_R + extra;
    var need = Math.max(textWidth(n.label, "l"), textWidth(sub, "s")) + pads;
    var lines = wrapLabel(n.label, NODE_MAX_W - pads, "l", 2);
    return (n.sized = { need: Math.max(NODE_MIN_W, Math.min(NODE_MAX_W, Math.ceil(need))), pads: pads,
                        lines: lines, sub: sub, h: nodeHeight(lines.length) });
  }
  function nodeHeight(lines) { return 35 + LINE_H * lines; }

  var byId = {}, up = {}, down = {};
  DATA.nodes.forEach(function (n) { byId[n.id] = n; up[n.id] = []; down[n.id] = []; });
  DATA.edges.forEach(function (e) {
    if (!byId[e[0]] || !byId[e[1]]) return;
    down[e[0]].push({ id: e[1], type: e[2] });
    up[e[1]].push({ id: e[0], type: e[2] });
  });
  // An item's id in its own model ("table:X" -> "<model key>|table:X" when there are several)
  function pid(n, local) { return n.m ? n.m + "|" + local : local; }
  function where(n) { return MULTI && n.model && n.type !== "model" ? (n.group ? n.group + " · " : "") + n.model : n.group; }
  var sourceModels = {};  // shared source id -> {model key: true}
  (DATA.model_edges || []).forEach(function (e) {
    if (byId[e[0]] && byId[e[0]].type === "source") (sourceModels[e[0]] = sourceModels[e[0]] || {})[byId[e[1]].m] = true;
  });

  var state = { selected: null, marked: null, direction: "both", types: {}, query: "", unusedOnly: false,
                model: "", scale: 1, tx: 0, ty: 0 };
  TYPES.forEach(function (t) { state.types[t] = true; });

  var $ = function (id) { return document.getElementById(id); };
  function color(type) { return "var(--" + type + ")"; }
  function html(tag, cls, text) {
    var node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text != null) node.textContent = text;
    return node;
  }

  // ---------- header, filters, results ----------
  $("title").textContent = DATA.report;
  $("summary").textContent = TYPES.map(function (t) {
    return DATA.stats[t] + " " + TYPE_LABEL[t].toLowerCase() + (DATA.stats[t] === 1 ? "" : "s");
  }).join(" · ") + (DATA.exact ? "" : " · dependencies by text matching");
  if ((DATA.rls || []).length) {
    $("rls").hidden = false;
    $("rls").textContent = MULTI
      ? "🔒 Row-level security in " + DATA.rls.length + (DATA.rls.length === 1 ? " model" : " models") +
        " (" + DATA.rls.join(", ") + ") - viewers only see the rows their role allows. Share with care."
      : "🔒 Row-level security: " + DATA.rls.length + (DATA.rls.length === 1 ? " role" : " roles") +
        " (" + DATA.rls.join(", ") + ") - viewers only see the rows their role allows. Share with care.";
  }
  if (MULTI) {
    $("everything").hidden = false;
    $("guideMulti").hidden = false;
    $("overview").title = "The start view: models, their reports and the sources they share";
    var filter = $("modelFilter");
    filter.hidden = false;
    DATA.nodes.filter(function (n) { return n.type === "model"; })
      .sort(function (a, b) { return a.label.localeCompare(b.label); })
      .forEach(function (n) { var o = html("option", null, n.label); o.value = n.m; filter.appendChild(o); });
    filter.addEventListener("change", function (e) { state.model = e.target.value; renderResults(); });
  }

  var unusedChip = $("unusedOnly").parentNode;
  if (MULTI) unusedChip.lastChild.textContent = " Unused";  // one chip more (Model): still 2 lines
  TYPES.forEach(function (t) {
    var chip = html("label", "chip");
    chip.title = "Show or hide " + TYPE_LABEL[t].toLowerCase() + "s in the list";
    var box = html("input"); box.type = "checkbox"; box.checked = true;
    box.addEventListener("change", function () { state.types[t] = box.checked; chip.classList.toggle("is-off", !box.checked); renderResults(); });
    var dot = html("span", "dot"); dot.style.background = color(t);
    chip.appendChild(box); chip.appendChild(dot); chip.appendChild(document.createTextNode(TYPE_LABEL[t]));
    $("types").insertBefore(chip, unusedChip);
  });
  // The search waits until typing pauses (SEARCH_DELAY ms)
  var searchTimer = null;
  $("search").addEventListener("input", function (e) {
    var value = e.target.value.toLowerCase();
    clearTimeout(searchTimer);
    searchTimer = setTimeout(function () { state.query = value; renderResults(); }, SEARCH_DELAY);
  });
  $("unusedOnly").addEventListener("change", function (e) {
    state.unusedOnly = e.target.checked; unusedChip.classList.toggle("is-off", !e.target.checked); renderResults();
  });
  $("direction").addEventListener("change", function (e) { state.direction = e.target.value; renderGraph(); });

  // Computed once: every node's lower-case search text, and the list order (type, group, label)
  var LIST_ORDER = DATA.nodes.slice();
  LIST_ORDER.forEach(function (n) { n.hay = (n.label + " " + where(n)).toLowerCase(); n.rank = TYPES.indexOf(n.type); });
  LIST_ORDER.sort(function (a, b) {
    return b.rank - a.rank || compare(a.group, b.group) || compare(a.label, b.label);
  });

  // The list shows LIST_PAGE rows at first, more when its end scrolls into view (or is clicked).
  // With a lineage open it shows what is in the view ("In this view (N)": the drawn items and
  // groups, a group's members below it when its ▸ is opened, or when the search matches them);
  // "Whole model" switches to every item (always so on an overview). The switch is kept for this
  // tab, in memory only.
  var listMatches = [], listShown = 0, listKey = null, rowEls = {}, rowActive = null, rowMarked = null, moreRow = null;
  var listScope = "view", listOpen = {}, listGen = 0, listMember = {}, listView = null, listTop = 0, viewSorted = null;
  var WHOLE = MULTI ? "All models" : "Whole model";
  var moreObserver = window.IntersectionObserver ? new IntersectionObserver(function (entries) {
    if (entries.some(function (e) { return e.isIntersecting; })) appendRows();
  }, { root: $("results") }) : null;
  $("results").addEventListener("scroll", function (e) { listTop = e.target.scrollTop; }, { passive: true });
  function listInView() { return listScope === "view" && !!graph && !!graph.selected; }
  // "In this view" starts with the shown item, then follows the flow (sources first, as the
  // graph reads); "Whole model" lists reports and pages first (LIST_ORDER)
  function byListOrder(a, b) { return a.rank - b.rank || compare(a.group || "", b.group || "") || compare(a.label, b.label); }
  // The drawn nodes in list order (once per drawing); a group's members sorted once
  function viewOrder() {
    if (viewSorted && viewSorted.gen === listGen) return viewSorted.list;
    var list = graph.ids.filter(function (id) { return id !== graph.selected; }).map(N);
    list.forEach(function (n) {
      if (!n.isGroup || n.hay) return;
      n.hay = (n.label + " " + (n.model || "")).toLowerCase(); n.rank = TYPES.indexOf(n.type);
    });
    viewSorted = { gen: listGen, list: [N(graph.selected)].concat(list.sort(byListOrder)) };  // the shown item first
    return viewSorted.list;
  }
  function membersOf(g) {
    return g.sortedMembers || (g.sortedMembers = g.members.map(function (id) { return byId[id]; }).sort(byListOrder));
  }
  function renderResults(force) {
    var inView = listInView();
    var key = [state.query, state.unusedOnly, state.model, TYPES.map(function (t) { return state.types[t] ? 1 : 0; }).join(""),
               inView ? "v" + listGen : "m"].join("\u0000");
    if (key === listKey && force !== true) { syncRows(); return; }  // same input: only the marks change
    listKey = key;
    var head = $("listHead"), canView = !!(graph && graph.selected);
    head.hidden = !canView;
    if (canView) {
      $("listTitle").textContent = inView ? "In this view (" + fmt(graph.itemCount) + ")" : WHOLE + " (" + fmt(DATA.nodes.length) + ")";
      $("listScope").textContent = inView ? WHOLE : "In this view";
      $("listScope").title = inView ? "List every item of the model" : "List only what is in the current view";
    }
    var list = $("results"), sameView = inView && listView === graph.selected, top = listTop;
    list.textContent = "";
    rowEls = {}; rowActive = null; rowMarked = null; listShown = 0; moreRow = null; listMember = {};
    var q = state.query;
    function keep(n) {
      if (!state.types[n.type]) return false;
      if (state.unusedOnly && !n.unused) return false;
      if (state.model && n.m !== state.model && !(n.type === "source" && (sourceModels[n.id] || {})[state.model])) return false;
      return true;
    }
    function hit(n) { return !q || n.hay.indexOf(q) >= 0; }
    if (!inView) listMatches = LIST_ORDER.filter(function (n) { return keep(n) && hit(n); });
    else {
      listMatches = [];
      viewOrder().forEach(function (n) {
        if (!n.isGroup) { if (keep(n) && hit(n)) listMatches.push(n); return; }
        if (!state.types[n.type] || (state.model && n.m !== state.model)) return;
        var members = membersOf(n).filter(keep);
        if (!members.length) return;
        var self = hit(n), found = self ? members : members.filter(hit);
        if (!self && !found.length) return;
        listMatches.push(n);
        // its members: when opened in the list, or the ones the search found
        var shown = listOpen[n.key] ? found : q && !self ? found : [];
        shown.forEach(function (m) { listMember[m.id] = n.id; listMatches.push(m); });
      });
    }
    listView = inView ? graph.selected : null;
    if (!listMatches.length) list.appendChild(html("div", "result muted", "No matches"));
    appendRows();
    if (sameView && top) list.scrollTop = top;  // the same view drawn again (a group expanded)
  }
  function appendRows() {
    var list = $("results");
    if (moreRow) { if (moreObserver) moreObserver.unobserve(moreRow); moreRow.remove(); moreRow = null; }
    var end = Math.min(listMatches.length, listShown + LIST_PAGE), frag = document.createDocumentFragment();
    for (var i = listShown; i < end; i++) frag.appendChild(resultRow(listMatches[i]));
    listShown = end;
    if (listShown < listMatches.length) {
      moreRow = html("div", "result more", "Show more (" + (listMatches.length - listShown) + " more)");
      frag.appendChild(moreRow);
    }
    list.appendChild(frag);
    if (moreRow && moreObserver) moreObserver.observe(moreRow);
    syncRows();
  }
  function resultRow(n) {
    var row = html("div", "result" + (n.isGroup ? " grouprow" : listMember[n.id] ? " member" : ""));
    row.setAttribute("role", "option");
    row.tabIndex = 0;
    row.dataset.id = n.id;
    if (n.isGroup) {
      row.appendChild(html("span", "twisty", listOpen[n.key] ? "▾" : "▸"));
      row.setAttribute("aria-expanded", listOpen[n.key] ? "true" : "false");
    }
    var dot = html("span", "dot"); dot.style.background = color(n.type);
    row.appendChild(dot);
    row.appendChild(html("span", "name", n.label));
    if (MULTI && DATA.differs[n.id]) row.appendChild(html("span", "diff", "≠"));
    if (where(n)) row.appendChild(html("span", "group", where(n)));
    row.title = n.isGroup ? TYPE_LABEL[n.type] + " group: " + n.label + "\n▸ its members here · click: details · double-click: expand it in the graph"
      : TYPE_LABEL[n.type] + ": " + n.label + (n.unused ? " (unused)" : "");
    rowEls[n.id] = row;
    return row;
  }
  // The shown (active) and marked rows follow the state: only the old and the new row change
  function syncRows() {
    rowActive = moveClass(rowEls, "active", rowActive, state.selected);
    rowMarked = moveClass(rowEls, "marked", rowMarked, state.marked);
  }
  function moveClass(els, cls, from, to) {
    if (from && els[from] && from !== to) els[from].classList.remove(cls);
    if (to && els[to]) els[to].classList.add(cls);
    return to && els[to] ? to : null;
  }
  $("listScope").addEventListener("click", function () {
    listScope = listScope === "view" ? "model" : "view";
    listTop = 0; $("results").scrollTop = 0;
    renderResults();
  });
  // A click marks, a double-click opens - also when the first click rebuilt the rows under the
  // mouse (a group expanded, new details): the second click opens what the first one marked
  var lastClick = null;
  function firstClick(id) { lastClick = { id: id, t: Date.now(), opened: false }; }
  function secondClick(ev) {
    if (!(ev.detail >= 2 && lastClick && !lastClick.opened && Date.now() - lastClick.t < 700)) return false;
    lastClick.opened = true; lastClick.t = Date.now();
    open(lastClick.id);
    return true;
  }
  function justOpened() { return !!lastClick && lastClick.opened && Date.now() - lastClick.t < 700; }
  // One handler set for the whole list. Like the graph: click marks it (details on the right),
  // double-click (or Enter) opens its lineage; a group's ▸ lists its members below it
  function rowOf(ev) { var row = ev.target.closest && ev.target.closest(".result"); return row && row.parentNode === $("results") ? row : null; }
  function toggleListGroup(gid) {
    var g = groupNodes[gid]; if (!g) return;
    listOpen[g.key] = !listOpen[g.key];
    renderResults(true);
    if (rowEls[gid]) rowEls[gid].focus({ preventScroll: true });
  }
  $("results").addEventListener("click", function (ev) {
    if (secondClick(ev)) return;
    var row = rowOf(ev); if (!row) return;
    if (row === moreRow) { lastClick = null; appendRows(); return; }
    if (!row.dataset.id) return;
    if (ev.target.classList.contains("twisty")) { lastClick = null; toggleListGroup(row.dataset.id); return; }
    firstClick(row.dataset.id);
    jump(row.dataset.id);
  });
  $("results").addEventListener("dblclick", function (ev) {
    if (justOpened()) return;
    var row = rowOf(ev); if (row && row.dataset.id && !ev.target.classList.contains("twisty")) open(row.dataset.id);
  });
  $("results").addEventListener("keydown", function (ev) {
    var row = rowOf(ev); if (!row || !row.dataset.id) return;
    if (ev.key === "Enter") open(row.dataset.id);
    else if (ev.key === " ") { ev.preventDefault(); jump(row.dataset.id); }
    else if ((ev.key === "ArrowRight" || ev.key === "ArrowLeft") && groupNodes[row.dataset.id] &&
             !!listOpen[groupNodes[row.dataset.id].key] === (ev.key === "ArrowLeft")) toggleListGroup(row.dataset.id);
  });

  // ---------- lineage ----------
  function walk(start, neighbours) {
    var seen = {}, queue = [start];
    seen[start] = true;
    for (var head = 0; head < queue.length; head++) {
      var id = queue[head];
      neighbours[id].forEach(function (n) { if (!seen[n.id]) { seen[n.id] = true; queue.push(n.id); } });
    }
    return seen;
  }
  function lineage(id) {
    var ids = {}; ids[id] = true;
    if (state.direction !== "down") Object.assign(ids, walk(id, up));
    if (state.direction !== "up") Object.assign(ids, walk(id, down));
    return Object.keys(ids);
  }

  // How tall a column may be (layout units): the graph area below the toolbar and the status
  // line, at the readable zoom
  function wrapLimit() { return Math.max(400, Math.floor((size().h - TOP_H - 20) / TEXT_MIN_SCALE)); }
  // Fill blocks ({h}) into columns no taller than limit, keeping their order (column by column,
  // about equally tall); returns [[block, ...], ...]
  function wrapColumns(blocks, gap, limit) {
    var total = blocks.reduce(function (s, b) { return s + b.h + gap; }, -gap);
    var target = total / Math.max(1, Math.ceil(total / limit)), columns = [], col = null, height = 0;
    blocks.forEach(function (b) {
      if (!col || (col.length && (height + gap + b.h > limit || height + (gap + b.h) / 2 > target))) {
        col = []; columns.push(col); height = -gap;
      }
      col.push(b); height += gap + b.h;
    });
    return columns;
  }
  // Layered layout: layer = data flow position; order within a layer by barycenter of neighbours.
  // bands (the overview's sources): {list: [{id, label, members}], of: {table id: band id}}; a
  // banded layer keeps the band order and its source layer the name order. A layer taller than
  // the graph area at the readable zoom wraps into side-by-side columns (a band inside its
  // frame); each column is as wide as its widest box. Returns {xy, box (id -> {w, h, ...}),
  // bands (band id -> {x, y, w, h, lines}), limit}.
  function layout(ids, upAdj, downAdj, bands) {
    var present = {}; ids.forEach(function (id) { present[id] = true; });
    var layers = {}, fixed = {};
    ids.forEach(function (id) { var l = N(id).layer; (layers[l] = layers[l] || []).push(id); });
    var keys = Object.keys(layers).map(Number).sort(function (a, b) { return a - b; });
    keys.forEach(function (k) {
      layers[k].sort(function (a, b) {
        var na = N(a), nb = N(b);
        return compare(na.model || "", nb.model || "") ||
               compare(na.group, nb.group) || compare(na.label, nb.label);
      });
    });
    if (bands) {
      bands.list.forEach(function (b) { if (b.members.length) fixed[N(b.members[0]).layer] = true; });
      ids.forEach(function (id) { if (N(id).type === "source") fixed[N(id).layer] = true; });
      keys.forEach(function (k) {  // a banded layer: the bands in order, then anything else
        if (!fixed[k] || N(layers[k][0]).type === "source") return;
        var rest = layers[k].filter(function (id) { return !bands.of[id]; });
        layers[k] = [].concat.apply([], bands.list.map(function (b) { return b.members; })).concat(rest);
      });
    }
    var pos = {};
    function index() { keys.forEach(function (k) { layers[k].forEach(function (id, i) { pos[id] = i; }); }); }
    function sweep(order, neighbours) {
      order.forEach(function (k) {
        if (fixed[k]) return;
        layers[k].sort(function (a, b) { return bary(a) - bary(b); });
        index();
      });
      function bary(id) {
        var ns = neighbours[id].filter(function (n) { return present[n.id]; });
        if (!ns.length) return pos[id];
        return ns.reduce(function (s, n) { return s + pos[n.id]; }, 0) / ns.length;
      }
    }
    index();
    for (var i = 0; i < 2; i++) { sweep(keys.slice(1), upAdj); sweep(keys.slice(0, -1).reverse(), downAdj); }

    // blocks per layer: a node, or a band (its members wrapped inside the frame)
    var limit = wrapLimit(), box = {}, bandBox = {}, bandById = {}, columns = [];
    if (bands) bands.list.forEach(function (b) { bandById[b.id] = b; });
    ids.forEach(function (id) { var t = nodeText(N(id)); box[id] = { w: t.need, h: t.h, text: t }; });
    function bandBlock(b) {
      var head = BAND_PAD + 2 * LINE_H;  // room for a 2-line header while wrapping
      var inner = wrapColumns(b.members.map(function (id) { return { id: id, h: box[id].h }; }), GAP_Y, limit - head - BAND_PAD);
      var widths = inner.map(function (col) { return Math.max.apply(null, col.map(function (c) { return box[c.id].w; })); });
      var innerW = widths.reduce(function (s, w) { return s + w; }, 0) + WRAP_GAP * (inner.length - 1);
      var w = Math.max(innerW, Math.min(NODE_MAX_W, Math.ceil(textWidth(b.label, "b")))) + 2 * BAND_PAD;
      var lines = wrapLabel(b.label, w - 2 * BAND_PAD, "b", 2), top = BAND_PAD + LINE_H * lines.length + 6;
      var tallest = Math.max.apply(null, inner.map(function (col) { return col.reduce(function (s, c) { return s + box[c.id].h + GAP_Y; }, -GAP_Y); }));
      return { band: b, inner: inner, widths: widths, lines: lines, top: top, w: w, h: top + tallest + BAND_PAD };
    }
    var layerCols = keys.map(function (k) {
      var blocks = [], seen = {};
      layers[k].forEach(function (id) {
        var bid = bands && bands.of[id];
        if (!bid) { blocks.push({ id: id, w: box[id].w, h: box[id].h }); return; }
        if (!seen[bid]) { seen[bid] = true; blocks.push(bandBlock(bandById[bid])); }
      });
      var cols = wrapColumns(blocks, blocks.some(function (b) { return b.band; }) ? BAND_GAP : GAP_Y, limit);
      return cols.map(function (col) {
        var height = col.reduce(function (s, b) { return s + b.h + (b.band ? BAND_GAP : GAP_Y); }, 0) - (col[col.length - 1].band ? BAND_GAP : GAP_Y);
        return { blocks: col, w: Math.max.apply(null, col.map(function (b) { return b.w; })), h: height };
      });
    });
    var tallestCol = 0;
    layerCols.forEach(function (cols) { cols.forEach(function (c) { tallestCol = Math.max(tallestCol, c.h); }); });
    var xy = {}, x = 0;
    layerCols.forEach(function (cols) {
      cols.forEach(function (c, ci) {
        if (ci) x += WRAP_GAP;
        var y = (tallestCol - c.h) / 2;
        c.blocks.forEach(function (b) {
          if (!b.band) {  // a column's boxes are as wide as its widest
            xy[b.id] = { x: x, y: y }; box[b.id].w = c.w;
            y += b.h + GAP_Y;
            return;
          }
          bandBox[b.band.id] = { x: x, y: y, w: c.w, h: b.h, lines: b.lines, label: b.band.label, n: b.band.members.length };
          var bx = x + BAND_PAD;
          b.inner.forEach(function (col, i) {
            var by = y + b.top;
            col.forEach(function (m) { xy[m.id] = { x: bx, y: by }; box[m.id].w = b.widths[i]; by += m.h + GAP_Y; });
            bx += b.widths[i] + WRAP_GAP;
          });
          y += b.h + BAND_GAP;
        });
        x += c.w;
      });
      x += GAP_X;
    });
    return { xy: xy, box: box, bands: bandBox, limit: limit };
  }

  // ---------- overviews ----------
  // Sources -> tables -> pages (or reports): the whole system (one model), one model of
  // several ("model" view) or every model ("all" view). A table feeds a page (report) when
  // anything upstream of it belongs to the table (also through measures in other tables).
  // With several models the start view is a level higher: shared sources -> models -> reports.
  // Each is built once, on first use.
  var overviews = {};
  function buildOverview(view) {
    var key = view.kind + ":" + (view.id || "");
    if (!overviews[key]) {
      overviews[key] = MULTI && view.kind === "overview" ? modelOverview()
        : flowOverview(view.kind === "model" ? byId[view.id].m : null);
    }
    return overviews[key];
  }
  function modelTop(m) {
    var reports = DATA.nodes.filter(function (n) { return n.m === m && n.type === "report"; }).length;
    return reports > 1 ? "report" : "page";
  }
  function flowOverview(m) {
    var top = m ? modelTop(m) : TOP;
    var ids = DATA.nodes.filter(function (n) {
      return (n.type === "table" || n.type === top) && (!m || n.m === m);
    }).map(function (n) { return n.id; });
    var present = {}, oUp = {}, oDown = {}, feeds = {};
    function add(id) { if (!present[id]) { present[id] = true; oUp[id] = []; oDown[id] = []; } }
    ids.forEach(add);
    function link(a, b, type) { oDown[a].push({ id: b, type: type }); oUp[b].push({ id: a, type: type }); }
    DATA.edges.forEach(function (e) {
      if (byId[e[0]] && byId[e[1]] && byId[e[0]].type === "source" && present[e[1]] && byId[e[1]].type === "table") {
        if (!present[e[0]]) { add(e[0]); ids.unshift(e[0]); }
        link(e[0], e[1], e[2]);
      }
    });
    ids.forEach(function (topId) {
      if (byId[topId].type !== top) return;
      Object.keys(walk(topId, up)).forEach(function (id) {
        if (present[id] && byId[id].type === "table") { link(id, topId, "uses"); feeds[id] = true; }
      });
    });
    var unused = {}, count = { source: 0, table: 0 };
    ids.forEach(function (id) {
      var t = byId[id].type;
      if (t === "table" && !feeds[id]) unused[id] = true;
      count[t] = (count[t] || 0) + 1;
    });
    // Tables grouped under their source (the first by name when several load it), by name;
    // tables without a source in one "No source" band at the end
    var bands = { list: [], of: {} }, bandOfSource = {}, loose = [];
    var sources = ids.filter(function (id) { return byId[id].type === "source"; })
      .sort(function (a, b) { return compare(byId[a].label, byId[b].label); });
    var rank = {}; sources.forEach(function (id, i) { rank[id] = i; });
    var byName = function (a, b) {
      return compare(byId[a].label, byId[b].label) || compare(byId[a].model || "", byId[b].model || "");
    };
    sources.forEach(function (id) { bandOfSource[id] = { id: "band:" + rank[id], source: id, members: [] }; });
    ids.forEach(function (id) {
      if (byId[id].type !== "table") return;
      var first = null;
      oUp[id].forEach(function (e) { if (rank[e.id] !== undefined && (first === null || rank[e.id] < rank[first])) first = e.id; });
      if (first === null) loose.push(id); else bandOfSource[first].members.push(id);
    });
    function tables(k) { return fmt(k) + (k === 1 ? " table" : " tables"); }
    sources.forEach(function (id) {
      var b = bandOfSource[id];
      if (!b.members.length) return;
      b.members.sort(byName);
      b.label = byId[id].label + " · " + tables(b.members.length);
      bands.list.push(b);
    });
    if (loose.length) bands.list.push({ id: "band:none", source: null, members: loose.sort(byName), label: "No source · " + tables(loose.length) });
    bands.list.forEach(function (b) { b.members.forEach(function (id) { bands.of[id] = b.id; }); });
    var n = Object.keys(unused).length;
    var text = (m ? byId["model|" + m].label + ": " : MULTI ? "Everything: " : "Overview: ") +
      count.source + " sources → " + count.table + " tables → " + (count[top] || 0) + " " + plural(top) +
      (n ? " · " + n + (n === 1 ? " table " : " tables ") + (n === 1 ? feedsNone(top) : feedsNone(top).replace("feeds", "feed")) : "") +
      " · click: details, double-click: lineage";
    return { ids: ids, up: oUp, down: oDown, unused: unused, top: top, status: text, bands: bands };
  }
  // Several models, one level up: sources loaded by more than one model -> models -> reports
  function modelOverview() {
    var ids = [], present = {}, oUp = {}, oDown = {}, single = 0;
    function add(id) { if (!present[id]) { present[id] = true; ids.push(id); oUp[id] = []; oDown[id] = []; } }
    DATA.nodes.forEach(function (n) { if (n.type === "model") add(n.id); });
    DATA.model_edges.forEach(function (e) {
      var from = byId[e[0]], to = byId[e[1]];
      if (!from || !to) return;
      if (from.type === "source" && (from.shared || 0) < 2) return;  // one model's own: inside it
      add(e[0]); add(e[1]);
      oDown[e[0]].push({ id: e[1], type: e[2] }); oUp[e[1]].push({ id: e[0], type: e[2] });
    });
    DATA.nodes.forEach(function (n) { if (n.type === "source" && (n.shared || 0) < 2) single++; });
    var count = {};
    ids.forEach(function (id) { var t = byId[id].type; count[t] = (count[t] || 0) + 1; });
    var text = (count.model || 0) + " models → " + (count.report || 0) + " reports · " + (count.source || 0) +
      " sources shared by several models" + (single ? " (" + single + " more inside the models)" : "") +
      " · double-click a model to open it, ☰ Show everything for all tables";
    return { ids: ids, up: oUp, down: oDown, unused: {}, top: "report", status: text };
  }

  // Draw the current view; focus = ids to frame instead of the usual Fit (an expanded group)
  function renderGraph(focus) {
    var edgesG = $("edges"), nodesG = $("nodes");
    edgesG.textContent = ""; nodesG.textContent = ""; $("bands").textContent = "";
    viewport.classList.remove("has-hi", "has-mark");
    graph = null; groupNodes = {};
    var view = current();
    if (!view) { $("empty").style.display = "flex"; $("status").textContent = ""; scheduleMarkers(); renderResults(); return; }
    $("empty").style.display = "none";

    if (view.kind !== "item") {
      var ov = buildOverview(view);
      $("status").textContent = ov.status;
      draw(ov.ids, ov.up, ov.down, null, ov.unused, feedsNone(ov.top), ov.bands);
      fit();
      renderResults();  // an overview lists the whole model
      return;
    }

    var ids = lineage(state.selected);
    var grouped = groupLineage(ids, view);
    var counts = {}; ids.forEach(function (id) { var t = byId[id].type; counts[t] = (counts[t] || 0) + 1; });
    $("status").textContent = fmt(ids.length) + (ids.length === 1 ? " item: " : " items: ") +
      TYPES.filter(function (t) { return counts[t]; })
        .map(function (t) { return fmt(counts[t]) + " " + TYPE_LABEL[t].toLowerCase() + (counts[t] === 1 ? "" : "s"); }).join(", ") +
      (grouped.groups ? " (grouped: " + grouped.groups + (grouped.groups === 1 ? " group)" : " groups)") : "");
    draw(grouped.ids, grouped.up, grouped.down, state.selected, null, "");
    graph.memberOf = grouped.memberOf;
    graph.itemCount = ids.length;
    fit(focus);
    renderResults();  // "In this view": the drawn items and groups
  }

  // ---------- groups (big lineages) ----------
  // The group nodes of the drawn lineage (id "group:<n>" -> a node-like object); N(id) finds
  // an item or a group
  var groupNodes = {};
  function N(id) { return byId[id] || groupNodes[id]; }
  // Within each layer, a kind (column, measure, visual) with more than GROUP_THRESHOLD items is
  // shown as one node per table / page (and model), except groups of one, the selected item and
  // the groups the view has expanded. Edges between the drawn nodes are drawn once, with the
  // number of edges they stand for.
  // When a layer would still show more than GROUP_THRESHOLD table (page) groups of one kind, they
  // are one group for the layer (and model) first, e.g. "57 tables · 201 measures": expanding it
  // shows the per-table groups.
  function tableKey(n) { return [n.type, n.layer, n.m || "", n.group].join("\u0001"); }
  function layerKey(n) { return [n.type, n.layer, n.m || "", "\u0002"].join("\u0001"); }
  function groupLineage(ids, view) {
    var expanded = view.expanded || {}, perLayer = {}, tables = {}, perTables = {}, buckets = {}, order = [];
    function groupable(id) {
      var n = byId[id];
      return GROUPED[n.type] && id !== view.id && perLayer[n.layer + ":" + n.type] > GROUP_THRESHOLD;
    }
    ids.forEach(function (id) {
      var n = byId[id];
      if (GROUPED[n.type]) perLayer[n.layer + ":" + n.type] = (perLayer[n.layer + ":" + n.type] || 0) + 1;
    });
    ids.forEach(function (id) {
      if (!groupable(id)) return;
      var n = byId[id], key = tableKey(n), lk = layerKey(n);
      if (!tables[key]) { tables[key] = true; perTables[lk] = (perTables[lk] || 0) + 1; }
    });
    ids.forEach(function (id) {
      if (!groupable(id)) return;
      var n = byId[id], key = layerKey(n);
      if (perTables[key] <= GROUP_THRESHOLD || expanded[key]) {
        key = tableKey(n);
        if (expanded[key]) return;
      }
      if (!buckets[key]) { buckets[key] = []; order.push(key); }
      buckets[key].push(id);
    });
    var memberOf = {}, groups = 0;
    order.forEach(function (key) {
      var members = buckets[key];
      if (members.length < 2) return;
      var first = byId[members[0]], gid = "group:" + groups++, whole = key === layerKey(first);
      var name = whole ? fmt(perTables[key]) + (first.type === "visual" ? " pages" : " tables")
        : first.group || (first.type === "visual" ? "(no page)" : "(no table)");
      groupNodes[gid] = { id: gid, isGroup: true, key: key, whole: whole, type: first.type, layer: first.layer,
        m: first.m, model: first.model, group: whole ? "" : first.group, name: name, members: members,
        label: name + " · " + fmt(members.length) + " " + GROUPED[first.type], unused: false, details: {} };
      members.forEach(function (id) { memberOf[id] = gid; });
    });
    var shown = [], gUp = {}, gDown = {}, inView = new Set(ids), links = {};
    ids.forEach(function (id) {
      var r = memberOf[id] || id;
      if (!gDown[r]) { gDown[r] = []; gUp[r] = []; shown.push(r); }
    });
    ids.forEach(function (a) {
      down[a].forEach(function (e) {
        if (!inView.has(e.id)) return;
        var ra = memberOf[a] || a, rb = memberOf[e.id] || e.id;
        if (ra === rb) return;
        var k = ra + "\u0000" + rb, link = links[k];
        if (!link) {
          link = links[k] = { id: rb, type: e.type, count: 0 };
          gDown[ra].push(link); gUp[rb].push({ id: ra, type: e.type });
        }
        link.count++;
        if (link.type === "filters" && e.type !== "filters") link.type = e.type;
      });
    });
    return { ids: shown, up: gUp, down: gDown, memberOf: memberOf, groups: groups };
  }
  // Expand a group of the current view: its members replace it, the view is laid out again
  function expand(gid) {
    var g = groupNodes[gid], view = current();
    if (!g || !view) return;
    (view.expanded = view.expanded || {})[g.key] = true;
    if (state.marked === gid) state.marked = null;
    renderGraph(g.members);
    restoreMark();
    updateButtons();
  }
  function collapseGroups() {
    var view = current();
    if (!view || !view.expanded) return;
    view.expanded = {};
    renderGraph();
    if (state.marked && !graph.nodes[state.marked] && graph.memberOf[state.marked]) state.marked = graph.memberOf[state.marked];
    restoreMark();
    updateButtons();
  }
  // After a new drawing: the marked item's classes (and details) again
  function restoreMark() { var id = state.marked; state.marked = null; mark(id); }
  // Go to an item from the list, the details or a link: its collapsed group (if any) expands,
  // then it is marked and brought into view
  function jump(id) {
    var gid = graph && graph.memberOf && graph.memberOf[id];
    if (gid && groupNodes[gid]) {
      var expanded = current().expanded = current().expanded || {};
      expanded[layerKey(byId[id])] = true; expanded[tableKey(byId[id])] = true;
      renderGraph([id]);
      updateButtons();
    }
    mark(id);
    reveal(id);
  }

  // Draw nodes and edges (edges from downAdj); unusedSet marks extra "unused" nodes (tooltip:
  // unusedText, e.g. "feeds no report")
  // Text for SVG markup built as one string (much faster than an element at a time)
  function esc(text) {
    return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }
  function draw(ids, upAdj, downAdj, selectedId, unusedSet, unusedText, bands) {
    var edgesG = $("edges"), nodesG = $("nodes");
    var present = {}; ids.forEach(function (id) { present[id] = true; });
    var placed = layout(ids, upAdj, downAdj, bands), xy = placed.xy, box = placed.box;
    // Built once per drawing: node elements, each node's edge elements and neighbours (marking
    // and highlighting touch only these), and the bounds Fit uses (from the layout)
    graph = { ids: ids, xy: xy, box: box, limit: placed.limit, selected: selectedId, nodes: {}, edges: {}, near: {},
              bounds: null, hi: null, memberOf: {}, itemCount: ids.length };
    listGen++;  // a new drawing: "In this view" lists it
    ids.forEach(function (id) { graph.edges[id] = []; graph.near[id] = [id]; });
    var edgeHtml = [], edgeEnds = [], nodeHtml = [], bandHtml = [], bandEdges = {};
    function path(x1, y1, x2, y2, type, title) {
      var mid = (x1 + x2) / 2;
      edgeHtml.push('<path d="M' + x1 + "," + y1 + " C" + mid + "," + y1 + " " + mid + "," + y2 + " " + x2 + "," + y2 +
                    '" class="edge ' + esc(type) + '"' + (title ? "><title>" + esc(title) + "</title></path>" : "/>"));
    }
    ids.forEach(function (id) {
      var a = xy[id], x1 = a.x + box[id].w, y1 = a.y + box[id].h / 2;
      downAdj[id].forEach(function (n) {
        if (!present[n.id]) return;
        graph.near[id].push(n.id); graph.near[n.id].push(id);
        var bid = bands && N(id).type === "source" && bands.of[n.id];
        if (bid) {  // a source's edge goes to the band of its tables, drawn once
          var key = id + "\u0000" + bid, ends = bandEdges[key];
          if (!ends) {
            var band = placed.bands[bid];
            ends = bandEdges[key] = [id];
            ends.at = edgeHtml.length;
            path(x1, y1, band.x, band.y + BAND_PAD + LINE_H / 2, n.type, null);
            edgeEnds.push(ends);
          }
          ends.push(n.id);
          return;
        }
        var b = xy[n.id];
        // an edge to or from a group stands for several: their number is its tooltip
        path(x1, y1, b.x, b.y + box[n.id].h / 2, n.type, n.count > 1 ? fmt(n.count) + " links" : null);
        edgeEnds.push([id, n.id]);
      });
    });
    Object.keys(bandEdges).forEach(function (key) {  // "loads 12 tables" on a band's edge
      var ends = bandEdges[key];
      if (ends.length > 2) {
        edgeHtml[ends.at] = edgeHtml[ends.at].replace("/>", "><title>" + fmt(ends.length - 1) + " tables</title></path>");
      }
    });

    var minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    function extend(x, y, w, h) {
      minX = Math.min(minX, x); minY = Math.min(minY, y); maxX = Math.max(maxX, x + w); maxY = Math.max(maxY, y + h);
    }
    Object.keys(placed.bands).forEach(function (bid) {
      var b = placed.bands[bid];
      extend(b.x, b.y, b.w, b.h);
      bandHtml.push('<g class="band" transform="translate(' + b.x + "," + b.y + ')"><rect width="' + b.w + '" height="' + b.h + '"/>' +
        b.lines.map(function (line, i) { return '<text x="' + BAND_PAD + '" y="' + (BAND_PAD + 17 + LINE_H * i) + '">' + esc(line) + "</text>"; }).join("") +
        "<title>" + esc(b.label) + "</title></g>");
    });
    ids.forEach(function (id) {
      var n = N(id), p = xy[id], s = box[id];
      extend(p.x, p.y, s.w, s.h);
      if (n.isGroup) { nodeHtml.push(groupMarkup(n, p, s)); return; }
      var unused = n.unused || !!(unusedSet && unusedSet[id]);
      var differs = MULTI && DATA.differs[id];
      nodeHtml.push('<g class="node' + (id === selectedId ? " selected" : "") + (unused ? " unused" : "") +
        '" transform="translate(' + p.x + "," + p.y + ')" tabindex="0" data-id="' + esc(id) + '">' +
        '<rect class="box" width="' + s.w + '" height="' + s.h + '" stroke="' + (unused ? "var(--unused)" : color(n.type)) + '"/>' +
        '<rect width="5" height="' + s.h + '" fill="' + color(n.type) + '" stroke="none"/>' + nodeLines(s) +
        (differs ? '<text x="' + (s.w - 22) + '" y="26" class="diff">≠</text>' : "") +
        "<title>" + esc(TYPE_LABEL[n.type] + ": " + n.label + (where(n) ? "\n" + where(n) : "") +
          (n.unused ? "\nUnused" : unused ? "\n" + unusedText.charAt(0).toUpperCase() + unusedText.slice(1) : "") +
          (differs ? "\n≠ Not the same in another model (see its details)" : "")) + "</title></g>");
    });
    if (ids.length) graph.bounds = { x: minX, y: minY, width: maxX - minX, height: maxY - minY };
    $("bands").innerHTML = bandHtml.join("");
    edgesG.innerHTML = edgeHtml.join("");
    nodesG.innerHTML = nodeHtml.join("");
    var paths = edgesG.children, gs = nodesG.children;
    edgeEnds.forEach(function (ends, i) { ends.forEach(function (id) { graph.edges[id].push(paths[i]); }); });
    ids.forEach(function (id, i) { graph.nodes[id] = gs[i]; });
    viewport.classList.remove("has-hi", "has-mark");
  }
  // A node's name (1 or 2 lines) and its sub-line (type · table, one line, cut to the box)
  function nodeLines(s) {
    var t = s.text;
    return t.lines.map(function (line, i) { return '<text x="' + PAD_L + '" y="' + (26 + LINE_H * i) + '">' + esc(line) + "</text>"; }).join("") +
      '<text x="' + PAD_L + '" y="' + (26 + LINE_H * (t.lines.length - 1) + 21) + '" class="type">' + esc(truncate(t.sub, s.w - t.pads, "s")) + "</text>";
  }
  // A group node: two boxes stacked behind its own, "<table> · <n> measures" and a "+"
  function groupMarkup(n, p, s) {
    var stroke = color(n.type), box = '" width="' + s.w + '" height="' + s.h + '" stroke="' + stroke + '"/>';
    return '<g class="node group" transform="translate(' + p.x + "," + p.y + ')" tabindex="0" data-id="' + esc(n.id) + '">' +
      '<rect class="stack" x="6" y="-6' + box + '<rect class="stack" x="3" y="-3' + box +
      '<rect class="box' + box +
      '<rect width="5" height="' + s.h + '" fill="' + stroke + '" stroke="none"/>' + nodeLines(s) +
      '<g class="expand"><rect x="' + (s.w - 36) + '" y="' + (s.h / 2 - 14) + '" width="28" height="28" rx="5"/>' +
      '<text x="' + (s.w - 22) + '" y="' + (s.h / 2 + 8) + '">+</text></g>' +
      "<title>" + esc(TYPE_LABEL[n.type] + " group: " + n.label + (MULTI && n.model ? "\n" + n.model : "") +
        "\nClick: its members · double-click, Enter or +: expand") + "</title></g>";
  }

  // One handler set on the node group for every node. Click: mark it and show its details;
  // double-click (or Enter): open its lineage; hover: highlight its lines
  var nodesG = $("nodes");
  function nodeOf(ev) { var g = ev.target.closest && ev.target.closest(".node"); return g && g.dataset.id ? g : null; }
  // A group opens by expanding (its members replace it), an item by showing its lineage
  function open(id) { if (groupNodes[id]) expand(id); else select(id); }
  nodesG.addEventListener("click", function (ev) {
    var g = nodeOf(ev); if (!g) return;
    ev.stopPropagation();
    if (groupNodes[g.dataset.id] && ev.target.closest(".expand")) expand(g.dataset.id); else mark(g.dataset.id);
  });
  nodesG.addEventListener("dblclick", function (ev) { var g = nodeOf(ev); if (g) { ev.stopPropagation(); open(g.dataset.id); } });
  nodesG.addEventListener("keydown", function (ev) {
    var g = nodeOf(ev); if (!g) return;
    if (ev.key === "Enter") open(g.dataset.id);
    else if (ev.key === " ") { ev.preventDefault(); mark(g.dataset.id); }
  });
  // mouseenter/-leave do not bubble, but an ancestor sees them while capturing
  nodesG.addEventListener("mouseenter", function (ev) {
    if (ev.target.classList && ev.target.classList.contains("node")) highlight(ev.target.dataset.id);
  }, true);
  nodesG.addEventListener("mouseleave", function (ev) {
    if (ev.target.classList && ev.target.classList.contains("node")) highlight(state.marked);
  }, true);

  // ---------- marking (single click) ----------
  // The drawn graph (see draw); null when nothing is drawn
  var graph = null;
  function mark(id) {
    var prev = state.marked;
    state.marked = id;
    if (graph) {
      if (prev && graph.nodes[prev]) graph.nodes[prev].classList.remove("marked");
      if (id && graph.nodes[id] && id !== state.selected) graph.nodes[id].classList.add("marked");
      viewport.classList.toggle("has-mark", !!id && !!graph.nodes[id]);
    }
    syncRows();
    highlight(id);
    renderDetails();
    if (id) { autoOpenDetails(); keepClear(id); }  // narrow pages: never under the details overlay
  }

  // Bring a node of the current graph into view (same zoom); nothing if it is not shown
  function reveal(id) {
    var p = graph && graph.xy[id];
    if (!p || !graph.nodes[id]) return;
    var w = areaW(), h = size().h;
    var x = state.tx + p.x * state.scale, y = state.ty + p.y * state.scale;
    var b = graph.box[id];
    if (x < 0 || y < 50 || x + b.w * state.scale > w || y + b.h * state.scale > h) {
      state.tx = w / 2 - (p.x + b.w / 2) * state.scale;
      state.ty = h / 2 - (p.y + b.h / 2) * state.scale;
      apply();
    }
  }

  // Highlight an item's lines and mark its neighbours (.rel): only the previous and the new
  // item's elements change; everything else is dimmed by one class on the graph group
  function highlight(id) {
    if (!graph) return;
    var prev = graph.hi;
    if (id && !graph.nodes[id]) id = null;
    if (prev === id) return;
    if (prev) {
      graph.edges[prev].forEach(function (p) { p.classList.remove("hi"); });
      graph.near[prev].forEach(function (n) { graph.nodes[n].classList.remove("rel"); });
    }
    if (id) {
      graph.edges[id].forEach(function (p) { p.classList.add("hi"); });
      graph.near[id].forEach(function (n) { graph.nodes[n].classList.add("rel"); });
    }
    graph.hi = id;
    viewport.classList.toggle("has-hi", !!id);
  }

  // ---------- details ----------
  // Entries are links with data-id, one handler set on the panel: a click marks the item (its
  // group expands), a double-click opens its lineage (also when the click rebuilt the details:
  // firstClick / secondClick). Long lists are built LIST_PAGE links at a time.
  var detailsObserver = window.IntersectionObserver ? new IntersectionObserver(function (entries) {
    entries.forEach(function (e) { if (e.isIntersecting && e.target.fill) e.target.fill(); });
  }, { root: $("detailsBody") }) : null;
  var SECTION_OPEN = 10;  // a section with more items starts collapsed
  function plain(t) { return t.label + (where(t) ? " (" + where(t) + ")" : "") + (t.unused ? " (unused)" : ""); }
  function typed(t) { return TYPE_LABEL[t.type] + ": " + t.label + (where(t) ? " (" + where(t) + ")" : ""); }
  function count(k, type) { return fmt(k) + " " + TYPE_LABEL[type].toLowerCase() + (k === 1 ? "" : "s"); }
  function byLabelId(a, b) { return compare(byId[a].label, byId[b].label) || compare(byId[a].group || "", byId[b].group || ""); }
  function lazyLinks(dd, ids, text) {
    var done = 0, more = null;
    function fill() {
      if (more) { if (detailsObserver) detailsObserver.unobserve(more); more.remove(); more = null; }
      var end = Math.min(ids.length, done + LIST_PAGE), frag = document.createDocumentFragment();
      for (var i = done; i < end; i++) {
        var a = html("a", null, text(byId[ids[i]])); a.dataset.id = ids[i]; frag.appendChild(a);
      }
      done = end;
      if (done < ids.length) {
        more = html("a", "more", "Show more (" + fmt(ids.length - done) + " more)"); more.fill = fill;
        frag.appendChild(more);
      }
      dd.appendChild(frag);
      if (more && detailsObserver) detailsObserver.observe(more);
    }
    if (!ids.length) dd.appendChild(html("span", "muted", "-")); else fill();
  }
  // One list: "<title> (N)"
  function linkList(title, ids) {
    var box = html("div", "kv");
    box.appendChild(html("dt", null, title + " (" + fmt(ids.length) + ")"));
    var dd = html("dd", "links");
    lazyLinks(dd, ids, typed);
    box.appendChild(dd);
    return box;
  }
  // "<title>: 42 columns · 512 measures · 18 visuals" and a collapsible section per bucket
  // ({name, ids}; more than SECTION_OPEN items: collapsed unless open), its links sorted and
  // built when it first opens
  function sections(title, summary, buckets, openAll) {
    var box = html("div", "kv grouped"), dt = html("dt", null, title + ": ");
    dt.appendChild(html("span", "counts", summary || "-"));
    box.appendChild(dt);
    var dd = html("dd");
    buckets.forEach(function (b) {
      var sec = html("details", "sect"), list = html("div", "links"), filled = false;
      sec.appendChild(html("summary", null, b.name + " (" + fmt(b.ids.length) + ")"));
      sec.appendChild(list);
      function fill() { if (!filled) { filled = true; lazyLinks(list, b.ids.slice().sort(byLabelId), plain); } }
      if (openAll || b.ids.length <= SECTION_OPEN) { sec.open = true; fill(); }
      sec.addEventListener("toggle", function () { if (sec.open) fill(); });
      dd.appendChild(sec);
    });
    box.appendChild(dd);
    return box;
  }
  // "Built from" / "Used by": per type, in the flow order
  function byType(title, ids) {
    var per = {};
    ids.forEach(function (id) { var t = byId[id].type; (per[t] = per[t] || []).push(id); });
    var types = TYPES.filter(function (t) { return per[t]; });
    return sections(title, types.map(function (t) { return count(per[t].length, t); }).join(" · "),
      types.map(function (t) { return { name: TYPE_LABEL[t] + "s", ids: per[t] }; }));
  }
  $("detailsBody").addEventListener("click", function (ev) {
    if (secondClick(ev)) return;
    var a = ev.target.closest && ev.target.closest("a");
    if (!a) return;
    if (a.fill) { lastClick = null; a.fill(); return; }
    if (!a.dataset.id) return;  // e.g. a model's own lineage viewer (a real link)
    firstClick(a.dataset.id);
    jump(a.dataset.id);
  });
  $("detailsBody").addEventListener("dblclick", function (ev) {
    if (justOpened()) return;
    var a = ev.target.closest && ev.target.closest("a[data-id]");
    if (a) open(a.dataset.id);
  });
  // DAX: formatted (dax_formatted, when it differs) or as written; the choice is kept for the
  // session (sessionStorage, try/catch: without it, for this page) and applies to every item
  var DAX_KEY = "pbixtractor.lineage.daxAsWritten", daxAsWritten = false;
  try { daxAsWritten = sessionStorage.getItem(DAX_KEY) === "1"; } catch (e) { /* no storage */ }
  function expressionBox(n, value) {
    var box = html("div", "kv"), dt = html("dt", "daxhead");
    dt.appendChild(html("span", null, "Expression"));
    var formatted = n.dax_formatted && !daxAsWritten;
    if (n.dax_formatted) {
      var toggle = html("button", "linkbtn", daxAsWritten ? "Show formatted" : "Show as written");
      toggle.id = "daxToggle";
      toggle.title = daxAsWritten ? "Show the DAX formatted (daxformatter.com style)" : "Show the DAX exactly as it is written in the model";
      toggle.addEventListener("click", function () {
        daxAsWritten = !daxAsWritten;
        try { sessionStorage.setItem(DAX_KEY, daxAsWritten ? "1" : "0"); } catch (e) { /* no storage */ }
        renderDetails();
      });
      dt.appendChild(toggle);
    }
    box.appendChild(dt);
    var dd = html("dd");
    dd.appendChild(html("pre", null, formatted ? n.dax_formatted : value));
    box.appendChild(dd);
    return box;
  }

  function renderDetails() {
    var body = $("detailsBody"); body.textContent = "";
    var view = current();
    var byLabel = function (a, b) { return byId[a].label.localeCompare(byId[b].label); };
    if (view && MULTI && view.kind === "overview" && !state.marked) {
      var start = buildOverview(view);
      body.appendChild(html("h2", null, "Lineage across models"));
      body.appendChild(html("p", "muted", "The documented models, their reports, and the sources " +
        "that more than one model loads. Double-click a model to open it; ☰ Show everything shows " +
        "every table of every model."));
      body.appendChild(linkList("Models", start.ids.filter(function (id) { return byId[id].type === "model"; }).sort(byLabel)));
      body.appendChild(linkList("Sources shared by several models", start.ids.filter(function (id) { return byId[id].type === "source"; }).sort(byLabel)));
      return;
    }
    if (view && view.kind !== "item" && !state.marked) {
      var ov = buildOverview(view), modelNode = view.kind === "model" ? byId[view.id] : null;
      body.appendChild(html("h2", null, modelNode ? modelNode.label : MULTI ? "Every model" : "System overview"));
      if (modelNode) modelLink(body, modelNode);
      body.appendChild(html("p", "muted", "Where the data comes from (sources), the model tables it " +
        "lands in, and the " + (ov.top === "report" ? "reports" : "report pages") + " that use each table. Click an item for its " +
        "details, double-click it to open its full lineage."));
      body.appendChild(linkList("Tables that " + feedsNone(ov.top).replace("feeds", "feed"), Object.keys(ov.unused).sort(byLabel)));
      body.appendChild(linkList(ov.top === "report" ? "Reports" : "Pages", ov.ids.filter(function (id) { return byId[id].type === ov.top; })));
      body.appendChild(linkList("Sources", ov.ids.filter(function (id) { return byId[id].type === "source"; }).sort(byLabel)));
      return;
    }
    var n = N(state.marked || state.selected);
    if (!n) { body.appendChild(html("p", "muted", "Nothing selected.")); return; }
    if (n.isGroup) { groupDetails(body, n); return; }
    var badge = html("span", "badge", TYPE_LABEL[n.type]); badge.style.background = color(n.type);
    var title = html("h2"); title.appendChild(badge); title.appendChild(document.createTextNode(n.label));
    body.appendChild(title);
    if (where(n)) body.appendChild(html("div", "muted", where(n)));
    if (n.type === "model") modelLink(body, n);
    if (n.id !== state.selected && !(n.type === "model" && view && view.kind === "model" && view.id === n.id)) {
      var openBtn = html("button", "btn primary", "Open its lineage");
      openBtn.style.marginTop = "10px";
      openBtn.title = "Same as double-clicking the item";
      openBtn.addEventListener("click", function () { select(n.id); });
      body.appendChild(openBtn);
    }
    var dl = html("dl"); dl.style.marginTop = "12px";
    Object.keys(n.details).forEach(function (key) {
      var value = String(n.details[key]);
      if (key === "Expression") { dl.appendChild(expressionBox(n, value)); return; }
      var box = html("div", "kv");
      box.appendChild(html("dt", null, key));
      box.appendChild(html("dd", null, value));
      dl.appendChild(box);
    });
    body.appendChild(dl);
    if (MULTI && DATA.same[n.id]) {
      var pairs = DATA.same[n.id], apart = pairs.filter(function (p) { return !p[1]; });
      body.insertBefore(html("div", "notice", apart.length
        ? "≠ Not the same in " + apart.length + " other model" + (apart.length === 1 ? "" : "s") + ": " +
          apart.map(function (p) { return byId[p[0]].model + " (" + p[2].join(", ") + ")"; }).join("; ") + "."
        : "= The same in " + pairs.length + " other model" + (pairs.length === 1 ? "" : "s") + "."), dl);
      body.appendChild(sameElsewhere(n, pairs));
    }
    if (n.type === "model") {
      body.appendChild(linkList("Reports", (DATA.model_edges || []).filter(function (e) { return e[0] === n.id; })
        .map(function (e) { return e[1]; })));
      return;
    }
    [["Built from", up], ["Used by", down]].forEach(function (pair) {
      var seen = new Set(), ids = [];
      pair[1][n.id].forEach(function (m) { if (byId[m.id] && !seen.has(m.id)) { seen.add(m.id); ids.push(m.id); } });
      body.appendChild(byType(pair[0], ids));
    });
  }

  // A group: its name, count and members, in the same sections (a click on one expands the
  // group and marks it); a layer's group lists its members per table (page)
  function groupDetails(body, g) {
    var badge = html("span", "badge", TYPE_LABEL[g.type] + " group"); badge.style.background = color(g.type);
    var title = html("h2"); title.appendChild(badge); title.appendChild(document.createTextNode(g.label));
    body.appendChild(title);
    if (MULTI && g.model) body.appendChild(html("div", "muted", g.model));
    body.appendChild(html("p", "muted", fmt(g.members.length) + " " + GROUPED[g.type] + " of " +
      (g.whole ? g.name : (g.type === "visual" ? "the page " : "the table ") + g.name) + " in this lineage, shown as one item." +
      (g.whole ? " Expanding it shows them per " + (g.type === "visual" ? "page." : "table.") : "")));
    var button = html("button", "btn primary", "+ Expand the group");
    button.title = "Same as double-clicking the group";
    button.addEventListener("click", function () { expand(g.id); });
    body.appendChild(button);
    var buckets;
    if (g.whole) {
      var per = {}, names = [];
      g.members.forEach(function (id) {
        var name = byId[id].group || (g.type === "visual" ? "(no page)" : "(no table)");
        if (!per[name]) { per[name] = []; names.push(name); }
        per[name].push(id);
      });
      buckets = names.sort(compare).map(function (name) { return { name: name, ids: per[name] }; });
    } else buckets = [{ name: TYPE_LABEL[g.type] + "s", ids: g.members }];
    var box = sections("Members", count(g.members.length, g.type) +
      (g.whole ? " in " + fmt(buckets.length) + (g.type === "visual" ? " pages" : " tables") : ""), buckets, !g.whole);
    box.style.marginTop = "12px";
    body.appendChild(box);
  }

  // The model's own lineage viewer (its full page, in a new tab)
  function modelLink(body, n) {
    if (!n.link) return;
    var a = html("a", "btn", "Open its own lineage viewer ↗");
    a.href = n.link; a.target = "_blank"; a.rel = "noopener";
    a.style.marginTop = "8px"; a.style.display = "inline-block";
    body.appendChild(a);
  }

  // The same measure / column / table in other models: identical, or what differs
  function sameElsewhere(n, pairs) {
    var box = html("div", "kv");
    box.appendChild(html("dt", null, "Same " + TYPE_LABEL[n.type].toLowerCase() + " in other models (" + pairs.length + ")"));
    var dd = html("dd", "links");
    pairs.forEach(function (pair) {
      var target = byId[pair[0]];
      if (!target) return;
      var a = html("a", null, (pair[1] ? "= identical · " : "≠ " + pair[2].join(", ") + " differs · ") + target.model +
        (target.group ? " (" + target.group + ")" : ""));
      a.title = pair[1] ? "The same (DAX formatting ignored)" : "Same name, different " + pair[2].join(", ");
      a.dataset.id = target.id;  // click: mark, double-click: open (the panel's handlers)
      dd.appendChild(a);
    });
    box.appendChild(dd);
    return box;
  }

  // ---------- views and history ----------
  // A view is {kind: "overview"}, {kind: "item", id} or - several models - {kind: "model", id}
  // (one model's overview) and {kind: "all"} (every model's tables); back/forward move through them
  var views = [], at = -1;
  function current() { return views[at]; }
  function show(view) {
    var cur = current();
    if (!(cur && cur.kind === view.kind && cur.id === view.id)) {
      views = views.slice(0, at + 1);
      views.push(view);
      at = views.length - 1;
    }
    render();
  }
  function render() {
    var view = current();
    state.selected = view && view.kind !== "overview" && view.kind !== "all" ? view.id : null;
    state.marked = null;
    if (state.selected) autoOpenDetails();  // before the drawing: Fit leaves its width out
    renderGraph(); renderDetails(); updateButtons();  // renderGraph also renders the list
    // Keep the address in sync (#<item id>) so a view can be linked to, without adding history
    try {
      history.replaceState(null, "", state.selected ? "#" + encodeURIComponent(state.selected) : location.pathname + location.search);
    } catch (e) { /* not allowed for this document: links to items just do not update */ }
  }
  function select(id) { show(byId[id] && byId[id].type === "model" ? { kind: "model", id: id } : { kind: "item", id: id }); }
  function back() { if (at > 0) { at--; render(); } }
  function forward() { if (at < views.length - 1) { at++; render(); } }

  // One level up: column/measure -> its table, visual -> its page, page -> its report, (several
  // models: table/report -> its model, model or everything -> the start view) else -> overview
  function parentOf(view) {
    if (!view || view.kind === "overview") return null;
    if (view.kind !== "item") return { kind: "overview" };
    var n = byId[view.id];
    var item = function (local) { var id = pid(n, local); return byId[id] ? { kind: "item", id: id } : null; };
    var parent = (n.type === "column" || n.type === "measure") ? item("table:" + n.group)
      : n.type === "visual" ? item("page:" + n.group)
      : n.type === "page" ? item("report:" + n.group) : null;
    if (parent) return parent;
    if (MULTI && n.m && byId["model|" + n.m]) return { kind: "model", id: "model|" + n.m };
    return { kind: "overview" };
  }
  function levelUp() { var parent = parentOf(current()); if (parent) show(parent); }

  function updateButtons() {
    var view = current(), parent = parentOf(view);
    $("back").disabled = at <= 0;
    $("forward").disabled = at >= views.length - 1;
    $("levelUp").disabled = !parent;
    $("levelUp").title = !parent ? "Already at the overview" : parent.kind === "overview"
      ? (MULTI ? "Zoom out one level: the start view (models)" : "Zoom out one level: the system overview")
      : "Zoom out one level: " + TYPE_LABEL[byId[parent.id].type].toLowerCase() + " " + byId[parent.id].label;
    $("overview").disabled = !!view && view.kind === "overview";
    $("everything").disabled = !!view && view.kind === "all";
    $("direction").disabled = !view || view.kind !== "item";
    $("collapse").hidden = !(view && view.expanded && Object.keys(view.expanded).length);
  }

  // ---------- pan & zoom ----------
  var svg = $("canvas"), viewport = $("viewport");
  // The graph area's size, read once (reading it right after drawing would make the browser lay
  // out the new graph at once); read again after a resize or the Panels toggle
  var svgSize = null;
  function size() {
    if (!svgSize) svgSize = { w: svg.clientWidth || 800, h: svg.clientHeight || 600 };
    return svgSize;
  }
  // The visible graph area's width: the graph minus the details overlay (narrow pages)
  function areaW() { return size().w - covered(); }

  // ---------- the details panel on narrow pages ----------
  // Below NARROW_W px page width the details are an overlay of at most DETAILS_W px on the right
  // of the graph, collapsed to a slim tab (TAB_W) by its handle. It starts collapsed and opens by
  // itself when an item is marked or opened, until the handle was used. Computed from these
  // numbers, never measured in the page.
  var NARROW_W = 1200, DETAILS_W = 320, TAB_W = 26, detailsTouched = false;
  var NARROW = window.matchMedia ? window.matchMedia("(max-width: " + (NARROW_W - 1) + "px)") : { matches: false };
  function detailsCollapsed() { return $("app").classList.contains("details-collapsed"); }
  function covered() {
    if (!NARROW.matches || $("app").classList.contains("wide")) return 0;
    return detailsCollapsed() ? TAB_W : Math.min(DETAILS_W, Math.max(0, window.innerWidth - 40));
  }
  function updateDetailsToggle() {
    var collapsed = detailsCollapsed(), t = $("detailsToggle");
    t.innerHTML = "<span>" + (collapsed ? "◂ Details" : "Hide details ▸") + "</span>";
    t.title = collapsed ? "Show the details panel" : "Collapse the details panel to a tab (the graph gets the space)";
    t.setAttribute("aria-expanded", collapsed ? "false" : "true");
    $("offscreen").style.right = covered() + "px";  // the right marker and the toolbar stay beside it
    $("toolbar").style.right = covered() + 10 + "px";
  }
  function setDetails(open, byHand) {
    if (byHand) detailsTouched = true;
    if (open === !detailsCollapsed()) return;
    $("app").classList.toggle("details-collapsed", !open);
    updateDetailsToggle();
    scheduleMarkers();
  }
  // Opened by itself (narrow pages) for a marked / opened item, unless collapsed by hand
  function autoOpenDetails() { if (NARROW.matches && !detailsTouched) setDetails(true); }
  // A marked item under the overlay is moved into the visible area
  function keepClear(id) {
    var p = graph && graph.xy[id];
    if (!p || !graph.nodes[id] || !covered()) return;
    if (state.tx + (p.x + graph.box[id].w) * state.scale > areaW()) reveal(id);
  }
  $("detailsToggle").addEventListener("click", function () {
    setDetails(detailsCollapsed(), true);
    if (!detailsCollapsed()) keepClear(state.marked || state.selected);
  });
  $("app").classList.add("details-collapsed");
  updateDetailsToggle();

  function apply() {
    viewport.setAttribute("transform", "translate(" + state.tx + "," + state.ty + ") scale(" + state.scale + ")");
    svg.classList.toggle("small-text", state.scale < TEXT_MIN_SCALE);
    if (!wheelTimer) scheduleMarkers();  // while the wheel zooms: once it stops
  }
  // Fit: the whole view when it fits with readable text (zoom >= TEXT_MIN_SCALE), else the
  // selected item and its direct neighbours (a group counts as one); focus = other ids to frame
  // (an expanded group's members, an item jumped to). Never zooms in beyond FIT_MAX (labels
  // about 14 px; + zooms further). Everything is framed in the visible graph area (areaW: the
  // details overlay left out). Bounds and positions come from the layout: nothing is measured.
  function fit(focus) {
    if (!graph || !graph.bounds) return;
    var whole = scaleFor(graph.bounds);
    var ids = focus || (graph.selected ? [graph.selected] : null);
    if (!focus && whole >= TEXT_MIN_SCALE) { frame(graph.bounds, whole); return; }
    if (!ids) { fromLeft(graph.bounds); return; }  // an overview too big to read at once
    var anchor = ids.length === 1 ? graph.xy[ids[0]] : null, size1 = anchor ? graph.box[ids[0]] : null;
    if (ids.length === 1) ids = graph.near[ids[0]] || ids;
    var box = boundsOf(ids);
    if (!box) { frame(graph.bounds, whole); return; }
    var scale = scaleFor(box);
    if (scale >= TEXT_MIN_SCALE) { frame(box, scale); return; }
    // too many neighbours to read at once: readable text around the item (vertically centred),
    // without empty space left or right of the neighbours (the markers show the rest)
    var s = TEXT_MIN_SCALE, w = areaW(), h = size().h, c = anchor ? { x: anchor.x + size1.w / 2, y: anchor.y + size1.h / 2 }
      : { x: box.x + box.width / 2, y: box.y + box.height / 2 };
    function place(mid, lo, hi, from, to) {  // screen offset: centred, kept inside [lo, hi] of the box
      return Math.max(Math.min((from + to) / 2 - mid * s, from + 20 - lo * s), to - 20 - hi * s);
    }
    state.scale = s;
    state.tx = place(c.x, box.x, box.x + box.width, 0, w);
    state.ty = (TOOLBAR_H + h) / 2 - c.y * s;
    apply();
  }
  // The readable zoom, the view's left edge at the left (the markers show the rest); centred
  // vertically when it fits (wrapped layers do), else from the top
  function fromLeft(box) {
    var s = TEXT_MIN_SCALE, room = size().h - TOP_H - 10;
    state.scale = s;
    state.tx = 20 - box.x * s;
    state.ty = TOP_H + Math.max(0, (room - box.height * s) / 2) - box.y * s;
    apply();
  }
  function scaleFor(box) {
    return Math.min(FIT_MAX, (areaW() - 40) / Math.max(box.width, 1), (size().h - TOP_H - 20) / Math.max(box.height, 1));
  }
  function frame(box, scale) {
    state.scale = scale;
    state.tx = (areaW() - box.width * scale) / 2 - box.x * scale;
    state.ty = TOP_H + (size().h - TOP_H - 10 - box.height * scale) / 2 - box.y * scale;
    apply();
  }
  function boundsOf(ids) {
    var minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    ids.forEach(function (id) {
      var at = graph.xy[id] ? id : graph.memberOf[id], p = graph.xy[at]; if (!p) return;  // or its group
      minX = Math.min(minX, p.x); minY = Math.min(minY, p.y);
      maxX = Math.max(maxX, p.x + graph.box[at].w); maxY = Math.max(maxY, p.y + graph.box[at].h);
    });
    return minX === Infinity ? null : { x: minX, y: minY, width: maxX - minX, height: maxY - minY };
  }
  $("fit").addEventListener("click", function () { fit(); });
  $("collapse").addEventListener("click", collapseGroups);

  // ---------- off-screen markers ----------
  // "12 more ←" at each edge of the graph area: items of the view beyond it, from the layout's
  // coordinates and the current pan/zoom; recounted at most once per animation frame
  var SIDES = ["left", "right", "top", "bottom"], ARROW = { left: "←", right: "→", top: "↑", bottom: "↓" };
  var markerEls = {}, markerFrame = 0, markerText = {};
  Array.prototype.forEach.call(document.querySelectorAll("#offscreen .off"), function (el) {
    markerEls[el.dataset.side] = el;
    el.addEventListener("click", function () { panToward(el.dataset.side); });
  });
  function scheduleMarkers() {
    if (!markerFrame) markerFrame = window.requestAnimationFrame ? requestAnimationFrame(updateMarkers) : setTimeout(updateMarkers, 16);
  }
  function hiddenBySide() {
    var out = { left: [], right: [], top: [], bottom: [] };
    if (!graph) return out;
    var w = areaW(), h = size().h, s = state.scale;
    graph.ids.forEach(function (id) {
      var p = graph.xy[id], x = state.tx + p.x * s, y = state.ty + p.y * s, nw = graph.box[id].w * s, nh = graph.box[id].h * s;
      if (x + nw < 0) out.left.push(id);
      else if (x > w) out.right.push(id);
      else if (y + nh < TOOLBAR_H - 10) out.top.push(id);
      else if (y > h) out.bottom.push(id);
    });
    return out;
  }
  function updateMarkers() {
    markerFrame = 0;
    var hidden = hiddenBySide();
    SIDES.forEach(function (side) {
      var n = hidden[side].length, el = markerEls[side];
      if (el.classList.contains("on") !== !!n) el.classList.toggle("on", !!n);
      if (!n || markerText[side] === n) return;  // unchanged: the page is not touched
      markerText[side] = n;
      el.textContent = fmt(n) + " more " + ARROW[side];
      el.title = fmt(n) + (n === 1 ? " item lies" : " items lie") + " beyond this edge - click to bring the nearest into view";
    });
  }
  // Pan (same zoom) so that the nearest hidden items on that side come into view
  function panToward(side) {
    var ids = hiddenBySide()[side];
    if (!ids.length) return;
    var s = state.scale, horizontal = side === "left" || side === "right", best = null;
    ids.forEach(function (id) {
      var p = graph.xy[id], v = horizontal ? p.x : p.y;
      if (!best || (side === "left" || side === "top" ? v > best : v < best)) best = v;
    });
    if (side === "left") state.tx = 30 - best * s;
    else if (side === "right") state.tx = areaW() - 30 - (best + NODE_MAX_W) * s;
    else if (side === "top") state.ty = TOOLBAR_H + 30 - best * s;
    else state.ty = size().h - 30 - (best + nodeHeight(2)) * s;
    apply();
  }
  function zoomBy(factor) {
    var mx = areaW() / 2, my = size().h / 2;
    var s = Math.max(0.05, Math.min(4, state.scale * factor));
    state.tx = mx - (mx - state.tx) * (s / state.scale); state.ty = my - (my - state.ty) * (s / state.scale);
    state.scale = s; apply();
  }
  $("zoomIn").addEventListener("click", function () { zoomBy(1.25); });
  $("zoomOut").addEventListener("click", function () { zoomBy(0.8); });
  $("back").addEventListener("click", back);
  $("forward").addEventListener("click", forward);
  $("levelUp").addEventListener("click", levelUp);
  $("overview").addEventListener("click", function () { show({ kind: "overview" }); });
  $("everything").addEventListener("click", function () { show({ kind: "all" }); });
  $("panels").addEventListener("click", function () {
    $("app").classList.toggle("wide");
    updateDetailsToggle();
    requestAnimationFrame(function () { svgSize = null; fit(); });
  });
  svg.addEventListener("wheel", function (e) {
    e.preventDefault();
    var r = svg.getBoundingClientRect(), mx = e.clientX - r.left, my = e.clientY - r.top;
    var factor = Math.exp(-e.deltaY * 0.0015), s = Math.max(0.05, Math.min(4, state.scale * factor));
    state.tx = mx - (mx - state.tx) * (s / state.scale); state.ty = my - (my - state.ty) * (s / state.scale);
    clearTimeout(wheelTimer); wheelTimer = wheelTimer || -1;
    state.scale = s; apply();
    // No text while the wheel (or a pinch) zooms: it comes back when it stops
    svg.classList.add("zooming");
    clearTimeout(wheelTimer);
    wheelTimer = setTimeout(function () { wheelTimer = null; svg.classList.remove("zooming"); scheduleMarkers(); }, 150);
  }, { passive: false });
  var wheelTimer = null;
  var drag = null, dragged = false;
  svg.addEventListener("mousedown", function (e) {
    drag = { x: e.clientX - state.tx, y: e.clientY - state.ty, startX: e.clientX, startY: e.clientY };
    dragged = false;
    svg.classList.add("dragging");
  });
  window.addEventListener("mousemove", function (e) {
    if (!drag) return;
    if (Math.abs(e.clientX - drag.startX) + Math.abs(e.clientY - drag.startY) > 4) dragged = true;
    state.tx = e.clientX - drag.x; state.ty = e.clientY - drag.y; apply();
  });
  // A click on empty space (not a drag) clears the marked item
  svg.addEventListener("click", function () { if (!dragged && state.marked) mark(null); });
  window.addEventListener("mouseup", function () { drag = null; svg.classList.remove("dragging"); });
  // A new height can change how layers wrap: then the view is laid out again
  window.addEventListener("resize", function () {
    svgSize = null;
    updateDetailsToggle();
    if (!current()) return;
    if (graph && graph.limit !== wrapLimit()) { renderGraph(); restoreMark(); } else fit();
  });
  window.addEventListener("keydown", function (e) {
    if (e.altKey && e.key === "ArrowLeft") { e.preventDefault(); back(); return; }
    if (e.altKey && e.key === "ArrowRight") { e.preventDefault(); forward(); return; }
    if (e.key !== "Escape") return;
    if ($("guide").classList.contains("open")) showGuide(false); else if (NARROW.matches) setDetails(false, true);
  });

  // ---------- guide ----------
  TYPES.filter(function (t) { return t !== "model"; }).forEach(function (t, i) {  // the data flow
    if (i) $("guideFlow").appendChild(html("span", "muted", "→"));
    var step = html("span", "step"), dot = html("span", "dot");
    dot.style.background = color(t);
    step.appendChild(dot); step.appendChild(document.createTextNode(TYPE_LABEL[t]));
    $("guideFlow").appendChild(step);
  });
  // The Guide never opens by itself: a small "New here?" hint shows beside its button until it
  // has been opened once (remembered per browser; storage can be blocked, e.g. file:// in some
  // browsers: then the hint shows each time)
  var GUIDE_KEY = "pbixtractor.lineage.guideSeen";
  function showGuide(open) {
    $("guide").classList.toggle("open", open);
    if (!open) return;
    $("guideClose").focus();
    $("guideHint").hidden = true;
    try { localStorage.setItem(GUIDE_KEY, "1"); } catch (e) { /* no storage */ }
  }
  $("help").addEventListener("click", function () { showGuide(true); });
  $("guideHint").addEventListener("click", function () { showGuide(true); });
  $("guideClose").addEventListener("click", function () { showGuide(false); });
  $("guide").addEventListener("click", function (e) { if (e.target === $("guide")) showGuide(false); });
  var guideSeen = false;
  try { guideSeen = localStorage.getItem(GUIDE_KEY) === "1"; } catch (e) { /* no storage: the hint shows */ }
  $("guideHint").hidden = guideSeen;

  // ---------- start: the system overview, then the linked item (#<item id>) if any ----------
  // A changed #<item id> (a link, an edited address): an item of the drawn view (also inside a
  // collapsed group) is marked there, any other item opens
  window.addEventListener("hashchange", function () {
    var id = "";
    try { id = decodeURIComponent(location.hash.slice(1)); } catch (e) { return; }
    if (!byId[id] || id === state.selected) return;
    if (graph && (graph.nodes[id] || graph.memberOf[id])) jump(id); else select(id);
  });
  var linked = decodeURIComponent(location.hash.slice(1));  // read before render() resets it
  size();  // measured before anything is drawn: cheap
  show({ kind: "overview" });
  if (linked && byId[linked]) select(linked);
})();
</script>
</body>
</html>
"""
