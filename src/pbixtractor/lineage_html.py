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
        viewer_nodes.append(
            {
                "id": node_id,
                "type": node_type,
                "label": visual_labels.get(node_id, node["label"]),
                # a field's table, a visual's page, a page's report
                "group": node.get("table") or node.get("page") or node.get("report") or "",
                "layer": layer,
                "unused": node_id in unused,
                "details": details.get(node_id, {}),
            }
        )

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
.chips { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
.chip { display: inline-flex; align-items: center; gap: 5px; padding: 3px 8px;
  border: 1px solid var(--border); border-radius: 999px; cursor: pointer; user-select: none; }
.chip input { margin: 0; }
.dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; }
#results { overflow: auto; flex: 1; padding: 4px 0; }
.result { padding: 5px 16px; cursor: pointer; display: flex; gap: 8px; align-items: baseline; }
.result:hover, .result.active { background: var(--bg); }
.result.marked { background: var(--bg); box-shadow: inset 3px 0 0 var(--attn); }
.node text.diff { fill: var(--attn); font-weight: 700; font-size: 15px; }
.result .diff, a .diff { color: var(--attn); font-weight: 700; }
.notice { margin: 10px 0 4px; padding: 6px 9px; border-radius: 8px; font-size: 12px;
  border-left: 3px solid var(--attn); background: var(--bg); }
.rls { margin-top: 8px; padding: 6px 9px; border-radius: 8px; font-size: 12px;
  border-left: 3px solid var(--attn); background: var(--bg); }
.result .name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; }
.result .group { color: var(--muted); font-size: 11px; white-space: nowrap; }
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
.node text { fill: var(--text); font-size: 12px; pointer-events: none; }
.node .type { fill: var(--muted); font-size: 10px; }
.node { cursor: pointer; }
.node.selected rect { stroke-width: 3; }
.node.marked rect:first-of-type { stroke: var(--attn); stroke-width: 3.5; }
.btn.primary { background: var(--accent); color: var(--on-color); border-color: var(--accent); }
.node.unused rect { stroke-dasharray: 5 3; }
.node.dim { opacity: .25; }
.edge { fill: none; stroke: var(--edge); stroke-width: 1.2; }
.edge.filters { stroke-dasharray: 4 3; }
.edge.hi { stroke: var(--edge-hi); stroke-width: 2.2; }
.edge.dim { opacity: .15; }
#empty { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center;
  color: var(--muted); pointer-events: none; }
#details .body { overflow: auto; padding: 10px 16px 20px; flex: 1; }
.kv { margin: 0 0 10px; }
.kv dt { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .03em; }
.kv dd { margin: 2px 0 0; white-space: pre-wrap; word-break: break-word; }
pre { margin: 2px 0 0; padding: 8px; background: var(--bg); border: 1px solid var(--border);
  border-radius: 8px; white-space: pre-wrap; word-break: break-word; font: 12px/1.45 Consolas, monospace; }
.links a { color: var(--accent); cursor: pointer; display: block; padding: 1px 0; }
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
@media (max-width: 1100px) { #app { grid-template-columns: 260px 1fr; } #details { display: none; }
  #app.show-details #details { display: flex; position: fixed; right: 0; top: 0; bottom: 0;
  width: min(360px, 100vw); z-index: 5; } }
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
      <div class="chips" id="types"></div>
      <label class="chip" style="margin-top:8px"><input type="checkbox" id="unusedOnly"> Only unused columns/measures</label>
      <select id="modelFilter" aria-label="Model" hidden style="margin-top:8px; width:100%"><option value="">All models</option></select>
    </div>
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
        <button class="btn" id="fit" title="Show the whole graph">Fit</button>
        <button class="btn" id="zoomIn" title="Zoom in" aria-label="Zoom in">+</button>
      </span>
      <button class="btn" id="panels" title="Hide or show the side panels for a bigger graph">⇔ Panels</button>
      <button class="btn" id="help" title="How to read this view">? Guide</button>
      <span id="status"></span>
    </div>
    <svg id="canvas" aria-label="Lineage graph"><g id="viewport"><g id="edges"></g><g id="nodes"></g></g></svg>
    <div id="empty">Select an item on the left to see its lineage.</div>
  </main>
  <section id="details"><div class="body" id="detailsBody"></div></section>
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
      <li><b>− Fit +</b>: zoom out, show everything, zoom in (the mouse wheel zooms too).</li>
      <li><b>⇔ Panels</b>: hide the side panels for a bigger graph (click again to bring them back).</li>
    </ul>

    <h3>Selecting an item</h3>
    <ul>
      <li><b>Click</b> an item in the graph to mark it: its lines are highlighted and its details
        appear on the right - the graph stays as it is. Click empty space to unmark.</li>
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
      <li><b>Left</b>: search, show/hide item types, and "Only unused columns/measures". Click an
        item to mark it (its node is highlighted in the graph when shown, and its details appear
        on the right); double-click (or Enter) to open its lineage.</li>
      <li><b>Middle</b>: the graph. Drag to move, scroll to zoom, hover an item to highlight its
        lines; click to mark, double-click to open.</li>
      <li><b>Right</b>: details of the marked (or shown) item - DAX, data type, storage mode,
        rows/size when the report was open in Power BI Desktop - and "Built from" / "Used by"
        lists; clicking an entry there opens its lineage.</li>
    </ul>

    <h3>Worth knowing</h3>
    <ul>
      <li><b>⚠</b> before a button: it points to a bookmark or page that no longer exists.</li>
      <li>"dependencies by text matching" at the top left: Tabular Editor was not used, so
        dependencies are found by searching the DAX text and can be incomplete.</li>
      <li>Very large lineages show only the nearest 600 items (the status bar says so).</li>
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
  var MAX_NODES = 600, NODE_W = 210, NODE_H = 34, GAP_X = 90, GAP_Y = 12;

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
  var svgNS = "http://www.w3.org/2000/svg";
  function color(type) { return "var(--" + type + ")"; }
  function el(tag, attrs, text) {
    var node = document.createElementNS(svgNS, tag);
    for (var k in attrs) node.setAttribute(k, attrs[k]);
    if (text != null) node.textContent = text;
    return node;
  }
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

  TYPES.forEach(function (t) {
    var chip = html("label", "chip");
    var box = html("input"); box.type = "checkbox"; box.checked = true;
    box.addEventListener("change", function () { state.types[t] = box.checked; renderResults(); });
    var dot = html("span", "dot"); dot.style.background = color(t);
    chip.appendChild(box); chip.appendChild(dot); chip.appendChild(document.createTextNode(TYPE_LABEL[t]));
    $("types").appendChild(chip);
  });
  $("search").addEventListener("input", function (e) { state.query = e.target.value.toLowerCase(); renderResults(); });
  $("unusedOnly").addEventListener("change", function (e) { state.unusedOnly = e.target.checked; renderResults(); });
  $("direction").addEventListener("change", function (e) { state.direction = e.target.value; renderGraph(); });

  function renderResults() {
    var list = $("results"); list.textContent = "";
    var matches = DATA.nodes.filter(function (n) {
      if (!state.types[n.type]) return false;
      if (state.unusedOnly && !n.unused) return false;
      if (state.model && n.m !== state.model && !(n.type === "source" && (sourceModels[n.id] || {})[state.model])) return false;
      if (!state.query) return true;
      return (n.label + " " + where(n)).toLowerCase().indexOf(state.query) >= 0;
    });
    matches.sort(function (a, b) {
      return TYPES.indexOf(b.type) - TYPES.indexOf(a.type) || a.group.localeCompare(b.group) ||
             a.label.localeCompare(b.label);
    });
    matches.slice(0, 300).forEach(function (n) {
      var row = html("div", "result" + (n.id === state.selected ? " active" : "") +
                     (n.id === state.marked ? " marked" : ""));
      row.setAttribute("role", "option");
      row.tabIndex = 0;
      row.dataset.id = n.id;
      var dot = html("span", "dot"); dot.style.background = color(n.type);
      row.appendChild(dot);
      row.appendChild(html("span", "name", n.label));
      if (MULTI && DATA.differs[n.id]) row.appendChild(html("span", "diff", "≠"));
      if (where(n)) row.appendChild(html("span", "group", where(n)));
      row.title = TYPE_LABEL[n.type] + ": " + n.label + (n.unused ? " (unused)" : "");
      // Like the graph: click marks it (details on the right), double-click opens its lineage
      row.addEventListener("click", function () { mark(n.id); reveal(n.id); });
      row.addEventListener("dblclick", function () { select(n.id); });
      row.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter") select(n.id);
        else if (ev.key === " ") { ev.preventDefault(); mark(n.id); reveal(n.id); }
      });
      list.appendChild(row);
    });
    if (matches.length > 300) list.appendChild(html("div", "result muted", (matches.length - 300) + " more - refine the search"));
    if (!matches.length) list.appendChild(html("div", "result muted", "No matches"));
  }

  // ---------- lineage ----------
  function walk(start, neighbours) {
    var seen = {}, queue = [start];
    seen[start] = true;
    while (queue.length) {
      var id = queue.shift();
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

  // Layered layout: layer = data flow position; order within a layer by barycenter of neighbours
  function layout(ids, upAdj, downAdj) {
    var present = {}; ids.forEach(function (id) { present[id] = true; });
    var layers = {};
    ids.forEach(function (id) { var l = byId[id].layer; (layers[l] = layers[l] || []).push(id); });
    var keys = Object.keys(layers).map(Number).sort(function (a, b) { return a - b; });
    keys.forEach(function (k) {
      layers[k].sort(function (a, b) {
        return (byId[a].model || "").localeCompare(byId[b].model || "") ||
               byId[a].group.localeCompare(byId[b].group) || byId[a].label.localeCompare(byId[b].label);
      });
    });
    var pos = {};
    function index() { keys.forEach(function (k) { layers[k].forEach(function (id, i) { pos[id] = i; }); }); }
    function sweep(order, neighbours) {
      order.forEach(function (k) {
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
    var tallest = Math.max.apply(null, keys.map(function (k) { return layers[k].length; }));
    var xy = {};
    keys.forEach(function (k, column) {
      var offset = (tallest - layers[k].length) * (NODE_H + GAP_Y) / 2;
      layers[k].forEach(function (id, row) {
        xy[id] = { x: column * (NODE_W + GAP_X), y: offset + row * (NODE_H + GAP_Y) };
      });
    });
    return xy;
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
    var n = Object.keys(unused).length;
    var text = (m ? byId["model|" + m].label + ": " : MULTI ? "Everything: " : "Overview: ") +
      count.source + " sources → " + count.table + " tables → " + (count[top] || 0) + " " + plural(top) +
      (n ? " · " + n + (n === 1 ? " table " : " tables ") + (n === 1 ? feedsNone(top) : feedsNone(top).replace("feeds", "feed")) : "") +
      " · click: details, double-click: lineage";
    return { ids: ids, up: oUp, down: oDown, unused: unused, top: top, status: text };
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

  function renderGraph() {
    var edgesG = $("edges"), nodesG = $("nodes");
    edgesG.textContent = ""; nodesG.textContent = "";
    var view = current();
    if (!view) { $("empty").style.display = "flex"; $("status").textContent = ""; return; }
    $("empty").style.display = "none";

    if (view.kind !== "item") {
      var ov = buildOverview(view);
      $("status").textContent = ov.status;
      draw(ov.ids, ov.up, ov.down, null, ov.unused, feedsNone(ov.top));
      return;
    }

    var ids = lineage(state.selected);
    var truncated = ids.length > MAX_NODES;
    if (truncated) {
      // keep the selection and its closest neighbours
      var keep = {}; keep[state.selected] = true;
      var frontier = [state.selected];
      while (frontier.length && Object.keys(keep).length < MAX_NODES) {
        var next = [];
        frontier.forEach(function (id) {
          up[id].concat(down[id]).forEach(function (n) {
            if (!keep[n.id] && ids.indexOf(n.id) >= 0 && Object.keys(keep).length < MAX_NODES) { keep[n.id] = true; next.push(n.id); }
          });
        });
        frontier = next;
      }
      ids = Object.keys(keep);
    }
    var counts = {}; ids.forEach(function (id) { var t = byId[id].type; counts[t] = (counts[t] || 0) + 1; });
    $("status").textContent = ids.length + " items: " + TYPES.filter(function (t) { return counts[t]; })
      .map(function (t) { return counts[t] + " " + TYPE_LABEL[t].toLowerCase(); }).join(", ") +
      (truncated ? " (nearest " + MAX_NODES + " shown)" : "");
    draw(ids, up, down, state.selected, null, "");
  }

  // Draw nodes and edges (edges from downAdj); unusedSet marks extra "unused" nodes (tooltip:
  // unusedText, e.g. "feeds no report")
  function draw(ids, upAdj, downAdj, selectedId, unusedSet, unusedText) {
    var edgesG = $("edges"), nodesG = $("nodes");
    var present = {}; ids.forEach(function (id) { present[id] = true; });
    var xy = layout(ids, upAdj, downAdj);
    var edgeEls = [];
    ids.forEach(function (id) {
      downAdj[id].forEach(function (n) {
        if (!present[n.id]) return;
        var a = xy[id], b = xy[n.id];
        var x1 = a.x + NODE_W, y1 = a.y + NODE_H / 2, x2 = b.x, y2 = b.y + NODE_H / 2;
        var mid = (x1 + x2) / 2;
        var path = el("path", { d: "M" + x1 + "," + y1 + " C" + mid + "," + y1 + " " + mid + "," + y2 + " " + x2 + "," + y2,
                                "class": "edge " + n.type });
        path.dataset.from = id; path.dataset.to = n.id;
        edgesG.appendChild(path); edgeEls.push(path);
      });
    });

    ids.forEach(function (id) {
      var n = byId[id], p = xy[id];
      var unused = n.unused || !!(unusedSet && unusedSet[id]);
      var g = el("g", { "class": "node" + (id === selectedId ? " selected" : "") + (unused ? " unused" : ""),
                        transform: "translate(" + p.x + "," + p.y + ")", tabindex: "0" });
      g.appendChild(el("rect", { width: NODE_W, height: NODE_H, stroke: unused ? "var(--unused)" : color(n.type) }));
      g.appendChild(el("rect", { width: 5, height: NODE_H, fill: color(n.type), stroke: "none" }));
      var label = n.label.length > 30 ? n.label.slice(0, 29) + "…" : n.label;
      g.appendChild(el("text", { x: 12, y: 15 }, label));
      var sub = MULTI && n.model && n.type !== "model" ? n.model + (n.group ? " / " + n.group : "") : n.group;
      g.appendChild(el("text", { x: 12, y: 28, "class": "type" },
        TYPE_LABEL[n.type] + (sub ? " · " + (sub.length > 26 ? sub.slice(0, 25) + "…" : sub) : "")));
      var differs = MULTI && DATA.differs[id];
      if (differs) g.appendChild(el("text", { x: NODE_W - 16, y: 16, "class": "diff" }, "≠"));
      g.appendChild(el("title", {}, TYPE_LABEL[n.type] + ": " + n.label + (where(n) ? "\n" + where(n) : "") +
        (n.unused ? "\nUnused" : unused ? "\n" + unusedText.charAt(0).toUpperCase() + unusedText.slice(1) : "") +
        (differs ? "\n≠ Not the same in another model (see its details)" : "")));
      // Click: mark it and show its details; double-click (or Enter): open its lineage
      g.addEventListener("click", function (ev) { ev.stopPropagation(); mark(id); });
      g.addEventListener("dblclick", function (ev) { ev.stopPropagation(); select(id); });
      g.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter") select(id);
        else if (ev.key === " ") { ev.preventDefault(); mark(id); }
      });
      g.addEventListener("mouseenter", function () { highlight(id); });
      g.addEventListener("mouseleave", function () { highlight(state.marked); });
      g.dataset.id = id;
      nodesG.appendChild(g);
    });
    shownEdges = edgeEls;
    fit();
  }

  // ---------- marking (single click) ----------
  var shownEdges = [];
  function mark(id) {
    state.marked = id;
    document.querySelectorAll(".node").forEach(function (g) {
      g.classList.toggle("marked", !!id && g.dataset.id === id && id !== state.selected);
    });
    document.querySelectorAll(".result").forEach(function (row) {
      row.classList.toggle("marked", !!id && row.dataset.id === id);
    });
    highlight(id);
    renderDetails();
  }

  // Bring a node of the current graph into view (same zoom); nothing if it is not shown
  function reveal(id) {
    var g = document.querySelector('.node[data-id="' + (window.CSS && CSS.escape ? CSS.escape(id) : id) + '"]');
    if (!g) return;
    var m = g.transform.baseVal.consolidate().matrix, w = svg.clientWidth || 800, h = svg.clientHeight || 600;
    var x = state.tx + m.e * state.scale, y = state.ty + m.f * state.scale;
    if (x < 0 || y < 50 || x + NODE_W * state.scale > w || y + NODE_H * state.scale > h) {
      state.tx = w / 2 - (m.e + NODE_W / 2) * state.scale;
      state.ty = h / 2 - (m.f + NODE_H / 2) * state.scale;
      apply();
    }
  }

  function highlight(id) {
    shownEdges.forEach(function (p) {
      var on = id && (p.dataset.from === id || p.dataset.to === id);
      p.classList.toggle("hi", !!on);
      p.classList.toggle("dim", !!id && !on);
    });
  }

  // ---------- details ----------
  function linkList(title, ids) {
    var box = html("div", "kv");
    box.appendChild(html("dt", null, title + " (" + ids.length + ")"));
    var dd = html("dd", "links");
    ids.slice(0, 200).forEach(function (id) {
      var target = byId[id];
      var a = html("a", null, TYPE_LABEL[target.type] + ": " + target.label + (where(target) ? " (" + where(target) + ")" : ""));
      a.addEventListener("click", function () { select(id); });
      dd.appendChild(a);
    });
    if (!ids.length) dd.appendChild(html("span", "muted", "-"));
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
    var n = byId[state.marked || state.selected];
    if (!n) { body.appendChild(html("p", "muted", "Nothing selected.")); return; }
    var badge = html("span", "badge", TYPE_LABEL[n.type]); badge.style.background = color(n.type);
    var title = html("h2"); title.appendChild(badge); title.appendChild(document.createTextNode(n.label));
    body.appendChild(title);
    if (where(n)) body.appendChild(html("div", "muted", where(n)));
    if (n.type === "model") modelLink(body, n);
    if (n.id !== state.selected && !(n.type === "model" && view && view.kind === "model" && view.id === n.id)) {
      var open = html("button", "btn primary", "Open its lineage");
      open.style.marginTop = "10px";
      open.title = "Same as double-clicking the item";
      open.addEventListener("click", function () { select(n.id); });
      body.appendChild(open);
    }
    var dl = html("dl"); dl.style.marginTop = "12px";
    Object.keys(n.details).forEach(function (key) {
      var box = html("div", "kv");
      box.appendChild(html("dt", null, key));
      var value = String(n.details[key]);
      var dd = html("dd");
      if (key === "Expression") dd.appendChild(html("pre", null, value)); else dd.textContent = value;
      box.appendChild(dd); dl.appendChild(box);
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
      var ids = pair[1][n.id].map(function (m) { return m.id; }).filter(function (id) { return byId[id]; });
      body.appendChild(linkList(pair[0], ids));
    });
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
      a.addEventListener("click", function () { mark(target.id); });
      a.addEventListener("dblclick", function () { select(target.id); });
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
    renderResults(); renderGraph(); renderDetails(); updateButtons();
    if (state.selected) $("app").classList.add("show-details");
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
  }

  // ---------- pan & zoom ----------
  var svg = $("canvas"), viewport = $("viewport");
  function apply() { viewport.setAttribute("transform", "translate(" + state.tx + "," + state.ty + ") scale(" + state.scale + ")"); }
  var MIN_READABLE = 0.45;
  // Fit the whole graph; if that makes text unreadable, zoom to the selected item instead
  // (unless forced by the Fit button)
  function fit(force) {
    var box = viewport.getBBox(), w = svg.clientWidth || 800, h = svg.clientHeight || 600;
    if (!box.width) return;
    var scale = Math.min(1.2, (w - 40) / box.width, (h - 70) / box.height);
    var selected = document.querySelector(".node.selected");
    if (scale < MIN_READABLE && force !== true && selected) {
      var b = selected.getBBox(), m = selected.transform.baseVal.consolidate().matrix;
      state.scale = 0.8;
      state.tx = w / 2 - (m.e + b.width / 2) * state.scale;
      state.ty = h / 2 - (m.f + b.height / 2) * state.scale;
    } else {
      state.scale = scale;
      state.tx = (w - box.width * scale) / 2 - box.x * scale;
      state.ty = 50 + (h - 60 - box.height * scale) / 2 - box.y * scale;
    }
    apply();
  }
  $("fit").addEventListener("click", function () { fit(true); });
  function zoomBy(factor) {
    var mx = svg.clientWidth / 2, my = svg.clientHeight / 2;
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
    requestAnimationFrame(function () { fit(); });
  });
  svg.addEventListener("wheel", function (e) {
    e.preventDefault();
    var r = svg.getBoundingClientRect(), mx = e.clientX - r.left, my = e.clientY - r.top;
    var factor = Math.exp(-e.deltaY * 0.0015), s = Math.max(0.05, Math.min(4, state.scale * factor));
    state.tx = mx - (mx - state.tx) * (s / state.scale); state.ty = my - (my - state.ty) * (s / state.scale);
    state.scale = s; apply();
  }, { passive: false });
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
  window.addEventListener("resize", function () { if (current()) fit(); });
  window.addEventListener("keydown", function (e) {
    if (e.altKey && e.key === "ArrowLeft") { e.preventDefault(); back(); return; }
    if (e.altKey && e.key === "ArrowRight") { e.preventDefault(); forward(); return; }
    if (e.key !== "Escape") return;
    if ($("guide").classList.contains("open")) showGuide(false); else $("app").classList.remove("show-details");
  });

  // ---------- guide ----------
  TYPES.filter(function (t) { return t !== "model"; }).forEach(function (t, i) {  // the data flow
    if (i) $("guideFlow").appendChild(html("span", "muted", "→"));
    var step = html("span", "step"), dot = html("span", "dot");
    dot.style.background = color(t);
    step.appendChild(dot); step.appendChild(document.createTextNode(TYPE_LABEL[t]));
    $("guideFlow").appendChild(step);
  });
  function showGuide(open) {
    $("guide").classList.toggle("open", open);
    if (open) $("guideClose").focus();
  }
  $("help").addEventListener("click", function () { showGuide(true); });
  $("guideClose").addEventListener("click", function () { showGuide(false); });
  $("guide").addEventListener("click", function (e) { if (e.target === $("guide")) showGuide(false); });
  // Open the guide automatically the first time (storage can be blocked, e.g. file:// in some browsers)
  try {
    if (!localStorage.getItem("pbixtractor.lineageGuideSeen")) {
      showGuide(true);
      localStorage.setItem("pbixtractor.lineageGuideSeen", "1");
    }
  } catch (e) { /* no storage: the Guide button still works */ }

  // ---------- start: the system overview, then the linked item (#<item id>) if any ----------
  var linked = decodeURIComponent(location.hash.slice(1));  // read before render() resets it
  show({ kind: "overview" });
  if (linked && byId[linked]) select(linked);
})();
</script>
</body>
</html>
"""
