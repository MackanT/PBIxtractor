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

TYPE_ORDER = ["source", "table", "column", "measure", "visual", "page"]


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

    for table in doc["model"]["tables"]:
        details[f"table:{table['name']}"] = {
            "Storage mode": table.get("storage_mode"),
            "Connector": table.get("connector"),
            "Source objects": ", ".join(table.get("source_objects") or []) or None,
            "Rows": table.get("rows"),
            "Size (MB)": round(table["size_bytes"] / 1e6, 2) if table.get("size_bytes") else None,
            "Hidden": table.get("hidden") or None,
            "Description": table.get("description"),
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
            elif item["type"] == "Group":
                label = f"{kind}: {item.get('name') or item['id']}"
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
                "group": node.get("table") or node.get("page") or "",
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
        "nodes": viewer_nodes,
        "edges": edges,
        "stats": counts,
    }


def render_lineage_html(doc: dict) -> str:
    """The complete HTML page for a JSON documentation dict."""
    data = build_viewer_data(doc)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    payload = payload.replace("</", "<\\/")  # never close the <script> tag early
    return _TEMPLATE.replace("__TITLE__", escape(f"{data['report']} - lineage")).replace(
        "__DATA__", payload
    )


def write_lineage_html(path: str | Path, doc: dict) -> None:
    """Write the lineage viewer next to the other output files."""
    Path(path).write_text(render_lineage_html(doc), encoding="utf-8")


_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
:root {
  --bg: #f6f7f9; --panel: #ffffff; --text: #1d2330; --muted: #5f6b7a; --border: #d9dee5;
  --accent: #2563eb; --edge: #9aa5b4; --edge-hi: #1d4ed8; --shadow: 0 1px 2px rgba(0,0,0,.06);
  --source: #7c3aed; --table: #0f766e; --column: #0891b2; --measure: #d97706;
  --visual: #2563eb; --page: #475569; --unused: #dc2626;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #11151c; --panel: #181e27; --text: #e6e9ee; --muted: #9aa5b4; --border: #2b3442;
    --accent: #60a5fa; --edge: #4b5667; --edge-hi: #93c5fd; --shadow: none;
    --source: #a78bfa; --table: #2dd4bf; --column: #22d3ee; --measure: #fbbf24;
    --visual: #60a5fa; --page: #94a3b8; --unused: #f87171;
  }
}
* { box-sizing: border-box; }
html, body { margin: 0; height: 100%; background: var(--bg); color: var(--text);
  font: 13px/1.4 "Segoe UI", system-ui, -apple-system, sans-serif; }
#app { display: grid; grid-template-columns: 290px 1fr 360px; height: 100vh; }
aside, #details { background: var(--panel); border-right: 1px solid var(--border);
  display: flex; flex-direction: column; min-height: 0; }
#details { border-right: 0; border-left: 1px solid var(--border); }
header { padding: 14px 16px 10px; border-bottom: 1px solid var(--border); }
h1 { font-size: 15px; margin: 0 0 2px; }
h2 { font-size: 14px; margin: 0; word-break: break-word; }
.muted { color: var(--muted); }
.section { padding: 10px 16px; border-bottom: 1px solid var(--border); }
input[type=search] { width: 100%; padding: 7px 9px; border: 1px solid var(--border);
  border-radius: 6px; background: var(--bg); color: var(--text); font: inherit; }
.chips { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
.chip { display: inline-flex; align-items: center; gap: 5px; padding: 3px 8px;
  border: 1px solid var(--border); border-radius: 999px; cursor: pointer; user-select: none; }
.chip input { margin: 0; }
.dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; }
#results { overflow: auto; flex: 1; padding: 4px 0; }
.result { padding: 5px 16px; cursor: pointer; display: flex; gap: 8px; align-items: baseline; }
.result:hover, .result.active { background: var(--bg); }
.result .name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; }
.result .group { color: var(--muted); font-size: 11px; white-space: nowrap; }
main { position: relative; min-width: 0; min-height: 0; }
#toolbar { position: absolute; top: 10px; left: 10px; right: 10px; display: flex; gap: 8px;
  align-items: center; flex-wrap: wrap; z-index: 2; pointer-events: none; }
#toolbar > * { pointer-events: auto; }
.btn, select { background: var(--panel); color: var(--text); border: 1px solid var(--border);
  border-radius: 6px; padding: 5px 9px; font: inherit; cursor: pointer; box-shadow: var(--shadow); }
#status { background: var(--panel); border: 1px solid var(--border); border-radius: 6px;
  padding: 5px 9px; box-shadow: var(--shadow); }
svg { width: 100%; height: 100%; display: block; cursor: grab; }
svg.dragging { cursor: grabbing; }
.node rect { fill: var(--panel); stroke-width: 1.5; rx: 6; }
.node text { fill: var(--text); font-size: 12px; pointer-events: none; }
.node .type { fill: var(--muted); font-size: 10px; }
.node { cursor: pointer; }
.node.selected rect { stroke-width: 3; }
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
  border-radius: 6px; white-space: pre-wrap; word-break: break-word; font: 12px/1.45 Consolas, monospace; }
.links a { color: var(--accent); cursor: pointer; display: block; padding: 1px 0; }
.badge { display: inline-block; padding: 1px 7px; border-radius: 999px; font-size: 11px;
  color: #fff; margin-right: 6px; }
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
    </header>
    <div class="section">
      <input type="search" id="search" placeholder="Search tables, columns, measures, visuals, pages" aria-label="Search">
      <div class="chips" id="types"></div>
      <label class="chip" style="margin-top:8px"><input type="checkbox" id="unusedOnly"> Only unused columns/measures</label>
    </div>
    <div id="results" role="listbox"></div>
  </aside>
  <main>
    <div id="toolbar">
      <select id="direction" aria-label="Direction">
        <option value="both">Upstream + downstream</option>
        <option value="up">Upstream only (built from)</option>
        <option value="down">Downstream only (used by)</option>
      </select>
      <button class="btn" id="fit">Fit</button>
      <span id="status"></span>
    </div>
    <svg id="canvas" aria-label="Lineage graph"><g id="viewport"><g id="edges"></g><g id="nodes"></g></g></svg>
    <div id="empty">Select an item on the left to see its lineage.</div>
  </main>
  <section id="details"><div class="body" id="detailsBody"></div></section>
</div>
<script type="application/json" id="data">__DATA__</script>
<script>
(function () {
  "use strict";
  var DATA = JSON.parse(document.getElementById("data").textContent);
  var TYPES = ["source", "table", "column", "measure", "visual", "page"];
  var TYPE_LABEL = { source: "Source", table: "Table", column: "Column", measure: "Measure",
                     visual: "Visual", page: "Page" };
  var MAX_NODES = 600, NODE_W = 210, NODE_H = 34, GAP_X = 90, GAP_Y = 12;

  var byId = {}, up = {}, down = {};
  DATA.nodes.forEach(function (n) { byId[n.id] = n; up[n.id] = []; down[n.id] = []; });
  DATA.edges.forEach(function (e) {
    if (!byId[e[0]] || !byId[e[1]]) return;
    down[e[0]].push({ id: e[1], type: e[2] });
    up[e[1]].push({ id: e[0], type: e[2] });
  });

  var state = { selected: null, direction: "both", types: {}, query: "", unusedOnly: false,
                scale: 1, tx: 0, ty: 0 };
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
      if (!state.query) return true;
      return (n.label + " " + n.group).toLowerCase().indexOf(state.query) >= 0;
    });
    matches.sort(function (a, b) {
      return TYPES.indexOf(b.type) - TYPES.indexOf(a.type) || a.group.localeCompare(b.group) ||
             a.label.localeCompare(b.label);
    });
    matches.slice(0, 300).forEach(function (n) {
      var row = html("div", "result" + (n.id === state.selected ? " active" : ""));
      row.setAttribute("role", "option");
      var dot = html("span", "dot"); dot.style.background = color(n.type);
      row.appendChild(dot);
      row.appendChild(html("span", "name", n.label));
      if (n.group) row.appendChild(html("span", "group", n.group));
      row.title = TYPE_LABEL[n.type] + ": " + n.label + (n.unused ? " (unused)" : "");
      row.addEventListener("click", function () { select(n.id); });
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
  function layout(ids) {
    var present = {}; ids.forEach(function (id) { present[id] = true; });
    var layers = {};
    ids.forEach(function (id) { var l = byId[id].layer; (layers[l] = layers[l] || []).push(id); });
    var keys = Object.keys(layers).map(Number).sort(function (a, b) { return a - b; });
    keys.forEach(function (k) {
      layers[k].sort(function (a, b) {
        return byId[a].group.localeCompare(byId[b].group) || byId[a].label.localeCompare(byId[b].label);
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
    for (var i = 0; i < 2; i++) { sweep(keys.slice(1), up); sweep(keys.slice(0, -1).reverse(), down); }
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

  function renderGraph() {
    var edgesG = $("edges"), nodesG = $("nodes");
    edgesG.textContent = ""; nodesG.textContent = "";
    if (!state.selected) { $("empty").style.display = "flex"; $("status").textContent = ""; return; }
    $("empty").style.display = "none";

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
    var present = {}; ids.forEach(function (id) { present[id] = true; });
    var xy = layout(ids);
    var counts = {}; ids.forEach(function (id) { var t = byId[id].type; counts[t] = (counts[t] || 0) + 1; });
    $("status").textContent = ids.length + " items: " + TYPES.filter(function (t) { return counts[t]; })
      .map(function (t) { return counts[t] + " " + TYPE_LABEL[t].toLowerCase(); }).join(", ") +
      (truncated ? " (nearest " + MAX_NODES + " shown)" : "");

    var edgeEls = [];
    ids.forEach(function (id) {
      down[id].forEach(function (n) {
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
      var g = el("g", { "class": "node" + (id === state.selected ? " selected" : "") + (n.unused ? " unused" : ""),
                        transform: "translate(" + p.x + "," + p.y + ")", tabindex: "0" });
      g.appendChild(el("rect", { width: NODE_W, height: NODE_H, stroke: n.unused ? "var(--unused)" : color(n.type) }));
      g.appendChild(el("rect", { width: 5, height: NODE_H, fill: color(n.type), stroke: "none" }));
      var label = n.label.length > 30 ? n.label.slice(0, 29) + "…" : n.label;
      g.appendChild(el("text", { x: 12, y: 15 }, label));
      g.appendChild(el("text", { x: 12, y: 28, "class": "type" },
        TYPE_LABEL[n.type] + (n.group ? " · " + (n.group.length > 26 ? n.group.slice(0, 25) + "…" : n.group) : "")));
      g.appendChild(el("title", {}, TYPE_LABEL[n.type] + ": " + n.label + (n.group ? "\n" + n.group : "") + (n.unused ? "\nUnused" : "")));
      g.addEventListener("click", function (ev) { ev.stopPropagation(); select(id); });
      g.addEventListener("keydown", function (ev) { if (ev.key === "Enter") select(id); });
      g.addEventListener("mouseenter", function () { highlight(id, edgeEls); });
      g.addEventListener("mouseleave", function () { highlight(null, edgeEls); });
      nodesG.appendChild(g);
    });
    fit();
  }

  function highlight(id, edgeEls) {
    edgeEls.forEach(function (p) {
      var on = id && (p.dataset.from === id || p.dataset.to === id);
      p.classList.toggle("hi", !!on);
      p.classList.toggle("dim", !!id && !on);
    });
  }

  // ---------- details ----------
  function renderDetails() {
    var body = $("detailsBody"); body.textContent = "";
    var n = byId[state.selected];
    if (!n) { body.appendChild(html("p", "muted", "Nothing selected.")); return; }
    var badge = html("span", "badge", TYPE_LABEL[n.type]); badge.style.background = color(n.type);
    var title = html("h2"); title.appendChild(badge); title.appendChild(document.createTextNode(n.label));
    body.appendChild(title);
    if (n.group) body.appendChild(html("div", "muted", n.group));
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
    [["Built from", up], ["Used by", down]].forEach(function (pair) {
      var items = pair[1][n.id];
      var box = html("div", "kv");
      box.appendChild(html("dt", null, pair[0] + " (" + items.length + ")"));
      var dd = html("dd", "links");
      items.slice(0, 200).forEach(function (m) {
        var target = byId[m.id]; if (!target) return;
        var a = html("a", null, TYPE_LABEL[target.type] + ": " + target.label + (target.group ? " (" + target.group + ")" : ""));
        a.addEventListener("click", function () { select(m.id); });
        dd.appendChild(a);
      });
      if (!items.length) dd.appendChild(html("span", "muted", "-"));
      box.appendChild(dd); body.appendChild(box);
    });
  }

  function select(id) {
    state.selected = id;
    renderResults(); renderGraph(); renderDetails();
    $("app").classList.add("show-details");
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
  svg.addEventListener("wheel", function (e) {
    e.preventDefault();
    var r = svg.getBoundingClientRect(), mx = e.clientX - r.left, my = e.clientY - r.top;
    var factor = Math.exp(-e.deltaY * 0.0015), s = Math.max(0.05, Math.min(4, state.scale * factor));
    state.tx = mx - (mx - state.tx) * (s / state.scale); state.ty = my - (my - state.ty) * (s / state.scale);
    state.scale = s; apply();
  }, { passive: false });
  var drag = null;
  svg.addEventListener("mousedown", function (e) { drag = { x: e.clientX - state.tx, y: e.clientY - state.ty }; svg.classList.add("dragging"); });
  window.addEventListener("mousemove", function (e) { if (!drag) return; state.tx = e.clientX - drag.x; state.ty = e.clientY - drag.y; apply(); });
  window.addEventListener("mouseup", function () { drag = null; svg.classList.remove("dragging"); });
  window.addEventListener("resize", function () { if (state.selected) fit(); });
  window.addEventListener("keydown", function (e) { if (e.key === "Escape") $("app").classList.remove("show-details"); });

  // ---------- start: the page with the most visuals ----------
  renderResults();
  var startPage = DATA.nodes.filter(function (n) { return n.type === "page"; })
    .sort(function (a, b) { return up[b.id].length - up[a.id].length; })[0];
  if (startPage) select(startPage.id); else renderDetails();
})();
</script>
</body>
</html>
"""
