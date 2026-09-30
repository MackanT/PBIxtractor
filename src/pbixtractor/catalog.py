"""Catalog: a folder that collects the documentation of many models, searchable in one page.

    add_to_catalog("C:/Catalog", report_json, report_path, model_path, files)
    # C:/Catalog/catalog.html       - search across every model, report, page, table, column,
    #                                   measure and source; offline, one file
    # C:/Catalog/catalog.json       - the same data for other tools
    # C:/Catalog/entries/<key>.json - one slim entry per semantic model (latest run only)
    # C:/Catalog/models/<key>/      - that model's lineage viewer and workbook (full details)

One entry per semantic model (with the report(s) documented on it); adding the same model again
replaces its entry. The key comes from where the model was read: the Fabric model id, the
DevOps repository + model path, or the local model file. Entries are slim on purpose - what
exists and where it is used; quality findings, bookmarks etc. stay in the model's own files.
"""

import hashlib
import json
import re
import shutil
import time
from html import escape
from pathlib import Path
from typing import Optional

from . import __version__

SEPARATOR = " › "  # report_extractor.PREFIX_SEPARATOR: "<report> › <page>" in model mode
CATALOG_HTML = "catalog.html"
CATALOG_JSON = "catalog.json"


# ============================================================================
# Identity: which model an entry describes
# ============================================================================


def _slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "-", text).strip("-").lower()[:60] or "model"


def _inside(catalog_dir: Path, folder: str, name: str) -> Path:
    """<catalog>/<folder>/<name>, refusing anything that would resolve outside <catalog>/<folder>."""
    base = (Path(catalog_dir) / folder).resolve()
    path = (base / name).resolve()
    if path.parent != base:
        raise ValueError(f"Catalog key {name!r} is not a plain name")
    return path


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def source_identity(report_path: Path, model_path: Path, model_name: str) -> dict:
    """
    Where a model was read from, and the catalog key derived from it.

    Returns:
        {"key", "kind" (fabric | devops | file), "label", ...details}
    """
    report_path = Path(report_path)
    stem = report_path.name.removesuffix(".Report") if report_path.is_dir() else report_path.stem
    fabric = _read_json(report_path.parent / f"{stem}.fabric_source.json")
    if fabric and fabric.get("semantic_model", {}).get("id"):
        model = fabric["semantic_model"]
        return {
            # _slug: the id comes from a file next to the report, never trust it as a path
            "key": f"fabric-{_slug(str(model['id']))}",
            "kind": "fabric",
            "label": f"Fabric · {model.get('workspace', '')} / {model.get('name', model_name)}",
            "workspace": model.get("workspace"),
            "model_id": model["id"],
        }
    devops = _read_json(report_path.parent / f"{stem}.devops_source.json")
    if devops and devops.get("semantic_model"):
        where = f"{devops['project']}/{devops['repository']}:{devops['semantic_model']}"
        return {
            "key": f"devops-{_slug(devops['project'])}-{_slug(devops['repository'])}-"
            + hashlib.sha1(devops["semantic_model"].lower().encode()).hexdigest()[:8],
            "kind": "devops",
            "label": f"Azure DevOps · {where} @ {devops.get('version', '')}",
            "version": devops.get("version"),
        }
    resolved = str(Path(model_path).resolve())
    return {
        "key": f"file-{_slug(model_name)}-{hashlib.sha1(resolved.lower().encode()).hexdigest()[:8]}",
        "kind": "file",
        "label": f"File · {resolved}",
    }


# ============================================================================
# Entries
# ============================================================================


def _split_page(page: str, single_report: Optional[str]) -> tuple[str, str]:
    """ "<report> › <page>" -> (report, page); a single report's pages have no prefix."""
    if single_report is None and SEPARATOR in page:
        report, _, name = page.partition(SEPARATOR)
        return report, name
    return single_report or "", page


def build_entry(doc: dict, identity: dict, name: str, links: dict) -> dict:
    """
    The slim catalog entry of one documentation run (json_report.documentation_to_dict()).

    Args:
        doc: The run's JSON documentation
        identity: From source_identity()
        name: Model name shown in the catalog
        links: Relative paths of the model's full documentation ({"lineage", "workbook"})
    """
    pages = doc["report"]["pages"]
    model_mode = bool(pages) and all(SEPARATOR in p["name"] for p in pages)
    single = None if model_mode else doc["report"]["name"]

    reports: dict[str, list] = {}
    usage: dict[str, list[str]] = {}

    def use(ref: str, page: str) -> None:
        pages_of = usage.setdefault(ref, [])
        if page not in pages_of:
            pages_of.append(page)

    for page in pages:
        report, page_name = _split_page(page["name"], single)
        visuals = [i for i in page["items"] if i["type"] in ("Visual", "Slicer")]
        reports.setdefault(report, []).append(
            {"id": page["name"], "name": page_name, "hidden": bool(page.get("hidden")), "visuals": len(visuals)}
        )
        for item in page["items"]:
            for field in item.get("fields", []):
                use(f"{field['table']}[{field['name']}]", page["name"])
            for visual_filter in item.get("filters", []):
                use(visual_filter["field"], page["name"])
            if item["type"] == "Filter" and item.get("field"):
                use(item["field"], page["name"])
    for report_filter in doc["report"].get("filters", []):
        if report_filter.get("field") and not report_filter.get("page"):
            use(report_filter["field"], "(report filter)")

    # Measure names are unique in a model: "[Total]" (DAX text matching) -> "Sales[Total]"
    measure_refs = {
        m["name"]: f"{t['name']}[{m['name']}]" for t in doc["model"]["tables"] for m in t["measures"]
    }
    depends_on: dict[str, list[str]] = {}
    for dependency in doc.get("dependencies", []):
        target = dependency["target"]
        if target.startswith("[") and target[1:-1] in measure_refs:
            target = measure_refs[target[1:-1]]
        targets = depends_on.setdefault(dependency["source"], [])
        if target not in targets:
            targets.append(target)

    unused = set(doc["unused"]["columns"]) | set(doc["unused"]["measures"])

    def ref(table: str, item: str) -> str:
        return f"{table}[{item}]"

    tables = []
    for table in doc["model"]["tables"]:
        tables.append(
            {
                "name": table["name"],
                "hidden": bool(table.get("hidden")),
                "description": table.get("description"),
                "storage_mode": table.get("storage_mode"),
                "connector": table.get("connector"),
                "sources": table.get("source_objects") or [],
                "rows": table.get("rows"),
                "columns": [
                    {
                        "name": c["name"],
                        "data_type": c.get("data_type"),
                        "kind": c.get("kind"),
                        "hidden": bool(c.get("hidden")),
                        "expression": c.get("expression"),
                        "description": c.get("description"),
                        "unused": ref(table["name"], c["name"]) in unused,
                    }
                    for c in table["columns"]
                ],
                "measures": [
                    {
                        "name": m["name"],
                        "expression": m.get("expression"),
                        "display_folder": m.get("display_folder"),
                        "format": m.get("format"),
                        "hidden": bool(m.get("hidden")),
                        "description": m.get("description"),
                        "unused": ref(table["name"], m["name"]) in unused,
                    }
                    for m in table["measures"]
                ],
            }
        )

    return {
        "key": identity["key"],
        "name": name,
        "source": identity,
        "documented": time.strftime("%Y-%m-%d %H:%M"),
        "generator": f"PBIxtractor {__version__}",
        "links": links,
        "reports": [{"name": report, "pages": report_pages} for report, report_pages in reports.items()],
        "tables": tables,
        "usage": usage,
        "depends_on": depends_on,
        # Reports on the model that could not be documented: "unused" may be incomplete
        "not_included": doc.get("not_included", []),
    }


# ============================================================================
# The catalog folder
# ============================================================================


def add_to_catalog(
    catalog_dir: Path,
    doc: dict,
    report_path: Path,
    model_path: Path,
    name: str,
    files: dict[str, Path],
) -> Path:
    """
    Add (or replace) a model's entry and rebuild the catalog page.

    Args:
        catalog_dir: The catalog folder (created if needed)
        doc: The run's JSON documentation
        report_path: The (first) documented report - its *_source.json tells the origin
        model_path: The model that was read
        name: Model name for the catalog
        files: The run's output files; the lineage viewer and workbook are copied

    Returns:
        Path of catalog.html
    """
    catalog_dir = Path(catalog_dir)
    identity = source_identity(report_path, model_path, name)
    model_folder = _inside(catalog_dir, "models", identity["key"])
    if model_folder.exists():
        shutil.rmtree(model_folder)
    model_folder.mkdir(parents=True)
    links = {}
    for kind in ("lineage", "workbook"):
        if files.get(kind) and Path(files[kind]).is_file():
            shutil.copy2(files[kind], model_folder / Path(files[kind]).name)
            links[kind] = f"models/{identity['key']}/{Path(files[kind]).name}"

    entry = build_entry(doc, identity, name, links)
    entry_file = _inside(catalog_dir, "entries", f"{identity['key']}.json")
    entry_file.parent.mkdir(parents=True, exist_ok=True)
    entry_file.write_text(
        json.dumps(entry, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    return rebuild_catalog(catalog_dir)


def list_entries(catalog_dir: Path) -> list[dict]:
    """All entries, sorted by model name."""
    entries = [
        entry
        for path in sorted((Path(catalog_dir) / "entries").glob("*.json"))
        if (entry := _read_json(path)) is not None
    ]
    return sorted(entries, key=lambda e: e["name"].lower())


def remove_from_catalog(catalog_dir: Path, key: str) -> bool:
    """Remove an entry and its copied files; True if it existed."""
    catalog_dir = Path(catalog_dir)
    entry = _inside(catalog_dir, "entries", f"{key}.json")
    if not entry.is_file():
        return False
    entry.unlink()
    shutil.rmtree(_inside(catalog_dir, "models", key), ignore_errors=True)
    rebuild_catalog(catalog_dir)
    return True


def rebuild_catalog(catalog_dir: Path) -> Path:
    """Write catalog.json and catalog.html from the entries."""
    catalog_dir = Path(catalog_dir)
    catalog_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "generator": f"PBIxtractor {__version__}",
        "updated": time.strftime("%Y-%m-%d %H:%M"),
        "entries": list_entries(catalog_dir),
    }
    (catalog_dir / CATALOG_JSON).write_text(
        json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    # No "<" inside the data block (see lineage_html.render_lineage_html): still valid JSON
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    html = _TEMPLATE.replace("__TITLE__", escape("PBIxtractor catalog")).replace("__DATA__", payload)
    (catalog_dir / CATALOG_HTML).write_text(html, encoding="utf-8")
    return catalog_dir / CATALOG_HTML


# ============================================================================
# The search page
# ============================================================================

_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
:root {
  --bg: #f6f7f9; --panel: #ffffff; --text: #1d2330; --muted: #5f6b7a; --border: #d9dee5;
  --accent: #2563eb; --shadow: 0 1px 2px rgba(0,0,0,.06);
  --model: #be185d; --report: #7c3aed; --page: #475569; --source: #6d28d9; --table: #0f766e;
  --column: #0891b2; --measure: #d97706; --unused: #dc2626;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #11151c; --panel: #181e27; --text: #e6e9ee; --muted: #9aa5b4; --border: #2b3442;
    --accent: #60a5fa; --shadow: none;
    --model: #f472b6; --report: #a78bfa; --page: #94a3b8; --source: #c4b5fd; --table: #2dd4bf;
    --column: #22d3ee; --measure: #fbbf24; --unused: #f87171;
  }
}
* { box-sizing: border-box; }
html, body { margin: 0; height: 100%; background: var(--bg); color: var(--text);
  font: 13px/1.45 "Segoe UI", system-ui, -apple-system, sans-serif; }
#app { display: grid; grid-template-columns: 400px 1fr; height: 100vh; }
aside { background: var(--panel); border-right: 1px solid var(--border); display: flex;
  flex-direction: column; min-height: 0; }
header { padding: 14px 16px 10px; border-bottom: 1px solid var(--border); }
h1 { font-size: 15px; margin: 0 0 2px; }
h2 { font-size: 17px; margin: 0 0 4px; word-break: break-word; }
h3 { font-size: 12px; margin: 18px 0 6px; text-transform: uppercase; letter-spacing: .04em; color: var(--muted); }
.muted { color: var(--muted); }
.section { padding: 10px 16px; border-bottom: 1px solid var(--border); }
input[type=search], select { width: 100%; padding: 7px 9px; border: 1px solid var(--border);
  border-radius: 6px; background: var(--bg); color: var(--text); font: inherit; }
.chips { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
.chip { display: inline-flex; align-items: center; gap: 5px; padding: 3px 8px; border: 1px solid var(--border);
  border-radius: 999px; cursor: pointer; user-select: none; }
.chip input { margin: 0; }
.row2 { display: flex; gap: 8px; margin-top: 8px; align-items: center; flex-wrap: wrap; }
.row2 select { width: auto; flex: 1; }
.dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; flex: none; }
#results { overflow: auto; flex: 1; padding: 4px 0; }
.result { padding: 6px 16px; cursor: pointer; display: flex; gap: 8px; align-items: baseline; }
.result:hover, .result.active { background: var(--bg); }
.result .name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; }
.result .where { color: var(--muted); font-size: 11px; white-space: nowrap; max-width: 45%;
  overflow: hidden; text-overflow: ellipsis; }
.result .hit { color: var(--muted); font-size: 11px; }
main { overflow: auto; padding: 20px 28px 40px; min-width: 0; }
.badge { display: inline-block; padding: 1px 7px; border-radius: 999px; font-size: 11px; color: #fff;
  margin-right: 6px; vertical-align: 2px; }
.warn { display: inline-block; padding: 1px 7px; border-radius: 999px; font-size: 11px;
  border: 1px solid var(--unused); color: var(--unused); margin-left: 6px; }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 12px; }
.card { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px;
  cursor: pointer; box-shadow: var(--shadow); }
.card:hover { border-color: var(--accent); }
.card .muted { overflow-wrap: anywhere; }
.stats { display: flex; flex-wrap: wrap; gap: 14px; margin: 8px 0 4px; }
.stat b { font-size: 18px; display: block; }
dl.kv { display: grid; grid-template-columns: max-content 1fr; gap: 4px 14px; margin: 10px 0; }
dl.kv dt { color: var(--muted); }
dl.kv dd { margin: 0; word-break: break-word; }
pre { margin: 4px 0; padding: 10px; background: var(--panel); border: 1px solid var(--border);
  border-radius: 6px; white-space: pre-wrap; word-break: break-word; font: 12px/1.45 Consolas, monospace; }
.links a, a.item { color: var(--accent); cursor: pointer; text-decoration: none; }
.links a:hover, a.item:hover { text-decoration: underline; }
ul.list { list-style: none; padding: 0; margin: 0; columns: 2 320px; }
ul.list li { padding: 2px 0; break-inside: avoid; }
.btn { display: inline-block; padding: 5px 10px; border: 1px solid var(--border); border-radius: 6px;
  background: var(--panel); color: var(--text); text-decoration: none; margin: 4px 6px 0 0; }
.btn:hover { border-color: var(--accent); }
@media (max-width: 800px) { #app { grid-template-columns: 1fr; grid-template-rows: 50vh 50vh; }
  aside { border-right: 0; border-bottom: 1px solid var(--border); overflow: hidden; }
  main { padding: 16px; } }
</style>
</head>
<body>
<div id="app">
  <aside>
    <header>
      <h1><a class="item" id="home">PBIxtractor catalog</a></h1>
      <div class="muted" id="summary"></div>
    </header>
    <div class="section">
      <input type="search" id="search" placeholder="Search names, DAX, sources, pages…" aria-label="Search" autofocus>
      <div class="chips" id="types"></div>
      <div class="row2">
        <select id="model" aria-label="Model"><option value="">All models</option></select>
        <label class="chip"><input type="checkbox" id="unused"> Unused only</label>
        <label class="chip"><input type="checkbox" id="dax" checked> Search DAX</label>
      </div>
    </div>
    <div id="results" role="listbox"></div>
  </aside>
  <main id="main"></main>
</div>
<script type="application/json" id="data">__DATA__</script>
<script>
(function () {
  "use strict";
  var DATA = JSON.parse(document.getElementById("data").textContent);
  var TYPES = ["model", "report", "page", "table", "column", "measure", "source"];
  var LABEL = { model: "Model", report: "Report", page: "Page", table: "Table", column: "Column",
                measure: "Measure", source: "Source" };
  var $ = function (id) { return document.getElementById(id); };
  function el(tag, cls, text) { var n = document.createElement(tag); if (cls) n.className = cls;
    if (text != null) n.textContent = text; return n; }
  function color(t) { return "var(--" + t + ")"; }
  function ref(t, n) { return t + "[" + n + "]"; }

  // ---------- index: one item per searchable thing ----------
  var items = [], byId = {}, entries = {}, sources = {};
  function add(item) { item.search = (item.name + " " + (item.where || "")).toLowerCase();
    items.push(item); byId[item.id] = item; return item; }
  DATA.entries.forEach(function (e) {
    entries[e.key] = e;
    e.pageReport = {}; e.usedBy = {};
    Object.keys(e.depends_on).forEach(function (s) { e.depends_on[s].forEach(function (t) {
      (e.usedBy[t] = e.usedBy[t] || []).push(s); }); });
    add({ id: e.key, type: "model", name: e.name, where: e.source.label, entry: e.key });
    e.reports.forEach(function (r) {
      add({ id: e.key + "|report|" + r.name, type: "report", name: r.name || e.name, where: e.name, entry: e.key, report: r });
      r.pages.forEach(function (p) {
        e.pageReport[p.id] = r.name;
        add({ id: e.key + "|page|" + p.id, type: "page", name: p.name,
              where: (r.name && r.name !== e.name ? r.name + " · " : "") + e.name,
              entry: e.key, page: p, report: r });
      });
    });
    e.tables.forEach(function (t) {
      add({ id: e.key + "|table|" + t.name, type: "table", name: t.name, where: e.name, entry: e.key, table: t });
      t.sources.forEach(function (s) {
        var src = sources[s.toLowerCase()] || add({ id: "source|" + s.toLowerCase(), type: "source", name: s, loads: [] });
        sources[s.toLowerCase()] = src;
        src.loads.push({ entry: e.key, table: t.name });
      });
      t.columns.forEach(function (c) {
        add({ id: e.key + "|column|" + ref(t.name, c.name), type: "column", name: c.name, where: t.name + " · " + e.name,
              entry: e.key, table: t, obj: c, ref: ref(t.name, c.name), unused: c.unused,
              text: ((c.expression || "") + " " + (c.description || "")).toLowerCase() });
      });
      t.measures.forEach(function (m) {
        add({ id: e.key + "|measure|" + ref(t.name, m.name), type: "measure", name: m.name, where: t.name + " · " + e.name,
              entry: e.key, table: t, obj: m, ref: ref(t.name, m.name), unused: m.unused,
              text: ((m.expression || "") + " " + (m.description || "") + " " + (m.display_folder || "")).toLowerCase() });
      });
    });
  });
  Object.keys(sources).forEach(function (k) { var s = sources[k];
    var models = {}; s.loads.forEach(function (l) { models[l.entry] = true; });
    s.where = Object.keys(models).length + " model" + (Object.keys(models).length === 1 ? "" : "s");
    s.search = (s.name + " " + s.where).toLowerCase(); });

  var counts = {}; items.forEach(function (i) { counts[i.type] = (counts[i.type] || 0) + 1; });
  $("summary").textContent = TYPES.filter(function (t) { return counts[t]; }).map(function (t) {
    return counts[t] + " " + LABEL[t].toLowerCase() + (counts[t] === 1 ? "" : "s"); }).join(" · ") +
    " · updated " + DATA.updated;

  // ---------- filters and results ----------
  var state = { types: {}, query: "", model: "", unused: false, dax: true, selected: null };
  TYPES.forEach(function (t) {
    state.types[t] = true;
    var chip = el("label", "chip"), box = el("input"); box.type = "checkbox"; box.checked = true;
    box.addEventListener("change", function () { state.types[t] = box.checked; renderResults(); });
    var dot = el("span", "dot"); dot.style.background = color(t);
    chip.appendChild(box); chip.appendChild(dot); chip.appendChild(document.createTextNode(LABEL[t]));
    $("types").appendChild(chip);
  });
  DATA.entries.forEach(function (e) { var o = el("option", null, e.name); o.value = e.key; $("model").appendChild(o); });
  $("search").addEventListener("input", function (ev) { state.query = ev.target.value.trim().toLowerCase(); renderResults(); });
  $("model").addEventListener("change", function (ev) { state.model = ev.target.value; renderResults(); });
  $("unused").addEventListener("change", function (ev) { state.unused = ev.target.checked; renderResults(); });
  $("dax").addEventListener("change", function (ev) { state.dax = ev.target.checked; renderResults(); });
  $("home").addEventListener("click", function () { select(null); });

  function score(item) {
    var q = state.query; if (!q) return 1;
    var name = item.name.toLowerCase();
    if (name === q) return 5;
    if (name.indexOf(q) === 0) return 4;
    if (name.indexOf(q) >= 0) return 3;
    if (item.search.indexOf(q) >= 0) return 2;
    if (state.dax && item.text && item.text.indexOf(q) >= 0) return 1.5;
    return 0;
  }
  function renderResults() {
    var list = $("results"); list.textContent = "";
    var hits = [];
    items.forEach(function (item) {
      if (!state.types[item.type]) return;
      if (state.unused && !item.unused) return;
      if (state.model && item.entry !== state.model &&
          !(item.type === "source" && item.loads.some(function (l) { return l.entry === state.model; }))) return;
      var s = score(item); if (s > 0) hits.push({ item: item, score: s });
    });
    hits.sort(function (a, b) { return b.score - a.score || TYPES.indexOf(a.item.type) - TYPES.indexOf(b.item.type) ||
      a.item.name.localeCompare(b.item.name); });
    hits.slice(0, 400).forEach(function (h) {
      var item = h.item, row = el("div", "result" + (state.selected === item.id ? " active" : ""));
      var dot = el("span", "dot"); dot.style.background = color(item.type); row.appendChild(dot);
      row.appendChild(el("span", "name", item.name));
      if (h.score === 1.5) row.appendChild(el("span", "hit", "in DAX"));
      if (item.unused) row.appendChild(el("span", "warn", "unused"));
      if (item.where) row.appendChild(el("span", "where", item.where));
      row.title = LABEL[item.type] + ": " + item.name + (item.where ? "\n" + item.where : "");
      row.addEventListener("click", function () { select(item.id); });
      list.appendChild(row);
    });
    if (hits.length > 400) list.appendChild(el("div", "result muted", (hits.length - 400) + " more - refine the search"));
    if (!hits.length) list.appendChild(el("div", "result muted", "No matches"));
  }

  // ---------- details ----------
  function link(id, text) { var a = el("a", "item", text); a.addEventListener("click", function () { select(id); }); return a; }
  function heading(type, name, sub) {
    var main = $("main"), h = el("h2"), b = el("span", "badge", LABEL[type]);
    b.style.background = color(type); h.appendChild(b); h.appendChild(document.createTextNode(name));
    main.appendChild(h); if (sub) main.appendChild(el("div", "muted", sub));
  }
  function kv(pairs) {
    var dl = el("dl", "kv");
    pairs.forEach(function (p) { if (p[1] == null || p[1] === "" || p[1] === false) return;
      dl.appendChild(el("dt", null, p[0]));
      var dd = el("dd"); if (p[1] instanceof Node) dd.appendChild(p[1]); else dd.textContent = String(p[1]);
      dl.appendChild(dd); });
    $("main").appendChild(dl);
  }
  function section(title, ids, empty) {
    var main = $("main"); main.appendChild(el("h3", null, title + " (" + ids.length + ")"));
    if (!ids.length) { main.appendChild(el("div", "muted", empty || "-")); return; }
    var ul = el("ul", "list");
    ids.slice(0, 500).forEach(function (x) { var li = el("li"), item = byId[x.id];
      li.appendChild(item ? link(x.id, x.text) : el("span", null, x.text));
      if (item && item.unused) li.appendChild(el("span", "warn", "unused"));
      ul.appendChild(li); });
    main.appendChild(ul);
  }
  // Only relative links into this catalog's models/ folder (an edited entry must not be able
  // to smuggle in a javascript: or external link)
  function safeLink(link) { return typeof link === "string" && /^models\/[^:]*$/.test(link) ? link : null; }
  function fullDocs(e, node) {
    var main = $("main"), box = el("div");
    var lineage = safeLink(e.links.lineage), workbook = safeLink(e.links.workbook);
    if (lineage) { var a = el("a", "btn", "Open in the lineage viewer ↗");
      a.href = lineage + (node ? "#" + encodeURIComponent(node) : ""); a.target = "_blank"; box.appendChild(a); }
    if (workbook) { var w = el("a", "btn", "Workbook ⭳"); w.href = workbook; box.appendChild(w); }
    main.appendChild(box);
  }
  function pageLink(e, pageId) {
    var p = byId[e.key + "|page|" + pageId];
    var report = e.pageReport[pageId];
    var text = p ? (report ? report + " › " + p.name : p.name) : pageId;
    return { id: e.key + "|page|" + pageId, text: text };
  }
  function fieldLink(e, r) {
    var kind = byId[e.key + "|measure|" + r] ? "measure" : "column";
    return { id: e.key + "|" + kind + "|" + r, text: r };
  }

  function showHome() {
    var main = $("main");
    main.appendChild(el("h2", null, "Catalog"));
    main.appendChild(el("div", "muted", DATA.entries.length + " documented semantic model" +
      (DATA.entries.length === 1 ? "" : "s") + " · search on the left, or pick a model"));
    var cards = el("div", "cards"); cards.style.marginTop = "14px";
    DATA.entries.forEach(function (e) {
      var card = el("div", "card"), measures = 0, unused = 0, pages = 0;
      e.tables.forEach(function (t) { measures += t.measures.length;
        t.columns.concat(t.measures).forEach(function (o) { if (o.unused) unused++; }); });
      e.reports.forEach(function (r) { pages += r.pages.length; });
      var h = el("div"); var b = el("span", "badge", "Model"); b.style.background = color("model");
      h.appendChild(b); h.appendChild(el("b", null, e.name)); card.appendChild(h);
      card.appendChild(el("div", "muted", e.source.label));
      card.appendChild(el("div", null, e.reports.length + " report" + (e.reports.length === 1 ? "" : "s") + " · " + pages +
        " pages · " + e.tables.length + " tables · " + measures + " measures"));
      if (unused) card.appendChild(el("span", "warn", unused + " unused"));
      card.appendChild(el("div", "muted", "documented " + e.documented));
      card.addEventListener("click", function () { select(e.key); });
      cards.appendChild(card);
    });
    main.appendChild(cards);
    if (!DATA.entries.length) main.appendChild(el("p", "muted", "Empty - add a model with the 'Add to catalog' option."));
  }

  function showModel(e) {
    heading("model", e.name, e.source.label);
    var measures = 0, columns = 0, unusedM = 0, unusedC = 0, pages = 0;
    e.tables.forEach(function (t) { measures += t.measures.length; columns += t.columns.length;
      t.measures.forEach(function (m) { if (m.unused) unusedM++; }); t.columns.forEach(function (c) { if (c.unused) unusedC++; }); });
    e.reports.forEach(function (r) { pages += r.pages.length; });
    kv([["Documented", e.documented + " (" + e.generator + ")"], ["Reports", e.reports.length], ["Pages", pages],
        ["Tables", e.tables.length], ["Columns", columns + (unusedC ? " (" + unusedC + " unused)" : "")],
        ["Measures", measures + (unusedM ? " (" + unusedM + " unused)" : "")]]);
    if ((e.not_included || []).length) {
      $("main").appendChild(el("span", "warn", "Not included - 'unused' may be incomplete:"));
      var ul = el("ul", "list");
      e.not_included.forEach(function (text) { ul.appendChild(el("li", null, text)); });
      $("main").appendChild(ul);
    }
    fullDocs(e);
    section("Reports", e.reports.map(function (r) { return { id: e.key + "|report|" + r.name, text: (r.name || e.name) + " - " + r.pages.length + " pages" }; }));
    section("Tables", e.tables.map(function (t) { return { id: e.key + "|table|" + t.name,
      text: t.name + (t.rows != null ? " - " + t.rows.toLocaleString() + " rows" : "") }; }));
  }
  function showReport(item) {
    var e = entries[item.entry], r = item.report;
    heading("report", r.name || e.name, "on model " + e.name);
    fullDocs(e);
    section("Pages", r.pages.map(function (p) { return { id: e.key + "|page|" + p.id,
      text: p.name + " - " + p.visuals + " visuals" + (p.hidden ? " (hidden)" : "") }; }));
  }
  function showPage(item) {
    var e = entries[item.entry], p = item.page;
    heading("page", p.name, (item.report.name ? item.report.name + " · " : "") + e.name);
    kv([["Visuals", p.visuals], ["Hidden", p.hidden ? "yes" : null]]);
    fullDocs(e, "page:" + p.id);
    var used = Object.keys(e.usage).filter(function (r) { return e.usage[r].indexOf(p.id) >= 0; }).sort();
    section("Fields used on this page", used.map(function (r) { return fieldLink(e, r); }));
  }
  function showTable(item) {
    var e = entries[item.entry], t = item.table;
    heading("table", t.name, e.name);
    kv([["Storage mode", t.storage_mode], ["Connector", t.connector], ["Rows", t.rows != null ? t.rows.toLocaleString() : null],
        ["Hidden", t.hidden ? "yes" : null], ["Description", t.description]]);
    fullDocs(e, "table:" + t.name);
    section("Loads from", t.sources.map(function (s) { return { id: "source|" + s.toLowerCase(), text: s }; }));
    var pages = {};
    t.columns.concat(t.measures).forEach(function (o) { (e.usage[ref(t.name, o.name)] || []).forEach(function (p) { pages[p] = true; }); });
    section("Used on pages", Object.keys(pages).sort().map(function (p) { return pageLink(e, p); }), "Not used by any page");
    section("Measures", t.measures.map(function (m) { return { id: e.key + "|measure|" + ref(t.name, m.name), text: m.name }; }));
    section("Columns", t.columns.map(function (c) { return { id: e.key + "|column|" + ref(t.name, c.name),
      text: c.name + (c.data_type ? " (" + c.data_type + ")" : "") }; }));
  }
  function showField(item) {
    var e = entries[item.entry], o = item.obj, measure = item.type === "measure";
    heading(item.type, o.name, item.table.name + " · " + e.name);
    if (item.unused) $("main").appendChild(el("span", "warn", "Unused: no visual, filter or DAX in the documented report(s) uses it"));
    kv([["Table", link(e.key + "|table|" + item.table.name, item.table.name)], ["Data type", o.data_type],
        ["Kind", o.kind], ["Display folder", o.display_folder], ["Format", o.format],
        ["Hidden", o.hidden ? "yes" : null], ["Description", o.description]]);
    if (o.expression) { $("main").appendChild(el("h3", null, measure ? "DAX" : "Expression"));
      $("main").appendChild(el("pre", null, o.expression)); }
    fullDocs(e, item.type + ":" + item.ref);
    section("Used on pages", (e.usage[item.ref] || []).map(function (p) { return pageLink(e, p); }), "Not used directly on any page");
    section("Depends on", (e.depends_on[item.ref] || []).map(function (r) { return fieldLink(e, r); }));
    section("Used by (DAX)", (e.usedBy[item.ref] || []).map(function (r) { return fieldLink(e, r); }));
  }
  function showSource(item) {
    heading("source", item.name, item.where);
    section("Loaded by", item.loads.map(function (l) { return { id: l.entry + "|table|" + l.table,
      text: l.table + " (" + entries[l.entry].name + ")" }; }));
  }

  function select(id) {
    state.selected = id;
    var main = $("main"); main.textContent = ""; main.scrollTop = 0;
    var item = id ? byId[id] : null;
    if (!item) showHome();
    else if (item.type === "model") showModel(entries[item.id]);
    else if (item.type === "report") showReport(item);
    else if (item.type === "page") showPage(item);
    else if (item.type === "table") showTable(item);
    else if (item.type === "source") showSource(item);
    else showField(item);
    renderResults();
    try { history.replaceState(null, "", id ? "#" + encodeURIComponent(id) : location.pathname + location.search); } catch (e) {}
  }

  var start = decodeURIComponent(location.hash.slice(1));
  select(start && byId[start] ? start : null);
})();
</script>
</body>
</html>
"""
