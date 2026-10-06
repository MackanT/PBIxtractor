"""Catalog: a folder that collects the documentation of many models, searchable in one page.

    add_to_catalog("C:/Catalog", report_json, report_path, model_path, files)
    # C:/Catalog/catalog.html       - search across every model, report, page, visual, table,
    #                                   column, measure and source; offline, one file
    # C:/Catalog/lineage.html       - the lineage viewer across all the models (models first)
    # C:/Catalog/catalog.json       - the same data for other tools
    # C:/Catalog/entries/<key>.json - one slim entry per semantic model (latest run only)
    # C:/Catalog/models/<key>/      - that model's lineage viewer and workbook (full details),
    #                                 and its lineage data (lineage.json, for lineage.html)

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
from .compare import compare_entries
from .design import PAGE_THEME_SCRIPT, page_css
from .lineage_html import _visual_labels, build_viewer_data, render_multi_lineage_html

SEPARATOR = " › "  # report_extractor.PREFIX_SEPARATOR: "<report> › <page>" in model mode
CATALOG_HTML = "catalog.html"
CATALOG_JSON = "catalog.json"
LINEAGE_HTML = "lineage.html"  # the lineage viewer across all models
LINEAGE_DATA = "lineage.json"  # per model: its build_viewer_data()


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
    visual_labels = _visual_labels(doc)

    def use(ref: str, page: str) -> None:
        pages_of = usage.setdefault(ref, [])
        if page not in pages_of:
            pages_of.append(page)

    # JSON schema 2 names each page's report; older documentation only has "<report> › <page>"
    for listed in doc.get("reports", []):
        reports.setdefault(listed["name"], [])
    for page in pages:
        if "report" in page:
            report, page_name = page["report"], page.get("title") or page["name"]
        else:
            report, page_name = _split_page(page["name"], single)
        visuals = [i for i in page["items"] if i["type"] in ("Visual", "Slicer")]
        reports.setdefault(report, []).append(
            {
                "id": page["name"],
                "name": page_name,
                "hidden": bool(page.get("hidden")),
                "visuals": len(visuals),
                # What each visual shows: searchable across reports ("Card: Total Sales")
                "items": [
                    {
                        "id": v["id"],
                        "type": v["visual_type"],
                        "label": visual_labels.get(f"visual:{page['name']}/{v['id']}", v["visual_type"]),
                        "title": v.get("title") or "",
                        "fields": list(dict.fromkeys(f"{f['table']}[{f['name']}]" for f in v.get("fields", []))),
                    }
                    for v in visuals
                ],
            }
        )
        for item in page["items"]:
            for field in item.get("fields", []):
                use(f"{field['table']}[{field['name']}]", page["name"])
            for visual_filter in item.get("filters", []):
                use(visual_filter["field"], page["name"])
            if item["type"] == "Filter" and item.get("field"):
                use(item["field"], page["name"])
    several = len(reports) > 1
    for report_filter in doc["report"].get("filters", []):
        if report_filter.get("field") and not report_filter.get("page"):
            owner = report_filter.get("report")
            use(report_filter["field"], f"(report filter, {owner})" if several and owner else "(report filter)")

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
        # Row-level security roles: the catalog card shows the model is protected
        "rls": [role["name"] for role in doc["model"].get("roles", [])],
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
    rebuild: bool = True,
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
        rebuild: Rebuild the pages now (a batch rebuilds once, after its last model)

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
    # The lineage viewer across models is built from every model's viewer data
    (model_folder / LINEAGE_DATA).write_text(
        json.dumps(build_viewer_data(doc), ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )

    entry = build_entry(doc, identity, name, links)
    entry_file = _inside(catalog_dir, "entries", f"{identity['key']}.json")
    entry_file.parent.mkdir(parents=True, exist_ok=True)
    entry_file.write_text(
        json.dumps(entry, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    return rebuild_catalog(catalog_dir) if rebuild else Path(catalog_dir) / CATALOG_HTML


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


def rebuild_lineage(catalog_dir: Path, entries: list[dict]) -> Optional[Path]:
    """
    Write lineage.html, the lineage viewer across the models (those added with their lineage
    data - models added by an older version need adding again). None when there is none.
    """
    models = []
    for entry in entries:
        try:
            data = _read_json(_inside(catalog_dir, "models", entry["key"]) / LINEAGE_DATA)
        except (ValueError, KeyError):  # an entry edited by hand: left out
            continue
        if data is not None:
            models.append((entry, data))
    path = Path(catalog_dir) / LINEAGE_HTML
    if not models:
        path.unlink(missing_ok=True)
        return None
    path.write_text(render_multi_lineage_html(models), encoding="utf-8")
    return path


def rebuild_catalog(catalog_dir: Path) -> Path:
    """Write catalog.json, catalog.html and lineage.html from the entries."""
    catalog_dir = Path(catalog_dir)
    catalog_dir.mkdir(parents=True, exist_ok=True)
    entries = list_entries(catalog_dir)
    lineage = rebuild_lineage(catalog_dir, entries)
    data = {
        "generator": f"PBIxtractor {__version__}",
        "updated": time.strftime("%Y-%m-%d %H:%M"),
        "lineage": LINEAGE_HTML if lineage else None,  # the catalog page links to it
        "entries": entries,
        # The same measure / column / table / visual in several models: the same or not?
        "compare": compare_entries(entries),
    }
    (catalog_dir / CATALOG_JSON).write_text(
        json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    # No "<" inside the data block (see lineage_html.render_lineage_html): still valid JSON
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    html = (
        _TEMPLATE.replace("__TITLE__", escape("PBIxtractor catalog"))
        .replace("__THEME_SCRIPT__", PAGE_THEME_SCRIPT)
        .replace("__THEME__", page_css())
        .replace("__DATA__", payload)  # last: the data must not be searched for placeholders
    )
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
__THEME_SCRIPT__
<style>
__THEME__
* { box-sizing: border-box; }
html, body { margin: 0; height: 100%; background: var(--bg); color: var(--text);
  font-size: 13.5px; line-height: 1.45; }
#app { display: grid; grid-template-columns: 400px 1fr; height: 100vh; }
aside { background: var(--panel); border-right: 1px solid var(--border); display: flex;
  flex-direction: column; min-height: 0; }
header { padding: 14px 16px 10px; border-bottom: 1px solid var(--border); }
h1 { font-size: 17px; margin: 0 0 2px; }
h2 { font-size: 17px; margin: 0 0 4px; word-break: break-word; }
h3 { font-size: 12px; margin: 18px 0 6px; text-transform: uppercase; letter-spacing: .04em; color: var(--muted); }
.muted { color: var(--muted); }
.section { padding: 10px 16px; border-bottom: 1px solid var(--border); }
input[type=search], select { width: 100%; padding: 7px 9px; border: 1px solid var(--border);
  border-radius: 8px; background: var(--bg); color: var(--text); font: inherit; }
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
.badge { display: inline-block; padding: 1px 7px; border-radius: 999px; font-size: 11px; color: var(--on-color);
  margin-right: 6px; vertical-align: 2px; }
.warn { display: inline-block; padding: 1px 7px; border-radius: 999px; font-size: 11px;
  border: 1px solid var(--unused); color: var(--unused); margin-left: 6px; }
/* Not the same in every model: copper (attention) */
.result .diff, li .diff { color: var(--attn); font-size: 11px; font-weight: 700; margin-left: 6px; white-space: nowrap; }
div.notice, div.same { margin: 10px 0; padding: 7px 10px; border-radius: 8px; background: var(--panel); }
div.notice { border-left: 3px solid var(--attn); }
div.same { border-left: 3px solid var(--border); color: var(--muted); }
dl.kv dt.differs { color: var(--attn); font-weight: 700; }
/* Row-level security: copper (attention), not the red of "unused" */
span.rls { display: inline-block; padding: 1px 7px; border-radius: 999px; font-size: 11px;
  border: 1px solid var(--attn); color: var(--attn); margin-left: 6px; }
div.rls { margin: 8px 0; padding: 6px 9px; border-radius: 8px; border-left: 3px solid var(--attn);
  background: var(--bg); }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 12px; }
.card { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 12px 14px;
  cursor: pointer; box-shadow: var(--shadow); }
.card:hover { border-color: var(--accent); }
.card .muted { overflow-wrap: anywhere; }
.stats { display: flex; flex-wrap: wrap; gap: 14px; margin: 8px 0 4px; }
.stat b { font-size: 18px; display: block; }
dl.kv { display: grid; grid-template-columns: max-content 1fr; gap: 4px 14px; margin: 10px 0; }
dl.kv dt { color: var(--muted); }
dl.kv dd { margin: 0; word-break: break-word; }
pre { margin: 4px 0; padding: 10px; background: var(--panel); border: 1px solid var(--border);
  border-radius: 8px; white-space: pre-wrap; word-break: break-word; font: 12px/1.45 Consolas, monospace; }
.links a, a.item { color: var(--accent); cursor: pointer; text-decoration: none; }
.links a:hover, a.item:hover { text-decoration: underline; }
ul.list { list-style: none; padding: 0; margin: 0; columns: 2 320px; }
ul.list li { padding: 2px 0; break-inside: avoid; }
.btn { display: inline-block; padding: 5px 10px; border: 1px solid var(--border); border-radius: 8px;
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
      <div id="crossLinks"></div>
    </header>
    <div class="section">
      <input type="search" id="search" placeholder="Search names, DAX, sources, pages, visuals…" aria-label="Search" autofocus>
      <div class="chips" id="types"></div>
      <div class="row2">
        <select id="model" aria-label="Model"><option value="">All models</option></select>
        <label class="chip"><input type="checkbox" id="unused"> Unused only</label>
        <label class="chip" title="Items found in several models that are not the same in all of them">
          <input type="checkbox" id="diffs"> Differences only</label>
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
  var TYPES = ["model", "report", "page", "visual", "table", "column", "measure", "source"];
  var LABEL = { model: "Model", report: "Report", page: "Page", visual: "Visual", table: "Table",
                column: "Column", measure: "Measure", source: "Source" };
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
    e.pageReport = {}; e.usedBy = {}; e.visualsOf = {};
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
        // What each visual shows (catalogs from before visuals were listed have none)
        (p.items || []).forEach(function (v) {
          var visual = add({ id: e.key + "|visual|" + p.id + "/" + v.id, type: "visual", name: v.label,
                             where: p.name + " · " + (r.name && r.name !== e.name ? r.name + " · " : "") + e.name,
                             entry: e.key, page: p, report: r, visual: v, text: v.fields.join(" ").toLowerCase() });
          v.fields.forEach(function (f) { (e.visualsOf[f] = e.visualsOf[f] || []).push(visual.id); });
        });
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

  // ---------- the same item in several models (compare.compare_entries) ----------
  // A group: the same measure / column / table (by name) or visual (type + title) in several
  // models (visuals: reports); members with the same "version" are the same
  var KIND_ID = { measure: "|measure|", column: "|column|", table: "|table|", visual: "|visual|" };
  var groupOf = {}, compareGroups = [];
  Object.keys(DATA.compare || {}).forEach(function (kind) {
    (DATA.compare[kind] || []).forEach(function (g, index) {
      g.kind = kind; g.cid = "compare|" + kind + "|" + index;
      compareGroups.push(g);
      g.members.forEach(function (m) { m.item = m.entry + KIND_ID[kind] + m.id; groupOf[m.item] = g; });
    });
  });
  function spread(g) { var s = {}; g.members.forEach(function (m) { s[m.entry + "|" + (m.report || "")] = true; }); return Object.keys(s).length; }
  function places(g, n) { var word = g.kind === "visual" ? "report" : "model"; return n === 1 ? word : word + "s"; }
  // As compared in Python: DAX/expressions without whitespace and case, sources without case
  function norm(name, value) {
    value = value || "";
    if (name === "DAX" || name === "Expression") return value.replace(/\s+/g, "").toLowerCase();
    return name === "Sources" ? value.toLowerCase() : value;
  }
  function apart(a, b) { return Object.keys(a.values).filter(function (n) { return norm(n, a.values[n]) !== norm(n, b.values[n]); }); }

  var counts = {}; items.forEach(function (i) { counts[i.type] = (counts[i.type] || 0) + 1; });
  $("summary").textContent = TYPES.filter(function (t) { return counts[t]; }).map(function (t) {
    return counts[t] + " " + LABEL[t].toLowerCase() + (counts[t] === 1 ? "" : "s"); }).join(" · ") +
    " · updated " + DATA.updated;
  if (DATA.lineage === "lineage.html") {  // only this fixed name: never a link from the data
    var across = el("a", "btn", "Lineage across models ↗");
    var theme = document.documentElement.dataset.theme;
    across.href = "lineage.html" + (theme ? "?theme=" + theme : "");
    across.target = "_blank";
    across.title = "All models in one lineage viewer - the models first, then into each";
    $("crossLinks").appendChild(across);
  }

  // ---------- filters and results ----------
  var state = { types: {}, query: "", model: "", unused: false, diffs: false, dax: true, selected: null };
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
  $("diffs").addEventListener("change", function (ev) { state.diffs = ev.target.checked; renderResults(); });
  $("dax").addEventListener("change", function (ev) { state.dax = ev.target.checked; renderResults(); });
  $("home").addEventListener("click", function () { select(null); });

  function score(item) {
    var q = state.query; if (!q) return 1;
    var name = item.name.toLowerCase();
    if (name === q) return 5;
    if (name.indexOf(q) === 0) return 4;
    if (name.indexOf(q) >= 0) return 3;
    if (item.search.indexOf(q) >= 0) return 2;
    if ((state.dax || item.type === "visual") && item.text && item.text.indexOf(q) >= 0) return 1.5;
    return 0;
  }
  function renderResults() {
    var list = $("results"); list.textContent = "";
    var hits = [];
    items.forEach(function (item) {
      if (!state.types[item.type]) return;
      if (state.unused && !item.unused) return;
      if (state.diffs && !(groupOf[item.id] && groupOf[item.id].differs.length)) return;
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
      if (h.score === 1.5) row.appendChild(el("span", "hit", item.type === "visual" ? "in fields" : "in DAX"));
      if (item.unused) row.appendChild(el("span", "warn", "unused"));
      var g = groupOf[item.id];
      if (g && g.differs.length) row.appendChild(el("span", "diff", "≠ differs"));
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
      li.appendChild(item || String(x.id).indexOf("compare|") === 0 ? link(x.id, x.text) : el("span", null, x.text));
      if (item && item.unused) li.appendChild(el("span", "warn", "unused"));
      if (item && groupOf[x.id] && groupOf[x.id].differs.length) li.appendChild(el("span", "diff", "≠"));
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
      var mode = document.documentElement.dataset.theme;  // keep the catalog's light/dark
      a.href = lineage + (mode ? "?theme=" + mode : "") + (node ? "#" + encodeURIComponent(node) : "");
      a.target = "_blank"; box.appendChild(a); }
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
      if ((e.rls || []).length) card.appendChild(el("span", "rls", "🔒 RLS: " + e.rls.length + (e.rls.length === 1 ? " role" : " roles")));
      card.appendChild(el("div", "muted", "documented " + e.documented));
      card.addEventListener("click", function () { select(e.key); });
      cards.appendChild(card);
    });
    main.appendChild(cards);
    if (!DATA.entries.length) main.appendChild(el("p", "muted", "Empty - add a model with the 'Add to catalog' option."));
    if (DATA.entries.length < 2) return;
    // Where models meet: what differs between them (often a mistake), what they share
    main.appendChild(el("h3", null, "Differences across models"));
    var differing = compareGroups.filter(function (g) { return g.differs.length; });
    main.appendChild(el("div", differing.length ? "notice" : "same", differing.length
      ? "≠ " + ["measure", "column", "table", "visual"].map(function (kind) {
          var n = differing.filter(function (g) { return g.kind === kind; }).length;
          return n ? n + " " + LABEL[kind].toLowerCase() + (n === 1 ? "" : "s") : null;
        }).filter(Boolean).join(" · ") + " with the same name are not the same in every model " +
        "(visuals: same type and title). Tick \"Differences only\" to list them on the left."
      : "= Everything found in several models is the same in all of them."));
    ["measure", "column", "table", "visual"].forEach(function (kind) {
      var groups = differing.filter(function (g) { return g.kind === kind; });
      if (!groups.length) return;
      section(LABEL[kind] + "s that differ", groups.map(function (g) {
        return { id: g.cid, text: g.name + " - " + spread(g) + " " + places(g) + " · " + g.differs.join(", ") +
          (g.differs.length === 1 ? " differs" : " differ") };
      }));
    });
    var sameMeasures = compareGroups.filter(function (g) { return g.kind === "measure" && !g.differs.length; });
    section("Measures that are the same in several models", sameMeasures.map(function (g) {
      return { id: g.cid, text: g.name + " - " + spread(g) + " models" }; }), "None");
    var shared = items.filter(function (i) {
      var models = {}; (i.loads || []).forEach(function (l) { models[l.entry] = true; });
      return i.type === "source" && Object.keys(models).length > 1;
    }).sort(function (a, b) { return a.name.localeCompare(b.name); });
    section("Sources loaded by several models", shared.map(function (i) { return { id: i.id, text: i.name + " - " + i.where }; }),
      "No source is loaded by more than one model");
  }

  // One item across the models: what is the same everywhere, and each version of what is not
  function valueBox(pairs) {
    var dl = el("dl", "kv");
    pairs.forEach(function (p) {
      dl.appendChild(el("dt", p[2] ? "differs" : null, (p[2] ? "≠ " : "") + p[0]));
      var dd = el("dd");
      if (p[0] === "DAX" || p[0] === "Expression") dd.appendChild(el("pre", null, p[1] || "(none)"));
      else dd.textContent = p[1] === "" ? "(none)" : p[1];
      dl.appendChild(dd);
    });
    $("main").appendChild(dl);
  }
  function showCompare(cid) {
    var parts = cid.split("|"), group = ((DATA.compare || {})[parts[1]] || [])[Number(parts[2])];
    if (!group) { showHome(); return; }
    var main = $("main"), n = spread(group);
    heading(group.kind, group.name, "in " + n + " " + places(group, n));
    main.appendChild(el("div", group.differs.length ? "notice" : "same", group.differs.length
      ? "≠ Not the same everywhere: " + group.differs.join(", ") + (group.differs.length === 1 ? " differs" : " differ") +
        " - " + group.versions + " versions, the most common first."
      : "= The same everywhere" + (group.kind === "measure" ? " (DAX formatting ignored)" : "") + "."));
    if (group.note) main.appendChild(el("p", "muted", group.note));
    var first = group.members[0].values;
    var common = Object.keys(first).filter(function (k) { return group.differs.indexOf(k) < 0; });
    if (common.length) {
      main.appendChild(el("h3", null, "The same everywhere"));
      valueBox(common.map(function (k) { return [k, first[k], false]; }));
    }
    var versions = {};
    group.members.forEach(function (m) { (versions[m.version] = versions[m.version] || []).push(m); });
    Object.keys(versions).sort(function (a, b) { return a - b; }).forEach(function (v) {
      var members = versions[v];
      main.appendChild(el("h3", null, (group.versions > 1 ? "Version " + v + " · " : "") + members.length + " " +
        places(group, members.length)));
      var ul = el("ul", "list");
      members.forEach(function (m) {
        var e = entries[m.entry], li = el("li");
        li.appendChild(byId[m.item] ? link(m.item, m.where) : el("span", null, m.where));
        if (group.kind === "measure" || group.kind === "column") {
          var pages = (e.usage[m.id] || []).length, visuals = (e.visualsOf[m.id] || []).length;
          li.appendChild(el("span", "muted", " - " + (pages || visuals
            ? pages + " page" + (pages === 1 ? "" : "s") + ", " + visuals + " visual" + (visuals === 1 ? "" : "s")
            : "not used on any page")));
        }
        if (byId[m.item] && byId[m.item].unused) li.appendChild(el("span", "warn", "unused"));
        ul.appendChild(li);
      });
      main.appendChild(ul);
      if (group.differs.length) valueBox(group.differs.map(function (k) { return [k, members[0].values[k], true]; }));
    });
  }
  // On an item's page: is it the same in the other models?
  function sameNotice(itemId) {
    var g = groupOf[itemId];
    if (!g) return;
    var me = g.members.filter(function (m) { return m.item === itemId; })[0];
    var others = g.members.filter(function (m) { return m.item !== itemId; });
    var differ = others.filter(function (m) { return m.version !== me.version; });
    var box = el("div", differ.length ? "notice" : "same");
    box.appendChild(document.createTextNode(differ.length
      ? "≠ Not the same in " + differ.length + " other " + places(g, differ.length) + ": " + differ.map(function (m) {
          return (g.kind === "visual" ? m.where : m.model) + " (" + apart(me, m).join(", ") + ")"; }).join("; ") + ". "
      : "= The same in " + others.length + " other " + places(g, others.length) + ". "));
    box.appendChild(link(g.cid, "Compare side by side →"));
    $("main").appendChild(box);
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
    if ((e.rls || []).length) {
      $("main").appendChild(el("div", "rls", "🔒 Row-level security: " + e.rls.join(", ") +
        " - viewers only see the rows their role allows. Share with care."));
    }
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
  function showVisual(item) {
    var e = entries[item.entry], v = item.visual;
    heading("visual", v.label, item.where);
    sameNotice(item.id);
    kv([["Type", v.type], ["Title", v.title], ["Page", link(e.key + "|page|" + item.page.id, item.page.name)]]);
    fullDocs(e, "visual:" + item.page.id + "/" + v.id);
    section("Fields", v.fields.map(function (r) { return fieldLink(e, r); }));
  }
  function showPage(item) {
    var e = entries[item.entry], p = item.page;
    heading("page", p.name, (item.report.name ? item.report.name + " · " : "") + e.name);
    kv([["Visuals", p.visuals], ["Hidden", p.hidden ? "yes" : null]]);
    fullDocs(e, "page:" + p.id);
    var used = Object.keys(e.usage).filter(function (r) { return e.usage[r].indexOf(p.id) >= 0; }).sort();
    section("Fields used on this page", used.map(function (r) { return fieldLink(e, r); }));
    if (p.items) section("Visuals", p.items.map(function (v) { return { id: e.key + "|visual|" + p.id + "/" + v.id, text: v.label }; }));
  }
  function showTable(item) {
    var e = entries[item.entry], t = item.table;
    heading("table", t.name, e.name);
    sameNotice(item.id);
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
    sameNotice(item.id);
    kv([["Table", link(e.key + "|table|" + item.table.name, item.table.name)], ["Data type", o.data_type],
        ["Kind", o.kind], ["Display folder", o.display_folder], ["Format", o.format],
        ["Hidden", o.hidden ? "yes" : null], ["Description", o.description]]);
    if (o.expression) { $("main").appendChild(el("h3", null, measure ? "DAX" : "Expression"));
      $("main").appendChild(el("pre", null, o.expression)); }
    fullDocs(e, item.type + ":" + item.ref);
    section("Used on pages", (e.usage[item.ref] || []).map(function (p) { return pageLink(e, p); }), "Not used directly on any page");
    if (e.visualsOf[item.ref]) section("Used by visuals", e.visualsOf[item.ref].map(function (id) {
      return { id: id, text: byId[id].name + " (" + byId[id].page.name + ")" }; }));
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
    if (id && String(id).indexOf("compare|") === 0) showCompare(id);
    else if (!item) showHome();
    else if (item.type === "model") showModel(entries[item.id]);
    else if (item.type === "report") showReport(item);
    else if (item.type === "page") showPage(item);
    else if (item.type === "visual") showVisual(item);
    else if (item.type === "table") showTable(item);
    else if (item.type === "source") showSource(item);
    else showField(item);
    renderResults();
    try { history.replaceState(null, "", id ? "#" + encodeURIComponent(id) : location.pathname + location.search); } catch (e) {}
  }

  var start = decodeURIComponent(location.hash.slice(1));
  select(start && (byId[start] || start.indexOf("compare|") === 0) ? start : null);
})();
</script>
</body>
</html>
"""
