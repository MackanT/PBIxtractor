# PBIxtractor — Claude Context

## Project Overview
PBIxtractor generates documentation for Power BI reports. It reads a `.pbix` (report
layout) plus a `.bim` (semantic model, read directly; Tabular Editor 2 optional) and writes
Excel workbooks
with a visual inventory per page, filters, colour-coded DAX, relationships (PNG graph),
measure dependencies, unused columns/measures, model tables/columns (sources, storage mode),
Best Practice Analyzer results, and row counts/sizes when the report is open in Desktop.

Owner reviews and commits everything — **never run `git commit` / `git push`**.

## Stack
- Python ≥3.11, packaged with `uv` (src-layout, setuptools backend), version in
  `pyproject.toml` **and** `src/pbixtractor/__init__.py` (keep in sync)
- NiceGUI (>=2.24, 3.x installed) web UI on http://localhost:8081 (`web_ui.py`, default);
  DearPyGUI (+ tkinter file dialogs) legacy desktop UI behind `--ui`
- pandas, xlsxwriter (rich-text cells), networkx + matplotlib (relationship PNG)
- jsonpath-ng (`jsonpath_ng.ext`) for layout JSON queries, pydantic for row models
- Optional: Tabular Editor 2 CLI (Windows-only), called via `subprocess`:
  - Tabular Editor analysis (default on; `--no-tabular-editor`, legacy `RUN_TE_ANALYSIS`): Best Practice Analyzer (`-A`), exact DAX
    dependencies (`-S` script using `DependsOn`) and live statistics — all local/offline.
  - TSV export (default off; `--tabular-editor-tsv`, legacy `USE_TABULAR_EDITOR`): old `documentation.tsv` export; runs FormatDax, which
    sends DAX to daxformatter.com.
- `psycopg` is declared but unused (reserved for a future SQL-source feature)

## Running
Working clone: `C:\Users\MarcusToftås\Documents\PBIxtractor` (moved off OneDrive on 2026-09-29;
the old copy under `OneDrive…\Dokument\Other\PBI-Ixtractor\PBIxtractor` is no longer worked in —
OneDrive broke uv hardlinks and git reflog writes).
```powershell
uv run --frozen pbixtractor             # web UI (NiceGUI) on :8081, opens the browser
uv run --frozen pbixtractor web --port 8090 --no-browser
uv run --frozen pbixtractor extract "C:\...\Sales.pbix" [--model X.bim] [-o out] [--no-tabular-editor]
uv run --frozen pbixtractor --ui        # legacy DearPyGUI UI
uv run --frozen pbixtractor --test      # dev run; paths hardcoded in extractor.run_test_extraction()
uv run --frozen --extra dev pytest -q   # tests (`--with pytest` does NOT work here: å in path)

# Optional smoke test against a real report kept outside the repo (never commit client data)
$env:PBIXTRACTOR_SAMPLE_PBIX = "C:\...\Reports V1\Invoices.pbix"
```
Output goes to `./output/<report name>/` (CWD-relative, gitignored) unless `-o` / the UI's output
folder says otherwise. The model is auto-found next to the report (`find_model_for_report`:
`<name>.bim`, `<name>.SemanticModel/model.bim`, `<name>.Dataset/model.bim`); `extract` exits 2
when there is none. By default the model is read
from the `.bim` on every run; a `documentation.tsv` is only used with `USE_TABULAR_EDITOR`.
Real test reports (local only, never commit):
- `…/_Arbete/Rowico/…/Reports V1/` (client data): `Invoices.pbix` + `Invoices.bim` (legacy
  Layout format; only one with a .bim) and 8 other legacy `.pbix` files.
- `C:\Users\MarcusToftås\Downloads\Adventure Works DW 2020.pbix` + `Model.bim` (owner's dummy
  report, **PBIR format**; model has no measures; no buttons/bookmarks/groups yet).
  Full pipeline for it: `pbixtractor extract "...\Adventure Works DW 2020.pbix" --model
  "...\Downloads\Model.bim"` (~12 s with Tabular Editor) — `--test` only knows the Invoices paths.
Tabular Editor 2 is installed at `C:\Program Files (x86)\Tabular Editor\`
(`tabular_editor.find_tabular_editor()`; extra folders in `Input/TabularEditorLocations.txt`).
Power BI Desktop is the Microsoft Store version: workspaces under
`%USERPROFILE%\Microsoft\Power BI Desktop Store App\AnalysisServicesWorkspaces`.

Git: only read-only commands (status/diff/log). **No commit, push or `git stash`** — the owner
reviews and commits everything. Compare against baselines by copying output elsewhere instead.

## Architecture
```
src/pbixtractor/
  cli.py            argparse: no command → web UI; `web [--port] [--no-browser]`;
                    `extract report [--model] [-o] [--name] [--no-tabular-editor]
                    [--tabular-editor-tsv] [--description-tag] [--no-log-file] [-q]`
                    (exit 0 ok/warnings, 1 error, 2 no model); flags --ui (legacy), --test.
  pipeline.py       The orchestration, no globals: run_extraction(ExtractionOptions,
                    progress(step, fraction), on_log(level, msg)) → ExtractionResult(status
                    success|warnings|error, message, files {workbook, data_workbook, json,
                    lineage, graph[, tsv, log]}, logs, documentation, report_json, seconds).
                    Steps: model → TE analysis (BPA, deps, live stats) → dataset →
                    ReportExtractor → build_documentation → graph → workbooks → JSON + lineage.
                    One run at a time (_RUN_LOCK: shared logger/TE/Excel); any captured log
                    record → "warnings" (+ logs/log_data_*.txt). report_name(),
                    find_model_for_report().
  report_extractor.py CONFIG = config.load_config() (sys.exit on failure) → visual_mapper,
                    visual_type_list, known_functions; ReportExtractor (readers.read_report →
                    ReportContext + PageExtractor).
  web_ui.py         NiceGUI app: index() root page (report/model/output inputs, server-side
                    PathPicker dialog, options, TE/Desktop status) → run_extraction in
                    run.io_bound; progress/log via a queue drained by ui.timer. Results:
                    stat cards, download buttons, tabs Lineage (iframe of <name>_lineage.html
                    served by /files/{token}/{name}; token → output folder, 404 outside it),
                    Model quality (BPA per rule), Unused, Bookmarks, Log. start() =
                    register_routes() + ui.run(index, reload=False). Local tool: binds
                    127.0.0.1, no auth. Unmatched URLs get the root page (HTTP 200).
  extractor.py      Legacy: DearPyGUI run_ui() + its globals (SAVE_NAME, _PBIX_=[stem, dir],
                    _BIM_, LOG_DATA, USE_TABULAR_EDITOR, RUN_TE_ANALYSIS); run_cmd() maps them
                    to ExtractionOptions and returns "Success"/"Log"/message; gen_tsv()
                    (tabular_editor.export_documentation_tsv), run_test_extraction().
                    Re-exports CONFIG/ReportExtractor/... for old imports.
  config.py         load_config() → Config(visual_mapper, supported_visual_types (derived:
                    standard_visuals + special_visuals), data_types, extract_types,
                    function_names). extract_type(visual_type): standard | button | skip.
  json_report.py    write_json() → <name>.json: report, model (incl. live stats), dependencies,
                    unused, quality, lineage {nodes, edges} (ids page:/visual:/table:/column:/
                    measure:/source:, edge types contains/uses/filters/depends_on/
                    relationship/loads_from). Every page gets a node (also empty ones).
  lineage_html.py   write_lineage_html(path, documentation_to_dict(...)) → <name>_lineage.html:
                    one self-contained offline page (vanilla JS + SVG, data embedded as JSON,
                    "</" escaped). Edges re-oriented to flow data → report (source → table →
                    column → measure → visual → page); relationships left out. Layer per
                    node (measures by dependency depth); in-browser layered layout with
                    barycenter ordering; select → upstream/downstream; details panel.
  documentation.py  Analysis stage, no file output: build_documentation() → Documentation
                    (report_info, filter_strings, pages: {page: [PageItem]}, model, objects
                    [OBJECT_COLUMNS], relations, unused_columns/measures, exact deps, BPA,
                    live stats, page_info, bookmarks (incl. used_by buttons), interactivity
                    {(page, visual id): notes}; depends_on(table, name)). Needs the
                    ReportDefinition (ReportExtractor.report) for page/bookmark details.
                    Also parse_tsv_object_name,
                    button_target_and_label, field_description.
  dax.py            find_functions/find_measures/find_columns, text_dependencies() (fallback
                    when no exact deps), highlight_dax() → rich-text segments.
  excel_report.py   create_formats(), write_main_workbook(), write_data_workbook(); shared
                    helpers per sheet type (_write_relations, _write_objects, _write_page_sheet,
                    _write_pages_sheet, _write_dependencies_sheet).
  relationship_graph.py save_relationship_graph() → PNG (networkx spring layout).
  semantic_model.py read_model(.bim or folder with model.bim) → SemanticModel (tables, columns,
                    measures, hierarchies+levels, partitions incl. Direct Lake entity, relationships
                    with cardinality/direction/active, shared expressions, roles/RLS).
                    model_to_dataset() → DataFrame identical to TE's TSV (verified on Invoices:
                    all 459 objects match; only DAX whitespace differs). structurally_used_columns():
                    relationship keys, sort-by and hierarchy-level columns. TMDL not supported yet.
  tabular_editor.py find_tabular_editor(); run_script() (write C# script with %FOLDER%, run
                    TE2 -S, collect output files); run_best_practice_analyzer() runs TE's
                    Analyzer in a script → (violations, rule errors) with rules from
                    data/BPARules.json (Microsoft, MIT, see BPARules.LICENSE). Do NOT use the
                    "-A" console output: it is truncated unpredictably (112 of 125 findings).
                    export_dependencies() runs a C# script (DependsOn) →
                    Dependency rows (measures, calc columns/tables, RLS TablePermission);
                    drop_redundant_table_refs() removes 'T' when T[col] is also referenced.
                    export_documentation_tsv() = the old TabularScript.cs TSV export.
  live_model.py     find_local_instances() (psutil: msmdsrv listening port + parent
                    PBIDesktop cmdline = .pbix path); collect_live_statistics() picks the
                    instance for the report (path match, else table-name Jaccard ≥ 0.8) and
                    runs TE2 "localhost:<port>" "" -S script with ExecuteReader on
                    INFO.STORAGETABLES/STORAGETABLECOLUMNS/STORAGETABLECOLUMNSEGMENTS,
                    COLUMNSTATISTICS() + TOM measure DataType; build_statistics() aggregates
                    like VertiPaq Analyzer (dictionary + data + H$ hierarchy per column).
  model_sheets.py   add_model_sheets() (model tables/columns/roles/parameters, optional live
                    stats columns) and add_bpa_sheets() (model quality summary + findings).
  readers.py        read_report(path): .pbix (legacy Report/Layout or PBIR Report/definition/),
                    .pbip, or <name>.Report folder → ReportDefinition → PageDefinition →
                    VisualDefinition (fields as FieldBinding(role, expr, query_ref,
                    display_name), aliases, objects, container_objects, filters, sync_group,
                    hidden). PageDefinition: hidden, page_type (Tooltip/Drillthrough, only
                    from string names), interactions (VisualInteraction source/target/kind).
                    ReportDefinition.bookmark_details: BookmarkDefinition (captures data/
                    display/page, target_visuals, hidden_visuals, group). All format-specific
                    JSON knowledge lives here; zip files are read on demand.
  extractors.py     Helpers: clean_literal, get_aliases/resolve_field (resolve fields via the
                    query's From aliases), iter_field_refs, ReportContext (page + bookmark
                    id → display name). BaseExtractor / VisualExtractor / FilterExtractor
                    (one condition describer for report/page/visual filters) / PageExtractor.
                    Emit ExtractedItem / ExtractedFilter (models.py) → .to_list() rows.
  models.py         ExtractedItem, ExtractedFilter (pydantic row models).
  visual_helpers.py VisualTypeMapper: visual type → (item_type, display name) from YAML.
  data/data.yaml    data_types (projection role → label), function_names (DAX highlight
                    list), visual_type_metadata (standard_visuals, special_visuals,
                    button_types), extract_types (visual type → standard/button/skip).
  constants.py      DEFAULT_COLORS (mutated at runtime by the UI), UI_COLORS, REPORT_COLUMNS.
  logger.py         setup_logger(capture=True) → LogCapture buffer; capture_logs(callback)
                    context manager (per run, used by the pipeline) + CallbackHandler.
  utils/__init__.py write_to_excel (rich strings), is_excel_open_with_file (psutil), etc.
```

### Data flow
1. `ReportExtractor.extract()` → `read_report()` normalises the report, builds
   `ReportContext`, extracts report filters, then per page → `PageExtractor.extract(page, ctx)`.
   Format differences handled in readers.py:
   | | Legacy (`Report/Layout`, UTF-16 LE, nested JSON strings) | PBIR (UTF-8, one file each) |
   |---|---|---|
   | pages | `sections[]` | `definition/pages/<p>/page.json`, order in `pages.json` |
   | fields | `singleVisual.prototypeQuery.Select` (+ `From` aliases), roles via `projections` queryRef, renames in `columnProperties` | `visual.query.queryState.<role>.projections[].field` (Entity refs), `displayName`/`nativeQueryRef` |
   | formatting / actions | `singleVisual.objects` / `vcObjects` | `visual.objects` / `visualContainerObjects` |
   | groups | `config.singleVisualGroup` | `visualGroup` |
   | filters | `filters` (string), field in `expression` | `filterConfig.filters`, field in `field` |
   | bookmarks | `config.bookmarks` (groups via `children`) | `definition/bookmarks/*.bookmark.json` + `bookmarks.json` |
   | report filters | top-level `filters` | `report.json` `filterConfig` |
   | hidden page | section `config.visibility == 1` | page.json `visibility: HiddenInViewMode` |
   | edit interactions | section `config.relationships` (type 1 Filter, 2 Highlight, 3 None) | page.json `visualInteractions` (DataFilter/HighlightFilter/NoFilter) |
   | slicer sync | `singleVisual.syncGroup.groupName` | `visual.syncGroup.groupName` |
   | hidden visual | `singleVisual.display.mode == hidden`, group `singleVisualGroup.isHidden` | visual.json `isHidden` |
   | bookmark capture | `options.suppressData/suppressDisplay/suppressActiveSection`, `targetVisualNames`; hidden visuals in `explorationState.sections.*.visualContainers.*.singleVisual.display` / `visualContainerGroups.*.isHidden` | same structure in `*.bookmark.json` |
   PBIR buttons/groups/bookmarks are so far only tested with the hand-built fixture
   (`tests/sample_pbir.py`), not a real report.
2. Item rows: `[Page, Visual Type, Visual ID, Table, Name, Display Name, Type]`
   (`REPORT_COLUMNS`). Type is the projection role label, `"Hierarchy"`, `"Formatting"`
   (field used only in formatting objects), or for buttons the action type.
   Buttons (and shapes/images with an action): Visual Type `actionButton`, Name = target
   display name (or `(missing bookmark: <id>)` / `(missing page: <id>)` when deleted —
   `documentation.is_missing_target()`; counted as PageInfo.broken_buttons, JSON item
   `"broken": true`, "⚠" in the viewer label), Display Name = button label.
   Groups: Visual Type `Group`, Visual ID = group name, Display Name = group display name.
   Filter rows: `[page, item_name, filter_type, table, field, operator, value]`,
   filter_type `"Visual"`, `"This Page"` or `"All Pages"` (report level; used for
   unused-detection only, not shown in the workbooks yet). Compound conditions have an
   empty operator and the full text in value (e.g. `"> 2019 and < 2030"`).
   Layout structures: button action in `singleVisual.vcObjects.visualLink[].properties`
   (`type`, `bookmark`, `navigationSection`); bookmarks in `config.bookmarks` (groups via
   `children`); fields in `singleVisual.prototypeQuery.Select` with roles in `projections`.
   Hierarchy levels: the layout only has the level name (queryRef is unreliable: sometimes
   column, sometimes level). documentation.resolve_hierarchy_columns() maps level → column
   from the model (level "Year" → column "Year Number").
3. `run_extraction()` always reads the model with `read_model(model_path)`. The measures/columns
   dataset comes from `model_to_dataset(model)`, or from `documentation.tsv` when
   `USE_TABULAR_EDITOR` (columns: Object, Name, Description, SourceColumn, Expression,
   FormatString, DataType, DisplayFolder; Object names `Model.T.<Table>`, `.C.<Col>`, `.M.`,
   `.H.`, `.P.`, `Relationship.<guid>`). Relationships (sheet columns Type, Child, Direction,
   Parent, Child Column, Parent Column, Cardinality, Active) and the table list always come
   from the model. Unused detection first drops `structurally_used_columns()`, then (with
   exact dependencies) everything any DAX references by (table, name); without Tabular Editor
   it falls back to DAX text matching of measures and calculated columns (misses unqualified
   refs, matches text in comments). Measures stay "unused" unless a visual, filter or other DAX
   uses them — listed separately as "Measures not used in this report".
   The TSV path garbles format strings starting with `"` (CSV quoting) — the .bim path doesn't.
4. Writes `<name>.xlsx` (Common + one tab per report page, also empty pages + Pages + model
   sheets) and
   `<name>_data.xlsx` (pages, common, relationships, unused measures [Type column],
   dependencies [MeasureName, Dependent, Object Type, Dependent Type], model tables,
   model columns, model roles/parameters if any, model quality summary, model quality).
   Live stats fill Rows/Size/% columns and measure DataType; otherwise they stay empty.
   Both workbooks also get "report pages", "bookmarks" (if any) and "report filters"
   sheets; the Interactivity column holds hidden state, sync group and changed interactions.
   Workbooks are created with strings_to_formulas=False (conditions like "= Grey" used to
   become broken formulas).
   Common sheets (OBJECT_COLUMNS) list measures and calculated columns (Type "Calculated
   Column" = a column with DAX; data columns are only on "model columns") in model order,
   with "Depends On" (what the object references; was "Dependants" before 2026-09-29).

## Known bugs / gotchas (open as of 2026-09-29)
- Never trust the Select `Name`/queryRef string for table/field — it goes stale when
  measures are renamed or moved (e.g. `_Measures.Total Sales Budget` is really
  `SalesBudgets[Total Sales Budget OC]`). Always use `resolve_field()`. The queryRef is
  only for matching projection roles / columnProperties and hierarchy columns.
- Tooltip/drillthrough page detection is untested on real files (no sample uses them);
  legacy numeric pageBinding types are ignored on purpose. Bookmark captured filter/slicer
  state is not listed yet (only capture options and hidden visuals).
- The "User Input" UI tab appends to `Input/*.csv`, which nothing reads any more (YAML config).
  The measures-table combo (`defMeasTable`) is hidden and its value unused.
- Text-matching dependency fallback order is set-based (not stable across runs). The data
  workbook's "dependencies" sheet headers (MeasureName, Dependent, ...) are unchanged on purpose
  (possible downstream consumers).
- Windows-only: backslash path joins, PowerShell launch of TE2, Excel-open check.
- Editing tip: shell heredocs/sed mangle backslash escapes (\t, \n) in Python/C# code —
  write patch scripts with the file tool instead. Files may have CRLF endings (autocrlf).
- `extractor.py` is not black-formatted and has pre-existing ruff warnings; don't mass-reformat
  it in a feature change (keeps diffs reviewable).

## Direction (agreed with owner, 2026-09-29)
- Must support **both** report formats: classic `.pbix` (`Report/Layout`) and PBIP/PBIR
  (`definition/pages/*/visuals/*/visual.json`), plus PBIR-Legacy `report.json`.
  Both readers produce the same typed report model.
- Must be able to read reports/models **remotely** (Fabric workspace / Azure DevOps git),
  e.g. via Semantic Link (`semantic-link` / `semantic-link-labs`) or the Fabric REST
  `getDefinition` API, not only from local files.
- Source databases are mostly **Azure SQL DB / Fabric (Warehouse, Lakehouse SQL endpoint)**
  → prefer Entra-token auth (`azure-identity`) + `mssql-python`/`pyodbc`; Postgres is secondary.
- Committed tests use one hand-built, anonymised sample report written in three formats
  (`tests/sample_layout.py` legacy, `tests/sample_pbir.py` PBIR .pbix + PBIP folder); every
  extraction test runs on all three and must give identical rows. Real reports are only used
  locally via `PBIXTRACTOR_SAMPLE_PBIX`.
- Plan order: Step 0 (fix regressions, done) → 1 typed model + local readers (done for
  reports: readers.py, and for .bim models: semantic_model.py; TMDL still TODO) →
  model extras (done 2026-09-29: unused measures, model sheets, BPA, exact dependencies,
  live statistics) → 2 modular handlers/writers (run_cmd split into documentation.py /
  excel_report.py / dax.py / relationship_graph.py, config.py + extract_types handler
  registry, JSON writer done) → 3 report details (done 2026-09-29: page visibility/type,
  interactions, sync groups, hidden visuals, bookmarks sheet, report filters sheet) →
  4 HTML lineage viewer (done 2026-09-29; Excel cannot show it inline) → 5 pipeline + CLI +
  NiceGUI web UI with the viewer embedded (done 2026-09-29; DearPyGUI kept behind --ui) →
  1b DevOps/Fabric readers → 6 SQL/Fabric source lineage (sqlglot).

## Checking the HTML viewer
No Node.js here, but headless Edge works (run from PowerShell; bash paths fail):
`msedge.exe --headless=new --disable-gpu --user-data-dir=<tmp> --window-size=1600,900
--virtual-time-budget=5000 --screenshot=<png> file:///<html>` — then view the PNG.
The web UI can be shot the same way at `http://127.0.0.1:<port>/` (start it with
`pbixtractor web --no-browser`); a "Connection lost" toast in the shot is headless virtual time,
not a bug. Clicking is not possible there: result views were checked with a scratch script that
calls `web_ui._render_result()` from a root page. Automated UI tests use NiceGUI's
`user_simulation(web_ui.index)` inside `asyncio.run` (tests/test_web_ui.py; no pytest-asyncio).

## Refactoring safety net
Output is deterministic (except the graph PNG layout; baseline refreshed 2026-09-29 after the
common-sheet fixes). Before a refactor, copy
`output/Invoices_NEW/*.xlsx` and `output/AdventureWorks/*.xlsx` somewhere, re-run, and compare
workbooks cell by cell incl. rich-text runs and resolved styles (a small zip/XML comparer was
used for Step 2; `tests/test_documentation.py::test_run_cmd_end_to_end` and
`tests/test_pipeline.py` cover the pipeline and CLI on the sample data).

## Conventions
- black/ruff, line length 100, py311 target. Google-style docstrings with Args/Returns.
- New config belongs in `data/data.yaml`, not in Python constants.
- Don't commit anything in `output/` or `Input/` (both gitignored). Sample output for
  regression comparison: `output/Invoices` (older `main` code) vs `output/Invoices_NEW` (`ft_v2`).

## Branches
- `main`: legacy single-file `PB-Ixtractor.py` (has working button/bookmark logic).
- `ft_v2` (current): src-layout refactor, YAML config, JSONPath extractors.
- Remote: https://github.com/MackanT/PBIxtractor
