# PBIxtractor — Claude Context

## Project Overview
PBIxtractor generates documentation for Power BI reports. It reads a `.pbix` (report
layout) plus the semantic model (`.bim` or TMDL folder, read directly; Tabular Editor 2
optional) and writes
Excel workbooks
with a visual inventory per page, filters, colour-coded DAX, relationships (PNG graph),
measure dependencies, unused columns/measures, model tables/columns (sources, storage mode),
Best Practice Analyzer results, and row counts/sizes when the report is open in Desktop.

Owner reviews and commits everything — **never run `git commit` / `git push`**.

## Stack
- Python ≥3.11, packaged with `uv` (src-layout, setuptools backend), version in
  `pyproject.toml` **and** `src/pbixtractor/__init__.py` (keep in sync)
- NiceGUI (>=2.24, 3.x installed) web UI on http://localhost:8081 (`web_ui.py`, default).
  The DearPyGUI desktop UI (extractor.py, --ui/--test) was removed on 2026-09-29.
- pandas, xlsxwriter (rich-text cells), networkx + matplotlib (relationship PNG)
- jsonpath-ng (`jsonpath_ng.ext`) for layout JSON queries, pydantic for row models
- Optional: Tabular Editor 2 CLI (Windows-only), called via `subprocess`:
  - Tabular Editor analysis (default on; `--no-tabular-editor`): Best Practice Analyzer (`-A`), exact DAX
    dependencies (`-S` script using `DependsOn`) and live statistics — all local/offline.
  - TSV export (default off; `--tabular-editor-tsv`): old `documentation.tsv` export; runs FormatDax, which
    sends DAX to daxformatter.com.
- sqlglot (native SQL in partitions), azure-identity (Fabric / DevOps sign-in)

## Running
Working clone: `C:\Users\MarcusToftås\Documents\PBIxtractor` (moved off OneDrive on 2026-09-29;
the old copy under `OneDrive…\Dokument\Other\PBI-Ixtractor\PBIxtractor` is no longer worked in —
OneDrive broke uv hardlinks and git reflog writes).
```powershell
uv run --frozen pbixtractor             # web UI (NiceGUI) on :8081, opens the browser
uv run --frozen pbixtractor web --port 8090 --no-browser
uv run --frozen pbixtractor extract "C:\...\Sales.pbix" [--model X.bim] [-o out] [--no-tabular-editor]
uv run --frozen --extra dev pytest -q   # tests (`--with pytest` does NOT work here: å in path)

# Optional smoke test against a real report kept outside the repo (never commit client data)
$env:PBIXTRACTOR_SAMPLE_PBIX = "C:\...\Reports V1\Invoices.pbix"
```
Output goes to `./output/<report name>/` (CWD-relative, gitignored) unless `-o` / the UI's output
folder says otherwise. The model is auto-found next to the report (`find_model_for_report`:
`<name>.bim`, then the PBIP model folder from `<name>.Report/definition.pbir`
(`datasetReference.byPath`), `<name>.SemanticModel`, `<name>.Dataset` - each as model.bim or
TMDL `definition/`); `extract` exits 2
when there is none. By default the model is read
from the `.bim` on every run; a `documentation.tsv` is only used with `USE_TABULAR_EDITOR`.
Real test reports (local only, never commit; the paths below are on the owner's first PC - on
another machine, list its reports in `Input/regression.json`, see "Refactoring safety net"):
- `…/_Arbete/Rowico/…/Reports V1/` (client data): `Invoices.pbix` + `Invoices.bim` (legacy
  Layout format; only one with a .bim) and 8 other legacy `.pbix` files.
- `C:\Users\MarcusToftås\Downloads\Adventure Works DW 2020.pbix` + `Model.bim` (owner's dummy
  report, **PBIR format**; model has no measures; no buttons/bookmarks/groups yet).
  Full pipeline for it: `pbixtractor extract "...\Adventure Works DW 2020.pbix" --model
  "...\Downloads\Model.bim"` (~12 s with Tabular Editor).
Tabular Editor 2 is installed at `C:\Program Files (x86)\Tabular Editor\`
(`tabular_editor.find_tabular_editor()`; extra folders in `Input/TabularEditorLocations.txt`,
written by `add_tabular_editor_location()` / the web UI's "Tabular Editor 2 folder" field).
Power BI Desktop is the Microsoft Store version: workspaces under
`%USERPROFILE%\Microsoft\Power BI Desktop Store App\AnalysisServicesWorkspaces`.

Git: only read-only commands (status/diff/log). **No commit, push or `git stash`** — the owner
reviews and commits everything. Compare against baselines with `tools/regress.py` instead.
The owner likes one commit per logical part: when a change spans parts, prepare `.patch` files
per part (verified by applying them in order to a `git archive HEAD` copy and running the tests
after each) and hand over `git apply --cached <part>.patch` + `git commit -m ...` commands;
when every file belongs to one part, plain `git add <files>` lists are enough. Staging is only
done on request.

## Architecture
```
src/pbixtractor/
  cli.py            argparse: no command → web UI; `web [--port] [--no-browser]`;
                    `devops list ORG [PROJECT [REPO]] [--history PATH]`, `devops fetch URL
                    [--version branch|tag:x|commit:x] [-o]`;
                    `extract [report | --fabric WS/REPORT | --devops URL [--version]] [--tenant] [--model] [-o] [--name]
                    [--no-tabular-editor] [--tabular-editor-tsv] [--description-tag]
                    [--no-log-file] [-q]` (exit 0 ok/warnings, 1 error, 2 no model/bad args);
                    `fabric list [WS]`, `fabric fetch WS/REPORT [-o]` (default download folder
                    output/_fabric/<ws>; with extract -o: <o>/source).
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
                    Drag & drop (ui.upload) copies .pbix/.bim to output/_uploads/ (browsers
                    never expose a dropped file's path). start() opens a browser tab only if no
                    tab connects within BROWSER_GRACE_SECONDS (7 s): after a restart the old
                    tab reconnects and reloads by itself, so no duplicate tabs.
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
                    Starts on an overview (source → table → page; table feeds a page if it is
                    upstream of it; tables feeding no page dashed). Toolbar: back/forward
                    history (Alt+←/→), "Up a level" (column/measure → table, visual → page,
                    else overview), zoom −/Fit/+, "Panels" (hide side panels), Guide overlay.
                    #<node id> in the URL opens that item (kept in sync via replaceState).
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
  m_sources.py      m_sources(M, queries, name) → (connector, source objects): Schema/Item,
                    Name (single-db connectors), Lakehouse Id/ItemKind, native SQL
                    (Value.NativeQuery / [Query=...] / DirectQuery "query" partitions) via
                    sqlglot tsql (sql_tables(), CTEs dropped), references to other queries
                    followed (shared expressions + single-M-partition tables; parameter
                    queries skipped), "Entered data". Partition.source_summary() uses it;
                    parse_model() sets Partition.queries.
  azure_auth.py     Shared sign-in + REST: get_credential(tenant) (one per process; AzureCli
                    if `az` exists, else InteractiveBrowserCredential with DPAPI cache + account
                    record ~/.pbixtractor/auth_record.json; one login serves Fabric and DevOps),
                    RestClient(api, scope, credential | pat) .request(method, url, body, raw)
                    with 429 retries, ApiError, FABRIC_SCOPE / DEVOPS_SCOPE.
  devops.py         Azure DevOps git: DevOpsClient(org) (PAT from AZURE_DEVOPS_PAT if set):
                    projects, repositories (defaultBranch), branches, files, find_reports
                    (*.Report/definition.pbir), commits(path) for the version picker,
                    download_folder (items $format=zip). fetch_report() downloads only the
                    .Report folder + the model folder its pbir byPath points to, keeping repo
                    paths under output/_devops/<project>/<repo>/<version>; byConnection reports
                    → error pointing to Fabric. parse_devops_url() takes browser URLs
                    (?path=...&version=GB|GT|GC...). Works for encrypted-label reports (git
                    holds plain PBIP).
  service_stats.py  Statistics for a model in the Power BI service via FabricClient.execute_query
                    (Power BI executeQueries, POWERBI_SCOPE). That API accepts plain DAX ONLY -
                    INFO functions and DMVs are rejected (HTTP 400, Microsoft docs; confirmed on
                    the owner's tenant) - so: row counts (one COUNTROWS UNION query, DirectQuery
                    tables and calc groups skipped) + distinct values (COLUMNSTATISTICS()). No
                    sizes / measure types (LiveStatistics.has_sizes False → writers leave size
                    columns empty); those need the XMLA endpoint (not built) or Desktop. Pipeline
                    uses it when options.service_model is set or <report>.fabric_source.json lies
                    next to the report; Desktop stats with sizes win over it.
  Model mode        ExtractionOptions.extra_reports: report_extractor.extract_reports() prefixes
                    page/bookmark display names "<report> › " (before extraction, so button
                    targets match) and page/bookmark ids "<n>:" (after), merges into one
                    ReportDefinition → one Documentation; "unused" = unused by all reports.
                    Fabric: fetch_report(all_reports, all_workspaces) finds reports by datasetId
                    (Power BI REST groups/{ws}/reports); DevOps: every definition.pbir whose byPath
                    resolves to the same model folder. Undownloadable reports → FetchedReport.skipped
                    (warned: "unused" may be incomplete). CLI --all-reports / --all-workspaces /
                    --also; UI switch "All reports on its semantic model". Output named after the
                    model (pipeline.model_name()).
  web_sources.py    Web UI panels FabricPanel (workspace → report) and DevOpsPanel (org →
                    project → repo → branch → report → version/commit); ready(), blocking
                    fetch(progress) → (report folder, model folder). make_fabric_client /
                    make_devops_client are the test seams. Choices remembered in
                    app.storage.general (.nicegui/, gitignored).
  fabric.py         Fabric REST (stdlib urllib): FabricClient (workspaces/reports/
                    semanticModels lists, getDefinition as long-running operation with
                    polling), fetch_report() → <report>.Report + <model>.SemanticModel (TMDL)
                    + fabric_source.json; definition.pbir rewritten to byPath. Model found via
                    semanticmodelid in the pbir connection string (other workspace by name).
                    default_credential(): AzureCliCredential if `az` exists, else
                    InteractiveBrowserCredential with DPAPI token cache + account record in
                    ~/.pbixtractor/. getDefinition needs Contributor (read+write) on the item.
  semantic_model.py model_source(path) → the .bim file or TMDL definition folder (accepts
                    .SemanticModel folders and model.tmdl too; the pipeline passes it to
                    Tabular Editor, which loads both). read_model(path) → SemanticModel
                    (tables, columns,
                    measures, hierarchies+levels, partitions incl. Direct Lake entity, relationships
                    with cardinality/direction/active, shared expressions, roles/RLS).
                    model_to_dataset() → DataFrame identical to TE's TSV (verified on Invoices:
                    all 459 objects match; only DAX whitespace differs). structurally_used_columns():
                    relationship keys, sort-by and hierarchy-level columns. Hierarchy levels are
                    sorted by `ordinal` (the .bim array order is not the level order).
  tmdl.py           TMDL folder → TMSL dict (same shape as a .bim) → parse_model(), so
                    both formats share one reader. parse_tmdl(): indentation tree (tabs),
                    `key: value` properties (".." with "" escapes), bare flags = true,
                    `///` descriptions, `= expr` inline / multi-line (two tabs deeper than
                    the object) / ``` fenced, `ref table X` at top level of model.tmdl =
                    table order. Verified against Tabular Editor 2.29 -TMDL exports of AW
                    (0 differences) and Invoices: only calculated columns with an inferred
                    type lack dataType in TMDL (DataType "Unknown"). Test fixture:
                    tests/sample_tmdl.py (TE export of the sample BIM).
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
  constants.py      DEFAULT_COLORS (DAX highlight colours), DESCRIPT_TAG, REPORT_COLUMNS.
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
- Old `Input/*.csv` files (from the removed DearPyGUI "User Input" tab) are not read; only
  `Input/TabularEditorLocations.txt` is.
- Text-matching dependency fallback order is set-based (not stable across runs). The data
  workbook's "dependencies" sheet headers (MeasureName, Dependent, ...) are unchanged on purpose
  (possible downstream consumers).
- Windows-only: backslash path joins, PowerShell launch of TE2, Excel-open check.
- Editing tip: shell heredocs/sed mangle backslash escapes (\t, \n) in Python/C# code —
  write patch scripts with the file tool instead. Files may have CRLF endings (autocrlf).

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
  reports: readers.py; models: semantic_model.py (.bim) + tmdl.py (TMDL, done 2026-09-29)) →
  model extras (done 2026-09-29: unused measures, model sheets, BPA, exact dependencies,
  live statistics) → 2 modular handlers/writers (run_cmd split into documentation.py /
  excel_report.py / dax.py / relationship_graph.py, config.py + extract_types handler
  registry, JSON writer done) → 3 report details (done 2026-09-29: page visibility/type,
  interactions, sync groups, hidden visuals, bookmarks sheet, report filters sheet) →
  4 HTML lineage viewer (done 2026-09-29; Excel cannot show it inline) → 5 pipeline + CLI +
  NiceGUI web UI with the viewer embedded (done 2026-09-29; DearPyGUI removed) →
  1b DevOps/Fabric readers → 6 SQL/Fabric source lineage (sqlglot).

## Next steps (handoff 2026-09-30)
Work moves to the owner's second PC, which has access to Fabric/DevOps and the source databases.
Everything local-file based is done (both report formats, .bim + TMDL, web UI, CLI). Open:

**Status 2026-09-30 (second PC, uncommitted):** 1b Fabric + Azure DevOps fetch built
(fabric.py, devops.py, azure_auth.py, CLI `fabric`/`devops`/`extract --fabric|--devops`, web UI
source toggle Local file / Fabric / Azure DevOps via web_sources.py). Tested against fake APIs,
a local HTTP server and NiceGUI user simulation; NOT yet run against a real tenant/org (no `az`
here; first call opens a browser login). Fabric getDefinition is blocked for reports with an
encrypting sensitivity label (Microsoft docs) - DevOps (git) is the route for those.
Fabric download verified by the owner on Navigator (2026-09-30). Service statistics and model
mode are built and tested with fakes, not yet on the real tenant. Tests: tests/conftest.py
blocks real sign-in (azure_auth.get_credential raises) - pass fake clients/credentials. 6a done (m_sources.py); no
local model has native SQL, so that part is covered by unit tests only; the Name-navigation,
query-reference and entered-data parts were verified on the Navigator/Hallbarhet/Rowico models.
6b (database connection) not started. Also: broken bookmarks (recorded on a deleted page),
Log (N) tab count, lineage viewer guide overlay, Purview-encrypted .pbix error,
listSlicer/textSlicer/pageNavigator in data.yaml.

**1b Remote reports/models (Fabric workspace, Azure DevOps).** Design: a fetch step that
materialises a PBIP layout in a temp/output folder (`<name>.Report/` + `<name>.SemanticModel/`)
and then runs the unchanged local pipeline on it - readers.py/tmdl.py already read that layout.
- Azure DevOps: PBIP projects in a git repo already work after `git clone` (point the tool at
  the .pbip / .Report folder). Optional: fetch items via the DevOps REST API instead of cloning.
- Fabric: REST `POST /v1/workspaces/{ws}/reports/{id}/getDefinition` and
  `.../semanticModels/{id}/getDefinition` (`?format=TMSL` for a .bim, default TMDL); long-running
  (202 → poll the operation, then fetch the result); response = definition parts (path +
  base64 payload) → write them to disk. Auth: `azure-identity` (InteractiveBrowserCredential or
  AzureCliCredential), scope `https://api.fabric.microsoft.com/.default`. Check the required
  item permissions in the Fabric docs (getDefinition has needed write access to the item).
  Semantic Link (`sempy` / `semantic-link-labs`) is mainly for Fabric notebooks; a local tool
  should use REST. Also list workspaces/reports so the web UI can offer a picker.
- CLI/web UI: e.g. `pbixtractor extract --fabric <workspace>/<report>`; tokens never stored in
  the repo.

**6 SQL/Fabric source lineage.** Today `Partition.source_summary()` already gives connector +
source objects (M `Sql.Database` navigation, Direct Lake `schemaName.entityName`), shown as
source nodes in the lineage graph.
- 6a (offline, no database needed): parse native SQL in partitions (`Value.NativeQuery`,
  `Sql.Database(..., [Query=...])`) with `sqlglot` (dialect `tsql`) → source tables/columns.
- 6b (needs the database): connect to Azure SQL / Fabric Warehouse / Lakehouse SQL endpoint with
  an Entra token (`azure-identity` + `mssql-python` or `pyodbc` + ODBC Driver 18), read view
  definitions (`sys.sql_modules`, `sys.sql_expression_dependencies`), parse with
  `sqlglot.lineage` → view → base table/column edges in the lineage graph + a "sources" sheet.
  Read-only account with VIEW DEFINITION is enough. (psycopg was dropped 2026-09-30; add a
  Postgres driver back only if Postgres sources appear.)

**Checklist for the new PC:** clone `ft_v2`; `uv sync --extra dev`; Tabular Editor 2 (optional,
found in Program Files or set in the web UI); create `Input/regression.json` with that PC's test
reports and run `tools/regress.py --save-baseline` before the first change; for 1b/6: Fabric
workspace access, a DevOps repo with a PBIP project, SQL endpoint + database, ODBC Driver 18.

**Second PC status (2026-09-30):** cloned to `C:\Users\MarcusToftås\Documents\PBIxtractor`, synced,
`Input/regression.json` = Invoices (legacy .pbix + .bim) and Hallbarhet (PBIP: PBIR + TMDL,
`Projects\Frontend\gold_workspaces\hallbarhet_rapportering_gold\Hallbarhet_New.Report`); baseline saved.
- `uv` is not on PATH in PowerShell: use `~\.local\bin\uv.exe`. `uv sync` error 396 = the uv cache
  holds cloud placeholders → `uv cache clean <packages from uv.lock>`, then sync again.
- Tabular Editor must be ≥2.29: 2.21 failed the BPA script (CS1545 on `AnalyzerResult`
  properties) and TMDL loading (`source`, `ref cultureInfo`). Upgraded to 2.29 on 2026-09-30;
  all tests pass and the baseline was re-saved with TE analysis.
- Many client .pbix files here are **Purview-encrypted** (start with `.pfile`, e.g. Castellum
  Navigator) → "File is not a zip file"; they cannot be read. A clear error message is an open item.

Smaller open items (no remote access needed): Excel → lineage viewer hyperlink; web UI polish
(remember last paths, run history, model tab, JSON search); list the filter/slicer state that
bookmarks capture. (The old plan docs describing removed code were deleted 2026-09-30; they
remain in git history. docs/ADD_VISUAL_TYPES.md is still current.)

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
Output is deterministic (except the graph PNG layout). Regression check on real reports:
```powershell
# Input/regression.json (gitignored): {"reports": [{"name": "AW", "report": "...pbix", "model": "...bim"}]}
uv run --frozen python tools/regress.py --save-baseline   # before the change -> output/_baseline/
uv run --frozen python tools/regress.py                   # after: compare every workbook
uv run --frozen python tools/compare_xlsx.py OLD.xlsx NEW.xlsx     # one pair, cell by cell
uv run --frozen python tools/compare_models.py Model.bim X.SemanticModel/definition  # .bim vs TMDL
```
compare_xlsx compares sheets, values, rich-text runs and resolved styles (not style indexes).
Intended output changes: show the diff to the owner, then re-save the baseline. Unit tests
(`tests/test_documentation.py::test_workbooks_end_to_end`, `tests/test_pipeline.py`) cover the
pipeline and CLI on the anonymised sample data.

## Conventions
- black/ruff, line length 100, py311 target. Google-style docstrings with Args/Returns.
- New config belongs in `data/data.yaml`, not in Python constants.
- Don't commit anything in `output/` or `Input/` (both gitignored; regression config and
  baselines live there and may contain client data).
- `tools/` holds developer scripts (not part of the package): regress.py, compare_xlsx.py,
  compare_models.py.

## Branches
- `main`: legacy single-file `PB-Ixtractor.py` (has working button/bookmark logic).
- `ft_v2` (current): src-layout refactor, YAML config, JSONPath extractors.
- Remote: https://github.com/MackanT/PBIxtractor
