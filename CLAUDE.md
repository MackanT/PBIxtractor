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
- NiceGUI (>=3.0) web UI on http://localhost:8081 (`web_ui.py`, default).
  The DearPyGUI desktop UI (extractor.py, --ui/--test) was removed on 2026-09-29.
- pandas, xlsxwriter (rich-text cells), networkx + matplotlib (relationship PNG)
- pydantic for row models (the jsonpath-ng dependency was removed 2026-09-30: nothing used it)
- Optional: Tabular Editor 2 CLI (Windows-only, ≥2.29), called via `subprocess`:
  - Tabular Editor analysis (default on; `--no-tabular-editor`): Best Practice Analyzer (a `-S`
    script calling the Analyzer, not `-A`), exact DAX dependencies (`-S` script using
    `DependsOn`) and live statistics — all local/offline.
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
.venv\Scripts\python.exe -m pytest -q -m "not tabular_editor"   # without the slow real-TE tests
# Plain `uv sync` removes pytest/ruff from the venv: always `uv sync --extra dev`

# Optional smoke test against a real report kept outside the repo (never commit client data)
$env:PBIXTRACTOR_SAMPLE_PBIX = "C:\...\Reports V1\Invoices.pbix"
```
Output goes to `./output/<report name>/` (CWD-relative, gitignored) unless `-o` / the UI's output
folder says otherwise. The model is auto-found (`find_model_for_report`): a .pbix with its own
model IS the model (pbix_model.py, no .bim needed; preferred over a sibling .bim, which can be
older/newer than the report); else `<name>.bim`, then the PBIP model folder from `<name>.Report/definition.pbir`
(`datasetReference.byPath`), `<name>.SemanticModel`, `<name>.Dataset` - each as model.bim or
TMDL `definition/`. A live-connected .pbix (no model inside) gets its published model from
Fabric (pbix_model.live_connection → fabric.fetch_connected_model; sign-in, Contributor on the
model). `extract` exits 2
when there is none. The model is read from the `.bim` / TMDL on every run; a
`documentation.tsv` is only used with `--tabular-editor-tsv` (ExtractionOptions.tabular_editor_tsv).
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
                    [--ref branch|tag:x|commit:x] [-o]`;
                    `extract [report | --fabric WS/REPORT | --devops URL [--ref]] [--tenant] [--model] [-o] [--name]
                    [--all-reports] [--all-workspaces] [--also REPORT] [--catalog FOLDER]
                    [--no-service-statistics] [--no-tabular-editor] [--tabular-editor-tsv]
                    [--description-tag] [--no-log-file] [-q]` (exit 0 ok/warnings, 1 error,
                    2 no model/bad args); `fabric list [WS]`, `fabric fetch WS/REPORT [-o]`
                    (default download folder output/_fabric/<ws>; with extract -o: <o>/source);
                    `catalog list|remove|rebuild FOLDER [KEY]`. `--ref` was `--version` (still
                    accepted); top-level `pbixtractor --version` prints the program version.
                    Fetch errors (ApiError/ValueError/OSError) → "ERROR: ..." + exit 1; anything
                    else keeps its traceback on purpose (a bug).
  pipeline.py       The orchestration, no globals: run_extraction(ExtractionOptions,
                    progress(step, fraction), on_log(level, msg)) → ExtractionResult(status
                    success|warnings|error, message, files {workbook, data_workbook, json,
                    lineage, graph[, tsv, log]}, logs, documentation, report_json, seconds).
                    Steps: model → TE analysis (BPA, deps, live stats) → dataset →
                    ReportExtractor → build_documentation → graph → workbooks → JSON + lineage.
                    One run at a time (_RUN_LOCK: shared logger/TE/Excel); any captured log
                    record → "warnings" (+ logs/log_data_<date>_<time>.txt, never overwritten).
                    ReportReadError (encrypted/unreadable report) → error without traceback.
                    report_name(), find_model_for_report(), model_name().
  report_extractor.py CONFIG = config.load_config() (RuntimeError on failure; importing never
                    exits the process) → visual_mapper, visual_type_list, known_functions;
                    ReportExtractor (readers.read_report → ReportContext + PageExtractor);
                    extract_reports(paths) for model mode.
  web_config.py     How the web UI is hosted: WebConfig(prefix, output_root, storage_prefix,
                    local_machine, embedded) in web_config.CONFIG (read it via the module - configure()
                    replaces it); url(), output_root(), stored_choices() (app.storage.general behind
                    a key prefix). Stand-alone = defaults; web_ui.register() sets the embedded ones.
  web_ui.py         NiceGUI app, stand-alone and embeddable (docs/EMBEDDING.md):
                    register(prefix="/pbixtractor", output_root, storage_prefix="pbixtractor.",
                    local_machine=False) for a host app, then build_page() inside the host's page
                    (no header/rail/theme/host check of ours; routes under the prefix; a catalog
                    link on the page). local_machine=False hides everything acting on the server's
                    machine: path inputs + PathPicker, Tabular Editor folder, Desktop detection,
                    output/catalog folder inputs (and never stores catalog_dir), "Open folder";
                    uploads fill the hidden inputs. _THEME_SYNC script: embedded lineage iframes
                    follow body.body--dark by postMessage (any host). Stand-alone: start() →
                    configure() defaults + register_routes() + host check + ui.run(index);
                    index() = apply_theme + _build_shell() (pine header + 60px icon rail, menu toggles
                    labels, light/dark toggle remembered in stored_choices; Catalog rail entry
                    once a catalog exists) + build_page() (report/model/output inputs, server-side
                    PathPicker dialog, options, TE/Desktop status) → run_extraction in
                    run.io_bound; progress/log via a queue drained by ui.timer. Results:
                    stat cards, download buttons, tabs Lineage (iframe of <name>_lineage.html
                    served by /files/{token}/{name}; token → output folder, 404 outside it),
                    Model quality (BPA per rule), Unused, Bookmarks, Log. start() =
                    register_routes() + ui.run(index, reload=False). Local tool: binds
                    127.0.0.1, no auth. Unmatched URLs get the root page (HTTP 200).
                    Drag & drop (ui.upload) copies .pbix/.bim to output/_uploads/ (browsers
                    never expose a dropped file's path). Files arriving within DROP_SECONDS (30)
                    of each other are one drop: several reports + one model → model mode (chips
                    "Documented together", removable); several models → _shared_model() refuses
                    ("one model is documented at a time"). start() opens a browser tab only if no
                    tab connects within BROWSER_GRACE_SECONDS (7 s): after a restart the old
                    tab reconnects and reloads by itself, so no duplicate tabs.
                    Security (it is a local server, but browsers can reach it): add_host_check()
                    middleware answers 421 unless Host is 127.0.0.1/localhost/::1 (DNS
                    rebinding); /files and /catalog pages get CSP `sandbox allow-scripts
                    allow-popups allow-downloads` (opaque origin: generated pages cannot call
                    the app); /catalog serves only catalog.html, catalog.json and
                    models/<key>/<file> (checked on the resolved path); logs render as escaped
                    text (_log_text), uploads capped at MAX_UPLOAD_BYTES (2 GB).
                    One download + run at a time across tabs (_JOB_LOCK; a second tab shows
                    "Waiting for a run in another tab"). Power BI Desktop detection runs in
                    run.io_bound (psutil takes seconds), debounced 0.5 s while typing a path.
                    Catalog option (switch + folder, default output/_catalog, remembered via
                    stored_choices()); header link "Catalog" → /catalog/.
  theme.py          Visual style ("field-station", shared with the data-platform web UI that
                    PBIxtractor will be embedded in): colour tokens (sap accent, copper = the one
                    primary action/attention, rust/lichen/glacier status, bone/bark grounds),
                    theme_css(accent) (header+rail band = darken(accent, .55)), apply_theme()
                    (stand-alone only - embedded pages reuse the host's identical class names),
                    serve_fonts() at /pbixtractor-fonts (Familjen Grotesk + Source Sans 3, OFL,
                    bundled in data/fonts with licences; never a CDN), page_title (masthead),
                    card_header, stat_tile (eyebrow + numeral + edge-*), empty_state. Pages use
                    Quasar colour names / these classes, never hex. Light is the default.
  config.py         load_config() → Config(visual_mapper, supported_visual_types (derived:
                    standard_visuals + special_visuals), data_types, extract_types,
                    function_names). extract_type(visual_type): standard | button | skip.
  json_report.py    write_json() → <name>.json (SCHEMA_VERSION 2): reports [{name, file, pages}],
                    report (pages with report/title, bookmarks and filters with report), model
                    (incl. live stats), dependencies, unused, quality, lineage {nodes, edges} (ids
                    report:/page:/visual:/table:/column:/measure:/source:, edge types contains
                    (report→page, page→visual, table→field)/uses/filters/depends_on/
                    relationship/loads_from). Every page gets a node (also empty ones), labelled
                    with its own title.
  lineage_html.py   write_lineage_html(path, documentation_to_dict(...)) → <name>_lineage.html:
                    one self-contained offline page (vanilla JS + SVG, data embedded as JSON,
                    "</" escaped). Edges re-oriented to flow data → report (source → table →
                    column → measure → visual → page → report); relationships left out. Layer per
                    node (measures by dependency depth); in-browser layered layout with
                    barycenter ordering; select → upstream/downstream; details panel.
                    Starts on an overview (source → table → page, or → report when several
                    reports; table feeds it if it is upstream; tables feeding nothing dashed).
                    Toolbar: back/forward history (Alt+←/→), "Up a level" (column/measure →
                    table, visual → page, page → report, else overview), zoom −/Fit/+, "Panels" (hide side panels), Guide overlay.
                    #<node id> in the URL opens that item (kept in sync via replaceState).
  documentation.py  Analysis stage, no file output: build_documentation() → Documentation
                    (report_info, filter_strings, pages: {page: [PageItem]}, model, objects
                    [OBJECT_COLUMNS], relations, unused_columns/measures, exact deps, BPA,
                    live stats, page_info, bookmarks (incl. used_by buttons and filters: BookmarkFilter
                    with changed True/False vs the saved report state, None when the page/
                    visual is gone; empty when Data is not captured), interactivity
                    {(page, visual id): notes}; depends_on(table, name)). Needs the
                    ReportDefinition (ReportExtractor.report) for page/bookmark details.
                    Also parse_tsv_object_name,
                    button_target_and_label, field_description.
  dax.py            find_functions/find_measures/find_columns, text_dependencies() (fallback
                    when no exact deps), highlight_dax() → rich-text segments.
  excel_report.py   create_formats(), write_main_workbook(), write_data_workbook(); shared
                    helpers per sheet type (_write_relations, _write_objects, _write_page_sheet,
                    _write_pages_sheet, _write_dependencies_sheet). _Workbook/_Worksheet: text
                    over Excel's 32,767-character cell limit ends in TRUNCATED_MARKER and logs a
                    warning (xlsxwriter cuts plain text silently and drops a too-long rich
                    string altogether). WORKBOOK_OPTIONS: no strings_to_formulas/_urls.
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
                    RestClient(api, scope, credential | pat) .request(method, url, body, raw):
                    https only (except localhost), retries 429/5xx/network errors (MAX_ATTEMPTS
                    5, Retry-After seconds or date, max 120 s), no Authorization header on a
                    redirect to another host, a 203/HTML answer = "sign-in page" error.
                    error_text() reads Fabric and Power BI error bodies. ApiError, FABRIC_SCOPE /
                    POWERBI_SCOPE / DEVOPS_SCOPE; default_credential() lives here.
  devops.py         Azure DevOps git: DevOpsClient(org) (PAT from AZURE_DEVOPS_PAT if set):
                    projects, repositories (defaultBranch), branches, files, find_reports
                    (*.Report/definition.pbir), commits(path) for the version picker,
                    download_folder (items $format=zip). fetch_report() downloads only the
                    .Report folder + the model folder its pbir byPath points to, keeping repo
                    paths under output/_devops/<project>/<repo>/<version>; byConnection reports
                    → fetch_report(connected_model=hook) gets the published model (CLI/web pass a
                    hook calling fabric.fetch_connected_model; model mode matches other reports
                    by the same model id); without a hook → error pointing to Fabric. parse_devops_url() takes browser URLs
                    (?path=...&version=GB|GT|GC...; also the short /org/_git/Repo form). Works
                    for encrypted-label reports (git holds plain PBIP). Safety: only
                    dev.azure.com / *.visualstudio.com over https (more hosts:
                    PBIXTRACTOR_DEVOPS_HOSTS=a,b); repo paths via safe_repo_path() and
                    _resolve_repo_path() (no "..", ":" or "\"; a pbir byPath may not leave the
                    repo); zips checked for containment and MAX_ARCHIVE_BYTES (2 GB unpacked);
                    a non-zip answer (usually a sign-in page) → ApiError.
  service_stats.py  Statistics for a model in the Power BI service via FabricClient.execute_query
                    (Power BI executeQueries, POWERBI_SCOPE). That API accepts plain DAX ONLY -
                    INFO functions and DMVs are rejected (HTTP 400, Microsoft docs; confirmed on
                    the owner's tenant) - so: row counts (one COUNTROWS UNION query, DirectQuery
                    tables and calc groups skipped) + distinct values (COLUMNSTATISTICS()). No
                    sizes / measure types (LiveStatistics.has_sizes False → writers leave size
                    columns empty); those would need the XMLA endpoint, which is NOT planned (a
                    report may live in a Pro workspace). Pipeline uses it when
                    options.service_model is set or <report>.fabric_source.json lies next to the
                    report; it then replaces Desktop statistics (a downloaded report only matches
                    a Desktop model by name, possibly another copy). DirectQuery tables are
                    found with SemanticModel.partition_mode() (partition "default" → the
                    model's defaultMode).
  Report level      Every page/bookmark knows its report (PageDefinition.report/.title,
                    BookmarkDefinition.report, ReportDefinition.reports {name: file}, set by
                    ReportExtractor.extract(report_name=)); filter rows get the report as an 8th
                    column via report_extractor.with_report() (an "All Pages" filter has no page
                    to tell; identical filters of two reports stay two rows). PageInfo/
                    BookmarkInfo.report, Documentation.reports; Excel "report pages",
                    "bookmarks", "report filters" sheets start with a Report column. The page
                    KEY stays the (prefixed) display name, so nothing keyed on pages changed.
                    Prepares running several models / a whole workspace (not built yet).
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
  catalog.py        Catalog folder (options.catalog_dir / --catalog / UI "Add to catalog"):
                    entries/<key>.json = one SLIM entry per semantic model (reports+pages,
                    tables/columns/measures with DAX, usage field→pages, DAX depends_on, unused
                    flags; no BPA/bookmarks/partitions), models/<key>/ = copied lineage viewer +
                    workbook (relative links, deep links #measure:T[M]), catalog.json + one offline
                    catalog.html (search names/DAX, filters, details, "Loaded by" across models).
                    Key from origin (source_identity): fabric-<model id> | devops-<proj>-<repo>-
                    <hash> | file-<name>-<hash>; same model again replaces the entry (latest only).
                    CLI `catalog list|remove|rebuild FOLDER`. Web UI serves it at /catalog/
                    (default folder output/_catalog). All writes/removals go through _inside()
                    (a crafted key cannot leave the catalog folder); the page only links
                    models/... (safeLink) and embeds JSON with "<" as <. Reports that
                    could not be downloaded are listed per entry ("not_included").
                    Known gap: source names are not database-qualified (Name navigation gives
                    "vw_x", Schema/Item "dbo.vw_x") - cross-model source matching needs server/db
                    (with 6b).
  web_sources.py    Web UI panels FabricPanel (workspace → report) and DevOpsPanel (org →
                    project → repo → branch → report → version/commit); ready(), blocking
                    fetch(progress) → (report folder, model folder). make_fabric_client /
                    make_devops_client are the test seams. Choices remembered in
                    app.storage.general (.nicegui/, gitignored).
  fabric.py         Fabric REST (stdlib urllib): FabricClient (workspaces/reports/
                    semanticModels lists, getDefinition as long-running operation with
                    polling), fetch_report() → <report>.Report + <model>.SemanticModel (TMDL)
                    (model part via download_semantic_model()); fetch_connected_model(client,
                    model_id, workspace?, root) = find_semantic_model() (named workspace first, else
                    every workspace you can open) + download into <root>/<workspace>/
                    + fabric_source.json; definition.pbir rewritten to byPath. Model found via
                    semanticmodelid in the pbir connection string (other workspace by name).
                    Sign-in via azure_auth.get_credential(). getDefinition needs Contributor
                    (read+write) on the item. reports_on_model(not_searched=...) records
                    workspaces it could not search (→ FetchedReport.skipped).
  pbix_model.py     The model inside a .pbix (no .bim, Desktop or TE needed): PBIXRay (MIT dep)
                    unpacks DataModel → metadata.sqlitedb (TOM tables) → database_from_metadata()
                    builds the TMSL dict (TOM enum codes → TMSL names; skips internal H$/R$/U$
                    tables (SystemFlags bit 1 - bit 2 marks calculated tables such as DATATABLE
                    or field parameters, which are kept) and rowNumber columns; carries annotations, so TE's BPA ignore rules survive, and
                    isAvailableInMdx). Verified on Invoices.pbix vs Invoices.bim: only the known
                    version differences; TE BPA 125 = 125 findings. statistics_from_metadata():
                    rows (row-number column), distinct values ONLY for hash dictionaries
                    (DictionaryStorage.Type 1; value encoding stores a range), sizes via PBIXRay.
                    The pipeline writes <name>_model.bim (TE input + download; never <name>.bim)
                    and uses the .pbix statistics when neither Desktop nor the service gives any.
                    has_embedded_model() (zip listing only), live_connection() (Connections file
                    → model id, workspace name if the connection string has it).
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
                    display/page, target_visuals, hidden_visuals, group, filters:
                    CapturedFilter(level All Pages|This Page|Visual|Slicer, page, visual, raw
                    filter-pane entry) from explorationState filters.byExpr (entries with a
                    condition) and slicer selections objects.merge.general[].properties.filter;
                    slicer_selection() reads the same spot of a visual). All format-specific
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
  logger.py         setup_logger()/get_logger() (console handler); capture_logs(callback)
                    context manager (per run, used by the pipeline) + CallbackHandler; captures
                    only the calling thread's records (parallel tabs keep their logs apart).
  utils/__init__.py write_to_excel (rich strings), is_excel_open_with_file (tries to open the
                    file for writing), excel_sheet_name, safe_name (file names from service
                    names; also avoids CON/NUL/...; fabric.safe_name = devops.safe_name = it).
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
  legacy numeric pageBinding types are ignored on purpose.
- Bookmark filter state (2026-09-30): only the owner's two reports were checked; none of their
  data-capturing bookmarks changes a filter (the "changed" path is unit-tested only). Not read:
  cross-highlight selections (`visualContainers.*.highlight`) and drill state. With "Selected
  visuals", page/report filters are still listed (assumed applied - not verified in Desktop).
- Old `Input/*.csv` files (from the removed DearPyGUI "User Input" tab) are not read; only
  `Input/TabularEditorLocations.txt` is.
- Text-matching dependency fallback order is set-based (not stable across runs). The data
  workbook's "dependencies" sheet headers (MeasureName, Dependent, ...) are unchanged on purpose
  (possible downstream consumers).
- Windows-only: Tabular Editor 2 and Power BI Desktop detection (the rest is portable).
- Editing tip: shell heredocs/sed mangle backslash escapes (\t, \n, \x00) in Python/C# code —
  write patch scripts with the file tool instead. Line endings: .gitattributes `text=auto`
  (LF in the repo, CRLF in the Windows checkout).

## Direction (agreed with owner, 2026-09-29)
- Must support **both** report formats: classic `.pbix` (`Report/Layout`) and PBIP/PBIR
  (`definition/pages/*/visuals/*/visual.json`), plus PBIR-Legacy `report.json`.
  Both readers produce the same typed report model.
- Must be able to read reports/models **remotely** (Fabric workspace / Azure DevOps git),
  via the Fabric REST `getDefinition` API and the DevOps git API (built), not only local files.
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

## Status and open items (2026-09-30)
Done: both report formats, .bim + TMDL, CLI + web UI, 1b Fabric + Azure DevOps fetch (UI pickers
and CLI; Fabric download verified on Navigator), model mode (all reports on one model), service
statistics (plain DAX after the tenant rejected INFO functions with HTTP 400; the plain-DAX
version is tested with fakes only, not yet re-run on the tenant), catalog
step 1, 6a (native SQL via sqlglot; Name-navigation / query-reference / entered data verified on
Navigator/Hallbarhet/Rowico). A full audit (security, extraction correctness, remote/pipeline/UI,
hygiene) was fixed on 2026-09-30; see git history for details.

Open, by owner priority:
- Module + stand-alone (owner, 2026-09-30): PBIxtractor must work stand-alone AND as a module
  inside the owner's private data-platform NiceGUI app (read only via `gh api`; never clone it
  or store its content on this PC). Phase 1 done: theme.py + restyled stand-alone UI.
  Phase 2 done (web_config.py, register/build_page, tests/test_embedding.py, docs/EMBEDDING.md).
  Phase 3 (2026-09-30): PBIxtractor side done - build_page(title=) + no padding when embedded,
  theme sync sent via run_javascript (works in ui.sub_pages hosts), register(access=...) gates
  the file routes (403, fails closed), azure_auth uses EnvironmentCredential when a service
  principal is in the environment (never a browser then), version 0.3.0. data-platform side:
  branch `feat/powerbi-docs` created via the GitHub API (owner opens the PR/merges); it pins
  `pbixtractor @ git+...@v0.3.0`, so the v0.3.0 tag must exist first. Earlier plan note: integration in data-platform
  (git dependency pinned to a tag + one nav section) - done there, not from here. Later: restyle
  the generated lineage/catalog HTML pages; logo/about page (logo_large.png kept for it).
- 6b database lineage (lower): connect to Azure SQL / Fabric Warehouse / Lakehouse SQL endpoint
  with an Entra token (`azure-identity` + `mssql-python` or `pyodbc` + ODBC Driver 18), read
  `sys.sql_modules` / `sys.sql_expression_dependencies`, parse with `sqlglot.lineage` → view →
  base table/column edges + a "sources" sheet. Read-only account with VIEW DEFINITION suffices.
  Source names are not database-qualified yet (needed for cross-model matching).
- Catalog step 2: document a whole workspace (not prio 1); cross-model impact after 6b.
- Excel → lineage viewer links (low). Web UI polish (run history, model tab, JSON search).
- Not planned: XMLA sizes for service models (reports may live in Pro workspaces).

Tests: tests/conftest.py blocks real sign-in (get_credential raises, AZURE_DEVOPS_PAT removed),
allows only local sockets, and points NiceGUI storage at a temp folder - pass fake
clients/credentials. Web UI tests poll (`_wait_for`) instead of fixed sleeps.

This PC (`C:\Users\MarcusToftås\Documents\PBIxtractor`): `Input/regression.json` = Invoices
(legacy .pbix + .bim) and Hallbarhet (PBIP: PBIR + TMDL,
`Projects\Frontend\gold_workspaces\hallbarhet_rapportering_gold\Hallbarhet_New.Report`).
- `uv` is not on PATH in PowerShell: use `~\.local\bin\uv.exe`. `uv sync` error 396 = the uv cache
  holds cloud placeholders → `uv cache clean <packages from uv.lock>`, then sync again.
- Tabular Editor must be ≥2.29 (2.21 failed the BPA script and TMDL loading).
- Many client .pbix files here are **Purview-encrypted** (start with `.pfile`, e.g. Castellum
  Navigator): they cannot be read; the run fails with a clear ReportReadError (no traceback).
  Use Fabric or DevOps for those.

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
- `ft_v2` (current): src-layout refactor, YAML config, typed readers/extractors.
- Remote: https://github.com/MackanT/PBIxtractor
