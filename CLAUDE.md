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
- DearPyGUI (+ tkinter file dialogs) for the desktop UI
- pandas, xlsxwriter (rich-text cells), networkx + matplotlib (relationship PNG)
- jsonpath-ng (`jsonpath_ng.ext`) for layout JSON queries, pydantic for row models
- Optional: Tabular Editor 2 CLI (Windows-only), called via `subprocess`:
  - `RUN_TE_ANALYSIS` (default on, UI checkbox): Best Practice Analyzer (`-A`), exact DAX
    dependencies (`-S` script using `DependsOn`) and live statistics — all local/offline.
  - `USE_TABULAR_EDITOR` (default off): old `documentation.tsv` export; runs FormatDax, which
    sends DAX to daxformatter.com.
- `psycopg` is declared but unused (reserved for a future SQL-source feature)

## Running
Working clone: `C:\Users\MarcusToftås\Documents\PBIxtractor` (moved off OneDrive on 2026-09-29;
the old copy under `OneDrive…\Dokument\Other\PBI-Ixtractor\PBIxtractor` is no longer worked in —
OneDrive broke uv hardlinks and git reflog writes).
```powershell
uv run --frozen pbixtractor --ui        # GUI (default when no args)
uv run --frozen pbixtractor --test      # dev run; paths hardcoded in extractor.run_test_extraction()
uv run --frozen --extra dev pytest -q   # tests (`--with pytest` does NOT work here: å in path)

# Optional smoke test against a real report kept outside the repo (never commit client data)
$env:PBIXTRACTOR_SAMPLE_PBIX = "C:\...\Reports V1\Invoices.pbix"
```
Output goes to `./output/<SAVE_NAME>/` (CWD-relative, gitignored). By default the model is read
from the `.bim` on every run; a `documentation.tsv` is only used with `USE_TABULAR_EDITOR`.
Real test reports (local only, never commit):
- `…/_Arbete/Rowico/…/Reports V1/` (client data): `Invoices.pbix` + `Invoices.bim` (legacy
  Layout format; only one with a .bim) and 8 other legacy `.pbix` files.
- `C:\Users\MarcusToftås\Downloads\Adventure Works DW 2020.pbix` + `Model.bim` (owner's dummy
  report, **PBIR format**; model has no measures; no buttons/bookmarks/groups yet).
  Full pipeline for it: set `extractor._PBIX_`, `_BIM_` (`["Model", downloads]`), `SAVE_NAME`
  and call `run_cmd()` — `--test` only knows the Invoices paths.
Tabular Editor 2 is installed at `C:\Program Files (x86)\Tabular Editor\`
(`tabular_editor.find_tabular_editor()`; extra folders in `Input/TabularEditorLocations.txt`).
Power BI Desktop is the Microsoft Store version: workspaces under
`%USERPROFILE%\Microsoft\Power BI Desktop Store App\AnalysisServicesWorkspaces`.

Git: only read-only commands (status/diff/log). **No commit, push or `git stash`** — the owner
reviews and commits everything. Compare against baselines by copying output elsewhere instead.

## Architecture
```
src/pbixtractor/
  cli.py            argparse entry (--ui / --test / --version). No real headless mode yet.
  extractor.py      ~1100 lines. CONFIG = config.load_config() (sys.exit on failure) →
                    visual_mapper, visual_type_list, known_functions; globals (SAVE_NAME, _PBIX_=[stem, dir],
                    _BIM_, LOG_DATA, DESCRIPT_TAG, USE_TABULAR_EDITOR, RUN_TE_ANALYSIS),
                    ReportExtractor (readers.read_report → ReportContext + PageExtractor),
                    run_ui() (DearPyGUI), gen_tsv() (TE2 TSV export), run_test_extraction(),
                    _tabular_editor_analysis(), run_cmd() (~100-line orchestration:
                    model → TE analysis → dataset → ReportExtractor → build_documentation →
                    graph → write_main_workbook / write_data_workbook / write_json → logs).
  config.py         load_config() → Config(visual_mapper, supported_visual_types (derived:
                    standard_visuals + special_visuals), data_types, extract_types,
                    function_names). extract_type(visual_type): standard | button | skip.
  json_report.py    write_json() → <name>.json: report, model (incl. live stats), dependencies,
                    unused, quality, lineage {nodes, edges} (ids page:/visual:/table:/column:/
                    measure:/source:, edge types contains/uses/filters/depends_on/
                    relationship/loads_from). Base for the Step 4 HTML graph.
  documentation.py  Analysis stage, no file output: build_documentation() → Documentation
                    (report_info, filter_strings, pages: {page: [PageItem]}, model, objects
                    [OBJECT_COLUMNS], relations, unused_columns/measures, exact deps, BPA,
                    live stats; depends_on(table, name)). Also parse_tsv_object_name,
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
                    display_name), aliases, objects, container_objects, filters). All
                    format-specific JSON knowledge lives here; zip files are read on demand.
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
  logger.py         setup_logger(capture=True) → LogCapture buffer; any captured log line
                    makes run_cmd() return "Log" instead of "Success".
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
   PBIR buttons/groups/bookmarks are so far only tested with the hand-built fixture
   (`tests/sample_pbir.py`), not a real report.
2. Item rows: `[Page, Visual Type, Visual ID, Table, Name, Display Name, Type]`
   (`REPORT_COLUMNS`). Type is the projection role label, `"Hierarchy"`, `"Formatting"`
   (field used only in formatting objects), or for buttons the action type.
   Buttons (and shapes/images with an action): Visual Type `actionButton`, Name = target
   display name (or `(missing bookmark: <id>)`), Display Name = button label.
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
3. `run_cmd()` always reads the model with `read_model(_BIM_)`. The measures/columns
   dataset comes from `model_to_dataset(model)`, or from `documentation.tsv` when
   `USE_TABULAR_EDITOR` (columns: Object, Name, Description, SourceColumn, Expression,
   FormatString, DataType, DisplayFolder; Object names `Model.T.<Table>`, `.C.<Col>`, `.M.`,
   `.H.`, `.P.`, `Relationship.<guid>`). Relationships (sheet columns Type, Child, Direction,
   Parent, Child Column, Parent Column, Cardinality, Active) and the table list always come
   from the model. Unused detection first drops `structurally_used_columns()`, then (with
   exact dependencies) everything any DAX references by (table, name); without Tabular Editor
   it falls back to DAX text matching (misses unqualified refs, matches text in comments,
   ignores calculated columns). Measures stay "unused" unless a visual, filter or other DAX
   uses them — listed separately as "Measures not used in this report".
   The TSV path garbles format strings starting with `"` (CSV quoting) — the .bim path doesn't.
4. Writes `<name>.xlsx` (Common + one tab per page + Pages + model sheets) and
   `<name>_data.xlsx` (pages, common, relationships, unused measures [Type column],
   dependencies [MeasureName, Dependent, Object Type, Dependent Type], model tables,
   model columns, model roles/parameters if any, model quality summary, model quality).
   Live stats fill Rows/Size/% columns and measure DataType; otherwise they stay empty.

## Known bugs / gotchas (open as of 2026-09-29)
- Never trust the Select `Name`/queryRef string for table/field — it goes stale when
  measures are renamed or moved (e.g. `_Measures.Total Sales Budget` is really
  `SalesBudgets[Total Sales Budget OC]`). Always use `resolve_field()`. The queryRef is
  only for matching projection roles / columnProperties and hierarchy columns.
- Report-level ("All Pages") filters, bookmark contents (captured state), drillthrough/
  tooltip/hidden pages, visual interactions and slicer sync groups are not in the output.
- The "User Input" UI tab appends to `Input/*.csv`, which nothing reads any more (YAML config).
  The measures-table combo (`defMeasTable`) is hidden and its value unused.
- Kept on purpose during the Step 2 refactor (output identical to before), candidates to fix:
  common sheets skip all `Type == "Column"` rows, so calculated columns (and their DAX) are
  never listed; report pages without visuals get no page sheet; the "Dependants" column lists
  what an object *depends on*; objects are listed in reverse model order (the old code
  prepended rows); text-matching fallback order is set-based (not stable across runs).
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
  registry, JSON writer done) →
  3 buttons/bookmarks/filters complete → 1b DevOps/Fabric readers → 4 HTML lineage graph →
  5 CLI + NiceGUI UI → 6 SQL/Fabric source lineage (sqlglot).

## Refactoring safety net
Output is deterministic (except the graph PNG layout). Before a refactor, copy
`output/Invoices_NEW/*.xlsx` and `output/AdventureWorks/*.xlsx` somewhere, re-run, and compare
workbooks cell by cell incl. rich-text runs and resolved styles (a small zip/XML comparer was
used for Step 2; `tests/test_documentation.py::test_run_cmd_end_to_end` covers the pipeline on
the sample data).

## Conventions
- black/ruff, line length 100, py311 target. Google-style docstrings with Args/Returns.
- New config belongs in `data/data.yaml`, not in Python constants.
- Don't commit anything in `output/` or `Input/` (both gitignored). Sample output for
  regression comparison: `output/Invoices` (older `main` code) vs `output/Invoices_NEW` (`ft_v2`).

## Branches
- `main`: legacy single-file `PB-Ixtractor.py` (has working button/bookmark logic).
- `ft_v2` (current): src-layout refactor, YAML config, JSONPath extractors.
- Remote: https://github.com/MackanT/PBIxtractor
