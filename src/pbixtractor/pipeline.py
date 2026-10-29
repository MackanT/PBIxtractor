"""The documentation pipeline as one function with explicit inputs (no globals).

    result = run_extraction(ExtractionOptions(report_path="Sales.pbix", model_path="Sales.bim",
                                              output_dir="output/Sales"))
    result.status   # "success" | "warnings" | "error"
    result.files    # {"workbook": Path, "data_workbook": ..., "json": ..., "lineage": ..., ...}

Used by the command line (cli.py) and the web UI (web_ui.py).
"""

import json
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

from . import bpa_custom
from .azure_auth import ApiError
from .bpa import run_bpa
from .catalog import add_to_catalog
from .constants import DESCRIPT_TAG
from .dax_parser import model_dependencies
from .documentation import Documentation, build_documentation
from .excel_report import write_data_workbook, write_main_workbook
from .json_report import documentation_to_dict, write_json_data
from .lineage_html import write_lineage_html
from .live_model import collect_live_statistics
from .logger import capture_logs, get_logger
from .pbix_model import has_embedded_model, read_pbix_model
from .readers import ReportReadError
from .relationship_graph import save_relationship_graph
from .report_extractor import (
    ReportExtractor,
    extract_reports,
    known_functions,
    report_label,
    visual_mapper,
    visual_type_list,
    with_report,
)
from .semantic_model import (
    model_source,
    model_to_dataset,
    parse_model,
    read_model,
    write_documentation_tsv,
)
from .service_stats import ServiceModel, read_service_statistics, service_model_for_report
from .tabular_editor import export_documentation_tsv, find_tabular_editor
from .utils import is_excel_open_with_file

logger = get_logger("pbixtractor")

# One extraction at a time: runs share the logger and Tabular Editor/Excel files
_RUN_LOCK = threading.Lock()

REPORT_SUFFIXES = (".pbix", ".pbip")


@dataclass
class ExtractionOptions:
    """Everything one documentation run needs."""

    report_path: Path  # .pbix, .pbip or <name>.Report folder
    model_path: Path  # .bim, TMDL folder / model.tmdl, or <name>.SemanticModel folder
    output_dir: Path  # all output files go here
    name: str = ""  # base name of the output files (default: report file name)
    description_tag: str = DESCRIPT_TAG  # delimiter of descriptions embedded in DAX
    tabular_editor_analysis: bool = True  # live statistics from Desktop (the BPA always runs)
    tabular_editor_tsv: bool = False  # use TE's TSV export (formats DAX via daxformatter.com)
    write_log_file: bool = True  # save warnings to <output_dir>/logs/
    # Model mode: more reports on the same model, documented together with report_path (pages
    # become "<report> › <page>"; "unused" then means unused by all of them)
    extra_reports: list[Path] = field(default_factory=list)
    # Model mode: reports (or workspaces) on the model that could not be read - logged as
    # warnings of the run and written to the JSON/catalog, since "unused" is then incomplete
    not_included: list[str] = field(default_factory=list)
    # Statistics from the Power BI service (row counts, sizes, measure types) for a model
    # downloaded from Fabric; None = look for <report>.fabric_source.json next to the report
    service_model: Optional[ServiceModel] = None
    service_statistics: bool = True
    # Also add the result to this catalog folder (catalog.py): one searchable page for many models
    catalog_dir: Optional[Path] = None
    catalog_rebuild: bool = True  # rebuild its pages now (a batch rebuilds once, at the end)
    # Custom model quality rules (bpa_custom.py); None = ~/.pbixtractor/bpa_rules.yaml if it
    # exists. A given file that is missing is a warning.
    bpa_rules: Optional[Path] = None

    def __post_init__(self):
        self.report_path = Path(self.report_path)
        self.model_path = Path(self.model_path)
        self.output_dir = Path(self.output_dir)
        self.extra_reports = [Path(p) for p in self.extra_reports]
        self.catalog_dir = Path(self.catalog_dir) if self.catalog_dir else None
        self.bpa_rules = Path(self.bpa_rules) if self.bpa_rules else None
        if not self.name:
            self.name = report_name(self.report_path)

    @property
    def report_paths(self) -> list[Path]:
        return [self.report_path] + self.extra_reports


@dataclass
class ExtractionResult:
    """Outcome of one run."""

    status: str  # "success" | "warnings" | "error"
    message: str
    files: dict[str, Path] = field(default_factory=dict)
    logs: str = ""
    documentation: Optional[Documentation] = None
    report_json: Optional[dict] = None
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status != "error"


def report_name(report_path: str | Path) -> str:
    """Report name from its path: "Sales.pbix" / "Sales.pbip" / "Sales.Report" -> "Sales"."""
    return report_label(report_path)


def model_name(model_path: str | Path) -> str:
    """Model name from its path: "Sales.bim", "Sales.SemanticModel[/definition[/model.tmdl]]"."""
    path = Path(model_path)
    if path.suffix.lower() == ".tmdl":
        path = path.parent
    if path.name.lower() == "definition":
        path = path.parent
    for suffix in (".SemanticModel", ".Dataset", ".bim"):
        if path.name.endswith(suffix):
            return path.name[: -len(suffix)]
    return path.stem


def find_model_for_report(report_path: str | Path) -> Optional[Path]:
    """
    Guess the model file that belongs to a report.

    A .pbix with its own model is its own model: the one the report was saved with (a .bim
    exported separately can be older or newer). Otherwise looks for <name>.bim next to the
    report, then for a PBIP project's semantic model: the folder the report's definition.pbir
    points to, or <name>.SemanticModel / <name>.Dataset; each as model.bim or TMDL
    (definition/*.tmdl). A live-connected .pbix has no model here (pbix_model.live_connection).

    Args:
        report_path: .pbix, .pbip or .Report folder

    Returns:
        Path to the .pbix itself, the .bim or the TMDL definition folder, or None
    """
    path = Path(report_path)
    if has_embedded_model(path):
        return path
    name = report_name(path)
    if path.with_name(f"{name}.bim").is_file():
        return path.with_name(f"{name}.bim")

    folders = []
    pbir = path.with_name(f"{name}.Report") / "definition.pbir"
    try:
        reference = json.loads(pbir.read_bytes().decode("utf-8-sig"))["datasetReference"]["byPath"]
        folders.append((pbir.parent / reference["path"]).resolve())
    except (OSError, ValueError, KeyError, TypeError):
        pass  # no PBIP report folder, or the model is not referenced by path
    folders += [path.with_name(f"{name}.SemanticModel"), path.with_name(f"{name}.Dataset")]
    for folder in folders:
        try:
            return model_source(folder)
        except FileNotFoundError:
            continue
    return None


def _dax_dependencies(model, progress):
    """
    Exact DAX dependencies from the Python analysis (dax_parser), on every run.

    Returns:
        The dependencies, or None (text matching) if the analysis crashed - a bug, logged as
        a warning, never a failed run
    """
    progress("Exact DAX dependencies", 0.15)
    try:
        return model_dependencies(model)
    except Exception as e:  # a bug in the analysis must not fail the run
        logger.warning(f"DAX dependency analysis failed, using DAX text matching: {e!r}")
        return None


def _live_statistics(model, options: ExtractionOptions, progress):
    """
    Live statistics if the report is open in Power BI Desktop (read with Tabular Editor 2,
    until ticket 08 removes Desktop detection).

    Returns:
        LiveStatistics, or None when unavailable
    """
    if not options.tabular_editor_analysis:
        return None

    tabular_editor = find_tabular_editor()
    if tabular_editor is None:
        # A note in the run log, not a warning: the normal case on Linux and without Desktop
        logger.info(
            "Live statistics from Power BI Desktop skipped: Tabular Editor 2 not found. Add its "
            "folder to Input/TabularEditorLocations.txt or disable the Tabular Editor analysis."
        )
        return None

    progress("Live statistics (Power BI Desktop)", 0.45)
    live_statistics = None
    try:
        live_statistics, live_note = collect_live_statistics(
            tabular_editor, options.report_path, {table.name for table in model.tables}
        )
        # Debug only: not having the report open in Desktop is the normal case
        logger.debug(f"Live statistics: {live_note}")
        if live_statistics and live_statistics.errors:
            logger.warning(
                "Some live statistics are missing (Power BI Desktop too old?): "
                + "; ".join(f"{k}: {v[:150]}" for k, v in live_statistics.errors.items())
            )
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as e:
        logger.warning(f"Reading live statistics from Power BI Desktop failed: {e}")
    return live_statistics


def _custom_rules(path: Optional[Path]) -> bpa_custom.RuleFile:
    """
    The custom model quality rules of a run: the given file, else the user's file if it exists.
    Each problem of the file is one warning (the built-in rules run regardless).
    """
    if path is None:
        path = bpa_custom.USER_RULES_PATH
        if not path.is_file():
            return bpa_custom.RuleFile()
    elif not path.is_file():
        logger.warning(f"Custom model quality rule file not found: {path}")
        return bpa_custom.RuleFile()
    rule_file = bpa_custom.load_rule_file(path)
    for error in rule_file.errors:
        logger.warning(f"Custom model quality rule {error}")
    logger.debug(
        f"Custom model quality rules from {path}: {len(rule_file.rules)} rule(s), "
        f"{len(rule_file.disabled)} built-in rule(s) switched off"
    )
    return rule_file


def _best_practice_analyzer(
    model, statistics, progress, rules_file: Optional[Path] = None
) -> tuple[Optional[list], list]:
    """
    The Python Best Practice Analyzer (bpa.py) on every run, with or without Tabular Editor,
    plus the custom rules (bpa_custom.py) of rules_file / the user's rule file.

    Returns:
        (violations, [(rule ID, reason)] for the rules not checked); violations is None when
        the analysis itself crashed (a bug: logged as a warning, never a failed run)
    """
    progress("Best Practice Analyzer", 0.48)
    try:
        rules = _custom_rules(rules_file)
        violations, not_checked = run_bpa(
            model, statistics, custom=rules.rules, disabled=rules.disabled
        )
    except Exception as e:  # a bug in the engine must not fail the run
        logger.warning(f"Best Practice Analyzer failed: {e!r}")
        return None, []
    if not_checked:
        logger.debug(
            f"{len(not_checked)} Best Practice Analyzer rule(s) not checked: "
            + "; ".join(f"{rule} ({reason})" for rule, reason in not_checked)
        )
    return violations, not_checked


def _service_statistics(model, options: ExtractionOptions, progress):
    """Row counts and distinct values from the Power BI service for a Fabric model (else None)."""
    if not options.service_statistics:
        return None
    service = options.service_model or service_model_for_report(options.report_path)
    if service is None:
        return None
    # Counting a DirectQuery table would query the source database
    countable = [
        table.name
        for table in model.tables
        if not table.is_calculation_group
        and not any(model.partition_mode(p) == "directQuery" for p in table.partitions)
    ]
    progress("Row counts from the Power BI service", 0.47)
    try:
        stats = read_service_statistics(service, countable)
    except (ApiError, OSError) as e:
        logger.warning(f"Reading statistics from the Power BI service failed: {e}")
        return None
    for query, error in stats.errors.items():
        logger.warning(f"Power BI service statistics ({query}) failed: {error[:400]}")
    return stats


def run_extraction(
    options: ExtractionOptions,
    progress: Optional[Callable[[str, float], None]] = None,
    on_log: Optional[Callable[[str, str], None]] = None,
) -> ExtractionResult:
    """
    Document a report: read model and report, analyse, and write all output files.

    Args:
        options: What to document and where to write it
        progress: Called with (step description, fraction 0..1) as the run advances
        on_log: Called with (level name, message) for every log record of this run

    Returns:
        ExtractionResult; status "warnings" when a warning or error was logged (see result.logs,
            which also holds INFO notes)
    """
    progress = progress or (lambda step, fraction: None)
    started = time.monotonic()
    levels: list[str] = []

    def forward(level: str, message: str) -> None:
        levels.append(level)
        if on_log is not None:
            on_log(level, message)

    with _RUN_LOCK, capture_logs(callback=forward) as capture:
        try:
            result = _run(options, progress)
        except ReportReadError as e:  # expected and explained, e.g. an encrypted .pbix
            logger.error(f"Extraction failed: {e}")
            result = ExtractionResult("error", f"Extraction failed: {e}")
        except Exception as e:  # report unexpected failures instead of crashing the UI/CLI
            logger.exception(f"Extraction failed: {e}")
            result = ExtractionResult("error", f"Extraction failed: {e}")
        result.logs = capture.get_logs()

    # INFO notes stay in result.logs only; a warning or worse makes the run "warnings"
    if result.ok and any(level not in ("DEBUG", "INFO") for level in levels):
        result.status = "warnings"
        if options.write_log_file:
            log_dir = options.output_dir / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            # Date and time, so runs on other days (or in the same second) do not overwrite
            stamp = time.strftime("%Y-%m-%d_%H_%M_%S")
            log_file = log_dir / f"log_data_{stamp}.txt"
            counter = 2
            while log_file.exists():
                log_file = log_dir / f"log_data_{stamp}_{counter}.txt"
                counter += 1
            log_file.write_text(result.logs, encoding="utf-8")
            result.files["log"] = log_file
    result.seconds = round(time.monotonic() - started, 1)
    progress("Done" if result.ok else "Failed", 1.0)
    return result


def _run(options: ExtractionOptions, progress) -> ExtractionResult:
    out = options.output_dir
    out.mkdir(parents=True, exist_ok=True)
    files = {
        "workbook": out / f"{options.name}.xlsx",
        "data_workbook": out / f"{options.name}_data.xlsx",
        "json": out / f"{options.name}.json",
        "lineage": out / f"{options.name}_lineage.html",
        "graph": out / f"{options.name}_Relationships.png",
    }
    for key in ("workbook", "data_workbook"):
        if is_excel_open_with_file(str(files[key])):
            return ExtractionResult("error", f"Please close {files[key].name} in Excel first.")
    for path in options.report_paths:
        if not path.exists():
            return ExtractionResult("error", f"Report not found: {path}")

    # 1. Model: always read from the .bim (relationships, sort-by and hierarchy columns come
    #    from here, even when Tabular Editor provides the TSV)
    for missing in options.not_included:
        logger.warning(f"Not included in this documentation ('unused' may be incomplete): {missing}")

    progress("Reading the model", 0.05)
    pbix_statistics = None
    try:
        source = model_source(options.model_path)  # the .bim file, TMDL folder or .pbix
        if source.suffix.lower() == ".pbix":
            pbix = read_pbix_model(source)
            model, pbix_statistics = parse_model(pbix.database), pbix.statistics
            # Tabular Editor reads a .bim, not a .pbix: hand it the model as one (also an
            # output file; "_model" so it never overwrites a .bim next to the report)
            source = files["model"] = out / f"{options.name}_model.bim"
            source.write_text(
                json.dumps(pbix.database, indent=2, ensure_ascii=False), encoding="utf-8"
            )
        else:
            model = read_model(source)
    except (OSError, ValueError) as e:
        return ExtractionResult("error", f"Could not read the model {options.model_path}: {e}")

    exact_dependencies = _dax_dependencies(model, progress)
    live_statistics = _live_statistics(model, options, progress)
    service_statistics = _service_statistics(model, options, progress)
    if service_statistics is not None:
        # The model as published wins: a downloaded report is never open in Desktop by its
        # own path, so Desktop statistics here come from a name-similarity match - possibly a
        # different (e.g. dev) copy of the model
        if live_statistics is not None:
            logger.debug("Using Power BI service statistics instead of a matching Desktop model")
        live_statistics = service_statistics
    if live_statistics is None and pbix_statistics is not None:
        # Not open in Desktop: the statistics saved in the .pbix (as of its last refresh)
        live_statistics = pbix_statistics

    # Measure data types are only known by a live model (the .bim usually lacks them)
    if live_statistics:
        for measure in model.all_measures:
            measure.data_type = live_statistics.measure_types.get(
                (measure.table, measure.name), measure.data_type
            )

    bpa_violations, bpa_not_checked = _best_practice_analyzer(
        model, live_statistics, progress, options.bpa_rules
    )

    if options.tabular_editor_tsv:
        progress("Tabular Editor TSV export", 0.5)
        tabular_editor = find_tabular_editor()
        if tabular_editor is None:
            return ExtractionResult("error", "Tabular Editor 2 not found (needed for the TSV export).")
        tsv_path = export_documentation_tsv(tabular_editor, source, out / "documentation.tsv")
        dataset = pd.read_csv(tsv_path, sep="\t", header=0)
        files["tsv"] = tsv_path
    else:
        dataset = model_to_dataset(model)
        # Always left in the output folder, as the old program did (measures and calculated
        # columns with formatted DAX)
        try:
            files["tsv"] = write_documentation_tsv(model, out / "documentation.tsv")
        except OSError as e:
            logger.warning(f"Could not write {out / 'documentation.tsv'}: {e}")

    # 2. Report(s)
    if options.extra_reports:
        progress(f"Reading {len(options.report_paths)} reports", 0.55)
        report_items, report_filters, report_definition = extract_reports(options.report_paths)
    else:
        progress("Reading the report", 0.55)
        extractor = ReportExtractor(str(options.report_path.parent), options.report_path.name)
        extractor.extract()
        report_items = extractor.result
        report_filters = with_report(extractor.filters, extractor.report_name)
        report_definition = extractor.report

    # 3. Analysis
    progress("Analysing", 0.65)
    documentation = build_documentation(
        report_items=report_items,
        report_filters=report_filters,
        model=model,
        dataset=dataset,
        report_name=options.name if options.extra_reports else report_name(options.report_path),
        description_tag=options.description_tag,
        visual_mapper=visual_mapper,
        visual_types=visual_type_list,
        logger=logger,
        exact_dependencies=exact_dependencies,
        bpa_violations=bpa_violations,
        live_statistics=live_statistics,
        report=report_definition,
        bpa_not_checked=bpa_not_checked,
    )

    # 4. Output
    progress("Writing the relationship graph", 0.7)
    save_relationship_graph(documentation.relations, str(files["graph"]))
    progress("Writing the Excel workbooks", 0.8)
    write_main_workbook(str(files["workbook"]), documentation, str(files["graph"]), known_functions)
    write_data_workbook(
        str(files["data_workbook"]), documentation, str(files["graph"]), known_functions
    )
    progress("Writing JSON and the lineage viewer", 0.95)
    report_json = documentation_to_dict(documentation)
    if options.not_included:
        report_json["not_included"] = list(options.not_included)
    write_json_data(str(files["json"]), report_json)
    write_lineage_html(files["lineage"], report_json)

    if options.catalog_dir:
        progress("Adding to the catalog", 0.98)
        try:
            files["catalog"] = add_to_catalog(
                options.catalog_dir,
                report_json,
                options.report_path,
                options.model_path,
                options.name if options.extra_reports else model_name(options.model_path),
                files,
                rebuild=options.catalog_rebuild,
            )
        except (OSError, ValueError, KeyError) as e:
            logger.warning(f"Adding to the catalog {options.catalog_dir} failed: {e}")

    return ExtractionResult(
        "success",
        f"Documentation written to {out}",
        files=files,
        documentation=documentation,
        report_json=report_json,
    )

