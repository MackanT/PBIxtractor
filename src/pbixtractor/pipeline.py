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

from .constants import DESCRIPT_TAG
from .documentation import Documentation, build_documentation
from .excel_report import write_data_workbook, write_main_workbook
from .json_report import documentation_to_dict, write_json_data
from .lineage_html import write_lineage_html
from .live_model import collect_live_statistics
from .logger import capture_logs, get_logger
from .readers import ReportReadError
from .relationship_graph import save_relationship_graph
from .report_extractor import ReportExtractor, known_functions, visual_mapper, visual_type_list
from .semantic_model import model_source, model_to_dataset, read_model
from .tabular_editor import (
    drop_redundant_table_refs,
    export_dependencies,
    export_documentation_tsv,
    find_tabular_editor,
    run_best_practice_analyzer,
)
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
    tabular_editor_analysis: bool = True  # BPA, exact dependencies, live statistics
    tabular_editor_tsv: bool = False  # use TE's TSV export (formats DAX via daxformatter.com)
    write_log_file: bool = True  # save warnings to <output_dir>/logs/

    def __post_init__(self):
        self.report_path = Path(self.report_path)
        self.model_path = Path(self.model_path)
        self.output_dir = Path(self.output_dir)
        if not self.name:
            self.name = report_name(self.report_path)


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
    path = Path(report_path)
    return path.name[: -len(".Report")] if path.name.endswith(".Report") else path.stem


def find_model_for_report(report_path: str | Path) -> Optional[Path]:
    """
    Guess the model file that belongs to a report.

    Looks for <name>.bim next to the report, then for a PBIP project's semantic model: the
    folder the report's definition.pbir points to, or <name>.SemanticModel / <name>.Dataset;
    each as model.bim or TMDL (definition/*.tmdl).

    Args:
        report_path: .pbix, .pbip or .Report folder

    Returns:
        Path to the .bim or TMDL definition folder, or None
    """
    path = Path(report_path)
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


def _tabular_editor_analysis(model, source: Path, options: ExtractionOptions, progress):
    """
    Optional, local Tabular Editor analysis: Best Practice Analyzer and exact DAX dependencies on
    the model (source: .bim file or TMDL folder, both loaded by Tabular Editor 2), plus live
    statistics if the report is open in Power BI Desktop.

    Returns:
        (bpa_violations, exact_dependencies, live_statistics); each None when unavailable
    """
    bpa_violations = exact_dependencies = live_statistics = None
    if not options.tabular_editor_analysis:
        return bpa_violations, exact_dependencies, live_statistics

    tabular_editor = find_tabular_editor()
    if tabular_editor is None:
        logger.warning(
            "Tabular Editor analysis skipped: Tabular Editor 2 not found. Add its folder to "
            "Input/TabularEditorLocations.txt or disable the Tabular Editor analysis."
        )
        return bpa_violations, exact_dependencies, live_statistics

    progress("Best Practice Analyzer", 0.2)
    try:
        bpa_violations, rule_errors = run_best_practice_analyzer(tabular_editor, source)
        if rule_errors:
            logger.warning(
                f"{len(rule_errors)} Best Practice Analyzer rule(s) could not be evaluated: "
                + "; ".join(error[:150] for error in rule_errors[:5])
            )
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as e:
        logger.warning(f"Best Practice Analyzer failed: {e}")

    progress("Exact DAX dependencies", 0.35)
    try:
        exact_dependencies = drop_redundant_table_refs(
            export_dependencies(tabular_editor, source)
        )
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as e:
        logger.warning(f"Exact dependency export failed, using DAX text matching: {e}")

    progress("Live statistics (Power BI Desktop)", 0.45)
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

    return bpa_violations, exact_dependencies, live_statistics


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
        ExtractionResult; status "warnings" when anything was logged (see result.logs)
    """
    progress = progress or (lambda step, fraction: None)
    started = time.monotonic()
    with _RUN_LOCK, capture_logs(callback=on_log) as capture:
        try:
            result = _run(options, progress)
        except ReportReadError as e:  # expected and explained, e.g. an encrypted .pbix
            logger.error(f"Extraction failed: {e}")
            result = ExtractionResult("error", f"Extraction failed: {e}")
        except Exception as e:  # report unexpected failures instead of crashing the UI/CLI
            logger.exception(f"Extraction failed: {e}")
            result = ExtractionResult("error", f"Extraction failed: {e}")
        result.logs = capture.get_logs()

    if result.ok and result.logs:
        result.status = "warnings"
        if options.write_log_file:
            log_dir = options.output_dir / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / f"log_data_{time.strftime('%H_%M_%S')}.txt"
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
    if not options.report_path.exists():
        return ExtractionResult("error", f"Report not found: {options.report_path}")

    # 1. Model: always read from the .bim (relationships, sort-by and hierarchy columns come
    #    from here, even when Tabular Editor provides the TSV)
    progress("Reading the model", 0.05)
    try:
        source = model_source(options.model_path)  # the .bim file or TMDL folder
        model = read_model(source)
    except (OSError, ValueError) as e:
        return ExtractionResult("error", f"Could not read the model {options.model_path}: {e}")

    bpa_violations, exact_dependencies, live_statistics = _tabular_editor_analysis(
        model, source, options, progress
    )

    # Measure data types are only known by a live model (the .bim usually lacks them)
    if live_statistics:
        for measure in model.all_measures:
            measure.data_type = live_statistics.measure_types.get(
                (measure.table, measure.name), measure.data_type
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

    # 2. Report
    progress("Reading the report", 0.55)
    extractor = ReportExtractor(str(options.report_path.parent), options.report_path.name)
    extractor.extract()

    # 3. Analysis
    progress("Analysing", 0.65)
    documentation = build_documentation(
        report_items=extractor.result,
        report_filters=extractor.filters,
        model=model,
        dataset=dataset,
        report_name=report_name(options.report_path),
        description_tag=options.description_tag,
        visual_mapper=visual_mapper,
        visual_types=visual_type_list,
        logger=logger,
        exact_dependencies=exact_dependencies,
        bpa_violations=bpa_violations,
        live_statistics=live_statistics,
        report=extractor.report,
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
    write_json_data(str(files["json"]), report_json)
    write_lineage_html(files["lineage"], report_json)

    return ExtractionResult(
        "success",
        f"Documentation written to {out}",
        files=files,
        documentation=documentation,
        report_json=report_json,
    )

