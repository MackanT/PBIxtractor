"""Report extraction: visual fields, buttons and filters of a report, as rows.

    extractor = ReportExtractor("C:/Reports", "Sales.pbix")
    extractor.extract()
    extractor.result   # [page, visual type, visual id, table, field, display name, role]
    extractor.filters  # [page, item, filter type, table, field, operator, value]
    extractor.report   # readers.ReportDefinition (pages, bookmarks, interactions, ...)

Every page and bookmark records the report it belongs to (PageDefinition.report/.title), and
with_report() adds the report to filter rows, so several reports can be documented together.
"""

import os
from pathlib import Path

import yaml

from .config import load_config
from .data import YAML_FILE
from .extractors import PageExtractor, ReportContext
from .logger import get_logger
from .readers import ReportDefinition, read_report

# Configuration from data/data.yaml
try:
    CONFIG = load_config()
except (OSError, ValueError, KeyError, yaml.YAMLError) as e:
    # Raised, not sys.exit(): importing must not end the process (the web UI imports this)
    raise RuntimeError(f"Error loading the configuration file {YAML_FILE}: {e}") from e



def report_label(file_name: str | Path) -> str:
    """Report name from its file/folder: "Sales.pbix" / "Sales.pbip" / "Sales.Report" -> "Sales"."""
    path = Path(file_name)
    return path.name[: -len(".Report")] if path.name.endswith(".Report") else path.stem


def with_report(filter_rows: list[list], report: str) -> list[list]:
    """Filter rows with their report as an 8th column (an "All Pages" filter has no page that
    says which report it belongs to)."""
    return [list(row) + [report] for row in filter_rows]


visual_mapper = CONFIG.visual_mapper
visual_type_list = sorted(CONFIG.supported_visual_types)
known_functions = CONFIG.function_names


class ReportExtractor:
    """Extracts visual and filter data from a Power BI report (.pbix, .pbip or .Report)."""

    def __init__(self, path: str, name: str):
        """
        Initialize report extractor.

        Args:
            path: Directory containing the report
            name: File (or folder) name of the report
        """
        self.path = path
        self.name = name
        self.result = []
        self.filters = []
        self.report = None  # readers.ReportDefinition after extract()
        self.report_name = report_label(name)
        self.logger = get_logger("pbixtractor")
        self.page_extractor = PageExtractor(config=CONFIG, logger=self.logger)

    def extract(self, prefix: str = "", report_name: str = "") -> None:
        """
        Extract all data from the Power BI report.

        1. Reads the report (legacy Layout or PBIR; .pbix, .pbip or .Report folder)
           into a normalised ReportDefinition (see readers.py)
        2. Extracts report-level filters and each page using PageExtractor

        Args:
            prefix: Put in front of every page and bookmark display name, e.g. "Sales › "
                (used when several reports are documented together). Applied before the pages
                are extracted, so button targets carry it too.
            report_name: The report's name (default: from its file name)
        """
        self.report_name = report_name or self.report_name
        report = self.report = read_report(os.path.join(self.path, self.name), self.logger)
        self.logger.debug(f"Read {self.name} ({report.format} format)")
        # Which report each page and bookmark belongs to; the title before any prefix
        report.reports = {self.report_name: self.name}
        for page in report.pages:
            page.report, page.title = self.report_name, page.display_name
        for bookmark in report.bookmark_details:
            bookmark.report = self.report_name
        if prefix:
            for page in report.pages:
                page.display_name = prefix + page.display_name
            for bookmark in report.bookmark_details:
                bookmark.display_name = prefix + bookmark.display_name
            report.bookmarks = {key: prefix + name for key, name in report.bookmarks.items()}
        context = ReportContext.from_report(report)

        # Report-level filters (apply to all pages)
        for filter_obj in self.page_extractor.filter_extractor.extract_filters(
            report.filters, "", "All Pages"
        ):
            self.filters.append(filter_obj.to_list())

        for page in report.pages:
            items, filters = self.page_extractor.extract(page, context)
            self.result.extend(item.to_list() for item in items)
            self.filters.extend(filter_obj.to_list() for filter_obj in filters)


PREFIX_SEPARATOR = " › "


def extract_reports(paths: list[Path]) -> tuple[list, list, ReportDefinition]:
    """
    Extract several reports on one semantic model as if they were one report.

    Page and bookmark display names get "<report> › " in front, so rows, sheets and lineage
    nodes stay apart; page and bookmark ids get "<n>:" in front, because two reports can use
    the same ids. Report-level ("All Pages") filters keep their empty page.

    Args:
        paths: .pbix / .pbip / .Report folder of each report

    Returns:
        (item rows, filter rows, merged ReportDefinition) - as ReportExtractor provides them
    """
    items, filters = [], []
    merged = None
    for index, path in enumerate(paths):
        path = Path(path)
        name = report_label(path)
        extractor = ReportExtractor(str(path.parent), path.name)
        extractor.extract(prefix=name + PREFIX_SEPARATOR, report_name=name)
        items += extractor.result
        filters += with_report(extractor.filters, name)

        report = extractor.report
        id_prefix = f"{index}:"
        for page in report.pages:
            page.name = id_prefix + page.name
        for bookmark in report.bookmark_details:
            bookmark.name = id_prefix + bookmark.name
            bookmark.page = id_prefix + bookmark.page if bookmark.page else ""
            for captured in bookmark.filters:
                captured.page = id_prefix + captured.page if captured.page else ""
        bookmarks = {id_prefix + key: value for key, value in report.bookmarks.items()}
        if merged is None:
            merged = ReportDefinition(format=report.format)
        merged.pages += report.pages
        merged.filters += report.filters
        merged.bookmarks.update(bookmarks)
        merged.bookmark_details += report.bookmark_details
        merged.reports.update(report.reports)
    return items, filters, merged
