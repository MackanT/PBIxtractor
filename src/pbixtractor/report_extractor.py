"""Report extraction: visual fields, buttons and filters of a report, as rows.

    extractor = ReportExtractor("C:/Reports", "Sales.pbix")
    extractor.extract()
    extractor.result   # [page, visual type, visual id, table, field, display name, role]
    extractor.filters  # [page, item, filter type, table, field, operator, value]
    extractor.report   # readers.ReportDefinition (pages, bookmarks, interactions, ...)
"""

import os
import sys
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
    print(f"Error loading YAML configuration file: {YAML_FILE}")
    print(f"Exception: {e}")
    sys.exit(1)

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
        self.logger = get_logger("pbixtractor")
        self.page_extractor = PageExtractor(config=CONFIG, logger=self.logger)

    def add_item(
        self,
        page: str,
        visual_type: str,
        item_name: str,
        table_name: str,
        val_name: str,
        disp_name: str,
        data_type: str,
    ) -> None:
        """Store an extracted item row (REPORT_COLUMNS order)."""
        self.result.append(
            [page, visual_type, item_name, table_name, val_name, disp_name, data_type]
        )

    def add_filter(
        self,
        page: str,
        item_name: str,
        filter_type: str,
        table_name: str,
        val_name: str,
        operator: str,
        value: str,
    ) -> None:
        """Store an extracted filter row."""
        self.filters.append([page, item_name, filter_type, table_name, val_name, operator, value])

    def extract(self, prefix: str = "") -> None:
        """
        Extract all data from the Power BI report.

        1. Reads the report (legacy Layout or PBIR; .pbix, .pbip or .Report folder)
           into a normalised ReportDefinition (see readers.py)
        2. Extracts report-level filters and each page using PageExtractor

        Args:
            prefix: Put in front of every page and bookmark display name, e.g. "Sales › "
                (used when several reports are documented together). Applied before the pages
                are extracted, so button targets carry it too.
        """
        report = self.report = read_report(os.path.join(self.path, self.name), self.logger)
        self.logger.debug(f"Read {self.name} ({report.format} format)")
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
        name = path.name[: -len(".Report")] if path.name.endswith(".Report") else path.stem
        extractor = ReportExtractor(str(path.parent), path.name)
        extractor.extract(prefix=name + PREFIX_SEPARATOR)
        items += extractor.result
        filters += extractor.filters

        report = extractor.report
        id_prefix = f"{index}:"
        for page in report.pages:
            page.name = id_prefix + page.name
        for bookmark in report.bookmark_details:
            bookmark.name = id_prefix + bookmark.name
            bookmark.page = id_prefix + bookmark.page if bookmark.page else ""
        bookmarks = {id_prefix + key: value for key, value in report.bookmarks.items()}
        if merged is None:
            merged = ReportDefinition(format=report.format)
        merged.pages += report.pages
        merged.filters += report.filters
        merged.bookmarks.update(bookmarks)
        merged.bookmark_details += report.bookmark_details
    return items, filters, merged
