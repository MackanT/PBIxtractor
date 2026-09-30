"""Excel sheets describing the semantic model (tables, columns, roles, parameters, BPA)."""

from collections import Counter
from typing import Optional

from xlsxwriter.workbook import Workbook

from .live_model import LiveStatistics
from .semantic_model import DATA_TYPES, SemanticModel
from .tabular_editor import BpaViolation

# Friendly names for TMSL storage modes
STORAGE_MODES = {
    "import": "Import",
    "directQuery": "DirectQuery",
    "directLake": "Direct Lake",
    "dual": "Dual",
    "default": "Default",
}


def _yes(flag: bool) -> str:
    return "Yes" if flag else ""


def _write_table(worksheet, header_format, headers: list[tuple], rows: list[list]):
    """Write a header row plus data rows, with column widths and an autofilter.

    headers: (title, width) or (title, width, cell format) per column.
    """
    for col, (title, width, *cell_format) in enumerate(headers):
        worksheet.write(0, col, title, header_format)
        worksheet.set_column(col, col, width, *cell_format)
    for row_num, row in enumerate(rows, start=1):
        for col, value in enumerate(row):
            if value not in ("", None):
                worksheet.write(row_num, col, value)
    worksheet.autofilter(0, 0, max(len(rows), 1), len(headers) - 1)
    worksheet.freeze_panes(1, 0)


def _share(part: int, whole: int) -> Optional[float]:
    """Fraction for Excel percentage cells, or None when unknown."""
    return round(part / whole, 4) if whole else None


# Public name for other sheet writers
write_table = _write_table


def model_table_rows(model: SemanticModel, stats: Optional[LiveStatistics] = None) -> list[list]:
    """One row per table: storage mode, source, object counts and (live) rows/size."""
    rows = []
    for table in model.tables:
        size = stats.table_sizes.get(table.name) if stats else None
        modes = sorted({STORAGE_MODES.get(p.mode, p.mode) for p in table.partitions if p.mode})
        sources = [p.source_summary() for p in table.partitions]
        connectors = ", ".join(dict.fromkeys(c for c, _ in sources if c))
        objects = ", ".join(dict.fromkeys(o for _, o in sources if o))
        if table.is_calculation_group:
            connectors, objects = "Calculation group", f"{len(table.calculation_items)} items"
        rows.append(
            [
                table.name,
                ", ".join(modes),
                connectors,
                objects,
                _yes(table.is_hidden),
                sum(1 for c in table.columns if c.kind != "calculated"),
                sum(1 for c in table.columns if c.kind == "calculated"),
                len(table.measures),
                len(table.hierarchies),
                stats.table_rows.get(table.name) if stats else None,
                round(size / 1_000_000, 2) if size is not None else None,
                _share(size, stats.model_size) if size is not None else None,
                table.description,
            ]
        )
    return rows


def model_column_rows(model: SemanticModel, stats: Optional[LiveStatistics] = None) -> list[list]:
    """One row per column with its model properties and (live) distinct values/size."""
    rows = []
    for column in model.all_columns:
        live = stats.columns.get((column.table, column.name)) if stats else None
        table_size = stats.table_sizes.get(column.table, 0) if stats else 0
        sized = live if live and stats.has_sizes else None  # sizes may be unknown
        rows.append(
            [
                column.table,
                column.name,
                DATA_TYPES.get(column.data_type, column.data_type),
                "Calculated" if column.kind == "calculated" else "Data",
                column.source_column,
                _yes(column.is_hidden),
                _yes(column.is_key),
                column.data_category,
                column.summarize_by,
                column.sort_by_column,
                column.format_string,
                column.display_folder,
                live.distinct_values if live else None,
                round(sized.total_size / 1000, 1) if sized else None,
                _share(sized.total_size, table_size) if sized else None,
                column.description,
            ]
        )
    return rows


def add_model_sheets(
    workbook: Workbook,
    model: SemanticModel,
    header_format,
    sheet_name=lambda name: name,
    stats: Optional[LiveStatistics] = None,
) -> None:
    """
    Add sheets describing the semantic model to a workbook.

    Sheets: "model tables", "model columns", plus "model roles" (RLS) and
    "model parameters" (shared M expressions) when the model has any.

    Args:
        workbook: xlsxwriter workbook
        model: Model from semantic_model.read_model()
        header_format: xlsxwriter format for header cells
        sheet_name: Callable returning a valid, unique sheet name
        stats: Live statistics (rows, sizes, distinct values) if the report was open in
            Power BI Desktop; those columns stay empty otherwise
    """
    percent = workbook.add_format({"num_format": "0.0%"})
    _write_table(
        workbook.add_worksheet(sheet_name("model tables")),
        header_format,
        [
            ("Table", 30),
            ("Storage Mode", 14),
            ("Connector", 18),
            ("Source Objects", 40),
            ("Hidden", 8),
            ("Columns", 9),
            ("Calculated Columns", 11),
            ("Measures", 9),
            ("Hierarchies", 11),
            ("Rows", 12),
            ("Size (MB)", 10),
            ("% of Model", 10, percent),
            ("Description", 60),
        ],
        model_table_rows(model, stats),
    )

    _write_table(
        workbook.add_worksheet(sheet_name("model columns")),
        header_format,
        [
            ("Table", 25),
            ("Column", 30),
            ("Data Type", 11),
            ("Kind", 11),
            ("Source Column", 25),
            ("Hidden", 8),
            ("Key", 6),
            ("Data Category", 14),
            ("Summarize By", 13),
            ("Sort By", 20),
            ("Format", 15),
            ("Display Folder", 18),
            ("Distinct Values", 12),
            ("Size (KB)", 10),
            ("% of Table", 10, percent),
            ("Description", 60),
        ],
        model_column_rows(model, stats),
    )

    if model.roles:
        role_rows = [
            [role.name, role.model_permission, table, dax]
            for role in model.roles
            for table, dax in (role.table_filters.items() or [("", "")])
        ]
        _write_table(
            workbook.add_worksheet(sheet_name("model roles")),
            header_format,
            [("Role", 25), ("Permission", 12), ("Table", 25), ("RLS Filter (DAX)", 80)],
            role_rows,
        )

    if model.expressions:
        _write_table(
            workbook.add_worksheet(sheet_name("model parameters")),
            header_format,
            [("Name", 30), ("Kind", 8), ("Expression", 100), ("Description", 50)],
            [[e.name, e.kind, e.expression, e.description] for e in model.expressions],
        )


def bpa_summary_rows(violations: list[BpaViolation]) -> list[list]:
    """One row per violated rule with its number of findings (input is sorted by severity)."""
    counts = Counter(v.rule for v in violations)
    first = {}
    for violation in violations:
        first.setdefault(violation.rule, violation)
    return [
        [v.severity, v.category, v.rule, counts[rule], v.description] for rule, v in first.items()
    ]


def add_bpa_sheets(
    workbook: Workbook,
    violations: list[BpaViolation],
    header_format,
    sheet_name=lambda name: name,
) -> None:
    """
    Add Best Practice Analyzer results: a per-rule summary and the full list of findings.

    Args:
        workbook: xlsxwriter workbook
        violations: Findings from tabular_editor.run_best_practice_analyzer()
        header_format: xlsxwriter format for header cells
        sheet_name: Callable returning a valid, unique sheet name
    """
    _write_table(
        workbook.add_worksheet(sheet_name("model quality summary")),
        header_format,
        [("Severity", 10), ("Category", 18), ("Rule", 70), ("Findings", 10), ("Description", 90)],
        bpa_summary_rows(violations),
    )
    _write_table(
        workbook.add_worksheet(sheet_name("model quality")),
        header_format,
        [
            ("Severity", 10),
            ("Category", 18),
            ("Rule", 70),
            ("Object Type", 22),
            ("Object", 50),
        ],
        [[v.severity, v.category, v.rule, v.object_type, v.object_name] for v in violations],
    )
