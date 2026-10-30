"""Tests for model statistics (statistics.py): row counts, sizes, distinct values."""

import csv
import io

from pbixtractor.model_sheets import model_column_rows, model_table_rows
from pbixtractor.semantic_model import parse_model
from pbixtractor.statistics import LiveStatistics, statistics_from_rows

from .test_semantic_model import BIM


def _tsv(*rows: str) -> list[dict]:
    text = "\n".join(rows) + "\n"
    return list(csv.DictReader(io.StringIO(text), delimiter="\t", quoting=csv.QUOTE_NONE))


# Shaped like the real INFO.* output (Adventure Works)
ROWS = {
    "tables": _tsv("Table", "Sales", "Dates"),
    "measures": _tsv("Table\tMeasure\tDataType", "Sales\tTotal Amount\tDecimal"),
    "storage_tables": _tsv(
        "Table\tTableId\tRows",
        "Sales\tSales (12)\t1000",
        "Sales\tH$Sales (12)$Amount (14)\t900",
        "Dates\tDates (20)\t365",
    ),
    "storage_columns": _tsv(
        "Table\tColumn\tTableId\tColumnId\tColumnType\tDictionarySize",
        "Sales\tRowNumber-2662979B\tSales (12)\tRowNumber (13)\tBASIC_DATA\t144",
        "Sales\tAmount\tSales (12)\tAmount (14)\tBASIC_DATA\t5000",
        "Sales\tAmount\tH$Sales (12)$Amount (14)\tPOS_TO_ID\tHIERARCHY_POSITION_TO_DATAID\t0",
        "Dates\tDate Key\tDates (20)\tDate Key (21)\tBASIC_DATA\t3000",
    ),
    "segments": _tsv(
        "Table\tTableId\tColumnId\tUsedSize",
        "Sales\tSales (12)\tRowNumber (13)\t128",
        "Sales\tSales (12)\tAmount (14)\t700",
        "Sales\tSales (12)\tAmount (14)\t300",  # second segment
        "Sales\tH$Sales (12)$Amount (14)\tPOS_TO_ID\t400",
        "Sales\tU$Sales (12)$Geo (99)\tMULTI_LEVEL_ID\t50",  # user hierarchy: table only
        "Dates\tDates (20)\tDate Key (21)\t200",
    ),
    "column_statistics": _tsv(
        "Table Name\tColumn Name\tMin\tMax\tCardinality\tMax Length",
        "Sales\tAmount\t0\t99\t850\t",
        "Dates\tDate Key\t1\t365\t365\t",
    ),
}


def test_statistics_from_rows():
    stats = statistics_from_rows(ROWS)

    assert stats.tables == {"Sales", "Dates"}
    assert stats.table_rows == {"Sales": 1000, "Dates": 365}  # H$ storage tables ignored
    assert stats.measure_types == {("Sales", "Total Amount"): "Decimal"}

    amount = stats.columns[("Sales", "Amount")]
    assert (amount.dictionary_size, amount.data_size, amount.hierarchy_size) == (5000, 1000, 400)
    assert amount.total_size == 6400
    assert amount.distinct_values == 850
    assert ("Sales", "RowNumber-2662979B") not in stats.columns

    # Table size = every storage structure of the table, incl. row number and user hierarchy
    assert stats.table_sizes["Sales"] == 144 + 5000 + 128 + 700 + 300 + 400 + 50
    assert stats.model_size == stats.table_sizes["Sales"] + 3000 + 200


def test_statistics_from_rows_tolerates_missing_queries():
    stats = statistics_from_rows({"tables": ROWS["tables"]})
    assert stats.tables == {"Sales", "Dates"}
    assert stats.columns == {} and stats.table_rows == {}


def test_model_sheet_rows_with_statistics():
    model = parse_model(BIM)
    stats = statistics_from_rows(ROWS)

    tables = {row[0]: row for row in model_table_rows(model, stats)}
    # ..., Rows, Size (MB), % of Model, Description
    assert tables["Sales"][9] == 1000
    assert tables["Sales"][11] == round(stats.table_sizes["Sales"] / stats.model_size, 4)

    columns = {(row[0], row[1]): row for row in model_column_rows(model, stats)}
    # ..., Distinct Values, Size (KB), % of Table, Description
    assert columns[("Sales", "Amount")][12:14] == [850, 6.4]
    assert columns[("Sales", "Is Big")][12:15] == [None, None, None]  # no statistics

    # Without statistics the columns are simply empty
    assert model_table_rows(model)[0][9:12] == [None, None, None]


def test_moved_names_still_import_from_tabular_editor():
    """The shared types moved out of tabular_editor.py (developer-only), which re-exports them."""
    from pbixtractor import bpa, dax_parser, statistics, tabular_editor

    assert tabular_editor.BpaViolation is bpa.BpaViolation
    assert tabular_editor.SEVERITIES is bpa.SEVERITIES
    assert tabular_editor.DEFAULT_BPA_RULES == bpa.DEFAULT_BPA_RULES
    assert tabular_editor.Dependency is dax_parser.Dependency
    assert tabular_editor.drop_redundant_table_refs is dax_parser.drop_redundant_table_refs
    assert tabular_editor.LiveStatistics is statistics.LiveStatistics is LiveStatistics
    assert tabular_editor.ColumnStatistics is statistics.ColumnStatistics
    assert tabular_editor.statistics_from_rows is statistics.statistics_from_rows
