"""Tests for live statistics (row counts, sizes, distinct values) from Power BI Desktop."""

from pathlib import Path

from pbixtractor.live_model import (
    LocalInstance,
    build_statistics,
    find_instance_for_report,
    tables_match,
)
from pbixtractor.model_sheets import model_column_rows, model_table_rows
from pbixtractor.semantic_model import parse_model

from .test_semantic_model import BIM


def _tsv(*rows: str) -> str:
    return "\n".join(rows) + "\n"


# Shaped like the real INFO.* output captured from Power BI Desktop (Adventure Works)
FILES = {
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


def test_build_statistics():
    stats = build_statistics(FILES)

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


def test_build_statistics_tolerates_missing_queries():
    stats = build_statistics({"tables": FILES["tables"]})
    assert stats.tables == {"Sales", "Dates"}
    assert stats.columns == {} and stats.table_rows == {}


def test_tables_match():
    assert tables_match({"Sales", "Dates"}, {"Sales", "Dates"})
    assert tables_match({"A", "B", "C", "D", "E"}, {"A", "B", "C", "D", "E", "F"})  # 5/6
    assert not tables_match({"Sales", "Dates"}, {"Customers", "Dates"})
    assert not tables_match(set(), {"Sales"})


def test_find_instance_for_report(tmp_path):
    report = tmp_path / "Report.pbix"
    instances = [
        LocalInstance(1111, tmp_path / "Other.pbix", 1),
        LocalInstance(2222, Path(str(report).upper()), 2),  # paths compare case-insensitively
        LocalInstance(3333, None, 3),  # opened via File > Open: report unknown
    ]
    exact, unknown = find_instance_for_report(report, instances)
    assert exact.port == 2222
    assert [i.port for i in unknown] == [3333]

    exact, unknown = find_instance_for_report(tmp_path / "Missing.pbix", instances)
    assert exact is None and [i.port for i in unknown] == [3333]


def test_model_sheet_rows_with_statistics():
    model = parse_model(BIM)
    stats = build_statistics(FILES)

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
