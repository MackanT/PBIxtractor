"""Model statistics: row counts, distinct values and sizes per table and column.

They come from the Power BI service (service_stats.py) or from the .pbix itself
(pbix_model.statistics_from_metadata); nothing here queries a model.

    stats = statistics_from_rows({"storage_tables": [...], "column_statistics": [...]})
    stats.table_rows["Sales"], stats.columns[("Sales", "Amount")].distinct_values
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ColumnStatistics:
    table: str
    column: str
    distinct_values: Optional[int] = None
    dictionary_size: int = 0
    data_size: int = 0
    hierarchy_size: int = 0

    @property
    def total_size(self) -> int:
        return self.dictionary_size + self.data_size + self.hierarchy_size


@dataclass
class LiveStatistics:
    tables: set[str] = field(default_factory=set)
    table_rows: dict[str, int] = field(default_factory=dict)
    table_sizes: dict[str, int] = field(default_factory=dict)  # all storage incl. hierarchies
    columns: dict[tuple[str, str], ColumnStatistics] = field(default_factory=dict)
    measure_types: dict[tuple[str, str], str] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)  # query name -> error message

    @property
    def model_size(self) -> int:
        return sum(self.table_sizes.values())

    @property
    def has_sizes(self) -> bool:
        """False when only row counts / distinct values are known (e.g. from executeQueries)."""
        return bool(self.table_sizes)


def _int(value: str) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def statistics_from_rows(rows: dict[str, list[dict]]) -> LiveStatistics:
    """
    Aggregate raw query rows (from the Power BI service).

    Column size follows VertiPaq Analyzer: dictionary + data segments in the table's main
    storage table + the column's attribute hierarchy (H$<table id>$<column id>).
    Table size is all storage of the table (also user hierarchies U$ and relationships R$).

    Args:
        rows: Query name -> rows; names and columns as the INFO.* functions give them:
            tables (Table), measures (Table, Measure, DataType), storage_tables,
            storage_columns, segments, column_statistics (Table Name, Column Name, Cardinality)

    Returns:
        LiveStatistics
    """
    stats = LiveStatistics()
    stats.tables = {row["Table"] for row in rows.get("tables", [])}

    for row in rows.get("measures", []):
        stats.measure_types[(row["Table"], row["Measure"])] = row["DataType"]

    # Main storage table per model table = the one without an H$/U$/R$ prefix
    main_table_id = {}
    for row in rows.get("storage_tables", []):
        if not row["TableId"].startswith(("H$", "U$", "R$")):
            main_table_id[row["Table"]] = row["TableId"]
            stats.table_rows[row["Table"]] = _int(row["Rows"])

    # Data columns: (storage table id, storage column id) -> model column
    storage_key = {}
    for row in rows.get("storage_columns", []):
        dictionary_size = _int(row["DictionarySize"])
        # Every dictionary counts towards the table, including the hidden row-number column
        stats.table_sizes[row["Table"]] = stats.table_sizes.get(row["Table"], 0) + dictionary_size
        if row["ColumnType"] != "BASIC_DATA" or row["Column"].startswith("RowNumber-"):
            continue
        column = stats.columns.setdefault(
            (row["Table"], row["Column"]), ColumnStatistics(row["Table"], row["Column"])
        )
        column.dictionary_size += dictionary_size
        storage_key[(row["TableId"], row["ColumnId"])] = column

    hierarchy_owner = {
        f"H${table_id}${column_id}": col for (table_id, column_id), col in storage_key.items()
    }

    for row in rows.get("segments", []):
        size = _int(row["UsedSize"])
        stats.table_sizes[row["Table"]] = stats.table_sizes.get(row["Table"], 0) + size
        if (row["TableId"], row["ColumnId"]) in storage_key:
            storage_key[(row["TableId"], row["ColumnId"])].data_size += size
        elif row["TableId"] in hierarchy_owner:
            hierarchy_owner[row["TableId"]].hierarchy_size += size

    for row in rows.get("column_statistics", []):
        key = (row.get("Table Name", ""), row.get("Column Name", ""))
        if key[1].startswith("RowNumber-"):
            continue
        column = stats.columns.setdefault(key, ColumnStatistics(*key))
        column.distinct_values = _int(row.get("Cardinality"))

    return stats
