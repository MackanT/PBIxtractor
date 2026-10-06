"""The semantic model inside a .pbix, read without Power BI Desktop, Tabular Editor or a .bim.

A .pbix saved with its own model (import / composite) holds it as "DataModel": a compressed
Analysis Services backup. Its metadata.sqlitedb is the model's full definition - one SQLite table
per TOM object type (Table, Column, Measure, Partition, Relationship, ...). PBIXRay (MIT) unpacks
it; read_pbix_model() turns those tables into the same TMSL document a .bim file holds, so
semantic_model.parse_model() reads both alike, and Tabular Editor can be given it as a .bim.

A live-connected .pbix ("thin" report) has no model of its own: live_connection() says which
published semantic model it uses, so it can be downloaded from the service instead.

    if has_embedded_model("Sales.pbix"):
        pbix = read_pbix_model("Sales.pbix")   # .database (TMSL dict), .statistics
    reference = live_connection("Thin.pbix")   # LiveConnection(model_id, workspace) or None
"""

import json
import logging
import re
import sqlite3
import urllib.parse
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .live_model import ColumnStatistics, LiveStatistics

logger = logging.getLogger("pbixtractor")

# Compatibility level written to a generated .bim (the metadata does not store it; Power BI
# Desktop models are 1550+). Only Tabular Editor reads it.
COMPATIBILITY_LEVEL = 1567

# TOM enumerations as stored in metadata.sqlitedb -> their TMSL (.bim) names
DATA_TYPES = {1: "automatic", 2: "string", 6: "int64", 8: "double", 9: "dateTime", 10: "decimal",
              11: "boolean", 17: "binary", 19: "unknown", 20: "variant"}
COLUMN_TYPES = {1: "data", 2: "calculated", 3: "rowNumber", 4: "calculatedTableColumn"}
SUMMARIZE_BY = {1: "default", 2: "none", 3: "sum", 4: "min", 5: "max", 6: "count", 7: "average",
                8: "distinctCount"}
PARTITION_TYPES = {1: "query", 2: "calculated", 3: "none", 4: "m", 5: "entity", 6: "policyRange",
                   7: "calculationGroup", 8: "inferred", 9: "parquet"}
MODES = {0: "import", 1: "directQuery", 2: "default", 3: "push", 4: "dual", 5: "directLake"}
CARDINALITIES = {1: "one", 2: "many"}
CROSS_FILTERING = {1: "oneDirection", 2: "bothDirections", 3: "automatic"}
SECURITY_FILTERING = {1: "oneDirection", 2: "bothDirections", 3: "none"}
MODEL_PERMISSIONS = {1: "none", 2: "read", 3: "readRefresh", 4: "refresh", 5: "administrator"}
EXPRESSION_KINDS = {0: "m", 1: "dax"}
# Annotation.ObjectType of the objects that carry annotations in a .bim (TOM ObjectType)
OBJECT_TYPES = {"model": 1, "table": 3, "column": 4, "partition": 6, "relationship": 7,
                "measure": 8, "hierarchy": 9, "role": 34, "expression": 41}
# Measure data types the way Power BI Desktop / Tabular Editor name them (LiveStatistics)
MEASURE_TYPE_NAMES = {2: "String", 6: "Int64", 8: "Double", 9: "DateTime", 10: "Decimal",
                      11: "Boolean", 17: "Binary", 19: "Unknown", 20: "Variant"}


@dataclass
class PbixModel:
    """The model of a .pbix: its TMSL document and the statistics stored with the data."""

    database: dict
    statistics: Optional[LiveStatistics] = None


@dataclass
class LiveConnection:
    """The published semantic model a live-connected report uses."""

    model_id: str
    workspace: Optional[str] = None  # workspace name, when the connection string has it


def has_embedded_model(path: str | Path) -> bool:
    """A .pbix with its own model (reads the zip listing only, no decompression)."""
    path = Path(path)
    if path.suffix.lower() != ".pbix" or not path.is_file():
        return False
    try:
        with zipfile.ZipFile(path) as archive:
            return "DataModel" in archive.namelist()
    except (OSError, zipfile.BadZipFile):
        return False


def live_connection(path: str | Path) -> Optional[LiveConnection]:
    """The published model a live-connected .pbix uses (its Connections file), or None."""
    try:
        with zipfile.ZipFile(path) as archive:
            data = json.loads(archive.read("Connections").decode("utf-8-sig"))
    except (OSError, KeyError, ValueError, zipfile.BadZipFile):
        return None
    for connection in data.get("Connections") or []:
        if str(connection.get("ConnectionType", "")).lower() != "pbiservicelive":
            continue
        text = connection.get("ConnectionString") or ""
        model_id = connection.get("PbiModelDatabaseName")
        match = re.search(r"semanticmodelid=([0-9a-fA-F-]{36})", text, re.IGNORECASE)
        if match:
            model_id = match.group(1)
        if not model_id:
            artifacts = data.get("RemoteArtifacts") or [{}]
            model_id = artifacts[0].get("DatasetId")
        workspace = re.search(r"powerbi://[^;]*/myorg/([^;\"]+)", text, re.IGNORECASE)
        if model_id:
            return LiveConnection(
                model_id, urllib.parse.unquote(workspace.group(1)).strip() if workspace else None
            )
    return None


def read_pbix_model(path: str | Path) -> PbixModel:
    """
    Read the model inside a .pbix.

    Args:
        path: The .pbix (with an embedded model: has_embedded_model())

    Returns:
        PbixModel (statistics None if they could not be read)

    Raises:
        ValueError: The file has no readable model (live connection, encrypted, damaged)
    """
    try:
        from pbixray.loader import DataModelLoader  # noqa: PLC0415 - heavy, only for .pbix
        from pbixray.utils import get_data_slice  # noqa: PLC0415
    except ImportError as e:  # pragma: no cover - a declared dependency
        raise ValueError(f"Reading a .pbix model needs the pbixray package: {e}") from e
    path = Path(path)
    try:
        data_model = DataModelLoader(str(path)).data_model
        metadata = get_data_slice(data_model, "metadata.sqlitedb")
    except Exception as e:  # noqa: BLE001 - PBIXRay raises many kinds for unreadable files
        raise ValueError(f"Could not read the model inside {path.name}: {e}") from e
    db = sqlite3.connect(":memory:")
    try:
        db.deserialize(metadata)
        database = database_from_metadata(db, name=path.stem)
        statistics = None
        try:
            statistics = statistics_from_metadata(db, _storage_sizes(data_model))
        except Exception as e:  # noqa: BLE001 - statistics are a bonus, never fail the model
            logger.debug(f"No statistics from {path.name}: {e}")
        return PbixModel(database, statistics)
    finally:
        db.close()


def _storage_sizes(data_model) -> dict[tuple[str, str], tuple[int, int, int]]:
    """(table, column) -> (dictionary, data, hierarchy) bytes, as PBIXRay measures them."""
    from pbixray.meta.metadata import Metadata  # noqa: PLC0415

    stats = Metadata(data_model).stats
    return {
        (row.TableName, row.ColumnName): (int(row.Dictionary), int(row.DataSize), int(row.HashIndex))
        for row in stats.itertuples()
    }


# ============================================================================
# metadata.sqlitedb -> TMSL
# ============================================================================


def _internal(row: dict) -> bool:
    """An engine-internal object (SystemFlags bit 1: H$/R$/U$ storage tables and their
    partitions). Bit 2 marks calculated tables - DATATABLE, field parameters - which are real."""
    return bool((row.get("SystemFlags") or 0) & 1)


def _rows(db: sqlite3.Connection, table: str) -> list[dict]:
    """All rows of a metadata table as dicts ([] if the table does not exist in this version)."""
    try:
        cursor = db.execute(f'SELECT * FROM "{table}"')
    except sqlite3.OperationalError:
        return []
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _set(target: dict, key: str, value) -> None:
    """Set a TMSL property only when it says something (like a .bim leaves defaults out)."""
    if value not in (None, "", 0, False):
        target[key] = value


def database_from_metadata(db: sqlite3.Connection, name: str = "Model") -> dict:
    """
    The TMSL database document (the content of a .bim) for a model's metadata.sqlitedb.

    Args:
        db: Connection to the metadata database
        name: Database name (the report's)

    Returns:
        {"name", "compatibilityLevel", "model": {...}} - see semantic_model.parse_model()
    """
    model_row = (_rows(db, "Model") or [{}])[0]
    tables = {row["ID"]: row for row in _rows(db, "Table") if not _internal(row)}
    columns = {row["ID"]: row for row in _rows(db, "Column") if row.get("TableID") in tables}
    column_names = {cid: row.get("ExplicitName") or row.get("InferredName") or "" for cid, row in columns.items()}
    format_strings = {row["ID"]: row.get("Expression") for row in _rows(db, "FormatStringDefinition")}
    expressions = {row["ID"]: row for row in _rows(db, "Expression")}
    data_sources = {row["ID"]: row for row in _rows(db, "DataSource")}
    # Annotations matter beyond show: Tabular Editor's Best Practice Analyzer reads its "ignore
    # rule" settings from them (BestPracticeAnalyzer_IgnoreRules)
    annotations: dict[tuple, list] = {}
    for row in _rows(db, "Annotation"):
        annotations.setdefault((row.get("ObjectType"), row.get("ObjectID")), []).append(
            {"name": row.get("Name") or "", "value": row.get("Value") or ""}
        )

    def annotate(target: dict, kind: str, object_id) -> dict:
        found = annotations.get((OBJECT_TYPES[kind], object_id))
        if found:
            target["annotations"] = found
        return target

    by_table: dict[int, dict] = {
        tid: {key: [] for key in ("columns", "measures", "hierarchies", "partitions")}
        for tid in tables
    }

    for cid, row in columns.items():
        kind = COLUMN_TYPES.get(row.get("Type"), "data")
        if kind == "rowNumber":
            continue  # implicit in every table
        column = {"name": column_names[cid]}
        data_type = row.get("ExplicitDataType")
        if data_type in (None, 1):  # automatic: the engine inferred it
            data_type = row.get("InferredDataType")
            if kind != "data":
                column["isDataTypeInferred"] = True
        _set(column, "dataType", DATA_TYPES.get(data_type) if data_type not in (None, 19) else None)
        if kind != "data":
            column["type"] = kind
        if kind in ("data", "calculatedTableColumn"):
            _set(column, "sourceColumn", row.get("SourceColumn"))
        if kind == "calculated":
            _set(column, "expression", row.get("Expression"))
        _set(column, "formatString", row.get("FormatString"))
        _set(column, "isHidden", bool(row.get("IsHidden")))
        _set(column, "isKey", bool(row.get("IsKey")))
        _set(column, "dataCategory", row.get("DataCategory"))
        _set(column, "displayFolder", row.get("DisplayFolder"))
        _set(column, "description", row.get("Description"))
        if row.get("IsAvailableInMDX") == 0:
            column["isAvailableInMdx"] = False
        summarize = SUMMARIZE_BY.get(row.get("SummarizeBy"))
        if summarize and summarize != "default":
            column["summarizeBy"] = summarize
        if row.get("SortByColumnID") in column_names:
            column["sortByColumn"] = column_names[row["SortByColumnID"]]
        by_table[row["TableID"]]["columns"].append(annotate(column, "column", cid))

    for row in _rows(db, "Measure"):
        if row.get("TableID") not in by_table:
            continue
        measure = {"name": row.get("Name") or "", "expression": row.get("Expression") or ""}
        _set(measure, "formatString", row.get("FormatString"))
        _set(measure, "displayFolder", row.get("DisplayFolder"))
        _set(measure, "description", row.get("Description"))
        _set(measure, "isHidden", bool(row.get("IsHidden")))
        if format_strings.get(row.get("FormatStringDefinitionID")):
            measure["formatStringDefinition"] = {
                "expression": format_strings[row["FormatStringDefinitionID"]]
            }
        by_table[row["TableID"]]["measures"].append(annotate(measure, "measure", row["ID"]))

    hierarchies = {}
    for row in _rows(db, "Hierarchy"):
        if row.get("TableID") not in by_table:
            continue
        hierarchy = {"name": row.get("Name") or "", "levels": []}
        _set(hierarchy, "isHidden", bool(row.get("IsHidden")))
        _set(hierarchy, "displayFolder", row.get("DisplayFolder"))
        _set(hierarchy, "description", row.get("Description"))
        hierarchies[row["ID"]] = annotate(hierarchy, "hierarchy", row["ID"])
        by_table[row["TableID"]]["hierarchies"].append(hierarchy)
    for row in sorted(_rows(db, "Level"), key=lambda r: r.get("Ordinal") or 0):
        if row.get("HierarchyID") in hierarchies:
            hierarchies[row["HierarchyID"]]["levels"].append(
                {
                    "name": row.get("Name") or "",
                    "ordinal": row.get("Ordinal") or 0,
                    "column": column_names.get(row.get("ColumnID"), ""),
                }
            )

    for row in _rows(db, "Partition"):
        if row.get("TableID") not in by_table or _internal(row):
            continue
        source_type = PARTITION_TYPES.get(row.get("Type"), "m")
        if source_type == "none":
            continue
        source = {"type": source_type}
        definition = row.get("QueryDefinition") or ""
        if source_type in ("m", "calculated"):
            source["expression"] = definition
        elif source_type == "query":
            source["query"] = definition
            data_source = data_sources.get(row.get("DataSourceID")) or {}
            _set(source, "dataSource", data_source.get("Name"))
        elif source_type == "entity":
            source["entityName"] = definition
            _set(source, "schemaName", row.get("SchemaName"))
            expression = expressions.get(row.get("ExpressionSourceID")) or {}
            _set(source, "expressionSource", expression.get("Name"))
        partition = {"name": row.get("Name") or "", "source": source}
        mode = MODES.get(row.get("Mode"))
        if mode and mode != "default":
            partition["mode"] = mode
        by_table[row["TableID"]]["partitions"].append(annotate(partition, "partition", row["ID"]))

    calculation_groups = {row["ID"]: row for row in _rows(db, "CalculationGroup")}
    calculation_items: dict[int, list] = {}
    for row in sorted(_rows(db, "CalculationItem"), key=lambda r: r.get("Ordinal") or 0):
        item = {"name": row.get("Name") or "", "expression": row.get("Expression") or ""}
        _set(item, "ordinal", row.get("Ordinal"))
        _set(item, "description", row.get("Description"))
        if format_strings.get(row.get("FormatStringDefinitionID")):
            item["formatStringDefinition"] = {
                "expression": format_strings[row["FormatStringDefinitionID"]]
            }
        calculation_items.setdefault(row.get("CalculationGroupID"), []).append(item)

    model_tables = []
    for tid, row in tables.items():
        table = {"name": row.get("Name") or ""}
        _set(table, "description", row.get("Description"))
        _set(table, "isHidden", bool(row.get("IsHidden")))
        _set(table, "isPrivate", bool(row.get("IsPrivate")))
        _set(table, "showAsVariationsOnly", bool(row.get("ShowAsVariationsOnly")))
        _set(table, "dataCategory", row.get("DataCategory"))
        group = calculation_groups.get(row.get("CalculationGroupID"))
        if group is not None:
            table["calculationGroup"] = {"calculationItems": calculation_items.get(group["ID"], [])}
            _set(table["calculationGroup"], "precedence", group.get("Precedence"))
        for key, values in by_table[tid].items():
            if values:
                table[key] = values
        model_tables.append(annotate(table, "table", tid))

    table_names = {tid: row.get("Name") or "" for tid, row in tables.items()}
    relationships = []
    for row in _rows(db, "Relationship"):
        if row.get("FromTableID") not in table_names or row.get("ToTableID") not in table_names:
            continue
        relationship = {
            "name": row.get("Name") or "",
            "fromTable": table_names[row["FromTableID"]],
            "fromColumn": column_names.get(row.get("FromColumnID"), ""),
            "toTable": table_names[row["ToTableID"]],
            "toColumn": column_names.get(row.get("ToColumnID"), ""),
        }
        if row.get("IsActive") == 0:
            relationship["isActive"] = False
        if CARDINALITIES.get(row.get("FromCardinality"), "many") != "many":
            relationship["fromCardinality"] = CARDINALITIES[row["FromCardinality"]]
        if CARDINALITIES.get(row.get("ToCardinality"), "one") != "one":
            relationship["toCardinality"] = CARDINALITIES[row["ToCardinality"]]
        cross = CROSS_FILTERING.get(row.get("CrossFilteringBehavior"), "oneDirection")
        if cross != "oneDirection":
            relationship["crossFilteringBehavior"] = cross
        security = SECURITY_FILTERING.get(row.get("SecurityFilteringBehavior"), "oneDirection")
        if security != "oneDirection":
            relationship["securityFilteringBehavior"] = security
        if row.get("JoinOnDateBehavior") == 2:
            relationship["joinOnDateBehavior"] = "datePartOnly"
        relationships.append(annotate(relationship, "relationship", row["ID"]))

    permissions: dict[int, list] = {}
    for row in _rows(db, "TablePermission"):
        if row.get("TableID") in table_names:
            permission = {"name": table_names[row["TableID"]]}
            _set(permission, "filterExpression", row.get("FilterExpression"))
            permissions.setdefault(row.get("RoleID"), []).append(permission)
    roles = []
    for row in _rows(db, "Role"):
        role = {
            "name": row.get("Name") or "",
            "modelPermission": MODEL_PERMISSIONS.get(row.get("ModelPermission"), "read"),
        }
        _set(role, "description", row.get("Description"))
        if permissions.get(row["ID"]):
            role["tablePermissions"] = permissions[row["ID"]]
        roles.append(annotate(role, "role", row["ID"]))

    shared_expressions = []
    for row in expressions.values():
        expression = {
            "name": row.get("Name") or "",
            "kind": EXPRESSION_KINDS.get(row.get("Kind"), "m"),
            "expression": row.get("Expression") or "",
        }
        _set(expression, "description", row.get("Description"))
        shared_expressions.append(annotate(expression, "expression", row["ID"]))

    model = {"culture": model_row.get("Culture") or "", "tables": model_tables}
    default_mode = MODES.get(model_row.get("DefaultMode"))
    if default_mode and default_mode != "import":
        model["defaultMode"] = default_mode
    annotate(model, "model", model_row.get("ID"))
    for key, values in (
        ("relationships", relationships),
        ("roles", roles),
        ("expressions", shared_expressions),
        ("perspectives", [{"name": r.get("Name") or ""} for r in _rows(db, "Perspective")]),
    ):
        if values:
            model[key] = values
    return {"name": name, "compatibilityLevel": COMPATIBILITY_LEVEL, "model": model}


def statistics_from_metadata(
    db: sqlite3.Connection, sizes: Optional[dict[tuple[str, str], tuple[int, int, int]]] = None
) -> LiveStatistics:
    """
    Row counts, distinct values, sizes and measure types as stored in the .pbix (as of the last
    refresh saved in the file).

    Rows come from each table's hidden row-number column. Distinct values only for columns with
    a dictionary (hash encoding): for value-encoded columns VertiPaq stores a value range there,
    not a count. Sizes per column: dictionary + data + hierarchy, as PBIXRay measures them.
    """
    stats = LiveStatistics()
    tables = {row["ID"]: row.get("Name") or "" for row in _rows(db, "Table") if not _internal(row)}
    stats.tables = set(tables.values())
    storage = {row["ID"]: row for row in _rows(db, "ColumnStorage")}
    dictionaries = {row.get("ColumnStorageID"): row for row in _rows(db, "DictionaryStorage")}
    for row in _rows(db, "Column"):
        table = tables.get(row.get("TableID"))
        if table is None:
            continue
        name = row.get("ExplicitName") or row.get("InferredName") or ""
        column_storage = storage.get(row.get("ColumnStorageID")) or {}
        if COLUMN_TYPES.get(row.get("Type")) == "rowNumber":
            if column_storage.get("Statistics_RowCount") is not None:
                stats.table_rows[table] = int(column_storage["Statistics_RowCount"])
            continue
        column = ColumnStatistics(table, name)
        dictionary, data, hierarchy = (sizes or {}).get((table, name), (0, 0, 0))
        column.dictionary_size, column.data_size, column.hierarchy_size = dictionary, data, hierarchy
        # DictionaryStorage.Type 1 = a hash dictionary (DistinctStates is a count); 2 = value
        # encoding (DistinctStates is a value range: "Invoice Key" 757 393 on 729 479 rows)
        encoding = (dictionaries.get(column_storage.get("ID")) or {}).get("Type")
        has_dictionary = encoding == 1 if encoding is not None else dictionary > 0
        if has_dictionary and column_storage.get("Statistics_DistinctStates") is not None:
            column.distinct_values = int(column_storage["Statistics_DistinctStates"])
        stats.columns[(table, name)] = column
    if sizes:
        for (table, _), (dictionary, data, hierarchy) in sizes.items():
            if table in stats.tables:
                stats.table_sizes[table] = stats.table_sizes.get(table, 0) + dictionary + data + hierarchy
    for row in _rows(db, "Measure"):
        table = tables.get(row.get("TableID"))
        type_name = MEASURE_TYPE_NAMES.get(row.get("DataType"))
        if table is not None and type_name:
            stats.measure_types[(table, row.get("Name") or "")] = type_name
    return stats
