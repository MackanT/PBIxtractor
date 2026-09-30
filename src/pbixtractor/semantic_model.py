"""Read a Power BI semantic model (.bim / TMSL JSON) directly, without Tabular Editor.

The .bim holds everything Tabular Editor's TSV export contains, plus details the TSV does not
(relationship cardinality/direction/active flag, sort-by columns, hierarchy level columns,
hidden flags, partitions with their M queries, shared expressions, roles).

    model = read_model("Model.bim")
    dataset = model_to_dataset(model)   # same columns as Tabular Editor's documentation.tsv

TMDL folders (the format of newer PBIP projects) are read through tmdl.py into the same
structure, so both formats give the same SemanticModel.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

from .m_sources import NATIVE_SQL, m_sources, sql_tables
from .tmdl import find_tmdl_folder, read_tmdl_folder

# Columns of Tabular Editor's ExportProperties TSV, as consumed by run_cmd()
DATASET_COLUMNS = [
    "Object",
    "Name",
    "Description",
    "SourceColumn",
    "Expression",
    "FormatString",
    "DataType",
    "DisplayFolder",
]

# TMSL dataType -> Tabular Editor DataType spelling
DATA_TYPES = {
    "string": "String",
    "int64": "Int64",
    "double": "Double",
    "decimal": "Decimal",
    "dateTime": "DateTime",
    "boolean": "Boolean",
    "binary": "Binary",
    "variant": "Variant",
}


def _text(value) -> str:
    """TMSL stores multi-line text (expressions, descriptions) as a list of lines."""
    if value is None:
        return ""
    if isinstance(value, list):
        return "\n".join(str(line) for line in value)
    return str(value)


# ============================================================================
# Model objects
# ============================================================================


@dataclass
class Column:
    table: str
    name: str
    data_type: str = ""
    kind: str = "data"  # data | calculated | calculatedTableColumn | rowNumber
    source_column: str = ""
    expression: str = ""  # DAX for calculated columns
    description: str = ""
    format_string: str = ""
    display_folder: str = ""
    is_hidden: bool = False
    is_key: bool = False
    data_category: str = ""
    summarize_by: str = ""
    sort_by_column: str = ""


@dataclass
class Measure:
    table: str
    name: str
    expression: str = ""
    description: str = ""
    format_string: str = ""
    format_string_expression: str = ""  # dynamic format string (DAX)
    display_folder: str = ""
    data_type: str = ""
    is_hidden: bool = False


@dataclass
class Level:
    name: str
    column: str


@dataclass
class Hierarchy:
    table: str
    name: str
    levels: list[Level] = field(default_factory=list)
    description: str = ""
    is_hidden: bool = False


@dataclass
class Partition:
    table: str
    name: str
    mode: str = ""  # import | directQuery | directLake | dual | default
    source_type: str = ""  # m | calculated | entity | query | policyRange
    expression: str = ""  # M query or DAX (calculated tables)
    entity_name: str = ""  # Direct Lake: source table/view
    schema_name: str = ""
    expression_source: str = ""  # Direct Lake: shared expression holding the connection
    # Other M queries of the model by name (shared expressions, other tables' queries), so
    # references to them can be followed; set by parse_model()
    queries: dict[str, str] = field(default_factory=dict, repr=False, compare=False)

    def source_summary(self) -> tuple[str, str]:
        """
        Summarise where this partition's data comes from.

        Returns:
            (connector, source objects), e.g. ("Sql.Database", "dbo.DimCustomer"),
            ("Direct Lake", "gold.dim_date") or ("DAX", "calculated table").
            Parsed from the query text (see m_sources.py), so unusual queries may give
            partial results.
        """
        if self.source_type == "calculated":
            return "DAX", "calculated table"
        if self.source_type == "entity":
            name = (
                f"{self.schema_name}.{self.entity_name}" if self.schema_name else self.entity_name
            )
            return "Direct Lake", name
        if self.source_type == "query":  # legacy DirectQuery partition: plain SQL
            return "SQL query", ", ".join(sql_tables(self.expression) or [NATIVE_SQL])
        if self.source_type != "m":
            return self.source_type, ""
        connector, objects = m_sources(self.expression, self.queries, self.table)
        return connector, ", ".join(objects)


@dataclass
class Relationship:
    name: str
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    from_cardinality: str = "many"
    to_cardinality: str = "one"
    cross_filtering: str = "oneDirection"  # oneDirection | bothDirections | automatic
    is_active: bool = True

    @property
    def direction_label(self) -> str:
        return "Two Way" if self.cross_filtering == "bothDirections" else "One Way"

    @property
    def cardinality_label(self) -> str:
        return f"{self.from_cardinality.capitalize()} to {self.to_cardinality.capitalize()}"

    @property
    def te_name(self) -> str:
        """Relationship name as Tabular Editor shows it: 'A'[x] --> 'B'[y]."""
        arrow = "<-->" if self.cross_filtering == "bothDirections" else "-->"
        return (
            f"'{self.from_table}'[{self.from_column}] {arrow} '{self.to_table}'[{self.to_column}]"
        )


@dataclass
class Table:
    name: str
    description: str = ""
    is_hidden: bool = False
    data_category: str = ""
    columns: list[Column] = field(default_factory=list)
    measures: list[Measure] = field(default_factory=list)
    hierarchies: list[Hierarchy] = field(default_factory=list)
    partitions: list[Partition] = field(default_factory=list)
    calculation_items: list[Measure] = field(default_factory=list)  # calculation groups

    @property
    def is_calculation_group(self) -> bool:
        return bool(self.calculation_items)


@dataclass
class SharedExpression:
    name: str
    kind: str = ""
    expression: str = ""
    description: str = ""


@dataclass
class Role:
    name: str
    model_permission: str = ""
    table_filters: dict[str, str] = field(default_factory=dict)  # table -> RLS DAX filter


@dataclass
class SemanticModel:
    name: str = ""
    compatibility_level: Optional[int] = None
    culture: str = ""
    default_mode: str = ""  # storage mode of partitions with mode "default" (empty: import)
    tables: list[Table] = field(default_factory=list)
    relationships: list[Relationship] = field(default_factory=list)
    expressions: list[SharedExpression] = field(default_factory=list)
    roles: list[Role] = field(default_factory=list)
    perspectives: list[str] = field(default_factory=list)

    @property
    def all_columns(self) -> list[Column]:
        return [column for table in self.tables for column in table.columns]

    def partition_mode(self, partition: "Partition") -> str:
        """The partition's storage mode, with "default"/empty resolved via the model."""
        if partition.mode in ("", "default"):
            return self.default_mode or "import"
        return partition.mode

    @property
    def all_measures(self) -> list[Measure]:
        return [measure for table in self.tables for measure in table.measures]

    @property
    def all_hierarchies(self) -> list[Hierarchy]:
        return [hierarchy for table in self.tables for hierarchy in table.hierarchies]

    def structurally_used_columns(self) -> set[tuple[str, str]]:
        """
        Columns the model itself depends on, even if no visual or DAX uses them.

        Relationship keys, sort-by columns and hierarchy level columns: removing any of
        these breaks the model, so they must not be reported as unused.

        Returns:
            Set of (table, column) tuples
        """
        used = set()
        for rel in self.relationships:
            used.add((rel.from_table, rel.from_column))
            used.add((rel.to_table, rel.to_column))
        for column in self.all_columns:
            if column.sort_by_column:
                used.add((column.table, column.sort_by_column))
        for hierarchy in self.all_hierarchies:
            for level in hierarchy.levels:
                used.add((hierarchy.table, level.column))
        return used


# ============================================================================
# Reading
# ============================================================================


def model_source(path: str | Path) -> Path:
    """
    The concrete model a path points to: a .bim file or a TMDL definition folder.

    Args:
        path: .bim file; <name>.SemanticModel / <name>.Dataset folder (model.bim or TMDL
            definition/); a TMDL definition folder; or a .tmdl file in it (e.g. model.tmdl)

    Returns:
        The .bim file or the folder holding the .tmdl files (Tabular Editor accepts both)
    """
    path = Path(path)
    if path.is_file() and path.suffix.lower() != ".tmdl":
        return path
    if path.is_dir() and (path / "model.bim").is_file():
        return path / "model.bim"
    folder = find_tmdl_folder(path)
    if folder is not None:
        return folder
    raise FileNotFoundError(f"No model.bim or TMDL model (definition/*.tmdl) found in {path}")


def read_model(path: str | Path) -> SemanticModel:
    """
    Read a semantic model from a .bim file or a TMDL folder.

    Args:
        path: See model_source()

    Returns:
        SemanticModel
    """
    source = model_source(path)
    if source.is_dir():
        return parse_model(read_tmdl_folder(source))
    database = json.loads(source.read_bytes().decode("utf-8-sig"))
    return parse_model(database)


def parse_model(database: dict) -> SemanticModel:
    """
    Parse a TMSL database document (the content of a .bim file).

    Args:
        database: Parsed .bim JSON ({"name", "compatibilityLevel", "model": {...}})

    Returns:
        SemanticModel
    """
    model = database.get("model", {})

    tables = [_parse_table(table) for table in model.get("tables", [])]

    relationships = [
        Relationship(
            name=rel.get("name", ""),
            from_table=rel.get("fromTable", ""),
            from_column=rel.get("fromColumn", ""),
            to_table=rel.get("toTable", ""),
            to_column=rel.get("toColumn", ""),
            from_cardinality=rel.get("fromCardinality", "many"),
            to_cardinality=rel.get("toCardinality", "one"),
            cross_filtering=rel.get("crossFilteringBehavior", "oneDirection"),
            is_active=rel.get("isActive", True),
        )
        for rel in model.get("relationships", [])
    ]

    expressions = [
        SharedExpression(
            name=expr.get("name", ""),
            kind=expr.get("kind", ""),
            expression=_text(expr.get("expression")),
            description=_text(expr.get("description")),
        )
        for expr in model.get("expressions", [])
    ]

    roles = [
        Role(
            name=role.get("name", ""),
            model_permission=role.get("modelPermission", ""),
            table_filters={
                permission.get("name", ""): _text(permission.get("filterExpression"))
                for permission in role.get("tablePermissions", [])
                if permission.get("filterExpression")
            },
        )
        for role in model.get("roles", [])
    ]

    # Queries a partition's M can refer to: tables with one M partition (their query has the
    # table's name) and shared expressions (these win on a name clash, as in Power Query)
    queries = {
        table.name: table.partitions[0].expression
        for table in tables
        if len(table.partitions) == 1 and table.partitions[0].source_type == "m"
    }
    queries.update({e.name: e.expression for e in expressions if e.kind in ("m", "")})
    for table in tables:
        for partition in table.partitions:
            partition.queries = queries

    return SemanticModel(
        name=database.get("name", ""),
        compatibility_level=database.get("compatibilityLevel"),
        culture=model.get("culture", ""),
        default_mode=model.get("defaultMode", ""),
        tables=tables,
        relationships=relationships,
        expressions=expressions,
        roles=roles,
        perspectives=[p.get("name", "") for p in model.get("perspectives", [])],
    )


def _parse_table(table: dict) -> Table:
    name = table.get("name", "")

    columns = [
        Column(
            table=name,
            name=col.get("name", ""),
            data_type=col.get("dataType", ""),
            kind=col.get("type", "data"),
            source_column=col.get("sourceColumn", ""),
            expression=_text(col.get("expression")),
            description=_text(col.get("description")),
            format_string=col.get("formatString", ""),
            display_folder=col.get("displayFolder", ""),
            is_hidden=col.get("isHidden", False),
            is_key=col.get("isKey", False),
            data_category=col.get("dataCategory", ""),
            summarize_by=col.get("summarizeBy", ""),
            sort_by_column=col.get("sortByColumn", ""),
        )
        for col in table.get("columns", [])
        if col.get("type") != "rowNumber"
    ]

    measures = [_parse_measure(name, measure) for measure in table.get("measures", [])]

    hierarchies = [
        Hierarchy(
            table=name,
            name=hierarchy.get("name", ""),
            # Array order is not the hierarchy order: "ordinal" is (TMDL lists by ordinal)
            levels=[
                Level(name=level.get("name", ""), column=level.get("column", ""))
                for _, level in sorted(
                    enumerate(hierarchy.get("levels", [])),
                    key=lambda item: (item[1].get("ordinal", item[0]), item[0]),
                )
            ],
            description=_text(hierarchy.get("description")),
            is_hidden=hierarchy.get("isHidden", False),
        )
        for hierarchy in table.get("hierarchies", [])
    ]

    partitions = []
    for partition in table.get("partitions", []):
        source = partition.get("source", {})
        partitions.append(
            Partition(
                table=name,
                name=partition.get("name", ""),
                mode=partition.get("mode", ""),
                source_type=source.get("type", ""),
                expression=_text(source.get("expression") or source.get("query")),
                entity_name=source.get("entityName", ""),
                schema_name=source.get("schemaName", ""),
                expression_source=source.get("expressionSource", ""),
            )
        )

    calculation_items = [
        _parse_measure(name, item)
        for item in table.get("calculationGroup", {}).get("calculationItems", [])
    ]

    return Table(
        name=name,
        description=_text(table.get("description")),
        is_hidden=table.get("isHidden", False),
        data_category=table.get("dataCategory", ""),
        columns=columns,
        measures=measures,
        hierarchies=hierarchies,
        partitions=partitions,
        calculation_items=calculation_items,
    )


def _parse_measure(table: str, measure: dict) -> Measure:
    return Measure(
        table=table,
        name=measure.get("name", ""),
        expression=_text(measure.get("expression")),
        description=_text(measure.get("description")),
        format_string=measure.get("formatString", ""),
        format_string_expression=_text(
            (measure.get("formatStringDefinition") or {}).get("expression")
        ),
        display_folder=measure.get("displayFolder", ""),
        data_type=measure.get("dataType", ""),
        is_hidden=measure.get("isHidden", False),
    )


# ============================================================================
# Tabular Editor compatible dataset
# ============================================================================


def model_to_dataset(model: SemanticModel) -> pd.DataFrame:
    """
    Build the same table Tabular Editor's TabularScript.cs exports to documentation.tsv.

    Object names follow Tabular Editor: Model.T.<table>, .C.<column>, .H.<hierarchy>[.<level>],
    .M.<measure>, .P.<partition>, and Relationship.<name>. Differences from the TSV: DAX is not
    re-formatted (TE runs FormatDax, which calls daxformatter.com), and measure data types are
    only known if stored in the .bim ("Unknown" otherwise, as in TE without a live model).

    Args:
        model: Model from read_model()

    Returns:
        DataFrame with DATASET_COLUMNS
    """
    rows = []

    def add(obj: str, name: str, **values):
        rows.append({"Object": obj, "Name": name, **values})

    for table in model.tables:
        add(f"Model.T.{table.name}", table.name, Description=table.description)

    for column in model.all_columns:
        add(
            f"Model.T.{column.table}.C.{column.name}",
            column.name,
            Description=column.description,
            SourceColumn=column.source_column,
            Expression=column.expression,
            FormatString=column.format_string,
            # Missing for calculated columns with an inferred type in TMDL (isDataTypeInferred)
            DataType=DATA_TYPES.get(column.data_type, column.data_type) or "Unknown",
            DisplayFolder=column.display_folder,
        )

    for hierarchy in model.all_hierarchies:
        prefix = f"Model.T.{hierarchy.table}.H.{hierarchy.name}"
        add(prefix, hierarchy.name, Description=hierarchy.description)
        for level in hierarchy.levels:
            add(f"{prefix}.{level.name}", level.name)

    for measure in model.all_measures:
        add(
            f"Model.T.{measure.table}.M.{measure.name}",
            measure.name,
            Description=measure.description,
            Expression=measure.expression,
            FormatString=measure.format_string,
            DataType=DATA_TYPES.get(measure.data_type, measure.data_type) or "Unknown",
            DisplayFolder=measure.display_folder,
        )

    for rel in model.relationships:
        add(f"Relationship.{rel.name}", rel.te_name)

    for table in model.tables:
        for partition in table.partitions:
            add(
                f"Model.T.{table.name}.P.{partition.name}",
                partition.name,
                Expression=partition.expression,
            )

    dataset = pd.DataFrame(rows, columns=DATASET_COLUMNS)
    # Match pd.read_csv on the TSV: empty cells are NaN
    return dataset.replace("", float("nan"))
