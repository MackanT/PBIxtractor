"""Best Practice Analyzer in Python: Microsoft's BPA rules checked on a SemanticModel.

Rule metadata (ID, name, category, description, severity, scope, compatibility level) comes from
data/BPARules.json. Each rule's check is ported by hand from its Tabular Editor (Dynamic LINQ)
Expression - a generic interpreter of those expressions is out of scope. Findings have the shape
Tabular Editor's analyzer gives (tabular_editor.BpaViolation), so the "Model quality" sheets and
column read them unchanged.

    violations, not_checked = run_bpa(model, statistics)
    violations, not_checked = run_bpa(model, statistics, custom=rules, disabled=ids)

Custom rules (bpa_custom.py, the user's rule file) run next to the built-in ones, and built-in
rules can be switched off by id.

Every rule of the bundled file has a check here. The DAX rules use the Python DAX tokenizer and
dax_parser.model_dependencies (Tabular Editor's DependsOn / ReferencedBy); their regexes run on the
expression text as in Tabular Editor. A rule without a check (one from another rules file), or one
that needs statistics the run does not have, is returned in not_checked with the reason - "not
checked" is never "passed".
"""

import json
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

from .bpa_custom import SCOPES as CUSTOM_SCOPES
from .bpa_custom import CustomRule, breaks
from .dax_parser import model_dependencies, model_index, reference_occurrences
from .live_model import LiveStatistics
from .semantic_model import DATA_TYPES, Column, Measure, Relationship, SemanticModel, Table
from .tabular_editor import DEFAULT_BPA_RULES, SEVERITIES, BpaViolation

__all__ = [
    "BpaRule",
    "BpaViolation",
    "CustomRule",
    "DEFAULT_BPA_RULES",
    "load_rules",
    "rule_by_id",
    "run_bpa",
]

logger = logging.getLogger("pbixtractor")

IGNORE_ANNOTATION = "BestPracticeAnalyzer_IgnoreRules"

# Reasons for a rule in not_checked
NEEDS_STATISTICS = "needs statistics"
NOT_OFFLINE = "not supported offline"

# Rules that inspect DAX (Expression, Tokenize, DependsOn, ReferencedBy or a regex on DAX)
DAX_RULES = {
    "REDUCE_USAGE_OF_CALCULATED_COLUMNS_THAT_USE_THE_RELATED_FUNCTION",
    "LIMIT_ROW_LEVEL_SECURITY_(RLS)_LOGIC",
    "MEASURES_USING_TIME_INTELLIGENCE_AND_MODEL_IS_USING_DIRECT_QUERY",
    "CHECK_IF_DYNAMIC_ROW_LEVEL_SECURITY_(RLS)_IS_NECESSARY",
    "DAX_COLUMNS_FULLY_QUALIFIED",
    "DAX_MEASURES_UNQUALIFIED",
    "AVOID_DUPLICATE_MEASURES",
    "USE_THE_TREATAS_FUNCTION_INSTEAD_OF_INTERSECT",
    "USE_THE_DIVIDE_FUNCTION_FOR_DIVISION",
    "AVOID_USING_THE_IFERROR_FUNCTION",
    "MEASURES_SHOULD_NOT_BE_DIRECT_REFERENCES_OF_OTHER_MEASURES",
    "FILTER_COLUMN_VALUES",
    "FILTER_MEASURE_VALUES_BY_COLUMNS",
    "INACTIVE_RELATIONSHIPS_THAT_ARE_NEVER_ACTIVATED",
    "AVOID_USING_'1-(X/Y)'_SYNTAX",
    "EVALUATEANDLOG_SHOULD_NOT_BE_USED_IN_PRODUCTION_MODELS",
    "EXPRESSION_RELIANT_OBJECTS_MUST_HAVE_AN_EXPRESSION",
    "AVOID_THE_USERELATIONSHIP_FUNCTION_AND_RLS_AGAINST_THE_SAME_TABLE",
    "UNNECESSARY_COLUMNS",
    "UNNECESSARY_MEASURES",
    "HIDE_FACT_TABLE_COLUMNS",
}

_SEVERITY_ORDER = {"High": 0, "Medium": 1, "Low": 2}


# ============================================================================
# Rules
# ============================================================================


@dataclass(frozen=True)
class BpaRule:
    """One rule's metadata from the rules file."""

    id: str
    name: str
    category: str = ""
    description: str = ""
    severity: str = ""  # Low | Medium | High
    scope: tuple[str, ...] = ()  # Table, DataColumn, Measure, Model, ...
    compatibility_level: int = 0


@lru_cache(maxsize=8)
def _load_rules(path: str) -> tuple[BpaRule, ...]:
    rules = json.loads(Path(path).read_bytes().decode("utf-8-sig"))
    return tuple(
        BpaRule(
            id=rule["ID"],
            name=rule.get("Name", rule["ID"]),
            category=rule.get("Category", ""),
            description=rule.get("Description", ""),
            severity=SEVERITIES.get(rule.get("Severity"), ""),
            scope=tuple(s.strip() for s in str(rule.get("Scope", "")).split(",") if s.strip()),
            compatibility_level=int(rule.get("CompatibilityLevel") or 0),
        )
        for rule in rules
    )


def load_rules(rules_path: Path = DEFAULT_BPA_RULES) -> list[BpaRule]:
    """
    Read a BPA rules file (Tabular Editor JSON format).

    Args:
        rules_path: Rules file (default: Microsoft's standard rules, data/BPARules.json)

    Returns:
        The rules in file order
    """
    return list(_load_rules(str(rules_path)))


def rule_by_id(rule_id: str, rules_path: Path = DEFAULT_BPA_RULES) -> Optional[BpaRule]:
    """The rule with this ID, or None."""
    return next((rule for rule in load_rules(rules_path) if rule.id == rule_id), None)


# ============================================================================
# Objects in the model, per rule scope
# ============================================================================


@dataclass
class BpaObject:
    """An object a rule is checked on, named the way Tabular Editor's analyzer names it."""

    scope: str  # the rule scope it falls under, e.g. DataColumn
    object_type: str  # e.g. Column, Calculated Table, Partition (M - Import)
    object_name: str  # e.g. 'Sales'[Amount]
    item: Any  # the semantic_model object (or the model)
    annotations: dict[str, str] = field(default_factory=dict)
    table: Optional[Table] = None  # the table it belongs to (columns, measures, ...)


@dataclass
class TablePermission:
    """One role's RLS filter on one table (Tabular Editor's TablePermission)."""

    role: str
    table: str
    expression: str


def dax_object_name(table: str, name: str) -> str:
    """Tabular Editor's DaxObjectFullName: 'Table'[Name], with ' doubled and ] as ]]."""
    return "'" + table.replace("'", "''") + "'[" + name.replace("]", "]]") + "]"


_PARTITION_SOURCES = {"m": "M", "query": "Legacy", "calculated": "Calculated", "entity": "Entity"}
_PARTITION_MODES = {
    "": "Import",
    "import": "Import",
    "directQuery": "DirectQuery",
    "dual": "Dual",
    "directLake": "DirectLake",
    "default": "Default",
    "push": "Push",
}
_COLUMN_SCOPES = {
    "data": ("DataColumn", "Column"),
    "calculated": ("CalculatedColumn", "Calculated Column"),
    "calculatedTableColumn": ("CalculatedTableColumn", "Calculated Table Column"),
}


def _table_scope(table: Table) -> tuple[str, str]:
    if table.is_calculation_group:
        return "CalculationGroup", "Calculation Group Table"
    if table.is_calculated:
        return "CalculatedTable", "Calculated Table"
    return "Table", "Table"


def iter_objects(model: SemanticModel) -> Iterator[BpaObject]:
    """Every object of the model a rule can be scoped to, in model order."""
    yield BpaObject("Model", "Model", model.model_name, model, model.annotations)
    for table in model.tables:
        scope, type_name = _table_scope(table)
        yield BpaObject(scope, type_name, table.name, table, table.annotations, table)
        for column in table.columns:
            scope, type_name = _COLUMN_SCOPES.get(column.kind, ("DataColumn", "Column"))
            name = dax_object_name(table.name, column.name)
            yield BpaObject(scope, type_name, name, column, column.annotations, table)
        for measure in table.measures:
            name = dax_object_name(table.name, measure.name)
            yield BpaObject("Measure", "Measure", name, measure, measure.annotations, table)
        for hierarchy in table.hierarchies:
            yield BpaObject(
                "Hierarchy", "Hierarchy", hierarchy.name, hierarchy, hierarchy.annotations, table
            )
            for level in hierarchy.levels:
                yield BpaObject("Level", "Level", level.name, level, {}, table)
        for partition in table.partitions:
            source = _PARTITION_SOURCES.get(partition.source_type, partition.source_type.title())
            mode = _PARTITION_MODES.get(partition.mode, partition.mode)
            yield BpaObject(
                "Partition",
                f"Partition ({source} - {mode})",
                partition.name,
                partition,
                partition.annotations,
                table,
            )
        for item in table.calculation_items:
            yield BpaObject(
                "CalculationItem", "Calculation Item", item.name, item, item.annotations, table
            )
    for relationship in model.relationships:
        yield BpaObject(
            "Relationship",
            "Relationship",
            relationship.te_name,
            relationship,
            relationship.annotations,
        )
    for perspective in model.perspectives:
        yield BpaObject("Perspective", "Perspective", perspective, perspective)
    for role in model.roles:
        yield BpaObject("ModelRole", "Role", role.name, role, role.annotations)
    for role in model.roles:
        for table_name, dax in role.table_filters.items():
            # Named like Tabular Editor's TablePermission.Name (the table): a best guess
            permission = TablePermission(role.name, table_name, dax)
            table = next((t for t in model.tables if t.name == table_name), None)
            yield BpaObject(
                "TablePermission", "Table Permission", table_name, permission, {}, table
            )
    for source in model.data_sources:
        if source.type.lower() == "structured":
            yield BpaObject("StructuredDataSource", "Structured Data Source", source.name, source)
        else:
            yield BpaObject("ProviderDataSource", "Provider Data Source", source.name, source)
    for expression in model.expressions:
        yield BpaObject(
            "NamedExpression", "Shared Expression", expression.name, expression,
            expression.annotations,
        )


# ============================================================================
# What the checks need to know about the model
# ============================================================================


def _int(value) -> int:
    """Convert.ToInt64 of an annotation value: missing -> 0; unparsable -> 0."""
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


def _te_type(column: Column) -> str:
    """The data type as Tabular Editor spells it: String, Int64, Double, DateTime, ..."""
    return DATA_TYPES.get(column.data_type, column.data_type)


def _many_to_many(rel: Relationship) -> bool:
    return rel.from_cardinality.lower() == "many" and rel.to_cardinality.lower() == "many"


def _both(rel: Relationship) -> bool:
    return rel.cross_filtering == "bothDirections"


class _Context:
    """The model plus indexes the checks share (relationships per column/table, ...)."""

    def __init__(self, model: SemanticModel, statistics: Optional[LiveStatistics]):
        self.model = model
        self.statistics = statistics
        self.tables = {table.name: table for table in model.tables}
        self.column_rels: dict[tuple[str, str], list[Relationship]] = {}
        self.table_rels: dict[str, list[Relationship]] = {}
        for rel in model.relationships:
            for key in {(rel.from_table, rel.from_column), (rel.to_table, rel.to_column)}:
                self.column_rels.setdefault(key, []).append(rel)
            for table in {rel.from_table, rel.to_table}:
                self.table_rels.setdefault(table, []).append(rel)
        # UsedInSortBy: columns other columns are sorted by
        self.sort_targets = {
            (c.table, c.sort_by_column) for c in model.all_columns if c.sort_by_column
        }
        self.hierarchy_columns = {
            (h.table, level.column) for h in model.all_hierarchies for level in h.levels
        }
        self.columns_by_name: dict[str, list[Column]] = {}
        for column in model.all_columns:
            self.columns_by_name.setdefault(column.name, []).append(column)
        self.rls: dict[str, list[str]] = {}
        for role in model.roles:
            for table, dax in role.table_filters.items():
                self.rls.setdefault(table, []).append(dax)

        self._dependencies = None
        self._index = None

    # DAX (ticket 04): Tabular Editor's DependsOn / ReferencedBy from dax_parser

    @property
    def index(self):
        """The model's case-insensitive name lookup (dax_parser.model_index)."""
        if self._index is None:
            self._index = model_index(self.model)
        return self._index

    @property
    def referenced_by(self) -> dict[tuple[str, str, str], set[tuple[str, str, str]]]:
        """(target type, table, name) -> {(source type, table, name)}: ReferencedBy."""
        if self._dependencies is None:
            self._dependencies = {}
            for dep in model_dependencies(self.model):
                target = (dep.target_type, dep.target_table, dep.target_name)
                source = (dep.source_type, dep.source_table, dep.source_name)
                self._dependencies.setdefault(target, set()).add(source)
        return self._dependencies

    def referenced(self, object_type: str, table: str, name: str) -> bool:
        """ReferencedBy.Count > 0."""
        return bool(self.referenced_by.get((object_type, table, name)))

    def measures_referencing(self, object_type: str, table: str, name: str) -> list[Measure]:
        """ReferencedBy.AllMeasures: measures referencing the object, directly or indirectly."""
        seen: set[tuple[str, str, str]] = set()
        todo = [(object_type, table, name)]
        while todo:
            for source in self.referenced_by.get(todo.pop(), ()):
                if source not in seen and source[0] in ("Measure", "Column", "Table"):
                    seen.add(source)
                    todo.append(source)
        return [m for m in self.model.all_measures if ("Measure", m.table, m.name) in seen]

    def hidden(self, column: Column) -> bool:
        """IsHidden or Table.IsHidden."""
        table = self.tables.get(column.table)
        return column.is_hidden or bool(table and table.is_hidden)

    def is_direct_query(self, table: Table) -> bool:
        """Tabular Editor's "Table (DirectQuery)": a partition in DirectQuery mode."""
        return any(self.model.partition_mode(p) == "directQuery" for p in table.partitions)

    # Statistics (D15): the run's LiveStatistics first, else the Vertipaq_* annotations
    # VertiPaq Analyzer may have stored in the model

    def has_annotation(self, name: str) -> bool:
        return any(name in obj.annotations for obj in iter_objects(self.model))

    def has_cardinality(self) -> bool:
        stats = self.statistics
        if stats and any(c.distinct_values is not None for c in stats.columns.values()):
            return True
        return self.has_annotation("Vertipaq_Cardinality")

    def cardinality(self, column: Column) -> int:
        stats = self.statistics
        found = stats.columns.get((column.table, column.name)) if stats else None
        if found is not None and found.distinct_values is not None:
            return found.distinct_values
        return _int(column.annotations.get("Vertipaq_Cardinality"))

    def has_row_counts(self) -> bool:
        if self.statistics and self.statistics.table_rows:
            return True
        return self.has_annotation("Vertipaq_RowCount")

    def row_count(self, table: Table) -> int:
        stats = self.statistics
        if stats and table.name in stats.table_rows:
            return stats.table_rows[table.name]
        return _int(table.annotations.get("Vertipaq_RowCount"))


# ============================================================================
# The checks: rule ID -> predicate(context, object) -> breaks the rule?
# ============================================================================

Check = Callable[[_Context, BpaObject], bool]
CHECKS: dict[str, Check] = {}
# Rule ID -> predicate(context): can the rule be evaluated? (else "needs statistics")
REQUIRES: dict[str, Callable[[_Context], bool]] = {}


def check(rule_id: str, requires: Optional[Callable[[_Context], bool]] = None):
    """Register a rule's check (and what it needs) under the rule's ID."""

    def register(function: Check) -> Check:
        CHECKS[rule_id] = function
        if requires is not None:
            REQUIRES[rule_id] = requires
        return function

    return register


def _annotation_required(name: str) -> Callable[[_Context], bool]:
    return lambda ctx: ctx.has_annotation(name)


def _control_characters(text: str) -> bool:
    """char.IsControl(it) and !char.IsWhiteSpace(it) for any character."""
    return any(
        unicodedata.category(ch) == "Cc" and ch not in "\t\n\v\f\r\x85" for ch in text or ""
    )


def _description(obj: BpaObject) -> str:
    return getattr(obj.item, "description", "") or ""


def _name(obj: BpaObject) -> str:
    """The object's own name (the model's, a table's, a column's), not the DAX full name."""
    item = obj.item
    if isinstance(item, Relationship):
        return item.te_name
    if isinstance(item, str):  # perspective
        return item
    if isinstance(item, SemanticModel):
        return item.model_name
    return getattr(item, "name", "")


# --- Columns -------------------------------------------------------------------------------


@check("AVOID_FLOATING_POINT_DATA_TYPES")
def _floating_point(ctx, obj):
    # DataType = "Double"
    return _te_type(obj.item) == "Double"


@check("ISAVAILABLEINMDX_FALSE_NONATTRIBUTE_COLUMNS")
def _mdx_false_non_attribute(ctx, obj):
    # IsAvailableInMDX and (IsHidden or Table.IsHidden) and not UsedInSortBy.Any() and
    # not UsedInHierarchies.Any() and not UsedInVariations.Any() and SortByColumn = null
    column = obj.item
    key = (column.table, column.name)
    return (
        column.is_available_in_mdx
        and ctx.hidden(column)
        and key not in ctx.sort_targets
        and key not in ctx.hierarchy_columns
        and key not in ctx.model.variation_columns
        and not column.sort_by_column
    )


@check(
    "AVOID_BI-DIRECTIONAL_RELATIONSHIPS_AGAINST_HIGH-CARDINALITY_COLUMNS",
    requires=lambda ctx: ctx.has_cardinality(),
)
def _bidirectional_high_cardinality(ctx, obj):
    # UsedInRelationships.Any(CrossFilteringBehavior == CrossFilteringBehavior.BothDirections)
    # and Convert.ToInt64(GetAnnotation("Vertipaq_Cardinality")) > 100000
    column = obj.item
    rels = ctx.column_rels.get((column.table, column.name), [])
    return any(_both(rel) for rel in rels) and ctx.cardinality(column) > 100000


@check(
    "REDUCE_USAGE_OF_LONG-LENGTH_COLUMNS_WITH_HIGH_CARDINALITY",
    requires=_annotation_required("LongLengthRowCount"),
)
def _long_length(ctx, obj):
    # Convert.ToInt64(GetAnnotation("LongLengthRowCount")) > 500000
    return _int(obj.annotations.get("LongLengthRowCount")) > 500000


@check("SPLIT_DATE_AND_TIME", requires=_annotation_required("DateTimeWithHourMinSec"))
def _split_date_and_time(ctx, obj):
    # Convert.ToInt32(GetAnnotation("DateTimeWithHourMinSec")) > 0
    return _int(obj.annotations.get("DateTimeWithHourMinSec")) > 0


@check("REMOVE_REDUNDANT_COLUMNS_IN_RELATED_TABLES")
def _redundant_columns(ctx, obj):
    # UsedInRelationships.Any() == false and Model.AllColumns.Any(Name == current.Name and
    # Table.Name != current.Table.Name and
    # Table.UsedInRelationships.Any(FromTable.Name == current.Table.Name))
    column = obj.item
    if ctx.column_rels.get((column.table, column.name)):
        return False
    return any(
        other.table != column.table
        and any(rel.from_table == column.table for rel in ctx.table_rels.get(other.table, []))
        for other in ctx.columns_by_name.get(column.name, [])
    )


@check("DATA_COLUMNS_MUST_HAVE_A_SOURCE_COLUMN")
def _source_column(ctx, obj):
    # string.IsNullOrWhitespace(SourceColumn)
    return not obj.item.source_column.strip()


@check("SET_ISAVAILABLEINMDX_TO_TRUE_ON_NECESSARY_COLUMNS")
def _mdx_needed(ctx, obj):
    # IsAvailableInMDX = false and ( UsedInSortBy.Any() or UsedInHierarchies.Any() or
    # UsedInVariations.Any() or SortByColumn != null )
    column = obj.item
    key = (column.table, column.name)
    return not column.is_available_in_mdx and (
        key in ctx.sort_targets
        or key in ctx.hierarchy_columns
        or key in ctx.model.variation_columns
        or bool(column.sort_by_column)
    )


@check("FORMAT_FLAG_COLUMNS_AS_YES/NO_VALUE_STRINGS")
def _flag_columns(ctx, obj):
    # ( Name.StartsWith("Is") and DataType = "Int64" and not (IsHidden or Table.IsHidden) ) or
    # ( Name.EndsWith(" Flag") and DataType <> "String" and not (IsHidden or Table.IsHidden) )
    column = obj.item
    if ctx.hidden(column):
        return False
    data_type = _te_type(column)
    return (column.name.startswith("Is") and data_type == "Int64") or (
        column.name.endswith(" Flag") and data_type != "String"
    )


@check("DATECOLUMN_FORMATSTRING")
def _date_format(ctx, obj):
    # Name.IndexOf("Date", "OrdinalIgnoreCase") >= 0 and DataType = "DateTime" and
    # FormatString <> "mm/dd/yyyy"
    column = obj.item
    return (
        "date" in column.name.lower()
        and _te_type(column) == "DateTime"
        and column.format_string != "mm/dd/yyyy"
    )


@check("MONTHCOLUMN_FORMATSTRING")
def _month_format(ctx, obj):
    # Name.IndexOf("Month", "OrdinalIgnoreCase") >= 0 and DataType = "DateTime" and
    # FormatString <> "MMMM yyyy"
    column = obj.item
    return (
        "month" in column.name.lower()
        and _te_type(column) == "DateTime"
        and column.format_string != "MMMM yyyy"
    )


@check("NUMERIC_COLUMN_SUMMARIZE_BY")
def _summarize_by(ctx, obj):
    # ( DataType = "Int64" or DataType="Decimal" or DataType="Double" ) and
    # SummarizeBy <> "None" and not (IsHidden or Table.IsHidden)
    column = obj.item
    return (
        _te_type(column) in ("Int64", "Decimal", "Double")
        and (column.summarize_by or "default").lower() != "none"
        and not ctx.hidden(column)
    )


@check("RELATIONSHIP_COLUMNS_SHOULD_BE_OF_INTEGER_DATA_TYPE")
def _relationship_integer(ctx, obj):
    # UsedInRelationships.Any() and DataType != DataType.Int64
    column = obj.item
    return bool(ctx.column_rels.get((column.table, column.name))) and _te_type(column) != "Int64"


@check("ADD_DATA_CATEGORY_FOR_COLUMNS")
def _data_category(ctx, obj):
    # string.IsNullOrWhiteSpace(DataCategory) && (
    #   ((Name.ToLower().Contains("country") || ...("continent") || ...("city"))
    #     && DataType == DataType.String)
    #   || ((Name.ToLower() == "latitude" || Name.ToLower() == "longitude")
    #     && (DataType == DataType.Decimal || DataType == DataType.Double)) )
    column = obj.item
    if column.data_category.strip():
        return False
    name, data_type = column.name.lower(), _te_type(column)
    places = any(word in name for word in ("country", "continent", "city"))
    return (places and data_type == "String") or (
        name in ("latitude", "longitude") and data_type in ("Decimal", "Double")
    )


@check("HIDE_FOREIGN_KEYS")
def _hide_foreign_keys(ctx, obj):
    # UsedInRelationships.Any(FromColumn.Name == current.Name and FromCardinality == "Many")
    # and IsHidden == false   (the name only, as in Tabular Editor)
    column = obj.item
    rels = ctx.column_rels.get((column.table, column.name), [])
    return not column.is_hidden and any(
        rel.from_column == column.name and rel.from_cardinality.lower() == "many" for rel in rels
    )


@check("MARK_PRIMARY_KEYS")
def _mark_primary_keys(ctx, obj):
    # UsedInRelationships.Any(ToTable.Name == current.Table.Name and
    # ToColumn.Name == current.Name and ToCardinality == "One") and IsKey == false and
    # current.Table.DataCategory != "Time"
    column = obj.item
    rels = ctx.column_rels.get((column.table, column.name), [])
    return (
        any(
            rel.to_table == column.table
            and rel.to_column == column.name
            and rel.to_cardinality.lower() == "one"
            for rel in rels
        )
        and not column.is_key
        and obj.table is not None
        and obj.table.data_category != "Time"
    )


@check("MONTH_(AS_A_STRING)_MUST_BE_SORTED")
def _month_sorted(ctx, obj):
    # Name.ToUpper().Contains("MONTH") and ! Name.ToUpper().Contains("MONTHS") and
    # DataType == DataType.String and SortByColumn == null
    column = obj.item
    name = column.name.upper()
    return (
        "MONTH" in name
        and "MONTHS" not in name
        and _te_type(column) == "String"
        and not column.sort_by_column
    )


# --- Tables --------------------------------------------------------------------------------


@check("LARGE_TABLES_SHOULD_BE_PARTITIONED", requires=lambda ctx: ctx.has_row_counts())
def _large_tables(ctx, obj):
    # Convert.ToInt64(GetAnnotation("Vertipaq_RowCount")) > 25000000 and Partitions.Count = 1
    return ctx.row_count(obj.item) > 25_000_000 and len(obj.item.partitions) == 1


@check("SNOWFLAKE_SCHEMA_ARCHITECTURE")
def _snowflake(ctx, obj):
    # UsedInRelationships.Any(current.Name == FromTable.Name) and
    # UsedInRelationships.Any(current.Name == ToTable.Name)
    rels = ctx.table_rels.get(obj.item.name, [])
    return any(rel.from_table == obj.item.name for rel in rels) and any(
        rel.to_table == obj.item.name for rel in rels
    )


def _is_marked_date_table(table: Table) -> bool:
    """DataCategory == "Time" && Columns.Any(IsKey == true && DataType == "DateTime")."""
    return table.data_category == "Time" and any(
        column.is_key and _te_type(column) == "DateTime" for column in table.columns
    )


@check("DATE/CALENDAR_TABLES_SHOULD_BE_MARKED_AS_A_DATE_TABLE")
def _date_table_marked(ctx, obj):
    # (Name.ToUpper().Contains("DATE") or Name.ToUpper().Contains("CALENDAR")) and
    # ( DataCategory <> "Time" or Columns.Any(IsKey == true && DataType == "DateTime") == false )
    name = obj.item.name.upper()
    return ("DATE" in name or "CALENDAR" in name) and not _is_marked_date_table(obj.item)


@check("REMOVE_AUTO-DATE_TABLE")
def _auto_date_table(ctx, obj):
    # ObjectTypeName == "Calculated Table" and ( Name.StartsWith("DateTableTemplate_") or
    # Name.StartsWith("LocalDateTable_") )
    return obj.object_type == "Calculated Table" and obj.item.name.startswith(
        ("DateTableTemplate_", "LocalDateTable_")
    )


@check("AVOID_USING_MANY-TO-MANY_RELATIONSHIPS_ON_TABLES_USED_FOR_DYNAMIC_ROW_LEVEL_SECURITY")
def _many_to_many_rls(ctx, obj):
    # UsedInRelationships.Any(FromCardinality == "Many" and ToCardinality== "Many") and
    # RowLevelSecurity.Any(it.Length > 0)
    rels = ctx.table_rels.get(obj.item.name, [])
    return any(_many_to_many(rel) for rel in rels) and any(
        len(dax) > 0 for dax in ctx.rls.get(obj.item.name, [])
    )


@check("UNPIVOT_PIVOTED_(MONTH)_DATA")
def _pivoted_months(ctx, obj):
    # Columns.Any(Name.ToUpper().Contains("JAN") && (DataType == DataType.Int64 ||
    # DataType == DataType.Decimal || DataType == DataType.Double)) and ... "FEB", "MAR",
    # "APR", "MAY", "JUN" alike
    numeric = [
        column.name.upper()
        for column in obj.item.columns
        if _te_type(column) in ("Int64", "Decimal", "Double")
    ]
    return all(
        any(month in name for name in numeric)
        for month in ("JAN", "FEB", "MAR", "APR", "MAY", "JUN")
    )


@check("REDUCE_USAGE_OF_CALCULATED_TABLES")
def _calculated_tables(ctx, obj):
    # 1=1 (every calculated table)
    return True


@check("ENSURE_TABLES_HAVE_RELATIONSHIPS")
def _no_relationships(ctx, obj):
    # UsedInRelationships.Count() == 0
    return not ctx.table_rels.get(obj.item.name)


@check("CALCULATION_GROUPS_WITH_NO_CALCULATION_ITEMS")
def _empty_calculation_group(ctx, obj):
    # CalculationItems.Count == 0
    return not obj.item.calculation_items


@check("PARTITION_NAME_SHOULD_MATCH_TABLE_NAME_FOR_SINGLE_PARTITION_TABLES")
def _partition_name(ctx, obj):
    # (Partitions.Count = 1 and Partitions[0].Name <> Name)
    partitions = obj.item.partitions
    return len(partitions) == 1 and partitions[0].name != obj.item.name


# --- Model ---------------------------------------------------------------------------------


@check("MODEL_SHOULD_HAVE_A_DATE_TABLE")
def _model_date_table(ctx, obj):
    # Tables.Any(DataCategory == "Time" && Columns.Any(IsKey == true && DataType == "DateTime"))
    # == false
    return not any(_is_marked_date_table(table) for table in ctx.model.tables)


@check("AVOID_EXCESSIVE_BI-DIRECTIONAL_OR_MANY-TO-MANY_RELATIONSHIPS")
def _excessive_bidirectional(ctx, obj):
    # ( Relationships.Where(CrossFilteringBehavior == CrossFilteringBehavior.BothDirections)
    #   .Count() + Relationships.Where(FromCardinality.ToString() == "Many" &&
    #   ToCardinality.ToString() == "Many").Count() ) /
    # Math.Max(Convert.ToDecimal(Relationships.Count), 1) > 0.3
    rels = ctx.model.relationships
    bad = sum(1 for rel in rels if _both(rel)) + sum(1 for rel in rels if _many_to_many(rel))
    return bad / max(len(rels), 1) > 0.3


@check("MODEL_USING_DIRECT_QUERY_AND_NO_AGGREGATIONS")
def _direct_query_no_aggregations(ctx, obj):
    # Tables.Any(ObjectTypeName == "Table (DirectQuery)") and
    # AllColumns.Any(AlternateOf != null) == false and
    # DefaultPowerBIDataSourceVersion.ToString() == "PowerBI_V3"
    return (
        any(ctx.is_direct_query(table) for table in ctx.model.tables)
        and not any(column.alternate_of for column in ctx.model.all_columns)
        and ctx.model.default_powerbi_datasource_version.lower() == "powerbi_v3"
    )


@check("REDUCE_NUMBER_OF_CALCULATED_COLUMNS")
def _many_calculated_columns(ctx, obj):
    # AllColumns.Where(Type.ToString() == "Calculated").Count() > 5
    return sum(1 for column in ctx.model.all_columns if column.kind == "calculated") > 5


# --- Relationships -------------------------------------------------------------------------


@check("MANY-TO-MANY_RELATIONSHIPS_SHOULD_BE_SINGLE-DIRECTION")
def _many_to_many_both(ctx, obj):
    # FromCardinality == "Many" and ToCardinality == "Many" and
    # CrossFilteringBehavior == "BothDirections"
    return _many_to_many(obj.item) and _both(obj.item)


@check("CHECK_IF_BI-DIRECTIONAL_AND_MANY-TO-MANY_RELATIONSHIPS_ARE_VALID")
def _bidirectional_or_many_to_many(ctx, obj):
    # FromCardinality.ToString() = "Many" and ToCardinality.ToString() = "Many" or
    # CrossFilteringBehavior == CrossFilteringBehavior.BothDirections
    return _many_to_many(obj.item) or _both(obj.item)


@check("RELATIONSHIP_COLUMNS_SAME_DATA_TYPE")
def _relationship_same_type(ctx, obj):
    # FromColumn.DataType != ToColumn.DataType
    rel = obj.item

    def column(table: str, name: str) -> Optional[Column]:
        found = ctx.tables.get(table)
        return next((c for c in found.columns if c.name == name), None) if found else None

    from_column = column(rel.from_table, rel.from_column)
    to_column = column(rel.to_table, rel.to_column)
    if from_column is None or to_column is None:
        return False  # a broken relationship: not this rule's finding
    return _te_type(from_column) != _te_type(to_column)


@check(
    "FIX_REFERENTIAL_INTEGRITY_VIOLATIONS",
    requires=_annotation_required("Vertipaq_RIViolationInvalidRows"),
)
def _referential_integrity(ctx, obj):
    # Convert.ToInt64(GetAnnotation("Vertipaq_RIViolationInvalidRows")) > 0
    return _int(obj.annotations.get("Vertipaq_RIViolationInvalidRows")) > 0


# --- Partitions, data sources, roles, perspectives -----------------------------------------

_POWER_QUERY_STEPS = (
    "Table.Combine(",
    "Table.Join(",
    "Table.NestedJoin(",
    "Table.AddColumn(",
    "Table.Group(",
    "Table.Sort(",
    "Table.Pivot(",
    "Table.Unpivot(",
    "Table.UnpivotOtherColumns(",
    "Table.Distinct(",
    '[Query="SELECT',
    "Value.NativeQuery",
    "OleDb.Query",
    "Odbc.Query",
)


@check("MINIMIZE_POWER_QUERY_TRANSFORMATIONS")
def _power_query_steps(ctx, obj):
    # SourceType.ToString() = "M" and ( Query.Contains("Table.Combine(") or ... or
    # Query.Contains("[Query=""SELECT") or Query.Contains("Value.NativeQuery") or
    # Query.Contains("OleDb.Query") or Query.Contains("Odbc.Query") )
    partition = obj.item
    return partition.source_type == "m" and any(
        step in partition.expression for step in _POWER_QUERY_STEPS
    )


@check("AVOID_STRUCTURED_DATA_SOURCES_WITH_PROVIDER_PARTITIONS")
def _structured_source_query_partition(ctx, obj):
    # SourceType == "Query" and DataSource.Type == "Structured"
    partition = obj.item
    if partition.source_type != "query":
        return False
    source = next((s for s in ctx.model.data_sources if s.name == partition.data_source), None)
    return source is not None and source.type.lower() == "structured"


@check("REMOVE_DATA_SOURCES_NOT_REFERENCED_BY_ANY_PARTITIONS")
def _unused_data_source(ctx, obj):
    # UsedByPartitions.Count() == 0 and not Model.Tables.Any(SourceExpression.Contains(
    # OuterIt.Name)) and not Model.AllPartitions.Any(Query.Contains(OuterIt.Name))
    name = obj.item.name
    partitions = [p for table in ctx.model.tables for p in table.partitions]
    return not any(p.data_source == name or name in p.expression for p in partitions)


@check("REMOVE_ROLES_WITH_NO_MEMBERS")
def _role_without_members(ctx, obj):
    # Members.Count() == 0
    return not obj.item.members


@check("PERSPECTIVES_WITH_NO_OBJECTS")
def _empty_perspective(ctx, obj):
    # Model.Tables.Any(InPerspective[current.Name]) == false
    return not ctx.model.perspective_tables.get(obj.item)


# --- Names, descriptions and format strings (several scopes) -------------------------------


@check("AVOID_INVALID_NAME_CHARACTERS")
def _invalid_name_characters(ctx, obj):
    # Name.ToCharArray().Any(char.IsControl(it) and !char.IsWhiteSpace(it))
    return _control_characters(_name(obj))


@check("AVOID_INVALID_DESCRIPTION_CHARACTERS")
def _invalid_description_characters(ctx, obj):
    # Description.ToCharArray().Any(char.IsControl(it) and !char.IsWhiteSpace(it))
    return _control_characters(_description(obj))


@check("SPECIAL_CHARS_IN_OBJECT_NAMES")
def _special_characters(ctx, obj):
    # Name.IndexOf(char(9)) > -1 or Name.IndexOf(char(10)) > -1 or Name.IndexOf(char(13)) > -1
    return any(ch in _name(obj) for ch in "\t\n\r")


@check("TRIM_OBJECT_NAMES")
def _trim_names(ctx, obj):
    # Name.StartsWith(" ") or Name.EndsWith(" ")
    name = _name(obj)
    return name.startswith(" ") or name.endswith(" ")


@check("OBJECTS_SHOULD_NOT_START_OR_END_WITH_A_SPACE")
def _space_names(ctx, obj):
    # Name.StartsWith(" ") or Name.EndsWith(" ")
    name = _name(obj)
    return name.startswith(" ") or name.endswith(" ")


@check("OBJECTS_WITH_NO_DESCRIPTION")
def _no_description(ctx, obj):
    # string.IsNullOrWhitespace(Description) and IsHidden == false
    return not _description(obj).strip() and not getattr(obj.item, "is_hidden", False)


@check("FIRST_LETTER_OF_OBJECTS_MUST_BE_CAPITALIZED")
def _capitalized(ctx, obj):
    # Name.Substring(0,1).ToUpper() != Name.Substring(0,1)
    first = _name(obj)[:1]
    upper = first.upper()
    return len(upper) == 1 and upper != first  # .NET keeps e.g. "ß" as it is


# --- Measures ------------------------------------------------------------------------------


@check("PROVIDE_FORMAT_STRING_FOR_MEASURES")
def _measure_format_string(ctx, obj):
    # not IsHidden and not Table.IsHidden and string.IsNullOrWhitespace(FormatString) and
    # string.IsNullOrWhitespace(FormatStringExpression)
    measure = obj.item
    return (
        not measure.is_hidden
        and not (obj.table and obj.table.is_hidden)
        and not measure.format_string.strip()
        and not measure.format_string_expression.strip()
    )


@check("PERCENTAGE_FORMATTING")
def _percentage_format(ctx, obj):
    # FormatString.Contains("%") and FormatString <> "#,0.0%;-#,0.0%;#,0.0%"
    text = obj.item.format_string
    return "%" in text and text != "#,0.0%;-#,0.0%;#,0.0%"


@check("INTEGER_FORMATTING")
def _integer_format(ctx, obj):
    # not FormatString.Contains("$") and not FormatString.Contains("%") and
    # not (FormatString = "#,0" or FormatString = "#,0.0")
    text = obj.item.format_string
    return "$" not in text and "%" not in text and text not in ("#,0", "#,0.0")


# --- DAX -----------------------------------------------------------------------------------
# Regexes run on the expression text, comments and strings included, as Tabular Editor's
# RegEx.IsMatch(Expression, ...) does; references (DependsOn / ReferencedBy) come from
# dax_parser and never count code in comments or strings. Names Tabular Editor pastes into a
# regex are escaped here (Tabular Editor pastes them as they are).


def _expression(obj: BpaObject) -> str:
    """Expression: the DAX of a measure, calculated column/table, calculation item or RLS."""
    if obj.scope == "CalculatedTable":
        return next(
            (p.expression for p in obj.item.partitions if p.source_type == "calculated"), ""
        )
    return getattr(obj.item, "expression", "") or ""


def _matches(pattern: str, text: str) -> bool:
    return re.search(pattern, text or "") is not None


def _own_table(obj: BpaObject) -> Optional[str]:
    """The table an unqualified [X] may mean as a column (the object's own table)."""
    return obj.table.name if obj.table is not None else None


@check("REDUCE_USAGE_OF_CALCULATED_COLUMNS_THAT_USE_THE_RELATED_FUNCTION")
def _calculated_related(ctx, obj):
    # RegEx.IsMatch(Expression,"(?i)RELATED\s*\(")
    return _matches(r"(?i)RELATED\s*\(", _expression(obj))


_RLS_STRING_FUNCTIONS = ("RIGHT", "LEFT", "UPPER", "LOWER", "FIND")


@check("LIMIT_ROW_LEVEL_SECURITY_(RLS)_LOGIC")
def _rls_logic(ctx, obj):
    # RowLevelSecurity.Any(RegEx.IsMatch(it.Replace(" ",""),"(?i)RIGHT\s*\(")) or ... LEFT,
    # UPPER, LOWER, FIND alike
    return any(
        _matches(rf"(?i){function}\s*\(", dax.replace(" ", ""))
        for dax in ctx.rls.get(obj.item.name, [])
        for function in _RLS_STRING_FUNCTIONS
    )


_TIME_INTELLIGENCE = (
    "CLOSINGBALANCEMONTH", "CLOSINGBALANCEQUARTER", "CLOSINGBALANCEYEAR", "DATEADD",
    "DATESBETWEEN", "DATESINPERIOD", "DATESMTD", "DATESQTD", "DATESYTD", "ENDOFMONTH",
    "ENDOFQUARTER", "ENDOFYEAR", "FIRSTDATE", "FIRSTNONBLANK", "FIRSTNONBLANKVALUE", "LASTDATE",
    "LASTNONBLANK", "LASTNONBLANKVALUE", "NEXTDAY", "NEXTMONTH", "NEXTQUARTER", "NEXTYEAR",
    "OPENINGBALANCEMONTH", "OPENINGBALANCEQUARTER", "OPENINGBALANCEYEAR", "PARALLELPERIOD",
    "PREVIOUSDAY", "PREVIOUSMONTH", "PREVIOUSQUARTER", "PREVIOUSYEAR", "SAMEPERIODLASTYEAR",
    "STARTOFMONTH", "STARTOFQUARTER", "STARTOFYEAR", "TOTALMTD", "TOTALQTD", "TOTALYTD",
)  # fmt: skip
_TIME_INTELLIGENCE_RE = re.compile("(?:" + "|".join(_TIME_INTELLIGENCE) + r")\s*\(")


@check("MEASURES_USING_TIME_INTELLIGENCE_AND_MODEL_IS_USING_DIRECT_QUERY")
def _time_intelligence_direct_query(ctx, obj):
    # Model.Tables.Any(ObjectTypeName == "Table (DirectQuery)") and
    # ( RegEx.IsMatch(Expression,"CLOSINGBALANCEMONTH\s*\(") or ... "TOTALYTD\s*\(" )
    # (case-sensitive: the rule has no (?i))
    return any(ctx.is_direct_query(table) for table in ctx.model.tables) and bool(
        _TIME_INTELLIGENCE_RE.search(_expression(obj))
    )


@check("CHECK_IF_DYNAMIC_ROW_LEVEL_SECURITY_(RLS)_IS_NECESSARY")
def _dynamic_rls(ctx, obj):
    # RegEx.IsMatch(Expression,"(?i)USERNAME\(") or RegEx.IsMatch(Expression,
    # "(?i)USERPRINCIPALNAME\(")
    dax = _expression(obj)
    return _matches(r"(?i)USERNAME\(", dax) or _matches(r"(?i)USERPRINCIPALNAME\(", dax)


@check("DAX_COLUMNS_FULLY_QUALIFIED")
def _columns_fully_qualified(ctx, obj):
    # DependsOn.Any(Key.ObjectType = "Column" and Value.Any(not FullyQualified))
    # (an unqualified [X] is a column when it is no measure and a column of the object's table)
    return any(
        ref.type == "Column" and not ref.qualified
        for ref in reference_occurrences(_expression(obj), ctx.index, _own_table(obj))
    )


@check("DAX_MEASURES_UNQUALIFIED")
def _measures_unqualified(ctx, obj):
    # DependsOn.Any(Key.ObjectType = "Measure" and Value.Any(FullyQualified))
    return any(
        ref.type == "Measure" and ref.qualified
        for ref in reference_occurrences(_expression(obj), ctx.index, _own_table(obj))
    )


def _squeezed(text: str) -> str:
    """Expression.Replace(" ","").Replace("\n","").Replace("\r","").Replace("\t","")."""
    for ch in " \n\r\t":
        text = text.replace(ch, "")
    return text


@check("AVOID_DUPLICATE_MEASURES")
def _duplicate_measures(ctx, obj):
    # Model.AllMeasures.Any(Expression.Replace(" ","").Replace("\n","").Replace("\r","")
    # .Replace("\t","") = outerIt.Expression.Replace(...) and it <> outerIt)
    own = _squeezed(obj.item.expression)
    return any(
        _squeezed(m.expression) == own for m in ctx.model.all_measures if m is not obj.item
    )


@check("USE_THE_TREATAS_FUNCTION_INSTEAD_OF_INTERSECT")
def _intersect(ctx, obj):
    # RegEx.IsMatch(Expression,"(?i)INTERSECT\s*\(")
    return _matches(r"(?i)INTERSECT\s*\(", _expression(obj))


@check("USE_THE_DIVIDE_FUNCTION_FOR_DIVISION")
def _division(ctx, obj):
    # RegEx.IsMatch(Expression,"\]\s*\/(?!\/)(?!\*)") or RegEx.IsMatch(Expression,
    # "\)\s*\/(?!\/)(?!\*)")
    dax = _expression(obj)
    return _matches(r"\]\s*\/(?!\/)(?!\*)", dax) or _matches(r"\)\s*\/(?!\/)(?!\*)", dax)


@check("AVOID_USING_THE_IFERROR_FUNCTION")
def _iferror(ctx, obj):
    # RegEx.IsMatch(Expression,"(?i)IFERROR\s*\(")
    return _matches(r"(?i)IFERROR\s*\(", _expression(obj))


def _dax_object_name(name: str) -> str:
    """A measure's DaxObjectName: [Name] with ] as ]]."""
    return "[" + name.replace("]", "]]") + "]"


@check("MEASURES_SHOULD_NOT_BE_DIRECT_REFERENCES_OF_OTHER_MEASURES")
def _direct_measure_reference(ctx, obj):
    # Model.AllMeasures.Any(DaxObjectName == current.Expression)
    return any(_dax_object_name(m.name) == obj.item.expression for m in ctx.model.all_measures)


# Python's re allows (?i) only at the start; the rules repeat it later, which in .NET only
# re-applies the flag already set
_FILTER_COLUMN_VALUES = (
    r"(?i)CALCULATE\s*\(\s*[^,]+,\s*FILTER\s*\(\s*\'*[A-Za-z0-9 _]+'*\s*,\s*\'*[A-Za-z0-9 _]+\'*"
    r"\[[A-Za-z0-9 _]+\]",
    r"(?i)CALCULATETABLE\s*\([^,]*,\s*FILTER\s*\(\s*\'*[A-Za-z0-9 _]+\'*,\s*\'*[A-Za-z0-9 _]+\'*"
    r"\[[A-Za-z0-9 _]+\]",
)


@check("FILTER_COLUMN_VALUES")
def _filter_column_values(ctx, obj):
    # RegEx.IsMatch(Expression,"(?i)CALCULATE\s*\(\s*[^,]+,\s*(?i)FILTER\s*\(\s*\'*[A-Za-z0-9 _]+
    # '*\s*,\s*\'*[A-Za-z0-9 _]+\'*\[[A-Za-z0-9 _]+\]") or RegEx.IsMatch(Expression,
    # "(?i)CALCULATETABLE\s*\([^,]*,\s*(?i)FILTER\s*\(\s*\'*[A-Za-z0-9 _]+\'*,\s*\'*
    # [A-Za-z0-9 _]+\'*\[[A-Za-z0-9 _]+\]")
    dax = _expression(obj)
    return any(_matches(pattern, dax) for pattern in _FILTER_COLUMN_VALUES)


_FILTER_MEASURE_VALUES = (
    r"(?i)CALCULATE\s*\(\s*[^,]+,\s*FILTER\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*,\s*\[[^\]]+\]",
    r"(?i)CALCULATETABLE\s*\([^,]*,\s*FILTER\s*\(\s*\'*[A-Za-z0-9 _]+\'*,\s*\[",
)


@check("FILTER_MEASURE_VALUES_BY_COLUMNS")
def _filter_measure_values(ctx, obj):
    # RegEx.IsMatch(Expression,"(?i)CALCULATE\s*\(\s*[^,]+,\s*(?i)FILTER\s*\(\s*\'*
    # [A-Za-z0-9 _]+\'*\s*,\s*\[[^\]]+\]") or RegEx.IsMatch(Expression,
    # "(?i)CALCULATETABLE\s*\([^,]*,\s*(?i)FILTER\s*\(\s*\'*[A-Za-z0-9 _]+\'*,\s*\[")
    dax = _expression(obj)
    return any(_matches(pattern, dax) for pattern in _FILTER_MEASURE_VALUES)


@check("INACTIVE_RELATIONSHIPS_THAT_ARE_NEVER_ACTIVATED")
def _inactive_never_activated(ctx, obj):
    # IsActive == false and not ( Model.AllMeasures.Any(RegEx.IsMatch(Expression,
    # "(?i)USERELATIONSHIP\s*\(\s*\'*" + current.FromTable.Name + "\'*\[" +
    # current.FromColumn.Name + "\]\s*,\s*\'*" + current.ToTable.Name + "\'*\[" +
    # current.ToColumn.Name + "\]")) or Model.AllCalculationItems.Any(the same) )
    rel = obj.item
    if rel.is_active:
        return False
    pattern = re.compile(
        r"(?i)USERELATIONSHIP\s*\(\s*\'*" + re.escape(rel.from_table) + r"\'*\["
        + re.escape(rel.from_column) + r"\]\s*,\s*\'*" + re.escape(rel.to_table) + r"\'*\["
        + re.escape(rel.to_column) + r"\]"
    )
    items = [item for table in ctx.model.tables for item in table.calculation_items]
    return not any(pattern.search(m.expression or "") for m in ctx.model.all_measures + items)


@check("AVOID_USING_'1-(X/Y)'_SYNTAX")
def _one_minus_ratio(ctx, obj):
    # RegEx.IsMatch(Expression,"[0-9]+\s*[-+]\s*[\(]*\s*(?i)SUM\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*
    # \[[A-Za-z0-9 _]+\]\s*\)\s*\/") or RegEx.IsMatch(Expression,"[0-9]+\s*[-+]\s*(?i)DIVIDE\s*\(")
    # ((?i) moved to the front: nothing before it has letters)
    dax = _expression(obj)
    return _matches(
        r"(?i)[0-9]+\s*[-+]\s*[\(]*\s*SUM\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*\[[A-Za-z0-9 _]+\]"
        r"\s*\)\s*\/",
        dax,
    ) or _matches(r"(?i)[0-9]+\s*[-+]\s*DIVIDE\s*\(", dax)


@check("EVALUATEANDLOG_SHOULD_NOT_BE_USED_IN_PRODUCTION_MODELS")
def _evaluateandlog(ctx, obj):
    # RegEx.IsMatch(Expression,"(?i)EVALUATEANDLOG\s*\(")
    return _matches(r"(?i)EVALUATEANDLOG\s*\(", _expression(obj))


@check("EXPRESSION_RELIANT_OBJECTS_MUST_HAVE_AN_EXPRESSION")
def _no_expression(ctx, obj):
    # string.IsNullOrWhiteSpace(Expression)
    return not _expression(obj).strip()


@check("AVOID_THE_USERELATIONSHIP_FUNCTION_AND_RLS_AGAINST_THE_SAME_TABLE")
def _userelationship_and_rls(ctx, obj):
    # Model.AllMeasures.Any(RegEx.IsMatch(Expression,"(?i)USERELATIONSHIP\s*\(\s*.+?(?=])\]\s*,
    # \s*'*" + current.Name + "'*\[")) and RowLevelSecurity.Any(it <> null)
    name = obj.item.name
    if not ctx.rls.get(name):
        return False
    pattern = re.compile(
        r"(?i)USERELATIONSHIP\s*\(\s*.+?(?=])\]\s*,\s*'*" + re.escape(name) + r"'*\["
    )
    return any(pattern.search(m.expression or "") for m in ctx.model.all_measures)


@check("UNNECESSARY_COLUMNS")
def _unnecessary_columns(ctx, obj):
    # (IsHidden or Table.IsHidden) and ReferencedBy.Count = 0 and (not UsedInRelationships.Any())
    # and (not UsedInSortBy.Any()) and (not UsedInHierarchies.Any())
    # and (not Table.RowLevelSecurity.Any(it <> null and
    #     it.IndexOf("[" + current.Name + "]", "OrdinalIgnoreCase") >= 0))
    # and (not Model.Roles.Any(RowLevelSecurity.Any(it <> null and (
    #     it.IndexOf(current.Table.Name + "[" + current.Name + "]", "OrdinalIgnoreCase") >= 0 or
    #     it.IndexOf("'" + current.Table.Name + "'[" + current.Name + "]", ...) >= 0))))
    # and not (ObjectLevelSecurity.Any(it.ToString() == "None"))
    # and not (Table.ObjectLevelSecurity.Any(it.ToString() == "None"))
    # (object-level security is not read from the model: taken as not set)
    column = obj.item
    key = (column.table, column.name)
    if not ctx.hidden(column) or ctx.referenced("Column", *key):
        return False
    if ctx.column_rels.get(key) or key in ctx.sort_targets or key in ctx.hierarchy_columns:
        return False
    name = column.name.lower()
    if any(f"[{name}]" in dax.lower() for dax in ctx.rls.get(column.table, [])):
        return False
    qualified = (
        f"{column.table}[{column.name}]".lower(),
        f"'{column.table}'[{column.name}]".lower(),
    )
    return not any(
        text in dax.lower() for filters in ctx.rls.values() for dax in filters for text in qualified
    )


@check("UNNECESSARY_MEASURES")
def _unnecessary_measures(ctx, obj):
    # (Table.IsHidden or IsHidden) and ReferencedBy.Count = 0
    measure = obj.item
    hidden = measure.is_hidden or (obj.table is not None and obj.table.is_hidden)
    return hidden and not ctx.referenced("Measure", measure.table, measure.name)


_AGGREGATIONS = (
    "COUNT", "COUNTBLANK", "SUM", "AVERAGE", "VALUES", "DISTINCT", "DISTINCTCOUNT", "MIN", "MAX",
    "COUNTA", "AVERAGEA", "MAXA", "MINA",
)  # fmt: skip


@check("HIDE_FACT_TABLE_COLUMNS")
def _hide_fact_table_columns(ctx, obj):
    # ( ReferencedBy.AllMeasures.Any(RegEx.IsMatch(Expression,"(?i)COUNT\s*\(\s*\'*" +
    #   outerit.Table.Name + "\'*\[" + outerit.Name + "\]\s*\)")) or ... COUNTBLANK, SUM,
    #   AVERAGE, VALUES, DISTINCT, DISTINCTCOUNT, MIN, MAX, COUNTA, AVERAGEA, MAXA, MINA alike )
    # and IsHidden == false and (DataType == "Int64" || DataType == "Decimal" ||
    # DataType == "Double")
    column = obj.item
    if column.is_hidden or _te_type(column) not in ("Int64", "Decimal", "Double"):
        return False
    pattern = re.compile(
        r"(?i)(?:" + "|".join(_AGGREGATIONS) + r")\s*\(\s*\'*" + re.escape(column.table)
        + r"\'*\[" + re.escape(column.name) + r"\]\s*\)"
    )
    measures = ctx.measures_referencing("Column", column.table, column.name)
    return any(pattern.search(m.expression or "") for m in measures)


# ============================================================================
# Engine
# ============================================================================


def _ignored_rules(annotations: dict[str, str]) -> set[str]:
    """Rule IDs in a BestPracticeAnalyzer_IgnoreRules annotation ({"RuleIDs": [...]})."""
    text = annotations.get(IGNORE_ANNOTATION)
    if not text:
        return set()
    try:
        ids = json.loads(text).get("RuleIDs") or []
    except (ValueError, AttributeError):
        return set()
    return {str(rule_id).upper() for rule_id in ids}


def rule_findings(
    rule: CustomRule, ctx: "_Context", objects: list[BpaObject]
) -> list[BpaViolation]:
    """
    One custom rule's findings: the objects of its scope that break it, minus the ones whose
    (or whose model's) ignore annotation lists its id.

    Args:
        rule: A validated custom rule (bpa_custom)
        ctx: The model's _Context
        objects: iter_objects(model)

    Returns:
        Findings in model order
    """
    rule_id = rule.id.upper()
    if rule_id in _ignored_rules(ctx.model.annotations):
        return []
    scopes = CUSTOM_SCOPES[rule.scope]
    return [
        BpaViolation(
            object_type=obj.object_type,
            object_name=obj.object_name,
            rule=rule.name,
            category=rule.category,
            severity=rule.severity,
            description=rule.description,
        )
        for obj in objects
        if obj.scope in scopes
        and rule_id not in _ignored_rules(obj.annotations)
        and breaks(ctx, obj, rule)
    ]


def run_bpa(
    model: SemanticModel,
    statistics: Optional[LiveStatistics] = None,
    rules_path: Path = DEFAULT_BPA_RULES,
    custom: Optional[list[CustomRule]] = None,
    disabled: tuple[str, ...] | list[str] = (),
) -> tuple[list[BpaViolation], list[tuple[str, str]]]:
    """
    Check the model against the Best Practice Analyzer rules, in Python.

    Rules whose CompatibilityLevel is above the model's are skipped, as Tabular Editor does.
    Objects (or the model) with a BestPracticeAnalyzer_IgnoreRules annotation listing a rule
    get no finding for it; the model's list applies to every object. This holds for custom
    rules' ids too.

    Args:
        model: Model from semantic_model.read_model()
        statistics: Row counts / distinct values (from the .pbix, the service or Desktop) for
            the rules on cardinality and table size; None: those rules are "not checked"
        rules_path: Rules file (default: Microsoft's standard rules)
        custom: Custom rules (bpa_custom.load_rule_file().rules), checked after the built-ins
        disabled: Built-in rule ids switched off: neither findings nor "not checked"

    Returns:
        (violations most severe first, [(rule ID, reason)] for the rules not checked)
    """
    ctx = _Context(model, statistics)
    objects = list(iter_objects(model))
    model_ignored = _ignored_rules(model.annotations)
    level = model.compatibility_level or 0
    switched_off = {rule_id.upper() for rule_id in disabled}

    violations: list[BpaViolation] = []
    not_checked: list[tuple[str, str]] = []
    for rule in load_rules(rules_path):
        if rule.id.upper() in switched_off:
            continue
        if level and rule.compatibility_level > level:
            logger.debug(f"BPA rule {rule.id} skipped: needs compatibility level "
                         f"{rule.compatibility_level}")
            continue
        function = CHECKS.get(rule.id)
        if function is None:
            not_checked.append((rule.id, NOT_OFFLINE))
            continue
        requires = REQUIRES.get(rule.id)
        if requires is not None and not requires(ctx):
            not_checked.append((rule.id, NEEDS_STATISTICS))
            continue
        if rule.id.upper() in model_ignored:
            continue

        found = []
        try:
            for obj in objects:
                if obj.scope not in rule.scope:
                    continue
                if rule.id.upper() in _ignored_rules(obj.annotations):
                    continue
                if function(ctx, obj):
                    found.append(
                        BpaViolation(
                            object_type=obj.object_type,
                            object_name=obj.object_name,
                            rule=rule.name,
                            category=rule.category,
                            severity=rule.severity,
                            description=rule.description,
                        )
                    )
        except Exception as e:  # a bug in one check must not fail the run
            logger.warning(f"Best Practice Analyzer rule {rule.id} could not be checked: {e!r}")
            not_checked.append((rule.id, f"error: {e}"))
            continue
        violations.extend(found)

    for custom_rule in custom or ():
        try:
            violations.extend(rule_findings(custom_rule, ctx, objects))
        except Exception as e:  # e.g. a property a model object lacks: never fail the run
            logger.warning(
                f"Custom model quality rule {custom_rule.id} could not be checked: {e!r}"
            )
            not_checked.append((custom_rule.id, f"error: {e}"))

    violations.sort(key=lambda v: (_SEVERITY_ORDER.get(v.severity, 3), v.category, v.rule))
    return violations, not_checked
