"""Tests for the Python Best Practice Analyzer (bpa.py) and the model properties it reads."""

import copy
import json
import logging
import zipfile

import pytest

from pbixtractor import bpa
from pbixtractor.bpa import (
    DAX_RULES,
    NEEDS_STATISTICS,
    load_rules,
    rule_by_id,
    run_bpa,
)
from pbixtractor.live_model import ColumnStatistics, LiveStatistics
from pbixtractor.model_sheets import bpa_by_object, bpa_cell, not_checked_rows
from pbixtractor.pipeline import ExtractionOptions, run_extraction
from pbixtractor.semantic_model import parse_model
from pbixtractor.tmdl import parse_tmdl, to_tmsl

from .sample_layout import write_sample_pbix
from .test_semantic_model import BIM

# ============================================================================
# Hand-built models
# ============================================================================


def col(name, data_type="int64", **extra):
    return {"name": name, "dataType": data_type, "sourceColumn": name, **extra}


def calc(name, data_type="int64", **extra):
    return {"name": name, "dataType": data_type, "type": "calculated", "expression": "1", **extra}


def table(name, columns=(), measures=(), partitions=None, **extra):
    if partitions is None:
        partitions = [{"name": name, "source": {"type": "m", "expression": "let x = 1 in x"}}]
    return {
        "name": name,
        "columns": list(columns),
        "measures": list(measures),
        "partitions": partitions,
        **extra,
    }


def calc_table(name, columns=(), **extra):
    partitions = [{"name": name, "source": {"type": "calculated", "expression": "{1}"}}]
    return table(name, columns, partitions=partitions, **extra)


def rel(from_table, from_column, to_table, to_column, **extra):
    return {
        "name": f"{from_table}-{to_table}",
        "fromTable": from_table,
        "fromColumn": from_column,
        "toTable": to_table,
        "toColumn": to_column,
        **extra,
    }


def db(tables, relationships=(), level=1567, **model):
    return {
        "name": "Test",
        "compatibilityLevel": level,
        "model": {"tables": list(tables), "relationships": list(relationships), **model},
    }


def fact_dim(rel_extra=None, fact_column=None, dim_column=None, **model):
    """Fact[Key] -> Dim[Key]."""
    return db(
        [
            table("Fact", [fact_column or col("Key")]),
            table("Dim", [dim_column or col("Key")]),
        ],
        [rel("Fact", "Key", "Dim", "Key", **(rel_extra or {}))],
        **model,
    )


def findings(database, statistics=None, rule_id=None):
    violations, _ = run_bpa(parse_model(database), statistics)
    if rule_id is None:
        return violations
    name = rule_by_id(rule_id).name
    return [v for v in violations if v.rule == name]


def fires(rule_id, database, object_name, statistics=None) -> bool:
    return any(
        v.object_name == object_name for v in findings(database, statistics, rule_id)
    )


def ignore(*rule_ids) -> list[dict]:
    return [{"name": bpa.IGNORE_ANNOTATION, "value": json.dumps({"RuleIDs": list(rule_ids)})}]


MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
MEASURE = {"name": "Total", "expression": "1", "formatString": "#,0"}
ROWS = LiveStatistics(table_rows={"Fact": 30_000_000, "Dim": 10})
FEW_ROWS = LiveStatistics(table_rows={"Fact": 100, "Dim": 10})
CARDINALITY = LiveStatistics(
    columns={("Fact", "Key"): ColumnStatistics("Fact", "Key", distinct_values=200_000)}
)
LOW_CARDINALITY = LiveStatistics(
    columns={("Fact", "Key"): ColumnStatistics("Fact", "Key", distinct_values=10)}
)
BOTH = {"crossFilteringBehavior": "bothDirections"}
MANY = {"toCardinality": "many"}


# Each non-DAX rule: (rule ID, a model where it fires, the object, a model where it does not,
# statistics for both runs)
CASES = [
    ("AVOID_FLOATING_POINT_DATA_TYPES",
     db([table("T", [col("A", "double")])]), "'T'[A]",
     db([table("T", [col("A", "decimal")])]), None),
    ("ISAVAILABLEINMDX_FALSE_NONATTRIBUTE_COLUMNS",
     db([table("T", [col("A", isHidden=True)])]), "'T'[A]",
     db([table("T", [col("A", isHidden=True, isAvailableInMdx=False)])]), None),
    ("AVOID_BI-DIRECTIONAL_RELATIONSHIPS_AGAINST_HIGH-CARDINALITY_COLUMNS",
     fact_dim(BOTH), "'Fact'[Key]", fact_dim(BOTH), "cardinality"),
    ("REDUCE_USAGE_OF_LONG-LENGTH_COLUMNS_WITH_HIGH_CARDINALITY",
     db([table("T", [col("A", annotations=[{"name": "LongLengthRowCount", "value": "600000"}])])]),
     "'T'[A]",
     db([table("T", [col("A", annotations=[{"name": "LongLengthRowCount", "value": "10"}])])]),
     None),
    ("SPLIT_DATE_AND_TIME",
     db([table("T", [col("A", "dateTime",
                         annotations=[{"name": "DateTimeWithHourMinSec", "value": "3"}])])]),
     "'T'[A]",
     db([table("T", [col("A", "dateTime",
                         annotations=[{"name": "DateTimeWithHourMinSec", "value": "0"}])])]),
     None),
    ("LARGE_TABLES_SHOULD_BE_PARTITIONED", fact_dim(), "'Fact'", fact_dim(), "rows"),
    ("SNOWFLAKE_SCHEMA_ARCHITECTURE",
     db([table("A", [col("K")]), table("T", [col("K"), col("L")]), table("B", [col("L")])],
        [rel("A", "K", "T", "K"), rel("T", "L", "B", "L")]), "'T'",
     db([table("A", [col("K")]), table("T", [col("K")])], [rel("A", "K", "T", "K")]), None),
    ("MODEL_SHOULD_HAVE_A_DATE_TABLE",
     db([table("T", [col("A")])]), "Model",
     db([table("D", [col("Date", "dateTime", isKey=True)], dataCategory="Time")]), None),
    ("DATE/CALENDAR_TABLES_SHOULD_BE_MARKED_AS_A_DATE_TABLE",
     db([table("Calendar", [col("Date", "dateTime")])]), "'Calendar'",
     db([table("Calendar", [col("Date", "dateTime", isKey=True)], dataCategory="Time")]), None),
    ("REMOVE_AUTO-DATE_TABLE",
     db([calc_table("LocalDateTable_1", [col("Date", "dateTime")])]), "'LocalDateTable_1'",
     db([table("LocalDateTable_1", [col("Date", "dateTime")])]), None),
    ("AVOID_EXCESSIVE_BI-DIRECTIONAL_OR_MANY-TO-MANY_RELATIONSHIPS",
     fact_dim(BOTH), "Model", fact_dim(), None),
    ("MODEL_USING_DIRECT_QUERY_AND_NO_AGGREGATIONS",
     db([table("T", [col("A")], partitions=[
         {"name": "T", "mode": "directQuery", "source": {"type": "m", "expression": "x"}}])],
        defaultPowerBIDataSourceVersion="powerBI_V3"), "Model",
     db([table("T", [col("A", alternateOf={"summarization": "sum"})], partitions=[
         {"name": "T", "mode": "directQuery", "source": {"type": "m", "expression": "x"}}])],
        defaultPowerBIDataSourceVersion="powerBI_V3"), None),
    ("MINIMIZE_POWER_QUERY_TRANSFORMATIONS",
     db([table("T", [col("A")], partitions=[
         {"name": "P", "source": {"type": "m", "expression": "Table.AddColumn(x, \"a\")"}}])]),
     "P", db([table("T", [col("A")], partitions=[
         {"name": "P", "source": {"type": "m", "expression": "let x = 1 in x"}}])]), None),
    ("AVOID_USING_MANY-TO-MANY_RELATIONSHIPS_ON_TABLES_USED_FOR_DYNAMIC_ROW_LEVEL_SECURITY",
     fact_dim(MANY, roles=[{"name": "R", "tablePermissions": [
         {"name": "Fact", "filterExpression": "Fact[Key] = 1"}]}]), "'Fact'",
     fact_dim(roles=[{"name": "R", "tablePermissions": [
         {"name": "Fact", "filterExpression": "Fact[Key] = 1"}]}]), None),
    ("UNPIVOT_PIVOTED_(MONTH)_DATA",
     db([table("T", [col(m) for m in MONTHS])]), "'T'",
     db([table("T", [col(m) for m in MONTHS[:5]])]), None),
    ("MANY-TO-MANY_RELATIONSHIPS_SHOULD_BE_SINGLE-DIRECTION",
     fact_dim({**MANY, **BOTH}), "'Fact'[Key] <--> 'Dim'[Key]", fact_dim(MANY), None),
    ("REDUCE_USAGE_OF_CALCULATED_TABLES",
     db([calc_table("C", [col("A")])]), "'C'", db([table("C", [col("A")])]), None),
    ("REMOVE_REDUNDANT_COLUMNS_IN_RELATED_TABLES",
     db([table("Fact", [col("Key"), col("Name", "string")]),
         table("Dim", [col("Key"), col("Name", "string")])],
        [rel("Fact", "Key", "Dim", "Key")]), "'Fact'[Name]",
     db([table("Fact", [col("Key"), col("Name", "string")]),
         table("Dim", [col("Key"), col("Name", "string")])]), None),
    ("REDUCE_NUMBER_OF_CALCULATED_COLUMNS",
     db([table("T", [calc(f"C{i}") for i in range(6)])]), "Model",
     db([table("T", [calc(f"C{i}") for i in range(5)])]), None),
    ("CHECK_IF_BI-DIRECTIONAL_AND_MANY-TO-MANY_RELATIONSHIPS_ARE_VALID",
     fact_dim(BOTH), "'Fact'[Key] <--> 'Dim'[Key]", fact_dim(), None),
    ("DATA_COLUMNS_MUST_HAVE_A_SOURCE_COLUMN",
     db([table("T", [{"name": "A", "dataType": "int64"}])]), "'T'[A]",
     db([table("T", [col("A")])]), None),
    ("AVOID_STRUCTURED_DATA_SOURCES_WITH_PROVIDER_PARTITIONS",
     db([table("T", [col("A")], partitions=[
         {"name": "P", "source": {"type": "query", "query": "SELECT 1", "dataSource": "S"}}])],
        dataSources=[{"name": "S", "type": "structured"}]), "P",
     db([table("T", [col("A")], partitions=[
         {"name": "P", "source": {"type": "query", "query": "SELECT 1", "dataSource": "S"}}])],
        dataSources=[{"name": "S", "type": "provider"}]), None),
    ("RELATIONSHIP_COLUMNS_SAME_DATA_TYPE",
     fact_dim(dim_column=col("Key", "string")), "'Fact'[Key] --> 'Dim'[Key]", fact_dim(), None),
    ("AVOID_INVALID_NAME_CHARACTERS",
     db([table("T", [col("A")], [{**MEASURE, "name": "To\x01tal"}])]), "[To\x01tal]",
     db([table("T", [col("A")], [MEASURE])]), None),
    ("AVOID_INVALID_DESCRIPTION_CHARACTERS",
     db([table("T", [col("A", description="bad\x07")])]), "'T'[A]",
     db([table("T", [col("A", description="two\nlines")])]), None),
    ("SET_ISAVAILABLEINMDX_TO_TRUE_ON_NECESSARY_COLUMNS",
     db([table("T", [col("N", isAvailableInMdx=False), col("M", "string", sortByColumn="N")])]),
     "'T'[N]",
     db([table("T", [col("N", isAvailableInMdx=False), col("M", "string")])]), None),
    ("FIX_REFERENTIAL_INTEGRITY_VIOLATIONS",
     fact_dim({"annotations": [{"name": "Vertipaq_RIViolationInvalidRows", "value": "5"}]}),
     "'Fact'[Key] --> 'Dim'[Key]",
     fact_dim({"annotations": [{"name": "Vertipaq_RIViolationInvalidRows", "value": "0"}]}),
     None),
    ("REMOVE_DATA_SOURCES_NOT_REFERENCED_BY_ANY_PARTITIONS",
     db([table("T", [col("A")])], dataSources=[{"name": "SqlServer", "type": "provider"}]),
     "SqlServer",
     db([table("T", [col("A")], partitions=[{"name": "P", "source": {
         "type": "query", "query": "SELECT 1", "dataSource": "SqlServer"}}])],
        dataSources=[{"name": "SqlServer", "type": "provider"}]), None),
    ("REMOVE_ROLES_WITH_NO_MEMBERS",
     db([table("T", [col("A")])], roles=[{"name": "R"}]), "R",
     db([table("T", [col("A")])], roles=[{"name": "R", "members": [{"memberName": "a@b.c"}]}]),
     None),
    ("ENSURE_TABLES_HAVE_RELATIONSHIPS",
     db([table("Fact", [col("Key")]), table("Dim", [col("Key")])]), "'Fact'", fact_dim(), None),
    ("OBJECTS_WITH_NO_DESCRIPTION",
     db([table("T", [col("A")])]), "'T'",
     db([table("T", [col("A")], description="Sales")]), None),
    ("PERSPECTIVES_WITH_NO_OBJECTS",
     db([table("T", [col("A")])], perspectives=[{"name": "P"}]), "P",
     db([table("T", [col("A")])], perspectives=[{"name": "P", "tables": [{"name": "T"}]}]), None),
    ("CALCULATION_GROUPS_WITH_NO_CALCULATION_ITEMS",
     db([table("G", [col("Name", "string")], calculationGroup={"calculationItems": []})]), "'G'",
     db([table("G", [col("Name", "string")], calculationGroup={
         "calculationItems": [{"name": "YTD", "expression": "SELECTEDMEASURE()"}]})]), None),
    ("PARTITION_NAME_SHOULD_MATCH_TABLE_NAME_FOR_SINGLE_PARTITION_TABLES",
     db([table("T", [col("A")], partitions=[
         {"name": "T-1234", "source": {"type": "m", "expression": "x"}}])]), "'T'",
     db([table("T", [col("A")])]), None),
    ("SPECIAL_CHARS_IN_OBJECT_NAMES",
     db([table("A\tB", [col("A")])]), "A\tB", db([table("AB", [col("A")])]), None),
    ("TRIM_OBJECT_NAMES",
     db([table("T ", [col("A")])]), "T ", db([table("T", [col("A")])]), None),
    ("OBJECTS_SHOULD_NOT_START_OR_END_WITH_A_SPACE",
     db([table("T", [col("A")], [{**MEASURE, "name": " Total"}])]), "[ Total]",
     db([table("T", [col("A")], [MEASURE])]), None),
    ("FORMAT_FLAG_COLUMNS_AS_YES/NO_VALUE_STRINGS",
     db([table("T", [col("IsActive")])]), "'T'[IsActive]",
     db([table("T", [col("IsActive", isHidden=True)])]), None),
    ("DATECOLUMN_FORMATSTRING",
     db([table("T", [col("Order Date", "dateTime")])]), "'T'[Order Date]",
     db([table("T", [col("Order Date", "dateTime", formatString="mm/dd/yyyy")])]), None),
    ("MONTHCOLUMN_FORMATSTRING",
     db([table("T", [col("Month Start", "dateTime")])]), "'T'[Month Start]",
     db([table("T", [col("Month Start", "dateTime", formatString="MMMM yyyy")])]), None),
    ("PROVIDE_FORMAT_STRING_FOR_MEASURES",
     db([table("T", [col("A")], [{"name": "M", "expression": "1"}])]), "[M]",
     db([table("T", [col("A")], [MEASURE])]), None),
    ("NUMERIC_COLUMN_SUMMARIZE_BY",
     db([table("T", [col("A")])]), "'T'[A]",
     db([table("T", [col("A", summarizeBy="none")])]), None),
    ("PERCENTAGE_FORMATTING",
     db([table("T", [col("A")], [{**MEASURE, "formatString": "0%"}])]), "[Total]",
     db([table("T", [col("A")], [{**MEASURE, "formatString": "#,0.0%;-#,0.0%;#,0.0%"}])]),
     None),
    ("INTEGER_FORMATTING",
     db([table("T", [col("A")], [{**MEASURE, "formatString": "0"}])]), "[Total]",
     db([table("T", [col("A")], [MEASURE])]), None),
    ("RELATIONSHIP_COLUMNS_SHOULD_BE_OF_INTEGER_DATA_TYPE",
     fact_dim(fact_column=col("Key", "string"), dim_column=col("Key", "string")), "'Fact'[Key]",
     fact_dim(), None),
    ("ADD_DATA_CATEGORY_FOR_COLUMNS",
     db([table("T", [col("Country", "string")])]), "'T'[Country]",
     db([table("T", [col("Country", "string", dataCategory="Country")])]), None),
    ("HIDE_FOREIGN_KEYS",
     fact_dim(), "'Fact'[Key]", fact_dim(fact_column=col("Key", isHidden=True)), None),
    ("MARK_PRIMARY_KEYS",
     fact_dim(), "'Dim'[Key]", fact_dim(dim_column=col("Key", isKey=True)), None),
    ("FIRST_LETTER_OF_OBJECTS_MUST_BE_CAPITALIZED",
     db([table("T", [col("A")], [{**MEASURE, "name": "total"}])]), "[total]",
     db([table("T", [col("A")], [MEASURE])]), None),
    ("MONTH_(AS_A_STRING)_MUST_BE_SORTED",
     db([table("T", [col("Month Name", "string")])]), "'T'[Month Name]",
     db([table("T", [col("Month Name", "string", sortByColumn="Month No"), col("Month No")])]),
     None),
]

STATISTICS = {"rows": (ROWS, FEW_ROWS), "cardinality": (CARDINALITY, LOW_CARDINALITY)}


# ============================================================================
# The DAX rules
# ============================================================================


def measure(name, expression, **extra):
    return {"name": name, "expression": expression, "formatString": "#,0", **extra}


def role(table_name, dax, name="R"):
    return {"name": name, "tablePermissions": [{"name": table_name, "filterExpression": dax}]}


def with_measures(*measures, columns=None, **model):
    """Table T (columns A and B unless given) with the measures."""
    return db([table("T", columns or [col("A"), col("B")], list(measures))], **model)


def m1_and(expression):
    """T[M1] = SUM ( T[A] ) and T[M2] = expression."""
    return with_measures(measure("M1", "SUM ( T[A] )"), measure("M2", expression))


DIRECT_QUERY = [{"name": "T", "mode": "directQuery", "source": {"type": "m", "expression": "x"}}]
USE_REL = "CALCULATE ( 1, USERELATIONSHIP ( Fact[Key], Dim[Key] ) )"
INACTIVE = "'Fact'[Key] --> 'Dim'[Key]"


def calc_group(item_expression):
    return db([{
        "name": "Group",
        "columns": [col("Name", "string")],
        "calculationGroup": {
            "calculationItems": [{"name": "Half", "expression": item_expression}]
        },
    }])


# Each DAX rule: (rule ID, a model where it fires, the object, a model where it does not).
# Regexes run on the text, as in Tabular Editor; references never count code in comments or
# strings, and an unqualified [X] counts when model_dependencies resolves it.
DAX_CASES = [
    ("REDUCE_USAGE_OF_CALCULATED_COLUMNS_THAT_USE_THE_RELATED_FUNCTION",
     db([table("T", [col("A"), calc("C", expression="related ( D[X] )")])]), "'T'[C]",
     db([table("T", [col("A"), calc("C", expression="T[A] * 2")])])),
    ("LIMIT_ROW_LEVEL_SECURITY_(RLS)_LOGIC",
     with_measures(roles=[role("T", 'UPPER ( T[A] ) = "X"')]), "'T'",
     with_measures(roles=[role("T", "T[A] = 1")])),
    ("MEASURES_USING_TIME_INTELLIGENCE_AND_MODEL_IS_USING_DIRECT_QUERY",
     db([table("T", [col("A")], [measure("M", "TOTALYTD ( SUM ( T[A] ), T[A] )")],
               partitions=DIRECT_QUERY)]), "[M]",
     db([table("T", [col("A")], [measure("M", "TOTALYTD ( SUM ( T[A] ), T[A] )")])])),
    ("CHECK_IF_DYNAMIC_ROW_LEVEL_SECURITY_(RLS)_IS_NECESSARY",
     with_measures(roles=[role("T", "T[A] = userprincipalname()")]), "T",
     with_measures(roles=[role("T", "T[A] = 1")])),
    ("DAX_COLUMNS_FULLY_QUALIFIED",
     with_measures(measure("M", "SUMX ( T, [A] )")), "[M]",
     with_measures(measure("M", "SUMX ( T, T[A] ) // or [A]"))),  # a comment never counts
    ("DAX_MEASURES_UNQUALIFIED",
     m1_and("T[M1] + 1"), "[M2]",
     m1_and('[M1] + LEN ( "T[M1]" )')),  # nor does a string
    ("AVOID_DUPLICATE_MEASURES",
     with_measures(measure("M1", "SUM(T[A])"), measure("M2", "SUM ( T[A] )")), "[M1]",
     with_measures(measure("M1", "SUM ( T[A] )"), measure("M2", "SUM ( T[B] )"))),
    ("USE_THE_TREATAS_FUNCTION_INSTEAD_OF_INTERSECT",
     m1_and("COUNTROWS ( INTERSECT ( VALUES ( T[A] ), VALUES ( T[B] ) ) )"), "[M2]",
     m1_and("CALCULATE ( [M1], TREATAS ( VALUES ( T[A] ), T[B] ) )")),
    ("USE_THE_DIVIDE_FUNCTION_FOR_DIVISION",
     m1_and("[M1] / 2"), "[M2]",
     m1_and("DIVIDE ( [M1], 2 ) // half")),
    ("AVOID_USING_THE_IFERROR_FUNCTION",
     m1_and("IFERROR ( [M1], 0 )"), "[M2]",
     m1_and("COALESCE ( [M1], 0 )")),
    ("MEASURES_SHOULD_NOT_BE_DIRECT_REFERENCES_OF_OTHER_MEASURES",
     m1_and("[M1]"), "[M2]",
     m1_and("[M1] + 1")),
    ("FILTER_COLUMN_VALUES",
     m1_and("CALCULATE ( [M1], FILTER ( T, T[A] = 1 ) )"), "[M2]",
     m1_and("CALCULATE ( [M1], T[A] = 1 )")),
    ("FILTER_MEASURE_VALUES_BY_COLUMNS",
     m1_and("CALCULATE ( [M1], FILTER ( T, [M1] > 1 ) )"), "[M2]",
     m1_and("CALCULATE ( [M1], FILTER ( T, T[A] > 1 ) )")),
    ("INACTIVE_RELATIONSHIPS_THAT_ARE_NEVER_ACTIVATED",
     fact_dim({"isActive": False}), INACTIVE,
     db([table("Fact", [col("Key")], [measure("M", USE_REL)]), table("Dim", [col("Key")])],
        [rel("Fact", "Key", "Dim", "Key", isActive=False)])),
    ("AVOID_USING_'1-(X/Y)'_SYNTAX",
     m1_and("1 - SUM ( T[A] ) / SUM ( T[B] )"), "[M2]",
     m1_and("DIVIDE ( SUM ( T[B] ) - SUM ( T[A] ), SUM ( T[B] ) )")),
    ("EVALUATEANDLOG_SHOULD_NOT_BE_USED_IN_PRODUCTION_MODELS",
     m1_and("EVALUATEANDLOG ( [M1] )"), "[M2]",
     m1_and("[M1] + 0")),
    ("EXPRESSION_RELIANT_OBJECTS_MUST_HAVE_AN_EXPRESSION",
     m1_and("  "), "[M2]",
     m1_and("[M1] + 0")),
    ("AVOID_THE_USERELATIONSHIP_FUNCTION_AND_RLS_AGAINST_THE_SAME_TABLE",
     db([table("Fact", [col("Key")], [measure("M", USE_REL)]), table("Dim", [col("Key")])],
        [rel("Fact", "Key", "Dim", "Key", isActive=False)], roles=[role("Dim", "Dim[Key] > 0")]),
     "'Dim'",
     db([table("Fact", [col("Key")], [measure("M", USE_REL)]), table("Dim", [col("Key")])],
        [rel("Fact", "Key", "Dim", "Key", isActive=False)], roles=[role("Fact", "Fact[Key] > 0")])),
    ("UNNECESSARY_COLUMNS",
     with_measures(measure("M", "1 // T[A]"), columns=[col("A", isHidden=True)]), "'T'[A]",
     db([table("T", [col("A", isHidden=True), calc("C", expression="[A] * 2")])])),  # unqualified
    ("UNNECESSARY_MEASURES",
     with_measures(measure("H", "1", isHidden=True), measure("M", 'LEN ( "[H]" )')), "[H]",
     with_measures(measure("H", "1", isHidden=True), measure("M", "[H] + 1"))),
    ("HIDE_FACT_TABLE_COLUMNS",
     with_measures(measure("M", "sum ( 'T'[A] )")), "'T'[A]",
     with_measures(measure("M", "1 // SUM ( T[A] )"))),
]


def test_every_rule_has_a_check_and_a_test():
    rule_ids = {rule.id for rule in load_rules()}
    assert len(rule_ids) == 71 and len(DAX_RULES) == 21
    assert DAX_RULES <= rule_ids
    assert set(bpa.CHECKS) == rule_ids
    assert {case[0] for case in CASES} == rule_ids - DAX_RULES
    assert {case[0] for case in DAX_CASES} == DAX_RULES


@pytest.mark.parametrize(
    "rule_id, firing, object_name, passing", DAX_CASES, ids=[c[0] for c in DAX_CASES]
)
def test_dax_rule_fires_and_does_not(rule_id, firing, object_name, passing):
    assert fires(rule_id, firing, object_name)
    assert not fires(rule_id, passing, object_name)


def test_dax_rules_on_calculation_items_and_table_permissions():
    found = findings(
        calc_group("SELECTEDMEASURE () / 2"), rule_id="USE_THE_DIVIDE_FUNCTION_FOR_DIVISION"
    )
    assert [(v.object_type, v.object_name) for v in found] == [("Calculation Item", "Half")]
    found = findings(
        with_measures(roles=[role("T", "[A] = USERNAME()")]),
        rule_id="DAX_COLUMNS_FULLY_QUALIFIED",
    )
    assert [(v.object_type, v.object_name) for v in found] == [("Table Permission", "T")]


def test_hide_fact_table_columns_follows_measures_and_types():
    # Hidden or not numeric: no finding
    assert not fires(
        "HIDE_FACT_TABLE_COLUMNS",
        with_measures(measure("M", "SUM ( T[A] )"), columns=[col("A", "string")]),
        "'T'[A]",
    )
    # Names with regex characters are matched literally
    database = db([table("T (x)", [col("A.b")], [measure("M", "SUM ( 'T (x)'[A.b] )")])])
    assert fires("HIDE_FACT_TABLE_COLUMNS", database, "'T (x)'[A.b]")


def test_unnecessary_columns_kept_by_rls_and_relationships():
    hidden = [col("A", isHidden=True)]
    assert not fires(
        "UNNECESSARY_COLUMNS", with_measures(columns=hidden, roles=[role("T", "[a] > 0")]), "'T'[A]"
    )
    assert not fires(
        "UNNECESSARY_COLUMNS",
        fact_dim(fact_column=col("Key", isHidden=True)),
        "'Fact'[Key]",
    )


@pytest.mark.parametrize(
    "rule_id, firing, object_name, passing, statistics", CASES, ids=[c[0] for c in CASES]
)
def test_rule_fires_and_does_not(rule_id, firing, object_name, passing, statistics):
    fire_stats, pass_stats = STATISTICS.get(statistics, (None, None))
    assert fires(rule_id, firing, object_name, fire_stats)
    assert not fires(rule_id, passing, object_name, pass_stats)


def test_findings_carry_the_rule_metadata_and_tabular_editor_names():
    violations = findings(fact_dim(dim_column=col("Key", "string")))
    rule = rule_by_id("RELATIONSHIP_COLUMNS_SAME_DATA_TYPE")
    found = next(v for v in violations if v.rule == rule.name)
    assert (found.object_type, found.category, found.severity, found.description) == (
        "Relationship",
        rule.category,
        "High",
        rule.description,
    )
    # Most severe first, as from Tabular Editor
    order = {"High": 0, "Medium": 1, "Low": 2}
    assert [order[v.severity] for v in violations] == sorted(order[v.severity] for v in violations)


def _types_and_names(database, rule_id):
    return [(v.object_type, v.object_name) for v in findings(database, rule_id=rule_id)]


def test_object_types_and_names():
    """D24: Tabular Editor's GetTypeName and DaxObjectFullName, names with ' and ]."""
    database = db(
        [
            table("O'Brien", [col("A]b", "double"), calc("C]", "double")],
                  [{"name": "M]x", "expression": "1"}],
                  hierarchies=[{"name": "H]y", "levels": [{"name": "L", "column": "A]b"}]}]),
            calc_table("Calc", [{"name": "X", "dataType": "double", "type": "calculatedTableColumn"}]),
        ]
    )  # fmt: skip
    assert set(_types_and_names(database, "AVOID_FLOATING_POINT_DATA_TYPES")) == {
        ("Column", "'O''Brien'[A]]b]"),
        ("Column", "'O''Brien'[C]]]"),  # calculated: still "Column"
        ("Column", "'Calc'[X]"),
    }
    assert ("Measure", "[M]]x]") in _types_and_names(database, "OBJECTS_WITH_NO_DESCRIPTION")
    # A hierarchy's "]" is not escaped in Tabular Editor's name
    hierarchy = next(o for o in bpa.iter_objects(parse_model(database)) if o.scope == "Hierarchy")
    assert (hierarchy.object_type, hierarchy.object_name) == ("Hierarchy", "'O''Brien'[H]y]")
    assert ("Table (Import)", "'O''Brien'") in _types_and_names(
        database, "OBJECTS_WITH_NO_DESCRIPTION"
    )
    assert _types_and_names(db([calc_table("C")]), "REDUCE_USAGE_OF_CALCULATED_TABLES") == [
        ("Calculated Table", "'C'")
    ]
    group = db([table("G", [col("Name", "string")], calculationGroup={"calculationItems": []})])
    assert _types_and_names(group, "CALCULATION_GROUPS_WITH_NO_CALCULATION_ITEMS") == [
        ("Calculation Group Table", "'G'")
    ]


def _m(name, mode=None, source=None):
    partition = {"name": name, "source": source or {"type": "m", "expression": "Table.Sort("}}
    if mode:
        partition["mode"] = mode
    return partition


@pytest.mark.parametrize(
    "partitions, default_mode, table_type, partition_types",
    [
        ([_m("P")], None, "Table (Import)", ["Partition (M - Import)"]),
        ([_m("P", "directQuery")], None, "Table (DirectQuery)", ["Partition (M - DirectQuery)"]),
        ([_m("P", "default")], "directQuery", "Table (DirectQuery)",
         ["Partition (M - DirectQuery)"]),
        ([_m("P", "dual")], None, "Table (Dual)", ["Partition (M - Dual)"]),
        ([_m("P"), _m("Q", "directQuery")], None, "Table (Hybrid)",
         ["Partition (M - Import)", "Partition (M - DirectQuery)"]),
        ([_m("P", "import", {"type": "query", "query": "SELECT 1", "dataSource": "S"})], None,
         "Table (Import)", ["Partition (Legacy - Import)"]),
        ([_m("P", "directLake", {"type": "entity", "entityName": "t"})], None,
         "Table (DQ over AS)", ["Partition (DQ over AS)"]),
        ([_m("P", None, {"type": "policyRange"})], None, "Table (Import)",
         ["Partition (Policy Range)"]),
    ],
)  # fmt: skip
def test_table_and_partition_types_follow_the_partitions(
    partitions, default_mode, table_type, partition_types
):
    extra = {"defaultMode": default_mode} if default_mode else {}
    model = parse_model(db([table("T", [col("A")], partitions=partitions)], **extra))
    objects = list(bpa.iter_objects(model))
    assert [(o.object_type, o.object_name) for o in objects if o.scope == "Table"] == [
        (table_type, "'T'")
    ]
    assert [o.object_type for o in objects if o.scope == "Partition"] == partition_types


def test_other_object_names():
    database = db(
        [table("T", [col("A")])],
        [rel("T", "A", "T", "A")],
        roles=[{"name": "R", "tablePermissions": [{"name": "T", "filterExpression": "1"}]}],
    )
    objects = {o.scope: (o.object_type, o.object_name)
               for o in bpa.iter_objects(parse_model(database))}  # fmt: skip
    assert objects["Model"] == ("Model", "Model")
    assert objects["TablePermission"] == ("Table Permission", "T")  # its table's name
    assert objects["ModelRole"] == ("Role", "R")
    assert objects["Relationship"][0] == "Relationship"


def test_data_source_and_expression_types():
    """D24: Tabular Editor's GetTypeName for data sources and shared expressions."""
    database = db(
        [table("T", [col("A")])],
        dataSources=[{"name": "S", "type": "structured"}, {"name": "L", "type": "provider"}],
        expressions=[{"name": "E", "kind": "m", "expression": "1"}],
    )
    objects = [(o.scope, o.object_type, o.object_name)
               for o in bpa.iter_objects(parse_model(database))]  # fmt: skip
    assert ("StructuredDataSource", "Data Source (Power Query)", "S") in objects
    assert ("ProviderDataSource", "Data Source (Legacy)", "L") in objects
    assert ("NamedExpression", "Expression", "E") in objects


def test_model_quality_column_reads_python_findings():
    violations = findings(db([table("Sales", [col("Amount", "double")])]))
    cells = bpa_by_object(violations)
    assert "Medium: [Performance] Do not use floating point data types" in bpa_cell(
        cells, "Sales", "Amount"
    )
    assert "[Maintenance] Visible objects with no description" in bpa_cell(cells, "Sales")


# ============================================================================
# Ignore annotations, compatibility level, failures, statistics
# ============================================================================


def test_ignore_annotation_on_the_object():
    database = db(
        [
            table(
                "T",
                [
                    col("A", "double", annotations=ignore("AVOID_FLOATING_POINT_DATA_TYPES")),
                    col("B", "double", annotations=ignore("SOME_OTHER_RULE")),
                ],
            )
        ]
    )
    found = {v.object_name for v in findings(database, rule_id="AVOID_FLOATING_POINT_DATA_TYPES")}
    assert found == {"'T'[B]"}


def test_ignore_annotation_on_the_model_applies_to_every_object():
    database = db(
        [table("T", [col("A", "double"), col("B", "double")])],
        annotations=ignore("AVOID_FLOATING_POINT_DATA_TYPES"),
    )
    assert findings(database, rule_id="AVOID_FLOATING_POINT_DATA_TYPES") == []
    assert findings(database, rule_id="NUMERIC_COLUMN_SUMMARIZE_BY")  # other rules still run


def test_ignore_annotation_with_bad_json_ignores_nothing():
    annotations = [{"name": bpa.IGNORE_ANNOTATION, "value": "{not json"}]
    database = db([table("T", [col("A", "double", annotations=annotations)])])
    assert findings(database, rule_id="AVOID_FLOATING_POINT_DATA_TYPES")


def test_rules_above_the_model_compatibility_level_are_skipped(tmp_path):
    rules = json.loads(bpa.DEFAULT_BPA_RULES.read_bytes().decode("utf-8-sig"))
    rule = next(r for r in rules if r["ID"] == "AVOID_FLOATING_POINT_DATA_TYPES")
    rules_file = tmp_path / "rules.json"
    rules_file.write_text(json.dumps([{**rule, "CompatibilityLevel": 1600}]), encoding="utf-8")
    database = db([table("T", [col("A", "double")])], level=1500)

    violations, not_checked = run_bpa(parse_model(database), rules_path=rules_file)
    assert violations == [] and not_checked == []  # skipped, as Tabular Editor does

    database["compatibilityLevel"] = 1600
    violations, _ = run_bpa(parse_model(database), rules_path=rules_file)
    assert [v.object_name for v in violations] == ["'T'[A]"]


def test_a_raising_check_goes_to_not_checked_with_a_warning(monkeypatch, caplog):
    def broken(ctx, obj):
        raise ValueError("bug")

    monkeypatch.setitem(bpa.CHECKS, "AVOID_FLOATING_POINT_DATA_TYPES", broken)
    database = db([table("T", [col("A", "double")])])
    with caplog.at_level(logging.WARNING, logger="pbixtractor"):
        violations, not_checked = run_bpa(parse_model(database))
    reasons = dict(not_checked)
    assert reasons["AVOID_FLOATING_POINT_DATA_TYPES"].startswith("error")
    assert "AVOID_FLOATING_POINT_DATA_TYPES could not be checked" in caplog.text
    assert violations  # the other rules ran


def test_only_rules_needing_statistics_are_not_checked():
    _, not_checked = run_bpa(parse_model(BIM))
    reasons = dict(not_checked)
    assert set(reasons.values()) == {NEEDS_STATISTICS}
    assert reasons["LARGE_TABLES_SHOULD_BE_PARTITIONED"] == NEEDS_STATISTICS
    assert not DAX_RULES & set(reasons)
    assert not any("ticket 04" in reason for reason in reasons.values())


def test_a_rule_without_a_check_is_not_checked(tmp_path):
    rules_file = tmp_path / "rules.json"
    rules_file.write_text(json.dumps([{"ID": "CUSTOM_X", "Name": "X", "Scope": "Model",
                                       "Expression": "1=1"}]), encoding="utf-8")
    _, not_checked = run_bpa(parse_model(BIM), rules_path=rules_file)
    assert not_checked == [("CUSTOM_X", bpa.NOT_OFFLINE)]


def test_statistics_present_and_absent():
    database = fact_dim(BOTH)
    _, without = run_bpa(parse_model(database))
    without = dict(without)
    for rule_id in (
        "LARGE_TABLES_SHOULD_BE_PARTITIONED",
        "AVOID_BI-DIRECTIONAL_RELATIONSHIPS_AGAINST_HIGH-CARDINALITY_COLUMNS",
        "FIX_REFERENTIAL_INTEGRITY_VIOLATIONS",
        "SPLIT_DATE_AND_TIME",
        "REDUCE_USAGE_OF_LONG-LENGTH_COLUMNS_WITH_HIGH_CARDINALITY",
    ):
        assert without[rule_id] == NEEDS_STATISTICS

    stats = LiveStatistics(
        table_rows={"Fact": 30_000_000},
        columns={("Fact", "Key"): ColumnStatistics("Fact", "Key", distinct_values=200_000)},
    )
    violations, with_stats = run_bpa(parse_model(database), stats)
    with_stats = dict(with_stats)
    assert "LARGE_TABLES_SHOULD_BE_PARTITIONED" not in with_stats
    assert "AVOID_BI-DIRECTIONAL_RELATIONSHIPS_AGAINST_HIGH-CARDINALITY_COLUMNS" not in with_stats
    assert with_stats["FIX_REFERENTIAL_INTEGRITY_VIOLATIONS"] == NEEDS_STATISTICS  # no such stat
    names = {v.rule for v in violations}
    assert rule_by_id("LARGE_TABLES_SHOULD_BE_PARTITIONED").name in names


def test_vertipaq_annotations_stand_in_for_statistics():
    database = db(
        [table("Big", [col("A")], annotations=[{"name": "Vertipaq_RowCount", "value": "26000000"}])]
    )
    assert fires("LARGE_TABLES_SHOULD_BE_PARTITIONED", database, "'Big'")


# ============================================================================
# The sample model (tests/test_tabular_editor.py: what Tabular Editor finds on it)
# ============================================================================


def test_sample_model_matches_tabular_editor():
    violations, _ = run_bpa(parse_model({**BIM, "compatibilityLevel": 1604}))
    found = {(v.object_type, v.object_name, v.rule) for v in violations}
    assert (
        "Relationship",
        "'Sales'[Amount] <--> 'Dates'[Year Number]",
        "[Error Prevention] Relationship columns should be of the same data type",
    ) in found
    assert ("Column", "'Dates'[Date Key]", "[Formatting] Hide foreign keys") in found
    # Checked by hand against the rule expressions
    assert ("Column", "'Sales'[Date Key]", "[Formatting] Hide foreign keys") not in found  # hidden
    assert ("Role", "Nordics", "[Maintenance] Remove roles with no members") in found
    assert ("Model", "Model", "[Performance] Model should have a date table") in found
    assert ("Table (Import)", "'Sales'",
            "[Naming Conventions] Partition name should match table name for single partition "
            "tables") in found
    assert ("Table (DQ over AS)", "'Dates'",
            "[Naming Conventions] Partition name should match table name for single partition "
            "tables") not in found
    assert all(v.category and v.severity for v in violations)
    # The DAX rules, checked by hand against the rule expressions
    assert ("Measure", "[Dynamic]",
            "[DAX Expressions] Measures should not be direct references of other measures") in found
    assert ("Relationship", "'Sales'[Amount] <--> 'Dates'[Year Number]",
            "[DAX Expressions] Inactive relationships that are never activated") in found
    assert ("Column", "'Sales'[Amount]", "[Formatting] Hide fact table columns") in found


# ============================================================================
# Model properties the rules read (semantic_model.py, tmdl.py)
# ============================================================================


def test_parse_model_reads_the_properties_the_rules_need():
    database = copy.deepcopy(BIM)
    model_part = database["model"]
    sales = model_part["tables"][0]
    sales["annotations"] = ignore("X")
    sales["columns"][2].update(  # Amount (index 0 is the row number column)
        isAvailableInMdx=False,
        alternateOf={"summarization": "sum"},
        variations=[{"name": "Variation", "defaultColumn": {"table": "Dates", "column": "Month"}}],
    )
    model_part["annotations"] = [{"name": "A", "value": ["line 1", "line 2"]}]
    model_part["dataSources"] = [{"name": "S", "type": "structured"}]
    model_part["perspectives"] = [{"name": "P", "tables": [{"name": "Sales"}]}]
    model_part["defaultPowerBIDataSourceVersion"] = "powerBI_V3"
    model_part["roles"][0]["members"] = [{"memberName": "a@b.c"}]
    model_part["tables"].append({"name": "Group", "calculationGroup": {"calculationItems": []}})

    model = parse_model(database)
    sales_table = model.tables[0]
    assert bpa.IGNORE_ANNOTATION in sales_table.annotations
    amount = sales_table.columns[1]
    assert (amount.is_available_in_mdx, amount.alternate_of) == (False, True)
    assert sales_table.columns[0].is_available_in_mdx is True
    assert model.variation_columns == {("Dates", "Month")}
    assert model.annotations == {"A": "line 1\nline 2"}
    assert [(s.name, s.type) for s in model.data_sources] == [("S", "structured")]
    assert model.perspectives == ["P"] and model.perspective_tables == {"P": ["Sales"]}
    assert model.default_powerbi_datasource_version == "powerBI_V3"
    assert model.roles[0].members == ["a@b.c"]
    assert model.model_name == "Model"
    group = model.tables[-1]
    assert group.is_calculation_group and not group.calculation_items  # an empty group
    assert not sales_table.is_calculated


TMDL = (
    "model Model\n"
    "\tdefaultPowerBIDataSourceVersion: powerBI_V3\n"
    "\n"
    '\tannotation BestPracticeAnalyzer_IgnoreRules = {"RuleIDs":["INTEGER_FORMATTING"]}\n'
    "\n"
    "table Sales\n"
    '\tannotation BestPracticeAnalyzer_IgnoreRules = {"RuleIDs":["OBJECTS_WITH_NO_DESCRIPTION"]}\n'
    "\n"
    "\tmeasure Total = 1\n"
    "\t\tannotation Note = hi\n"
    "\n"
    "\tcolumn Amount\n"
    "\t\tdataType: double\n"
    "\t\tisAvailableInMdx: false\n"
    "\t\tsourceColumn: Amount\n"
    "\n"
    "\t\talternateOf\n"
    "\t\t\tsummarization: sum\n"
    "\n"
    "\t\tvariation Variation\n"
    "\t\t\tisDefault\n"
    "\t\t\tdefaultColumn: Dates.Date\n"
    "\n"
    "\tpartition Sales-1 = m\n"
    "\t\tmode: import\n"
    "\t\tsource = let x = 1 in x\n"
    "\n"
    "role Readers\n"
    "\tmodelPermission: read\n"
    "\n"
    "\tmember 'a@b.c'\n"
    "\n"
    "perspective Finance\n"
    "\tperspectiveTable Sales\n"
)


def test_tmdl_carries_annotations_and_bpa_properties():
    model = parse_model(to_tmsl(parse_tmdl(TMDL)))
    assert "INTEGER_FORMATTING" in model.annotations[bpa.IGNORE_ANNOTATION]
    sales = model.tables[0]
    assert "OBJECTS_WITH_NO_DESCRIPTION" in sales.annotations[bpa.IGNORE_ANNOTATION]
    assert sales.measures[0].annotations == {"Note": "hi"}
    amount = sales.columns[0]
    assert (amount.is_available_in_mdx, amount.alternate_of) == (False, True)
    assert model.variation_columns == {("Dates", "Date")}
    assert model.roles[0].members == ["a@b.c"]
    assert model.perspective_tables == {"Finance": ["Sales"]}
    assert model.default_powerbi_datasource_version == "powerBI_V3"

    rule = rule_by_id("OBJECTS_WITH_NO_DESCRIPTION").name
    violations, _ = run_bpa(model)
    assert not any(v.rule == rule and v.object_name == "'Sales'" for v in violations)
    assert not any(v.rule == rule_by_id("INTEGER_FORMATTING").name for v in violations)


# ============================================================================
# Workbooks and the pipeline
# ============================================================================


def test_not_checked_rows_name_the_rule_and_reason():
    rows = not_checked_rows([("LARGE_TABLES_SHOULD_BE_PARTITIONED", NEEDS_STATISTICS),
                             ("CUSTOM_X", "why")])
    rule = rule_by_id("LARGE_TABLES_SHOULD_BE_PARTITIONED")
    assert rows[0] == [rule.severity, rule.category, rule.name, "Not checked", NEEDS_STATISTICS]
    assert rows[1] == ["", "", "CUSTOM_X", "Not checked", "why"]


def test_pipeline_without_tabular_editor_has_a_bpa_result(tmp_path):
    write_sample_pbix(tmp_path / "Sample.pbix")
    (tmp_path / "Sample.bim").write_text(json.dumps(BIM), encoding="utf-8")
    result = run_extraction(
        ExtractionOptions(
            report_path=tmp_path / "Sample.pbix",
            model_path=tmp_path / "Sample.bim",
            output_dir=tmp_path / "out",
            tabular_editor_analysis=False,
            write_log_file=False,
        )
    )
    assert result.ok, result.message
    documentation = result.documentation
    assert documentation.bpa_violations
    assert {reason for _, reason in documentation.bpa_not_checked} == {NEEDS_STATISTICS}
    assert any(v.category == "DAX Expressions" for v in documentation.bpa_violations)
    assert result.report_json["quality"]

    with zipfile.ZipFile(result.files["workbook"]) as book:
        workbook = book.read("xl/workbook.xml").decode()
        strings = book.read("xl/sharedStrings.xml").decode()
    assert 'name="model quality summary"' in workbook
    assert "Model quality" in strings  # the column
    assert "[Formatting] Hide foreign keys" in strings
    assert "Rules not checked" in strings and NEEDS_STATISTICS in strings
    assert "ticket 04" not in strings
    with zipfile.ZipFile(result.files["data_workbook"]) as book:
        assert "Rules not checked" in book.read("xl/sharedStrings.xml").decode()


def test_a_run_without_tabular_editor_is_a_success(tmp_path, monkeypatch):
    monkeypatch.setattr("pbixtractor.pipeline.find_tabular_editor", lambda *a, **k: None)
    write_sample_pbix(tmp_path / "Sample.pbix")
    (tmp_path / "Sample.bim").write_text(json.dumps(BIM), encoding="utf-8")

    def run(tabular_editor_analysis):
        return run_extraction(
            ExtractionOptions(
                report_path=tmp_path / "Sample.pbix",
                model_path=tmp_path / "Sample.bim",
                output_dir=tmp_path / "out",
                tabular_editor_analysis=tabular_editor_analysis,
                write_log_file=False,
            )
        )

    result = run(True)
    assert result.ok, result.message
    assert "INFO: Live statistics from Power BI Desktop skipped" in result.logs
    # The sample report warns about itself (an unbound field, a broken button) - nothing more
    warnings = [line for line in result.logs.splitlines() if not line.startswith("INFO:")]
    assert warnings == run(False).logs.splitlines()
    assert not any("Tabular Editor" in line for line in warnings)
    documentation = result.documentation
    assert documentation.bpa_violations
    assert {reason for _, reason in documentation.bpa_not_checked} == {NEEDS_STATISTICS}


def test_info_notes_alone_keep_a_run_a_success(tmp_path, monkeypatch):
    from pbixtractor import pipeline

    def run(options, progress):
        pipeline.logger.info("Live statistics from Power BI Desktop skipped")
        return pipeline.ExtractionResult("success", "done")

    monkeypatch.setattr(pipeline, "_run", run)
    options = ExtractionOptions(
        report_path=tmp_path / "R.pbix", model_path=tmp_path / "R.bim", output_dir=tmp_path / "out"
    )
    result = run_extraction(options)
    assert result.status == "success"
    assert "INFO: Live statistics" in result.logs and "log" not in result.files

    def warn(options, progress):
        pipeline.logger.warning("Something is off")
        return pipeline.ExtractionResult("success", "done")

    monkeypatch.setattr(pipeline, "_run", warn)
    result = run_extraction(options)
    assert result.status == "warnings" and result.files["log"].is_file()


# ============================================================================
# Review fixes (ticket 09)
# ============================================================================


def _hidden_column_used_unqualified():
    """T[Amount] is hidden and only used as [Amount] in a T measure (D23: a column)."""
    return db([
        table("Other", [col("Key")], [measure("Amount", "1")]),  # a same-named measure elsewhere
        table("T", [col("Amount", isHidden=True)], [measure("Sum", "SUMX ( T, [Amount] )")]),
    ])  # fmt: skip


def test_an_unqualified_own_column_is_not_unnecessary():
    database = _hidden_column_used_unqualified()
    assert not fires("UNNECESSARY_COLUMNS", database, "'T'[Amount]")
    # ... and it is a column reference, not an unqualified measure
    assert fires("DAX_COLUMNS_FULLY_QUALIFIED", database, "[Sum]")


def test_an_unqualified_own_column_is_not_unused():
    """documentation.find_unused drops every exact dependency's target: this one is a column."""
    from pbixtractor.dax_parser import model_dependencies

    model = parse_model(_hidden_column_used_unqualified())
    targets = {(d.target_type, d.target_table, d.target_name) for d in model_dependencies(model)}
    assert ("Column", "T", "Amount") in targets
    assert ("Measure", "Other", "Amount") not in targets


def test_a_crashing_dependency_index_is_not_cached(monkeypatch, caplog):
    calls = []

    def broken(model):
        calls.append(1)
        raise RuntimeError("bug")

    monkeypatch.setattr(bpa, "model_dependencies", broken)
    database = db([table("T", [col("A", isHidden=True)], [measure("M", "1", isHidden=True)])])
    with caplog.at_level(logging.WARNING, logger="pbixtractor"):
        violations, not_checked = run_bpa(parse_model(database))
    reasons = dict(not_checked)
    for rule_id in ("UNNECESSARY_COLUMNS", "UNNECESSARY_MEASURES"):
        assert reasons[rule_id].startswith("error")  # each rule that asks: "not checked"
    assert len(calls) >= 2  # nothing half-built was cached after the first failure
    assert violations  # the other rules ran


def test_given_dependencies_are_used(monkeypatch):
    """D29: run_bpa takes the run's dependencies instead of computing them again."""
    from pbixtractor.dax_parser import model_dependencies

    model = parse_model(db([table("T", [col("A")], [measure("M", "1", isHidden=True),
                                                     measure("N", "[M]")])]))  # fmt: skip
    dependencies = model_dependencies(model)
    monkeypatch.setattr(bpa, "model_dependencies", lambda m: pytest.fail("computed again"))
    violations, _ = run_bpa(model, dependencies=dependencies)
    name = rule_by_id("UNNECESSARY_MEASURES").name
    assert not any(v.rule == name for v in violations)  # [M] is referenced by N
    violations, _ = run_bpa(model, dependencies=[])  # the given list wins
    assert any(v.rule == name and v.object_name == "[M]" for v in violations)


def test_columns_without_a_distinct_count_are_skipped():
    """D26: only columns with a count are checked; "needs statistics" when none has one."""
    rule_id = "AVOID_BI-DIRECTIONAL_RELATIONSHIPS_AGAINST_HIGH-CARDINALITY_COLUMNS"
    database = fact_dim(BOTH)
    partial = LiveStatistics(columns={
        ("Fact", "Key"): ColumnStatistics("Fact", "Key", distinct_values=None),
        ("Dim", "Key"): ColumnStatistics("Dim", "Key", distinct_values=200_000),
    })  # fmt: skip
    violations, not_checked = run_bpa(parse_model(database), partial)
    assert rule_id not in dict(not_checked)
    name = rule_by_id(rule_id).name
    assert [v.object_name for v in violations if v.rule == name] == ["'Dim'[Key]"]

    none = LiveStatistics(columns={
        ("Fact", "Key"): ColumnStatistics("Fact", "Key", distinct_values=None),
    })  # fmt: skip
    _, not_checked = run_bpa(parse_model(database), none)
    assert dict(not_checked)[rule_id] == NEEDS_STATISTICS


def test_a_rule_the_model_ignores_is_never_not_checked():
    database = fact_dim(annotations=ignore("LARGE_TABLES_SHOULD_BE_PARTITIONED"))
    _, not_checked = run_bpa(parse_model(database))  # no statistics
    reasons = dict(not_checked)
    assert "LARGE_TABLES_SHOULD_BE_PARTITIONED" not in reasons
    assert reasons["SPLIT_DATE_AND_TIME"] == NEEDS_STATISTICS  # others still listed


def test_perspectives_with_unknown_objects_are_not_checked():
    rule_id = "PERSPECTIVES_WITH_NO_OBJECTS"
    database = db([table("T", [col("A")])], perspectives=[{"name": "P"}])
    assert fires(rule_id, database, "P")
    database["model"]["annotations"] = [{"name": bpa.PERSPECTIVE_OBJECTS_UNKNOWN, "value": "true"}]
    violations, not_checked = run_bpa(parse_model(database))
    assert dict(not_checked)[rule_id] == bpa.NO_PERSPECTIVE_OBJECTS
    assert not any(v.rule == rule_by_id(rule_id).name for v in violations)
