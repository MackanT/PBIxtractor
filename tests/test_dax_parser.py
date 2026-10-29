"""Tests for the Python DAX tokenizer and exact dependencies (dax_parser.py)."""

import pytest

from pbixtractor.dax_parser import model_dependencies, tokenize
from pbixtractor.semantic_model import parse_model

from .test_semantic_model import BIM

# ============================================================================
# Tokenizer
# ============================================================================

SAMPLES = [
    "",
    "SUM ( Sales[Amount] )",
    "VAR x = 1.5 + .5 * 1E3 - 2e-2\r\nRETURN\tx ^ 2",
    "'Sales Data'[Amount] && [M] || NOT ( a <> b ) == c <= d >= e < f > g & \"t\"",
    "// line\n-- other line\n/* block\n */ { 1, 2 } . x",
    "PERCENTILE.INC ( T[C], 0.5 ) ? ~ ` | @",
    "'Bob''s'[X]]Y] & \"say \"\"hi\"\"\"",
    "/* never closed",
    '"never closed',
    "'never closed",
    "[never closed",
    "Ünïcode_Tåble[Kolumn] + _var1",
]


@pytest.mark.parametrize("dax", SAMPLES)
def test_tokenize_is_lossless(dax):
    tokens = tokenize(dax)
    assert "".join(t.text for t in tokens) == dax
    position = 0
    for token in tokens:
        assert token.position == position and token.text
        position += len(token.text)


def _kinds(dax):
    return [(t.kind, t.text) for t in tokenize(dax) if t.kind != "whitespace"]


def test_token_kinds():
    assert _kinds("VAR x = 1.5 RETURN .5 + 1E3") == [
        ("identifier", "VAR"),
        ("identifier", "x"),
        ("operator", "="),
        ("number", "1.5"),
        ("identifier", "RETURN"),
        ("number", ".5"),
        ("operator", "+"),
        ("number", "1E3"),
    ]
    assert [k for k, _ in _kinds("&& || <= >= <> == + - * / ^ & = < >")] == ["operator"] * 15
    assert [t for _, t in _kinds("&& || <= >= <> ==")] == ["&&", "||", "<=", ">=", "<>", "=="]
    assert _kinds("( ) , { } .") == [("punctuation", c) for c in "(),{}."]
    assert _kinds("? @") == [("unknown", "?"), ("unknown", "@")]
    assert _kinds("PERCENTILE.INC (") == [("identifier", "PERCENTILE.INC"), ("punctuation", "(")]
    assert tokenize(" \r\n\t")[0].kind == "whitespace"


def test_escapes():
    tokens = [t for t in tokenize("'Bob''s'[X]]Y] & \"a\"\"b\"") if t.kind != "whitespace"]
    assert [t.kind for t in tokens] == ["quoted_name", "bracket_name", "operator", "string"]
    assert tokens[0].name == "Bob's"
    assert tokens[1].name == "X]Y"
    assert tokens[3].text == '"a""b"'


def test_comments():
    tokens = tokenize("1 // a\n2 -- b\n3 /* c\nd */ 4")
    comments = [t.text for t in tokens if t.kind == "comment"]
    assert comments == ["// a", "-- b", "/* c\nd */"]
    assert [t.text for t in tokens if t.kind == "number"] == ["1", "2", "3", "4"]


def test_unclosed_comment_and_string_run_to_the_end():
    assert [t.kind for t in tokenize("1 /* x [M]")] == ["number", "whitespace", "comment"]
    assert [t.kind for t in tokenize('1 & "x [M]')] == ["number", "whitespace", "operator",
                                                        "whitespace", "string"]  # fmt: skip
    assert tokenize("[a")[0].name == "a"
    assert tokenize("'a")[0].name == "a"


# ============================================================================
# Dependencies
# ============================================================================


def _model(tables, roles=None):
    return parse_model({"name": "m", "model": {"tables": tables, "roles": roles or []}})


def _table(name, columns=(), measures=(), calculated=None, partitions=None, **extra):
    table = {
        "name": name,
        "columns": [
            c if isinstance(c, dict) else {"name": c, "dataType": "string", "sourceColumn": c}
            for c in columns
        ],
        "measures": [{"name": n, "expression": e} for n, e in measures],
        **extra,
    }
    if calculated is not None:
        table["partitions"] = [{"name": name, "source": {"type": "calculated",
                                                         "expression": calculated}}]  # fmt: skip
    elif partitions:
        table["partitions"] = partitions
    return table


def _found(model):
    return {(d.source_type, d.source_ref, d.target_type, d.target_ref)
            for d in model_dependencies(model)}  # fmt: skip


def _targets(model, table, name):
    return [(d.target_type, d.target_ref) for d in model_dependencies(model)
            if (d.source_table, d.source_name) == (table, name)]  # fmt: skip


SALES = ["Amount", "Qty", {"name": "Big", "type": "calculated", "dataType": "boolean",
                          "expression": "[Amount] > 100 && Sales[Qty] > 1"}]  # fmt: skip


def test_qualified_and_unqualified_columns_and_measures():
    model = _model([
        _table("Sales", SALES, [
            ("Total", "SUM ( Sales[Amount] )"),
            ("Ratio", "DIVIDE ( [total], 'sales'[QTY] )"),  # case-insensitive
            ("Via table", "Sales[Total] * 2"),  # a measure reached via T[M]
            ("Row", "SUMX ( Sales, [Amount] )"),  # [Amount] is no measure: no dependency
        ]),
    ])  # fmt: skip
    assert _targets(model, "Sales", "Total") == [("Column", "Sales[Amount]")]
    assert _targets(model, "Sales", "Ratio") == [
        ("Measure", "Sales[Total]"),
        ("Column", "Sales[Qty]"),
    ]
    assert _targets(model, "Sales", "Via table") == [("Measure", "Sales[Total]")]
    assert _targets(model, "Sales", "Row") == [("Table", "'Sales'")]


def test_calculated_column_resolves_its_own_table_column():
    model = _model([_table("Sales", SALES), _table("Other", ["Amount"])])
    assert _targets(model, "Sales", "Big") == [("Column", "Sales[Amount]"), ("Column", "Sales[Qty]")]


def test_comments_and_strings_are_ignored():
    model = _model([
        _table("Sales", SALES, [
            ("Total", "SUM ( Sales[Amount] )"),
            ("M", "1 // [Total]\n-- Sales[Qty]\n/* COUNTROWS ( Sales ) */ & \"[Total] Sales[Qty]\""),
        ]),
    ])  # fmt: skip
    assert _targets(model, "Sales", "M") == []


def test_unknown_table_gives_nothing():
    model = _model([_table("Sales", SALES, [("M", "SUM ( Nope[Amount] ) + Sales[Nope]")])])
    assert _targets(model, "Sales", "M") == []


def test_var_shadows_a_table_until_its_return_block_ends():
    model = _model([
        _table("Sales", SALES, [
            ("Shadow", "VAR Sales = 1 RETURN Sales + 1"),
            ("Own value", "VAR Sales = COUNTROWS ( Sales ) RETURN Sales"),
            ("Scope ends", "IF ( VAR Sales = 1 RETURN Sales, COUNTROWS ( Sales ) )"),
        ]),
    ])  # fmt: skip
    assert _targets(model, "Sales", "Shadow") == []
    assert _targets(model, "Sales", "Own value") == [("Table", "'Sales'")]
    assert _targets(model, "Sales", "Scope ends") == [("Table", "'Sales'")]


def test_function_with_a_table_name_is_no_table():
    model = _model([
        _table("Calendar", ["Date"], [
            ("Days", "COUNTROWS ( CALENDAR ( DATE ( 2020, 1, 1 ), DATE ( 2020, 12, 31 ) ) )"),
            ("Rows", "COUNTROWS ( Calendar )"),
        ]),
    ])  # fmt: skip
    assert _targets(model, "Calendar", "Days") == []
    assert _targets(model, "Calendar", "Rows") == [("Table", "'Calendar'")]


def test_bare_table_and_redundant_table_ref():
    model = _model([
        _table("Sales", SALES, [
            ("Rows", "COUNTROWS ( Sales )"),
            ("Sum", "SUM ( Sales[Amount] )"),
            ("Both", "SUMX ( Sales, Sales[Amount] ) + COUNTROWS ( 'Sales' )"),
            ("Quoted", "COUNTROWS ( FILTER ( 'Sales Data', TRUE ) )"),
        ]),
        _table("Sales Data", ["X"]),
    ])  # fmt: skip
    assert _targets(model, "Sales", "Rows") == [("Table", "'Sales'")]
    assert _targets(model, "Sales", "Sum") == [("Column", "Sales[Amount]")]
    assert _targets(model, "Sales", "Both") == [("Column", "Sales[Amount]")]  # listed once, no table
    assert _targets(model, "Sales", "Quoted") == [("Table", "'Sales Data'")]


def test_return_before_a_measure_is_no_table():
    model = _model([_table("Sales", SALES, [("A", "1"), ("B", "VAR x = 1 RETURN [A] + NOT [A]")])])
    assert _targets(model, "Sales", "B") == [("Measure", "Sales[A]")]


def test_nameof_counts():
    model = _model([
        _table("Sales", SALES, [("Total", "1")]),
        _table("Pick", ["Name"], calculated='{ ( "Total", NAMEOF ( \'Sales\'[Total] ), 0 ) }'),
    ])  # fmt: skip
    assert ("Table", "'Pick'", "Measure", "Sales[Total]") in _found(model)


def test_rls_filter_and_calculation_item_as_sources():
    model = _model(
        [
            _table("Sales", SALES, [("Total", "SUM ( Sales[Amount] )")]),
            _table("Time Calc", [{"name": "Name", "dataType": "string", "sourceColumn": "Name"}],
                   calculationGroup={"calculationItems": [
                       {"name": "YTD", "expression": "CALCULATE ( SELECTEDMEASURE (), [Total] )"},
                   ]}),  # fmt: skip
        ],
        roles=[{"name": "Big only", "tablePermissions": [
            {"name": "Sales", "filterExpression": "[Amount] > 100"}]}],  # fmt: skip
    )
    found = _found(model)
    assert ("TablePermission", "RLS: Big only (Sales)", "Column", "Sales[Amount]") in found
    assert ("CalculationItem", "Time Calc[YTD]", "Measure", "Sales[Total]") in found


def test_addcolumns_column_gives_no_dependency():
    model = _model([
        _table("Sales", SALES, [
            ("M", 'MAXX ( ADDCOLUMNS ( VALUES ( Sales[Qty] ), "@Extra", 1 ), [@Extra] )'),
        ]),
    ])  # fmt: skip
    assert _targets(model, "Sales", "M") == [("Column", "Sales[Qty]")]


def test_malformed_dax_never_raises():
    model = _model([_table("Sales", SALES, [
        ("M", "VAR VAR RETURN ))) ( , [Unclosed"), ("N", "'Sales"), ("O", "VAR")])])  # fmt: skip
    model_dependencies(model)


def test_sample_model_matches_tabular_editor():
    """The same set test_tabular_editor.py expects from Tabular Editor on this model."""
    assert _found(parse_model(BIM)) == {
        ("Measure", "Sales[Total Amount]", "Column", "Sales[Amount]"),
        ("Measure", "Sales[Dynamic]", "Measure", "Sales[Total Amount]"),
        ("Column", "Sales[Is Big]", "Column", "Sales[Amount]"),
        ("TablePermission", "RLS: Nordics (Sales)", "Column", "Sales[Amount]"),
    }
