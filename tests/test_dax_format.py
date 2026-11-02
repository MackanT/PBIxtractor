"""Tests for the offline DAX formatter (dax_format.py)."""

import logging

import pytest

from pbixtractor.dax_format import LINE_LENGTH, _signature, format_dax
from pbixtractor.semantic_model import parse_model, read_model

from .sample_tmdl import write_sample_tmdl
from .test_semantic_model import BIM


def _formatted(dax: str) -> str:
    result = format_dax(dax)
    assert format_dax(result) == result  # idempotent
    assert _signature(result) == _signature(dax)  # same tokens
    for line in result.split("\n"):
        assert line == line.rstrip()
    assert result == result.strip("\n")
    return result


def test_short_calls_stay_on_one_line():
    assert _formatted("sum(Sales[Amount])") == "SUM ( Sales[Amount] )"
    assert _formatted("calculate()") == "CALCULATE ( )"
    assert _formatted("[A]+[B]*2") == "[A] + [B] * 2"
    assert _formatted("DIVIDE([A],[B],0)") == "DIVIDE ( [A], [B], 0 )"


def test_a_long_calculate_is_broken_per_argument():
    dax = (
        'CALCULATE(SUM(Sales[Amount]),FILTER(ALL(Dates),Dates[Year]=2024),Sales[Channel]="Online")'
    )
    assert _formatted(dax) == (
        "CALCULATE (\n"
        "    SUM ( Sales[Amount] ),\n"
        "    FILTER ( ALL ( Dates ), Dates[Year] = 2024 ),\n"
        '    Sales[Channel] = "Online"\n'
        ")"
    )


def test_a_call_longer_than_the_line_is_broken():
    long_name = "'" + "Very long table name " * 6 + "'[Column]"
    result = _formatted(f"SUM({long_name})")
    assert result == f"SUM (\n    {long_name}\n)"
    assert len(f"SUM ( {long_name} )") > LINE_LENGTH


def test_one_nested_call_stays_inline_two_break():
    assert _formatted("ROUND(SUM(Sales[Amount]),0)") == "ROUND ( SUM ( Sales[Amount] ), 0 )"
    assert _formatted("IF(ISBLANK(SUM(T[x])),0,1)") == (
        "IF (\n    ISBLANK ( SUM ( T[x] ) ),\n    0,\n    1\n)"
    )
    assert _formatted("CALCULATE([M],ALL(T),ALL(U))") == (
        "CALCULATE (\n    [M],\n    ALL ( T ),\n    ALL ( U )\n)"
    )


def test_var_return_blocks():
    dax = "var a = sum(T[x])\nvar b = a*2 return divide(a,b)"
    assert _formatted(dax) == ("VAR a = SUM ( T[x] )\nVAR b = a * 2\nRETURN\n    DIVIDE ( a, b )")


def test_nested_var_return_blocks():
    dax = "VAR x = VAR y = 1 RETURN y + 1 RETURN CALCULATE([M], VAR z = 2 RETURN T[c] > z)"
    assert _formatted(dax) == (
        "VAR x =\n"
        "    VAR y = 1\n"
        "    RETURN\n"
        "        y + 1\n"
        "RETURN\n"
        "    CALCULATE (\n"
        "        [M],\n"
        "        VAR z = 2\n"
        "        RETURN\n"
        "            T[c] > z\n"
        "    )"
    )


def test_a_long_var_value_goes_to_the_next_line():
    dax = "VAR b = CALCULATE(SUM(T[y]), ALL(T)) RETURN b"
    assert _formatted(dax) == (
        "VAR b =\n    CALCULATE (\n        SUM ( T[y] ),\n        ALL ( T )\n    )\nRETURN\n    b"
    )


def test_comments_keep_their_place():
    # // and -- at the end of a line, on their own line, and /* */ inline
    dax = (
        "// header\n"
        "VAR a = SUM ( T[x] ) // total\n"
        "-- own line\n"
        "RETURN\n"
        "CALCULATE ( // why\n"
        "  a, /* inline */ ALL ( T ), -- after comma\n"
        "  T[y] = 1 )"
    )
    assert _formatted(dax) == (
        "// header\n"
        "VAR a = SUM ( T[x] ) // total\n"
        "-- own line\n"
        "RETURN\n"
        "    CALCULATE ( // why\n"
        "        a, /* inline */\n"
        "        ALL ( T ), -- after comma\n"
        "        T[y] = 1\n"
        "    )"
    )
    assert _formatted("[A] /* why */ + [B]") == "[A] /* why */ + [B]"
    assert _formatted("CALCULATE ( [M],\n /* own */ ALL ( T ), ALL ( U ) )") == (
        "CALCULATE (\n    [M],\n    /* own */ ALL ( T ),\n    ALL ( U )\n)"
    )
    assert _formatted("[A] + [B] // end") == "[A] + [B] // end"


def test_a_line_comment_before_a_comma_does_not_swallow_it():
    result = _formatted("DIVIDE ( [A] // numerator\n, [B] )")
    assert "// numerator\n" in result
    assert result.count(",") == 1


def test_strings_and_names_stay_as_written():
    assert _formatted('IF([A]>0,"say ""hi""","no")') == 'IF ( [A] > 0, "say ""hi""", "no" )'
    assert _formatted("'It''s'[a]]b] * 2") == "'It''s'[a]]b] * 2"
    assert _formatted('FORMAT ( [A], "0.00 %" )') == 'FORMAT ( [A], "0.00 %" )'
    assert _formatted("sales[amount] + 1.50E3") == "sales[amount] + 1.50E3"


def test_keyword_and_function_casing():
    result = _formatted("if(not isblank([A]) && t[x] in {1,2}, true, false)")
    assert result == "IF ( NOT ISBLANK ( [A] ) && t[x] IN { 1, 2 }, TRUE, FALSE )"
    assert _formatted("TOPN(5,T,[M],desc)") == "TOPN ( 5, T, [M], DESC )"
    assert _formatted("percentile.inc(T[x],0.5)") == "PERCENTILE.INC ( T[x], 0.5 )"
    assert _formatted("[A]*-1") == "[A] * -1"


@pytest.mark.parametrize(
    "dax",
    [
        "SUM ( T[x]",
        "SUM ( T[x] ) )",
        '"unclosed',
        "/* unclosed",
        "'unclosed[x]",
        "T[unclosed",
        "VAR x = 1",
        "RETURN 1",
        "{ 1, 2 )",
        "a ; b",
    ],
)
def test_unparseable_dax_is_returned_unchanged(dax, caplog):
    with caplog.at_level(logging.DEBUG, logger="pbixtractor"):
        assert format_dax(dax) == dax
    assert all(record.levelno == logging.DEBUG for record in caplog.records)
    assert caplog.records


def test_empty_and_missing_values():
    assert format_dax("") == ""
    assert format_dax("   ") == "   "
    assert format_dax(None) is None


def test_a_failing_safety_check_returns_the_input(monkeypatch):
    import pbixtractor.dax_format as dax_format

    monkeypatch.setattr(dax_format._Output, "text", lambda self: "SUM ( Other[x] )")
    assert format_dax("SUM(T[x])") == "SUM(T[x])"


def _sample_expressions(tmp_path) -> list[str]:
    models = [parse_model(BIM), read_model(write_sample_tmdl(tmp_path))]
    expressions = []
    for model in models:
        expressions += [m.expression for m in model.all_measures]
        expressions += [m.format_string_expression for m in model.all_measures]
        expressions += [c.expression for c in model.all_columns]
    return [e for e in expressions if e]


def test_every_sample_expression_keeps_its_tokens(tmp_path):
    expressions = _sample_expressions(tmp_path)
    assert len(expressions) >= 4
    for dax in expressions:
        _formatted(dax)
