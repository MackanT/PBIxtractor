"""Tests for the same item in several models (compare.py)."""

from pbixtractor.compare import compare_entries, differences
from pbixtractor.extractors import visual_title
from pbixtractor.readers import VisualDefinition


def _entry(key, measures=(), columns=(), tables=None, pages=()):
    """A catalog entry: one table "Sales" (unless tables are given) and one report."""
    return {
        "key": key,
        "name": key.title(),
        "tables": tables if tables is not None else [{
            "name": "Sales",
            "sources": ["dbo.Sales"],
            "storage_mode": "import",
            "columns": [{"name": n, "data_type": t, "kind": "data", "expression": None} for n, t in columns],
            "measures": [{"name": n, "expression": dax, "format": "#,0"} for n, dax in measures],
        }],
        "reports": [{"name": f"{key.title()} report", "pages": [
            {"id": f"p{i}", "name": f"Page {i}", "items": items} for i, items in enumerate(pages)
        ]}],
    }


def _group(groups, kind, name):
    return next(g for g in groups[kind] if g["name"] == name)


def test_measures_with_the_same_name_are_compared_without_formatting():
    groups = compare_entries([
        _entry("a", measures=[("Total", "SUM ( Sales[Amount] )"), ("Margin", "[Total] - [Cost]")]),
        _entry("b", measures=[("Total", "sum(Sales[Amount])"), ("Margin", "[Total] - [Cost] * 2")]),
        _entry("c", measures=[("Margin", "[Total]-[Cost]"), ("Only here", "1")]),
    ])
    total = _group(groups, "measure", "Total")
    assert total["differs"] == [] and total["versions"] == 1  # whitespace and case ignored
    margin = _group(groups, "measure", "Margin")
    assert margin["differs"] == ["DAX"] and margin["versions"] == 2
    # Version 1 = the most common definition (a and c), b is the odd one out
    assert {m["entry"]: m["version"] for m in margin["members"]} == {"a": 1, "b": 2, "c": 1}
    assert differences(margin["members"][0], margin["members"][1]) == ["DAX"]
    assert all(g["name"] != "Only here" for g in groups["measure"])  # in one model only
    # Differences first
    assert groups["measure"][0]["name"] == "Margin"


def test_columns_and_tables_are_compared():
    groups = compare_entries([
        _entry("a", columns=[("Amount", "decimal"), ("Region", "string")]),
        _entry("b", columns=[("Amount", "double"), ("Region", "string"), ("Extra", "string")]),
    ])
    assert _group(groups, "column", "Sales[Amount]")["differs"] == ["Data type"]
    assert _group(groups, "column", "Sales[Region]")["differs"] == []
    sales = _group(groups, "table", "Sales")
    assert sales["differs"] == ["Columns"]
    assert sales["note"] == "Columns only some of them have: Extra"


def test_visuals_with_the_same_type_and_title_in_several_reports():
    card = {"id": "v1", "type": "Card", "title": "Revenue YTD", "fields": ["Sales[Revenue YTD]"]}
    other_card = {**card, "id": "v2", "fields": ["Sales[Revenue YTD old]"]}
    untitled = {"id": "v3", "type": "Card", "title": "", "fields": ["Sales[X]"]}
    groups = compare_entries([_entry("a", pages=[[card, untitled]]), _entry("b", pages=[[other_card, untitled]])])
    (revenue,) = groups["visual"]  # untitled visuals are not compared
    assert revenue["name"] == "Card: Revenue YTD" and revenue["differs"] == ["Fields"]
    assert [m["where"] for m in revenue["members"]] == ["A report › Page 0", "B report › Page 0"]
    # Within one report only: nothing to compare
    assert compare_entries([_entry("a", pages=[[card], [other_card]])])["visual"] == []
    # A title one report uses for different fields is generic: not compared
    generic = compare_entries([_entry("a", pages=[[card], [other_card]]), _entry("b", pages=[[card]])])
    assert generic["visual"] == []
    # The same title, the same fields on two pages of one report: still one visual
    repeated = compare_entries([_entry("a", pages=[[card], [card]]), _entry("b", pages=[[other_card]])])
    assert repeated["visual"][0]["differs"] == ["Fields"]


def test_visual_title_from_its_formatting():
    literal = {"title": [{"properties": {"text": {"expr": {"Literal": {"Value": "'Revenue YTD'"}}}}}]}
    assert visual_title(VisualDefinition("v", "card", container_objects=literal)) == "Revenue YTD"
    assert visual_title(VisualDefinition("v", "card")) == ""
