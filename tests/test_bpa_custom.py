"""Tests for custom model quality rules (bpa_custom.py) and their place in a run."""

import json
import logging

import pytest
import yaml

from pbixtractor import bpa_custom
from pbixtractor.bpa import load_rules, run_bpa
from pbixtractor.bpa_custom import (
    OPS,
    PROPERTIES,
    SCOPES,
    CustomRule,
    evaluate_rule,
    load_rule_file,
    rule_from_dict,
    save_rule_file,
    validate_rule,
)
from pbixtractor.cli import main
from pbixtractor.model_sheets import bpa_by_object, bpa_cell
from pbixtractor.pipeline import ExtractionOptions, run_extraction
from pbixtractor.semantic_model import parse_model

from .sample_layout import write_sample_pbix
from .test_bpa import calc, col, db, ignore, rel, table
from .test_semantic_model import BIM

# ============================================================================
# A hand-built model
# ============================================================================


def model(**extra):
    """Sales (Amount double, hidden Key, calculated Big), Dim, and measures using them."""
    measures = [
        {"name": "Total", "expression": "SUM ( Sales[Amount] )", "formatString": "#,0",
         "displayFolder": "KPIs", "description": "All sales"},
        {"name": "twice", "expression": "[Total] * 2", "isHidden": True},
        {"name": "Ratio", "expression": "DIVIDE ( [Total], [twice] )", "formatString": "0%"},
    ]
    sales = table(
        "Sales",
        [col("Amount", "double", description="Amount in SEK"), col("Key", isHidden=True),
         calc("Big", "boolean", expression="Sales[Amount] > 100")],
        measures,
        description="Fact",
    )
    dim = table("Dim", [col("Key"), col("Label", "string", displayFolder="Texts")], isHidden=True)
    return parse_model(db([sales, dim], [rel("Sales", "Key", "Dim", "Key")], **extra))


def rule(scope="Measure", conditions=(), any_=(), **extra):
    data = {"id": "R1", "name": "Rule one", "severity": "Medium", "scope": scope,
            "conditions": list(conditions), **extra}
    if any_:
        data["any"] = list(any_)
    return data


def cond(prop, op, value=None, **extra):
    data = {"property": prop, "op": op, **extra}
    if value is not None:
        data["value"] = value
    return data


def found(rule_data, the_model=None) -> list[str]:
    return [v.object_name for v in evaluate_rule(rule_data, the_model or model())]


# ============================================================================
# Ops and properties
# ============================================================================


@pytest.mark.parametrize(
    "condition, expected",
    [
        (cond("name", "equals", "TOTAL"), ["'Sales'[Total]"]),
        (cond("name", "not_equals", "total"), ["'Sales'[twice]", "'Sales'[Ratio]"]),
        (cond("expression", "contains", "divide"), ["'Sales'[Ratio]"]),
        (cond("name", "matches", "^[a-z]"), ["'Sales'[Total]", "'Sales'[twice]", "'Sales'[Ratio]"]),
        (cond("name", "matches", "^t"), ["'Sales'[Total]", "'Sales'[twice]"]),
        (cond("display_folder", "is_empty"), ["'Sales'[twice]", "'Sales'[Ratio]"]),
        (cond("display_folder", "not_empty"), ["'Sales'[Total]"]),
        (cond("format_string", "in", ["0%", "#,0.00"]), ["'Sales'[Ratio]"]),
        (cond("description", "contains", "sales"), ["'Sales'[Total]"]),
        (cond("table", "equals", "sales"), ["'Sales'[Total]", "'Sales'[twice]", "'Sales'[Ratio]"]),
        (cond("hidden", "equals", True), ["'Sales'[twice]"]),
        (cond("hidden", "not_equals", "true"), ["'Sales'[Total]", "'Sales'[Ratio]"]),
        (cond("dependants", "equals", 0), ["'Sales'[Ratio]"]),
        (cond("dependants", "in", [2]), ["'Sales'[Total]"]),
        (cond("dependants", "not_equals", 0), ["'Sales'[Total]", "'Sales'[twice]"]),
        (cond("data_type", "is_empty"), ["'Sales'[Total]", "'Sales'[twice]", "'Sales'[Ratio]"]),
    ],
)
def test_each_op_and_property_on_measures(condition, expected):
    assert found(rule(conditions=[condition])) == expected


def test_column_properties():
    assert found(rule("Column", [cond("data_type", "equals", "double")])) == ["'Sales'[Amount]"]
    assert found(rule("Column", [cond("data_type", "in", ["String", "Boolean"])])) == [
        "'Sales'[Big]", "'Dim'[Label]"
    ]
    assert found(rule("Column", [cond("hidden", "equals", True)])) == ["'Sales'[Key]"]
    assert found(rule("Column", [cond("display_folder", "equals", "texts")])) == ["'Dim'[Label]"]
    assert found(rule("Column", [cond("dependants", "not_equals", 0)])) == ["'Sales'[Amount]"]
    assert found(rule("CalculatedColumn", [cond("expression", "contains", "> 100")])) == [
        "'Sales'[Big]"
    ]


def test_all_conditions_and_one_of_any():
    data = rule(
        conditions=[cond("hidden", "equals", False)],
        any_=[cond("name", "equals", "Ratio"), cond("display_folder", "not_empty")],
    )
    assert found(data) == ["'Sales'[Total]", "'Sales'[Ratio]"]
    # any alone
    assert found(rule(any_=[cond("name", "equals", "twice")])) == ["'Sales'[twice]"]
    # all must hold
    data = rule(conditions=[cond("hidden", "equals", False), cond("name", "equals", "twice")])
    assert found(data) == []


@pytest.mark.parametrize(
    "scope, condition, object_type, object_name",
    [
        ("Measure", cond("name", "equals", "Total"), "Measure", "'Sales'[Total]"),
        ("Column", cond("name", "equals", "Amount"), "Column", "'Sales'[Amount]"),
        ("Column", cond("name", "equals", "Big"), "Calculated Column", "'Sales'[Big]"),
        ("CalculatedColumn", cond("name", "matches", "."), "Calculated Column", "'Sales'[Big]"),
        ("Table", cond("hidden", "equals", True), "Table", "Dim"),
        ("Table", cond("description", "equals", "fact"), "Table", "Sales"),
        ("Relationship", cond("name", "contains", "'Dim'[Key]"), "Relationship",
         "'Sales'[Key] --> 'Dim'[Key]"),
        ("Model", cond("name", "equals", "Model"), "Model", "Model"),
    ],
)
def test_each_scope(scope, condition, object_type, object_name):
    violations = evaluate_rule(rule(scope, [condition]), model())
    assert [(v.object_type, v.object_name) for v in violations] == [(object_type, object_name)]


def test_findings_carry_the_rule_metadata():
    data = rule(conditions=[cond("name", "equals", "Total")], category="House style",
                description="Why", severity="High")
    [violation] = evaluate_rule(data, model())
    assert (violation.rule, violation.category, violation.severity, violation.description) == (
        "Rule one", "House style", "High", "Why"
    )
    # category defaults to Custom
    [violation] = evaluate_rule(rule(conditions=[cond("name", "equals", "Total")]), model())
    assert violation.category == "Custom"


def test_allowed_values_for_the_form():
    assert set(SCOPES) == {"Measure", "Column", "CalculatedColumn", "Table", "Relationship",
                           "Model"}
    assert set(OPS) == {"equals", "not_equals", "contains", "matches", "is_empty", "not_empty",
                        "in"}
    assert set(PROPERTIES) == {"name", "table", "description", "format_string", "data_type",
                               "display_folder", "hidden", "expression", "dependants"}
    assert "Table" not in PROPERTIES["expression"]


# ============================================================================
# Validation and the rule file (D18)
# ============================================================================


@pytest.mark.parametrize(
    "data, problem",
    [
        (rule(id="bad id!"), "bad id"),
        (rule(scope="Hierarchy"), "bad scope"),
        (rule(severity="Urgent"), "bad severity"),
        (rule(conditions=[cond("size", "equals", 1)]), "unknown property 'size'"),
        (rule(conditions=[cond("name", "like", "x")]), "unknown op 'like'"),
        (rule(conditions=[cond("name", "matches", "([")]), "bad regex"),
        (rule("Table", [cond("expression", "is_empty")]), "does not exist on Table"),
        (rule(conditions=[cond("name", "in", "Total")]), "needs a list"),
        (rule(conditions=[cond("name", "equals")]), "needs a value"),
        (rule(conditions=[cond("name", "is_empty", "x")]), "takes no value"),
        (rule(conditions=[]), "no conditions"),
        (rule(id="AVOID_FLOATING_POINT_DATA_TYPES", conditions=[cond("name", "is_empty")]),
         "built-in"),
    ],
)
def test_validate_rule(data, problem):
    errors = validate_rule(data)
    assert any(problem in error for error in errors), errors
    with pytest.raises(ValueError):
        rule_from_dict(data)
    with pytest.raises(ValueError):
        evaluate_rule(data, model())


def test_a_valid_rule_has_no_errors():
    assert validate_rule(rule(conditions=[cond("name", "is_empty")])) == []


def write(path, data):
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path


GOOD = rule(id="GOOD", conditions=[cond("name", "equals", "Total")])


def test_yaml_that_does_not_parse(tmp_path):
    path = tmp_path / "rules.yaml"
    path.write_text("rules: [\n  - id: x\n  bad", encoding="utf-8")
    loaded = load_rule_file(path)
    assert loaded.rules == [] and loaded.disabled == []
    assert len(loaded.errors) == 1 and loaded.errors[0].startswith(f"file {path}:")


def test_invalid_rules_are_dropped_and_named(tmp_path):
    builtin = load_rules()[0].id
    path = write(tmp_path / "rules.yaml", {
        "version": 1,
        "disabled": ["NO_SUCH_RULE", builtin.lower()],
        "rules": [
            GOOD,
            rule(id="BAD_REGEX", conditions=[cond("name", "matches", "([")]),
            {"name": "No id", "scope": "Measure", "severity": "Low",
             "conditions": [cond("name", "is_empty")]},
            dict(GOOD, name="Same id again"),
            rule(id=builtin, conditions=[cond("name", "is_empty")]),
        ],
    })
    loaded = load_rule_file(path)
    assert [r.id for r in loaded.rules] == ["GOOD"]
    assert loaded.disabled == [builtin]
    labels = [error.split(":")[0] for error in loaded.errors]
    assert labels == ["NO_SUCH_RULE", "BAD_REGEX", "rule #3", "GOOD", builtin]
    assert "second rule" in loaded.errors[3]


def test_disabled_builtin_is_neither_a_finding_nor_not_checked():
    the_model = model()
    rule_id = "AVOID_FLOATING_POINT_DATA_TYPES"
    name = next(r.name for r in load_rules() if r.id == rule_id)
    violations, _ = run_bpa(the_model)
    assert any(v.rule == name for v in violations)
    violations, not_checked = run_bpa(the_model, disabled=[rule_id])
    assert not any(v.rule == name for v in violations)
    assert rule_id not in [r for r, _ in not_checked]
    # a rule needing statistics, switched off, is not "not checked" either
    _, not_checked = run_bpa(the_model)
    needing = not_checked[0][0]
    _, not_checked = run_bpa(the_model, disabled=[needing])
    assert needing not in [r for r, _ in not_checked]


def test_custom_rules_run_next_to_the_builtins():
    custom = [rule_from_dict(GOOD)]
    violations, _ = run_bpa(model(), custom=custom)
    assert any(v.rule == "Rule one" and v.object_name == "'Sales'[Total]" for v in violations)
    assert any(v.rule != "Rule one" for v in violations)


def test_custom_id_in_an_ignore_annotation():
    the_model = model()
    the_model.tables[0].measures[0].annotations.update(
        {a["name"]: a["value"] for a in ignore("good")}
    )
    data = rule(id="GOOD", conditions=[cond("name", "matches", "^t")])
    assert found(data, the_model) == ["'Sales'[twice]"]
    # on the model: every object
    the_model = model(annotations=ignore("GOOD"))
    assert found(data, the_model) == []
    violations, _ = run_bpa(the_model, custom=[rule_from_dict(data)])
    assert not any(v.rule == "Rule one" for v in violations)


def test_a_raising_custom_rule_goes_to_not_checked(monkeypatch, caplog):
    def broken(ctx, obj, rule):
        raise RuntimeError("boom")

    monkeypatch.setattr("pbixtractor.bpa.breaks", broken)
    with caplog.at_level(logging.WARNING, logger="pbixtractor"):
        violations, not_checked = run_bpa(model(), custom=[rule_from_dict(GOOD)])
    assert ("GOOD", "error: boom") in not_checked
    assert "Custom model quality rule GOOD could not be checked" in caplog.text
    assert violations  # the built-ins still ran


def test_save_and_load_round_trip(tmp_path):
    rules = [
        rule_from_dict(rule(id="A", conditions=[cond("display_folder", "is_empty")],
                            any_=[cond("name", "matches", "^[A-Z]")], description="Why")),
        CustomRule(id="B", name="Tables described", scope="Table", severity="Low",
                   conditions=(bpa_custom.Condition("description", "is_empty"),)),
    ]
    path = tmp_path / "sub" / "bpa_rules.yaml"
    save_rule_file(path, rules, ["AVOID_FLOATING_POINT_DATA_TYPES"])
    loaded = load_rule_file(path)
    assert loaded.errors == []
    assert loaded.rules == rules
    assert loaded.disabled == ["AVOID_FLOATING_POINT_DATA_TYPES"]
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert list(data) == ["version", "disabled", "rules"] and data["version"] == 1
    assert list(data["rules"][0]) == ["id", "name", "category", "severity", "description",
                                      "scope", "conditions", "any"]
    assert not list(path.parent.glob("*.tmp"))
    # mappings are written as given, in the file's key order
    save_rule_file(path, [{"scope": "Measure", **GOOD}])
    assert list(yaml.safe_load(path.read_text(encoding="utf-8"))["rules"][0])[0] == "id"


def test_evaluate_rule_tests_one_rule():
    assert found(GOOD) == ["'Sales'[Total]"]
    assert found(rule_from_dict(GOOD)) == ["'Sales'[Total]"]


# ============================================================================
# In a run
# ============================================================================


@pytest.fixture
def sample(tmp_path):
    write_sample_pbix(tmp_path / "Sample.pbix")
    (tmp_path / "Sample.bim").write_text(json.dumps(BIM), encoding="utf-8")
    return tmp_path


def _measure(name="Total Amount"):
    return {"id": "TOTAL_CHECK", "name": "Total measures are flagged", "severity": "High",
            "scope": "Measure", "conditions": [{"property": "name", "op": "equals",
                                                "value": name}]}


def _run(sample, **options):
    return run_extraction(
        ExtractionOptions(
            report_path=sample / "Sample.pbix",
            model_path=sample / "Sample.bim",
            output_dir=sample / "out",
            tabular_editor_analysis=False,
            **options,
        )
    )


def test_a_run_uses_the_user_rule_file(sample):
    sales = next(t for t in BIM["model"]["tables"] if t["name"] == "Sales")
    measure = sales["measures"][0]["name"]
    write(bpa_custom.USER_RULES_PATH, {"version": 1, "rules": [_measure(measure)]})
    result = _run(sample)
    assert result.status != "error", result.message
    cells = bpa_by_object(result.documentation.bpa_violations)
    assert "High: Total measures are flagged" in bpa_cell(cells, "Sales", measure)
    assert any(q["rule"] == "Total measures are flagged" for q in result.report_json["quality"])


def test_rule_file_problems_are_warnings_naming_the_rule(sample):
    path = write(sample / "mine.yaml", {
        "rules": [_measure(), dict(_measure(), id="BROKEN", scope="Galaxy")],
        "disabled": ["NOT_A_RULE"],
    })
    result = _run(sample, bpa_rules=path)
    assert result.status == "warnings"
    assert "Custom model quality rule BROKEN: bad scope" in result.logs
    assert "Custom model quality rule NOT_A_RULE:" in result.logs
    assert any(v.rule == "Total measures are flagged"
               for v in result.documentation.bpa_violations)


def test_a_missing_rule_file_is_a_warning(sample):
    result = _run(sample, bpa_rules=sample / "missing.yaml")
    assert result.status == "warnings"
    assert "Custom model quality rule file not found" in result.logs
    assert result.documentation.bpa_violations  # the built-ins still ran


def test_bpa_rules_on_catalog_add(sample, monkeypatch):
    seen = {}

    def fake_run_batch(jobs, settings, **kwargs):
        seen["bpa_rules"] = settings.bpa_rules
        return []

    monkeypatch.setattr("pbixtractor.batch.run_batch", fake_run_batch)
    path = sample / "mine.yaml"
    with pytest.raises(SystemExit):
        main(["catalog", "add", str(sample / "catalog"), str(sample / "Sample.pbix"),
              "--bpa-rules", str(path), "-q"])
    assert seen["bpa_rules"] == path
