"""Custom model quality rules: a simple YAML rule file defined by PBIxtractor (spec D6, D17, D18).

    rule_file = load_rule_file(USER_RULES_PATH)   # never raises for content problems
    violations, not_checked = run_bpa(model, stats, custom=rule_file.rules,
                                      disabled=rule_file.disabled)

The file (``~/.pbixtractor/bpa_rules.yaml`` for every run on this PC, or ``--bpa-rules FILE``):

    version: 1
    disabled: [AVOID_FLOATING_POINT_DATA_TYPES]   # built-in rule ids switched off
    rules:
      - id: MEASURES_IN_FOLDERS                   # unique; [A-Za-z0-9_-]+
        name: Measures should be in a display folder
        category: Custom                          # optional, default "Custom"
        severity: Medium                          # Low | Medium | High
        description: Put every measure in a display folder.   # optional
        scope: Measure    # Measure | Column | CalculatedColumn | Table | Relationship | Model
        conditions:       # all must hold (may be empty if `any` is given)
          - {property: display_folder, op: is_empty}
        any:              # optional: at least one must hold
          - {property: name, op: matches, value: "^[a-z]"}   # starts with a lower-case letter

A rule finds every object of its scope for which all `conditions` and (when given) at least one
of `any` hold. Findings are bpa.BpaViolation rows named like the built-in checks', so the model
quality sheets and the "Model quality" column show them like any other.

`equals`, `not_equals`, `contains` and `in` ignore case. `matches` is a regular expression and,
as in Tabular Editor (.NET Regex.IsMatch), case-sensitive unless the pattern starts with `(?i)`:
"^[a-z]" finds "sales" but not "Sales"; "(?i)^[a-z]" finds both. Patterns run with the `regex`
package and a MATCH_TIMEOUT (2 s) per object: a pattern that takes longer (catastrophic
backtracking) does not match that object, and the run logs one warning per rule and continues.

The web UI's rules page (ticket 06) reads PROPERTIES / OPS / SCOPES for its form and uses
validate_rule(), save_rule_file() and evaluate_rule().
"""

import logging
import os
import re
import tempfile
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Optional, Union

import regex
import yaml

from .azure_auth import AUTH_DIR
from .live_model import LiveStatistics
from .semantic_model import DATA_TYPES, SemanticModel

USER_RULES_PATH = AUTH_DIR / "bpa_rules.yaml"
FILE_VERSION = 1

ID_PATTERN = re.compile(r"[A-Za-z0-9_-]+")
SEVERITY_NAMES = ("Low", "Medium", "High")
DEFAULT_CATEGORY = "Custom"
MATCH_TIMEOUT = 2.0  # seconds per `matches` search (D32)

logger = logging.getLogger("pbixtractor")

# Custom scope -> the bpa.iter_objects() scopes it covers. "Column" = every column (data,
# calculated, calculated table column); "CalculatedColumn" = only those with DAX.
SCOPES: dict[str, tuple[str, ...]] = {
    "Measure": ("Measure",),
    "Column": ("DataColumn", "CalculatedColumn", "CalculatedTableColumn"),
    "CalculatedColumn": ("CalculatedColumn",),
    "Table": ("Table", "CalculatedTable", "CalculationGroup"),
    "Relationship": ("Relationship",),
    "Model": ("Model",),
}

# Property -> the scopes it exists on
_FIELDS = ("Measure", "Column", "CalculatedColumn")
PROPERTIES: dict[str, tuple[str, ...]] = {
    "name": tuple(SCOPES),
    "table": _FIELDS,
    "description": _FIELDS + ("Table",),
    "format_string": _FIELDS,
    "data_type": _FIELDS,
    "display_folder": _FIELDS,
    "hidden": _FIELDS + ("Table",),
    "expression": ("Measure", "CalculatedColumn"),
    "dependants": _FIELDS + ("Table",),
}

# Op -> takes a value?
OPS: dict[str, bool] = {
    "equals": True,
    "not_equals": True,
    "contains": True,
    "matches": True,
    "is_empty": False,
    "not_empty": False,
    "in": True,
}

RULE_KEYS = ("id", "name", "category", "severity", "description", "scope", "conditions", "any")
CONDITION_KEYS = ("property", "op", "value")


@dataclass(frozen=True)
class Condition:
    """One condition of a custom rule: {property, op, value}."""

    property: str
    op: str
    value: Any = None


@dataclass(frozen=True)
class CustomRule:
    """One validated custom rule."""

    id: str
    name: str
    scope: str
    severity: str
    category: str = DEFAULT_CATEGORY
    description: str = ""
    conditions: tuple[Condition, ...] = ()
    any: tuple[Condition, ...] = ()


@dataclass
class RuleFile:
    """A loaded rule file: the valid rules, the built-in ids switched off and every problem."""

    rules: list[CustomRule] = field(default_factory=list)
    disabled: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)  # "<rule id / position>: <problem>"


# ============================================================================
# Validation
# ============================================================================


def builtin_ids() -> list[str]:
    """The ids of the built-in rules (data/BPARules.json)."""
    from .bpa import load_rules  # bpa imports this module lazily too

    return [rule.id for rule in load_rules()]


def _condition_errors(condition: Any, scope: Optional[str], where: str) -> list[str]:
    if not isinstance(condition, dict):
        return [f"{where} is not a {{property, op, value}} mapping"]
    errors = [f"{where}: unknown key {key!r}" for key in condition if key not in CONDITION_KEYS]
    prop, op = condition.get("property"), condition.get("op")
    if prop not in PROPERTIES:
        errors.append(f"{where}: unknown property {prop!r} (one of {', '.join(PROPERTIES)})")
    elif scope in SCOPES and scope not in PROPERTIES[prop]:
        errors.append(f"{where}: property {prop!r} does not exist on {scope}")
    if op not in OPS:
        errors.append(f"{where}: unknown op {op!r} (one of {', '.join(OPS)})")
        return errors
    value = condition.get("value")
    if OPS[op] and "value" not in condition:
        errors.append(f"{where}: op {op!r} needs a value")
    elif not OPS[op] and value is not None:
        errors.append(f"{where}: op {op!r} takes no value")
    elif op == "in" and not isinstance(value, list):
        errors.append(f"{where}: op 'in' needs a list")
    elif op == "matches":
        try:
            _pattern(str(value))
        except regex.error as e:
            errors.append(f"{where}: bad regex {value!r}: {e}")
    elif op in ("equals", "not_equals", "contains") and isinstance(value, (list, dict)):
        errors.append(f"{where}: op {op!r} needs a single value")
    return errors


def validate_rule(rule: Any, builtins: Optional[Iterable[str]] = None) -> list[str]:
    """
    The problems of one custom rule (empty: valid).

    Args:
        rule: The rule as a mapping (as in the YAML file)
        builtins: Built-in rule ids it must not reuse (default: data/BPARules.json's)

    Returns:
        Error texts, without the rule's id (the caller names it)
    """
    if not isinstance(rule, dict):
        return ["not a mapping"]
    errors = [f"unknown key {key!r}" for key in rule if key not in RULE_KEYS]
    rule_id = rule.get("id")
    if not isinstance(rule_id, str) or not ID_PATTERN.fullmatch(rule_id):
        errors.append(f"bad id {rule_id!r} (letters, digits, _ and - only)")
    else:
        taken = {i.upper() for i in (builtin_ids() if builtins is None else builtins)}
        if rule_id.upper() in taken:
            errors.append("the id of a built-in rule (switch that off under disabled: instead)")
    if not str(rule.get("name") or "").strip():
        errors.append("no name")
    if rule.get("severity") not in SEVERITY_NAMES:
        errors.append(f"bad severity {rule.get('severity')!r} (Low, Medium or High)")
    scope = rule.get("scope")
    if scope not in SCOPES:
        errors.append(f"bad scope {scope!r} (one of {', '.join(SCOPES)})")
    for key in ("category", "description"):
        if rule.get(key) is not None and not isinstance(rule.get(key), str):
            errors.append(f"{key} is not text")
    groups = {}
    for key in ("conditions", "any"):
        value = rule.get(key)
        if value is None:
            value = []
        if not isinstance(value, list):
            errors.append(f"{key} is not a list")
            value = []
        groups[key] = value
        for number, condition in enumerate(value, 1):
            errors += _condition_errors(condition, scope, f"{key} #{number}")
    if not groups["conditions"] and not groups["any"]:
        errors.append("no conditions")
    return errors


def _to_rule(rule: dict) -> CustomRule:
    def conditions(key: str) -> tuple[Condition, ...]:
        return tuple(
            Condition(c["property"], c["op"], c.get("value")) for c in rule.get(key) or []
        )

    return CustomRule(
        id=rule["id"],
        name=str(rule["name"]).strip(),
        scope=rule["scope"],
        severity=rule["severity"],
        category=rule.get("category") or DEFAULT_CATEGORY,
        description=rule.get("description") or "",
        conditions=conditions("conditions"),
        any=conditions("any"),
    )


def rule_from_dict(rule: dict, builtins: Optional[Iterable[str]] = None) -> CustomRule:
    """
    A CustomRule from its mapping.

    Raises:
        ValueError: The rule is not valid (the message lists the problems)
    """
    errors = validate_rule(rule, builtins)
    if errors:
        raise ValueError("; ".join(errors))
    return _to_rule(rule)


def rule_to_dict(rule: CustomRule) -> dict:
    """The rule as a mapping in the file's key order (optional keys left out when empty)."""
    def conditions(items: tuple[Condition, ...]) -> list[dict]:
        out = []
        for c in items:
            item = {"property": c.property, "op": c.op}
            if OPS.get(c.op, True):
                item["value"] = c.value
            out.append(item)
        return out

    data: dict[str, Any] = {"id": rule.id, "name": rule.name, "category": rule.category,
                            "severity": rule.severity}
    if rule.description:
        data["description"] = rule.description
    data["scope"] = rule.scope
    data["conditions"] = conditions(rule.conditions)
    if rule.any:
        data["any"] = conditions(rule.any)
    return data


# ============================================================================
# Loading and saving
# ============================================================================


def load_rule_file(path: Union[str, Path]) -> RuleFile:
    """
    Read a custom rule file. Content problems never raise (D18): an invalid rule is dropped and
    named in errors, YAML that does not parse gives no rules at all; the built-in rules run
    either way.

    Args:
        path: The YAML file

    Returns:
        RuleFile(rules, disabled built-in ids, errors as "<rule id or position>: <problem>")
    """
    path = Path(path)
    result = RuleFile()
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as e:
        result.errors.append(f"file {path}: {' '.join(str(e).split())}")
        return result
    if data is None:
        return result
    if not isinstance(data, dict):
        result.errors.append(f"file {path}: not a mapping with version, disabled and rules")
        return result
    for key in data:
        if key not in ("version", "disabled", "rules"):
            result.errors.append(f"file {path}: unknown key {key!r}")
    if data.get("version", FILE_VERSION) != FILE_VERSION:
        result.errors.append(f"file {path}: version {data.get('version')!r} (expected 1)")

    builtins = builtin_ids()
    by_upper = {i.upper(): i for i in builtins}
    disabled = data.get("disabled") or []
    if not isinstance(disabled, list):
        result.errors.append(f"file {path}: disabled is not a list of rule ids")
        disabled = []
    for rule_id in disabled:
        known = by_upper.get(str(rule_id).upper())
        if known is None:
            result.errors.append(f"{rule_id}: disabled, but there is no built-in rule by that id")
        elif known not in result.disabled:
            result.disabled.append(known)

    rules = data.get("rules") or []
    if not isinstance(rules, list):
        result.errors.append(f"file {path}: rules is not a list")
        rules = []
    seen: set[str] = set()
    for number, rule in enumerate(rules, 1):
        rule_id = rule.get("id") if isinstance(rule, dict) else None
        label = rule_id if isinstance(rule_id, str) and rule_id else f"rule #{number}"
        errors = validate_rule(rule, builtins)
        if not errors and rule_id.upper() in seen:
            errors = ["a second rule with this id"]
        if errors:
            result.errors.append(f"{label}: {'; '.join(errors)}")
            continue
        seen.add(rule_id.upper())
        result.rules.append(_to_rule(rule))
    return result


def save_rule_file(
    path: Union[str, Path],
    rules: Iterable[Union[CustomRule, dict]],
    disabled: Iterable[str] = (),
) -> None:
    """
    Write a rule file (version 1) atomically: a temp file next to it, then replaced.

    Args:
        path: The YAML file (its folder is created)
        rules: CustomRule objects or rule mappings (written as given, in the file's key order)
        disabled: Built-in rule ids switched off
    """
    path = Path(path)
    items = []
    for rule in rules:
        if isinstance(rule, CustomRule):
            items.append(rule_to_dict(rule))
        else:
            items.append({k: rule[k] for k in RULE_KEYS if k in rule})
    data = {"version": FILE_VERSION, "disabled": list(disabled), "rules": items}
    text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True, default_flow_style=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temp = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as file:
            file.write(text)
        os.replace(temp, path)
    except BaseException:
        Path(temp).unlink(missing_ok=True)
        raise


# ============================================================================
# Evaluation
# ============================================================================


def _property(ctx, obj, prop: str) -> Any:
    """A property's value on one bpa.BpaObject."""
    item = obj.item
    if prop == "name":
        if obj.scope == "Relationship":
            return item.te_name
        if obj.scope == "Model":
            return item.model_name
        return item.name
    if prop == "table":
        return item.table
    if prop == "hidden":
        return bool(item.is_hidden)
    if prop == "data_type":
        return DATA_TYPES.get(item.data_type, item.data_type) or ""
    if prop == "dependants":
        if obj.scope == "Measure":
            key = ("Measure", item.table, item.name)
        elif obj.scope in SCOPES["Table"]:
            key = ("Table", item.name, item.name)
        else:
            key = ("Column", item.table, item.name)
        return len(ctx.referenced_by.get(key, ()))
    return getattr(item, prop, "") or ""


def _same(actual: Any, expected: Any) -> bool:
    if isinstance(actual, bool):
        if isinstance(expected, str):
            return str(actual).lower() == expected.strip().lower()
        return actual == bool(expected)
    if isinstance(actual, int):
        try:
            return actual == int(expected)
        except (TypeError, ValueError):
            return False
    return str(actual).lower() == str(expected if expected is not None else "").lower()


@lru_cache(maxsize=256)
def _pattern(text: str) -> "regex.Pattern":
    """A `matches` pattern, compiled with the regex package (case-sensitive unless (?i))."""
    return regex.compile(text)


def _holds(ctx, obj, condition: Condition) -> bool:
    actual = _property(ctx, obj, condition.property)
    op, value = condition.op, condition.value
    if op == "equals":
        return _same(actual, value)
    if op == "not_equals":
        return not _same(actual, value)
    if op == "in":
        return any(_same(actual, v) for v in value or [])
    if op == "is_empty":
        return actual in ("", None)
    if op == "not_empty":
        return actual not in ("", None)
    text = str(actual)
    if op == "contains":
        return str(value).lower() in text.lower()
    if op == "matches":
        # TimeoutError goes to breaks(): that object does not match
        return _pattern(str(value)).search(text, timeout=MATCH_TIMEOUT) is not None
    raise ValueError(f"unknown op {op!r}")


def breaks(ctx, obj, rule: CustomRule) -> bool:
    """
    Does the object (in the rule's scope) break the rule? A `matches` that takes longer than
    MATCH_TIMEOUT counts as no match; the first such object per rule and run is logged.
    """
    try:
        if not all(_holds(ctx, obj, c) for c in rule.conditions):
            return False
        return not rule.any or any(_holds(ctx, obj, c) for c in rule.any)
    except TimeoutError:
        warned = vars(ctx).setdefault("custom_timeouts", set())
        if rule.id not in warned:
            warned.add(rule.id)
            logger.warning(
                f"Custom model quality rule {rule.id}: the pattern took longer than "
                f"{MATCH_TIMEOUT:g} s on {obj.object_name}; skipped"
            )
        return False


def evaluate_rule(
    rule: Union[CustomRule, dict],
    model: SemanticModel,
    statistics: Optional[LiveStatistics] = None,
) -> list:
    """
    One custom rule against one model (the rules page's "Test"). Ignore annotations apply.

    Args:
        rule: A CustomRule, or its mapping (validated; ValueError when invalid)
        model: Model from semantic_model.read_model()
        statistics: Row counts / distinct values, if any

    Returns:
        The rule's findings (bpa.BpaViolation), in model order

    Raises:
        ValueError: The rule mapping is not valid
    """
    from .bpa import _Context, iter_objects, rule_findings

    if not isinstance(rule, CustomRule):
        rule = rule_from_dict(rule)
    ctx = _Context(model, statistics)
    return rule_findings(rule, ctx, list(iter_objects(model)))
