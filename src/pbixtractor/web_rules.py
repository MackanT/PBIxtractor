"""The web UI's "Model quality rules" page (stand-alone only; spec D7, D19, D22).

    web_rules.build_rules_page()   # inside the app shell (web_ui.rules_index, route /rules)

Lists the built-in Best Practice Analyzer rules (data/BPARules.json), each with a switch "On"
(off = its id under `disabled:`), and the custom rules of the user's rule file
(bpa_custom.USER_RULES_PATH, ~/.pbixtractor/bpa_rules.yaml) with a form to add, edit and delete
them. Every change is saved at once (D31): the file is read again, only this one change (add,
edit, delete, switch on or off) is applied to it, it is saved through bpa_custom.save_rule_file
(atomic) and the lists are shown from the file again - a stale tab or a hand edit is never
overwritten.

"Test" runs the form's rule (unsaved values, validated first) on the model of the last
successful documentation run in this process (any tab, single or batch): web_ui calls
remember_run() / remember_model_file() after each run.
"""

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from nicegui import run, ui

from . import bpa_custom
from .bpa import load_rules
from .live_model import LiveStatistics
from .semantic_model import SemanticModel

TEST_LIMIT = 50  # objects listed by "Test"
CONFIRM_OVERWRITE = "This removes the rules that could not be read"


# ============================================================================
# The last documented model (for "Test")
# ============================================================================


@dataclass
class LastRun:
    """The model of the last successful documentation run in this process."""

    name: str  # the model's name (shown as "N findings on <name>")
    report: str = ""
    model: Optional[SemanticModel] = None
    statistics: Optional[LiveStatistics] = None
    loader: Optional[Callable[[], tuple[SemanticModel, Optional[LiveStatistics]]]] = None

    def load(self) -> tuple[SemanticModel, Optional[LiveStatistics]]:
        """The model and its statistics (read once from its file after a batch run)."""
        if self.model is None and self.loader is not None:
            self.model, self.statistics = self.loader()
            self.loader = None
        return self.model, self.statistics


_LAST: dict[str, Optional[LastRun]] = {"run": None}
_LAST_LOCK = threading.Lock()


def remember_run(
    name: str,
    model: SemanticModel,
    statistics: Optional[LiveStatistics] = None,
    report: str = "",
) -> None:
    """Keep a finished run's model for "Test" (called from any thread)."""
    with _LAST_LOCK:
        _LAST["run"] = LastRun(name, report, model, statistics)


def _read_model_file(path: Path) -> tuple[SemanticModel, Optional[LiveStatistics]]:
    from .pbix_model import read_pbix_model
    from .semantic_model import model_source, parse_model, read_model

    source = model_source(path)
    if source.suffix.lower() == ".pbix":
        pbix = read_pbix_model(source)
        return parse_model(pbix.database), pbix.statistics
    return read_model(source), None


def remember_model_file(name: str, model_path: Path, report: str = "") -> None:
    """Keep a batch run's model for "Test": read from its file when a test needs it (batch
    results carry no model; statistics then come from a .pbix only)."""
    path = Path(model_path)
    with _LAST_LOCK:
        _LAST["run"] = LastRun(name, report, loader=lambda: _read_model_file(path))


def last_run() -> Optional[LastRun]:
    """The last documented model, or None before any run."""
    return _LAST["run"]


def forget_last_run() -> None:
    _LAST["run"] = None


def run_test(rule: dict) -> tuple[str, list]:
    """
    Run one rule (a validated mapping) on the last documented model.

    Returns:
        (model name, findings)

    Raises:
        LookupError: No documentation run yet
    """
    last = last_run()
    if last is None:
        raise LookupError("Run a documentation first")
    with _LAST_LOCK:
        model, statistics = last.load()
    return last.name, bpa_custom.evaluate_rule(rule, model, statistics)


# ============================================================================
# The rule file
# ============================================================================


def rules_path() -> Path:
    """The file this page edits (read at call time: tests point it elsewhere)."""
    return Path(bpa_custom.USER_RULES_PATH)


def _load() -> bpa_custom.RuleFile:
    path = rules_path()
    if not path.exists():
        return bpa_custom.RuleFile()
    return bpa_custom.load_rule_file(path)


def properties_for(scope: Optional[str]) -> list[str]:
    """The condition properties a scope has (bpa_custom.PROPERTIES)."""
    return [p for p, scopes in bpa_custom.PROPERTIES.items() if scope in scopes]


def _condition(row: dict) -> dict:
    """A form row → a {property, op, value} mapping (value left out for ops without one)."""
    condition = {"property": row.get("property"), "op": row.get("op")}
    if bpa_custom.OPS.get(row.get("op"), True):
        value = row.get("value") or ""
        if row.get("op") == "in":
            condition["value"] = [v.strip() for v in str(value).split(",") if v.strip()]
        else:
            condition["value"] = value
    return condition


def _row(condition: bpa_custom.Condition) -> dict:
    value = condition.value
    if isinstance(value, list):
        value = ", ".join(str(v) for v in value)
    elif isinstance(value, bool):
        value = str(value).lower()
    return {
        "property": condition.property,
        "op": condition.op,
        "value": "" if value is None else str(value),
    }


def form_rule(values: dict) -> dict:
    """The form's values → a rule mapping as in the file (empty optional keys left out)."""
    rule = {
        "id": (values.get("id") or "").strip(),
        "name": (values.get("name") or "").strip(),
        "category": (values.get("category") or "").strip() or bpa_custom.DEFAULT_CATEGORY,
        "severity": values.get("severity"),
        "scope": values.get("scope"),
        "conditions": [_condition(r) for r in values.get("conditions", [])],
    }
    if (values.get("description") or "").strip():
        rule["description"] = values["description"].strip()
    if values.get("any"):
        rule["any"] = [_condition(r) for r in values["any"]]
    return rule


def form_errors(rule: dict, others: list[bpa_custom.CustomRule]) -> list[str]:
    """validate_rule + unique ids among the other custom rules."""
    errors = bpa_custom.validate_rule(rule)
    rule_id = rule.get("id")
    if isinstance(rule_id, str) and any(r.id.upper() == rule_id.upper() for r in others):
        errors.append("a custom rule with this id exists already")
    return errors


# ============================================================================
# The page
# ============================================================================


# A change to the rule file: applied to the file as just read; returns None (applied), ""
# (nothing to do: the file already says so) or why it is refused
Change = Callable[[bpa_custom.RuleFile], Optional[str]]


def _find(rules: list[bpa_custom.CustomRule], rule_id: str) -> Optional[int]:
    return next((i for i, r in enumerate(rules) if r.id.upper() == rule_id.upper()), None)


def add_rule(new: bpa_custom.CustomRule) -> Change:
    """Add a rule; refused if its id exists (meanwhile) in the file."""
    def change(current: bpa_custom.RuleFile) -> Optional[str]:
        if _find(current.rules, new.id) is not None:
            return f"A custom rule with the id {new.id} exists already in {rules_path()}"
        current.rules.append(new)
        return None

    return change


def replace_rule(rule_id: str, new: bpa_custom.CustomRule) -> Change:
    """Replace the rule with this id (added again if it was deleted meanwhile)."""
    def change(current: bpa_custom.RuleFile) -> Optional[str]:
        position = _find(current.rules, rule_id)
        clash = _find(current.rules, new.id)
        if clash is not None and clash != position:
            return f"A custom rule with the id {new.id} exists already in {rules_path()}"
        if position is None:
            current.rules.append(new)
        else:
            current.rules[position] = new
        return None

    return change


def delete_rule_id(rule_id: str) -> Change:
    """Delete the rule with this id (nothing to do if it is gone already)."""
    def change(current: bpa_custom.RuleFile) -> Optional[str]:
        position = _find(current.rules, rule_id)
        if position is None:
            return ""
        current.rules.pop(position)
        return None

    return change


def switch_builtin(rule_id: str, on: bool) -> Change:
    """Remove (on) or add (off) one built-in id under `disabled:`."""
    def change(current: bpa_custom.RuleFile) -> Optional[str]:
        if (rule_id not in current.disabled) == on:
            return ""
        if on:
            current.disabled = [i for i in current.disabled if i != rule_id]
        else:
            current.disabled.append(rule_id)
        return None

    return change


def apply_change(change: Change) -> tuple[bpa_custom.RuleFile, Optional[str]]:
    """
    Read the file fresh and apply one change to it, without saving (the page asks first when
    the file has rules that could not be read).

    Returns:
        (the file with the change applied, None applied / "" nothing to do / refusal text)
    """
    current = _load()
    return current, change(current)


def build_rules_page() -> None:
    """The page's content: the error banner, the built-in rules and the custom rules."""
    from .theme import card_header, empty_state, page_title

    loaded = _load()
    state = {
        "rules": list(loaded.rules),
        "disabled": list(loaded.disabled),
        "errors": list(loaded.errors),
    }
    switches: dict[str, ui.switch] = {}

    def show(current: bpa_custom.RuleFile) -> None:
        """Show the lists as the file has them. A switch set here emits a change; its toggle()
        then finds the file in that state already and does nothing (no save, no dialog)."""
        state["rules"] = list(current.rules)
        state["disabled"] = list(current.disabled)
        state["errors"] = list(current.errors)
        banner.refresh()
        custom_list.refresh()
        for rule_id, switch in switches.items():
            on = rule_id not in state["disabled"]
            if switch.value != on:
                switch.value = on

    async def confirmed(errors: list[str]) -> bool:
        """Saving drops the rules that could not be read: only when the user agrees."""
        with ui.dialog() as dialog, ui.card():
            ui.label(f"{CONFIRM_OVERWRITE}:").classes("font-semibold")
            for error in errors:
                ui.label(error).classes("text-sm text-negative")
            with ui.row().classes("w-full justify-end"):
                ui.button("Cancel", on_click=lambda: dialog.submit(False)).props(
                    "flat"
                ).mark("confirm_cancel")
                ui.button("Save anyway", on_click=lambda: dialog.submit(True)).props(
                    "color=negative"
                ).mark("confirm_overwrite")
        agreed = await dialog
        dialog.delete()
        return bool(agreed)

    async def save(change: Change) -> bool:
        """Apply this one change to the file as it is now, save it and show the result."""
        current, outcome = apply_change(change)
        if outcome == "":
            show(current)
            return True
        if outcome is not None:
            ui.notify(outcome, type="negative")
            show(current)
            return False
        if current.errors and not await confirmed(current.errors):
            show(_load())  # e.g. puts a switch back
            return False
        try:
            bpa_custom.save_rule_file(rules_path(), current.rules, current.disabled)
        except OSError as error:
            ui.notify(f"Could not save {rules_path()}: {error}", type="negative")
            show(_load())
            return False
        show(_load())
        return True

    with ui.column().classes("w-full max-w-screen-2xl mx-auto p-4 gap-4"):
        page_title(
            "Model quality rules",
            f"The Best Practice Analyzer rules of every run on this PC, saved in {rules_path()}",
            icon="rule",
        )

        @ui.refreshable
        def banner() -> None:
            if not state["errors"]:
                return
            with ui.card().classes("w-full edge-neg tint-warning").props("flat").mark(
                "rules_file_errors"
            ):
                with ui.row().classes("items-center gap-3 w-full no-wrap"):
                    card_header(
                        "error",
                        "Some rules in the file could not be read",
                        "They are left out of every run. Saving here rewrites the file "
                        "without them.",
                        color="negative",
                    )
                for error in state["errors"]:
                    ui.label(error).classes("text-sm text-negative")

        banner()

        # ── custom rules ──────────────────────────────────────────────────────
        with ui.card().classes("w-full"):
            with ui.row().classes("items-center gap-3 w-full no-wrap"):
                card_header(
                    "tune",
                    "Custom rules",
                    "Your own rules: they show up on the model quality sheet and in the "
                    "Model quality column like the built-in ones",
                )
                ui.button("Add rule", icon="add", on_click=lambda: open_form(None)).props(
                    "color=primary"
                ).mark("add_rule")

            @ui.refreshable
            def custom_list() -> None:
                if not state["rules"]:
                    empty_state("rule", "No custom rules yet", "Add one with \"Add rule\".")
                    return
                for rule in state["rules"]:
                    with ui.row().classes("items-center gap-3 w-full no-wrap").mark(
                        f"custom_{rule.id}"
                    ):
                        with ui.column().classes("gap-0 flex-1 min-w-0"):
                            ui.label(rule.name).classes("font-semibold")
                            ui.label(
                                f"{rule.id} · {rule.scope} · {rule.severity} · {rule.category}"
                            ).classes("text-sm text-grey-7")
                        ui.button(
                            icon="edit", on_click=lambda i=rule.id: open_form(i)
                        ).props('flat round dense aria-label="Edit"').tooltip("Edit").mark(
                            f"edit_{rule.id}"
                        )
                        ui.button(
                            icon="delete", on_click=lambda i=rule.id: delete_rule(i)
                        ).props('flat round dense color=negative aria-label="Delete"').tooltip(
                            "Delete"
                        ).mark(f"delete_{rule.id}")

            custom_list()

        async def delete_rule(rule_id: str) -> None:
            position = _find(state["rules"], rule_id)
            if position is None:
                return
            rule = state["rules"][position]
            with ui.dialog() as dialog, ui.card():
                ui.label(f"Delete the rule {rule.name} ({rule.id})?")
                with ui.row().classes("w-full justify-end"):
                    ui.button("Cancel", on_click=lambda: dialog.submit(False)).props("flat")
                    ui.button("Delete", on_click=lambda: dialog.submit(True)).props(
                        "color=negative"
                    ).mark("confirm_delete")
            agreed = await dialog
            dialog.delete()
            if not agreed:
                return
            if await save(delete_rule_id(rule.id)):
                ui.notify(f"Deleted {rule.name}", type="positive")

        # ── built-in rules ────────────────────────────────────────────────────
        builtins = load_rules()
        with ui.card().classes("w-full"):
            with ui.row().classes("items-center gap-3 w-full no-wrap"):
                card_header(
                    "verified",
                    f"Built-in rules ({len(builtins)})",
                    "Microsoft's standard Best Practice Analyzer rules. Switch off the ones "
                    "you do not want checked.",
                )
                search = (
                    ui.input(placeholder="Search name, category or id")
                    .props("dense outlined clearable")
                    .classes("w-72")
                    .mark("builtin_search")
                )
            rows: list[tuple[str, ui.element]] = []
            for rule in builtins:
                text = f"{rule.name} {rule.category} {rule.id} {rule.severity}".lower()
                with ui.row().classes("items-center gap-3 w-full no-wrap").mark(
                    f"builtin_row_{rule.id}"
                ) as row:
                    switch = ui.switch(value=rule.id not in state["disabled"]).props(
                        f'dense aria-label="On: {rule.name}"'
                    ).mark(f"builtin_{rule.id}")
                    with ui.column().classes("gap-0 flex-1 min-w-0"):
                        ui.label(rule.name).classes("font-semibold").tooltip(
                            rule.description or rule.name
                        )
                        ui.label(
                            f"{rule.category} · {rule.severity} · {', '.join(rule.scope)}"
                        ).classes("text-sm text-grey-7")
                switch.on_value_change(lambda e, r=rule: toggle(r.id, e.value))
                switches[rule.id] = switch
                rows.append((text, row))

            def filter_rows() -> None:
                needle = (search.value or "").strip().lower()
                for text, row in rows:
                    row.visible = needle in text

            search.on_value_change(filter_rows)

        async def toggle(rule_id: str, on: bool) -> None:
            # No flag: a switch put back by show() asks for the file's state, so nothing to do
            await save(switch_builtin(rule_id, on))

        # ── the form ──────────────────────────────────────────────────────────
        def open_form(rule_id: Optional[str]) -> None:
            position = _find(state["rules"], rule_id) if rule_id is not None else None
            existing = state["rules"][position] if position is not None else None
            values = {
                "id": existing.id if existing else "",
                "name": existing.name if existing else "",
                "category": existing.category if existing else bpa_custom.DEFAULT_CATEGORY,
                "severity": existing.severity if existing else "Medium",
                "description": existing.description if existing else "",
                "scope": existing.scope if existing else "Measure",
                "conditions": [_row(c) for c in existing.conditions] if existing else [
                    {"property": "name", "op": "matches", "value": ""}
                ],
                "any": [_row(c) for c in existing.any] if existing else [],
            }
            others = [r for r in state["rules"] if existing is None or r.id != existing.id]

            with ui.dialog() as dialog, ui.card().classes("w-[760px] max-w-full"):
                with ui.row().classes("items-center gap-3 w-full no-wrap"):
                    card_header("rule", "Edit rule" if existing else "Add rule")
                with ui.row().classes("w-full gap-3"):
                    ui.input("Id").bind_value(values, "id").classes("w-48").mark("rule_id")
                    ui.input("Name").bind_value(values, "name").classes("grow").mark("rule_name")
                with ui.row().classes("w-full gap-3"):
                    ui.input("Category").bind_value(values, "category").classes("w-48").mark(
                        "rule_category"
                    )
                    ui.select(list(bpa_custom.SEVERITY_NAMES), label="Severity").bind_value(
                        values, "severity"
                    ).classes("w-40").mark("rule_severity")
                    scope_select = ui.select(list(bpa_custom.SCOPES), label="Scope").bind_value(
                        values, "scope"
                    )
                    scope_select.classes("w-48").mark("rule_scope")
                ui.textarea("Description").bind_value(values, "description").props(
                    "autogrow"
                ).classes("w-full").mark("rule_description")

                @ui.refreshable
                def conditions(group: str) -> None:
                    choices = properties_for(values["scope"])
                    for number, row in enumerate(values[group]):
                        with ui.row().classes("items-center gap-2 w-full no-wrap"):
                            ui.select(choices, label="Property").bind_value(
                                row, "property"
                            ).classes("w-44").mark(f"cond_property_{group}_{number}")
                            op = ui.select(list(bpa_custom.OPS), label="Op").bind_value(
                                row, "op"
                            ).classes("w-36").mark(f"cond_op_{group}_{number}")
                            value = ui.input().bind_value(row, "value").classes("grow").mark(
                                f"cond_value_{group}_{number}"
                            )
                            show_value(value, row["op"])
                            # No refresh here: a rebuilt, bound select would fire it again
                            op.on_value_change(lambda e, v=value: show_value(v, e.value))
                            ui.button(
                                icon="close",
                                on_click=lambda g=group, n=number: remove_condition(g, n),
                            ).props('flat round dense aria-label="Remove"').mark(
                                f"cond_remove_{group}_{number}"
                            )
                    ui.button(
                        "Add condition", icon="add", on_click=lambda g=group: add_condition(g)
                    ).props("flat dense color=primary").mark(f"add_{group}")

                def show_value(value_input, op: Optional[str]) -> None:
                    """The value field: hidden for ops without a value, a list for `in`."""
                    value_input.visible = bpa_custom.OPS.get(op, True)
                    value_input.props(
                        f'label="{"Values, comma-separated" if op == "in" else "Value"}"'
                    )

                def add_condition(group: str) -> None:
                    first = (properties_for(values["scope"]) or ["name"])[0]
                    values[group].append({"property": first, "op": "equals", "value": ""})
                    conditions.refresh(group)

                def remove_condition(group: str, number: int) -> None:
                    values[group].pop(number)
                    conditions.refresh(group)

                def scope_changed() -> None:
                    choices = properties_for(values["scope"])
                    for group in ("conditions", "any"):
                        for row in values[group]:
                            if row["property"] not in choices:
                                row["property"] = choices[0] if choices else None
                        conditions.refresh(group)

                scope_select.on_value_change(scope_changed)

                ui.label("Conditions (all must hold)").classes("font-semibold mt-2")
                with ui.column().classes("w-full gap-1"):
                    conditions("conditions")
                ui.label("Any of (optional: at least one must hold)").classes("font-semibold")
                with ui.column().classes("w-full gap-1"):
                    conditions("any")

                errors_box = ui.column().classes("w-full gap-0").mark("rule_errors")
                test_box = ui.column().classes("w-full gap-1").mark("test_result")

                def checked() -> Optional[dict]:
                    rule = form_rule(values)
                    errors = form_errors(rule, others)
                    errors_box.clear()
                    with errors_box:
                        for error in errors:
                            ui.label(error).classes("text-sm text-negative")
                    return None if errors else rule

                async def test() -> None:
                    rule = checked()
                    test_box.clear()
                    if rule is None:
                        return
                    with test_box:
                        ui.spinner()
                    try:
                        name, findings = await run.io_bound(run_test, rule)
                    except Exception as error:  # UI boundary: a broken model must not hang
                        test_box.clear()
                        with test_box:
                            ui.label(f"Test failed: {error}").classes("text-negative")
                        return
                    test_box.clear()
                    with test_box:
                        count = len(findings)
                        ui.label(
                            f"{count} finding{'' if count == 1 else 's'} on {name}"
                        ).classes("font-semibold").mark("test_summary")
                        for finding in findings[:TEST_LIMIT]:
                            ui.label(f"{finding.object_type}: {finding.object_name}").classes(
                                "text-sm"
                            )
                        if count > TEST_LIMIT:
                            ui.label(f"… and {count - TEST_LIMIT} more").classes(
                                "text-sm text-grey-7"
                            )

                async def save_form() -> None:
                    rule = checked()
                    if rule is None:
                        return
                    new = bpa_custom.rule_from_dict(rule)
                    change = add_rule(new) if existing is None else replace_rule(existing.id, new)
                    if not await save(change):
                        return
                    ui.notify(f"Saved {new.name}", type="positive")
                    dialog.close()

                with ui.row().classes("w-full items-center justify-end gap-2"):
                    ready = last_run() is not None
                    test_button = ui.button("Test", icon="science", on_click=test).props(
                        "flat color=primary"
                    ).mark("rule_test")
                    if not ready:
                        test_button.disable()
                        ui.label("Run a documentation first").classes(
                            "text-sm text-grey-7"
                        ).mark("test_needs_run")
                    ui.space()
                    ui.button("Cancel", on_click=dialog.close).props("flat").mark("rule_cancel")
                    ui.button("Save", icon="save", on_click=save_form).props(
                        "color=primary"
                    ).mark("rule_save")
            # Closed (Save, Cancel, Esc or a click outside): gone, so it never shows stale values
            dialog.on_value_change(lambda e: None if e.value else dialog.delete())
            dialog.open()
