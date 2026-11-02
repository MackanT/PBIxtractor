"""The web UI's "Model quality rules" page (web_rules.py, route /rules, stand-alone only)."""

import asyncio
import json

import pytest
import yaml

pytest.importorskip("nicegui")

from nicegui import ui  # noqa: E402
from nicegui.testing.user_simulation import user_simulation  # noqa: E402

import pbixtractor.web_rules as web_rules  # noqa: E402
import pbixtractor.web_ui as web_ui  # noqa: E402
from pbixtractor import bpa_custom  # noqa: E402
from pbixtractor.bpa import load_rules  # noqa: E402
from pbixtractor.semantic_model import parse_model  # noqa: E402

from .test_semantic_model import BIM  # noqa: E402
from .test_web_ui import _any_element, _wait_for  # noqa: E402

FIRST = load_rules()[0].id


@pytest.fixture
def page(tmp_path, monkeypatch):
    """No last run, the temp rule file of conftest."""
    monkeypatch.setitem(web_ui._CATALOG, "dir", None)
    monkeypatch.setitem(web_rules._LAST, "run", None)
    monkeypatch.chdir(tmp_path)
    return bpa_custom.USER_RULES_PATH


def _simulate(scenario) -> None:
    async def run() -> None:
        async with user_simulation(web_ui.index) as user:
            web_ui.register_routes()
            # As web_ui.register_app_pages(), through a test function: NiceGUI's teardown
            # unloads the module of every page function (it would unload pbixtractor)
            ui.page("/rules")(lambda: web_ui.rules_index())
            await user.open("/rules")
            await scenario(user)

    asyncio.run(run())


def _el(user, marker: str):
    return _any_element(user, marker)


def _saved(path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


async def _fill(user, rule_id="NAMES_UPPER", name="Names start upper case", value="^[a-z]"):
    user.find(marker="add_rule").click()
    await _wait_for(lambda: any("rule_id" in e._markers for e in user.client.elements.values()))
    _el(user, "rule_id").value = rule_id
    _el(user, "rule_name").value = name
    _el(user, "cond_value_conditions_0").value = value


def test_the_page_lists_the_builtins_and_has_a_rail_entry(page):
    async def scenario(user):
        await user.should_see("Model quality rules")
        await user.should_see(f"Built-in rules ({len(load_rules())})")
        for rule in load_rules():
            assert _el(user, f"builtin_{rule.id}").value is True
        await user.should_see(marker="nav_rules")
        await user.should_see(marker="nav_document")
        _el(user, "builtin_search").value = load_rules()[1].name
        assert not _el(user, f"builtin_row_{FIRST}").visible
        assert _el(user, f"builtin_row_{load_rules()[1].id}").visible

    _simulate(scenario)


def test_switching_a_builtin_off_and_on_writes_disabled(page):
    async def scenario(user):
        _el(user, f"builtin_{FIRST}").value = False
        await _wait_for(lambda: page.exists())
        assert _saved(page)["disabled"] == [FIRST]
        _el(user, f"builtin_{FIRST}").value = True
        await _wait_for(lambda: _saved(page)["disabled"] == [])

    _simulate(scenario)


def test_add_edit_and_delete_a_custom_rule(page):
    async def scenario(user):
        await _fill(user)
        _el(user, "cond_op_conditions_0").value = "in"
        await _wait_for(lambda: _el(user, "cond_value_conditions_0").visible)
        _el(user, "cond_value_conditions_0").value = "a, b"
        user.find(marker="rule_save").click()
        await _wait_for(lambda: page.exists())
        (rule,) = _saved(page)["rules"]
        assert rule["id"] == "NAMES_UPPER" and rule["category"] == "Custom"
        assert rule["conditions"] == [{"property": "name", "op": "in", "value": ["a", "b"]}]
        await user.should_see(marker="custom_NAMES_UPPER")

        user.find(marker="edit_NAMES_UPPER").click()
        await _wait_for(lambda: _el(user, "rule_name").value == "Names start upper case")
        _el(user, "rule_name").value = "Renamed"
        _el(user, "cond_op_conditions_0").value = "is_empty"
        await _wait_for(lambda: not _el(user, "cond_value_conditions_0").visible)
        user.find(marker="rule_save").click()
        await _wait_for(lambda: _saved(page)["rules"][0]["name"] == "Renamed")
        assert _saved(page)["rules"][0]["conditions"] == [{"property": "name", "op": "is_empty"}]

        user.find(marker="delete_NAMES_UPPER").click()
        await _wait_for(lambda: any("confirm_delete" in e._markers for e in user.client.elements.values()))
        user.find(marker="confirm_delete").click()
        await _wait_for(lambda: _saved(page)["rules"] == [])

    _simulate(scenario)


def test_an_invalid_form_shows_errors_and_writes_nothing(page):
    async def scenario(user):
        await _fill(user, rule_id="bad id!", name="")
        user.find(marker="rule_save").click()
        await user.should_see("no name")
        await user.should_see("bad id")
        assert not page.exists()

    _simulate(scenario)


def test_duplicate_and_builtin_ids_are_refused(page):
    bpa_custom.save_rule_file(
        page,
        [{"id": "MINE", "name": "Mine", "severity": "Low", "scope": "Measure",
          "conditions": [{"property": "name", "op": "not_empty"}]}],
    )
    before = page.read_text(encoding="utf-8")

    async def scenario(user):
        await _fill(user, rule_id="mine")
        user.find(marker="rule_save").click()
        await user.should_see("a custom rule with this id exists already")
        _el(user, "rule_id").value = FIRST
        user.find(marker="rule_save").click()
        await user.should_see("the id of a built-in rule", retries=10)
        assert page.read_text(encoding="utf-8") == before

    _simulate(scenario)


def test_a_broken_file_shows_the_banner_and_asks_before_overwriting(page):
    page.parent.mkdir(parents=True, exist_ok=True)
    page.write_text(
        "version: 1\nrules:\n  - id: BROKEN\n    name: x\n    severity: Huge\n    scope: Measure\n"
        "    conditions: [{property: name, op: not_empty}]\n",
        encoding="utf-8",
    )

    async def scenario(user):
        await user.should_see(marker="rules_file_errors")
        await user.should_see("BROKEN", retries=1)
        _el(user, f"builtin_{FIRST}").value = False
        await user.should_see(web_rules.CONFIRM_OVERWRITE, retries=20)
        assert "BROKEN" in page.read_text(encoding="utf-8")  # not yet
        user.find(marker="confirm_overwrite").click()
        await _wait_for(lambda: "BROKEN" not in page.read_text(encoding="utf-8"))
        assert _saved(page)["disabled"] == [FIRST]
        await user.should_not_see(marker="rules_file_errors")

    _simulate(scenario)


def test_test_is_disabled_before_a_run(page):
    async def scenario(user):
        await _fill(user)
        await user.should_see("Run a documentation first")
        assert not _el(user, "rule_test").enabled

    _simulate(scenario)


def test_test_shows_findings_after_a_run(page):
    model = parse_model(json.loads(json.dumps(BIM)))
    web_rules.remember_run("Sample", model)

    async def scenario(user):
        await _fill(user, value=".")  # every measure has a name
        _el(user, "cond_property_conditions_0").value = "name"
        assert _el(user, "rule_test").enabled
        user.find(marker="rule_test").click()
        count = len(model.all_measures)
        await user.should_see(f"{count} finding{'' if count == 1 else 's'} on Sample", retries=50)
        assert not page.exists()  # Test never saves

    _simulate(scenario)


def test_a_batch_run_is_read_from_its_model_file(tmp_path, page):
    bim = tmp_path / "Batch.bim"
    bim.write_text(json.dumps(BIM), encoding="utf-8")
    web_rules.remember_model_file("Batch", bim)
    name, findings = web_rules.run_test(
        {"id": "X", "name": "x", "severity": "Low", "scope": "Measure",
         "conditions": [{"property": "name", "op": "not_empty"}]}
    )
    assert name == "Batch" and len(findings) == len(parse_model(BIM).all_measures)


def test_a_successful_run_is_remembered(page):
    model = parse_model(BIM)

    class Doc:
        live_statistics = None

    Doc.model = model
    result = web_ui.ExtractionResult("success", "ok", documentation=Doc())
    options = web_ui.ExtractionOptions(report_path="Sales.pbix", model_path="Sales.bim", output_dir=".")
    web_ui._remember_for_rules(result, options)
    assert web_rules.last_run().name == "Sales" and web_rules.last_run().model is model


def test_property_choices_follow_the_scope():
    assert "expression" in web_rules.properties_for("Measure")
    assert web_rules.properties_for("Relationship") == ["name"]
    assert ui  # nicegui imported (the page module needs it)


def test_start_adds_the_rules_page(monkeypatch):
    pages = {}
    monkeypatch.setattr(web_ui, "register_routes", lambda: None)
    monkeypatch.setattr(web_ui, "add_host_check", lambda port: None)
    monkeypatch.setattr(web_ui.ui, "run", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        web_ui.ui, "page", lambda path, **kwargs: (lambda func: pages.setdefault(path, func))
    )
    web_ui.start(open_browser=False)
    assert pages == {"/rules": web_ui.rules_index}


def _rule(rule_id, name="A rule"):
    return {"id": rule_id, "name": name, "severity": "Low", "scope": "Measure",
            "conditions": [{"property": "name", "op": "not_empty"}]}


def test_a_save_keeps_what_another_tab_saved_meanwhile(page):
    """D31: tab B (loaded before tab A added X) switches a built-in off; both are kept."""
    async def scenario(user):
        await user.should_see(f"Built-in rules ({len(load_rules())})")
        bpa_custom.save_rule_file(page, [_rule("X")])  # tab A, after this page loaded
        _el(user, f"builtin_{FIRST}").value = False
        await _wait_for(lambda: _saved(page)["disabled"] == [FIRST])
        assert [r["id"] for r in _saved(page)["rules"]] == ["X"]
        await user.should_see(marker="custom_X")  # the list shows the file again

    _simulate(scenario)


def test_a_save_keeps_a_hand_edit(page):
    bpa_custom.save_rule_file(page, [_rule("OLD")])

    async def scenario(user):
        await user.should_see(marker="custom_OLD")
        page.write_text(  # edited by hand after the page loaded
            "version: 1\ndisabled: []\nrules:\n"
            "  - {id: OLD, name: Edited by hand, severity: High, scope: Measure,\n"
            "     conditions: [{property: name, op: not_empty}]}\n"
            "  - {id: HAND, name: Hand, severity: Low, scope: Table,\n"
            "     conditions: [{property: hidden, op: equals, value: true}]}\n",
            encoding="utf-8",
        )
        await _fill(user, rule_id="NEW")
        user.find(marker="rule_save").click()
        await _wait_for(lambda: len(_saved(page)["rules"]) == 3)
        rules = _saved(page)["rules"]
        assert [r["id"] for r in rules] == ["OLD", "HAND", "NEW"]
        assert rules[0]["name"] == "Edited by hand" and rules[0]["severity"] == "High"

    _simulate(scenario)


def test_changes_apply_to_the_file_as_it_is_now(page):
    bpa_custom.save_rule_file(page, [_rule("A"), _rule("B")])
    new_a = bpa_custom.rule_from_dict(_rule("A", "New A"))

    current, outcome = web_rules.apply_change(web_rules.add_rule(new_a))
    assert outcome and "exists already" in outcome  # add: refused, the id exists now
    current, outcome = web_rules.apply_change(web_rules.replace_rule("A", new_a))
    assert outcome is None and [r.name for r in current.rules] == ["New A", "A rule"]
    renamed = bpa_custom.rule_from_dict(_rule("B"))
    _, outcome = web_rules.apply_change(web_rules.replace_rule("A", renamed))
    assert outcome and "exists already" in outcome  # edit to another rule's id
    bpa_custom.save_rule_file(page, [_rule("B")])  # A deleted meanwhile
    current, outcome = web_rules.apply_change(web_rules.replace_rule("A", new_a))
    assert outcome is None and [r.id for r in current.rules] == ["B", "A"]  # added again
    _, outcome = web_rules.apply_change(web_rules.delete_rule_id("GONE"))
    assert outcome == ""  # nothing to do
    current, outcome = web_rules.apply_change(web_rules.switch_builtin(FIRST, False))
    assert outcome is None and current.disabled == [FIRST]
    _, outcome = web_rules.apply_change(web_rules.switch_builtin(FIRST, True))
    assert outcome == ""  # on already


def test_cancel_puts_the_switch_back_without_asking_again(page):
    page.parent.mkdir(parents=True, exist_ok=True)
    page.write_text(
        "version: 1\nrules:\n  - id: BROKEN\n    name: x\n    severity: Huge\n    scope: Measure\n"
        "    conditions: [{property: name, op: not_empty}]\n",
        encoding="utf-8",
    )
    before = page.read_text(encoding="utf-8")

    def dialogs(user):
        return [e for e in user.client.elements.values() if "confirm_cancel" in e._markers]

    async def scenario(user):
        await user.should_see(marker="rules_file_errors")
        _el(user, f"builtin_{FIRST}").value = False
        await user.should_see(web_rules.CONFIRM_OVERWRITE, retries=20)
        user.find(marker="confirm_cancel").click()
        await _wait_for(lambda: _el(user, f"builtin_{FIRST}").value is True)
        for _ in range(20):  # give a looping handler time to show itself
            await asyncio.sleep(0.05)
            assert not dialogs(user)
        assert page.read_text(encoding="utf-8") == before
        await user.should_see(marker="rules_file_errors")

    _simulate(scenario)


def test_an_embedded_run_keeps_no_model(page, monkeypatch):
    """D27: embedded there is no rules page, so the last run's model is not kept."""
    monkeypatch.setattr(web_ui.web_config, "CONFIG", web_ui.web_config.WebConfig(embedded=True))
    model = parse_model(BIM)

    class Doc:
        live_statistics = None

    Doc.model = model
    result = web_ui.ExtractionResult("success", "ok", documentation=Doc())
    options = web_ui.ExtractionOptions(report_path="Sales.pbix", model_path="Sales.bim", output_dir=".")
    web_ui._remember_for_rules(result, options)
    assert web_rules.last_run() is None
    outcome = web_ui.batch.JobOutcome(job=None, status="success", message="", options=options)
    web_ui._remember_outcome_for_rules(outcome)
    assert web_rules.last_run() is None
