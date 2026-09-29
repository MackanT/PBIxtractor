"""Tests for data.yaml configuration and the visual handler registry."""

import logging

import pytest

from pbixtractor.config import load_config, parse_config
from pbixtractor.extractors import ReportContext, VisualExtractor
from pbixtractor.readers import FieldBinding, VisualDefinition

YAML = {
    "data_types": [{"name": "Values", "friendly_name": "Values"}],
    "function_names": ["SUM"],
    "visual_type_metadata": {
        "standard_visuals": ["lineChart"],
        "special_visuals": {
            "tableEx": {"display_name": "Table", "item_type": "Visual"},
            "actionButton": {"display_name": "Button", "item_type": "Button"},
            "Group": {"display_name": "Panel", "item_type": "Group"},
        },
    },
    "extract_types": {"actionButton": "button", "shape": "skip", "fancyNavigator": "button"},
}

BOOKMARK_LINK = {
    "visualLink": [
        {
            "properties": {
                "type": {"expr": {"Literal": {"Value": "'Bookmark'"}}},
                "bookmark": {"expr": {"Literal": {"Value": "'Bookmark1'"}}},
            }
        }
    ]
}
CONTEXT = ReportContext(bookmark_names={"Bookmark1": "Panel Open"})


def _extractor() -> VisualExtractor:
    return VisualExtractor(parse_config(YAML), logging.getLogger("test_config"))


def test_supported_types_derived_from_metadata():
    config = parse_config(YAML)
    assert config.supported_visual_types == {"lineChart", "tableEx"}  # not button/group
    assert config.data_types == {"Values": "Values"}
    assert config.extract_type("fancyNavigator") == "button"
    assert config.extract_type("lineChart") == "standard"  # default


def test_invalid_extract_type_is_rejected():
    with pytest.raises(ValueError, match="Unknown extract_types"):
        parse_config({**YAML, "extract_types": {"shape": "ignore"}})


def test_bundled_config_loads():
    config = load_config()
    assert "tableEx" in config.supported_visual_types
    assert config.extract_type("actionButton") == "button"
    assert config.extract_type("textbox") == "skip"
    assert "CALCULATE" in config.function_names


def test_new_button_type_needs_only_yaml():
    visual = VisualDefinition(
        name="nav1", visual_type="fancyNavigator", container_objects=BOOKMARK_LINK
    )
    items = _extractor().extract(visual, "Page", CONTEXT)
    assert [i.to_list() for i in items] == [
        ["Page", "fancyNavigator", "nav1", "", "Panel Open", None, "Bookmark"]
    ]


def test_skip_type_dropped_unless_it_has_an_action():
    extractor = _extractor()
    assert extractor.extract(VisualDefinition(name="s1", visual_type="shape"), "Page") == []
    clickable = VisualDefinition(name="s2", visual_type="shape", container_objects=BOOKMARK_LINK)
    assert extractor.extract(clickable, "Page", CONTEXT)[0].visual_type == "actionButton"


def test_unknown_type_extracted_generically_and_logged(caplog):
    visual = VisualDefinition(
        name="v1",
        visual_type="someCustomVisual",
        fields=[
            FieldBinding(
                role="Values",
                expr={
                    "Column": {
                        "Expression": {"SourceRef": {"Entity": "Sales"}},
                        "Property": "Amount",
                    }
                },
                query_ref="Sales.Amount",
            )
        ],
    )
    with caplog.at_level(logging.WARNING, logger="test_config"):
        items = _extractor().extract(visual, "Page")
    assert [(i.table_name, i.val_name) for i in items] == [("Sales", "Amount")]
    assert "Unknown visual type: someCustomVisual" in caplog.text
