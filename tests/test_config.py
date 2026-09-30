"""Tests for data.yaml configuration and the visual handler registry."""

import logging

import pytest

from pbixtractor.config import load_config, parse_config
from pbixtractor.extractors import ReportContext, VisualExtractor
from pbixtractor.readers import FieldBinding, VisualDefinition
from pbixtractor.visual_helpers import custom_visual_name

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


def _field(role: str) -> FieldBinding:
    return FieldBinding(
        role=role,
        expr={"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Amount"}},
        query_ref="Sales.Amount",
    )


@pytest.mark.parametrize(
    "visual_type, expected",
    [
        ("PowerApps_PBI_CV_C29F1DCC_81F5_4973_94AD_0517D44CC06A", "Power Apps"),
        ("castellumCharts9A467DB81DD645A3AF0FB12DA8C0231E", "Castellum Charts"),
        ("ChicletSlicer1448559807354", "Chiclet Slicer"),
        ("clusteredColumnChart", None),
        ("tableEx", None),
    ],
)
def test_custom_visual_names(visual_type, expected):
    assert custom_visual_name(visual_type) == expected


def test_custom_visuals_are_documented_without_warnings(caplog):
    visual_type = "castellumCharts9A467DB81DD645A3AF0FB12DA8C0231E"
    visual = VisualDefinition(name="c1", visual_type=visual_type, fields=[_field("tooltip_cols")])
    with caplog.at_level(logging.WARNING, logger="test_config"):
        items = _extractor().extract(visual, "Page")
    assert [(i.val_name, i.data_type) for i in items] == [("Amount", "Tooltip cols")]
    assert caplog.text == ""
    mapper = parse_config(YAML).visual_mapper
    assert mapper.get_visual_info(visual_type) == ("Visual", "Castellum Charts (custom visual)")


def test_unknown_types_and_roles_are_reported_once(caplog):
    extractor = _extractor()
    with caplog.at_level(logging.WARNING, logger="test_config"):
        for page in ("Page 1", "Page 2"):
            for name in ("v1", "v2"):
                visual = VisualDefinition(
                    name=name, visual_type="someNewVisual", fields=[_field("odd"), _field("odd")]
                )
                extractor.extract(visual, page)
    assert caplog.text.count("Unknown visual type: someNewVisual (first on Page 1)") == 1
    assert caplog.text.count("Unknown visual role 'odd'") == 1
