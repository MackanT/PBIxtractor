"""Tests for the JSON output (json_report.py)."""

import json
import logging
import tempfile
from pathlib import Path

import pytest

import pbixtractor.extractor as extractor
from pbixtractor.documentation import build_documentation
from pbixtractor.json_report import SCHEMA_VERSION, documentation_to_dict, write_json
from pbixtractor.semantic_model import model_to_dataset, parse_model
from pbixtractor.tabular_editor import parse_dependencies

from .sample_layout import write_sample_pbix
from .test_semantic_model import BIM

DEPENDENCIES = parse_dependencies(
    "SourceType\tSourceTable\tSourceName\tTargetType\tTargetTable\tTargetName\n"
    "Measure\tSales\tTotal Amount\tColumn\tSales\tAmount\n"
    "Measure\tSales\tDynamic\tMeasure\tSales\tTotal Amount\n"
    "TablePermission\tSales\tNordics\tColumn\tSales\tAmount\n"
)


@pytest.fixture(scope="module")
def data():
    folder = Path(tempfile.mkdtemp())
    write_sample_pbix(folder / "Sample.pbix")
    report = extractor.ReportExtractor(str(folder), "Sample.pbix")
    report.extract()
    model = parse_model(BIM)
    documentation = build_documentation(
        report_items=report.result,
        report_filters=report.filters,
        model=model,
        dataset=model_to_dataset(model),
        report_name="Sample",
        description_tag="////",
        visual_mapper=extractor.visual_mapper,
        visual_types=extractor.visual_type_list,
        logger=logging.getLogger("test"),
        exact_dependencies=DEPENDENCIES,
        report=report.report,
    )
    return documentation_to_dict(documentation)


def test_top_level_sections(data):
    assert data["schema_version"] == SCHEMA_VERSION
    assert data["generator"].startswith("pbixtractor ")
    assert set(data) >= {"report", "model", "dependencies", "unused", "quality", "lineage"}
    assert data["quality"] is None  # BPA not run
    assert data["dependencies_exact"] is True


def test_pages_and_bookmarks(data):
    pages = {p["name"]: p for p in data["report"]["pages"]}
    assert pages["Detail"]["hidden"] is True and pages["Detail"]["items"] == []
    assert pages["Sales"]["sync_groups"] == ["Year"]
    slicer = next(i for i in pages["Sales"]["items"] if i.get("id") == "slc1")
    assert slicer["interactivity"] == ["Sync group: Year", "No effect on Table (tbl1)"]

    bookmark = next(b for b in data["report"]["bookmarks"] if b["id"] == "Bookmark1")
    assert bookmark["captures"] == ["Display", "Current page"]
    assert bookmark["used_by"] == ["Sales (btn1)"]
    assert bookmark["hides"] == ["Table (tbl1) on Sales", "Panel (grp1) on Sales"]


def test_report_section(data):
    page = next(p for p in data["report"]["pages"] if p["name"] == "Sales")
    by_id = {item.get("id"): item for item in page["items"]}

    table = by_id["tbl1"]
    assert {
        "table": "Budget",
        "name": "Budget OC",
        "display_name": "Budget (OC)",
        "role": "Values",
    } in table["fields"]
    assert table["filters"] == [{"field": "Dates[Year]", "condition": "> 2019 and < 2030"}]

    assert by_id["btn1"] == {
        "type": "Button",
        "visual_type": "Button",
        "id": "btn1",
        "action": "Bookmark",
        "target": "Panel Open",
        "label": "Show panel",
    }
    page_filter = next(i for i in page["items"] if i["type"] == "Filter")
    assert page_filter == {
        "type": "Filter",
        "visual_type": "This Page",
        "field": "Warehouses[Code]",
        "condition": "<> D1",
    }
    levels = {f["level"] for f in data["report"]["filters"]}
    assert levels == {"All Pages", "This Page", "Visual"}


def test_model_section(data):
    tables = {t["name"]: t for t in data["model"]["tables"]}
    assert tables["Dates"]["storage_mode"] == "Direct Lake"
    assert tables["Dates"]["source_objects"] == ["gold.dim_date"]
    assert tables["Sales"]["rows"] is None  # no live statistics
    measure = tables["Sales"]["measures"][0]
    assert measure["expression"] == "SUM ( Sales[Amount] )"
    assert data["model"]["relationships"][1]["active"] is False
    assert data["model"]["roles"][0]["filters"] == {"Sales": "Sales[Amount] > 0"}


def test_unused_and_dependencies(data):
    assert data["unused"]["measures"] == ["Sales[Dynamic]"]
    assert {"source": "Sales[Dynamic]", "target": "Sales[Total Amount]"}.items() <= next(
        d for d in data["dependencies"] if d["source"] == "Sales[Dynamic]"
    ).items()


def test_lineage_graph_is_consistent(data):
    nodes = {n["id"]: n for n in data["lineage"]["nodes"]}
    edges = data["lineage"]["edges"]
    assert all(e["source"] in nodes and e["target"] in nodes for e in edges)

    edge_set = {(e["source"], e["target"], e["type"]) for e in edges}
    assert ("page:Sales", "visual:Sales/tbl1", "contains") in edge_set
    assert ("visual:Sales/tbl1", "column:Sales[Amount]", "uses") in edge_set
    assert ("visual:Sales/tbl1", "column:Dates[Year]", "filters") in edge_set
    assert ("measure:Sales[Dynamic]", "measure:Sales[Total Amount]", "depends_on") in edge_set
    assert ("measure:Sales[Total Amount]", "column:Sales[Amount]", "depends_on") in edge_set
    assert ("table:Dates", "source:gold.dim_date", "loads_from") in edge_set
    # Both relationships between Sales and Dates are kept (role-playing style)
    relationships = [e for e in edges if e["type"] == "relationship"]
    assert len(relationships) == 2
    assert {r["from_column"] for r in relationships} == {"Date Key", "Amount"}


def test_write_json_round_trip(tmp_path, monkeypatch):
    content = {"schema_version": 1, "text": "åäö"}
    monkeypatch.setattr("pbixtractor.json_report.documentation_to_dict", lambda _: content)
    path = tmp_path / "out.json"

    write_json(str(path), documentation=None)

    assert json.loads(path.read_text(encoding="utf-8")) == content
    assert "åäö" in path.read_text(encoding="utf-8")  # not \u-escaped
