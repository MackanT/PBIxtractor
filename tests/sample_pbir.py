"""The sample report from sample_layout.py, in the PBIR format (one JSON file per page/visual).

Structure (inside a .pbix under "Report/", or in a PBIP "<name>.Report/" folder):
    definition/report.json                       report settings + report filters
    definition/pages/pages.json                  page order
    definition/pages/<page>/page.json            page + page filters
    definition/pages/<page>/visuals/<v>/visual.json
    definition/bookmarks/bookmarks.json          bookmark order + groups
    definition/bookmarks/<name>.bookmark.json
"""

import copy
import json
import zipfile
from pathlib import Path

from . import sample_layout as legacy


def _pbir_filters(filters: list) -> list:
    """PBIR filters use "field" where the legacy layout uses "expression"."""
    converted = copy.deepcopy(filters)
    for filter_obj in converted:
        if "expression" in filter_obj:
            filter_obj["field"] = filter_obj.pop("expression")
    return converted


def _projection(field: dict, query_ref: str, native: str, display_name: str = None) -> dict:
    projection = {"field": field, "queryRef": query_ref, "nativeQueryRef": native}
    if display_name:
        projection["displayName"] = display_name
    return projection


def _column(table: str, prop: str) -> dict:
    return {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": prop}}


def _visual(name: str, visual: dict, filters: list = None) -> dict:
    visual_json = {"name": name, "position": {"x": 0, "y": 0, "z": 0}, "visual": visual}
    if filters:
        visual_json["filterConfig"] = {"filters": _pbir_filters(filters)}
    return visual_json


def _legacy_single_visual(container: dict) -> dict:
    return container["config"]["singleVisual"]


def _button(container: dict) -> dict:
    """Convert a legacy action button / shape to a PBIR visual."""
    single_visual = _legacy_single_visual(container)
    visual = {
        "visualType": single_visual["visualType"],
        "objects": single_visual.get("objects", {}),
    }
    if "vcObjects" in single_visual:
        visual["visualContainerObjects"] = single_visual["vcObjects"]
    return _visual(container["config"]["name"], visual)


VISUALS = [
    _visual(
        "tbl1",
        {
            "visualType": "tableEx",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            _projection(_column("Sales", "Amount"), "Sales.Amount", "Amount"),
                            # Stale queryRef: the measure was renamed and moved to Budget
                            _projection(
                                {
                                    "Measure": {
                                        "Expression": {"SourceRef": {"Entity": "Budget"}},
                                        "Property": "Budget OC",
                                    }
                                },
                                "_Measures.Budget",
                                "Budget",
                                display_name="Budget (OC)",
                            ),
                        ]
                    }
                }
            },
            "objects": _legacy_single_visual(legacy.TABLE)["objects"],
        },
        filters=legacy.TABLE["filters"],
    ),
    _visual(
        "slc1",
        {
            "visualType": "slicer",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            _projection(
                                {
                                    "HierarchyLevel": {
                                        "Expression": {
                                            "Hierarchy": {
                                                "Expression": {"SourceRef": {"Entity": "Dates"}},
                                                "Hierarchy": "Date Hierarchy",
                                            }
                                        },
                                        "Level": "Year",
                                    }
                                },
                                "Dates.Date Hierarchy.Year Number",
                                "Date Hierarchy Year",
                            )
                        ]
                    }
                }
            },
            "syncGroup": {"groupName": "Year", "fieldChanges": True, "filterChanges": True},
        },
    ),
    _visual(
        "mtx1",
        {
            "visualType": "pivotTable",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            _projection(
                                {
                                    "Aggregation": {
                                        "Expression": _column("Sales", "Qty"),
                                        "Function": 0,
                                    }
                                },
                                "Sum(Sales.Qty)",
                                "Sum of Qty",
                            )
                        ]
                    }
                }
            },
        },
    ),
    _button(legacy.BOOKMARK_BUTTON),
    _button(legacy.NAVIGATION_BUTTON),
    _button(legacy.ICON_BUTTON),
    _button(legacy.BROKEN_BUTTON),
    {
        "name": "grp1",
        "position": {},
        "visualGroup": {"displayName": "Filter Popup"},
        "isHidden": True,
    },
    _button(legacy.SHAPE),
    _button(legacy.CLICKABLE_SHAPE),
]


def _definition_files() -> dict[str, dict]:
    """All files of the definition/ folder, keyed by relative path."""
    files = {
        "definition/version.json": {"version": "2.0.0"},
        "definition/report.json": {
            "filterConfig": {"filters": _pbir_filters(legacy.REPORT_FILTERS)}
        },
        "definition/pages/pages.json": {"pageOrder": ["ReportSectionA", "ReportSectionB"]},
        "definition/pages/ReportSectionA/page.json": {
            "name": "ReportSectionA",
            "displayName": "Sales",
            "filterConfig": {"filters": _pbir_filters(legacy.PAGE_FILTERS)},
            "visualInteractions": [{"source": "slc1", "target": "tbl1", "type": "NoFilter"}],
        },
        "definition/pages/ReportSectionB/page.json": {
            "name": "ReportSectionB",
            "displayName": "Detail",
            "visibility": "HiddenInViewMode",
        },
        "definition/bookmarks/bookmarks.json": {
            "items": [
                {"name": "Bookmark1"},
                {"name": "BookmarkGroup", "displayName": "Group", "children": ["Bookmark2"]},
            ]
        },
        "definition/bookmarks/Bookmark1.bookmark.json": {
            "name": "Bookmark1",
            "displayName": "Panel Open",
            **legacy.BOOKMARK_STATE,
        },
        "definition/bookmarks/Bookmark2.bookmark.json": {
            "name": "Bookmark2",
            "displayName": "Nested",
        },
    }
    for visual in VISUALS:
        files[f"definition/pages/ReportSectionA/visuals/{visual['name']}/visual.json"] = visual
    return files


def write_sample_pbir_pbix(path: Path) -> Path:
    """Write a .pbix (zip) that stores the report in the PBIR format."""
    with zipfile.ZipFile(path, "w") as zip_file:
        for name, content in _definition_files().items():
            zip_file.writestr(f"Report/{name}", json.dumps(content))
    return path


def write_sample_pbip(folder: Path, name: str = "Sample") -> Path:
    """Write a PBIP project (<name>.pbip + <name>.Report/definition/...); returns the .pbip."""
    for relative, content in _definition_files().items():
        target = folder / f"{name}.Report" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        # Power BI writes UTF-8 with BOM for some files
        target.write_text(json.dumps(content, indent=2), encoding="utf-8-sig")
    pbip = folder / f"{name}.pbip"
    pbip.write_text(json.dumps({"artifacts": [{"report": {"path": f"{name}.Report"}}]}))
    return pbip
