"""Hand-built, anonymised report layout mirroring the structure of real .pbix files."""

import json
import zipfile
from pathlib import Path


def _literal(value: str) -> dict:
    return {"expr": {"Literal": {"Value": value}}}


def _column(source: str, prop: str) -> dict:
    return {"Column": {"Expression": {"SourceRef": {"Source": source}}, "Property": prop}}


def _visual(name: str, single_visual: dict, filters: list = None) -> dict:
    container = {"config": {"name": name, "singleVisual": single_visual}}
    if filters is not None:
        container["filters"] = filters
    return container


TABLE = _visual(
    "tbl1",
    {
        "visualType": "tableEx",
        "projections": {"Values": [{"queryRef": "Sales.Amount"}, {"queryRef": "_Measures.Budget"}]},
        "prototypeQuery": {
            "Version": 2,
            "From": [
                {"Name": "s", "Entity": "Sales", "Type": 0},
                {"Name": "b", "Entity": "Budget", "Type": 0},
            ],
            "Select": [
                {**_column("s", "Amount"), "Name": "Sales.Amount", "NativeReferenceName": "Amount"},
                # Stale Name: the measure was renamed and moved to the Budget table
                {
                    "Measure": {
                        "Expression": {"SourceRef": {"Source": "b"}},
                        "Property": "Budget OC",
                    },
                    "Name": "_Measures.Budget",
                    "NativeReferenceName": "Budget",
                },
            ],
        },
        "columnProperties": {"_Measures.Budget": {"displayName": "Budget (OC)"}},
        "objects": {
            "values": [
                {
                    "properties": {
                        "backColor": {
                            "solid": {
                                "color": {
                                    "expr": {
                                        "Measure": {
                                            "Expression": {"SourceRef": {"Entity": "_Measures"}},
                                            "Property": "Color Flag",
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        },
    },
    filters=[
        {
            "name": "Filter1",
            "expression": {
                "Column": {"Expression": {"SourceRef": {"Entity": "Dates"}}, "Property": "Year"}
            },
            "filter": {
                "Version": 2,
                "From": [{"Name": "d", "Entity": "Dates", "Type": 0}],
                "Where": [
                    {
                        "Condition": {
                            "And": {
                                "Left": {
                                    "Comparison": {
                                        "ComparisonKind": 1,
                                        "Left": _column("d", "Year"),
                                        "Right": {"Literal": {"Value": "2019L"}},
                                    }
                                },
                                "Right": {
                                    "Comparison": {
                                        "ComparisonKind": 4,
                                        "Left": _column("d", "Year"),
                                        "Right": {"Literal": {"Value": "2030L"}},
                                    }
                                },
                            }
                        }
                    }
                ],
            },
            "type": "Advanced",
        },
        # In the filter pane but without a condition -> ignored
        {
            "expression": {
                "Column": {"Expression": {"SourceRef": {"Entity": "Dates"}}, "Property": "Month"}
            },
            "type": "Categorical",
        },
    ],
)

SLICER = _visual(
    "slc1",
    {
        "visualType": "slicer",
        "syncGroup": {"groupName": "Year", "fieldChanges": True, "filterChanges": True},
        "projections": {"Values": [{"queryRef": "Dates.Date Hierarchy.Year Number"}]},
        "prototypeQuery": {
            "Version": 2,
            "From": [{"Name": "d", "Entity": "Dates", "Type": 0}],
            "Select": [
                {
                    "HierarchyLevel": {
                        "Expression": {
                            "Hierarchy": {
                                "Expression": {"SourceRef": {"Source": "d"}},
                                "Hierarchy": "Date Hierarchy",
                            }
                        },
                        "Level": "Year",
                    },
                    "Name": "Dates.Date Hierarchy.Year Number",
                    "NativeReferenceName": "Date Hierarchy Year",
                }
            ],
        },
    },
)

MATRIX = _visual(
    "mtx1",
    {
        "visualType": "pivotTable",
        "projections": {"Values": [{"queryRef": "Sum(Sales.Qty)"}], "Rows": []},
        "prototypeQuery": {
            "Version": 2,
            "From": [{"Name": "s", "Entity": "Sales", "Type": 0}],
            "Select": [
                {
                    "Aggregation": {"Expression": _column("s", "Qty"), "Function": 0},
                    "Name": "Sum(Sales.Qty)",
                    "NativeReferenceName": "Sum of Qty",
                },
                # Selected but not bound to any projection role
                {**_column("s", "Hidden"), "Name": "Sales.Hidden"},
            ],
        },
    },
)

BOOKMARK_BUTTON = _visual(
    "btn1",
    {
        "visualType": "actionButton",
        "vcObjects": {
            "visualLink": [
                {
                    "properties": {
                        "show": _literal("true"),
                        "type": _literal("'Bookmark'"),
                        "bookmark": _literal("'Bookmark1'"),
                    }
                }
            ],
            "title": [{"properties": {"text": _literal("'Show panel'")}}],
        },
    },
)

NAVIGATION_BUTTON = _visual(
    "btn2",
    {
        "visualType": "actionButton",
        "objects": {"text": [{"properties": {"text": _literal("'Go to detail'")}}]},
        "vcObjects": {
            "visualLink": [
                {
                    "properties": {
                        "type": _literal("'PageNavigation'"),
                        "navigationSection": _literal("'ReportSectionB'"),
                    }
                }
            ]
        },
    },
)

ICON_BUTTON = _visual("btn3", {"visualType": "actionButton", "objects": {}})

# Points to a bookmark that was deleted from the report
BROKEN_BUTTON = _visual(
    "btn4",
    {
        "visualType": "actionButton",
        "vcObjects": {
            "visualLink": [
                {
                    "properties": {
                        "type": _literal("'Bookmark'"),
                        "bookmark": _literal("'Bookmarkdeadbeef'"),
                    }
                }
            ]
        },
    },
)

GROUP = {
    "config": {
        "name": "grp1",
        "singleVisualGroup": {"displayName": "Filter Popup", "isHidden": True},
    }
}

# Bookmark1: captures display + current page (not data) for two selected visuals, hides both
BOOKMARK_STATE = {
    "explorationState": {
        "activeSection": "ReportSectionA",
        "sections": {
            "ReportSectionA": {
                "visualContainers": {"tbl1": {"singleVisual": {"display": {"mode": "hidden"}}}},
                "visualContainerGroups": {"grp1": {"isHidden": True}},
            }
        },
    },
    "options": {"suppressData": True, "targetVisualNames": ["tbl1", "grp1"]},
}

SHAPE = _visual("shp1", {"visualType": "shape", "objects": {}})

# A shape used as a button (has an action)
CLICKABLE_SHAPE = _visual(
    "shp2",
    {
        "visualType": "shape",
        "vcObjects": {
            "visualLink": [
                {
                    "properties": {
                        "type": _literal("'Bookmark'"),
                        "bookmark": _literal("'Bookmark2'"),
                    }
                }
            ]
        },
    },
)

PAGE_FILTERS = [
    {
        "name": "Filter2",
        "expression": {
            "Column": {"Expression": {"SourceRef": {"Entity": "Warehouses"}}, "Property": "Code"}
        },
        "filter": {
            "Version": 2,
            "From": [{"Name": "w", "Entity": "Warehouses", "Type": 0}],
            "Where": [
                {
                    "Condition": {
                        "Not": {
                            "Expression": {
                                "In": {
                                    "Expressions": [_column("w", "Code")],
                                    "Values": [[{"Literal": {"Value": "'D1'"}}]],
                                }
                            }
                        }
                    }
                }
            ],
        },
        "type": "Categorical",
    }
]

REPORT_FILTERS = [
    {
        "expression": {
            "Column": {"Expression": {"SourceRef": {"Entity": "Dates"}}, "Property": "Is Future"}
        },
        "filter": {
            "Version": 2,
            "From": [{"Name": "d", "Entity": "Dates", "Type": 0}],
            "Where": [
                {
                    "Condition": {
                        "In": {
                            "Expressions": [_column("d", "Is Future")],
                            "Values": [[{"Literal": {"Value": "false"}}]],
                        }
                    }
                }
            ],
        },
        "type": "Categorical",
    }
]

LAYOUT = {
    "config": {
        "bookmarks": [
            {"name": "Bookmark1", "displayName": "Panel Open", **BOOKMARK_STATE},
            {
                "name": "BookmarkGroup",
                "displayName": "Group",
                "children": [{"name": "Bookmark2", "displayName": "Nested"}],
            },
        ]
    },
    "filters": REPORT_FILTERS,
    "sections": [
        {
            "name": "ReportSectionA",
            "displayName": "Sales",
            # "Edit interactions": the slicer has no effect on the table (type 3)
            "config": {"relationships": [{"source": "slc1", "target": "tbl1", "type": 3}]},
            "filters": PAGE_FILTERS,
            "visualContainers": [
                TABLE,
                SLICER,
                MATRIX,
                BOOKMARK_BUTTON,
                NAVIGATION_BUTTON,
                ICON_BUTTON,
                BROKEN_BUTTON,
                GROUP,
                SHAPE,
                CLICKABLE_SHAPE,
            ],
        },
        {
            "name": "ReportSectionB",
            "displayName": "Detail",
            "config": {"visibility": 1},  # hidden page
            "filters": "[]",
            "visualContainers": [],
        },
    ],
}


def _stringify(layout: dict) -> dict:
    """Encode nested fields as JSON strings, the way Power BI stores them in Report/Layout."""
    encoded = json.loads(json.dumps(layout))
    encoded["config"] = json.dumps(encoded["config"])
    encoded["filters"] = json.dumps(encoded["filters"])
    for section in encoded["sections"]:
        if not isinstance(section["filters"], str):
            section["filters"] = json.dumps(section["filters"])
        if "config" in section:
            section["config"] = json.dumps(section["config"])
        for container in section["visualContainers"]:
            for key in ("config", "filters"):
                if key in container:
                    container[key] = json.dumps(container[key])
    return encoded


def write_sample_pbix(path: Path) -> Path:
    """Write a minimal .pbix (zip) containing only Report/Layout."""
    with zipfile.ZipFile(path, "w") as zip_file:
        zip_file.writestr("Report/Layout", json.dumps(_stringify(LAYOUT)).encode("utf-16-le"))
    return path
