"""JSON output: the whole documentation as one machine-readable file.

Sections: report (pages, items, filters), model (tables with columns/measures/hierarchies/
partitions, relationships, roles, expressions), dependencies, unused, quality (BPA) and
lineage (nodes + edges for graph views: page -> visual -> field -> DAX -> table -> source).

    write_json("Report.json", documentation)
"""

import json
import math
from typing import Any, Optional

from . import __version__
from .dax import text_dependencies
from .documentation import (
    Documentation,
    PageItem,
    button_target_and_label,
    field_display_name,
    is_missing_target,
)
from .m_sources import NATIVE_SQL
from .model_sheets import STORAGE_MODES

SCHEMA_VERSION = 1


def _clean(value: Any) -> Any:
    """JSON-safe scalar: NaN/None -> None, numpy numbers -> int/float, others as is."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):  # numpy scalar
        return _clean(value.item())
    return value


def _ref(table: str, name: str) -> str:
    return f"{table}[{name}]"


def _split_ref(ref: str) -> tuple[str, str]:
    """
    "Table[Name]" / "'My Table'[Name]" / "'Params'" -> (table, name); name is "" for a table.

    Only the brackets around the name are removed ("Sales[Price [EUR]]" -> "Price [EUR]"),
    and DAX escapes are undone ('' inside a quoted table, ]] inside a name).
    """
    if ref.startswith("'"):
        end = 1
        while True:  # the closing quote is one not followed by another quote
            end = ref.index("'", end)
            if ref[end + 1 : end + 2] != "'":
                break
            end += 2
        table, rest = ref[1:end].replace("''", "'"), ref[end + 1 :]
    else:
        table, bracket, rest = ref.partition("[")
        rest = bracket + rest
    if rest.startswith("[") and rest.endswith("]"):
        return table, rest[1:-1].replace("]]", "]")
    return table, ""


# ============================================================================
# Sections
# ============================================================================


def _item_dict(item: PageItem, interactivity: list[str]) -> dict:
    data = {"type": item.item_type, "visual_type": item.visual_type}
    if item.item_type == "Filter":
        data.update(field=item.filter_field, condition=item.filter_condition)
        return data

    data["id"] = _clean(item.id)
    if item.item_type in ("Visual", "Slicer"):
        data["fields"] = [
            {
                "table": row["Table"],
                "name": row["Name"],
                "display_name": field_display_name(row),
                "role": row["Type"],
            }
            for _, row in item.fields.iterrows()
        ]
    else:
        target, label = button_target_and_label(item.first_row)
        if item.item_type == "Button":
            data.update(action=item.first_row["Type"], target=target, label=label)
            if is_missing_target(target):
                data["broken"] = True  # its bookmark or page was deleted
        else:
            data["name"] = target
    if item.visual_filters:
        data["filters"] = [{"field": f, "condition": c} for f, c in item.visual_filters]
    if interactivity:
        data["interactivity"] = interactivity
    return data


def _report_section(documentation: Documentation) -> dict:
    info = {p.name: p for p in documentation.page_info}
    page_names = list(info) or list(documentation.pages)  # page_info also has empty pages
    return {
        "name": documentation.report_name,
        "pages": [
            {
                "name": page,
                "hidden": info[page].hidden if page in info else None,
                "page_type": (info[page].page_type or None) if page in info else None,
                "sync_groups": info[page].sync_groups if page in info else [],
                "items": [
                    _item_dict(item, documentation.interactivity.get((page, str(item.id)), []))
                    for item in documentation.pages.get(page, [])
                ],
            }
            for page in page_names
        ],
        "bookmarks": [
            {
                "id": b.name,
                "name": b.display_name,
                "group": b.group or None,
                "page": b.page or None,
                "captures": [c for c in b.captures.split(", ") if c],
                "applies_to": b.applies_to,
                "hides": b.hidden_visuals,
                "used_by": b.used_by,
                **({"broken": True} if b.broken else {}),
            }
            for b in documentation.bookmarks
        ],
        "filters": [
            {
                "page": page or None,
                "item": _clean(item),
                "level": level,
                "field": field,
                "condition": condition,
            }
            for page, item, level, field, condition in documentation.filter_strings
        ],
    }


def _model_section(documentation: Documentation) -> dict:
    model = documentation.model
    stats = documentation.live_statistics

    tables = []
    for table in model.tables:
        sources = [p.source_summary() for p in table.partitions]
        column_stats = stats.columns if stats else {}
        tables.append(
            {
                "name": table.name,
                "description": table.description or None,
                "hidden": table.is_hidden,
                "storage_mode": ", ".join(
                    sorted({STORAGE_MODES.get(p.mode, p.mode) for p in table.partitions if p.mode})
                )
                or None,
                "connector": ", ".join(dict.fromkeys(c for c, _ in sources if c)) or None,
                "source_objects": list(
                    dict.fromkeys(o for _, objects in sources for o in objects.split(", ") if o)
                ),
                "rows": stats.table_rows.get(table.name) if stats else None,
                "size_bytes": stats.table_sizes.get(table.name) if stats else None,
                "columns": [
                    {
                        "name": c.name,
                        "data_type": c.data_type,
                        "kind": c.kind,
                        "source_column": c.source_column or None,
                        "expression": c.expression or None,
                        "hidden": c.is_hidden,
                        "key": c.is_key,
                        "data_category": c.data_category or None,
                        "summarize_by": c.summarize_by or None,
                        "sort_by": c.sort_by_column or None,
                        "format": c.format_string or None,
                        "display_folder": c.display_folder or None,
                        "description": c.description or None,
                        "distinct_values": getattr(
                            column_stats.get((table.name, c.name)), "distinct_values", None
                        ),
                        "size_bytes": getattr(
                            column_stats.get((table.name, c.name)), "total_size", None
                        )
                        if stats and stats.has_sizes
                        else None,
                    }
                    for c in table.columns
                ],
                "measures": [
                    {
                        "name": m.name,
                        "expression": m.expression,
                        "description": m.description or None,
                        "format": m.format_string or None,
                        "dynamic_format": m.format_string_expression or None,
                        "display_folder": m.display_folder or None,
                        "data_type": m.data_type or None,
                        "hidden": m.is_hidden,
                    }
                    for m in table.measures
                ],
                "hierarchies": [
                    {
                        "name": h.name,
                        "levels": [{"name": lv.name, "column": lv.column} for lv in h.levels],
                    }
                    for h in table.hierarchies
                ],
                "partitions": [
                    {
                        "name": p.name,
                        "mode": p.mode or None,
                        "source_type": p.source_type or None,
                        "connector": p.source_summary()[0] or None,
                        "source_objects": [o for o in p.source_summary()[1].split(", ") if o],
                        "expression": p.expression or None,
                    }
                    for p in table.partitions
                ],
            }
        )

    return {
        "name": model.name or None,
        "compatibility_level": model.compatibility_level,
        "culture": model.culture or None,
        "size_bytes": stats.model_size if stats and stats.has_sizes else None,
        "tables": tables,
        "relationships": [
            {
                "from_table": r.from_table,
                "from_column": r.from_column,
                "to_table": r.to_table,
                "to_column": r.to_column,
                "cardinality": r.cardinality_label,
                "cross_filter": r.direction_label,
                "active": r.is_active,
            }
            for r in model.relationships
        ],
        "roles": [
            {"name": r.name, "permission": r.model_permission or None, "filters": r.table_filters}
            for r in model.roles
        ],
        "expressions": [
            {"name": e.name, "kind": e.kind or None, "expression": e.expression}
            for e in model.expressions
        ],
    }


def _dependencies_section(documentation: Documentation) -> list[dict]:
    if documentation.exact_dependencies is not None:
        return [
            {
                "source": d.source_ref,
                "source_type": d.source_type,
                "target": d.target_ref,
                "target_type": d.target_type,
                "exact": True,
            }
            for d in documentation.exact_dependencies
        ]
    dependencies = []
    for _, row in documentation.objects.iterrows():
        if row["Type"] == "Column":
            continue
        for target in text_dependencies(row["Definition"]):
            dependencies.append(
                {
                    "source": _ref(row["Table"], row["Name"]),
                    "source_type": row["Type"],
                    "target": target,
                    "target_type": "Column" if not target.startswith("[") else None,
                    "exact": False,
                }
            )
    return dependencies


def _quality_section(documentation: Documentation) -> Optional[list[dict]]:
    if documentation.bpa_violations is None:
        return None
    return [
        {
            "severity": v.severity or None,
            "category": v.category or None,
            "rule": v.rule,
            "object_type": v.object_type or None,
            "object": v.object_name,
            "description": v.description or None,
        }
        for v in documentation.bpa_violations
    ]


# ============================================================================
# Lineage graph
# ============================================================================


def _lineage_section(documentation: Documentation, dependencies: list[dict]) -> dict:
    """
    Nodes and edges for graph views.

    Node ids: "page:<page>", "visual:<page>/<id>", "table:<table>", "column:<Table[Col]>",
    "measure:<Table[Measure]>", "source:<schema.object>".
    Edge types: contains (page->visual, table->column/measure), uses (visual->field),
    filters (visual->field), depends_on (DAX), relationship (table->table), loads_from
    (table->source).
    """
    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    measure_refs = {_ref(m.table, m.name) for m in documentation.model.all_measures}

    def node(node_id: str, node_type: str, label: str, **extra) -> str:
        nodes.setdefault(node_id, {"id": node_id, "type": node_type, "label": label, **extra})
        return node_id

    def field_node(table: str, name: str) -> str:
        ref = _ref(table, name)
        kind = "measure" if ref in measure_refs else "column"
        table_id = node(f"table:{table}", "table", table)
        field_id = node(f"{kind}:{ref}", kind, name, table=table)
        edge(table_id, field_id, "contains")
        return field_id

    seen_edges = set()

    def edge(source: str, target: str, edge_type: str, **extra) -> None:
        # Extra attributes are part of the key: role-playing relationships between the same
        # two tables (e.g. order/ship date) stay separate edges
        key = (source, target, edge_type, tuple(sorted(extra.items())))
        if key not in seen_edges:
            seen_edges.add(key)
            edges.append({"source": source, "target": target, "type": edge_type, **extra})

    model = documentation.model
    for table in model.tables:
        table_id = node(f"table:{table.name}", "table", table.name)
        for column in table.columns:
            field_node(table.name, column.name)
        for measure in table.measures:
            field_node(table.name, measure.name)
        for partition in table.partitions:
            for source in partition.source_summary()[1].split(", "):
                if source and source not in ("calculated table", NATIVE_SQL):
                    edge(table_id, node(f"source:{source}", "source", source), "loads_from")

    for rel in model.relationships:
        edge(
            f"table:{rel.from_table}",
            f"table:{rel.to_table}",
            "relationship",
            from_column=rel.from_column,
            to_column=rel.to_column,
            active=rel.is_active,
            cardinality=rel.cardinality_label,
            cross_filter=rel.direction_label,
        )

    # Every page gets a node, also pages without visuals (from page_info)
    for info in documentation.page_info:
        node(f"page:{info.name}", "page", info.name)

    for page, items in documentation.pages.items():
        page_id = node(f"page:{page}", "page", page)
        for item in items:
            if item.item_type == "Filter":
                continue
            visual_id = node(f"visual:{page}/{item.id}", "visual", str(item.visual_type), page=page)
            edge(page_id, visual_id, "contains")
            if item.fields is not None:
                for _, row in item.fields.iterrows():
                    if row["Table"] and row["Name"]:
                        edge(visual_id, field_node(row["Table"], row["Name"]), "uses")
            for field, _ in item.visual_filters:
                edge(visual_id, field_node(*_split_ref(field)), "filters")

    def ref_node(ref: str, ref_type: str) -> str:
        """Node of a dependency end: a table (e.g. a field parameter) or a column/measure."""
        table, name = _split_ref(ref)
        if ref_type == "Table" or not name:
            return node(f"table:{table}", "table", table)
        return field_node(table, name)

    for dependency in dependencies:
        if not dependency["exact"] or dependency["source_type"] in ("TablePermission", "CalculationItem"):
            continue  # text-matched refs may be unresolved; RLS and calc items have no node yet
        source_id = ref_node(dependency["source"], dependency["source_type"])
        target_id = ref_node(dependency["target"], dependency["target_type"])
        edge(source_id, target_id, "depends_on")

    return {"nodes": list(nodes.values()), "edges": edges}


# ============================================================================
# Entry points
# ============================================================================


def documentation_to_dict(documentation: Documentation) -> dict:
    """
    Convert the documentation to plain JSON-compatible data.

    Args:
        documentation: From documentation.build_documentation()

    Returns:
        Dict (see module docstring for the sections)
    """
    dependencies = _dependencies_section(documentation)
    return {
        "schema_version": SCHEMA_VERSION,
        "generator": f"pbixtractor {__version__}",
        "report": _report_section(documentation),
        "model": _model_section(documentation),
        "dependencies": dependencies,
        "dependencies_exact": documentation.exact_dependencies is not None,
        "unused": {
            "columns": [_ref(t, n) for t, n in documentation.unused_columns],
            "measures": [_ref(t, n) for t, n in documentation.unused_measures],
        },
        "quality": _quality_section(documentation),
        "lineage": _lineage_section(documentation, dependencies),
    }


def write_json(path: str, documentation: Documentation) -> None:
    """Write the documentation as UTF-8 JSON (indented, stable key order)."""
    write_json_data(path, documentation_to_dict(documentation))


def write_json_data(path: str, data: dict) -> None:
    """Write an already converted documentation dict (see documentation_to_dict)."""
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
        file.write("\n")
