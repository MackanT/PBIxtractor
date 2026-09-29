"""Analysis stage: combine the report, the model and Tabular Editor results into one object.

Everything the writers need is computed here; nothing in this module writes files.

    documentation = build_documentation(report_items, report_filters, model, dataset, ...)
    excel_report.write_main_workbook(path, documentation, ...)
"""

import logging
import re
from dataclasses import dataclass, field
from functools import cached_property
from typing import Optional

import pandas as pd

from .constants import REPORT_COLUMNS
from .dax import find_columns, find_measures
from .live_model import LiveStatistics
from .semantic_model import SemanticModel
from .tabular_editor import BpaViolation, Dependency
from .utils import find_nth_occurrence
from .visual_helpers import VisualTypeMapper

# Columns of the "common" sheets (one row per column/measure of the model)
OBJECT_COLUMNS = [
    "Type",
    "Name",
    "DataType",
    "Description",
    "Definition",
    "Table",
    "Dependants",
    "Format",
    "Folder",
    "Comment",
    "Report File",
]

RELATION_COLUMNS = [
    "Type",
    "Child",
    "Direction",
    "Parent",
    "Child Column",
    "Parent Column",
    "Cardinality",
    "Active",
]

# Order of items on the page sheets
ITEM_TYPE_ORDER = ["Visual", "Slicer", "Filter", "Button", "Group"]

# Columns of the intermediate per-page table (kept for identical sorting behaviour)
_PAGE_ITEM_COLUMNS = [
    "Item Type",
    "Visual Type",
    "Type",
    "Field",
    "DisplayName",
    "Visual Filters",
    "Interactivity",
    "Comment",
    "ID",
]


# ============================================================================
# Result objects
# ============================================================================


@dataclass
class PageItem:
    """One row on a page sheet: a visual/slicer, button/group or page filter."""

    item_type: str  # Visual | Slicer | Filter | Button | Group
    visual_type: str  # display name, e.g. "Clustered Column Chart", or "This Page"
    id: object  # visual id, or index into Documentation.filter_strings for page filters
    fields: Optional[pd.DataFrame] = None  # Visual/Slicer: its REPORT_COLUMNS rows
    first_row: Optional[pd.Series] = None  # Button/Group: its report row
    visual_filters: list[tuple[str, str]] = field(default_factory=list)  # (field, condition)
    filter_field: str = ""  # Filter: "Table[Field]"
    filter_condition: str = ""  # Filter: "operator value"


@dataclass
class Documentation:
    """Everything the documentation writers need."""

    report_name: str
    report_info: pd.DataFrame  # REPORT_COLUMNS, one row per visual field/button/group
    filter_strings: list[list]  # [page, item, filter type, "Table[Field]", "operator value"]
    pages: dict[str, list[PageItem]]  # page name -> sorted items
    model: SemanticModel
    objects: pd.DataFrame  # OBJECT_COLUMNS
    relations: pd.DataFrame  # RELATION_COLUMNS
    unused_columns: list[tuple[str, str]]
    unused_measures: list[tuple[str, str]]
    exact_dependencies: Optional[list[Dependency]] = None
    bpa_violations: Optional[list[BpaViolation]] = None
    live_statistics: Optional[LiveStatistics] = None

    @cached_property
    def _dependencies_by_source(self) -> dict[tuple[str, str], list[str]]:
        grouped = {}
        for d in self.exact_dependencies or []:
            grouped.setdefault((d.source_table, d.source_name), []).append(d.target_ref)
        return grouped

    def depends_on(self, table: str, name: str) -> Optional[list[str]]:
        """Exact dependencies of an object as "Table[Col]" refs; None if not available."""
        if self.exact_dependencies is None:
            return None
        return self._dependencies_by_source.get((table, name), [])


# ============================================================================
# Helpers
# ============================================================================


def parse_tsv_object_name(object_name: str) -> tuple[str, str, str]:
    """
    Parse TSV object name to extract type, table, and column.

    Args:
        object_name: Object name from TSV (e.g., "Model.T.Table.C.Column")

    Returns:
        Tuple of (type, table, column) where type is Table/Column/Hierarchy/Measure
    """
    data_type = "Table"
    start_pos = find_nth_occurrence(".", object_name, 2) + 1
    end_pos = find_nth_occurrence(".", object_name, 3)

    if end_pos == -1:
        table = object_name[start_pos:]
    else:
        table = object_name[start_pos:end_pos]

    column = ""
    if any(substring in object_name for substring in [".C.", ".H.", ".M."]):
        start_pos = find_nth_occurrence(".", object_name, 4) + 1
        end_pos = find_nth_occurrence(".", object_name, 5)

        if end_pos == -1 or end_pos < len(object_name):
            column = object_name[start_pos:]
        else:
            column = object_name[start_pos:end_pos]

        column = column.strip("[]")

        if ".C." in object_name:
            data_type = "Column"
        elif ".H." in object_name:
            data_type = "Hierarchy"
        elif ".M." in object_name:
            data_type = "Measure"

    return (data_type, table, column)


def button_target_and_label(row: pd.Series) -> tuple[str, str]:
    """
    Get the target and label of a button or group row from the report DataFrame.

    Args:
        row: Report row (REPORT_COLUMNS) of a button or group

    Returns:
        (target, label). Buttons: (bookmark/page name, button text).
        Groups: (group name, "").
    """
    display_name = row["Display Name"]
    display_name = "" if pd.isna(display_name) or not display_name else str(display_name)

    if row["Type"] == "Group":
        return display_name, ""
    return row["Name"] or "", display_name


def field_display_name(row: pd.Series) -> str:
    """Display name of a visual field, falling back to its name."""
    display_name = row["Display Name"]
    return str(display_name) if not pd.isna(display_name) and display_name else row["Name"]


def field_description(fields: pd.DataFrame) -> str:
    """
    Describe a visual's fields grouped by role, e.g. "Values:\\n  Sales[Amount] (Total)".

    Args:
        fields: The visual's REPORT_COLUMNS rows

    Returns:
        Multi-line description
    """
    parts = []
    for field_type, group in fields.groupby("Type", sort=False):
        parts.append(f"{field_type}:")
        for _, row in group.iterrows():
            display_name = field_display_name(row)
            if display_name != row["Name"]:
                parts.append(f"  {row['Table']}[{row['Name']}] ({display_name})")
            else:
                parts.append(f"  {row['Table']}[{row['Name']}]")
    return "\n".join(parts)


# ============================================================================
# Building blocks
# ============================================================================


def unique_filters(filters: list[list]) -> tuple[list[list], list[list]]:
    """
    Remove duplicate filters and format them for display.

    Args:
        filters: Rows [page, item, filter type, table, field, operator, value]

    Returns:
        (unique filters, display rows [page, item, filter type, "Table[Field]", "op value"])
    """
    unique = []
    for row in filters:
        if row not in unique:
            unique.append(row)
    strings = [
        [row[0], row[1], row[2], f"{row[3]}[{row[4]}]", " ".join(row[5:]).strip()] for row in unique
    ]
    return unique, strings


def build_page_items(
    report_info: pd.DataFrame,
    filter_strings: list[list],
    visual_mapper: VisualTypeMapper,
    visual_types: list[str],
    logger: logging.Logger,
) -> dict[str, list[PageItem]]:
    """
    Sort each page's visuals, page filters, buttons and groups for the page sheets.

    Args:
        report_info: REPORT_COLUMNS rows from ReportExtractor
        filter_strings: Display rows from unique_filters()
        visual_mapper: Maps visual types to (item type, display name)
        visual_types: Supported visual types from data.yaml
        logger: Logger for unsupported visual types

    Returns:
        Page name -> items in display order
    """
    pages = {}
    for page in report_info["Page"].unique().tolist():
        page_rows = report_info[report_info["Page"] == page]
        visual_ids = page_rows["Visual ID"].unique().tolist()
        sorted_rows = page_rows.sort_values(by=["Visual Type", "Type"])

        rows = []
        for visual in visual_ids:
            visual_type = sorted_rows[sorted_rows["Visual ID"] == visual].iloc[0]["Visual Type"]
            item_type, display_type = visual_mapper.get_visual_info(visual_type)

            if (
                not visual_mapper.is_special_visual(visual_type)
                and visual_type not in visual_types
                and visual_type not in ["Group"]
                and not visual_mapper.is_button_type(visual_type)
            ):
                logger.warning(f"New Visual type not yet supported: {visual_type}")

            rows.append({"Item Type": item_type, "Visual Type": display_type, "ID": visual})

        for index, filter_row in enumerate(filter_strings):
            if filter_row[2] == "This Page" and filter_row[0] == page:
                rows.append({"Item Type": "Filter", "Visual Type": "This Page", "ID": index})

        table = pd.DataFrame(rows, columns=_PAGE_ITEM_COLUMNS)
        table["Item Type"] = pd.Categorical(
            table["Item Type"], categories=ITEM_TYPE_ORDER, ordered=True
        )
        table = table.sort_values(by=["Item Type", "Visual Type"])

        items = []
        for _, row in table.iterrows():
            if isinstance(row["Item Type"], float):  # not in ITEM_TYPE_ORDER
                logger.error(f"NaN Item Type Encountered: {row}")
                continue

            item = PageItem(
                item_type=row["Item Type"], visual_type=row["Visual Type"], id=row["ID"]
            )
            item.visual_filters = [
                (f[3], f[4])
                for f in filter_strings
                if f[2] == "Visual" and f[0] == page and f[1] == row["ID"]
            ]
            if item.item_type in ("Visual", "Slicer"):
                item.fields = sorted_rows[sorted_rows["Visual ID"] == row["ID"]]
            elif item.item_type in ("Button", "Group"):
                item.first_row = report_info[report_info["Visual ID"] == row["ID"]].iloc[0]
            elif item.item_type == "Filter":
                item.filter_field, item.filter_condition = filter_strings[row["ID"]][3:5]
            items.append(item)
        pages[page] = items
    return pages


def build_objects(
    dataset: pd.DataFrame,
    table_names: list[str],
    report_name: str,
    description_tag: str,
) -> pd.DataFrame:
    """
    Build the common-sheet table (one row per column/measure) from the model dataset.

    Args:
        dataset: Tabular Editor style table (semantic_model.model_to_dataset or the TSV)
        table_names: Model table names (quotes around them are removed from DAX)
        report_name: Report name for the "Report File" column
        description_tag: Delimiter of descriptions embedded in DAX (e.g. "////")

    Returns:
        DataFrame with OBJECT_COLUMNS
    """
    dataset = dataset.copy()

    # Remove excess " ' " surrounding table names
    escape_pattern = r"'(?:\s*)(" + "|".join(map(re.escape, table_names)) + r")(?:\s*)'"
    for i, row in enumerate(dataset.iloc()):
        expression = row["Expression"]
        if pd.isna(expression):
            continue
        expression = expression.replace("\\t", "    ")
        match = re.search(escape_pattern, expression)
        if match:
            dataset.at[i, "Expression"] = expression.replace(match.group(0), match.group(1))

    rows = []
    for i in range(len(dataset)):
        line = dataset.iloc[i]
        object_type, table, name_in_object = parse_tsv_object_name(line["Object"])
        if object_type in ("Table", "Hierarchy"):
            continue

        if not isinstance(line["Expression"], float):
            definition = line["Expression"].replace("    ", "\t").replace("\\n", "\n")
        else:
            definition = ""

        # Extract description if embedded in definition
        if definition.find(description_tag) != -1:
            comment_start = find_nth_occurrence(description_tag, definition, 1) + 5
            comment_end = find_nth_occurrence(description_tag, definition, 2) - 1
            definition_start = comment_end + 6
        else:
            comment_start = comment_end = definition_start = 0

        if pd.isna(line["Description"]):
            description = definition[comment_start:comment_end].strip().replace("\\n", "\\r\\n")
        else:
            description = line["Description"]

        format_string = line.get("FormatString", "")
        display_folder = line.get("DisplayFolder", "")
        rows.append(
            {
                "Type": object_type,
                "Name": line["Name"],
                "DataType": line["DataType"],
                "Description": description,
                "Definition": definition[definition_start:]
                .strip()
                .replace("\r\n", "\n")
                .replace("\r", "\n"),
                "Table": table,
                "Dependants": "",
                "Format": "" if pd.isna(format_string) else format_string,
                "Folder": "" if pd.isna(display_folder) else display_folder,
                "Comment": "",
                "Report File": report_name,
            }
        )
    # The original implementation prepended rows, so the sheets list objects in reverse
    # model order; kept to avoid reshuffling existing documentation
    rows.reverse()
    return pd.DataFrame(rows, columns=OBJECT_COLUMNS)


def build_relations(model: SemanticModel) -> pd.DataFrame:
    """Relationships sheet rows. Child = "from" (usually many) side, Parent = "to" side."""
    return pd.DataFrame(
        [
            {
                "Type": "Relationship",
                "Child": rel.from_table,
                "Direction": rel.direction_label,
                "Parent": rel.to_table,
                "Child Column": rel.from_column,
                "Parent Column": rel.to_column,
                "Cardinality": rel.cardinality_label,
                "Active": "Yes" if rel.is_active else "No",
            }
            for rel in sorted(model.relationships, key=lambda r: r.te_name)
        ],
        columns=RELATION_COLUMNS,
    )


def find_unused(
    dataset: pd.DataFrame,
    objects: pd.DataFrame,
    model: SemanticModel,
    report_info: pd.DataFrame,
    report_filters: list[list],
    exact_dependencies: Optional[list[Dependency]],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """
    Columns and measures not used by the model, any visual, filter or DAX expression.

    Args:
        dataset: Tabular Editor style table (candidates: its columns and measures)
        objects: Common-sheet table (DAX definitions for the text-matching fallback)
        model: Semantic model (structurally used columns, measure list)
        report_info: Visual fields
        report_filters: Unique filter rows
        exact_dependencies: Tabular Editor dependencies, or None to use text matching

    Returns:
        (unused columns, unused measures) as (table, name) tuples
    """
    unused = []
    for object_name in dataset["Object"]:
        object_type, table, name = parse_tsv_object_name(object_name)
        if object_type in ("Column", "Measure"):
            unused.append((table, name))

    # Relationship keys, sort-by and hierarchy level columns are used by the model itself
    structurally_used = model.structurally_used_columns()
    unused = [col for col in unused if col not in structurally_used]

    if exact_dependencies is not None:
        # Objects referenced by any DAX (measures, calculated columns/tables, RLS)
        referenced = {
            (d.target_table, d.target_name)
            for d in exact_dependencies
            if d.target_type in ("Column", "Measure")
        }
        unused = [col for col in unused if col not in referenced]

    used_in_report = {(row["Table"], row["Name"]) for _, row in report_info.iterrows()}
    used_in_report |= {(row[3], row[4]) for row in report_filters}
    unused = [col for col in unused if col not in used_in_report]

    if exact_dependencies is None:
        # Fallback: text matching of the DAX of measures (calculated columns are not scanned)
        for _, row in objects.iterrows():
            if row["Type"] == "Column":
                continue
            columns = find_columns(row["Definition"])
            referenced_names = {measure[1:-1] for measure in find_measures(row["Definition"])}
            unused = [
                col for col in unused if col not in columns and col[1] not in referenced_names
            ]

    measure_keys = {(measure.table, measure.name) for measure in model.all_measures}
    return (
        [col for col in unused if col not in measure_keys],
        [col for col in unused if col in measure_keys],
    )


def resolve_hierarchy_columns(report_info: pd.DataFrame, model: SemanticModel) -> pd.DataFrame:
    """
    Replace hierarchy level names with the model column behind each level.

    The report only knows the level (Display Name "Date Hierarchy: Year"); its queryRef is not
    reliable either (sometimes the column, sometimes the level name). The model has the exact
    level -> column mapping (e.g. level "Year" -> column "Year Number").

    Args:
        report_info: REPORT_COLUMNS rows; Hierarchy rows have Display Name "<hierarchy>: <level>"
        model: Semantic model

    Returns:
        report_info with the Name of resolvable hierarchy rows set to the level's column
    """
    levels = {
        (hierarchy.table, hierarchy.name, level.name): level.column
        for hierarchy in model.all_hierarchies
        for level in hierarchy.levels
    }
    for index, row in report_info.iterrows():
        display_name = row["Display Name"]
        if row["Type"] != "Hierarchy" or not isinstance(display_name, str):
            continue
        hierarchy, _, level = display_name.partition(": ")
        column = levels.get((row["Table"], hierarchy, level))
        if column:
            report_info.at[index, "Name"] = column
    return report_info


def build_documentation(
    report_items: list[list],
    report_filters: list[list],
    model: SemanticModel,
    dataset: pd.DataFrame,
    report_name: str,
    description_tag: str,
    visual_mapper: VisualTypeMapper,
    visual_types: list[str],
    logger: logging.Logger,
    exact_dependencies: Optional[list[Dependency]] = None,
    bpa_violations: Optional[list[BpaViolation]] = None,
    live_statistics: Optional[LiveStatistics] = None,
) -> Documentation:
    """
    Combine report extraction, model and analysis results.

    Args:
        report_items: ReportExtractor.result rows (REPORT_COLUMNS)
        report_filters: ReportExtractor.filters rows
        model: Semantic model
        dataset: Tabular Editor style table of model objects
        report_name: Report name (file name without extension)
        description_tag: Delimiter of descriptions embedded in DAX
        visual_mapper: Visual type display names
        visual_types: Supported visual types
        logger: Logger
        exact_dependencies: Tabular Editor dependencies (None: text matching)
        bpa_violations: Best Practice Analyzer findings (None: not run)
        live_statistics: Statistics from Power BI Desktop (None: not available)

    Returns:
        Documentation
    """
    report_info = resolve_hierarchy_columns(
        pd.DataFrame(report_items, columns=REPORT_COLUMNS), model
    )
    unique, filter_strings = unique_filters(report_filters)
    objects = build_objects(dataset, [t.name for t in model.tables], report_name, description_tag)
    unused_columns, unused_measures = find_unused(
        dataset, objects, model, report_info, unique, exact_dependencies
    )
    return Documentation(
        report_name=report_name,
        report_info=report_info,
        filter_strings=filter_strings,
        pages=build_page_items(report_info, filter_strings, visual_mapper, visual_types, logger),
        model=model,
        objects=objects,
        relations=build_relations(model),
        unused_columns=unused_columns,
        unused_measures=unused_measures,
        exact_dependencies=exact_dependencies,
        bpa_violations=bpa_violations,
        live_statistics=live_statistics,
    )
