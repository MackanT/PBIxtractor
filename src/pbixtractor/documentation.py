"""Analysis stage: combine the report, the model and Tabular Editor results into one object.

Everything the writers need is computed here; nothing in this module writes files.

    documentation = build_documentation(report_items, report_filters, model, dataset, ...)
    excel_report.write_main_workbook(path, documentation, ...)
"""

import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from functools import cached_property
from typing import Optional

import pandas as pd

from .constants import REPORT_COLUMNS
from .dax import find_columns, find_measures
from .extractors import FilterExtractor
from .live_model import LiveStatistics
from .readers import BookmarkDefinition, ReportDefinition, slicer_selection
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
    "Depends On",
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
class PageInfo:
    """Summary of one report page."""

    name: str  # unique page key: "<report> › <page>" when several reports are documented
    hidden: bool = False
    page_type: str = ""  # "", "Tooltip" or "Drillthrough"
    report: str = ""  # the report the page belongs to
    title: str = ""  # the page's own name, without the report prefix
    visuals: int = 0  # visuals and slicers
    buttons: int = 0
    broken_buttons: int = 0  # buttons pointing to a deleted bookmark or page
    page_filters: int = 0
    changed_interactions: int = 0
    sync_groups: list[str] = field(default_factory=list)


@dataclass
class BookmarkFilter:
    """A filter condition or slicer selection that a bookmark applies."""

    level: str  # "All Pages", "This Page", "Visual" or "Slicer"
    where: str  # "All pages", the page, or the visual's label
    field: str  # Table[Field]
    operator: str
    value: str
    # Differs from the report's saved state; None: nothing to compare with (the page or
    # visual no longer exists)
    changed: Optional[bool] = None

    @property
    def text(self) -> str:
        """One line, e.g. "Slicer (a1b2) on Sales, selection: Dates[Year] = 2024 (changed)"."""
        kind = {"Visual": ", visual filter", "Slicer": ", selection"}.get(self.level, "")
        note = {True: " (changed)", None: " (page/visual no longer exists)"}.get(self.changed, "")
        condition = f"{self.field} {self.operator} {self.value}".replace("  ", " ")
        return f"{self.where}{kind}: {condition}{note}"


@dataclass
class BookmarkInfo:
    """A bookmark, what it captures, and which buttons use it."""

    name: str  # id
    display_name: str
    group: str = ""
    page: str = ""  # page display name it navigates to (if it captures the page)
    captures: str = ""  # e.g. "Data, Display, Current page"
    applies_to: str = ""  # "All visuals" or "N selected visuals"
    hidden_visuals: list[str] = field(default_factory=list)  # labels of visuals it hides
    used_by: list[str] = field(default_factory=list)  # "Page (visual id)" of buttons using it
    broken: bool = False  # recorded on a page that no longer exists
    report: str = ""  # the report the bookmark belongs to
    # Filters/slicer selections it applies (empty when it does not capture "Data")
    filters: list[BookmarkFilter] = field(default_factory=list)


@dataclass
class Documentation:
    """Everything the documentation writers need."""

    report_name: str  # the documentation's name: the report's, or the model's for several
    report_info: pd.DataFrame  # REPORT_COLUMNS, one row per visual field/button/group
    # [page, item, filter type, "Table[Field]", "operator value", report]
    filter_strings: list[list]
    pages: dict[str, list[PageItem]]  # page name -> sorted items
    model: SemanticModel
    objects: pd.DataFrame  # OBJECT_COLUMNS
    relations: pd.DataFrame  # RELATION_COLUMNS
    unused_columns: list[tuple[str, str]]
    unused_measures: list[tuple[str, str]]
    exact_dependencies: Optional[list[Dependency]] = None
    bpa_violations: Optional[list[BpaViolation]] = None
    live_statistics: Optional[LiveStatistics] = None
    page_info: list[PageInfo] = field(default_factory=list)
    bookmarks: list[BookmarkInfo] = field(default_factory=list)
    # (page, visual id) -> notes such as "Sync group: Year", "No effect on Table (a1b2)"
    interactivity: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    # The documented reports, in order: name -> the file/folder it was read from
    reports: dict[str, str] = field(default_factory=dict)

    def interactivity_text(self, page: str, visual_id) -> str:
        """Interactivity notes of a visual as one multi-line string."""
        return "\n".join(self.interactivity.get((page, str(visual_id)), []))

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


_OBJECT_KINDS = {"C": "Column", "M": "Measure", "H": "Hierarchy", "P": "Table"}


def parse_tsv_object_name(
    object_name: str, table_names: Optional[list[str]] = None
) -> tuple[str, str, str]:
    """
    Parse TSV object name to extract type, table, and column.

    Args:
        object_name: Object name from TSV (e.g., "Model.T.Table.C.Column")
        table_names: The model's table names. Pass them whenever available: table names may
            contain dots ("dbo.Customer", "01. Sales"), which only a known-name match splits
            correctly - the dot-counting fallback would read "Model.T.dbo.Customer.C.Id" as
            table "dbo", column "C.Id".

    Returns:
        Tuple of (type, table, column) where type is Table/Column/Hierarchy/Measure
    """
    if table_names and object_name.startswith("Model.T."):
        rest = object_name[len("Model.T.") :]
        for table in sorted(table_names, key=len, reverse=True):  # "dbo.Customer" before "dbo"
            if rest == table:
                return ("Table", table, "")
            if rest.startswith(table + ".") and rest[len(table) + 2 : len(table) + 3] == ".":
                kind, name = rest[len(table) + 1], rest[len(table) + 3 :]
                if kind in _OBJECT_KINDS:
                    return (_OBJECT_KINDS[kind], table, "" if kind == "P" else name.strip("[]"))

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


def split_embedded_description(definition: str, tag: str) -> tuple[str, str]:
    """
    Take a description embedded in DAX as a comment: "<tag> description <tag>".

    The first tag pair counts, wherever it is ("VAR x =\\n //// text ////\\n ..." happens);
    only that comment is removed, the DAX around it is kept as written. A single tag
    ("x //// note"), or a pair with nothing between (a line of slashes as a separator), is
    not a description: the DAX is returned unchanged.

    Returns:
        (description or "", DAX without the embedded description)
    """
    start = definition.find(tag) if tag else -1
    end = definition.find(tag, start + len(tag)) if start != -1 else -1
    if end == -1 or not definition[start + len(tag) : end].strip():
        return "", definition
    description = definition[start + len(tag) : end].strip()
    return description, definition[:start] + definition[end + len(tag) :]


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


def is_missing_target(target: str) -> bool:
    """True for a button target the extractor marked as deleted, e.g. "(missing page: id)"."""
    return str(target).startswith("(missing ")


def is_broken_button(item: PageItem) -> bool:
    """A button whose bookmark or page no longer exists."""
    return item.item_type == "Button" and is_missing_target(
        button_target_and_label(item.first_row)[0]
    )


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
        filters: Rows [page, item, filter type, table, field, operator, value(, report)]

    Returns:
        (unique filters, display rows [page, item, filter type, "Table[Field]", "op value",
        report]). The same filter in two reports stays two rows.
    """
    unique = []
    for row in filters:
        if row not in unique:
            unique.append(row)
    strings = [
        [
            row[0],
            row[1],
            row[2],
            f"{row[3]}[{row[4]}]",
            " ".join(row[5:7]).strip(),
            row[7] if len(row) > 7 else "",
        ]
        for row in unique
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
    # Pages with visuals, plus pages that only have page-level filters (else those are lost)
    page_order = list(
        dict.fromkeys(
            report_info["Page"].unique().tolist()
            + [row[0] for row in filter_strings if row[2] == "This Page" and row[0]]
        )
    )
    for page in page_order:
        page_rows = report_info[report_info["Page"] == page]
        visual_ids = page_rows["Visual ID"].unique().tolist()
        sorted_rows = page_rows.sort_values(by=["Visual Type", "Type"])

        rows = []
        for visual in visual_ids:
            visual_type = sorted_rows[sorted_rows["Visual ID"] == visual].iloc[0]["Visual Type"]
            # Unknown visual types were already reported (once) by the extractor
            item_type, display_type = visual_mapper.get_visual_info(visual_type)
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
                # page_rows, not report_info: in model mode a copied report reuses visual ids
                item.first_row = page_rows[page_rows["Visual ID"] == row["ID"]].iloc[0]
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
        object_type, table, name_in_object = parse_tsv_object_name(line["Object"], table_names)
        if object_type in ("Table", "Hierarchy"):
            continue

        if not isinstance(line["Expression"], float):
            definition = line["Expression"].replace("    ", "\t").replace("\\n", "\n")
        else:
            definition = ""

        embedded, definition = split_embedded_description(definition, description_tag)
        if pd.isna(line["Description"]):
            description = embedded.replace("\\n", "\\r\\n")
        else:
            description = line["Description"]

        format_string = line.get("FormatString", "")
        display_folder = line.get("DisplayFolder", "")
        if object_type == "Column" and definition.strip():
            object_type = "Calculated Column"
        rows.append(
            {
                "Type": object_type,
                "Name": line["Name"],
                "DataType": line["DataType"],
                "Description": description,
                "Definition": definition.strip()
                .replace("\r\n", "\n")
                .replace("\r", "\n"),
                "Table": table,
                "Depends On": "",
                "Format": "" if pd.isna(format_string) else format_string,
                "Folder": "" if pd.isna(display_folder) else display_folder,
                "Comment": "",
                "Report File": report_name,
            }
        )
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
    table_names = [table.name for table in model.tables]
    for object_name in dataset["Object"]:
        object_type, table, name = parse_tsv_object_name(object_name, table_names)
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
        # Fallback: text matching of all DAX in the model - measures, calculated columns,
        # calculated tables (field parameters' NAMEOF), calculation items and RLS filters
        dax_texts = [row["Definition"] for _, row in objects.iterrows() if row["Type"] != "Column"]
        for table in model.tables:
            dax_texts += [p.expression for p in table.partitions if p.source_type == "calculated"]
            dax_texts += [item.expression for item in table.calculation_items]
        dax_texts += [dax for role in model.roles for dax in role.table_filters.values()]
        for dax in dax_texts:
            if not dax:
                continue
            columns = find_columns(dax)
            referenced_names = {measure[1:-1] for measure in find_measures(dax)}
            unused = [
                col for col in unused if col not in columns and col[1] not in referenced_names
            ]

    measure_keys = {(measure.table, measure.name) for measure in model.all_measures}
    return (
        [col for col in unused if col not in measure_keys],
        [col for col in unused if col in measure_keys],
    )


def _visual_labels(pages: dict[str, list[PageItem]]) -> dict[tuple[str, str], str]:
    """(page, visual id) -> readable label, e.g. "Table (a1b2c3) on Sales".

    Keyed by page as well: in model mode a copied report reuses the same visual ids.
    """
    labels = {}
    for page, items in pages.items():
        for item in items:
            if item.item_type != "Filter":
                labels[(page, str(item.id))] = f"{item.visual_type} ({item.id}) on {page}"
    return labels


# How a changed interaction is described on the source visual
_INTERACTION_TEXT = {"None": "No effect on", "Filter": "Filters", "Highlight": "Highlights"}


def build_interactivity(
    report: ReportDefinition, pages: dict[str, list[PageItem]]
) -> dict[tuple[str, str], list[str]]:
    """
    Interactivity notes per visual: hidden state, slicer sync group, changed interactions.

    Args:
        report: Report definition (readers.read_report)
        pages: Page items from build_page_items()

    Returns:
        (page, visual id) -> notes
    """
    labels = _visual_labels(pages)
    notes: dict[tuple[str, str], list[str]] = {}
    for page in report.pages:
        for visual in page.visuals:
            key = (page.display_name, visual.name)
            if visual.hidden:
                notes.setdefault(key, []).append("Hidden on page")
            if visual.sync_group:
                notes.setdefault(key, []).append(f"Sync group: {visual.sync_group}")
        for interaction in page.interactions:
            if interaction.kind == "Default":
                continue
            action = _INTERACTION_TEXT.get(interaction.kind, f"{interaction.kind}:")
            target = labels.get((page.display_name, interaction.target), interaction.target)
            target = target.removesuffix(f" on {page.display_name}")
            notes.setdefault((page.display_name, interaction.source), []).append(
                f"{action} {target}"
            )
    return notes


def build_page_info(
    report: ReportDefinition, pages: dict[str, list[PageItem]], filter_strings: list[list]
) -> list[PageInfo]:
    """One summary per report page, including pages without visuals."""
    info = []
    for page in report.pages:
        items = pages.get(page.display_name, [])
        info.append(
            PageInfo(
                name=page.display_name,
                hidden=page.hidden,
                page_type=page.page_type,
                report=page.report,
                title=page.title or page.display_name,
                visuals=sum(1 for i in items if i.item_type in ("Visual", "Slicer")),
                buttons=sum(1 for i in items if i.item_type == "Button"),
                broken_buttons=sum(1 for i in items if is_broken_button(i)),
                page_filters=sum(
                    1 for f in filter_strings if f[2] == "This Page" and f[0] == page.display_name
                ),
                changed_interactions=sum(1 for i in page.interactions if i.kind != "Default"),
                sync_groups=sorted({v.sync_group for v in page.visuals if v.sync_group}),
            )
        )
    return info


def build_bookmarks(
    report: ReportDefinition,
    report_info: pd.DataFrame,
    pages: dict[str, list[PageItem]],
    logger: Optional[logging.Logger] = None,
) -> list[BookmarkInfo]:
    """
    Bookmarks with their capture options, hidden visuals and the buttons that use them.

    Buttons are matched on the bookmark's display name (what the button rows hold). A bookmark
    recorded on a deleted page gets page "(missing page: <id>)" and broken=True.
    """
    labels = _visual_labels(pages)
    # Hidden visuals are looked up on the bookmark's own page first, by id alone as a fallback
    by_id: dict[str, str] = {}
    for (_, visual_id), label in labels.items():
        by_id.setdefault(visual_id, label)
    page_names = {page.name: page.display_name for page in report.pages}
    button_rows = report_info[report_info["Type"] == "Bookmark"]
    # Button rows only carry the bookmark's display name. When several bookmarks share one
    # (e.g. "Reset" on every page), a button belongs to the one recorded on its own page.
    shared_names = {
        name
        for name, count in Counter(b.display_name for b in report.bookmark_details).items()
        if count > 1
    }

    bookmarks = []
    for bookmark in report.bookmark_details:
        captures = [
            label
            for label, captured in (
                ("Data", bookmark.captures_data),
                ("Display", bookmark.captures_display),
                ("Current page", bookmark.captures_page),
            )
            if captured
        ]
        users = button_rows[button_rows["Name"] == bookmark.display_name]
        if bookmark.display_name in shared_names:
            users = users[users["Page"] == page_names.get(bookmark.page, "")]
        used_by = [f"{row['Page']} ({row['Visual ID']})" for _, row in users.iterrows()]
        broken = bool(bookmark.page) and bookmark.page not in page_names
        if broken:
            page = f"(missing page: {bookmark.page})"
            if logger:
                logger.warning(
                    f"Bookmark {bookmark.display_name} was recorded on a page that no longer "
                    f"exists: {bookmark.page}"
                )
        elif bookmark.captures_page:
            page = page_names[bookmark.page] if bookmark.page else ""
        else:
            page = ""
        bookmarks.append(
            BookmarkInfo(
                name=bookmark.name,
                display_name=bookmark.display_name,
                report=bookmark.report,
                group=bookmark.group,
                page=page,
                broken=broken,
                captures=", ".join(captures),
                applies_to=(
                    f"{len(bookmark.target_visuals)} selected visuals"
                    if bookmark.target_visuals
                    else "All visuals"
                ),
                hidden_visuals=[
                    labels.get((page_names.get(bookmark.page, ""), v)) or by_id.get(v, v)
                    for v in bookmark.hidden_visuals
                ],
                used_by=list(dict.fromkeys(used_by)),
                filters=(
                    _bookmark_filters(bookmark, report, labels, by_id)
                    if bookmark.captures_data
                    else []
                ),
            )
        )
    return bookmarks


def _bookmark_filters(
    bookmark: BookmarkDefinition,
    report: ReportDefinition,
    labels: dict[tuple[str, str], str],
    by_id: dict[str, str],
) -> list[BookmarkFilter]:
    """
    The filter/slicer state a bookmark applies, each compared with the report's saved state.

    With "Selected visuals" only the state of those visuals is applied, so other visuals' state
    is left out; filter-pane state of the page and report is kept.
    """
    describer = FilterExtractor(config=None)
    pages = {page.name: page for page in report.pages}

    def conditions(entries: list[dict]) -> dict[str, tuple[str, str]]:
        """Table[Field] -> (operator, value) of filter-pane entries."""
        return {
            f"{f.table_name}[{f.val_name}]": (f.operator, f.value)
            for f in describer.extract_filters(entries, "", "")
        }

    result = []
    for captured in bookmark.filters:
        page = pages.get(captured.page)
        page_name = page.display_name if page else captured.page
        visual = None
        if captured.visual:
            if bookmark.target_visuals and captured.visual not in bookmark.target_visuals:
                continue
            visual = next((v for v in page.visuals if v.name == captured.visual), None) if page else None
            where = labels.get((page_name, captured.visual)) or by_id.get(
                captured.visual, f"{captured.visual} on {page_name}"
            )
        else:
            where = page_name if captured.level == "This Page" else "All pages"

        if captured.level == "All Pages":
            saved = report.filters
        elif captured.level == "This Page":
            saved = page.filters if page else None
        elif visual is None:
            saved = None
        elif captured.level == "Slicer":
            selection = slicer_selection(visual.objects)
            saved = [selection] if selection else []
        else:
            saved = visual.filters
        saved_conditions = conditions(saved) if saved is not None else None

        for field_name, (operator, value) in conditions([captured.filter]).items():
            result.append(
                BookmarkFilter(
                    level=captured.level,
                    where=where,
                    field=field_name,
                    operator=operator,
                    value=value,
                    changed=(
                        None
                        if saved_conditions is None
                        else saved_conditions.get(field_name) != (operator, value)
                    ),
                )
            )
    return result


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
    report: Optional[ReportDefinition] = None,
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
        report: Report definition for page info, bookmarks and interactivity (optional)

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
    pages = build_page_items(report_info, filter_strings, visual_mapper, visual_types, logger)
    return Documentation(
        report_name=report_name,
        report_info=report_info,
        filter_strings=filter_strings,
        pages=pages,
        model=model,
        objects=objects,
        relations=build_relations(model),
        unused_columns=unused_columns,
        unused_measures=unused_measures,
        exact_dependencies=exact_dependencies,
        bpa_violations=bpa_violations,
        live_statistics=live_statistics,
        page_info=build_page_info(report, pages, filter_strings) if report else [],
        bookmarks=build_bookmarks(report, report_info, pages, logger) if report else [],
        interactivity=build_interactivity(report, pages) if report else {},
        reports=(report.reports if report and report.reports else {report_name: ""}),
    )
