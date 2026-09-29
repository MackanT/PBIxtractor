"""Excel output: the main documentation workbook and the flat "_data" workbook.

write_main_workbook("Report.xlsx", documentation, graph_path, known_functions)
write_data_workbook("Report_data.xlsx", documentation, graph_path, known_functions)
"""

import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell

from .constants import DEFAULT_COLORS
from .dax import find_columns, find_measures, highlight_dax, text_dependencies
from .documentation import (
    OBJECT_COLUMNS,
    Documentation,
    PageItem,
    button_target_and_label,
    field_description,
    field_display_name,
)
from .model_sheets import add_bpa_sheets, add_model_sheets, write_table
from .utils import excel_sheet_name, rgba_tuple_to_hex, write_to_excel

# Colours for nested brackets in DAX (cycled by nesting depth)
PARENTHESIS_COLORS = ["#0433fa", "#319331", "#7b3831"]

# Everything we write is text: without this, values such as the filter condition "= Grey"
# would be written as (broken) Excel formulas
WORKBOOK_OPTIONS = {"strings_to_formulas": False}

DEFINITION_INDEX = OBJECT_COLUMNS.index("Definition")
DEPENDS_ON_INDEX = OBJECT_COLUMNS.index("Depends On")

PAGE_SHEET_HEADERS = [
    "Item Type",
    "Visual Type",
    "ID",
    "Description",
    "Visual Filters",
    "Interactivity",
    "Comment",
]
PAGES_SHEET_HEADERS = [
    "Page",
    "Item Type",
    "Visual Type",
    "ID",
    "Type",
    "Field",
    "DisplayName",
    "Visual Filters",
    "Interactivity",
    "Comment",
]


def create_formats(workbook: xlsxwriter.Workbook) -> dict:
    """
    Cell and rich-text formats. DAX colours come from constants.DEFAULT_COLORS.
    """

    def color(index: int):
        return workbook.add_format({"color": rgba_tuple_to_hex(DEFAULT_COLORS[index][1])})

    return {
        "top_wrap": workbook.add_format({"align": "top", "text_wrap": True}),
        "wrap": workbook.add_format({"text_wrap": True}),
        "function": color(0),
        "measure": color(1),
        "return": color(2),
        "varname": color(3),
        "comment": color(4),
        "quote": color(5),
        "var": color(6),
        "bold": workbook.add_format({"bold": True}),
        "italic": workbook.add_format({"italic": True}),
        "bi": workbook.add_format({"bold": True, "italic": True}),
        "para": [workbook.add_format({"color": c}) for c in PARENTHESIS_COLORS * 5],
    }


# ============================================================================
# Shared sheet sections
# ============================================================================


def _filters_rich_text(item: PageItem, formats: dict) -> list:
    """Visual filters as rich text: bold field, then the condition, one per line."""
    segments = []
    for i, (field, condition) in enumerate(item.visual_filters):
        newline = "\n" if i < len(item.visual_filters) - 1 else ""
        segments.extend([formats["bold"], field, " " + condition + newline])
    return segments


def _write_relations(worksheet, documentation: Documentation, formats: dict, graph_path) -> int:
    """
    Write the relationship table (and graph image) from row 0.

    Returns:
        Next free row
    """
    relations = documentation.relations
    row_num = 1
    if len(relations) == 0:
        return row_num

    for col, name in enumerate(relations.columns):
        worksheet.write(0, col, name, formats["bi"])
    # Graph to the right of the relationship columns
    worksheet.insert_image(
        xl_rowcol_to_cell(0, len(relations.columns) + 1), graph_path, {"x_scale": 1, "y_scale": 1}
    )
    for _, row in relations.iterrows():
        for col, value in enumerate(row):
            worksheet.write(row_num, col, value)
        row_num += 1
    return row_num


def _set_object_columns(worksheet, formats: dict) -> None:
    worksheet.set_column(0, len(OBJECT_COLUMNS), 30, formats["wrap"])
    worksheet.set_column(DEFINITION_INDEX, DEFINITION_INDEX, 100, formats["top_wrap"])
    worksheet.set_column(DEFINITION_INDEX + 1, DEFINITION_INDEX + 1, 30, formats["wrap"])
    worksheet.set_column(DEPENDS_ON_INDEX, DEPENDS_ON_INDEX, 50, formats["wrap"])


def _write_objects(
    worksheet, documentation: Documentation, formats: dict, row_num: int, known_functions
) -> int:
    """
    Write measures and calculated columns with highlighted DAX and dependencies.

    Returns:
        Next free row
    """
    for _, row in documentation.objects.iterrows():
        if row["Type"] == "Column":  # data columns: see the "model columns" sheet
            continue

        definition = highlight_dax(row["Definition"], formats, known_functions)
        parents = documentation.depends_on(row["Table"], row["Name"])
        if parents is None:
            parents = text_dependencies(row["Definition"])
        parents_rich = []
        for parent in parents:
            parents_rich.extend([parent, "\n"])
        parents_rich = parents_rich[:-1]

        for col, value in enumerate(row):
            if col == DEFINITION_INDEX and definition:
                write_to_excel(worksheet, row_num, col, definition)
            elif col == DEPENDS_ON_INDEX and parents_rich:
                write_to_excel(worksheet, row_num, col, parents_rich)
            elif value != "":
                worksheet.write(row_num, col, value)
        row_num += 1
    return row_num


def _write_page_sheet(
    worksheet, page: str, items: list[PageItem], documentation: Documentation, formats: dict
) -> None:
    """One sheet per report page: one row per visual, button, group or page filter."""
    worksheet.set_column(0, 6, 30, formats["top_wrap"])
    worksheet.set_column(2, 2, 50, formats["top_wrap"])
    worksheet.set_column(3, 3, 60, formats["top_wrap"])
    worksheet.set_column(4, 4, 60, formats["top_wrap"])
    for col, name in enumerate(PAGE_SHEET_HEADERS):
        worksheet.write(0, col, name, formats["bi"])
    if not items:
        worksheet.write(1, 0, "(no visuals, buttons or page filters on this page)", formats["italic"])

    for row_num, item in enumerate(items, start=1):
        worksheet.write(row_num, 0, item.item_type)
        worksheet.write(row_num, 1, item.visual_type)

        if item.item_type == "Filter":
            worksheet.write(row_num, 2, "")
            worksheet.write(row_num, 3, f"{item.filter_field} {item.filter_condition}")
            continue

        worksheet.write(row_num, 2, item.id)
        if item.item_type in ("Visual", "Slicer"):
            description = field_description(item.fields)
        else:
            target, label = button_target_and_label(item.first_row)
            if item.item_type == "Button":
                action = item.first_row["Type"]
                description = f"{action}: {target}" if target else action
                if label:
                    description += f" ({label})"
            else:
                description = target
        worksheet.write(row_num, 3, description)
        if item.visual_filters:
            write_to_excel(worksheet, row_num, 4, _filters_rich_text(item, formats))
        interactivity = documentation.interactivity_text(page, item.id)
        if interactivity:
            worksheet.write(row_num, 5, interactivity)


def _write_pages_sheet(
    worksheet, documentation: Documentation, formats: dict, with_description: bool
) -> None:
    """All pages on one sheet: one row per visual field, button, group or page filter."""
    worksheet.set_column(0, 9, 30, formats["top_wrap"])
    worksheet.set_column(3, 3, 50, formats["top_wrap"])
    worksheet.set_column(4, 4, 20, formats["top_wrap"])
    worksheet.set_column(5, 5, 50, formats["top_wrap"])
    worksheet.set_column(6, 6, 30, formats["top_wrap"])
    worksheet.set_column(7, 7, 60, formats["top_wrap"])
    headers = list(PAGES_SHEET_HEADERS)
    if with_description:
        worksheet.set_column(8, 8, 60, formats["top_wrap"])
        headers.append("Description")
    for col, name in enumerate(headers):
        worksheet.write(0, col, name, formats["bi"])

    row_num = 1
    for page, items in documentation.pages.items():
        for item in items:
            filters = _filters_rich_text(item, formats)
            interactivity = documentation.interactivity_text(page, item.id)

            if item.item_type in ("Visual", "Slicer"):
                description = field_description(item.fields) if with_description else None
                for _, field_row in item.fields.iterrows():
                    worksheet.write(row_num, 0, page)
                    worksheet.write(row_num, 1, item.item_type)
                    worksheet.write(row_num, 2, item.visual_type)
                    worksheet.write(row_num, 3, item.id)
                    worksheet.write(row_num, 4, field_row["Type"])
                    worksheet.write(row_num, 5, f"{field_row['Table']}[{field_row['Name']}]")
                    worksheet.write(row_num, 6, field_display_name(field_row))
                    if filters:
                        write_to_excel(worksheet, row_num, 7, filters)
                    if interactivity:
                        worksheet.write(row_num, 8, interactivity)
                    if with_description:
                        worksheet.write(row_num, 10, description)
                    row_num += 1

            elif item.item_type in ("Button", "Group"):
                target, label = button_target_and_label(item.first_row)
                worksheet.write(row_num, 0, page)
                worksheet.write(row_num, 1, item.item_type)
                worksheet.write(row_num, 2, item.visual_type)
                worksheet.write(row_num, 3, item.id)
                worksheet.write(row_num, 4, item.first_row["Type"])
                worksheet.write(row_num, 5, target)
                worksheet.write(row_num, 6, label)
                if filters:
                    write_to_excel(worksheet, row_num, 7, filters)
                if interactivity:
                    worksheet.write(row_num, 8, interactivity)
                row_num += 1

            else:  # page filter
                worksheet.write(row_num, 0, page)
                worksheet.write(row_num, 1, item.item_type)
                worksheet.write(row_num, 2, item.visual_type)
                worksheet.write(row_num, 3, "")
                worksheet.write(row_num, 5, item.filter_field)
                worksheet.write(row_num, 6, item.filter_condition)
                row_num += 1


def add_report_sheets(
    workbook: xlsxwriter.Workbook,
    documentation: Documentation,
    formats: dict,
    sheet_name=lambda name: name,
) -> None:
    """
    Report-level sheets: "report pages" (hidden/tooltip/drillthrough pages and counts),
    "bookmarks" (captures, hidden visuals, buttons using them) and "report filters" (every
    filter on every level, including report-level filters).
    """
    if documentation.page_info:
        write_table(
            workbook.add_worksheet(sheet_name("report pages")),
            formats["bi"],
            [
                ("Page", 30),
                ("Hidden", 8),
                ("Page Type", 13),
                ("Visuals", 9),
                ("Buttons", 9),
                ("Broken Buttons", 9),
                ("Page Filters", 11),
                ("Changed Interactions", 12),
                ("Sync Groups", 50),
            ],
            [
                [
                    p.name,
                    "Yes" if p.hidden else "",
                    p.page_type,
                    p.visuals,
                    p.buttons,
                    p.broken_buttons or "",
                    p.page_filters,
                    p.changed_interactions,
                    ", ".join(p.sync_groups),
                ]
                for p in documentation.page_info
            ],
        )

    if documentation.bookmarks:
        write_table(
            workbook.add_worksheet(sheet_name("bookmarks")),
            formats["bi"],
            [
                ("Bookmark", 40),
                ("Group", 20),
                ("Page", 25),
                ("Captures", 28),
                ("Applies To", 20),
                ("Hides", 60, formats["wrap"]),
                ("Used By Buttons", 50, formats["wrap"]),
                ("ID", 30),
            ],
            [
                [
                    b.display_name,
                    b.group,
                    b.page,
                    b.captures,
                    b.applies_to,
                    "\n".join(b.hidden_visuals),
                    "\n".join(b.used_by) if b.used_by else "(not used by any button)",
                    b.name,
                ]
                for b in documentation.bookmarks
            ],
        )

    if documentation.filter_strings:
        write_table(
            workbook.add_worksheet(sheet_name("report filters")),
            formats["bi"],
            [
                ("Level", 12),
                ("Page", 25),
                ("Visual / Filter", 30),
                ("Field", 40),
                ("Condition", 60),
            ],
            [
                [level, page or "(all pages)", item, field, condition]
                for page, item, level, field, condition in documentation.filter_strings
            ],
        )


# ============================================================================
# Workbooks
# ============================================================================


def write_main_workbook(
    path: str, documentation: Documentation, graph_path: str, known_functions: list
) -> None:
    """
    The documentation workbook: "<report> Common" (relationships, measures with DAX, unused
    objects), one sheet per page, "Pages", and the model/BPA sheets.

    Args:
        path: .xlsx file to write
        documentation: From documentation.build_documentation()
        graph_path: Relationship PNG to embed
        known_functions: DAX function names to highlight
    """
    workbook = xlsxwriter.Workbook(path, WORKBOOK_OPTIONS)
    used_names = set()

    def sheet_name(name: str) -> str:
        return excel_sheet_name(name, used_names)

    formats = create_formats(workbook)

    worksheet = workbook.add_worksheet(sheet_name(f"{documentation.report_name} Common"))
    _set_object_columns(worksheet, formats)
    row_num = _write_relations(worksheet, documentation, formats, graph_path) + 2
    for col, name in enumerate(OBJECT_COLUMNS):
        worksheet.write(row_num, col, name, formats["bi"])
    row_num = _write_objects(worksheet, documentation, formats, row_num + 1, known_functions)

    row_num += 6
    for title, objects in (
        ("Unused Columns", documentation.unused_columns),
        ("Measures not used in this report", documentation.unused_measures),
    ):
        worksheet.write(row_num, 0, f"{title} ({len(objects)})", formats["bi"])
        row_num += 1
        for table_name, field_name in objects:
            worksheet.write(row_num, 0, f"{table_name}[{field_name}]")
            row_num += 1
        row_num += 1

    # Every report page gets a sheet, also pages without visuals
    page_names = [p.name for p in documentation.page_info]
    page_names += [page for page in documentation.pages if page not in page_names]
    for page in page_names:
        _write_page_sheet(
            workbook.add_worksheet(sheet_name(page)),
            page,
            documentation.pages.get(page, []),
            documentation,
            formats,
        )

    _write_pages_sheet(
        workbook.add_worksheet(sheet_name("Pages")), documentation, formats, with_description=False
    )
    add_report_sheets(workbook, documentation, formats, sheet_name)

    add_model_sheets(
        workbook, documentation.model, formats["bi"], sheet_name, documentation.live_statistics
    )
    if documentation.bpa_violations is not None:
        add_bpa_sheets(workbook, documentation.bpa_violations, formats["bi"], sheet_name)

    workbook.close()


def write_data_workbook(
    path: str, documentation: Documentation, graph_path: str, known_functions: list
) -> None:
    """
    The flat data workbook: pages, common, relationships, unused measures, dependencies, and
    the model/BPA sheets.

    Args:
        path: .xlsx file to write
        documentation: From documentation.build_documentation()
        graph_path: Relationship PNG to embed
        known_functions: DAX function names to highlight
    """
    workbook = xlsxwriter.Workbook(path, WORKBOOK_OPTIONS)
    formats = create_formats(workbook)

    _write_pages_sheet(
        workbook.add_worksheet("pages"), documentation, formats, with_description=True
    )

    worksheet = workbook.add_worksheet("common")
    _set_object_columns(worksheet, formats)
    for col, name in enumerate(OBJECT_COLUMNS):
        worksheet.write(0, col, name, formats["bi"])
    _write_objects(worksheet, documentation, formats, 1, known_functions)

    worksheet = workbook.add_worksheet("relationships")
    worksheet.set_column(0, len(documentation.relations.columns) - 1, 25, formats["wrap"])
    _write_relations(worksheet, documentation, formats, graph_path)

    worksheet = workbook.add_worksheet("unused measures")
    worksheet.set_column(0, 0, 60, formats["wrap"])
    worksheet.set_column(1, 1, 15, formats["wrap"])
    worksheet.write(0, 0, "Unused Columns and Measures", formats["bi"])
    worksheet.write(0, 1, "Type", formats["bi"])
    row_num = 1
    for object_type, objects in (
        ("Column", documentation.unused_columns),
        ("Measure", documentation.unused_measures),
    ):
        for table_name, field_name in objects:
            worksheet.write(row_num, 0, f"{table_name}[{field_name}]")
            worksheet.write(row_num, 1, object_type)
            row_num += 1

    _write_dependencies_sheet(workbook.add_worksheet("dependencies"), documentation, formats)
    add_report_sheets(workbook, documentation, formats)

    add_model_sheets(
        workbook, documentation.model, formats["bi"], stats=documentation.live_statistics
    )
    if documentation.bpa_violations is not None:
        add_bpa_sheets(workbook, documentation.bpa_violations, formats["bi"])

    workbook.close()


def _write_dependencies_sheet(worksheet, documentation: Documentation, formats: dict) -> None:
    """One row per object -> dependency; exact when Tabular Editor was available."""
    worksheet.set_column(0, 0, 50, formats["wrap"])
    worksheet.set_column(1, 1, 50, formats["wrap"])
    worksheet.set_column(2, 3, 16, formats["wrap"])
    for col, title in enumerate(["MeasureName", "Dependent", "Object Type", "Dependent Type"]):
        worksheet.write(0, col, title, formats["bi"])

    rows = []
    if documentation.exact_dependencies is not None:
        # Exact (Tabular Editor): measures, calculated columns/tables and RLS filters
        rows = [
            (d.source_ref, d.target_ref, d.source_type, d.target_type)
            for d in documentation.exact_dependencies
        ]
    else:
        for _, row in documentation.objects.iterrows():
            if row["Type"] == "Column":
                continue
            name = f"{row['Table']}[{row['Name']}]"
            columns = find_columns(row["Definition"])
            columns_clean = ["[" + column + "]" for _, column in columns]
            standalone = [m for m in find_measures(row["Definition"]) if m not in columns_clean]
            # Text matching cannot tell columns from measures for unqualified [Name] references
            rows += [(name, f"{t}[{c}]", row["Type"], "Column") for t, c in columns]
            rows += [(name, m, row["Type"], "Measure/Column") for m in standalone]

    for row_num, values in enumerate(rows, start=1):
        for col, value in enumerate(values):
            worksheet.write(row_num, col, value)
