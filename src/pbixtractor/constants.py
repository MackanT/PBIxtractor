"""Constants and configuration for PBI-Ixtractor."""

# Default color scheme for syntax highlighting
DEFAULT_COLORS = [
    ["Functions", (49, 101, 187, 255)],
    ["Measures", (0, 16, 128, 255)],
    ["Return", (24, 0, 255, 255)],
    ["Variables", (9, 134, 88, 255)],
    ["Comments", (8, 128, 15, 255)],
    ["Quotes", (163, 21, 21, 255)],
    ["VarNames", (0, 15, 255, 255)],
]

# UI color codes
UI_COLORS = {
    "W": (255, 255, 255),  # White
    "G": (102, 204, 102),  # Green
    "Y": (255, 255, 102),  # Yellow
    "O": (255, 153, 51),   # Orange
    "R": (255, 77, 77),    # Red
}

# Description tag delimiter
DESCRIPT_TAG = "////"

# Excel column widths
EXCEL_COLUMN_WIDTHS = {
    "default": 30,
    "type": 50,
    "field": 60,
    "display_name": 60,
}

# Report data columns
REPORT_COLUMNS = [
    "Page",
    "Visual Type",
    "Visual ID",
    "Table",
    "Name",
    "Display Name",
    "Type",
]

# Tab replacement token for formatting
TAB_REPLACEMENT = " XXX "
