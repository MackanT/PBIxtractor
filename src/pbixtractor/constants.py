"""Constants and configuration for PBI-Ixtractor."""

# Colours of the DAX syntax highlighting in the workbooks: [name, RGBA]
DEFAULT_COLORS = [
    ["Functions", (49, 101, 187, 255)],
    ["Measures", (0, 16, 128, 255)],
    ["Return", (24, 0, 255, 255)],
    ["Variables", (9, 134, 88, 255)],
    ["Comments", (8, 128, 15, 255)],
    ["Quotes", (163, 21, 21, 255)],
    ["VarNames", (0, 15, 255, 255)],
]

# Description tag delimiter
DESCRIPT_TAG = "////"

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
