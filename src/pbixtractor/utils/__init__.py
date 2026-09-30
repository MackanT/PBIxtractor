"""Utility functions for PBI-Ixtractor."""

import os
import re

# Windows cannot create files or folders with these names (with any extension)
_RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL"} | {f"{d}{i}" for d in ("COM", "LPT") for i in range(1, 10)}


def safe_name(name: str) -> str:
    """A file/folder name for a workspace, report, project, ... name from a service."""
    clean = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", name).strip(" .") or "item"
    if clean.split(".")[0].upper() in _RESERVED_NAMES:
        clean = f"_{clean}"
    return clean


def rgba_tuple_to_hex(color: tuple) -> str:
    """
    Convert RGBA tuple to hexadecimal color code.

    Args:
        color: RGBA tuple (r, g, b, a)

    Returns:
        Hex color string (e.g., "#ff0000")
    """
    r, g, b, _ = color
    return f"#{r:02x}{g:02x}{b:02x}"


def find_nth_occurrence(substring: str, string: str, n: int) -> int:
    """
    Find starting index of n-th occurrence of substring in string.

    Args:
        substring: String to search for
        string: String to search in
        n: Which occurrence to find (1-indexed)

    Returns:
        Starting index of n-th occurrence, or -1 if not found
    """
    count = 0
    index = -1

    while count < n:
        index = string.find(substring, index + 1)
        if index == -1:
            break
        count += 1

    return index


def find_vars(string: str) -> tuple[str]:
    """
    Extract formatted variable names from a string.

    Args:
        string: String to parse for variable names

    Returns:
        Tuple of variable names found
    """
    var_names = []
    tokens = string.split()

    for i, token in enumerate(tokens):
        if token in ["VAR", "var"] and i + 1 < len(tokens):  # "... no var" at the very end
            var_names.append(tokens[i + 1])

    return tuple(var_names)


def write_to_excel(worksheet, row: int, col: int, text: list[str]):
    """
    Write formatted text to Excel worksheet with bold markers.

    Args:
        worksheet: XlsxWriter worksheet object
        row: Row number
        col: Column number
        text: List of text segments with bold format markers
    """
    if isinstance(text, list):
        if len(text) <= 2:
            # Join the elements into a single string and write to the cell
            worksheet.write(row, col, " ".join(map(str, text)))
        else:
            # Write a rich formatted string for lists longer than 2 elements
            # Convert any non-string values (like floats) to strings, but keep format objects as-is
            cleaned_text = []
            for item in text:
                # Check if item is a format object (has 'xf_format_indices' attribute)
                if hasattr(item, "xf_format_indices"):
                    cleaned_text.append(item)
                else:
                    # Convert to string if not already
                    str_item = str(item)
                    # Skip empty strings to avoid Excel warnings
                    if str_item:
                        cleaned_text.append(str_item)
            # Only write if we have content
            if cleaned_text:
                worksheet.write_rich_string(row, col, *cleaned_text)
            else:
                worksheet.write(row, col, "")
    else:
        # If text is not a list, write the single value
        worksheet.write(row, col, text)


def is_excel_open_with_file(file_path: str) -> bool:
    """
    Check if a workbook is open in Excel (or locked by another program), so it cannot be
    overwritten.

    Excel locks open workbooks against writing, so trying to open the file for writing is
    enough. (Listing the open files of every process with psutil took ~35 s per call.)

    Args:
        file_path: Path to Excel file

    Returns:
        True if the file exists and cannot be opened for writing
    """
    if not os.path.exists(file_path):
        return False
    try:
        with open(file_path, "r+b"):
            return False
    except PermissionError:
        return True


def excel_sheet_name(name: str, used: set[str]) -> str:
    """
    Make a valid, unique Excel worksheet name.

    Excel sheet names are max 31 characters, cannot contain []:*?/\\ and must be
    unique (case-insensitive) within a workbook.

    Args:
        name: Desired sheet name
        used: Lower-cased names already used in the workbook (updated in place)

    Returns:
        Valid sheet name
    """
    clean = re.sub(r"[\[\]:*?/\\]", "_", name).strip("'") or "Sheet"

    def fit(text: str, suffix: str = "") -> str:
        # Excel rejects a name that starts or ends with ' - also after cutting to 31 characters
        return (text[: 31 - len(suffix)].strip("'") or "Sheet") + suffix

    candidate = fit(clean)
    if candidate.lower() == "history":  # reserved by Excel
        candidate = fit(clean, " (page)")
    counter = 2
    while candidate.lower() in used:
        candidate = fit(clean, f" ({counter})")
        counter += 1
    used.add(candidate.lower())
    return candidate
