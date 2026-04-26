"""Utility functions for PBI-Ixtractor."""

import os

import psutil


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
        if token in ["VAR", "var"]:
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
    Check if Excel is currently open with the specified file.

    Args:
        file_path: Path to Excel file

    Returns:
        True if file is open in Excel, False otherwise
    """
    if not os.path.exists(file_path):
        return False

    for proc in psutil.process_iter(["name", "open_files"]):
        try:
            if proc.info["name"] == "EXCEL.EXE":
                if proc.info["open_files"]:
                    for file in proc.info["open_files"]:
                        if os.path.normpath(file.path) == os.path.normpath(file_path):
                            return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return False


def ensure_directory(path: str) -> None:
    """
    Ensure directory exists, create if necessary.

    Args:
        path: Directory path to ensure
    """
    os.makedirs(path, exist_ok=True)
