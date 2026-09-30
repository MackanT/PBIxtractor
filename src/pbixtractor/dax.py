"""DAX text helpers: reference extraction (text matching) and syntax highlighting for Excel.

Text matching is the fallback when Tabular Editor's exact dependencies are unavailable; it
cannot resolve unqualified [Name] references and also matches text inside comments.
"""

import re

from .utils import find_vars

# Tokens for highlight_dax(): brackets, [refs], commas, comments, decimals, words, dots, other
_TOKEN = re.compile(r"(\(|\)|\[.*?\]|,|//|\d+\.\d+|\w+|(?<!\d)\.(?!\d)|\W)")
_BRACKET_REF = re.compile(r"\[.*?\]")
# Table[Column] and 'Table name'[Column] ('' escapes a quote inside a quoted name)
_COLUMN_REF = re.compile(r"('(?:[^']|'')+'|\w+)\[(.*?)\]")

# Placeholders for highlight_dax(): private-use characters, which cannot occur in real DAX
# (plain words like "XXX" could - a column or variable with that name was rewritten)
_TAB, _NEWLINE, _AND, _OR = "", "", "", ""


def find_functions(dax_code: str, known_functions: list) -> list[str]:
    """
    Find all known DAX functions used in code.

    Args:
        dax_code: DAX code string
        known_functions: List of known function names

    Returns:
        List of functions found in the code
    """
    return [func for func in known_functions if func in dax_code]


def find_measures(dax_code: str) -> list[str]:
    """
    Extract measure references from DAX code.

    Args:
        dax_code: DAX code string

    Returns:
        List of unique measure references (e.g., "[Measure Name]")
    """
    return list(set(_BRACKET_REF.findall(dax_code)))


def find_columns(dax_code: str) -> list[tuple[str, str]]:
    """
    Extract column references from DAX code.

    Args:
        dax_code: DAX code string

    Returns:
        List of (table, column) tuples; quoted table names are unquoted ("Sales Data")
    """
    return list(
        {
            (table[1:-1].replace("''", "'") if table.startswith("'") else table, column)
            for table, column in _COLUMN_REF.findall(dax_code)
        }
    )


def text_dependencies(dax_code: str) -> list[str]:
    """
    Objects a DAX expression refers to, by text matching.

    Args:
        dax_code: DAX code string

    Returns:
        "Table[Column]" for qualified references, then "[Name]" for unqualified ones
    """
    columns = find_columns(dax_code)
    columns_clean = ["[" + column + "]" for _, column in columns]
    standalone = [m for m in find_measures(dax_code) if m not in columns_clean]
    return [table + "[" + column + "]" for table, column in columns] + standalone


def highlight_dax(dax_code: str, formats: dict, known_functions: list) -> list:
    """
    Split DAX into rich-text segments for xlsxwriter's write_rich_string.

    Args:
        dax_code: DAX code string
        formats: Excel formats with keys comment, quote, para (list), var, varname, measure,
            function, return (see excel_report.create_formats)
        known_functions: DAX function names to highlight

    Returns:
        Alternating formats and text segments (empty list for empty DAX)
    """
    var_names = find_vars(dax_code)
    function_names = find_functions(dax_code, known_functions)
    columns = find_columns(dax_code)
    tables = [table for table, _ in columns]
    columns_clean = ["[" + column + "]" for _, column in columns]
    measures = find_measures(dax_code)

    # Protect whitespace and operators that the tokenizer would otherwise split or drop
    text = dax_code.replace("\t", f" {_TAB} ")
    text = text.replace("\r\n", f" {_NEWLINE} ")
    text = text.replace("\n", f" {_NEWLINE} ")
    text = text.replace("&&", f" {_AND} ")
    text = text.replace("||", f" {_OR} ")
    tokens = [token for token in _TOKEN.findall(text) if token.strip()]

    segments = []
    add = segments.extend
    parenthesis_count = -1
    is_whole_line_comment = False
    quote_counter = 0
    deepest = len(formats["para"]) - 1

    def para(depth: int):
        """Parenthesis colour for a nesting depth (the palette repeats its last colour)."""
        return formats["para"][max(0, min(depth, deepest))]

    for token in tokens:
        if token == "//":
            is_whole_line_comment = True
        elif token == _NEWLINE:
            is_whole_line_comment = False

        if token == '"' and not is_whole_line_comment:
            quote_counter += 1

        if is_whole_line_comment:
            add((formats["comment"], token + " "))
        elif quote_counter > 0:
            add((formats["quote"],))
            if quote_counter == 2:
                add((token + " ",))
                quote_counter = 0
            else:
                add((token,))
        elif token == _TAB:
            add(("\t",))
        elif token == _NEWLINE:
            add(("\n",))
        elif token == _AND:
            add(("&& ",))
        elif token == _OR:
            add(("|| ",))
        elif token == "(":
            parenthesis_count += 1
            add((para(parenthesis_count), token + " "))
        elif token == ")":
            add((para(parenthesis_count), token + " "))
            parenthesis_count -= 1
        elif token == "VAR":
            add((formats["var"], token + " "))
        elif token in var_names:
            add((formats["varname"], token + " "))
        elif token in measures:
            add(
                (
                    para(parenthesis_count + 1),
                    token[0],
                    formats["measure"],
                    token[1:-1],
                    para(parenthesis_count + 1),
                    token[-1] + " ",
                )
            )
        elif token in tables or token in columns_clean:
            add((formats["measure"], token))
        elif token in function_names:
            add((formats["function"], token + " "))
        elif token == "RETURN":
            add((formats["return"], token + " "))
        else:
            add((token, " "))

    return segments
