"""DAX tokenizer and exact dependencies, in Python (no Tabular Editor needed).

    tokens = tokenize("SUM ( Sales[Amount] ) // total")
    "".join(t.text for t in tokens)          # the input, exactly
    dependencies = model_dependencies(model)  # Dependency rows

The tokenizer is lossless and never raises on malformed DAX. model_dependencies() resolves
references the way DAX does (case-insensitively): qualified T[X] to a column or measure of T,
unqualified [X] in Tabular Editor's order (FormulaFixup: a column of the expression's own table,
a measure of that table, then a measure in any table), bare table names to the table; code
inside comments and strings is never a reference.
"""

import re
from dataclasses import dataclass, field
from typing import NamedTuple, Optional

from .semantic_model import SemanticModel, Table

WHITESPACE = "whitespace"
COMMENT = "comment"
STRING = "string"  # "text" with "" as an escaped quote
QUOTED_NAME = "quoted_name"  # 'Table name' with '' as an escaped quote
BRACKET_NAME = "bracket_name"  # [Column or measure] with ]] as an escaped bracket
NUMBER = "number"
IDENTIFIER = "identifier"  # functions, unquoted tables, variables, keywords
OPERATOR = "operator"
PUNCTUATION = "punctuation"
UNKNOWN = "unknown"

TRIVIA = (WHITESPACE, COMMENT)


@dataclass(frozen=True)
class Dependency:
    """A direct DAX dependency: source (measure, calculated column/table, RLS) -> target."""

    # Measure | Column | Table | TablePermission (RLS filter; name = role) | CalculationItem
    source_type: str
    source_table: str
    source_name: str
    target_type: str  # Measure | Column | Table | ...
    target_table: str
    target_name: str

    @property
    def source_ref(self) -> str:
        return _dax_ref(self.source_type, self.source_table, self.source_name)

    @property
    def target_ref(self) -> str:
        return _dax_ref(self.target_type, self.target_table, self.target_name)


def _dax_ref(object_type: str, table: str, name: str) -> str:
    """Table[Name] for columns/measures, 'Table' for tables, "RLS: role (table)" for filters."""
    if object_type == "Table":
        return f"'{name}'"
    if object_type == "TablePermission":
        return f"RLS: {name} ({table})"
    return f"{table}[{name}]"


def drop_redundant_table_refs(dependencies: list[Dependency]) -> list[Dependency]:
    """
    Remove "depends on table X" entries when the same source also uses a column of X.

    Tabular Editor lists 'Sales' as well as Sales[Amount] for SUM ( Sales[Amount] ); the table
    entry only adds information when it is the sole reference, e.g. COUNTROWS ( Sales ).

    Args:
        dependencies: Dependency rows, e.g. of model_dependencies()

    Returns:
        Filtered list, original order kept
    """
    tables_with_columns = {
        (d.source_type, d.source_table, d.source_name, d.target_table)
        for d in dependencies
        if d.target_type == "Column"
    }
    return [
        d
        for d in dependencies
        if not (
            d.target_type == "Table"
            and (d.source_type, d.source_table, d.source_name, d.target_name) in tables_with_columns
        )
    ]


@dataclass(frozen=True)
class Token:
    """One DAX token: its kind, its exact text and its offset in the input."""

    kind: str
    text: str
    position: int

    @property
    def name(self) -> str:
        """The unquoted name of a quoted/bracket name or identifier ('A''s' -> A's)."""
        if self.kind == QUOTED_NAME:
            body = self.text[1:-1] if self.text.endswith("'") and len(self.text) > 1 else self.text[1:]
            return body.replace("''", "'")
        if self.kind == BRACKET_NAME:
            body = self.text[1:-1] if self.text.endswith("]") and len(self.text) > 1 else self.text[1:]
            return body.replace("]]", "]")
        return self.text


# Order matters: comments before the / and - operators, numbers before the "." punctuation.
# The last alternative takes any character, so the matches cover the whole input.
_TOKEN_RE = re.compile(
    r"""
    (?P<whitespace>\s+)
  | (?P<comment>(?://|--)[^\r\n]*|/\*.*?(?:\*/|\Z))
  | (?P<string>"(?:[^"]|"")*"?)
  | (?P<quoted_name>'(?:[^']|'')*'?)
  | (?P<bracket_name>\[(?:[^\]]|\]\])*\]?)
  | (?P<number>(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)
  | (?P<identifier>[^\W\d]\w*(?:\.[^\W\d]\w*)*)
  | (?P<operator>&&|\|\||<=|>=|<>|==|[-+*/^&=<>])
  | (?P<punctuation>[(),{}.])
  | (?P<unknown>.)
    """,
    re.VERBOSE | re.DOTALL,
)


def tokenize(dax: str) -> list[Token]:
    """
    Split DAX into tokens; joining their texts gives back the input exactly.

    Args:
        dax: DAX expression (may be malformed: unclosed strings, names and comments run to
            the end)

    Returns:
        Tokens in input order
    """
    return [
        Token(match.lastgroup, match.group(), match.start())
        for match in _TOKEN_RE.finditer(dax or "")
    ]


# ============================================================================
# Dependencies
# ============================================================================

# Words that are never a table in front of a [name] separated by whitespace ("RETURN [X]")
_KEYWORDS = {
    "VAR", "RETURN", "NOT", "IN", "TRUE", "FALSE", "AND", "OR", "DEFINE", "EVALUATE",
    "MEASURE", "COLUMN", "TABLE", "ORDER", "BY", "ASC", "DESC", "START", "AT",
}  # fmt: skip


@dataclass
class _VarBlock:
    """VAR ... RETURN block: its variables shadow same-named tables until it ends."""

    depth: int
    names: set[str] = field(default_factory=set)
    pending: Optional[str] = None  # the variable being defined (not visible in its own value)
    returned: bool = False


class _ModelIndex:
    """Case-insensitive lookup of tables, columns and measures."""

    def __init__(self, model: SemanticModel):
        self.tables: dict[str, Table] = {t.name.lower(): t for t in model.tables}
        self.columns = {
            t.name.lower(): {c.name.lower(): c.name for c in t.columns} for t in model.tables
        }
        self.table_measures = {
            t.name.lower(): {m.name.lower(): m.name for m in t.measures} for t in model.tables
        }
        self.measures = {
            m.name.lower(): (m.table, m.name) for m in reversed(model.all_measures)
        }  # the first one wins should a name repeat

    def qualified(self, table: str, name: str) -> Optional[tuple[str, str, str]]:
        key = table.lower()
        if key not in self.tables:
            return None
        canonical = self.tables[key].name
        if name.lower() in self.columns[key]:
            return "Column", canonical, self.columns[key][name.lower()]
        if name.lower() in self.table_measures[key]:
            return "Measure", canonical, self.table_measures[key][name.lower()]
        return None

    def unqualified(self, name: str, own_table: Optional[str]) -> Optional[tuple[str, str, str]]:
        """
        What an unqualified [X] means, in Tabular Editor's order (its FormulaFixup): a column of
        the expression's own table, else a measure of that table, else a measure in any table
        (the first table in model order that has one).
        """
        key = name.lower()
        own = own_table.lower() if own_table is not None else None
        if own is not None and own in self.tables:
            canonical = self.tables[own].name
            if key in self.columns[own]:
                return "Column", canonical, self.columns[own][key]
            if key in self.table_measures[own]:
                return "Measure", canonical, self.table_measures[own][key]
        if key in self.measures:
            return ("Measure", *self.measures[key])
        return None


class Reference(NamedTuple):
    """One reference in a DAX expression, as written there."""

    type: str  # Measure | Column | Table
    table: str
    name: str
    qualified: bool  # written as Table[Name] (always True for a table reference)


def code_tokens(dax: str) -> list[Token]:
    """The tokens of an expression without whitespace and comments (Tabular Editor's Tokenize)."""
    return [t for t in tokenize(dax) if t.kind not in TRIVIA]


def model_index(model: SemanticModel) -> "_ModelIndex":
    """The model's name lookup, for reference_occurrences() on many expressions."""
    return _ModelIndex(model)


def reference_occurrences(
    dax: str, index: "_ModelIndex", own_table: Optional[str] = None
) -> list[Reference]:
    """
    Every reference one DAX expression makes, in order, one per occurrence (with repeats).

    Code inside comments and strings is never a reference.

    Args:
        dax: The expression
        index: The model's lookup (model_index())
        own_table: The expression's own table (of a measure, calculated column, table
            permission or calculation item): an unqualified [X] is first its column, then its
            measure, then any measure; None (a calculated table): only ever a measure

    Returns:
        References with whether each one was written fully qualified
    """
    tokens = code_tokens(dax)
    gap = [False] * len(tokens)  # whitespace/comment before token i
    position = 0
    for i, token in enumerate(tokens):
        gap[i] = token.position > position
        position = token.position + len(token.text)

    found: list[Reference] = []

    def add(reference, qualified: bool):
        if reference:
            found.append(Reference(*reference, qualified))

    blocks: list[_VarBlock] = []
    depth = 0
    i = 0
    while i < len(tokens):
        token = tokens[i]
        following = tokens[i + 1] if i + 1 < len(tokens) else None
        kind, upper = token.kind, token.text.upper()

        if kind == PUNCTUATION:
            if token.text in "({":
                depth += 1
            elif token.text in ")}":
                depth -= 1
                blocks = [b for b in blocks if b.depth <= depth]
            elif token.text == ",":
                blocks = [b for b in blocks if b.depth < depth]
            i += 1
            continue

        if kind == IDENTIFIER and upper == "VAR":
            name = following.text.lower() if following and following.kind == IDENTIFIER else None
            top = blocks[-1] if blocks else None
            if top and top.depth == depth and not top.returned:
                if top.pending:
                    top.names.add(top.pending)
                top.pending = name
            else:
                blocks.append(_VarBlock(depth, pending=name))
            i += 2 if name else 1
            continue

        if kind == IDENTIFIER and upper == "RETURN":
            top = blocks[-1] if blocks else None
            if top and top.depth == depth and not top.returned:
                if top.pending:
                    top.names.add(top.pending)
                top.pending, top.returned = None, True
            i += 1
            continue

        if kind in (IDENTIFIER, QUOTED_NAME):
            if following and following.kind == PUNCTUATION and following.text == "(":
                i += 1  # a function, never a table
                continue
            table = token.name
            shadowed = kind == IDENTIFIER and any(table.lower() in b.names for b in blocks)
            if following and following.kind == BRACKET_NAME:
                qualifies = (
                    not gap[i + 1]
                    or kind == QUOTED_NAME
                    or (upper not in _KEYWORDS and table.lower() in index.tables and not shadowed)
                )
                if qualifies:
                    add(index.qualified(table, following.name), True)
                    i += 2
                    continue
            previous = tokens[i - 1] if i > 0 else None
            after_dot = previous is not None and previous.text == "." and previous.kind == PUNCTUATION
            if table.lower() in index.tables and not shadowed and not after_dot:
                canonical = index.tables[table.lower()].name
                add(("Table", canonical, canonical), True)
            i += 1
            continue

        if kind == BRACKET_NAME:
            add(index.unqualified(token.name, own_table), False)
        i += 1
    return found


def _references(
    dax: str, index: _ModelIndex, own_table: Optional[str] = None
) -> list[tuple[str, str, str]]:
    """
    Objects one DAX expression references, as (type, table, name), in order, no duplicates.

    Args:
        dax: The expression
        index: The model's lookup
        own_table: The expression's own table (see reference_occurrences); None for
            calculated tables

    Returns:
        (target type, target table, target name) tuples
    """
    found: list[tuple[str, str, str]] = []
    for reference in reference_occurrences(dax, index, own_table):
        target = (reference.type, reference.table, reference.name)
        if target not in found:
            found.append(target)
    return found


def model_dependencies(model: SemanticModel) -> list[Dependency]:
    """
    Every direct DAX dependency in the model, like Tabular Editor's DependsOn.

    Sources: measures (expression and dynamic format string), calculated columns, calculated
    tables, RLS table filters (source name = the role) and calculation items. Redundant
    table entries are dropped (drop_redundant_table_refs).

    Args:
        model: The semantic model

    Returns:
        Dependencies, sources in the order above, targets in order of appearance
    """
    index = _ModelIndex(model)
    sources: list[tuple[str, str, str, list[str], Optional[str]]] = []
    for measure in model.all_measures:
        sources.append(("Measure", measure.table, measure.name,
                        [measure.expression, measure.format_string_expression],
                        measure.table))  # fmt: skip
    for column in model.all_columns:
        if column.kind == "calculated":
            sources.append(("Column", column.table, column.name, [column.expression], column.table))
    for table in model.tables:
        for partition in table.partitions:
            if partition.source_type == "calculated":
                sources.append(("Table", table.name, table.name, [partition.expression], None))
    for role in model.roles:
        for table, dax in role.table_filters.items():
            sources.append(("TablePermission", table, role.name, [dax], table))
    for table in model.tables:
        for item in table.calculation_items:
            sources.append(("CalculationItem", table.name, item.name,
                            [item.expression, item.format_string_expression],
                            table.name))  # fmt: skip

    dependencies: list[Dependency] = []
    for source_type, source_table, source_name, expressions, own_table in sources:
        targets: list[tuple[str, str, str]] = []
        for dax in expressions:
            for target in _references(dax, index, own_table):
                if target not in targets:
                    targets.append(target)
        dependencies += [
            Dependency(source_type, source_table, source_name, *target) for target in targets
        ]
    return drop_redundant_table_refs(dependencies)
