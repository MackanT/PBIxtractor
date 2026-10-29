"""DAX tokenizer and exact dependencies, in Python (no Tabular Editor needed).

    tokens = tokenize("SUM ( Sales[Amount] ) // total")
    "".join(t.text for t in tokens)          # the input, exactly
    dependencies = model_dependencies(model)  # tabular_editor.Dependency rows

The tokenizer is lossless and never raises on malformed DAX. model_dependencies() resolves
references the way DAX does (case-insensitively): qualified T[X] to a column or measure of T,
unqualified [X] to a measure (or, with a row context, a column of the object's own table),
bare table names to the table; code inside comments and strings is never a reference.
"""

import re
from dataclasses import dataclass, field
from typing import Optional

from .semantic_model import SemanticModel, Table
from .tabular_editor import Dependency, drop_redundant_table_refs

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


def _references(
    dax: str, index: _ModelIndex, own_table: Optional[str] = None
) -> list[tuple[str, str, str]]:
    """
    Objects one DAX expression references, as (type, table, name), in order, no duplicates.

    Args:
        dax: The expression
        index: The model's lookup
        own_table: Table whose columns an unqualified [X] may mean (row context of a
            calculated column or RLS filter); None for measures and calculated tables

    Returns:
        (target type, target table, target name) tuples
    """
    tokens = [t for t in tokenize(dax) if t.kind not in TRIVIA]
    gap = [False] * len(tokens)  # whitespace/comment before token i
    position = 0
    for i, token in enumerate(tokens):
        gap[i] = token.position > position
        position = token.position + len(token.text)

    found: list[tuple[str, str, str]] = []

    def add(reference):
        if reference and reference not in found:
            found.append(reference)

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
                    add(index.qualified(table, following.name))
                    i += 2
                    continue
            previous = tokens[i - 1] if i > 0 else None
            after_dot = previous is not None and previous.text == "." and previous.kind == PUNCTUATION
            if table.lower() in index.tables and not shadowed and not after_dot:
                canonical = index.tables[table.lower()].name
                add(("Table", canonical, canonical))
            i += 1
            continue

        if kind == BRACKET_NAME:
            name = token.name.lower()
            if name in index.measures:
                table, measure = index.measures[name]
                add(("Measure", table, measure))
            elif own_table is not None and name in index.columns.get(own_table.lower(), {}):
                add(("Column", index.tables[own_table.lower()].name,
                     index.columns[own_table.lower()][name]))  # fmt: skip
        i += 1
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
                        [measure.expression, measure.format_string_expression], None))  # fmt: skip
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
                            [item.expression, item.format_string_expression], None))  # fmt: skip

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
