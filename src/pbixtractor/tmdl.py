"""Read a TMDL model folder (Tabular Model Definition Language) into a TMSL (.bim) dictionary.

TMDL is the text format newer PBIP projects use for the semantic model
(<name>.SemanticModel/definition/*.tmdl). TMDL property names are the TMSL (.bim JSON) names, so
the folder is turned into the same dictionary a .bim file holds and read by
semantic_model.parse_model() - one code path for both formats.

    database = read_tmdl_folder("Sales.SemanticModel/definition")   # {"name", "model": {...}}

Syntax handled (https://learn.microsoft.com/analysis-services/tmdl/tmdl-overview):
    table 'Sales Order'                  object: keyword + name ('quoted' when needed, '' = ')
        /// Description                  description lines before an object
        column Amount                    child objects are indented one tab deeper
            dataType: double             properties: "name: value" ("..." with "" = ")
            isHidden                     boolean flag = true
        measure Total = SUM(Sales[Amount])   default property (expression) after "="
        measure Multi =                  multi-line expression, indented two tabs deeper than
                VAR x = 1                the object, or fenced with ``` ... ```
                RETURN x
        partition Sales = m              partition source type after "="
            source =                     expression property (no name)
    ref table Sales                      table order (model.tmdl)
Annotations, lineage tags, cultures and similar metadata are read but not used.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

_PROPERTY = re.compile(r"^([A-Za-z_][\w]*)\s*:\s*(.*)$")
_KEYWORD = re.compile(r"^([A-Za-z_][\w]*)\s*(.*)$")


@dataclass
class TmdlNode:
    """One TMDL object, e.g. a table, column, measure or partition."""

    keyword: str
    name: str = ""
    value: Optional[str] = None  # the default property / expression after "="
    properties: dict = field(default_factory=dict)
    children: list["TmdlNode"] = field(default_factory=list)
    description: str = ""
    indent: int = -1

    def child(self, keyword: str) -> Optional["TmdlNode"]:
        return next((c for c in self.children if c.keyword == keyword), None)

    def all(self, keyword: str) -> list["TmdlNode"]:
        return [c for c in self.children if c.keyword == keyword]


# ============================================================================
# Lexical helpers
# ============================================================================


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip("\t"))


def unquote_name(text: str) -> str:
    """'Sales Order' -> Sales Order; 'It''s' -> It's; bare names unchanged."""
    text = text.strip()
    if len(text) >= 2 and text[0] == "'" and text[-1] == "'":
        return text[1:-1].replace("''", "'")
    return text


def _split_name(rest: str) -> tuple[str, str]:
    """Split "'My Name' = expr" / "Name = expr" / "Name" into (name, remainder)."""
    rest = rest.strip()
    if rest.startswith("'"):
        i = 1
        while i < len(rest):
            if rest[i] == "'":
                if i + 1 < len(rest) and rest[i + 1] == "'":
                    i += 2
                    continue
                break
            i += 1
        return unquote_name(rest[: i + 1]), rest[i + 1 :].strip()
    match = re.match(r"^([^\s=]*)\s*(.*)$", rest)
    return match.group(1), match.group(2).strip()


def split_qualified(reference: str) -> tuple[str, str]:
    """'Sales Territory'.SalesTerritoryKey -> ("Sales Territory", "SalesTerritoryKey")."""
    table, rest = _split_name(reference)
    if rest.startswith("."):
        return table, unquote_name(rest[1:])
    # Bare names: split at the first dot
    table, _, column = reference.partition(".")
    return unquote_name(table), unquote_name(column)


def _scalar(value: str):
    """Property value: true/false, "quoted text" ("" = "), or the text as-is."""
    value = value.strip()
    if value in ("true", "false"):
        return value == "true"
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1].replace('""', '"')
    return value


def _dedent(lines: list[str], tabs: int) -> str:
    out = []
    for line in lines:
        strip = min(tabs, _indent(line))
        out.append(line[strip:].rstrip("\r"))
    while out and not out[-1].strip():
        out.pop()
    while out and not out[0].strip():
        out.pop(0)
    return "\n".join(out)


# ============================================================================
# Parser
# ============================================================================


def parse_tmdl(text: str) -> list[TmdlNode]:
    """
    Parse one .tmdl document.

    Args:
        text: Content of a .tmdl file

    Returns:
        Top-level objects of the document
    """
    lines = text.replace("\r\n", "\n").split("\n")
    root = TmdlNode("root")
    stack = [root]
    description: list[str] = []
    last_node: Optional[TmdlNode] = None
    i = 0
    while i < len(lines):
        raw = lines[i]
        stripped = raw.strip()
        i += 1
        if not stripped:
            continue
        level = _indent(raw)
        while stack[-1].indent >= level:
            stack.pop()
        parent = stack[-1]

        if stripped.startswith("///"):
            text_line = stripped[3:]
            description.append(text_line[1:] if text_line.startswith(" ") else text_line)
            continue

        prop = _PROPERTY.match(stripped)
        if prop and parent is not root:
            parent.properties[prop.group(1)] = _scalar(prop.group(2))
            description = []
            continue

        keyword_match = _KEYWORD.match(stripped)
        if keyword_match is None:
            # Not a keyword line, e.g. the continuation of an expression that started on
            # the object's own line ("measure A = DIVIDE([A],"): keep it with that expression
            if last_node is not None and last_node.value is not None:
                last_node.value += "\n" + stripped
            continue
        keyword, rest = keyword_match.groups()
        if keyword == "ref":  # ref table Sales
            ref_type, _, ref_name = rest.partition(" ")
            node = TmdlNode("ref", unquote_name(ref_name), ref_type, indent=level)
        elif rest.startswith("="):  # expression property without a name: source = ...
            node = TmdlNode(keyword, "", rest[1:].strip(), indent=level)
        else:
            name, remainder = _split_name(rest)
            value = remainder[1:].strip() if remainder.startswith("=") else None
            node = TmdlNode(keyword, name, value, indent=level)

        if node.value is not None and node.value.startswith("```"):
            # Fenced expression: verbatim until the closing ```
            body = []
            while i < len(lines) and lines[i].strip() != "```":
                body.append(lines[i])
                i += 1
            i += 1  # closing fence
            node.value = _dedent(body, level + 2)
        elif node.value == "":
            # Multi-line expression: lines indented two tabs deeper than the object
            body = []
            while i < len(lines) and (not lines[i].strip() or _indent(lines[i]) >= level + 2):
                body.append(lines[i])
                i += 1
            node.value = _dedent(body, level + 2)

        # A bare keyword without deeper lines is a boolean flag (isHidden, isKey, ...)
        next_line = next((line for line in lines[i:] if line.strip()), None)
        has_children = next_line is not None and _indent(next_line) > level
        if not node.name and node.value is None and not has_children and parent is not root:
            parent.properties[keyword] = True
            description = []
            continue

        node.description = "\n".join(description)
        description = []
        parent.children.append(node)
        stack.append(node)
        last_node = node
    return root.children


# ============================================================================
# TMDL objects -> TMSL dictionary
# ============================================================================


def _with_description(data: dict, node: TmdlNode) -> dict:
    if node.description:
        data["description"] = node.description
    return data


def _measure(node: TmdlNode) -> dict:
    data = {"name": node.name, **node.properties}
    if node.value is not None:
        data["expression"] = node.value
    format_definition = node.child("formatStringDefinition")
    if format_definition is not None and format_definition.value is not None:
        data["formatStringDefinition"] = {"expression": format_definition.value}
    return _with_description(data, node)


def _column(node: TmdlNode, calculated_table: bool) -> dict:
    data = {"name": node.name, **node.properties}
    if node.value is not None:
        data["type"] = "calculated"
        data["expression"] = node.value
    elif calculated_table:
        data["type"] = "calculatedTableColumn"
    if "sortByColumn" in data:
        data["sortByColumn"] = unquote_name(data["sortByColumn"])
    return _with_description(data, node)


def _partition(node: TmdlNode) -> dict:
    source = {"type": node.value or ""}
    source_node = node.child("source")
    if source_node is not None:
        if source_node.value is not None:
            source["expression"] = source_node.value
        for key, value in source_node.properties.items():
            source[key] = unquote_name(value) if isinstance(value, str) else value
    data = {"name": node.name, **node.properties, "source": source}
    return data


def _table(node: TmdlNode) -> dict:
    partitions = [_partition(p) for p in node.all("partition")]
    calculated_table = any(p["source"]["type"] == "calculated" for p in partitions)
    data = {
        "name": node.name,
        **node.properties,
        "columns": [_column(c, calculated_table) for c in node.all("column")],
        "measures": [_measure(m) for m in node.all("measure")],
        "hierarchies": [
            _with_description(
                {
                    "name": h.name,
                    **h.properties,
                    "levels": [
                        {"name": lv.name, "column": unquote_name(str(lv.properties.get("column", "")))}
                        for lv in h.all("level")
                    ],
                },
                h,
            )
            for h in node.all("hierarchy")
        ],
        "partitions": partitions,
    }
    group = node.child("calculationGroup")
    if group is not None:
        data["calculationGroup"] = {
            **group.properties,
            "calculationItems": [_measure(item) for item in group.all("calculationItem")],
        }
    return _with_description(data, node)


def _relationship(node: TmdlNode) -> dict:
    data = {"name": node.name, **node.properties}
    for side in ("from", "to"):
        reference = data.get(f"{side}Column")
        if isinstance(reference, str):
            data[f"{side}Table"], data[f"{side}Column"] = split_qualified(reference)
    return data


def _role(node: TmdlNode) -> dict:
    return _with_description(
        {
            "name": node.name,
            **node.properties,
            "tablePermissions": [
                {"name": p.name, "filterExpression": p.value or "", **p.properties}
                for p in node.all("tablePermission")
            ],
        },
        node,
    )


def to_tmsl(nodes: list[TmdlNode]) -> dict:
    """
    Build the TMSL database dictionary (as in a .bim file) from parsed TMDL objects.

    Args:
        nodes: Top-level objects of all .tmdl files of the model

    Returns:
        {"name", "compatibilityLevel", "model": {"culture", "tables", "relationships", ...}}
    """
    database = next((n for n in nodes if n.keyword == "database"), TmdlNode("database"))
    model_node = next((n for n in nodes if n.keyword == "model"), TmdlNode("model"))

    # model.tmdl lists tables/roles in model order ("ref table X", top level after "model");
    # the files themselves come alphabetically
    refs = [n for n in nodes if n.keyword == "ref"] + model_node.all("ref")

    def in_model_order(keyword: str) -> list[TmdlNode]:
        objects = {n.name: n for n in nodes if n.keyword == keyword}
        order = [r.name for r in refs if r.value == keyword and r.name in objects]
        order = list(dict.fromkeys(order)) + [name for name in objects if name not in order]
        return [objects[name] for name in order]

    model = {
        **model_node.properties,
        "tables": [_table(n) for n in in_model_order("table")],
        "relationships": [_relationship(n) for n in nodes if n.keyword == "relationship"],
        "expressions": [
            _with_description(
                {"name": n.name, "kind": "m", **n.properties, "expression": n.value or ""}, n
            )
            for n in nodes
            if n.keyword == "expression"
        ],
        "roles": [_role(n) for n in in_model_order("role")],
        "perspectives": [{"name": n.name} for n in nodes if n.keyword == "perspective"],
    }
    result = {"name": database.name or model_node.name, **database.properties, "model": model}
    level = result.get("compatibilityLevel")
    if isinstance(level, str) and level.isdigit():
        result["compatibilityLevel"] = int(level)
    return result


def find_tmdl_folder(path: str | Path) -> Optional[Path]:
    """
    The folder holding the .tmdl files for a model path, or None if it is not TMDL.

    Accepts the definition folder itself, a <name>.SemanticModel folder (with definition/),
    or any .tmdl file inside the definition folder (e.g. model.tmdl).
    """
    path = Path(path)
    if path.is_file() and path.suffix.lower() == ".tmdl":
        path = path.parent
        if path.name in ("tables", "roles", "cultures", "perspectives"):
            path = path.parent
    if not path.is_dir():
        return None
    for folder in (path / "definition", path):
        if (folder / "model.tmdl").is_file() or (folder / "database.tmdl").is_file():
            return folder
    return None


def read_tmdl_folder(folder: str | Path) -> dict:
    """
    Read all .tmdl files of a model into a TMSL database dictionary.

    Args:
        folder: The definition folder (with database.tmdl / model.tmdl / tables/...)

    Returns:
        Dictionary with the structure of a .bim file
    """
    folder = Path(folder)
    nodes = []
    for file in sorted(folder.rglob("*.tmdl")):
        nodes.extend(parse_tmdl(file.read_bytes().decode("utf-8-sig")))
    return to_tmsl(nodes)
