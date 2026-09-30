"""Where a partition's data comes from: source objects in M queries and native SQL.

    connector, objects = m_sources(partition.expression, queries)
    sql_tables('SELECT * FROM dbo.Sales s JOIN [dim].[Customer] c ON ...')
    # -> ["dbo.Sales", "dim.Customer"]

Parsed from the query text only (no connection to the source), so unusual queries may give
partial results:
- navigation steps: Schema/Item (SQL Server, Fabric), Name (single-database connectors),
  Id/ItemKind "Table" (Lakehouse.Contents)
- native SQL (Value.NativeQuery, [Query="..."], DirectQuery query partitions) parsed with
  sqlglot (T-SQL dialect); CTE names are left out
- references to other queries (shared expressions or other tables' queries) are followed, so a
  table built with Table.Combine({a, b}) gets the sources of a and b
- "Enter data" tables (rows embedded in the query) get connector "Entered data"
"""

import re
from typing import Optional

import sqlglot
from sqlglot import exp

NATIVE_SQL = "native SQL query"  # placeholder object when the SQL cannot be parsed

_CONNECTOR = re.compile(
    r"\b(Sql\.Databases?|PostgreSQL\.Database|Lakehouse\.Contents|Fabric\.Warehouse|"
    r"Snowflake\.Databases|Databricks\.Catalogs|Odbc\.\w+|OleDb\.\w+|Oracle\.Database|"
    r"Excel\.Workbook|Csv\.Document|Json\.Document|SharePoint\.\w+|Web\.Contents|"
    r"AzureStorage\.\w+|PowerPlatform\.Dataflows|Dataflows?\.\w+)\s*\("
)
_SCHEMA_ITEM = re.compile(r'Schema\s*=\s*"([^"]+)"\s*,\s*Item\s*=\s*"([^"]+)"')
# Source{[Name="vw_x"]}[Data] - only for connectors whose first navigation level is the object
_NAME_NAVIGATION = re.compile(r'\{\s*\[\s*Name\s*=\s*"([^"]+)"')
_NAME_CONNECTORS = {"Sql.Database", "PostgreSQL.Database", "Oracle.Database", "Fabric.Warehouse"}
_LAKEHOUSE_TABLE = re.compile(r'\[\s*Id\s*=\s*"([^"]+)"\s*,\s*ItemKind\s*=\s*"Table"')
# An M text literal: "..." with "" as the escaped quote
_M_STRING = r'"((?:[^"]|"")*)"'
_NATIVE_QUERY = re.compile(r"Value\.NativeQuery\s*\(\s*[^,]+,\s*" + _M_STRING)
_QUERY_OPTION = re.compile(r"\[\s*Query\s*=\s*" + _M_STRING)
_ENTERED_DATA = re.compile(r"Table\.FromRows\s*\(\s*Json\.Document\s*\(\s*Binary\.Decompress")
_PARAMETER = re.compile(r"meta\s*\[\s*IsParameterQuery\s*=\s*true", re.IGNORECASE)
_M_ESCAPES = {"#(lf)": "\n", "#(cr)": "\r", "#(tab)": "\t", "#(cr,lf)": "\r\n"}


def m_text(literal: str) -> str:
    """The value of an M text literal's content: "" -> ", #(lf) -> newline, etc."""
    text = literal.replace('""', '"')
    for escape, char in _M_ESCAPES.items():
        text = text.replace(escape, char)
    return text


def sql_tables(sql: str) -> Optional[list[str]]:
    """
    Tables and views a SQL query reads from.

    Args:
        sql: SQL text (T-SQL dialect; also fine for most ANSI SQL)

    Returns:
        Names as written ("schema.table", "db.schema.table" or "table"), without CTE names,
        in a stable order (outer query first). None if the SQL cannot be parsed.
    """
    try:
        statements = sqlglot.parse(sql, read="tsql")
    except sqlglot.errors.SqlglotError:
        return None
    tables = []
    for statement in statements:
        if statement is None:
            continue
        ctes = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}
        for table in statement.find_all(exp.Table):
            if not table.name or (not table.db and table.name.lower() in ctes):
                continue
            tables.append(".".join(part for part in (table.catalog, table.db, table.name) if part))
    return list(dict.fromkeys(tables))


def _native_sql_objects(expression: str) -> list[str]:
    """Source tables of the native SQL in an M query ([NATIVE_SQL] if it cannot be parsed)."""
    literals = _NATIVE_QUERY.findall(expression) + _QUERY_OPTION.findall(expression)
    objects = []
    for literal in literals:
        objects += sql_tables(m_text(literal)) or [NATIVE_SQL]
    return objects


def _references(expression: str, queries: dict[str, str]) -> list[str]:
    """Names of other queries an M expression refers to (#"name" or a bare identifier)."""
    found = []
    for name in queries:
        quoted = f'#"{name}"' in expression
        bare = re.fullmatch(r"[A-Za-z_][\w.]*", name) and re.search(
            rf'(?<![\w."#]){re.escape(name)}(?![\w"])', expression
        )
        # A step of this query with the same name (e.g. "Source = ...") is not a reference
        own_step = re.search(
            rf'(?:^|\blet\b|,)\s*(?:#"{re.escape(name)}"|{re.escape(name)})\s*=[^=>]',
            expression,
            re.MULTILINE,
        )
        if (quoted or bare) and not own_step:
            found.append(name)
    return found


def m_sources(
    expression: str, queries: Optional[dict[str, str]] = None, name: str = ""
) -> tuple[str, list[str]]:
    """
    Connector and source objects of an M query.

    Args:
        expression: The M query
        queries: Other queries by name (shared expressions, other tables' queries); their
            sources are included when this query refers to them
        name: This query's own name (never followed as a reference)

    Returns:
        (connector, objects), e.g. ("Sql.Database", ["dbo.DimCustomer"]); connector "M" when
        no known connector is used, "Entered data" for tables typed into Power Query.
    """
    return _m_sources(expression, queries or {}, {name})


def _m_sources(expression: str, queries: dict[str, str], seen: set) -> tuple[str, list[str]]:
    connectors = _CONNECTOR.findall(expression)
    if _ENTERED_DATA.search(expression) and set(connectors) <= {"Json.Document"}:
        return "Entered data", []
    connector = connectors[0] if connectors else ""

    objects = [f"{schema}.{item}" for schema, item in _SCHEMA_ITEM.findall(expression)]
    objects += _LAKEHOUSE_TABLE.findall(expression)
    if not objects and connector in _NAME_CONNECTORS:
        objects = _NAME_NAVIGATION.findall(expression)
    objects += _native_sql_objects(expression)

    for name in _references(expression, queries):
        if name in seen or _PARAMETER.search(queries[name]):
            continue
        seen.add(name)
        ref_connector, ref_objects = _m_sources(queries[name], queries, seen)
        if not connector and ref_connector not in ("M", "Entered data"):
            connector = ref_connector
        objects += ref_objects

    return connector or "M", list(dict.fromkeys(objects))
