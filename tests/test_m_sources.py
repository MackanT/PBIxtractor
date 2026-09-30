"""Tests for source detection in M queries and native SQL (m_sources.py)."""

from pbixtractor.m_sources import NATIVE_SQL, m_sources, m_text, sql_tables
from pbixtractor.semantic_model import Partition


def test_sql_tables_joins_brackets_and_ctes():
    sql = """
        WITH recent AS (SELECT * FROM [dbo].[FactSales] WHERE [Date] > '2024-01-01')
        SELECT r.*, c.Name
        FROM recent r
        JOIN dim.Customer c ON c.Id = r.CustomerId
        LEFT JOIN Warehouse.gold.DimDate d ON d.DateKey = r.DateKey
        WHERE r.Id IN (SELECT Id FROM staging.Keep)
    """
    tables = sql_tables(sql)
    assert sorted(tables) == [
        "Warehouse.gold.DimDate",
        "dbo.FactSales",
        "dim.Customer",
        "staging.Keep",
    ]
    assert sql_tables(sql) == tables  # stable order


def test_sql_tables_unparseable():
    assert sql_tables("SELEC nonsense FROM (") is None


def test_m_text_unescapes():
    assert m_text('SELECT ""a b"" FROM t#(lf)WHERE x = 1') == 'SELECT "a b" FROM t\nWHERE x = 1'


def test_native_query_in_m():
    m = """let
    Source = Sql.Database("server", "db"),
    Result = Value.NativeQuery(Source, "SELECT s.Amount FROM dbo.Sales s#(lf)JOIN dbo.Region r ON r.Id = s.RegionId", null, [EnableFolding=true])
in
    Result"""
    assert m_sources(m) == ("Sql.Database", ["dbo.Sales", "dbo.Region"])


def test_query_option_in_m():
    m = 'let Source = Sql.Database("srv", "db", [Query="select * from ""sales"".""Orders"""]) in Source'
    assert m_sources(m) == ("Sql.Database", ["sales.Orders"])


def test_unparseable_native_query_keeps_placeholder():
    m = 'let Source = Sql.Database("srv", "db", [Query="EXEC dbo.GetSales @x = " & Text.From(p)]) in Source'
    connector, objects = m_sources(m)
    assert connector == "Sql.Database"
    assert objects in ([NATIVE_SQL], [])  # a procedure call names no table


def test_name_navigation_on_single_database():
    m = """let
    Source = Sql.Database(#"ConnectionString", "gold"),
    nav = Source{[Name="vw_dim_property"]}[Data]
in
    nav"""
    assert m_sources(m) == ("Sql.Database", ["vw_dim_property"])


def test_name_navigation_ignored_for_multi_database_connector():
    # Sql.Databases: the first Name is the database, the object comes from Schema/Item
    m = """let
    Source = Sql.Databases("srv"),
    db = Source{[Name="gold"]}[Data],
    tbl = db{[Schema="dbo",Item="DimCustomer"]}[Data]
in
    tbl"""
    assert m_sources(m) == ("Sql.Databases", ["dbo.DimCustomer"])


def test_lakehouse_navigation():
    m = """let
    Source = Lakehouse.Contents(null),
    ws = Source{[workspaceId="abc"]}[Data],
    lh = ws{[lakehouseId="def"]}[Data],
    tbl = lh{[Id="dim_date",ItemKind="Table"]}[Data]
in
    tbl"""
    assert m_sources(m) == ("Lakehouse.Contents", ["dim_date"])


def test_entered_data():
    m = (
        'let Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i44FAA==", '
        "BinaryEncoding.Base64), Compression.Deflate))) in Source"
    )
    assert m_sources(m) == ("Entered data", [])


def test_references_to_other_queries_are_followed():
    queries = {
        "ConnectionString": '"srv.database.windows.net" meta [IsParameterQuery=true, Type="Text"]',
        "versions_lh": 'let S = Sql.Database(ConnectionString, "gold"), '
        'T = S{[Schema="dbo",Item="vw_versions_lh"]}[Data] in T',
        "versions comments": 'let S = Sql.Database(ConnectionString, "gold"), '
        'T = S{[Schema="dbo",Item="vw_comments"]}[Data] in T',
    }
    m = """let
    Source = Table.Combine({versions_lh, #"versions comments"}),
    Distinct = Table.Distinct(Source)
in
    Distinct"""
    assert m_sources(m, queries, "dim_version") == (
        "Sql.Database",
        ["dbo.vw_versions_lh", "dbo.vw_comments"],
    )


def test_own_step_and_self_are_not_references():
    queries = {
        "Source": 'let S = Sql.Database("a", "b"), T = S{[Schema="x",Item="wrong"]}[Data] in T',
        "dim_a": "let Source = dim_a in Source",
    }
    m = 'let Source = Csv.Document(File.Contents("c:/a.csv")) in Source'
    assert m_sources(m, queries, "dim_a") == ("Csv.Document", [])
    assert m_sources(queries["dim_a"], queries, "dim_a") == ("M", [])


def test_circular_references_stop():
    queries = {"a": "let x = b in x", "b": "let y = a in y"}
    assert m_sources(queries["a"], queries, "a") == ("M", [])


def test_directquery_sql_partition():
    partition = Partition(
        table="Sales",
        name="Sales",
        source_type="query",
        expression="SELECT * FROM dbo.Sales s JOIN dbo.Product p ON p.Id = s.ProductId",
    )
    assert partition.source_summary() == ("SQL query", "dbo.Sales, dbo.Product")
