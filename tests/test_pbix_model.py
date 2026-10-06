"""Tests for pbix_model.py: the model inside a .pbix (metadata.sqlitedb -> TMSL), its statistics,
and live-connected reports. The metadata database is built here with the TOM tables and columns
Power BI uses, so no real .pbix is needed (a real one: PBIXTRACTOR_SAMPLE_PBIX, see the end)."""

import json
import os
import sqlite3
import zipfile
from pathlib import Path

import pytest

from pbixtractor import pbix_model
from pbixtractor.pbix_model import (
    PbixModel,
    database_from_metadata,
    has_embedded_model,
    live_connection,
    statistics_from_metadata,
)
from pbixtractor.semantic_model import parse_model

SCHEMA = {
    "Model": ["ID", "Name", "Culture", "DefaultMode"],
    "Table": ["ID", "Name", "Description", "IsHidden", "DataCategory", "SystemFlags", "IsPrivate",
              "ShowAsVariationsOnly", "CalculationGroupID"],
    "Column": ["ID", "TableID", "ExplicitName", "InferredName", "ExplicitDataType", "InferredDataType",
               "Type", "SourceColumn", "Expression", "FormatString", "IsHidden", "IsKey", "DataCategory",
               "DisplayFolder", "Description", "SummarizeBy", "SortByColumnID", "IsAvailableInMDX",
               "ColumnStorageID"],
    "Measure": ["ID", "TableID", "Name", "Expression", "FormatString", "DisplayFolder", "Description",
                "IsHidden", "DataType", "FormatStringDefinitionID"],
    "FormatStringDefinition": ["ID", "Expression"],
    "Partition": ["ID", "TableID", "Name", "Type", "Mode", "QueryDefinition", "SystemFlags",
                  "DataSourceID", "ExpressionSourceID", "SchemaName"],
    "Relationship": ["ID", "Name", "FromTableID", "FromColumnID", "ToTableID", "ToColumnID", "IsActive",
                     "FromCardinality", "ToCardinality", "CrossFilteringBehavior",
                     "SecurityFilteringBehavior", "JoinOnDateBehavior"],
    "Hierarchy": ["ID", "TableID", "Name", "IsHidden", "DisplayFolder", "Description"],
    "Level": ["ID", "HierarchyID", "Ordinal", "Name", "ColumnID"],
    "Role": ["ID", "Name", "Description", "ModelPermission"],
    "TablePermission": ["ID", "RoleID", "TableID", "FilterExpression"],
    "Expression": ["ID", "Name", "Kind", "Expression", "Description"],
    "Annotation": ["ID", "ObjectID", "ObjectType", "Name", "Value"],
    "CalculationGroup": ["ID", "TableID", "Precedence"],
    "CalculationItem": ["ID", "CalculationGroupID", "Name", "Expression", "Ordinal",
                        "FormatStringDefinitionID", "Description"],
    "ColumnStorage": ["ID", "Statistics_DistinctStates", "Statistics_RowCount"],
    "DictionaryStorage": ["ID", "ColumnStorageID", "Type", "Size"],
}

ROWS = {
    "Model": [(1, "Model", "sv-SE", 0)],
    "Table": [
        (10, "Sales", None, 0, None, 0, 0, 0, None),
        (20, "Dates", "Calendar", 0, "Time", 0, 0, 0, None),
        (30, "H$Sales$Amount", None, 1, None, 1, 0, 0, None),  # internal storage table: skipped
        (40, "Time Calc", None, 0, None, 0, 0, 0, 1),  # a calculation group
        (50, "Target", None, 0, None, 2, 0, 0, None),  # SystemFlags 2: a calculated table (kept)
    ],
    "Column": [
        # id, table, name, inferred, explicit type, inferred type, type, source, expression,
        # format, hidden, key, category, folder, description, summarize, sort-by, mdx, storage
        (100, 10, "RowNumber-1", None, 6, 19, 3, None, None, None, 1, 1, None, None, None, 1, None, 1, 900),
        (101, 10, "Amount", None, 8, 19, 1, "amount", None, "#,0.00", 0, 0, None, "Money", None, 3, None, 1, 901),
        (102, 10, "Date Key", None, 6, 19, 1, "date_key", None, None, 1, 0, None, None, None, 2, None, 0, 902),
        (103, 10, "Is Big", None, 1, 11, 2, None, "[Amount] > 100", None, 0, 0, None, None, None, 1, None, 1, 903),
        (200, 20, "RowNumber-2", None, 6, 19, 3, None, None, None, 1, 1, None, None, None, 1, None, 1, 910),
        (201, 20, "Date Key", None, 6, 19, 1, "date_key", None, None, 0, 1, None, None, None, 2, None, 1, 911),
        (202, 20, "Month", None, 2, 19, 1, "month", None, None, 0, 0, None, None, "Month name", 1, 203, 1, 912),
        (203, 20, "Month Number", None, 6, 19, 1, "month_no", None, None, 1, 0, None, None, None, 2, None, 1, 913),
        (204, 20, "Year", None, 6, 19, 1, "year", None, None, 0, 0, None, None, None, 2, None, 1, 914),
        (300, 30, "Amount", None, 8, 19, 1, None, None, None, 1, 0, None, None, None, 1, None, 1, None),
        (400, 50, "Target", None, 2, 19, 4, "[Target]", None, None, 0, 0, None, None, None, 1, None, 1, None),
    ],
    "Measure": [
        (500, 10, "Total", "SUM ( Sales[Amount] )", "#,0", "KPIs", "All sales", 0, 8, None),
        (501, 10, "Dynamic", "[Total] * 2", None, None, None, 1, 10, 600),
    ],
    "FormatStringDefinition": [(600, '"#,0." & REPT("0", 2)')],
    "Partition": [
        (700, 10, "Sales", 4, 0, 'let Source = Sql.Database("srv", "db") in Source', 0, None, None, None),
        (701, 20, "Dates", 4, 1, "let Source = ... in Source", 0, None, None, None),
        (702, 30, "H$", 3, 2, None, 1, None, None, None),  # system partition: skipped
        (703, 40, "Time Calc", 7, 0, None, 0, None, None, None),
        (704, 50, "Target", 2, 0, '{ ("Budget", NAMEOF([Total]), 0) }', 2, None, None, None),
    ],
    "Relationship": [
        (800, "r1", 10, 102, 20, 201, 1, 2, 1, 2, 1, 1),
        (801, "r2", 10, 102, 20, 201, 0, 2, 2, 1, 1, 2),  # inactive many-to-many, date part only
    ],
    "Hierarchy": [(810, 20, "Calendar", 0, None, None)],
    "Level": [(820, 810, 1, "Month", 202), (821, 810, 0, "Year", 204)],  # stored out of order
    "Role": [(830, "Nordics", None, 2)],
    "TablePermission": [(831, 830, 10, '[Region] = "Nordics"')],
    "Expression": [(840, "ServerName", 0, '"srv" meta [IsParameterQuery=true]', None)],
    "Annotation": [
        (850, 1, 1, "BestPracticeAnalyzer_IgnoreRules", '{"RuleIDs":["OBJECTS_WITH_NO_DESCRIPTION"]}'),
        (851, 500, 8, "PBI_FormatHint", '{"isGeneralNumber":true}'),
    ],
    "CalculationGroup": [(1, 40, 5)],
    "CalculationItem": [(860, 1, "YTD", "TOTALYTD ( SELECTEDMEASURE (), Dates[Date] )", 1, None, None),
                        (861, 1, "Current", "SELECTEDMEASURE ()", 0, 600, None)],
    "ColumnStorage": [(900, 1000, 1000), (901, 950, 1000), (902, 4000, 1000), (903, 2, 1000),
                      (910, 365, 365), (911, 9000, 365), (912, 12, 365), (913, 12, 365), (914, 1, 365)],
    # Type 1 = hash dictionary (DistinctStates is a count), 2 = value encoding (a value range)
    "DictionaryStorage": [(1, 901, 1, 4000), (2, 902, 2, 128), (3, 903, 1, 64), (4, 911, 2, 128),
                          (5, 912, 1, 300), (6, 913, 2, 128), (7, 914, 2, 128)],
}


@pytest.fixture
def metadata():
    db = sqlite3.connect(":memory:")
    for table, columns in SCHEMA.items():
        db.execute(f'CREATE TABLE "{table}" ({", ".join(f"[{c}]" for c in columns)})')
        for row in ROWS.get(table, []):
            db.execute(f'INSERT INTO "{table}" VALUES ({", ".join("?" * len(row))})', row)
    yield db
    db.close()


def test_the_metadata_becomes_a_tmsl_document(metadata):
    database = database_from_metadata(metadata, name="Sales report")
    assert database["name"] == "Sales report" and database["compatibilityLevel"] >= 1550
    model = database["model"]
    # The internal H$ table goes; a calculated table (field parameter, SystemFlags 2) stays
    assert [t["name"] for t in model["tables"]] == ["Sales", "Dates", "Time Calc", "Target"]
    target = model["tables"][3]
    assert target["partitions"][0]["source"] == {"type": "calculated",
                                                 "expression": '{ ("Budget", NAMEOF([Total]), 0) }'}
    assert target["columns"] == [{"name": "Target", "dataType": "string", "type": "calculatedTableColumn",
                                  "sourceColumn": "[Target]"}]
    assert model["annotations"][0]["name"] == "BestPracticeAnalyzer_IgnoreRules"  # BPA reads it
    sales = model["tables"][0]
    columns = {c["name"]: c for c in sales["columns"]}
    assert "RowNumber-1" not in columns
    assert columns["Amount"] == {"name": "Amount", "dataType": "double", "sourceColumn": "amount",
                                 "formatString": "#,0.00", "displayFolder": "Money", "summarizeBy": "sum"}
    assert columns["Date Key"]["isAvailableInMdx"] is False and columns["Date Key"]["summarizeBy"] == "none"
    calculated = columns["Is Big"]
    assert calculated["type"] == "calculated" and calculated["dataType"] == "boolean"
    assert calculated["isDataTypeInferred"] is True and calculated["expression"] == "[Amount] > 100"
    total = next(m for m in sales["measures"] if m["name"] == "Total")
    assert total["annotations"][0]["name"] == "PBI_FormatHint"
    assert sales["partitions"] == [{"name": "Sales", "mode": "import", "source": {
        "type": "m", "expression": 'let Source = Sql.Database("srv", "db") in Source'}}]
    dates = model["tables"][1]
    assert dates["partitions"][0]["mode"] == "directQuery"
    assert dates["hierarchies"][0]["levels"][0]["name"] == "Year"  # by ordinal, not storage order
    calc = model["tables"][2]["calculationGroup"]
    assert calc["precedence"] == 5 and [i["name"] for i in calc["calculationItems"]] == ["Current", "YTD"]
    first, second = model["relationships"]
    assert first == {"name": "r1", "fromTable": "Sales", "fromColumn": "Date Key", "toTable": "Dates",
                     "toColumn": "Date Key", "crossFilteringBehavior": "bothDirections"}
    assert second["isActive"] is False and second["toCardinality"] == "many"
    assert second["joinOnDateBehavior"] == "datePartOnly"
    assert model["roles"] == [{"name": "Nordics", "modelPermission": "read", "tablePermissions": [
        {"name": "Sales", "filterExpression": '[Region] = "Nordics"'}]}]
    assert model["expressions"][0] == {"name": "ServerName", "kind": "m",
                                       "expression": '"srv" meta [IsParameterQuery=true]'}


def test_the_existing_model_reader_reads_it(metadata):
    model = parse_model(database_from_metadata(metadata))
    sales = model.tables[0]
    assert {c.name for c in sales.columns} == {"Amount", "Date Key", "Is Big"}
    dynamic = next(m for m in sales.measures if m.name == "Dynamic")
    assert dynamic.format_string_expression == '"#,0." & REPT("0", 2)' and dynamic.is_hidden
    assert next(c for c in model.tables[1].columns if c.name == "Month").sort_by_column == "Month Number"
    assert model.roles[0].table_filters == {"Sales": '[Region] = "Nordics"'}
    assert model.partition_mode(model.tables[1].partitions[0]) == "directQuery"
    assert model.culture == "sv-SE"


def test_statistics_count_rows_and_only_real_distinct_values(metadata):
    sizes = {("Sales", "Amount"): (4000, 2000, 500), ("Dates", "Month"): (300, 10, 0)}
    stats = statistics_from_metadata(metadata, sizes)
    assert stats.table_rows == {"Sales": 1000, "Dates": 365}  # from the row-number columns
    assert stats.columns[("Sales", "Amount")].distinct_values == 950
    assert stats.columns[("Sales", "Amount")].total_size == 6500
    # Value-encoded: VertiPaq stores a range there (4000 "distinct" keys on 1000 rows) - not shown
    assert stats.columns[("Sales", "Date Key")].distinct_values is None
    assert stats.columns[("Dates", "Month")].distinct_values == 12
    assert stats.table_sizes == {"Sales": 6500, "Dates": 310}
    assert stats.measure_types == {("Sales", "Total"): "Double", ("Sales", "Dynamic"): "Decimal"}


def _pbix(path: Path, **entries) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in entries.items():
            archive.writestr(name, content)
    return path


def test_live_connected_reports_name_their_published_model(tmp_path):
    model_id = "11111111-2222-3333-4444-555555555555"
    connections = {"Version": 3, "Connections": [{
        "Name": "EntityDataSource", "ConnectionType": "pbiServiceLive", "PbiModelDatabaseName": model_id,
        "ConnectionString": "Data Source=pbiazure://api.powerbi.com;Initial Catalog=x"}],
        "RemoteArtifacts": [{"DatasetId": model_id, "ReportId": "r"}]}
    thin = _pbix(tmp_path / "Thin.pbix", Connections=json.dumps(connections), Layout="{}")
    assert not has_embedded_model(thin)
    assert live_connection(thin) == pbix_model.LiveConnection(model_id, None)
    # Newer files name the workspace in the connection string
    connections["Connections"][0]["ConnectionString"] = (
        "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Sales%20WS;semanticmodelid=" + model_id)
    thin = _pbix(tmp_path / "Thin2.pbix", Connections=json.dumps(connections))
    assert live_connection(thin) == pbix_model.LiveConnection(model_id, "Sales WS")
    thick = _pbix(tmp_path / "Thick.pbix", DataModel=b"\x00", Connections='{"Connections": []}')
    assert has_embedded_model(thick) and live_connection(thick) is None


def test_a_pbix_with_its_own_model_needs_no_bim(tmp_path, monkeypatch):
    """The pipeline reads the model inside the .pbix, hands Tabular Editor a generated .bim and
    uses the statistics stored in the file."""
    from pbixtractor import pipeline
    from pbixtractor.live_model import LiveStatistics

    from .sample_layout import write_sample_pbix
    from .test_semantic_model import BIM

    report = tmp_path / "Sample.pbix"
    write_sample_pbix(report)
    with zipfile.ZipFile(report, "a") as archive:
        archive.writestr("DataModel", b"\x00")  # PBIXRay is replaced below
    (tmp_path / "Sample.bim").write_text("not used")  # the report's own model wins
    assert pipeline.find_model_for_report(report) == report

    stats = LiveStatistics(table_rows={"Sales": 42}, tables={"Sales"})
    monkeypatch.setattr(pipeline, "read_pbix_model", lambda path: PbixModel(BIM, stats))
    result = pipeline.run_extraction(pipeline.ExtractionOptions(
        report_path=report, model_path=report, output_dir=tmp_path / "out", tabular_editor_analysis=False))
    assert result.ok, result.message
    assert json.loads(result.files["model"].read_text(encoding="utf-8")) == BIM
    assert result.files["model"].name == "Sample_model.bim"  # never "Sample.bim" next to it
    sales = next(t for t in result.report_json["model"]["tables"] if t["name"] == "Sales")
    assert sales["rows"] == 42


@pytest.mark.skipif(
    not os.environ.get("PBIXTRACTOR_SAMPLE_PBIX"), reason="set PBIXTRACTOR_SAMPLE_PBIX to a real .pbix"
)
def test_a_real_pbix():
    path = Path(os.environ["PBIXTRACTOR_SAMPLE_PBIX"])
    if not has_embedded_model(path):
        pytest.skip("live-connected report")
    pbix = pbix_model.read_pbix_model(path)
    model = parse_model(pbix.database)
    assert model.tables and pbix.statistics.table_rows
