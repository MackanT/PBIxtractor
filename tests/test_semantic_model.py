"""Tests for reading the semantic model (.bim) without Tabular Editor."""

import json

import pandas as pd
import pytest

from pbixtractor.semantic_model import DATASET_COLUMNS, model_to_dataset, parse_model, read_model

# Anonymised, trimmed-down .bim covering the TMSL features we read
BIM = {
    "name": "SampleModel",
    "compatibilityLevel": 1567,
    "model": {
        "culture": "en-US",
        "tables": [
            {
                "name": "Sales",
                "columns": [
                    {"type": "rowNumber", "name": "RowNumber-2662979B", "dataType": "int64"},
                    {
                        "name": "Date Key",
                        "dataType": "int64",
                        "sourceColumn": "DateKey",
                        "isHidden": True,
                    },
                    {
                        "name": "Amount",
                        "dataType": "decimal",
                        "sourceColumn": "Amount",
                        "formatString": '"TRUE";"TRUE";"FALSE"',
                    },
                    {
                        "type": "calculated",
                        "name": "Is Big",
                        "dataType": "boolean",
                        "expression": ["IF (", "    Sales[Amount] > 100,", "    TRUE()", ")"],
                    },
                ],
                "measures": [
                    {
                        "name": "Total Amount",
                        "expression": "SUM ( Sales[Amount] )",
                        "formatString": "#,0",
                        "displayFolder": "_Totals",
                        "description": ["Sum of", "amount"],
                    },
                    {
                        "name": "Dynamic",
                        "expression": "[Total Amount]",
                        "formatStringDefinition": {"expression": '"#,0"'},
                    },
                ],
                "partitions": [
                    {
                        "name": "Sales-1234",
                        "mode": "import",
                        "source": {
                            "type": "m",
                            "expression": ["let", "  Source = 1", "in", "  Source"],
                        },
                    }
                ],
            },
            {
                "name": "Dates",
                "columns": [
                    {"name": "Date Key", "dataType": "int64", "sourceColumn": "DateKey"},
                    {
                        "name": "Month",
                        "dataType": "string",
                        "sourceColumn": "Month",
                        "sortByColumn": "Month Number",
                    },
                    {"name": "Month Number", "dataType": "int64", "sourceColumn": "MonthNumber"},
                    {"name": "Year Number", "dataType": "int64", "sourceColumn": "Year"},
                ],
                "hierarchies": [
                    {
                        "name": "Date Hierarchy",
                        "levels": [
                            {"name": "Year", "column": "Year Number"},
                            {"name": "Month", "column": "Month"},
                        ],
                    }
                ],
                "partitions": [
                    {
                        "name": "Dates",
                        "mode": "directLake",
                        "source": {
                            "type": "entity",
                            "entityName": "dim_date",
                            "schemaName": "gold",
                            "expressionSource": "DatabaseQuery",
                        },
                    }
                ],
            },
        ],
        "relationships": [
            {
                "name": "rel-1",
                "fromTable": "Sales",
                "fromColumn": "Date Key",
                "toTable": "Dates",
                "toColumn": "Date Key",
            },
            {
                "name": "rel-2",
                "fromTable": "Sales",
                "fromColumn": "Amount",
                "toTable": "Dates",
                "toColumn": "Year Number",
                "toCardinality": "many",
                "crossFilteringBehavior": "bothDirections",
                "isActive": False,
            },
        ],
        "expressions": [
            {"name": "DatabaseQuery", "kind": "m", "expression": ["let", "  x = 1", "in", "  x"]}
        ],
        "roles": [
            {
                "name": "Nordics",
                "modelPermission": "read",
                "tablePermissions": [{"name": "Sales", "filterExpression": "Sales[Amount] > 0"}],
            }
        ],
    },
}


@pytest.fixture
def model():
    return parse_model(BIM)


def test_tables_columns_and_row_number_skipped(model):
    assert [table.name for table in model.tables] == ["Sales", "Dates"]
    assert [column.name for column in model.tables[0].columns] == ["Date Key", "Amount", "Is Big"]


def test_multiline_expressions_are_joined(model):
    calculated = model.tables[0].columns[2]
    assert calculated.kind == "calculated"
    assert calculated.expression == "IF (\n    Sales[Amount] > 100,\n    TRUE()\n)"
    assert model.all_measures[0].description == "Sum of\namount"


def test_measures(model):
    total, dynamic = model.all_measures
    assert (total.table, total.format_string, total.display_folder) == ("Sales", "#,0", "_Totals")
    assert dynamic.format_string_expression == '"#,0"'


def test_relationship_defaults_and_labels(model):
    active, inactive = model.relationships
    assert (active.cardinality_label, active.direction_label, active.is_active) == (
        "Many to One",
        "One Way",
        True,
    )
    assert active.te_name == "'Sales'[Date Key] --> 'Dates'[Date Key]"
    assert (inactive.cardinality_label, inactive.direction_label, inactive.is_active) == (
        "Many to Many",
        "Two Way",
        False,
    )
    assert inactive.te_name == "'Sales'[Amount] <--> 'Dates'[Year Number]"


def test_partitions_expressions_and_roles(model):
    sales_partition = model.tables[0].partitions[0]
    assert (sales_partition.mode, sales_partition.source_type) == ("import", "m")
    assert sales_partition.expression.startswith("let\n")

    direct_lake = model.tables[1].partitions[0]
    assert (direct_lake.entity_name, direct_lake.schema_name, direct_lake.expression_source) == (
        "dim_date",
        "gold",
        "DatabaseQuery",
    )
    assert model.expressions[0].name == "DatabaseQuery"
    assert model.roles[0].table_filters == {"Sales": "Sales[Amount] > 0"}


def test_structurally_used_columns(model):
    assert model.structurally_used_columns() == {
        ("Sales", "Date Key"),  # relationship
        ("Dates", "Date Key"),
        ("Sales", "Amount"),
        ("Dates", "Year Number"),  # relationship + hierarchy level
        ("Dates", "Month Number"),  # sort-by column
        ("Dates", "Month"),  # hierarchy level
    }


def test_dataset_matches_tabular_editor_layout(model):
    dataset = model_to_dataset(model)
    assert list(dataset.columns) == DATASET_COLUMNS

    rows = dataset.set_index("Object")
    assert rows.at["Model.T.Sales.C.Amount", "DataType"] == "Decimal"
    assert rows.at["Model.T.Sales.C.Amount", "FormatString"] == '"TRUE";"TRUE";"FALSE"'
    assert rows.at["Model.T.Sales.M.Total Amount", "Expression"] == "SUM ( Sales[Amount] )"
    assert rows.at["Model.T.Sales.M.Total Amount", "DataType"] == "Unknown"
    assert rows.at["Relationship.rel-1", "Name"] == "'Sales'[Date Key] --> 'Dates'[Date Key]"
    assert "Model.T.Dates.H.Date Hierarchy.Year" in rows.index
    assert "Model.T.Sales.P.Sales-1234" in rows.index
    # Empty cells are NaN, as when pandas reads the TSV
    assert pd.isna(rows.at["Model.T.Sales", "Description"])


def test_read_model_from_file_and_folder(tmp_path):
    bim = tmp_path / "model.bim"
    bim.write_text(json.dumps(BIM), encoding="utf-8-sig")  # Power BI writes a BOM

    assert read_model(bim).name == "SampleModel"
    assert read_model(tmp_path).name == "SampleModel"  # e.g. a <name>.SemanticModel folder


def test_tmdl_folder_gives_clear_error(tmp_path):
    (tmp_path / "definition").mkdir()
    with pytest.raises(NotImplementedError, match="TMDL"):
        read_model(tmp_path)
