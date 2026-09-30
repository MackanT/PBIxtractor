"""Tests for the TMDL reader (tmdl.py) and TMDL models in the pipeline."""

import dataclasses
import json

import pytest

from pbixtractor.pipeline import ExtractionOptions, find_model_for_report, run_extraction
from pbixtractor.semantic_model import model_source, model_to_dataset, parse_model, read_model
from pbixtractor.tabular_editor import export_dependencies, find_tabular_editor
from pbixtractor.tmdl import parse_tmdl, split_qualified, to_tmsl, unquote_name

from .sample_layout import write_sample_pbix
from .sample_tmdl import write_sample_tmdl
from .test_semantic_model import BIM


def _normalised(model) -> dict:
    """Model as a dict, with multi-line text compared line by line (trailing spaces ignored)."""

    def clean(value):
        if isinstance(value, str):
            return "\n".join(line.rstrip() for line in value.strip().splitlines())
        if isinstance(value, dict):
            return {k: clean(v) for k, v in value.items()}
        if isinstance(value, list):
            return [clean(v) for v in value]
        return value

    return clean(dataclasses.asdict(model))


def test_tmdl_gives_the_same_model_as_the_bim(tmp_path):
    """Tabular Editor's TMDL export of the sample model reads exactly like the .bim."""
    from_tmdl = read_model(write_sample_tmdl(tmp_path / "definition"))
    from_bim = parse_model({**BIM, "compatibilityLevel": 1604})
    assert _normalised(from_tmdl) == _normalised(from_bim)
    assert model_to_dataset(from_tmdl).equals(model_to_dataset(from_bim))

    # Spot checks of what the comparison covers
    sales = from_tmdl.tables[0]
    assert [t.name for t in from_tmdl.tables] == ["Sales", "Dates"]  # model order, not file order
    total = next(m for m in sales.measures if m.name == "Total Amount")
    assert total.description == "Sum of\namount"
    dynamic = next(m for m in sales.measures if m.name == "Dynamic")
    assert dynamic.format_string_expression == '"#,0"'
    assert from_tmdl.relationships[0].to_table == "Dates"
    assert from_tmdl.relationships[0].to_column == "Date Key"
    assert from_tmdl.roles[0].table_filters == {"Sales": "Sales[Amount] > 0"}


def test_parse_tmdl_syntax():
    text = (
        "table 'It''s a table'\r\n"
        "\t/// First line\r\n"
        "\t///Second line\r\n"
        "\tmeasure 'Inline' = SUM ( T[A] )\r\n"
        "\t\tformatString: 0.0 \"kr\"\r\n"
        "\t\tisHidden\r\n"
        "\r\n"
        "\tcolumn Fenced = ```\r\n"
        "\t\t\tVAR x = 1\r\n"
        "\r\n"
        "\t\t\tRETURN x\r\n"
        "\t\t\t```\r\n"
        "\t\tdataType: int64\r\n"
        "\t\tisHidden: false\r\n"
        "\r\n"
        "\tcalculationGroup\r\n"
        "\t\tprecedence: 1\r\n"
        "\r\n"
        "\t\tcalculationItem YTD =\r\n"
        "\t\t\t\tCALCULATE (\r\n"
        "\t\t\t\t    SELECTEDMEASURE ()\r\n"
        "\t\t\t\t)\r\n"
        "\r\n"
        "\tannotation Note = {\"a\": 1}\r\n"
    )
    database = to_tmsl(parse_tmdl(text))
    table = database["model"]["tables"][0]
    assert table["name"] == "It's a table"

    measure = table["measures"][0]
    assert measure["expression"] == "SUM ( T[A] )"
    assert measure["description"] == "First line\nSecond line"
    assert measure["formatString"] == '0.0 "kr"'
    assert measure["isHidden"] is True

    column = table["columns"][0]
    assert column["type"] == "calculated"
    assert column["expression"] == "VAR x = 1\n\nRETURN x"
    assert column["isHidden"] is False

    item = table["calculationGroup"]["calculationItems"][0]
    assert item["name"] == "YTD"
    assert item["expression"] == "CALCULATE (\n    SELECTEDMEASURE ()\n)"
    model = parse_model(database)
    assert model.tables[0].calculation_items[0].name == "YTD"


def test_names_and_references():
    assert unquote_name("'Sales Order'") == "Sales Order"
    assert unquote_name("'It''s'") == "It's"
    assert unquote_name("Sales") == "Sales"
    assert split_qualified("'Sales Territory'.SalesTerritoryKey") == (
        "Sales Territory",
        "SalesTerritoryKey",
    )
    assert split_qualified("Sales.'Date Key'") == ("Sales", "Date Key")
    assert split_qualified("'A.B'.'C.D'") == ("A.B", "C.D")


def test_model_source_accepts_the_usual_paths(tmp_path):
    definition = write_sample_tmdl(tmp_path / "Model.SemanticModel" / "definition")
    for path in (
        definition,
        definition.parent,  # the .SemanticModel folder
        definition / "model.tmdl",
        definition / "tables" / "Sales.tmdl",
    ):
        assert model_source(path) == definition
    bim = tmp_path / "Other.SemanticModel" / "model.bim"
    bim.parent.mkdir()
    bim.write_text(json.dumps(BIM), encoding="utf-8")
    assert model_source(bim.parent) == bim
    with pytest.raises(FileNotFoundError):
        model_source(tmp_path)


def test_find_model_for_pbip_report_follows_definition_pbir(tmp_path):
    """The PBIP report points to its model; the folder name need not match the report."""
    definition = write_sample_tmdl(tmp_path / "Shared Model.SemanticModel" / "definition")
    report_folder = tmp_path / "Sales.Report"
    report_folder.mkdir()
    (report_folder / "definition.pbir").write_text(
        json.dumps(
            {"version": "4.0", "datasetReference": {"byPath": {"path": "../Shared Model.SemanticModel"}}}
        ),
        encoding="utf-8",
    )
    (tmp_path / "Sales.pbip").write_text("{}", encoding="utf-8")
    assert find_model_for_report(tmp_path / "Sales.pbip") == definition.resolve()
    assert find_model_for_report(report_folder) == definition.resolve()

    # Without definition.pbir: <name>.SemanticModel next to the report
    other = write_sample_tmdl(tmp_path / "Budget.SemanticModel" / "definition")
    assert find_model_for_report(tmp_path / "Budget.pbip") == other


def test_pipeline_with_tmdl_model(tmp_path):
    write_sample_pbix(tmp_path / "Sample.pbix")
    definition = write_sample_tmdl(tmp_path / "Sample.SemanticModel" / "definition")
    result = run_extraction(
        ExtractionOptions(
            report_path=tmp_path / "Sample.pbix",
            model_path=definition.parent,
            output_dir=tmp_path / "out",
            tabular_editor_analysis=False,
        )
    )
    assert result.ok, result.message
    tables = {t["name"] for t in result.report_json["model"]["tables"]}
    assert tables == {"Sales", "Dates"}
    assert result.report_json["unused"]["measures"] == ["Sales[Dynamic]"]


@pytest.mark.skipif(find_tabular_editor() is None, reason="Tabular Editor 2 not installed")
def test_tabular_editor_reads_tmdl_folder(tmp_path):
    definition = write_sample_tmdl(tmp_path / "definition")
    dependencies = export_dependencies(find_tabular_editor(), definition)
    assert any(
        d.source_name == "Dynamic" and d.target_name == "Total Amount" for d in dependencies
    )
