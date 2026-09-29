"""Tests for the pipeline (pipeline.py) and the command line (cli.py)."""

import json

import pytest

from pbixtractor.cli import main
from pbixtractor.pipeline import (
    ExtractionOptions,
    find_model_for_report,
    report_name,
    run_extraction,
)

from .sample_layout import write_sample_pbix
from .test_semantic_model import BIM


@pytest.fixture
def sample(tmp_path):
    """Sample.pbix with Sample.bim next to it."""
    write_sample_pbix(tmp_path / "Sample.pbix")
    (tmp_path / "Sample.bim").write_text(json.dumps(BIM), encoding="utf-8")
    return tmp_path


def test_report_name():
    assert report_name("C:/x/Sales.pbix") == "Sales"
    assert report_name("C:/x/Sales.pbip") == "Sales"
    assert report_name("C:/x/Sales.Report") == "Sales"


def test_find_model_for_report(tmp_path):
    assert find_model_for_report(tmp_path / "Sales.pbix") is None

    pbip_model = tmp_path / "Sales.SemanticModel" / "model.bim"
    pbip_model.parent.mkdir()
    pbip_model.write_text("{}", encoding="utf-8")
    assert find_model_for_report(tmp_path / "Sales.pbip") == pbip_model
    assert find_model_for_report(tmp_path / "Sales.Report") == pbip_model

    # A .bim next to the report wins
    (tmp_path / "Sales.bim").write_text("{}", encoding="utf-8")
    assert find_model_for_report(tmp_path / "Sales.pbix") == tmp_path / "Sales.bim"


def test_run_extraction(sample):
    steps = []
    options = ExtractionOptions(
        report_path=sample / "Sample.pbix",
        model_path=sample / "Sample.bim",
        output_dir=sample / "out",
        tabular_editor_analysis=False,
    )
    result = run_extraction(options, progress=lambda step, fraction: steps.append(fraction))

    assert result.ok, result.message
    assert set(result.files) >= {"workbook", "data_workbook", "json", "lineage", "graph"}
    assert all(path.is_file() for path in result.files.values())
    assert result.files["workbook"].name == "Sample.xlsx"
    assert result.report_json["report"]["name"] == "Sample"
    assert result.report_json["quality"] is None  # BPA not run
    assert steps == sorted(steps) and steps[-1] == 1.0
    if result.status == "warnings":
        assert result.logs and "log" in result.files


def test_run_extraction_errors(sample):
    missing_report = run_extraction(
        ExtractionOptions(
            report_path=sample / "Missing.pbix",
            model_path=sample / "Sample.bim",
            output_dir=sample / "out",
            tabular_editor_analysis=False,
        )
    )
    assert missing_report.status == "error" and "Report not found" in missing_report.message

    bad_model = sample / "Bad.bim"
    bad_model.write_text("not json", encoding="utf-8")
    result = run_extraction(
        ExtractionOptions(
            report_path=sample / "Sample.pbix",
            model_path=bad_model,
            output_dir=sample / "out",
            tabular_editor_analysis=False,
        )
    )
    assert result.status == "error" and "model file" in result.message


def test_cli_extract(sample, capsys):
    output = sample / "cli_out"
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", str(sample / "Sample.pbix"), "-o", str(output), "--no-tabular-editor", "-q"])
    assert exit_info.value.code == 0
    assert (output / "Sample.xlsx").is_file()
    assert (output / "Sample_lineage.html").is_file()
    assert "Done" in capsys.readouterr().out


def test_cli_extract_without_model(tmp_path, capsys):
    write_sample_pbix(tmp_path / "Lonely.pbix")
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", str(tmp_path / "Lonely.pbix")])
    assert exit_info.value.code == 2
    assert "--model" in capsys.readouterr().err
