"""Tests for the pipeline (pipeline.py) and the command line (cli.py)."""

import json
import sys

import pytest

from pbixtractor.cli import main
from pbixtractor.pipeline import (
    ExtractionOptions,
    find_model_for_report,
    report_name,
    run_extraction,
)
from pbixtractor.utils import is_excel_open_with_file

from .sample_layout import write_sample_pbix
from .sample_pbir import write_sample_pbip
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
    assert result.files["tsv"] == sample / "out" / "documentation.tsv"
    assert result.files["tsv"].read_text(encoding="utf-8").startswith("Object\tName\t")
    assert result.report_json["report"]["name"] == "Sample"
    # The Python BPA runs on every run, also without Tabular Editor
    quality = result.report_json["quality"]
    assert quality and any(
        q["rule"] == "[Error Prevention] Relationship columns should be of the same data type"
        for q in quality
    )
    assert result.documentation.bpa_not_checked  # the rules needing statistics
    assert steps == sorted(steps) and steps[-1] == 1.0
    if result.status == "warnings":
        assert result.logs and "log" in result.files
    # Exact dependencies from the Python DAX analysis, also without Tabular Editor
    assert result.report_json["dependencies_exact"] is True
    assert result.documentation.depends_on("Sales", "Dynamic") == ["Sales[Total Amount]"]
    assert result.documentation.depends_on("Sales", "Is Big") == ["Sales[Amount]"]


def test_a_crash_in_the_dax_analysis_falls_back_to_text_matching(sample, monkeypatch):
    def broken(model):
        raise ValueError("bug")

    monkeypatch.setattr("pbixtractor.pipeline.model_dependencies", broken)
    result = run_extraction(ExtractionOptions(
        report_path=sample / "Sample.pbix", model_path=sample / "Sample.bim",
        output_dir=sample / "out", tabular_editor_analysis=False, write_log_file=False,
    ))
    assert result.status == "warnings", result.message
    assert "DAX dependency analysis failed" in result.logs
    assert result.report_json["dependencies_exact"] is False


def _detail_report(folder):
    """A second report on the sample model (PBIP) with one extra card using Sales[Dynamic]."""
    write_sample_pbip(folder, "Detail")
    card = {
        "name": "card9",
        "position": {"x": 0, "y": 0, "z": 0},
        "visual": {
            "visualType": "card",
            "query": {"queryState": {"Values": {"projections": [{
                "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "Sales"}},
                                      "Property": "Dynamic"}},
                "queryRef": "Sales.Dynamic",
                "nativeQueryRef": "Dynamic",
            }]}}},
        },
    }
    target = folder / "Detail.Report/definition/pages/ReportSectionA/visuals/card9/visual.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps(card), encoding="utf-8")
    return folder / "Detail.Report"


def test_model_mode_documents_reports_together(sample):
    detail = _detail_report(sample / "detail")
    alone = run_extraction(ExtractionOptions(
        report_path=sample / "Sample.pbix", model_path=sample / "Sample.bim",
        output_dir=sample / "alone", tabular_editor_analysis=False,
    ))
    assert "Sales[Dynamic]" in alone.report_json["unused"]["measures"]

    result = run_extraction(ExtractionOptions(
        report_path=sample / "Sample.pbix", model_path=sample / "Sample.bim",
        output_dir=sample / "model", name="Sample model", tabular_editor_analysis=False,
        extra_reports=[detail],
    ))
    assert result.ok, result.message
    doc = result.report_json
    assert doc["report"]["name"] == "Sample model"
    # Used by the second report only: no longer unused
    assert "Sales[Dynamic]" not in doc["unused"]["measures"]
    pages = [p["name"] for p in doc["report"]["pages"]]
    assert pages == ["Sample › Sales", "Sample › Detail", "Detail › Sales", "Detail › Detail"]
    # Bookmarks keep to their own report: button btn1 of each report uses its own bookmark
    panels = {b["name"]: b for b in doc["report"]["bookmarks"]}
    assert panels["Sample › Panel Open"]["used_by"] == ["Sample › Sales (btn1)"]
    assert panels["Detail › Panel Open"]["used_by"] == ["Detail › Sales (btn1)"]
    assert panels["Sample › Panel Open"]["page"] == "Sample › Sales"
    lineage_pages = {n["id"] for n in doc["lineage"]["nodes"] if n["type"] == "page"}
    assert "page:Detail › Sales" in lineage_pages
    assert result.files["workbook"].name == "Sample model.xlsx"

    # The report is a level of its own: pages, bookmarks and filters say which report
    assert [(r["name"], r["pages"]) for r in doc["reports"]] == [
        ("Sample", ["Sample › Sales", "Sample › Detail"]),
        ("Detail", ["Detail › Sales", "Detail › Detail"]),
    ]
    by_key = {p["name"]: p for p in doc["report"]["pages"]}
    assert (by_key["Detail › Sales"]["report"], by_key["Detail › Sales"]["title"]) == ("Detail", "Sales")
    assert panels["Detail › Panel Open"]["report"] == "Detail"
    # Both reports have the same report-level filter: each keeps its own (it used to merge)
    all_pages = [f["report"] for f in doc["report"]["filters"] if f["level"] == "All Pages"]
    assert sorted(all_pages) == ["Detail", "Sample"]
    nodes = {n["id"]: n for n in doc["lineage"]["nodes"]}
    assert nodes["page:Detail › Sales"]["label"] == "Sales"  # its own title; the report is above
    assert {"source": "report:Detail", "target": "page:Detail › Sales", "type": "contains"} in doc[
        "lineage"
    ]["edges"]


def test_model_mode_keeps_copied_reports_apart(sample):
    """A copied report reuses every visual id: each button must keep its own report's target."""
    detail = _detail_report(sample / "detail")
    button = detail / "definition/pages/ReportSectionA/visuals/btn1/visual.json"
    text = button.read_text(encoding="utf-8-sig")
    assert "'Bookmark1'" in text
    button.write_text(text.replace("'Bookmark1'", "'BookmarkGone'"), encoding="utf-8")

    result = run_extraction(ExtractionOptions(
        report_path=sample / "Sample.pbix", model_path=sample / "Sample.bim",
        output_dir=sample / "model", name="Sample model", tabular_editor_analysis=False,
        extra_reports=[detail],
    ))
    assert result.ok, result.message
    pages = {p["name"]: p for p in result.report_json["report"]["pages"]}
    broken = {
        name: sorted(i["id"] for i in page["items"] if i.get("broken"))
        for name, page in pages.items()
    }
    assert broken["Sample › Sales"] == ["btn4"]
    assert broken["Detail › Sales"] == ["btn1", "btn4"]


def test_not_included_reports_make_the_run_warn(sample):
    result = run_extraction(ExtractionOptions(
        report_path=sample / "Sample.pbix", model_path=sample / "Sample.bim",
        output_dir=sample / "out", tabular_editor_analysis=False,
        not_included=["Other report (WS): HTTP 403"],
    ))
    assert result.status == "warnings"
    assert "Not included in this documentation" in result.logs
    assert result.report_json["not_included"] == ["Other report (WS): HTTP 403"]


def test_cli_also_documents_local_reports_together(sample, capsys):
    detail = _detail_report(sample / "detail")
    output = sample / "doc"
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", str(sample / "Sample.pbix"), "--also", str(detail), "-o", str(output),
              "--no-tabular-editor", "-q"])
    assert exit_info.value.code == 0, capsys.readouterr().err
    assert (output / "Sample.xlsx").is_file()  # named after the model (Sample.bim)


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
    assert result.status == "error" and "Could not read the model" in result.message


@pytest.mark.skipif(sys.platform != "win32", reason="Windows file locking")
def test_workbook_open_in_excel_is_not_overwritten(sample):
    """Excel opens workbooks without write sharing; the run must stop instead of failing late."""
    import ctypes

    options = ExtractionOptions(
        report_path=sample / "Sample.pbix",
        model_path=sample / "Sample.bim",
        output_dir=sample / "out",
        tabular_editor_analysis=False,
    )
    assert run_extraction(options).ok
    workbook = sample / "out" / "Sample.xlsx"
    assert not is_excel_open_with_file(str(workbook))

    kernel32 = ctypes.windll.kernel32
    kernel32.CreateFileW.restype = ctypes.c_void_p
    generic_read, share_read, open_existing = 0x80000000, 0x1, 3
    handle = kernel32.CreateFileW(str(workbook), generic_read, share_read, None, open_existing, 0, None)
    assert handle not in (None, ctypes.c_void_p(-1).value)
    try:
        assert is_excel_open_with_file(str(workbook))
        result = run_extraction(options)
        assert result.status == "error" and "close Sample.xlsx" in result.message
    finally:
        kernel32.CloseHandle(ctypes.c_void_p(handle))


def test_cli_extract(sample, capsys):
    output = sample / "cli_out"
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", str(sample / "Sample.pbix"), "-o", str(output), "--no-tabular-editor", "-q"])
    assert exit_info.value.code == 0
    assert (output / "Sample.xlsx").is_file()
    assert (output / "Sample_lineage.html").is_file()
    assert "Done" in capsys.readouterr().out


def test_cli_extract_with_custom_rules(sample, capsys):
    rules = sample / "rules.yaml"
    rules.write_text(
        "version: 1\n"
        "rules:\n"
        "  - id: NO_AMOUNT\n"
        "    name: Columns called Amount\n"
        "    severity: Low\n"
        "    scope: Column\n"
        "    conditions:\n"
        "      - {property: name, op: equals, value: amount}\n",
        encoding="utf-8",
    )
    output = sample / "cli_rules"
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", str(sample / "Sample.pbix"), "-o", str(output), "--bpa-rules",
              str(rules), "-q"])
    assert exit_info.value.code == 0
    quality = json.loads((output / "Sample.json").read_text(encoding="utf-8"))["quality"]
    assert {"Columns called Amount"} == {
        q["rule"] for q in quality if q["rule"] == "Columns called Amount"
    }


def test_cli_extract_without_model(tmp_path, capsys):
    write_sample_pbix(tmp_path / "Lonely.pbix")
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", str(tmp_path / "Lonely.pbix")])
    assert exit_info.value.code == 2
    assert "--model" in capsys.readouterr().err


def test_dax_dependencies_are_computed_once_per_run(sample, monkeypatch):
    """D29: one model_dependencies() per run, shared by the documentation and the BPA."""
    from pbixtractor import bpa, pipeline
    from pbixtractor.dax_parser import model_dependencies

    calls = []

    def counted(model):
        calls.append(1)
        return model_dependencies(model)

    monkeypatch.setattr(pipeline, "model_dependencies", counted)
    monkeypatch.setattr(bpa, "model_dependencies", lambda model: pytest.fail("computed again"))
    result = run_extraction(ExtractionOptions(
        report_path=sample / "Sample.pbix", model_path=sample / "Sample.bim",
        output_dir=sample / "out", tabular_editor_analysis=False, write_log_file=False,
    ))  # fmt: skip
    assert result.ok, result.message
    assert calls == [1]
    assert result.documentation.bpa_violations


def test_warning_messages_leave_out_info_notes():
    from pbixtractor.cli import warning_messages

    logs = "INFO: a note\nWARNING: first\nERROR: second\n  more of it\nDEBUG: detail\nINFO: x"
    assert warning_messages(logs) == ["WARNING: first", "ERROR: second\n  more of it"]
    assert warning_messages("") == []


def test_cli_counts_and_logs_only_warnings(sample, capsys, monkeypatch):
    from pbixtractor import pipeline

    def service_statistics(model, options, progress):
        pipeline.logger.info("A note that is no warning")
        pipeline.logger.warning("A real warning")
        return None

    monkeypatch.setattr(pipeline, "_service_statistics", service_statistics)
    output = sample / "cli_warn"
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", str(sample / "Sample.pbix"), "-o", str(output)])
    assert exit_info.value.code == 0
    out = capsys.readouterr().out
    listed = out.split(" warning(s):\n", 1)
    count = int(listed[0].rsplit("\n", 1)[-1])
    assert count == len([line for line in listed[1].splitlines() if line.startswith("  WARNING")])
    assert "WARNING: A real warning" in out
    assert "A note that is no warning" not in out
    (log_file,) = (output / "logs").glob("log_data_*.txt")
    text = log_file.read_text(encoding="utf-8")
    assert "A real warning" in text and "A note that is no warning" not in text


# ---------------------------------------------------------------- old Tabular Editor options


@pytest.mark.parametrize(
    "old_option", [{"tabular_editor_analysis": False}, {"tabular_editor_tsv": True}]
)
def test_old_tabular_editor_options_only_log_a_note(sample, old_option):
    from pbixtractor.pipeline import TABULAR_EDITOR_NOTE

    def run(folder, **options):
        return run_extraction(ExtractionOptions(
            report_path=sample / "Sample.pbix", model_path=sample / "Sample.bim",
            output_dir=sample / folder, write_log_file=False, **options,
        ))  # fmt: skip

    default, old = run("default"), run("old", **old_option)
    assert old.status == default.status and old.ok
    assert old.logs.count(TABULAR_EDITOR_NOTE) == 1 and TABULAR_EDITOR_NOTE not in default.logs
    assert f"INFO: {TABULAR_EDITOR_NOTE}" in old.logs
    # Nothing else changes: the same warnings, and the TSV is always written offline
    assert old.logs.replace(f"INFO: {TABULAR_EDITOR_NOTE}\n", "") == default.logs
    assert old.files["tsv"].read_bytes() == default.files["tsv"].read_bytes()


@pytest.mark.parametrize("old_option", ["--no-tabular-editor", "--tabular-editor-tsv"])
def test_cli_accepts_the_old_tabular_editor_options_with_a_note(sample, capsys, old_option):
    from pbixtractor.pipeline import TABULAR_EDITOR_NOTE

    with pytest.raises(SystemExit) as exit_info:
        main(["extract", str(sample / "Sample.pbix"), "-o", str(sample / "old"), old_option, "-q"])
    assert exit_info.value.code == 0
    captured = capsys.readouterr()
    assert captured.err.count(TABULAR_EDITOR_NOTE) == 1
    assert (sample / "old" / "documentation.tsv").is_file()


@pytest.mark.parametrize("command", [["extract"], ["catalog", "add"], []])
def test_help_mentions_no_tabular_editor(capsys, command):
    with pytest.raises(SystemExit):
        main(command + ["--help"])
    text = capsys.readouterr().out
    assert "tabular" not in text.lower() and "desktop" not in text.lower()
