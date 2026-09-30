"""Tests for the catalog folder (catalog.py) and its use from the pipeline and CLI."""

import json
import shutil

import pytest

from pbixtractor.catalog import (
    list_entries,
    rebuild_catalog,
    remove_from_catalog,
    source_identity,
)
from pbixtractor.cli import main
from pbixtractor.pipeline import ExtractionOptions, run_extraction

from .sample_layout import write_sample_pbix
from .test_pipeline import _detail_report
from .test_semantic_model import BIM


@pytest.fixture
def sample(tmp_path):
    write_sample_pbix(tmp_path / "Sample.pbix")
    (tmp_path / "Sample.bim").write_text(json.dumps(BIM), encoding="utf-8")
    return tmp_path


def _run(sample, catalog, output="out", **kwargs):
    result = run_extraction(
        ExtractionOptions(
            report_path=sample / "Sample.pbix",
            model_path=kwargs.pop("model_path", sample / "Sample.bim"),
            output_dir=sample / output,
            tabular_editor_analysis=False,
            catalog_dir=catalog,
            **kwargs,
        )
    )
    assert result.ok, result.message
    return result


def test_run_adds_a_slim_entry(sample):
    catalog = sample / "catalog"
    result = _run(sample, catalog)
    assert result.files["catalog"] == catalog / "catalog.html"

    (entry,) = list_entries(catalog)
    assert entry["key"].startswith("file-sample-") and entry["name"] == "Sample"
    assert entry["source"]["kind"] == "file"
    assert [r["name"] for r in entry["reports"]] == ["Sample"]
    assert [p["name"] for p in entry["reports"][0]["pages"]] == ["Sales", "Detail"]
    # Where things are used: visual fields and filters
    assert entry["usage"]["Sales[Amount]"] == ["Sales"]
    assert "Sales[Total Amount]" in entry["depends_on"]["Sales[Dynamic]"]
    sales = next(t for t in entry["tables"] if t["name"] == "Sales")
    dynamic = next(m for m in sales["measures"] if m["name"] == "Dynamic")
    assert dynamic["unused"] and dynamic["expression"]
    # Slim: no quality findings, bookmarks or partitions
    text = json.dumps(entry)
    assert "bookmarks" not in text and "partitions" not in text and "quality" not in text
    # The full documentation is copied next to it and linked relatively
    assert entry["links"]["lineage"] == f"models/{entry['key']}/Sample_lineage.html"
    assert (catalog / entry["links"]["lineage"]).is_file()
    assert (catalog / entry["links"]["workbook"]).is_file()


def test_same_model_replaces_its_entry_and_models_share_sources(sample):
    catalog = sample / "catalog"
    _run(sample, catalog)
    _run(sample, catalog, output="again")
    assert len(list_entries(catalog)) == 1

    other = sample / "other"
    other.mkdir()
    shutil.copy(sample / "Sample.bim", other / "Finance.bim")
    _run(sample, catalog, output="finance", model_path=other / "Finance.bim")
    entries = list_entries(catalog)
    assert [e["name"] for e in entries] == ["Finance", "Sample"]

    data = json.loads((catalog / "catalog.json").read_text(encoding="utf-8"))
    loaders = [e["name"] for e in data["entries"] for t in e["tables"] if "gold.dim_date" in t["sources"]]
    assert loaders == ["Finance", "Sample"]  # one source, two models: what breaks if it changes

    assert remove_from_catalog(catalog, entries[0]["key"])
    assert [e["name"] for e in list_entries(catalog)] == ["Sample"]
    assert not (catalog / "models" / entries[0]["key"]).exists()
    assert not remove_from_catalog(catalog, "no-such-key")


def test_model_mode_entry_lists_each_report(sample):
    catalog = sample / "catalog"
    detail = _detail_report(sample / "detail")
    _run(sample, catalog, name="Sample", extra_reports=[detail])
    (entry,) = list_entries(catalog)
    reports = {r["name"]: [p["name"] for p in r["pages"]] for r in entry["reports"]}
    assert reports == {"Sample": ["Sales", "Detail"], "Detail": ["Sales", "Detail"]}
    assert entry["usage"]["Sales[Dynamic]"] == ["Detail › Sales"]  # page ids stay unique
    dynamic = next(
        m for t in entry["tables"] if t["name"] == "Sales" for m in t["measures"] if m["name"] == "Dynamic"
    )
    assert not dynamic["unused"]


def test_source_identity_from_fabric_and_devops_downloads(tmp_path):
    report = tmp_path / "Sales.Report"
    report.mkdir()
    model = tmp_path / "Sales Model.SemanticModel"
    (tmp_path / "Sales.fabric_source.json").write_text(json.dumps({"semantic_model": {
        "id": "abc-123", "name": "Sales Model", "workspace": "Prod"}}), encoding="utf-8")
    fabric = source_identity(report, model, "Sales Model")
    assert (fabric["key"], fabric["kind"]) == ("fabric-abc-123", "fabric")
    assert fabric["label"] == "Fabric · Prod / Sales Model"

    (tmp_path / "Sales.fabric_source.json").unlink()
    (tmp_path / "Sales.devops_source.json").write_text(json.dumps({
        "project": "BI Team", "repository": "reports", "semantic_model": "/Reports/Sales Model.SemanticModel",
        "version": "main"}), encoding="utf-8")
    devops = source_identity(report, model, "Sales Model")
    assert devops["kind"] == "devops" and devops["key"].startswith("devops-bi-team-reports-")
    assert "@ main" in devops["label"]


def test_crafted_source_file_cannot_delete_outside_the_catalog(sample):
    """A <report>.fabric_source.json next to an untrusted report must not steer the catalog key
    outside the catalog folder (the audit reproduced deleting an arbitrary folder)."""
    victim = sample / "victim"
    victim.mkdir()
    (victim / "keep.txt").write_text("precious")
    (sample / "Sample.fabric_source.json").write_text(json.dumps({"semantic_model": {
        "id": "../../../victim", "name": "Evil", "workspace": "x"}}), encoding="utf-8")
    catalog = sample / "catalog"
    _run(sample, catalog)
    (entry,) = list_entries(catalog)
    assert "/" not in entry["key"] and ".." not in entry["key"]
    assert (victim / "keep.txt").read_text() == "precious"
    assert (catalog / "models" / entry["key"]).is_dir()


def test_catalog_links_are_limited_to_the_models_folder(sample):
    catalog = sample / "catalog"
    _run(sample, catalog)
    html = (catalog / "catalog.html").read_text(encoding="utf-8")
    assert "function safeLink" in html and "^models\\/" in html


def test_catalog_page_embeds_data_safely(sample):
    catalog = sample / "catalog"
    _run(sample, catalog)
    html = (catalog / "catalog.html").read_text(encoding="utf-8")
    assert html.startswith("<!doctype html>")
    # theme switch + data block + code: the data block cannot close the script early
    assert html.count("</script>") == 3
    assert "https://" not in html and "http://" not in html  # fully offline
    start = html.index('id="data">') + len('id="data">')
    data = json.loads(html[start : html.index("</script>", start)])
    assert data["entries"][0]["name"] == "Sample"
    assert rebuild_catalog(catalog) == catalog / "catalog.html"


def test_cli_catalog(sample, capsys):
    catalog = sample / "catalog"
    with pytest.raises(SystemExit) as exit_info:
        main(["extract", str(sample / "Sample.pbix"), "-o", str(sample / "out"), "--catalog",
              str(catalog), "--no-tabular-editor", "-q"])
    assert exit_info.value.code == 0
    capsys.readouterr()  # drop the extract output
    with pytest.raises(SystemExit):
        main(["catalog", "list", str(catalog)])
    listing = capsys.readouterr().out
    key = listing.split()[0]
    assert key.startswith("file-sample-") and "reports: Sample" in listing
    with pytest.raises(SystemExit) as exit_info:
        main(["catalog", "remove", str(catalog), key])
    assert exit_info.value.code == 0 and list_entries(catalog) == []
