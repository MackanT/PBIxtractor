# PBIxtractor

**Power BI Report Documentation Generator**

PBIxtractor documents Power BI reports (.pbix, PBIP/PBIR) and their semantic models (.bim, TMDL) -
from local files, a Fabric workspace or an Azure DevOps repository - as Excel workbooks, JSON and an
offline HTML lineage viewer, with:
- Color-coded DAX formulas (syntax highlighting)
- Relationship diagrams
- Visual inventory per page
- Filter documentation
- Unused measure/column detection
- Dependency tracking

## Features

- 📊 **Complete Visual Inventory** - Documents all charts, slicers, tables, and buttons
- 🔍 **DAX Analysis** - Color-coded syntax highlighting for measures and calculated columns
- 📈 **Relationship Diagrams** - NetworkX-powered visualization of table relationships
- 🎯 **Filter Documentation** - Page-level and visual-level filter tracking
- 🔗 **Dependency Tracking** - Shows which measures use which columns/tables
- ⚡ **Tabular Editor Integration** - Leverages TE2 for deep semantic model analysis

## Installation

### Using uv (Recommended)

```powershell
# Clone the repository
git clone https://github.com/MackanT/PBIxtractor.git
cd PBIxtractor

# Create the environment (with the test/lint tools)
uv sync --extra dev
uv run --frozen pbixtractor
```

### Using pip

```powershell
pip install -e .
```

## Usage

### Web interface

```powershell
# Start the web UI on http://localhost:8081 (opens a browser tab)
pbixtractor

# Other port / no browser
pbixtractor web --port 8090 --no-browser
```

Drop a report (`.pbix`) on the page or click + to add it - several reports with their one
`.bim` are documented together; reports on different models get one documentation each, all
added to the catalog. A PBIP project (`.pbip` or a `.Report` folder) is chosen under "Use files
on this PC instead"; or pick a report from a Fabric workspace or an Azure DevOps repository.
Fabric's "Whole workspaces" and Azure DevOps' "Whole repository" list the semantic models found
("List models"); the ones you tick are documented - one documentation per model, with all its
reports - into one searchable catalog. The model is found automatically:
the model inside the `.pbix` itself (no `.bim` needed), else `<name>.bim` next to the report, or a
PBIP project's semantic model as `model.bim` or TMDL
(the `definition` folder newer Power BI Desktop versions write). After a run the
page shows the lineage viewer inline, the Best Practice Analyzer findings, unused objects,
bookmarks, row-level security roles (flagged, so protected reports are shared with care) and
download links for all output files. Everything runs locally.

### Command line

```powershell
pbixtractor extract C:\Reports\Sales.pbix                  # model: the one inside the .pbix
pbixtractor extract Sales.pbix --model Model.bim -o out\Sales --no-tabular-editor
pbixtractor extract Sales.pbip                              # PBIP: model.bim or TMDL
pbixtractor extract --help

# From the Power BI service / Azure DevOps (sign-in opens a browser, or uses `az login`)
pbixtractor fabric list                                     # your workspaces
pbixtractor extract --fabric "Sales WS/Sales"              # download + document
pbixtractor extract --fabric "Sales WS/Sales" --all-reports  # every report on its model
pbixtractor extract --devops "https://dev.azure.com/org/Proj/_git/Repo?path=/Sales.Report"
pbixtractor devops fetch "<url>" --ref commit:a1b2c3d       # only download an older version

# Catalog: one searchable page over every documented model
pbixtractor extract Sales.pbix --catalog output\_catalog
pbixtractor catalog add output\_catalog --fabric-workspace "Sales WS"  # every model in a workspace
pbixtractor catalog add output\_catalog A.pbix B.pbix C.pbix            # one documentation per model
pbixtractor catalog add output\_catalog --devops "https://dev.azure.com/org/Proj/_git/Repo" --list
pbixtractor catalog add output\_catalog --devops "<repo url>" --only "Sales*"  # just those models
pbixtractor catalog list output\_catalog
```

Reports with an encrypting sensitivity label cannot be downloaded from Fabric (and encrypted
.pbix files cannot be read); use the PBIP project in Azure DevOps for those.

Output (default `output\<report name>\`): `<name>.xlsx`, `<name>_data.xlsx`, `<name>.json`,
`<name>_lineage.html` (offline lineage viewer) and `<name>_Relationships.png`.

### From Python

```python
from pbixtractor.pipeline import ExtractionOptions, run_extraction

result = run_extraction(
    ExtractionOptions(
        report_path="C:/Reports/Sales.pbix",
        model_path="C:/Reports/Sales.bim",
        output_dir="output/Sales",
    )
)
print(result.status, result.files)  # "success" | "warnings" | "error", {"workbook": Path, ...}
```

## Requirements

- **Python 3.11+**
- Optional: **Tabular Editor 2** ([download](https://github.com/TabularEditor/TabularEditor/releases))
  for the Best Practice Analyzer, exact DAX dependencies and live statistics (Windows). Found in
  Program Files automatically; otherwise set its folder in the web UI.

## Project Structure

```
pbixtractor/
├── src/
│   └── pbixtractor/          # Main package
│       ├── __init__.py       # Package initialization
│       ├── cli.py            # Command line (extract, web, fabric, devops, catalog)
│       ├── web_ui.py         # Web UI (NiceGUI)
│       ├── pipeline.py       # run_extraction(): the whole documentation run
│       ├── readers.py        # .pbix / PBIR / PBIP report readers
│       ├── semantic_model.py # model reader (.bim)
│       ├── pbix_model.py     # the model inside a .pbix (no .bim needed)
│       ├── fabric.py / devops.py # download from Fabric / Azure DevOps
│       ├── catalog.py        # catalog over many models
│       ├── tmdl.py           # TMDL model folders (newer PBIP projects)
│       └── data/             # data.yaml (visual types, DAX functions), BPA rules
├── tests/                    # Test suite
├── docs/                     # Documentation
├── output/                   # Generated reports (gitignored)
└── pyproject.toml           # Project metadata
```

## Configuration

Configuration lives in `src/pbixtractor/data/data.yaml`: supported visual types and how
they are extracted, field role labels (X-axis, Values, ...) and the DAX function names used for
syntax highlighting. See `docs/ADD_VISUAL_TYPES.md`.

## Development

### Running Tests

```powershell
uv sync --extra dev
uv run --frozen pytest -q
uv run --frozen pytest -q -m "not tabular_editor"   # skip the tests that run Tabular Editor 2
uv run --frozen pytest --cov=pbixtractor --cov-report=html
```

### Code Formatting

```powershell
uv run --frozen black src/ tests/
uv run --frozen ruff check src/ tests/
```

## Roadmap

- ✅ Modular package structure, YAML visual-type configuration
- ✅ Legacy `.pbix` layout and PBIR / PBIP reports; `.bim` and TMDL models
- ✅ JSON output and an interactive HTML lineage viewer; web UI and CLI
- ✅ Source tables from M queries and native SQL
- ✅ Reading reports from Fabric workspaces and Azure DevOps repositories (web UI + CLI)
- ✅ All reports on one semantic model documented together; row counts from the Power BI service
- ✅ Catalog: search measures, DAX and usage across models
- ✅ Bookmarks: the filters and slicer selections they apply (changed ones marked)
- 🔄 Source database lineage (views → base tables)
- ✅ Whole Fabric workspaces, Azure DevOps repositories or many files documented into the
  catalog in one go (you pick the models)

## Known Issues

- Bookmarks: cross-highlight selections and drill state are not listed.
- Tooltip/drillthrough page detection is not yet tested on real reports.
- Without Tabular Editor 2, DAX dependencies come from text matching (can miss unqualified
  references and match text in comments).
- Tabular Editor 2 and Power BI Desktop detection are Windows-only.

## License

MIT License - See LICENSE file for details

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Support

For issues and questions, please use the [GitHub Issues](https://github.com/MackanT/PBIxtractor/issues) page.

## Credits

Created by Marcus Toftås

Built with: pandas, xlsxwriter, matplotlib, networkx, NiceGUI, sqlglot, azure-identity
