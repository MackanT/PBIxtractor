# PBIxtractor

**Power BI Report Documentation Generator**

PBIxtractor extracts metadata and documentation from Power BI (.pbix) files, generating comprehensive Excel reports with:
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
git clone https://github.com/yourusername/pbixtractor.git
cd pbixtractor/PBIxtractor

# Install in editable mode with uv
uv pip install -e .

# Or install with dev dependencies
uv pip install -e ".[dev]"
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

Pick a report (`.pbix`, `.pbip` or a `.Report` folder). The model is found automatically:
`<name>.bim` next to the report, or a PBIP project's semantic model as `model.bim` or TMDL
(the `definition` folder newer Power BI Desktop versions write). After a run the
page shows the lineage viewer inline, the Best Practice Analyzer findings, unused objects,
bookmarks and download links for all output files. Everything runs locally.

### Command line

```powershell
pbixtractor extract C:\Reports\Sales.pbix                  # model: Sales.bim next to it
pbixtractor extract Sales.pbix --model Model.bim -o out\Sales --no-tabular-editor
pbixtractor extract Sales.pbip                              # PBIP: model.bim or TMDL
pbixtractor extract --help
```

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
│       ├── cli.py            # Command line (extract, web)
│       ├── web_ui.py         # Web UI (NiceGUI)
│       ├── pipeline.py       # run_extraction(): the whole documentation run
│       ├── readers.py        # .pbix / PBIR / PBIP report readers
│       ├── semantic_model.py # model reader (.bim)
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
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=pbixtractor --cov-report=html
```

### Code Formatting

```powershell
# Format with black
black src/ tests/

# Lint with ruff
ruff check src/ tests/
```

## Roadmap

- ✅ Modular package structure, YAML visual-type configuration
- ✅ Legacy `.pbix` layout and PBIR / PBIP reports; `.bim` and TMDL models
- ✅ JSON output and an interactive HTML lineage viewer; web UI and CLI
- ✅ Source tables from M queries and native SQL
- 🔄 Reading reports from Fabric workspaces and Azure DevOps repositories
- 🔄 Bookmark filter/slicer state
- 🔄 Source database lineage (views → base tables)

## Known Issues

See [docs/RESTRUCTURE_PLAN.md](docs/RESTRUCTURE_PLAN.md) for migration notes.

From the original ReadMe.txt:
- Buttons/bookmarks: Not always connected correctly
- Hierarchies: Sometimes missed
- Non-visual elements (shapes, images): Inconsistently documented

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

Built with: pandas, xlsxwriter, matplotlib, networkx, NiceGUI
