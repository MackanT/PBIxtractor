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

### Graphical Interface

```powershell
# Run the GUI
pbixtractor --ui

# Or using Python module syntax
python -m pbixtractor --ui
```

### From Python

```python
from pbixtractor import ReportExtractor

# Create extractor instance
extractor = ReportExtractor(
    path="path/to/your/report",
    name="MyReport.pbix"
)

# Extract report metadata
extractor.extract()

# Access results
print(extractor.result)  # Visual inventory
print(extractor.filters)  # Filter definitions
```

## Requirements

- **Python 3.11+**
- **Tabular Editor 2** - Required for DAX extraction ([Download here](https://tabulareditor.com/))
- Windows OS (for Tabular Editor integration)

## Project Structure

```
pbixtractor/
├── src/
│   └── pbixtractor/          # Main package
│       ├── __init__.py       # Package initialization
│       ├── cli.py            # Command-line interface
│       ├── extractor.py      # Core extraction logic
│       └── data/             # Configuration CSV files
├── tests/                    # Test suite
├── docs/                     # Documentation
├── output/                   # Generated reports (gitignored)
└── pyproject.toml           # Project metadata
```

## Configuration

The tool uses CSV files in `src/pbixtractor/data/` for configuration:

- **VisualTypes.csv** - Supported visual types
- **DataTypes.csv** - Field type mappings (X-axis, Y-axis, etc.)
- **FunctionNames.csv** - DAX functions for syntax highlighting

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

See [docs/IMPROVEMENT_PLAN.md](docs/IMPROVEMENT_PLAN.md) for planned enhancements:

- ✅ Modular package structure (v0.2.0)
- 🔄 Azure DevOps integration (v0.3.0)
- 🔄 .pbir format support (v0.3.0)
- 🔄 Plugin architecture for visual types (v0.4.0)
- 🔄 HTML/JSON/Markdown output formats (v0.5.0)
- 🔄 Similar measure detection (v0.6.0)

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

For issues and questions, please use the [GitHub Issues](https://github.com/yourusername/pbixtractor/issues) page.

## Credits

Created by Marcus Toftås

Built with: pandas, xlsxwriter, matplotlib, networkx, DearPyGUI
