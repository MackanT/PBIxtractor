"""PBIxtractor - Power BI Report Documentation Generator

A tool for extracting and documenting Power BI reports with support for:
- PBIX file parsing and metadata extraction
- Tabular Editor integration for DAX analysis
- Visual inventory and filter documentation
- Relationship diagrams and dependency tracking
- Excel output with color-coded DAX formulas
"""

__version__ = "0.2.0"

from .extractor import ReportExtractor

__all__ = ["ReportExtractor", "__version__"]
