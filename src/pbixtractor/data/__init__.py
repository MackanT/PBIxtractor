"""Package data directory containing configuration CSV files"""

from pathlib import Path

# Get the directory containing the data files
DATA_DIR = Path(__file__).parent

# Paths to configuration files
VISUAL_TYPES_CSV = DATA_DIR / "VisualTypes.csv"
DATA_TYPES_CSV = DATA_DIR / "DataTypes.csv"
FUNCTION_NAMES_CSV = DATA_DIR / "FunctionNames.csv"
