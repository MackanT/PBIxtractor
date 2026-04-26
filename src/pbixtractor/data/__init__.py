"""Package data directory containing configuration files"""

from pathlib import Path

# Get the directory containing the data files
DATA_DIR = Path(__file__).parent

# Path to main YAML configuration file
YAML_FILE = DATA_DIR / "data.yaml"
