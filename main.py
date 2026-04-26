import sys
import os
from pathlib import Path
import importlib.util

# Add PBIxtractor directory to path
pbixtractor_path = Path(__file__).parent / "PBIxtractor"
sys.path.insert(0, str(pbixtractor_path))

# Change working directory to PBIxtractor folder so it can find Input files
os.chdir(pbixtractor_path)

spec = importlib.util.spec_from_file_location(
    "pb_ixtractor", pbixtractor_path / "PB-Ixtractor.py"
)
pb_ixtractor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pb_ixtractor)


def main():
    """Run PB-Ixtractor in UI mode by default."""
    print("Starting PBIxtractor...")

    # Option 1: Run in UI mode (default)
    pb_ixtractor.run_ui()

    # Option 2: Run in command-line mode (uncomment to use)
    # result = pb_ixtractor.run_cmd()
    # print(result)


if __name__ == "__main__":
    main()
