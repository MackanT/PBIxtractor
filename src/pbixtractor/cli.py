#!/usr/bin/env python3
"""Command-line interface for PBIxtractor"""

import sys
from pathlib import Path

# Import version at module level
from . import __version__


def main():
    """Main entry point for PBIxtractor CLI"""
    import argparse

    # Import from extractor module
    from .extractor import run_ui, run_cmd

    parser = argparse.ArgumentParser(
        description="PBIxtractor - Power BI Documentation Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  pbixtractor --ui                      # Launch GUI
  pbixtractor --test                    # Run test extraction (dev mode)
  python -m pbixtractor --ui            # Alternative way to launch GUI

Note: For --test mode, update test file paths in extractor.py -> run_test_extraction()
        """
    )

    parser.add_argument(
        "--ui", 
        action="store_true", 
        help="Run with graphical user interface (DearPyGUI)"
    )

    parser.add_argument(
        "--test",
        action="store_true",
        help="Run test extraction with hardcoded test files (for development)"
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"PBIxtractor {__version__}"
    )

    # Future CLI arguments (for phase 2)
    # parser.add_argument("--pbix", type=str, help="Path to .pbix file")
    # parser.add_argument("--bim", type=str, help="Path to .bim file")
    # parser.add_argument("--output", type=str, help="Output directory name")

    args = parser.parse_args()

    if args.test:
        print("Running test extraction with hardcoded test files...")
        from .extractor import run_test_extraction
        result = run_test_extraction()

        if result == "Success":
            print("\n[SUCCESS] Test extraction completed successfully!")
            sys.exit(0)
        elif result == "Log":
            print("\n[WARNING] Test extraction completed with warnings. Check logs for details.")
            sys.exit(0)
        else:
            print(f"\n[ERROR] Test extraction failed: {result}")
            sys.exit(1)

    elif args.ui or len(sys.argv) == 1:  # Default to UI if no args
        print("Starting PBIxtractor GUI...")
        run_ui()
    else:
        # CLI mode not yet implemented in v0.2
        print("Command-line mode coming soon!")
        print("Use --ui to launch the graphical interface.")
        sys.exit(1)


if __name__ == "__main__":
    main()
