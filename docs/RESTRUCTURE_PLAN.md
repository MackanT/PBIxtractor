# PBIxtractor Project Restructure Plan

## Current Issues Found

### 1. **Nested Duplicate Folder Structure** ❌
```
PBIxtractor/              # Project root
├── PB-Ixtractor.py       # Duplicate at root (should not exist)
├── main.py               # Wrapper that imports from nested folder
├── Input/                # Duplicate Input folder
└── PBIxtractor/          # NESTED duplicate folder (confusing!)
    ├── PB-Ixtractor.py   # The actual code
    ├── Input/            # The actual Input data
    ├── __pycache__/      # Should be in .gitignore
    ├── .gitignore        # Duplicate .gitignore
    ├── Invoices/         # OUTPUT folder (should not be in repo!)
    ├── Navigator/        # OUTPUT folder (should not be in repo!)
    └── SemanticModell/   # OUTPUT folder (should not be in repo!)
```

### 2. **Output Folders Committed to Git** ❌
- `Invoices/`, `Navigator/`, `SemanticModell/` contain generated `.xlsx`, `.tsv`, `.cs` files
- These should be excluded via `.gitignore`

### 3. **Non-Standard Python Package Structure** ❌
- No `src/` layout (recommended for uv projects)
- No `__init__.py` files
- No proper package structure

### 4. **Redundant Files** ❌
- Two `.gitignore` files (root and nested)
- Duplicate `Input/` folders
- `.bat` files for pip (no longer needed with uv)

---

## Proposed Structure (uv Best Practices)

```
pbixtractor/                          # Project root (lowercase per PEP 8)
├── .git/
├── .github/                          # Optional: CI/CD workflows
│   └── workflows/
│       └── test.yml
├── src/
│   └── pbixtractor/                  # Main package
│       ├── __init__.py               # Package marker + version
│       ├── __main__.py               # Entry point for `python -m pbixtractor`
│       ├── cli.py                    # Click-based CLI
│       ├── extractor.py              # ReportExtractor class
│       ├── data/                     # Package data
│       │   ├── __init__.py
│       │   ├── DataTypes.csv
│       │   ├── FunctionNames.csv
│       │   └── VisualTypes.csv
│       └── utils/
│           ├── __init__.py
│           └── logger.py
├── tests/
│   ├── __init__.py
│   ├── test_extractor.py
│   └── fixtures/
│       └── sample.pbix
├── docs/                             # Optional: Documentation
│   ├── IMPROVEMENT_PLAN.md
│   └── usage.md
├── output/                           # Default output directory (gitignored)
├── .gitignore
├── .python-version
├── LICENSE
├── README.md
├── pyproject.toml
└── uv.lock
```

---

## Migration Steps

### Step 1: Create Proper Package Structure
```powershell
# Create src layout
mkdir src\pbixtractor
mkdir src\pbixtractor\data
mkdir src\pbixtractor\utils
mkdir tests
mkdir docs
mkdir output

# Create __init__.py files
New-Item src\pbixtractor\__init__.py
New-Item src\pbixtractor\data\__init__.py
New-Item src\pbixtractor\utils\__init__.py
New-Item tests\__init__.py
```

### Step 2: Move Files to Correct Locations
```powershell
# Move main code
Move-Item PBIxtractor\PB-Ixtractor.py src\pbixtractor\extractor.py

# Move Input data to package data
Move-Item PBIxtractor\Input\*.csv src\pbixtractor\data\

# Move main.py → cli.py
Move-Item main.py src\pbixtractor\cli.py

# Move documentation
Move-Item ..\IMPROVEMENT_PLAN.md docs\

# Move assets (if needed)
Move-Item PBIxtractor\logo*.png src\pbixtractor\data\  # Or delete if not used
Move-Item PBIxtractor\logo.ico src\pbixtractor\data\
```

### Step 3: Delete Redundant/Generated Files
```powershell
# Delete nested duplicate folder
Remove-Item PBIxtractor\PBIxtractor -Recurse -Force

# Delete duplicate Input folder
Remove-Item Input -Recurse -Force

# Delete old pip artifacts
Remove-Item *.bat -Force

# Delete old README (keep main one)
Remove-Item PBIxtractor\ReadMe.txt
```

### Step 4: Update `.gitignore`
Consolidate into one `.gitignore` at root:
```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python

# Virtual environments
.env
.venv
env/
venv/
ENV/

# uv
.uv/
uv.lock  # Optionally include this

# Package artifacts
*.egg-info/
dist/
build/
*.egg

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# PBIxtractor specific output
output/
*.cs
*.tsv
*_Relationships.png
*.xlsx
log_data_*.txt
TabularScript.cs

# Don't ignore example/template files
!src/pbixtractor/data/*.csv
```

### Step 5: Update `pyproject.toml`
```toml
[project]
name = "pbixtractor"
version = "0.2.0"
description = "Power BI report documentation extractor with Azure DevOps support"
readme = "README.md"
requires-python = ">=3.11"
license = { text = "MIT" }
authors = [
    { name = "Marcus Toftås", email = "your.email@example.com" }
]
keywords = ["powerbi", "documentation", "dax", "tabular-editor"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
]

dependencies = [
    "pandas>=2.0.0",
    "xlsxwriter>=3.2.0",
    "matplotlib>=3.8.0",
    "networkx>=3.0.0",
    "dearpygui>=2.0.0",
    "psutil>=5.9.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "pytest-cov>=4.1.0",
    "black>=23.0.0",
    "ruff>=0.1.0",
]

[project.urls]
Homepage = "https://github.com/yourusername/pbixtractor"
Repository = "https://github.com/yourusername/pbixtractor"
Issues = "https://github.com/yourusername/pbixtractor/issues"

[project.scripts]
pbixtractor = "pbixtractor.cli:main"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/pbixtractor"]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]

[tool.black]
line-length = 100
target-version = ["py311"]

[tool.ruff]
line-length = 100
target-version = "py311"
```

### Step 6: Create Package Entry Points

**src/pbixtractor/__init__.py**:
```python
"""PBIxtractor - Power BI Report Documentation Generator"""

__version__ = "0.2.0"

from .extractor import ReportExtractor

__all__ = ["ReportExtractor", "__version__"]
```

**src/pbixtractor/__main__.py**:
```python
"""Allow running as `python -m pbixtractor`"""

from .cli import main

if __name__ == "__main__":
    main()
```

**src/pbixtractor/cli.py** (refactor from main.py):
```python
#!/usr/bin/env python3
"""Command-line interface for PBIxtractor"""

import sys
from pathlib import Path

# Add src to path for development
if __name__ == "__main__":
    src_path = Path(__file__).parent.parent
    sys.path.insert(0, str(src_path))

from pbixtractor.extractor import run_ui, run_cmd


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="PBIxtractor - Power BI Documentation Tool")
    parser.add_argument("--ui", action="store_true", help="Run with GUI")
    parser.add_argument("--pbix", type=str, help="Path to .pbix file")
    parser.add_argument("--bim", type=str, help="Path to .bim file")
    parser.add_argument("--output", type=str, help="Output directory name")
    
    args = parser.parse_args()
    
    if args.ui:
        run_ui()
    else:
        # TODO: Implement CLI mode with args
        print("CLI mode not yet implemented. Use --ui for GUI mode.")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

---

## Testing the Migration

### 1. Verify Package Installation
```powershell
# Install in editable mode
uv pip install -e .

# Check if command works
pbixtractor --help

# Or run as module
python -m pbixtractor --ui
```

### 2. Run Tests (after creating)
```powershell
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Check coverage
pytest tests/ --cov=pbixtractor --cov-report=html
```

### 3. Build Package
```powershell
# Build wheel and sdist
uv build

# Check dist/ folder
ls dist/
```

---

## File-by-File Action Plan

| Current Location | Action | New Location |
|-----------------|--------|--------------|
| `PBIxtractor/PBIxtractor/PB-Ixtractor.py` | Move & Rename | `src/pbixtractor/extractor.py` |
| `main.py` | Refactor | `src/pbixtractor/cli.py` |
| `PBIxtractor/Input/*.csv` | Move | `src/pbixtractor/data/*.csv` |
| `PBIxtractor/PBIxtractor/Input/*.csv` | Delete | (Duplicate) |
| `PBIxtractor/logo*.png` | Move or Delete | `src/pbixtractor/data/` or delete |
| `PBIxtractor/PB-Ixtractor.py` | Delete | (Duplicate) |
| `Input/` | Delete | (Duplicate) |
| `PBIxtractor/PBIxtractor/.gitignore` | Delete | (Consolidate to root) |
| `PBIxtractor/PBIxtractor/Invoices/` | Delete | (Output folder) |
| `PBIxtractor/PBIxtractor/Navigator/` | Delete | (Output folder) |
| `PBIxtractor/PBIxtractor/SemanticModell/` | Delete | (Output folder) |
| `PBIxtractor/PBIxtractor/__pycache__/` | Delete | (Generated) |
| `install_packages.bat` | Delete | (Use uv instead) |
| `PBIxtractor/ReadMe.txt` | Keep content | Merge into `README.md` |
| `../IMPROVEMENT_PLAN.md` | Move | `docs/IMPROVEMENT_PLAN.md` |

---

## Git Cleanup Commands

After restructuring, clean git history:

```powershell
# Stage deletions
git add -A

# Commit restructure
git commit -m "refactor: restructure project to follow uv/src-layout conventions

- Move code to src/pbixtractor/ package structure
- Consolidate duplicate folders and .gitignore files
- Remove output folders from version control
- Update pyproject.toml with proper metadata and entry points
- Add __init__.py for proper package structure"

# Verify what's tracked
git ls-files

# Clean untracked files (BE CAREFUL!)
git clean -fd -n   # Dry run
git clean -fd      # Actually delete
```

---

## Benefits of This Restructure

1. ✅ **Standard Python packaging** - Follows PEP 517/518
2. ✅ **Clean separation** - Source code vs. data vs. tests vs. output
3. ✅ **Better imports** - `from pbixtractor import ReportExtractor`
4. ✅ **Installable** - `pip install -e .` or `uv pip install -e .`
5. ✅ **CLI script** - `pbixtractor --ui` from anywhere
6. ✅ **Testable** - Proper test structure
7. ✅ **Publishable** - Ready for PyPI if desired
8. ✅ **IDE-friendly** - Better autocomplete and navigation

---

## Optional: Pre-commit Hooks

Create `.pre-commit-config.yaml`:
```yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.12.0
    hooks:
      - id: black
        language_version: python3.11

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.1.9
    hooks:
      - id: ruff
        args: [--fix]

  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
```

Install:
```powershell
uv pip install pre-commit
pre-commit install
```

Let me know when you're ready to execute this restructure!
