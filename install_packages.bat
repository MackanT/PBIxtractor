@echo off
:: List of Python packages to install
set packages=argparse pandas xlsxwriter psutil zipfile36 networkx matplotlib

:: Install each package if it's not already installed
for %%p in (%packages%) do (
    echo Checking for package %%p...
    python -c "import %%p" 2>nul
    if errorlevel 1 (
        echo Package %%p not found. Installing...
        pip install %%p
    ) else (
        echo Package %%p is already installed.
    )
)

echo All packages are installed.
pause
