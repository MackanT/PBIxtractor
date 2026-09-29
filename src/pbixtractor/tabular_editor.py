"""Tabular Editor 2 command-line integration (optional, Windows only).

Everything here runs locally against the .bim file; nothing is sent to external services.

    exe = find_tabular_editor()
    violations = run_best_practice_analyzer(exe, "Model.bim")
    dependencies = export_dependencies(exe, "Model.bim")
"""

import csv
import ctypes
import io
import json
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .data import DATA_DIR

# Microsoft's standard Best Practice Analyzer rules (MIT licence, see BPARules.LICENSE)
# Source: https://github.com/microsoft/Analysis-Services/tree/master/BestPracticeRules
DEFAULT_BPA_RULES = DATA_DIR / "BPARules.json"

# Where to look for "Tabular Editor/TabularEditor.exe" when Input/TabularEditorLocations.txt
# does not list any folders
DEFAULT_SEARCH_DIRS = [Path(r"C:\Program Files"), Path(r"C:\Program Files (x86)")]

# Rule severity as used in the BPA rules file
SEVERITIES = {1: "Low", 2: "Medium", 3: "High"}

# "Column 'Sales'[Amount] violates rule "[Performance] Do not use floating point data types""
_VIOLATION = re.compile(r'^(?P<object>.+?) violates rule "(?P<rule>.+)"\s*$')
# Object type is the first word, optionally followed by "(...)", e.g. "Partition (M - Import)"
_OBJECT = re.compile(r"^(?P<type>\w+(?: \([^)]*\))?) (?P<name>.+)$")


def find_tabular_editor(locations_file: Optional[Path] = None) -> Optional[Path]:
    """
    Locate TabularEditor.exe (Tabular Editor 2).

    Args:
        locations_file: Text file with one folder per line to search in (default:
            Input/TabularEditorLocations.txt in the working directory, as used by the UI)

    Returns:
        Path to TabularEditor.exe, or None if not found
    """
    locations_file = locations_file or Path(os.getcwd()) / "Input" / "TabularEditorLocations.txt"
    folders = list(DEFAULT_SEARCH_DIRS)
    if locations_file.is_file():
        listed = [Path(line.strip()) for line in locations_file.read_text().splitlines()]
        folders = [folder for folder in listed if str(folder) != "."] + folders

    for folder in folders:
        for candidate in (
            folder / "Tabular Editor" / "TabularEditor.exe",
            folder / "TabularEditor.exe",
        ):
            if candidate.is_file():
                return candidate
    return None


@dataclass
class BpaViolation:
    """One Best Practice Analyzer finding."""

    object_type: str  # Column, Measure, Table, Relationship, Partition (M - Import), Model, ...
    object_name: str  # e.g. 'Sales'[Amount]
    rule: str  # rule name, e.g. "[Performance] Do not use floating point data types"
    category: str = ""
    severity: str = ""  # Low | Medium | High
    description: str = ""


def load_bpa_rules(rules_path: Path = DEFAULT_BPA_RULES) -> dict[str, dict]:
    """
    Load a BPA rules file.

    Args:
        rules_path: JSON rules file (Tabular Editor format)

    Returns:
        Dict of rule name -> rule definition
    """
    rules = json.loads(Path(rules_path).read_bytes().decode("utf-8-sig"))
    return {rule["Name"]: rule for rule in rules}


def parse_bpa_output(output: str, rules: dict[str, dict]) -> list[BpaViolation]:
    """
    Parse Tabular Editor's "-A" console output.

    Args:
        output: Console output of TabularEditor.exe <model> -A <rules>
        rules: Rule definitions from load_bpa_rules()

    Returns:
        List of violations, most severe first
    """
    violations = []
    for line in output.splitlines():
        match = _VIOLATION.match(line.strip())
        if not match:
            continue
        obj = _OBJECT.match(match["object"])
        object_type, object_name = (obj["type"], obj["name"]) if obj else ("", match["object"])
        rule = rules.get(match["rule"], {})
        violations.append(
            BpaViolation(
                object_type=object_type,
                object_name=object_name,
                rule=match["rule"],
                category=rule.get("Category", ""),
                severity=SEVERITIES.get(rule.get("Severity"), ""),
                description=rule.get("Description", ""),
            )
        )

    order = {"High": 0, "Medium": 1, "Low": 2}
    return sorted(violations, key=lambda v: (order.get(v.severity, 3), v.category, v.rule))


def _console_encoding() -> str:
    """Tabular Editor writes redirected output in the OEM code page (e.g. cp437/cp850)."""
    try:
        return f"cp{ctypes.windll.kernel32.GetOEMCP()}"
    except AttributeError:  # not Windows
        return "utf-8"


def run_best_practice_analyzer(
    exe: Path, bim_path: str | Path, rules_path: Path = DEFAULT_BPA_RULES, timeout: int = 300
) -> list[BpaViolation]:
    """
    Run Tabular Editor's Best Practice Analyzer on a .bim file.

    Args:
        exe: TabularEditor.exe from find_tabular_editor()
        bim_path: Model file
        rules_path: BPA rules file (default: Microsoft's standard rules)
        timeout: Seconds before giving up

    Returns:
        List of violations, most severe first

    Raises:
        RuntimeError: If Tabular Editor fails to load the model
        subprocess.TimeoutExpired: If it takes longer than timeout
    """
    # Exit code is non-zero whenever violations are found, so it is not an error signal
    process = subprocess.run(
        [str(exe), str(bim_path), "-A", str(rules_path)],
        capture_output=True,
        timeout=timeout,
    )
    output = process.stdout.decode(_console_encoding(), errors="replace")
    if "Running Best Practice Analyzer" not in output:
        error = process.stderr.decode(_console_encoding(), errors="replace") or output
        raise RuntimeError(f"Tabular Editor could not analyse {bim_path}: {error.strip()[-500:]}")
    return parse_bpa_output(output, load_bpa_rules(rules_path))


# ============================================================================
# Exact DAX dependencies
# ============================================================================

# Tabular Editor C# script: export every direct DAX dependency as TSV.
# DependsOn is Tabular Editor's own semantic analysis of the DAX (not text matching).
_DEPENDENCY_SCRIPT = r"""
var sb = new System.Text.StringBuilder();
sb.AppendLine("SourceType\tSourceTable\tSourceName\tTargetType\tTargetTable\tTargetName");

Func<ITabularNamedObject, string> tableOf = o =>
    o is Table ? ((Table)o).Name
    : o is TablePermission ? ((TablePermission)o).Table.Name
    : (o is ITabularTableObject ? ((ITabularTableObject)o).Table.Name : "");
Func<string, string> clean = s => (s ?? "").Replace("\t", " ").Replace("\r", " ").Replace("\n", " ");

var sources = new List<IDaxDependantObject>();
sources.AddRange(Model.AllMeasures);
sources.AddRange(Model.AllColumns.OfType<CalculatedColumn>());
sources.AddRange(Model.Tables.OfType<CalculatedTable>());
foreach (var role in Model.Roles) sources.AddRange(role.TablePermissions);

foreach (var source in sources)
{
    var named = (ITabularNamedObject)source;
    var sourceName = source is TablePermission ? ((TablePermission)source).Role.Name : named.Name;
    foreach (var target in source.DependsOn.Keys)
    {
        sb.AppendLine(string.Join("\t", new[] {
            named.ObjectType.ToString(), clean(tableOf(named)), clean(sourceName),
            target.ObjectType.ToString(), clean(tableOf(target)), clean(target.Name) }));
    }
}
SaveFile(@"%OUTPUT%", sb.ToString());
"""


@dataclass(frozen=True)
class Dependency:
    """A direct DAX dependency: source (measure, calculated column/table, RLS) -> target."""

    source_type: str  # Measure | Column | Table | TablePermission (RLS filter; name = role)
    source_table: str
    source_name: str
    target_type: str  # Measure | Column | Table | ...
    target_table: str
    target_name: str

    @property
    def source_ref(self) -> str:
        return _dax_ref(self.source_type, self.source_table, self.source_name)

    @property
    def target_ref(self) -> str:
        return _dax_ref(self.target_type, self.target_table, self.target_name)


def _dax_ref(object_type: str, table: str, name: str) -> str:
    """Table[Name] for columns/measures, 'Table' for tables, "RLS: role (table)" for filters."""
    if object_type == "Table":
        return f"'{name}'"
    if object_type == "TablePermission":
        return f"RLS: {name} ({table})"
    return f"{table}[{name}]"


def parse_dependencies(tsv: str) -> list[Dependency]:
    """
    Parse the TSV written by the dependency script.

    Args:
        tsv: File content with a header row

    Returns:
        List of dependencies
    """
    reader = csv.reader(io.StringIO(tsv), delimiter="\t", quoting=csv.QUOTE_NONE)
    next(reader, None)  # header
    return [Dependency(*row) for row in reader if len(row) == 6]


def export_dependencies(exe: Path, bim_path: str | Path, timeout: int = 300) -> list[Dependency]:
    """
    Export exact DAX dependencies using Tabular Editor's semantic analysis.

    Covers measures, calculated columns, calculated tables and RLS filters. Unlike text
    matching, this resolves unqualified column references and same-named columns in
    different tables correctly.

    Args:
        exe: TabularEditor.exe from find_tabular_editor()
        bim_path: Model file
        timeout: Seconds before giving up

    Returns:
        List of direct dependencies

    Raises:
        RuntimeError: If Tabular Editor fails to load the model or run the script
        subprocess.TimeoutExpired: If it takes longer than timeout
    """
    with tempfile.TemporaryDirectory(prefix="pbixtractor_") as folder:
        output = Path(folder) / "dependencies.tsv"
        script = Path(folder) / "dependencies.cs"
        script.write_text(_DEPENDENCY_SCRIPT.replace("%OUTPUT%", str(output)), encoding="utf-8")

        process = subprocess.run(
            [str(exe), str(bim_path), "-S", str(script)],
            capture_output=True,
            timeout=timeout,
        )
        if not output.is_file():
            log = (process.stdout + process.stderr).decode(_console_encoding(), errors="replace")
            raise RuntimeError(
                f"Tabular Editor could not export dependencies from {bim_path}: "
                f"{log.strip()[-500:]}"
            )
        return parse_dependencies(output.read_bytes().decode("utf-8-sig"))


def drop_redundant_table_refs(dependencies: list[Dependency]) -> list[Dependency]:
    """
    Remove "depends on table X" entries when the same source also uses a column of X.

    Tabular Editor lists 'Sales' as well as Sales[Amount] for SUM ( Sales[Amount] ); the table
    entry only adds information when it is the sole reference, e.g. COUNTROWS ( Sales ).

    Args:
        dependencies: Output of export_dependencies()

    Returns:
        Filtered list, original order kept
    """
    tables_with_columns = {
        (d.source_type, d.source_table, d.source_name, d.target_table)
        for d in dependencies
        if d.target_type == "Column"
    }
    return [
        d
        for d in dependencies
        if not (
            d.target_type == "Table"
            and (d.source_type, d.source_table, d.source_name, d.target_name) in tables_with_columns
        )
    ]
