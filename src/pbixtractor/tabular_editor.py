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


# Tabular Editor C# script: run the Best Practice Analyzer and write every finding to a TSV.
# (The "-A" console output is not used: it is truncated unpredictably for larger models.)
_BPA_SCRIPT = r"""
Func<string, string> clean = s => (s ?? "").Replace("\t", " ").Replace("\r", " ").Replace("\n", " ");
var rules = Newtonsoft.Json.JsonConvert.DeserializeObject<List<TabularEditor.BestPracticeAnalyzer.BestPracticeRule>>(
    System.IO.File.ReadAllText(@"%RULES%"));
var analyzer = new TabularEditor.BestPracticeAnalyzer.Analyzer();
analyzer.SetModel(Model, null);

var sb = new System.Text.StringBuilder("RuleName\tObjectType\tObjectName\tError\n");
foreach (var result in analyzer.Analyze(rules))
{
    if (result.Ignored) continue;
    if (result.RuleHasError)
        sb.AppendLine(clean(result.RuleName) + "\t\t\t" + clean(result.RuleError));
    else if (result.Object != null)
        sb.AppendLine(clean(result.RuleName) + "\t" + clean(result.ObjectType) + "\t" + clean(result.ObjectName) + "\t");
}
SaveFile(System.IO.Path.Combine(@"%FOLDER%", "bpa.tsv"), sb.ToString());
"""


def _console_encoding() -> str:
    """Tabular Editor writes redirected output in the OEM code page (e.g. cp437/cp850)."""
    try:
        return f"cp{ctypes.windll.kernel32.GetOEMCP()}"
    except AttributeError:  # not Windows
        return "utf-8"


def run_script(exe: Path, target: list[str], script: str, timeout: int = 300) -> dict[str, str]:
    """
    Run a Tabular Editor C# script and collect the files it writes.

    Args:
        exe: TabularEditor.exe
        target: Model to load, e.g. ["Model.bim"] or ["localhost:53162", ""]
        script: C# script; "%FOLDER%" is replaced by a temporary output folder
        timeout: Seconds before giving up

    Returns:
        Output file stem -> content, for every file the script saved (.tsv/.txt/.error)

    Raises:
        RuntimeError: If the script produced no output (model failed to load, script error)
        subprocess.TimeoutExpired: If it takes longer than timeout
    """
    with tempfile.TemporaryDirectory(prefix="pbixtractor_") as folder:
        script_path = Path(folder) / "script.cs"
        script_path.write_text(script.replace("%FOLDER%", folder), encoding="utf-8")
        process = subprocess.run(
            [str(exe), *target, "-S", str(script_path)], capture_output=True, timeout=timeout
        )
        outputs = {
            path.name: path.read_bytes().decode("utf-8-sig", errors="replace")
            for path in Path(folder).iterdir()
            if path.name != "script.cs"
        }
        if not outputs:
            log = (process.stdout + process.stderr).decode(_console_encoding(), errors="replace")
            raise RuntimeError(f"Tabular Editor script failed on {target[0]}: {log.strip()[-500:]}")
        return outputs


def parse_bpa_results(tsv: str, rules: dict[str, dict]) -> tuple[list[BpaViolation], list[str]]:
    """
    Parse the TSV written by the BPA script.

    Args:
        tsv: Script output (RuleName, ObjectType, ObjectName, Error)
        rules: Rule definitions from load_bpa_rules()

    Returns:
        (violations most severe first, messages for rules that could not be evaluated)
    """
    violations, errors = [], []
    for row in csv.DictReader(io.StringIO(tsv), delimiter="\t", quoting=csv.QUOTE_NONE):
        if row["Error"]:
            errors.append(f"{row['RuleName']}: {row['Error']}")
            continue
        rule = rules.get(row["RuleName"], {})
        violations.append(
            BpaViolation(
                object_type=row["ObjectType"],
                object_name=row["ObjectName"],
                rule=row["RuleName"],
                category=rule.get("Category", ""),
                severity=SEVERITIES.get(rule.get("Severity"), ""),
                description=rule.get("Description", ""),
            )
        )

    order = {"High": 0, "Medium": 1, "Low": 2}
    violations.sort(key=lambda v: (order.get(v.severity, 3), v.category, v.rule))
    return violations, errors


def run_best_practice_analyzer(
    exe: Path, bim_path: str | Path, rules_path: Path = DEFAULT_BPA_RULES, timeout: int = 300
) -> tuple[list[BpaViolation], list[str]]:
    """
    Run Tabular Editor's Best Practice Analyzer on a .bim file.

    Args:
        exe: TabularEditor.exe from find_tabular_editor()
        bim_path: Model file
        rules_path: BPA rules file (default: Microsoft's standard rules)
        timeout: Seconds before giving up

    Returns:
        (violations most severe first, messages for rules that could not be evaluated)

    Raises:
        RuntimeError: If Tabular Editor fails to load the model or run the analysis
        subprocess.TimeoutExpired: If it takes longer than timeout
    """
    outputs = run_script(
        exe, [str(bim_path)], _BPA_SCRIPT.replace("%RULES%", str(rules_path)), timeout
    )
    if "bpa.tsv" not in outputs:
        raise RuntimeError(f"Best Practice Analyzer produced no results for {bim_path}")
    return parse_bpa_results(outputs["bpa.tsv"], load_bpa_rules(rules_path))


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
SaveFile(System.IO.Path.Combine(@"%FOLDER%", "dependencies.tsv"), sb.ToString());
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
    outputs = run_script(exe, [str(bim_path)], _DEPENDENCY_SCRIPT, timeout)
    if "dependencies.tsv" not in outputs:
        raise RuntimeError(f"Tabular Editor could not export dependencies from {bim_path}")
    return parse_dependencies(outputs["dependencies.tsv"])


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
