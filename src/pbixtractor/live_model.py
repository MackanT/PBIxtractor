"""Statistics from a live model: the report open in Power BI Desktop (read-only).

Power BI Desktop runs a local Analysis Services instance per open report. Through Tabular
Editor 2 we read, without changing anything:
    - row counts per table                        INFO.STORAGETABLES()
    - distinct values per column                  COLUMNSTATISTICS()
    - memory per column (dictionary + data +      INFO.STORAGETABLECOLUMNS(),
      attribute hierarchy), as VertiPaq Analyzer  INFO.STORAGETABLECOLUMNSEGMENTS()
    - measure data types                          the live model's measure metadata

    instance = find_instance_for_report(report_path)
    stats = read_live_statistics(tabular_editor_exe, instance.port)

INFO.* DAX functions need a recent Power BI Desktop (2023 or later).
"""

import csv
import io
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import psutil

# Tabular Editor C# script: dump raw rows to one TSV per query into %FOLDER%.
# Each query is independent; a failing one writes <name>.error instead of stopping the rest.
_STATISTICS_SCRIPT = r"""
var folder = @"%FOLDER%";
var invariant = System.Globalization.CultureInfo.InvariantCulture;
Func<object, string> text = v => Convert.ToString(v, invariant).Replace("\t", " ").Replace("\r", " ").Replace("\n", " ");

Action<string, string> dump = (name, dax) => {
    try {
        var sb = new System.Text.StringBuilder();
        using (var reader = ExecuteReader(dax)) {
            var names = new List<string>();
            for (int i = 0; i < reader.FieldCount; i++) names.Add(reader.GetName(i).Trim('[', ']'));
            sb.AppendLine(string.Join("\t", names));
            while (reader.Read()) {
                var values = new object[reader.FieldCount];
                reader.GetValues(values);
                sb.AppendLine(string.Join("\t", values.Select(text)));
            }
        }
        SaveFile(System.IO.Path.Combine(folder, name + ".tsv"), sb.ToString());
    } catch (Exception e) {
        SaveFile(System.IO.Path.Combine(folder, name + ".error"), e.Message);
    }
};

dump("storage_tables", "EVALUATE SELECTCOLUMNS(INFO.STORAGETABLES(), \"Table\", [DIMENSION_NAME], \"TableId\", [TABLE_ID], \"Rows\", [ROWS_COUNT])");
dump("storage_columns", "EVALUATE SELECTCOLUMNS(INFO.STORAGETABLECOLUMNS(), \"Table\", [DIMENSION_NAME], \"Column\", [ATTRIBUTE_NAME], \"TableId\", [TABLE_ID], \"ColumnId\", [COLUMN_ID], \"ColumnType\", [COLUMN_TYPE], \"DictionarySize\", [DICTIONARY_SIZE])");
dump("segments", "EVALUATE SELECTCOLUMNS(INFO.STORAGETABLECOLUMNSEGMENTS(), \"Table\", [DIMENSION_NAME], \"TableId\", [TABLE_ID], \"ColumnId\", [COLUMN_ID], \"UsedSize\", [USED_SIZE])");
dump("column_statistics", "EVALUATE COLUMNSTATISTICS()");

var measures = new System.Text.StringBuilder("Table\tMeasure\tDataType\n");
foreach (var m in Model.AllMeasures) measures.AppendLine(text(m.Table.Name) + "\t" + text(m.Name) + "\t" + m.DataType);
SaveFile(System.IO.Path.Combine(folder, "measures.tsv"), measures.ToString());

var tables = new System.Text.StringBuilder("Table\n");
foreach (var t in Model.Tables) tables.AppendLine(text(t.Name));
SaveFile(System.IO.Path.Combine(folder, "tables.tsv"), tables.ToString());
"""


# ============================================================================
# Finding Power BI Desktop instances
# ============================================================================


@dataclass
class LocalInstance:
    """A Power BI Desktop Analysis Services instance on this machine."""

    port: int
    report_path: Optional[Path]  # the .pbix/.pbip Desktop was started with, if known
    pid: int


def find_local_instances() -> list[LocalInstance]:
    """
    Find Analysis Services instances started by Power BI Desktop.

    The port comes from the msmdsrv process's listening socket (or its workspace
    msmdsrv.port.txt), and the report path from its parent PBIDesktop command line.

    Returns:
        Running instances (empty if Power BI Desktop is not running)
    """
    instances = []
    for process in psutil.process_iter(["name", "pid", "ppid"]):
        if (process.info["name"] or "").lower() != "msmdsrv.exe":
            continue
        try:
            port = _listening_port(process)
            if port is None:
                continue
            report_path = None
            parent = psutil.Process(process.info["ppid"])
            if (parent.name() or "").lower() == "pbidesktop.exe":
                report_path = next(
                    (
                        Path(arg)
                        for arg in parent.cmdline()[1:]
                        if arg.lower().endswith((".pbix", ".pbip"))
                    ),
                    None,
                )
            instances.append(LocalInstance(port, report_path, process.info["pid"]))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return instances


def _listening_port(process: psutil.Process) -> Optional[int]:
    """Port an msmdsrv process listens on."""
    try:
        ports = [c.laddr.port for c in process.net_connections(kind="tcp") if c.status == "LISTEN"]
        if ports:
            return ports[0]
    except (psutil.AccessDenied, OSError):
        pass
    # Fallback: "-s <workspace>\Data" on the command line holds msmdsrv.port.txt
    cmdline = process.cmdline()
    if "-s" in cmdline and cmdline.index("-s") + 1 < len(cmdline):
        port_file = Path(cmdline[cmdline.index("-s") + 1]) / "msmdsrv.port.txt"
        if port_file.is_file():
            return int(port_file.read_bytes().decode("utf-16").strip())
    return None


def _same_file(a: Path, b: Path) -> bool:
    return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


def find_instance_for_report(
    report_path: str | Path, instances: Optional[list[LocalInstance]] = None
) -> tuple[Optional[LocalInstance], list[LocalInstance]]:
    """
    Pick the Power BI Desktop instance that has this report open.

    Args:
        report_path: The .pbix being documented
        instances: From find_local_instances() (looked up if None)

    Returns:
        (instance with a matching path or None, instances whose report is unknown).
        Unknown ones must be verified by comparing table names, see tables_match().
    """
    instances = find_local_instances() if instances is None else instances
    exact = next(
        (i for i in instances if i.report_path and _same_file(i.report_path, Path(report_path))),
        None,
    )
    return exact, [i for i in instances if i.report_path is None]


def tables_match(live_tables: set[str], model_tables: set[str], threshold: float = 0.8) -> bool:
    """True if the live model is (almost) the same model as the .bim (Jaccard similarity)."""
    if not live_tables or not model_tables:
        return False
    return len(live_tables & model_tables) / len(live_tables | model_tables) >= threshold


# ============================================================================
# Reading statistics
# ============================================================================


@dataclass
class ColumnStatistics:
    table: str
    column: str
    distinct_values: Optional[int] = None
    dictionary_size: int = 0
    data_size: int = 0
    hierarchy_size: int = 0

    @property
    def total_size(self) -> int:
        return self.dictionary_size + self.data_size + self.hierarchy_size


@dataclass
class LiveStatistics:
    tables: set[str] = field(default_factory=set)
    table_rows: dict[str, int] = field(default_factory=dict)
    table_sizes: dict[str, int] = field(default_factory=dict)  # all storage incl. hierarchies
    columns: dict[tuple[str, str], ColumnStatistics] = field(default_factory=dict)
    measure_types: dict[tuple[str, str], str] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)  # query name -> error message

    @property
    def model_size(self) -> int:
        return sum(self.table_sizes.values())

    @property
    def has_sizes(self) -> bool:
        """False when only row counts / distinct values are known (e.g. from executeQueries)."""
        return bool(self.table_sizes)


def _rows(tsv: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(tsv), delimiter="\t", quoting=csv.QUOTE_NONE))


def _int(value: str) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def build_statistics(files: dict[str, str]) -> LiveStatistics:
    """
    Aggregate the raw rows dumped by the statistics script.

    Args:
        files: File name (without extension) -> TSV content, e.g. {"segments": "..."}

    Returns:
        LiveStatistics
    """
    return statistics_from_rows({name: _rows(tsv) for name, tsv in files.items()})


def statistics_from_rows(rows: dict[str, list[dict]]) -> LiveStatistics:
    """
    Aggregate raw query rows (from Power BI Desktop or the Power BI service).

    Column size follows VertiPaq Analyzer: dictionary + data segments in the table's main
    storage table + the column's attribute hierarchy (H$<table id>$<column id>).
    Table size is all storage of the table (also user hierarchies U$ and relationships R$).

    Args:
        rows: Query name -> rows; names and columns as in the statistics script:
            tables (Table), measures (Table, Measure, DataType), storage_tables,
            storage_columns, segments, column_statistics (Table Name, Column Name, Cardinality)

    Returns:
        LiveStatistics
    """
    stats = LiveStatistics()
    stats.tables = {row["Table"] for row in rows.get("tables", [])}

    for row in rows.get("measures", []):
        stats.measure_types[(row["Table"], row["Measure"])] = row["DataType"]

    # Main storage table per model table = the one without an H$/U$/R$ prefix
    main_table_id = {}
    for row in rows.get("storage_tables", []):
        if not row["TableId"].startswith(("H$", "U$", "R$")):
            main_table_id[row["Table"]] = row["TableId"]
            stats.table_rows[row["Table"]] = _int(row["Rows"])

    # Data columns: (storage table id, storage column id) -> model column
    storage_key = {}
    for row in rows.get("storage_columns", []):
        dictionary_size = _int(row["DictionarySize"])
        # Every dictionary counts towards the table, including the hidden row-number column
        stats.table_sizes[row["Table"]] = stats.table_sizes.get(row["Table"], 0) + dictionary_size
        if row["ColumnType"] != "BASIC_DATA" or row["Column"].startswith("RowNumber-"):
            continue
        column = stats.columns.setdefault(
            (row["Table"], row["Column"]), ColumnStatistics(row["Table"], row["Column"])
        )
        column.dictionary_size += dictionary_size
        storage_key[(row["TableId"], row["ColumnId"])] = column

    hierarchy_owner = {
        f"H${table_id}${column_id}": col for (table_id, column_id), col in storage_key.items()
    }

    for row in rows.get("segments", []):
        size = _int(row["UsedSize"])
        stats.table_sizes[row["Table"]] = stats.table_sizes.get(row["Table"], 0) + size
        if (row["TableId"], row["ColumnId"]) in storage_key:
            storage_key[(row["TableId"], row["ColumnId"])].data_size += size
        elif row["TableId"] in hierarchy_owner:
            hierarchy_owner[row["TableId"]].hierarchy_size += size

    for row in rows.get("column_statistics", []):
        key = (row.get("Table Name", ""), row.get("Column Name", ""))
        if key[1].startswith("RowNumber-"):
            continue
        column = stats.columns.setdefault(key, ColumnStatistics(*key))
        column.distinct_values = _int(row.get("Cardinality"))

    return stats


def read_live_statistics(exe: Path, port: int, timeout: int = 600) -> LiveStatistics:
    """
    Read statistics from a local Power BI Desktop model through Tabular Editor 2.

    Only queries are run; the model is not changed or saved.

    Args:
        exe: TabularEditor.exe
        port: Local Analysis Services port (see find_local_instances())
        timeout: Seconds before giving up

    Returns:
        LiveStatistics (errors holds queries that failed, e.g. on old Desktop versions)

    Raises:
        RuntimeError: If Tabular Editor could not connect
        subprocess.TimeoutExpired: If it takes longer than timeout
    """
    with tempfile.TemporaryDirectory(prefix="pbixtractor_live_") as folder:
        script = Path(folder) / "statistics.cs"
        script.write_text(_STATISTICS_SCRIPT.replace("%FOLDER%", folder), encoding="utf-8")
        # "" = the only database on a Power BI Desktop instance
        process = subprocess.run(
            [str(exe), f"localhost:{port}", "", "-S", str(script)],
            capture_output=True,
            timeout=timeout,
        )
        files = {
            path.stem: path.read_bytes().decode("utf-8-sig") for path in Path(folder).glob("*.tsv")
        }
        if "tables" not in files:
            log = (process.stdout + process.stderr).decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Tabular Editor could not read the model on localhost:{port}: {log.strip()[-500:]}"
            )
        stats = build_statistics(files)
        stats.errors = {
            path.stem: path.read_text(encoding="utf-8-sig", errors="replace")
            for path in Path(folder).glob("*.error")
        }
        return stats


def collect_live_statistics(
    exe: Path, report_path: str | Path, model_tables: set[str]
) -> tuple[Optional[LiveStatistics], str]:
    """
    Read statistics for the report if it is open in Power BI Desktop.

    Args:
        exe: TabularEditor.exe
        report_path: The .pbix being documented
        model_tables: Table names from the .bim, used to verify the instance

    Returns:
        (statistics or None, human-readable note on what happened)
    """
    exact, unknown = find_instance_for_report(report_path)
    if exact is None and not unknown:
        return None, "report is not open in Power BI Desktop"

    for instance in ([exact] if exact else []) + unknown:
        stats = read_live_statistics(exe, instance.port)
        if instance is exact or tables_match(stats.tables, model_tables):
            if not tables_match(stats.tables, model_tables):
                note = f"localhost:{instance.port} (tables differ from the .bim - is it saved?)"
            else:
                note = f"localhost:{instance.port}"
            return stats, note

    return None, "no open Power BI Desktop model matches the .bim"
