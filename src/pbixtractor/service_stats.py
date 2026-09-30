"""Statistics of a semantic model in the Power BI service (read-only DAX queries).

Uses the Power BI executeQueries REST API, which only accepts plain DAX - no INFO functions or
DMVs (Microsoft docs). So from the service we get:
    - row counts per table            COUNTROWS, one query for all tables
    - distinct values per column      COLUMNSTATISTICS()
but not column/table sizes or measure data types (those need INFO/DMV queries, i.e. the XMLA
endpoint, or the report open in Power BI Desktop - see live_model.py).

DirectQuery tables are left out of the row counts: counting them would query the source.
Needs read + build permission on the model and the tenant setting "Dataset Execute Queries
REST API".

    stats = read_service_statistics(ServiceModel(workspace_id, model_id), ["Sales", "Dates"])
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .azure_auth import ApiError
from .live_model import LiveStatistics, statistics_from_rows

logger = logging.getLogger("pbixtractor")

COLUMN_STATISTICS = "EVALUATE COLUMNSTATISTICS()"


@dataclass
class ServiceModel:
    """A semantic model in a Fabric / Power BI workspace."""

    workspace_id: str
    model_id: str
    name: str = ""


def make_client():
    """The client used for the queries (tests replace this)."""
    from .fabric import FabricClient

    return FabricClient()


def row_count_query(tables: list[str]) -> str:
    """One DAX query returning (Table, Rows) for every table."""
    rows = [
        f'ROW("Table", "{name.replace(chr(34), chr(34) * 2)}", '
        f"\"Rows\", COUNTROWS('{name.replace(chr(39), chr(39) * 2)}'))"
        for name in tables
    ]
    return "EVALUATE " + (rows[0] if len(rows) == 1 else f"UNION({', '.join(rows)})")


def read_service_statistics(model: ServiceModel, tables: list[str], client=None) -> LiveStatistics:
    """
    Read row counts and distinct values of a semantic model in the service.

    Args:
        model: The model in the service
        tables: Tables to count (leave out DirectQuery tables and calculation groups)
        client: FabricClient (default: make_client())

    Returns:
        LiveStatistics without sizes (has_sizes is False); errors holds failed queries

    Raises:
        ApiError: If no query succeeded (e.g. no access, API disabled in the tenant)
    """
    client = client or make_client()
    rows, errors = {"tables": [{"Table": name} for name in tables]}, {}
    queries = {"column_statistics": COLUMN_STATISTICS}
    if tables:
        queries["row_counts"] = row_count_query(tables)
    for name, dax in queries.items():
        try:
            rows[name] = client.execute_query(model.workspace_id, model.model_id, dax)
        except ApiError as error:
            errors[name] = str(error)
    if len(errors) == len(queries):
        raise ApiError(f"No statistics could be read: {next(iter(errors.values()))}")

    # Row counts in the shape statistics_from_rows expects from INFO.STORAGETABLES
    rows["storage_tables"] = [
        {"Table": r["Table"], "TableId": r["Table"], "Rows": r["Rows"]}
        for r in rows.pop("row_counts", [])
    ]
    stats = statistics_from_rows(rows)
    stats.errors = errors
    return stats


def service_model_for_report(report_folder: Path) -> Optional[ServiceModel]:
    """The service model a report downloaded from Fabric came from (its fabric_source.json)."""
    source = Path(report_folder).parent / f"{Path(report_folder).stem}.fabric_source.json"
    try:
        info = json.loads(source.read_text(encoding="utf-8"))["semantic_model"]
        return ServiceModel(info["workspace_id"], info["id"], info.get("name", ""))
    except (OSError, ValueError, KeyError):
        return None
