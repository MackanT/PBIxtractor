"""Document many semantic models in one go - a whole Fabric workspace (or several), or several
report files - one documentation run per model, each added to one catalog.

    jobs = plan_fabric(client, ["Sales WS"], output_root=Path("output"))  # list calls only
    jobs = plan_files([Path("A.pbix"), Path("B.pbix")])                    # local files
    outcomes = run_batch(jobs, BatchSettings(Path("output"), Path("output/_catalog")))

Planning only lists items, so it is quick; a job downloads its model and reports when its turn
comes. Reports are grouped by the model they use: a model's documentation includes all its
reports found (so "unused" means unused by all of them, and its catalog entry is complete). One
failing model never stops the others - its outcome says why.
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from . import fabric
from .azure_auth import ApiError
from .pbix_model import LiveConnection, live_connection
from .pipeline import (
    ExtractionOptions,
    find_model_for_report,
    model_name,
    report_name,
    run_extraction,
)
from .service_stats import ServiceModel
from .utils import safe_name

logger = logging.getLogger("pbixtractor")

Say = Callable[[str], None]


@dataclass
class Prepared:
    """What a job documents, once its files are local."""

    report_paths: list[Path]
    model_path: Path
    model: str = ""  # the model's name, when only a download tells it (a published model)
    name: str = ""  # output name, when only a download tells it
    service_model: Optional[ServiceModel] = None
    not_included: list[str] = field(default_factory=list)  # reports that could not be downloaded


@dataclass
class BatchJob:
    """One semantic model, and the reports on it, to document."""

    model: str  # the model's name as shown
    reports: list[str]  # names of the reports on it
    source: str  # where it comes from: "Fabric · <workspace>", "Files", ...
    prepare: Optional[Callable[[Say], Prepared]] = None  # gets the files (downloads them)
    skip_reason: str = ""  # why it cannot be documented: shown, never run
    name: str = ""  # output folder and file name (default: the one report's, else the model's)
    qualifier: str = ""  # tells two jobs with the same name apart (e.g. the model's workspace)
    downloads: bool = False  # prepare() downloads (slow, can fail); else the files are local

    def __post_init__(self):
        if not self.name:
            self.name = self.reports[0] if len(self.reports) == 1 else self.model


@dataclass
class JobOutcome:
    """How a job went: status "running" while it runs, then "success" | "warnings" | "error"
    | "skipped"."""

    job: BatchJob
    status: str
    message: str
    options: Optional[ExtractionOptions] = None  # the run's options (output folder, name, ...)
    files: dict[str, Path] = field(default_factory=dict)
    logs: str = ""
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status in ("success", "warnings")


@dataclass
class BatchSettings:
    """Options shared by every run of a batch."""

    output_root: Path  # each model's documentation goes to <root>/<name>
    catalog_dir: Path  # every model is added to this catalog
    tabular_editor_analysis: bool = True
    tabular_editor_tsv: bool = False
    service_statistics: bool = True
    write_log_file: bool = True
    description_tag: str = ExtractionOptions.description_tag


# ============================================================================
# Planning: local files
# ============================================================================


def _key(path: Path) -> str:
    return os.path.normcase(str(Path(path).resolve()))


def _local_job(reports: list[Path], model: Path) -> BatchJob:
    def prepare(say: Say) -> Prepared:
        return Prepared(list(reports), model)

    return BatchJob(model_name(model), [report_name(r) for r in reports], "Files", prepare)


def _published_job(
    reference: LiveConnection,
    reports: list[Path],
    make_client: Optional[Callable[[], "fabric.FabricClient"]],
    output_root: Path,
) -> BatchJob:
    """Live-connected reports on one published model: it is downloaded from Fabric."""

    def prepare(say: Say) -> Prepared:
        fetched = fabric.fetch_connected_model(
            make_client(), reference.model_id, reference.workspace, Path(output_root) / "_fabric", say
        )
        return Prepared(
            list(reports),
            fetched.model_path,
            model=fetched.model,
            name=fetched.model if len(reports) > 1 else "",
            service_model=ServiceModel(fetched.workspace_id, fetched.model_id, fetched.model),
        )

    where = f" in {reference.workspace}" if reference.workspace else ""
    return BatchJob(
        f"Published model{where}",
        [report_name(r) for r in reports],
        "Fabric (published model)",
        prepare if make_client else None,
        skip_reason="" if make_client else "Its model is published in the Power BI service",
        name=report_name(reports[0]) if len(reports) == 1 else "",
        downloads=True,
    )


def plan_files(
    reports: list[Path],
    models: list[Path] = (),
    chosen: Optional[Path] = None,
    make_client: Optional[Callable[[], "fabric.FabricClient"]] = None,
    output_root: Path = Path("output"),
) -> list[BatchJob]:
    """
    Group report files by their semantic model: one job per model.

    Args:
        reports: .pbix / .pbip / .Report paths
        models: Model files given with them (.bim): one is used for all the reports; with
            several, each report uses the one named like it (<name>.bim next to it)
        chosen: One model for all the reports (overrides their own)
        make_client: Makes a FabricClient for live-connected reports (their model is published)
        output_root: Published models are downloaded to <root>/_fabric

    Returns:
        Jobs in the order of the reports; jobs that cannot run (no model, a model without
        report) last, with their skip_reason
    """
    reports = [Path(r) for r in reports]
    models = [Path(m) for m in models]
    if chosen is None and len(models) == 1:
        chosen = models[0]
    if chosen is not None:
        return [_local_job(reports, Path(chosen))]

    local: dict[str, tuple[Path, list[Path]]] = {}
    published: dict[str, tuple[LiveConnection, list[Path]]] = {}
    skipped: list[BatchJob] = []
    for report in reports:
        model = find_model_for_report(report)  # its own (.pbix), <name>.bim, PBIP model
        if model is not None:
            local.setdefault(_key(model), (model, []))[1].append(report)
        elif (reference := live_connection(report)) is not None:
            published.setdefault(reference.model_id.lower(), (reference, []))[1].append(report)
        else:
            name = report_name(report)
            skipped.append(
                BatchJob(name, [name], "Files", skip_reason=f"No model found - add {name}.bim with it")
            )
    for model in models:
        if _key(model) not in local:
            skipped.append(
                BatchJob(model_name(model), [], "Files", skip_reason="No report on this model was given")
            )
    jobs = [_local_job(group, model) for model, group in local.values()]
    jobs += [
        _published_job(reference, group, make_client, output_root)
        for reference, group in published.values()
    ]
    return jobs + skipped


# ============================================================================
# Planning: Fabric workspaces
# ============================================================================


def _fabric_prepare(client, model_ws: dict, model: dict, reports: list[tuple[dict, dict]], destination: Path):
    def prepare(say: Say) -> Prepared:
        fetched = fabric.fetch_model_reports(client, model_ws, model, reports, destination, say)
        # Statistics: the pipeline reads the service model from <report>.fabric_source.json
        return Prepared(fetched.report_folders, fetched.model_path, not_included=list(fetched.skipped))

    return prepare


def plan_fabric(
    client: "fabric.FabricClient",
    workspaces: list[str],
    all_workspaces: bool = False,
    output_root: Path = Path("output"),
    progress: Optional[Say] = None,
) -> list[BatchJob]:
    """
    Every semantic model used by a report in the given workspaces (and every model in them):
    one job per model, with all its reports in the searched workspaces.

    Args:
        client: FabricClient
        workspaces: Workspace names or ids
        all_workspaces: Also look for the models' reports in every other workspace you can
            open (slower; else only the given workspaces are searched)
        output_root: Downloads go to <root>/_fabric/<model workspace>/<model>
        progress: Called with a short status text per step

    Returns:
        Jobs sorted by model name; jobs that cannot run (paginated reports, models without a
        report, unreachable models, unlistable workspaces) last, with their skip_reason

    Raises:
        FabricError: A workspace that does not exist (or that you cannot see)
    """
    say = progress or (lambda text: None)
    say("Listing your workspaces")
    accessible = client.workspaces()
    by_id = {w["id"].lower(): w for w in accessible}
    chosen: list[dict] = []
    for name_or_id in workspaces:
        ws = fabric._find(accessible, name_or_id, "workspace")
        if ws not in chosen:
            chosen.append(ws)
    chosen_ids = {w["id"].lower() for w in chosen}
    search = chosen + [w for w in accessible if w["id"].lower() not in chosen_ids] if all_workspaces else chosen

    listed_models: dict[str, dict[str, dict]] = {}

    def models_in(workspace_id: str) -> dict[str, dict]:
        key = workspace_id.lower()
        if key not in listed_models:
            try:
                listed_models[key] = {m["id"].lower(): m for m in client.semantic_models(workspace_id)}
            except ApiError as error:  # a workspace you see but cannot list items in
                logger.debug(f"Could not list the semantic models of workspace {workspace_id}: {error}")
                listed_models[key] = {}
        return listed_models[key]

    skipped: list[BatchJob] = []
    on_model: dict[str, list[tuple[dict, dict]]] = {}  # model id -> [(workspace, report)]
    model_workspace: dict[str, str] = {}  # model id -> its workspace id
    for ws in search:
        say(f"Listing the reports in {ws['displayName']}")
        try:
            reports = client.powerbi_reports(ws["id"])
        except ApiError as error:
            skipped.append(
                BatchJob(f"Workspace {ws['displayName']}", [], f"Fabric · {ws['displayName']}",
                         skip_reason=f"Its reports could not be listed: {error}")
            )
            continue
        for report in sorted(reports, key=lambda r: (r.get("name") or "").lower()):
            rep = {"id": report["id"], "displayName": report.get("name") or report["id"]}
            if report.get("reportType") == "PaginatedReport" or not report.get("datasetId"):
                if ws["id"].lower() in chosen_ids:
                    skipped.append(
                        BatchJob(rep["displayName"], [rep["displayName"]], f"Fabric · {ws['displayName']}",
                                 skip_reason="Paginated report - not supported"
                                 if report.get("reportType") == "PaginatedReport"
                                 else "No semantic model")
                    )
                continue
            model_id = report["datasetId"].lower()
            on_model.setdefault(model_id, []).append((ws, rep))
            if report.get("datasetWorkspaceId"):
                model_workspace.setdefault(model_id, report["datasetWorkspaceId"])

    # The models to document: used by a report in a chosen workspace, or living in one
    wanted = [m for m, group in on_model.items() if any(w["id"].lower() in chosen_ids for w, _ in group)]
    for ws in chosen:
        say(f"Listing the semantic models in {ws['displayName']}")
        for model_id, model in models_in(ws["id"]).items():
            model_workspace.setdefault(model_id, ws["id"])
            if model_id not in on_model:
                skipped.append(
                    BatchJob(model["displayName"], [], f"Fabric · {ws['displayName']}",
                             skip_reason="No report on this model"
                             + ("" if all_workspaces else " in the chosen workspaces"))
                )
            elif model_id not in wanted:
                wanted.append(model_id)

    jobs: list[BatchJob] = []
    for model_id in wanted:
        group = on_model[model_id]
        model_ws = by_id.get((model_workspace.get(model_id) or group[0][0]["id"]).lower())
        model = models_in(model_ws["id"]).get(model_id) if model_ws else None
        if model is None:  # not where the API says (or it did not say): the reports' workspaces
            for ws, _ in group:
                if (found := models_in(ws["id"]).get(model_id)) is not None:
                    model_ws, model = ws, found
                    break
        names = [rep["displayName"] for _, rep in group]
        if len(set(n.lower() for n in names)) < len(names):  # same name in several workspaces
            names = [f"{rep['displayName']} ({ws['displayName']})" for ws, rep in group]
        if model is None:
            skipped.append(
                BatchJob(f"Model {model_id}", names, "Fabric",
                         skip_reason="Its semantic model is in a workspace you cannot open")
            )
            continue
        destination = (
            Path(output_root) / "_fabric" / safe_name(model_ws["displayName"]) / safe_name(model["displayName"])
        )
        jobs.append(
            BatchJob(
                model["displayName"],
                names,
                f"Fabric · {model_ws['displayName']}",
                _fabric_prepare(client, model_ws, model, group, destination),
                qualifier=model_ws["displayName"],
                downloads=True,
            )
        )
    jobs.sort(key=lambda j: (j.model.lower(), j.qualifier.lower()))
    return jobs + skipped


# ============================================================================
# Running
# ============================================================================


def _unique(name: str, qualifier: str, used: set[str]) -> str:
    """A name no earlier job of the batch has: "<name> (<qualifier>)", else "<name> (2)"."""
    candidate = name
    if candidate.lower() in used and qualifier:
        candidate = safe_name(f"{name} ({qualifier})")
    base, number = candidate, 2
    while candidate.lower() in used:
        candidate, number = f"{base} ({number})", number + 1
    used.add(candidate.lower())
    return candidate


def _run_job(
    job: BatchJob,
    settings: BatchSettings,
    used: set[str],
    progress: Callable[[str, float], None],
    on_log: Optional[Callable[[str, str], None]],
) -> JobOutcome:
    progress("Getting the files", 0.0)
    try:
        prepared = job.prepare(lambda text: progress(text, 0.02))
    except (ApiError, ValueError, OSError) as error:
        return JobOutcome(job, "error", f"Download failed: {error}")
    except Exception as error:  # one model's failure never stops the batch
        logger.exception(f"Download of {job.model} failed: {error}")
        return JobOutcome(job, "error", f"Download failed: {error}")
    if prepared.model:
        job.model = prepared.model
    name = _unique(safe_name(prepared.name or job.name), job.qualifier, used)
    options = ExtractionOptions(
        report_path=prepared.report_paths[0],
        model_path=prepared.model_path,
        output_dir=Path(settings.output_root) / name,
        name=name,
        description_tag=settings.description_tag,
        tabular_editor_analysis=settings.tabular_editor_analysis,
        tabular_editor_tsv=settings.tabular_editor_tsv,
        write_log_file=settings.write_log_file,
        extra_reports=prepared.report_paths[1:],
        not_included=prepared.not_included,
        service_model=prepared.service_model,
        service_statistics=settings.service_statistics,
        catalog_dir=settings.catalog_dir,
    )
    result = run_extraction(
        options,
        progress=lambda step, fraction: progress(step, 0.05 + 0.95 * fraction),
        on_log=(lambda level, message: on_log(level, f"{job.model}: {message}")) if on_log else None,
    )
    return JobOutcome(job, result.status, result.message, options, result.files, result.logs, result.seconds)


def run_batch(
    jobs: list[BatchJob],
    settings: BatchSettings,
    progress: Optional[Callable[[str, float], None]] = None,
    on_log: Optional[Callable[[str, str], None]] = None,
    on_outcome: Optional[Callable[[int, JobOutcome], None]] = None,
    should_stop: Optional[Callable[[], bool]] = None,
) -> list[JobOutcome]:
    """
    Document each job's model in turn; every run adds its model to settings.catalog_dir.

    Args:
        jobs: From plan_files() / plan_fabric()
        settings: Output root, catalog and run options
        progress: Called with (step text, fraction of the whole batch)
        on_log: Called with (level, "<model>: <message>") for every log record of the runs
        on_outcome: Called with (job index, outcome) when a job starts (status "running")
            and when it ends
        should_stop: Checked before each job; True skips the rest ("stopped")

    Returns:
        One outcome per job, in order
    """
    progress = progress or (lambda text, fraction: None)
    outcomes: list[JobOutcome] = []
    used: set[str] = set()
    total = max(len(jobs), 1)
    for index, job in enumerate(jobs):
        if job.skip_reason or job.prepare is None:
            outcome = JobOutcome(job, "skipped", job.skip_reason or "Nothing to document")
        elif should_stop is not None and should_stop():
            outcome = JobOutcome(job, "skipped", "Stopped before this model")
        else:
            if on_outcome:
                on_outcome(index, JobOutcome(job, "running", "Running"))
            prefix = f"{index + 1}/{len(jobs)} {job.model}"
            outcome = _run_job(
                job,
                settings,
                used,
                lambda text, fraction, i=index, p=prefix: progress(f"{p}: {text}", (i + fraction) / total),
                on_log,
            )
        outcomes.append(outcome)
        if on_outcome:
            on_outcome(index, outcome)
    progress("Done", 1.0)
    return outcomes


def summary(outcomes: list[JobOutcome]) -> str:
    """ "3 of 4 models documented (1 with warnings), 1 failed, 2 skipped"."""
    documented = [o for o in outcomes if o.ok]
    runnable = [o for o in outcomes if o.status != "skipped" or o.message.startswith("Stopped")]
    text = f"{len(documented)} of {len(runnable)} model{'s' if len(runnable) != 1 else ''} documented"
    warned = sum(1 for o in documented if o.status == "warnings")
    if warned:
        text += f" ({warned} with warnings)"
    failed = sum(1 for o in outcomes if o.status == "error")
    if failed:
        text += f", {failed} failed"
    skipped = sum(1 for o in outcomes if o.status == "skipped")
    if skipped:
        text += f", {skipped} skipped"
    return text
