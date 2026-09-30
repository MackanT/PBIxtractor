"""Web UI (NiceGUI): pick a report, run the documentation, and browse the results.

    pbixtractor            # starts this UI on http://localhost:8081
    pbixtractor web --port 9000 --no-browser

Runs locally; the browser is only the front end. The lineage viewer is shown inline, and all
output files can be downloaded. The extraction itself runs in a background thread
(pipeline.run_extraction) while progress and log messages stream into the page.
"""

import asyncio
import logging
import os
import queue
import re
import secrets
import time
import webbrowser
from collections import Counter
from pathlib import Path
from typing import Optional

from fastapi import HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from nicegui import app, background_tasks, events, run, ui

from . import __version__
from .azure_auth import ApiError
from .data import DATA_DIR
from .live_model import find_local_instances
from .pipeline import (
    REPORT_SUFFIXES,
    ExtractionOptions,
    ExtractionResult,
    find_model_for_report,
    report_name,
    run_extraction,
)
from .tabular_editor import add_tabular_editor_location, find_tabular_editor
from .web_sources import DevOpsPanel, FabricPanel, Fetched, stored_choices

# Output folders of runs in this session, served read-only under /files/<token>/<file name>
_OUTPUT_DIRS: dict[str, Path] = {}

def default_catalog_dir() -> Path:
    """Default catalog folder: inside output/, which is gitignored (client DAX stays out of git)."""
    return Path.cwd() / "output" / "_catalog"


UPLOAD_SUFFIXES = (".pbix", ".bim")
MAX_UPLOAD_BYTES = 2_000_000_000  # real .pbix files reach several hundred MB
UPLOAD_FOLDER = Path("output") / "_uploads"  # dropped files are copied here (CWD-relative)
# After a restart the open tab reconnects on its own (socket.io retries at most every 5 s, then
# the page reloads because the new server does not know it); only open a new tab if none does
BROWSER_GRACE_SECONDS = 7

FILE_LABELS = {
    "workbook": ("Documentation workbook", "table_view"),
    "data_workbook": ("Data workbook", "dataset"),
    "lineage": ("Lineage viewer", "account_tree"),
    "json": ("JSON", "data_object"),
    "graph": ("Relationship graph", "hub"),
    "tsv": ("Tabular Editor TSV", "description"),
    "log": ("Log file", "article"),
}


# Generated pages (lineage viewer, catalog) run sandboxed: scripts yes, but in an opaque origin,
# so even a page built from a hostile report could not act as the app (same origin) in the UI
_SANDBOX = {"Content-Security-Policy": "sandbox allow-scripts allow-popups allow-downloads"}


def _serve_output_file(token: str, name: str) -> FileResponse:
    """Serve a file from a run's output folder (only folders created by this app)."""
    folder = _OUTPUT_DIRS.get(token)
    path = (folder / name).resolve() if folder else None
    if path is None or path.parent != folder.resolve() or not path.is_file():
        raise HTTPException(status_code=404)
    return FileResponse(path, headers=_SANDBOX)


# The catalog folder in use (set when a run adds to it or the page finds one), served at /catalog/
_CATALOG = {"dir": None}


def _serve_catalog_file(path: str) -> FileResponse:
    """
    Serve the catalog: catalog.html, catalog.json and models/<key>/<file> only - never other
    files that happen to be in (or below) the chosen catalog folder.
    """
    folder = _CATALOG["dir"]
    if folder is None:
        raise HTTPException(status_code=404)
    root = folder.resolve()
    target = (root / path).resolve()
    allowed = target.parent == root and target.name in ("catalog.html", "catalog.json")
    allowed |= target.parent.parent == root / "models"  # models/<key>/<file>, checked resolved
    if not allowed or not target.is_file():
        raise HTTPException(status_code=404)
    return FileResponse(target, headers=_SANDBOX)


def _allowed_hosts(port: int) -> set[str]:
    return {f"127.0.0.1:{port}", f"localhost:{port}", f"[::1]:{port}"}


def add_host_check(port: int) -> None:
    """
    Answer only requests addressed to this machine's loopback name (DNS rebinding: a web page
    the user visits could otherwise point its own host name at 127.0.0.1 and use the app,
    including the file picker and the cached sign-in).
    """
    allowed = _allowed_hosts(port)

    @app.middleware("http")
    async def check_host(request, call_next):
        if request.headers.get("host", "").lower() not in allowed:
            return PlainTextResponse("Unknown host", status_code=421)
        return await call_next(request)


def register_routes() -> None:
    """Add the output file routes (the lineage iframe and the catalog)."""
    app.add_api_route("/files/{token}/{name}", _serve_output_file, methods=["GET"])
    app.add_api_route("/catalog/{path:path}", _serve_catalog_file, methods=["GET"])


# ============================================================================
# Local file picker (the browser cannot hand out local paths, the server can)
# ============================================================================


class PathPicker(ui.dialog):
    """Browse the local file system and pick a file (or a PBIP .Report folder)."""

    def __init__(self, start: Path, suffixes: tuple[str, ...], allow_report_folders: bool = False):
        super().__init__()
        self.suffixes = tuple(s.lower() for s in suffixes)
        self.allow_report_folders = allow_report_folders
        self.path = start if start.is_dir() else Path.home()
        with self, ui.card().classes("w-[640px] max-w-full"):
            with ui.row().classes("w-full items-center no-wrap"):
                ui.button(icon="arrow_upward", on_click=self._up).props("flat round dense")
                self.location = ui.label().classes("text-sm break-all grow")
            self.listing = ui.column().classes("w-full h-[420px] overflow-auto gap-0")
            with ui.row().classes("w-full justify-end"):
                ui.button("Cancel", on_click=lambda: self.submit(None)).props("flat")
        self._refresh()

    def _up(self) -> None:
        if self.path.parent != self.path:
            self.path = self.path.parent
            self._refresh()

    def _open(self, path: Path) -> None:
        if path.is_dir() and not (self.allow_report_folders and path.name.endswith(".Report")):
            self.path = path
            self._refresh()
        else:
            self.submit(path)

    def _refresh(self) -> None:
        self.location.text = str(self.path)
        self.listing.clear()
        try:
            entries = sorted(self.path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
        except OSError as e:
            with self.listing:
                ui.label(f"Cannot open this folder: {e}").classes("text-negative")
            return
        with self.listing:
            for entry in entries:
                if entry.name.startswith("."):
                    continue
                is_report_folder = self.allow_report_folders and entry.name.endswith(".Report")
                if entry.is_dir() or entry.suffix.lower() in self.suffixes or is_report_folder:
                    icon = "description" if entry.is_file() or is_report_folder else "folder"
                    with ui.item(on_click=lambda e=entry: self._open(e)).props("dense clickable"):
                        with ui.item_section().props("avatar"):
                            ui.icon(icon, color="primary" if icon == "description" else None)
                        ui.item_section(entry.name)


# ============================================================================
# Page
# ============================================================================


def _stats(report_json: dict) -> list[tuple[str, object, str]]:
    """Headline numbers for the result: (label, value, colour)."""
    pages = report_json["report"]["pages"]
    items = [item for page in pages for item in page["items"]]
    measures = sum(len(t["measures"]) for t in report_json["model"]["tables"])
    bookmarks = report_json["report"].get("bookmarks", [])
    unused_bookmarks = sum(1 for b in bookmarks if not b["used_by"])
    broken_bookmarks = sum(1 for b in bookmarks if b.get("broken"))
    broken = sum(1 for i in items if i.get("broken"))
    quality = Counter(v["severity"] for v in report_json["quality"] or [])
    stats = [
        ("Pages", len(pages), "primary"),
        ("Visuals", sum(1 for i in items if i["type"] in ("Visual", "Slicer")), "primary"),
        ("Tables", len(report_json["model"]["tables"]), "primary"),
        ("Measures", measures, "primary"),
        ("Unused columns", len(report_json["unused"]["columns"]), "warning"),
        ("Unused measures", len(report_json["unused"]["measures"]), "warning"),
        ("Buttons", sum(1 for i in items if i["type"] == "Button"), "primary"),
        ("Broken buttons", broken, "negative" if broken else "positive"),
        ("Unused bookmarks", unused_bookmarks, "warning" if unused_bookmarks else "positive"),
    ]
    if broken_bookmarks:
        stats.append(("Broken bookmarks", broken_bookmarks, "negative"))
    if report_json["quality"] is not None:
        stats.append(("BPA high", quality.get("High", 0), "negative"))
        stats.append(("BPA medium", quality.get("Medium", 0), "warning"))
    return stats


def _table(rows: list[dict], columns: list[tuple[str, str]], empty: str) -> None:
    if not rows:
        ui.label(empty).classes("text-grey-7 q-pa-md")
        return
    ui.table(
        rows=rows,
        columns=[
            {"name": key, "label": label, "field": key, "align": "left", "sortable": True}
            for key, label in columns
        ],
        row_key="_id",
        pagination=25,
    ).classes("w-full").props("dense flat wrap-cells")


def _log_text(logs: str) -> None:
    """Show log text as plain, escaped text (ui.code renders markdown: a report name holding a
    code fence could inject links or images there)."""
    ui.label(logs).classes(
        "w-full whitespace-pre-wrap break-words font-mono text-sm q-pa-sm rounded "
        "bg-grey-2 dark:bg-grey-9"
    ).mark("log_text")


def _log_count(logs: str) -> int:
    """Number of log messages (a message can span several lines, e.g. Tabular Editor output)."""
    return len(re.findall(r"^(?:DEBUG|INFO|WARNING|ERROR|CRITICAL): ", logs or "", re.MULTILINE))


def _render_result(container: ui.element, result: ExtractionResult, options: ExtractionOptions):
    """Status, headline numbers, downloads and result tabs."""
    container.clear()
    with container:
        colour = {"success": "positive", "warnings": "warning", "error": "negative"}[result.status]
        icon = {"success": "check_circle", "warnings": "warning", "error": "error"}[result.status]
        with ui.card().classes("w-full"):
            with ui.row().classes("items-center"):
                ui.icon(icon, color=colour, size="md")
                ui.label(
                    {"success": "Done", "warnings": "Done with warnings", "error": "Failed"}[
                        result.status
                    ]
                ).classes("text-h6")
                ui.label(f"{result.message} ({result.seconds}s)").classes("text-grey-7 break-all")

        if not result.ok or result.report_json is None:
            if result.logs:
                _log_text(result.logs)
            return

        token = secrets.token_urlsafe(8)
        _OUTPUT_DIRS[token] = options.output_dir

        with ui.row().classes("w-full gap-2"):
            for label, value, stat_colour in _stats(result.report_json):
                with ui.card().classes("q-pa-sm min-w-[120px]").props("flat bordered"):
                    ui.label(str(value)).classes(f"text-h5 text-{stat_colour}")
                    ui.label(label).classes("text-caption text-grey-7")

        with ui.row().classes("w-full gap-2 items-center"):
            for kind, path in result.files.items():
                if kind == "catalog":
                    _CATALOG["dir"] = Path(path).parent
                    ui.button(
                        "Open catalog",
                        icon="menu_book",
                        on_click=lambda: ui.navigate.to("/catalog/catalog.html", new_tab=True),
                    ).props("outline no-caps").mark("open_catalog")
                    continue
                label, file_icon = FILE_LABELS.get(kind, (kind, "download"))
                ui.button(
                    label, icon=file_icon, on_click=lambda p=path: ui.download.file(p)
                ).props("outline no-caps")
            if hasattr(os, "startfile"):
                ui.button(
                    "Open folder",
                    icon="folder_open",
                    on_click=lambda: os.startfile(options.output_dir),  # noqa: S606 (local app)
                ).props("flat no-caps")

        doc = result.report_json
        with ui.tabs().classes("w-full") as tabs:
            lineage_tab = ui.tab("Lineage", icon="account_tree")
            quality_tab = ui.tab("Model quality", icon="rule")
            unused_tab = ui.tab("Unused", icon="block")
            bookmarks_tab = ui.tab("Bookmarks", icon="bookmarks")
            log_count = _log_count(result.logs)
            log_tab = ui.tab(f"Log ({log_count})" if log_count else "Log", icon="article")
        with ui.tab_panels(tabs, value=lineage_tab).classes("w-full"):
            with ui.tab_panel(lineage_tab).classes("p-0"):
                lineage_url = f"/files/{token}/{result.files['lineage'].name}"
                with ui.row().classes("w-full justify-end"):
                    ui.link("Open in a new tab", lineage_url, new_tab=True).classes("text-sm")
                ui.element("iframe").props(f'src="{lineage_url}"').classes(
                    "w-full h-[85vh] min-h-[600px] border rounded"
                )
            with ui.tab_panel(quality_tab):
                if doc["quality"] is None:
                    ui.label(
                        "Best Practice Analyzer did not run (Tabular Editor analysis off or "
                        "Tabular Editor 2 not found)."
                    ).classes("text-grey-7")
                else:
                    counts = Counter(v["rule"] for v in doc["quality"])
                    first = {}
                    for violation in doc["quality"]:
                        first.setdefault(violation["rule"], violation)
                    _table(
                        [
                            {
                                "_id": rule,
                                "severity": v["severity"],
                                "category": v["category"],
                                "rule": rule,
                                "findings": counts[rule],
                            }
                            for rule, v in first.items()
                        ],
                        [
                            ("severity", "Severity"),
                            ("category", "Category"),
                            ("rule", "Rule"),
                            ("findings", "Findings"),
                        ],
                        "No findings.",
                    )
            with ui.tab_panel(unused_tab):
                _table(
                    [
                        {"_id": f"{kind}:{ref}", "object": ref, "type": kind}
                        for kind, refs in (
                            ("Measure", doc["unused"]["measures"]),
                            ("Column", doc["unused"]["columns"]),
                        )
                        for ref in refs
                    ],
                    [("object", "Object"), ("type", "Type")],
                    "Everything is used.",
                )
            with ui.tab_panel(bookmarks_tab):
                broken_buttons = [
                    {
                        "_id": f"{page['name']}/{item['id']}",
                        "page": page["name"],
                        "button": item.get("label") or item["id"],
                        "action": item.get("action", ""),
                        "target": item.get("target", ""),
                    }
                    for page in doc["report"]["pages"]
                    for item in page["items"]
                    if item.get("broken")
                ]
                if broken_buttons:
                    ui.label("Broken buttons (bookmark or page deleted)").classes(
                        "text-subtitle2 text-negative"
                    )
                    _table(
                        broken_buttons,
                        [
                            ("page", "Page"),
                            ("button", "Button"),
                            ("action", "Action"),
                            ("target", "Target"),
                        ],
                        "",
                    )
                    ui.label("Bookmarks").classes("text-subtitle2 q-mt-md")
                _table(
                    [
                        {
                            "_id": b["id"],
                            "name": b["name"],
                            "page": b["page"] or "",
                            "captures": ", ".join(b["captures"]),
                            "applies_to": b["applies_to"],
                            "used_by": ", ".join(b["used_by"]) or "(not used by any button)",
                        }
                        for b in doc["report"].get("bookmarks", [])
                    ],
                    [
                        ("name", "Bookmark"),
                        ("page", "Page"),
                        ("captures", "Captures"),
                        ("applies_to", "Applies to"),
                        ("used_by", "Used by buttons"),
                    ],
                    "No bookmarks.",
                )
            with ui.tab_panel(log_tab):
                if result.logs:
                    _log_text(result.logs)
                else:
                    ui.label("No warnings.").classes("text-grey-7")


def index() -> None:
    """The single page of the app (the root page passed to ui.run)."""
    tabular_editor = find_tabular_editor()

    with ui.header().classes("items-center justify-between"):
        with ui.row().classes("items-center gap-2"):
            ui.icon("insights", size="md")
            ui.label("PBIxtractor").classes("text-h6")
            ui.label(f"v{__version__}").classes("text-caption opacity-70")
        with ui.row().classes("items-center gap-4"):
            known = Path(stored_choices().get("catalog_dir") or default_catalog_dir())
            if (known / "catalog.html").is_file():
                _CATALOG["dir"] = known
                ui.link("Catalog", "/catalog/catalog.html", new_tab=True).classes(
                    "text-white text-sm"
                ).mark("header_catalog")
            ui.label("Power BI report documentation").classes("text-caption opacity-80")

    with ui.column().classes("w-full max-w-[1400px] mx-auto q-pa-md gap-4"):
        with ui.row().classes("w-full gap-4 items-stretch"):
            # ---------------- inputs ----------------
            # basis-0: long help texts wrap instead of pushing the Environment card down
            with ui.card().classes("grow basis-0 min-w-[420px]"):
                with ui.row().classes("w-full items-center justify-between"):
                    ui.label("Report").classes("text-subtitle1 text-weight-medium")
                    source = (
                        ui.toggle(
                            {"local": "Local file", "fabric": "Fabric", "devops": "Azure DevOps"},
                            value="local",
                        )
                        .props("no-caps dense unelevated")
                        .mark("source")
                    )
                with ui.column().classes("w-full gap-2") as local_box:

                    def picker_button(target: ui.input, suffixes, report_folders=False):
                        async def pick() -> None:
                            current = Path(target.value) if target.value else Path.home()
                            chosen = await PathPicker(
                                current.parent if current.suffix else current,
                                suffixes,
                                report_folders,
                            )
                            if chosen:
                                target.value = str(chosen)

                        ui.button(icon="folder_open", on_click=pick).props("flat round")

                    with ui.row().classes("w-full items-center no-wrap"):
                        report_input = ui.input(
                            "Report (.pbix, .pbip or .Report folder)",
                            placeholder=r"C:\Reports\Sales.pbix",
                        ).classes("grow").mark("report")
                        picker_button(report_input, REPORT_SUFFIXES, report_folders=True)
                    with ui.row().classes("w-full items-center no-wrap"):
                        model_input = ui.input(
                            "Model (.bim or TMDL model.tmdl) - found automatically for most reports"
                        ).classes("grow").mark("model")
                        picker_button(model_input, (".bim", ".tmdl"))

                    # Drag & drop: the browser never reveals a dropped file's path, so the file is
                    # copied to output/_uploads and that copy is documented
                    last_model_upload = {"time": 0.0}

                    async def on_upload(event: events.UploadEventArguments) -> None:
                        name = Path(event.file.name).name
                        suffix = Path(name).suffix.lower()
                        if suffix not in UPLOAD_SUFFIXES:
                            ui.notify(f"{name}: drop a .pbix report or a .bim model.", type="warning")
                            return
                        target = Path.cwd() / UPLOAD_FOLDER / name
                        await event.file.save(target)
                        if suffix == ".bim":
                            model_input.value = str(target)
                            last_model_upload["time"] = time.monotonic()
                        else:
                            # A new report: find its model again, unless one came in the same drop
                            if time.monotonic() - last_model_upload["time"] > 30:
                                model_input.value = ""
                            report_input.value = str(target)
                        ui.notify(f"{name} copied to {target.parent}", type="positive")

                    ui.upload(
                        label="…or drop a .pbix and/or .bim file here",
                        multiple=True,
                        auto_upload=True,
                        on_upload=on_upload,
                        max_file_size=MAX_UPLOAD_BYTES,
                        on_rejected=lambda: ui.notify(
                            f"Files larger than {MAX_UPLOAD_BYTES // 1_000_000_000} GB are not "
                            "accepted - use the folder picker for those.",
                            type="warning",
                        ),
                    ).props('accept=".pbix,.bim" flat bordered').classes("w-full").mark("upload")
                local_box.bind_visibility_from(source, "value", value="local")

                # Remote sources: pick a report; it is downloaded when the documentation runs
                suggested_output = {"value": None}  # replaced by the model name in model mode

                def suggest_output(name: str) -> None:
                    output_input.value = suggested_output["value"] = str(Path.cwd() / "output" / name)

                remote = {}
                for key, panel_class in (("fabric", FabricPanel), ("devops", DevOpsPanel)):
                    with ui.column().classes("w-full gap-2") as box:
                        remote[key] = panel_class(on_choose=suggest_output)
                    box.bind_visibility_from(source, "value", value=key)

                output_input = ui.input("Output folder").classes("w-full").mark("output")

                with ui.expansion("Options", icon="tune").classes("w-full"):
                    te_analysis = ui.switch(
                        "Tabular Editor analysis: Best Practice Analyzer, exact DAX dependencies, "
                        "live statistics (runs locally)",
                        value=tabular_editor is not None,
                    )
                    te_tsv = ui.switch(
                        "Use Tabular Editor TSV export (formats DAX via daxformatter.com - "
                        "sends DAX online)",
                        value=False,
                    )
                    service_stats = ui.switch(
                        "Row counts and distinct values from the Power BI service (reports "
                        "downloaded from Fabric; read-only DAX queries)",
                        value=True,
                    )
                    log_file = ui.switch("Write warnings to a log file", value=True)
                    remembered = stored_choices()
                    with ui.row().classes("w-full items-center no-wrap gap-2"):
                        add_catalog = (
                            ui.switch("Add to catalog", value=bool(remembered.get("catalog_enabled")))
                            .tooltip(
                                "Also add this model to a catalog folder: one searchable page for "
                                "all documented models (the same model again replaces its entry)"
                            )
                            .mark("catalog_switch")
                        )
                        catalog_input = (
                            ui.input(
                                "Catalog folder",
                                value=remembered.get("catalog_dir") or str(default_catalog_dir()),
                            )
                            .classes("grow")
                            .bind_visibility_from(add_catalog, "value")
                            .mark("catalog_dir")
                        )
                    description_tag = ui.input(
                        "Description tag in DAX", value=ExtractionOptions.description_tag
                    ).classes("w-48")

            # ---------------- environment ----------------
            with ui.card().classes("w-[380px] max-w-full"):
                ui.label("Environment").classes("text-subtitle1 text-weight-medium")
                with ui.row().classes("items-center no-wrap"):
                    if tabular_editor:
                        ui.icon("check_circle", color="positive")
                        ui.label("Tabular Editor 2 found").tooltip(str(tabular_editor))
                    else:
                        ui.icon("cancel", color="negative")
                        ui.label(
                            "Tabular Editor 2 not found - BPA, exact dependencies and live "
                            "statistics are unavailable"
                        ).classes("text-sm")
                if not tabular_editor:

                    def save_location() -> None:
                        if add_tabular_editor_location(te_folder.value or ""):
                            ui.navigate.reload()
                        else:
                            ui.notify("No TabularEditor.exe in that folder.", type="warning")

                    with ui.row().classes("w-full items-center no-wrap"):
                        te_folder = (
                            ui.input("Tabular Editor 2 folder", placeholder=r"C:\Tools\TabularEditor")
                            .classes("grow")
                            .mark("te_folder")
                        )
                        ui.button("Save", on_click=save_location).props("flat dense no-caps")
                desktop_box = ui.column().classes("gap-1")

                refresh_state = {"seq": 0}

                async def refresh_desktop(delay: float = 0.0) -> None:
                    """Scan for Power BI Desktop off the event loop (psutil can take seconds);
                    while typing, only the last change within `delay` seconds scans."""
                    refresh_state["seq"] += 1
                    seq = refresh_state["seq"]
                    if delay:
                        await asyncio.sleep(delay)
                        if seq != refresh_state["seq"]:
                            return
                    instances = await run.io_bound(find_local_instances)
                    if seq != refresh_state["seq"] or desktop_box.is_deleted:
                        return  # a newer scan runs, or the page is gone
                    desktop_box.clear()
                    selected = Path(report_input.value) if report_input.value else None
                    with desktop_box:
                        if not instances:
                            with ui.row().classes("items-center no-wrap"):
                                ui.icon("info", color="grey")
                                ui.label(
                                    "No report open in Power BI Desktop (open it to add row "
                                    "counts, sizes and measure types)"
                                ).classes("text-sm text-grey-8")
                        for instance in instances:
                            name = instance.report_path.name if instance.report_path else "?"
                            match = bool(
                                selected
                                and instance.report_path
                                and os.path.normcase(instance.report_path)
                                == os.path.normcase(selected)
                            )
                            with ui.row().classes("items-center no-wrap"):
                                ui.icon(
                                    "monitoring", color="positive" if match else "grey"
                                ).classes("flex-none")
                                ui.label(
                                    f"Desktop: {name} (port {instance.port})"
                                    + (" - live statistics will be included" if match else "")
                                ).classes("text-sm")

                ui.button("Refresh", icon="refresh", on_click=lambda: refresh_desktop()).props(
                    "flat dense no-caps"
                )
                ui.timer(0.05, lambda: refresh_desktop(), once=True)  # after the page is sent

        # ---------------- run ----------------
        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center"):
                run_button = ui.button("Create documentation", icon="play_arrow").props(
                    "unelevated no-caps"
                )
                step_label = ui.label("").classes("text-grey-7")
            progress_bar = ui.linear_progress(value=0, show_value=False).classes("w-full")
            progress_bar.visible = False
            log_view = ui.log(max_lines=400).classes("w-full h-40")
            log_view.visible = False

    # Results use the full window width: the lineage viewer needs the room
    result_box = ui.column().classes("w-full q-px-md q-pb-md gap-3")

    # ---------------- behaviour ----------------
    def on_report_change() -> None:
        value = (report_input.value or "").strip().strip('"')
        if value != report_input.value:
            report_input.value = value
            return
        if not value:
            return
        path = Path(value)
        model = find_model_for_report(path)
        # Replace an empty box or the model found for the PREVIOUS report; keep one the user
        # chose (typed, picked or dropped) - it may be deliberate for this report too
        if not model_input.value or model_input.value == auto_model["value"]:
            model_input.value = str(model) if model else ""
            auto_model["value"] = model_input.value or None
        output_input.value = str(Path.cwd() / "output" / report_name(path))
        background_tasks.create(refresh_desktop(delay=0.5), name="refresh_desktop")

    auto_model = {"value": None}  # the model value on_report_change filled in last
    report_input.on_value_change(lambda _: on_report_change())

    async def download_remote_report(panel) -> Optional[Fetched]:
        """Fetch the chosen Fabric/DevOps report; None on error.

        The download has no measurable fraction (Fabric prepares the definition in the
        background), so the bar is indeterminate and the label shows the current step.
        """
        step_label.text = "Downloading the report…"
        messages: queue.Queue = queue.Queue()

        def show_progress() -> None:
            while not messages.empty():
                step_label.text = messages.get_nowait()

        timer = ui.timer(0.2, show_progress)
        run_button.disable()
        progress_bar.props("indeterminate")
        progress_bar.visible = True
        try:
            return await run.io_bound(panel.fetch, messages.put)
        except (ApiError, ValueError, OSError) as error:
            ui.notify(f"Download failed: {error}", type="negative", multi_line=True, timeout=20000)
            return None
        except Exception as error:  # UI boundary: never let a click end without any message
            logging.getLogger("pbixtractor").exception(f"Download failed unexpectedly: {error}")
            ui.notify(f"Download failed: {error}", type="negative", multi_line=True, timeout=20000)
            return None
        finally:
            timer.cancel()
            run_button.enable()
            progress_bar.props(remove="indeterminate")
            progress_bar.visible = False
            step_label.text = ""

    async def start_run() -> None:
        """One download + documentation run at a time across all tabs: a second tab's download
        replaces the same _fabric/_devops folder, and extractions share the logger."""
        if _JOB_LOCK.locked():
            run_button.disable()
            step_label.text = "Waiting for a run in another tab to finish…"
        try:
            async with _JOB_LOCK:
                step_label.text = ""
                await _start_run()
        finally:
            run_button.enable()

    async def _start_run() -> None:
        if source.value != "local":
            panel = remote[source.value]
            if not panel.ready():
                ui.notify("Choose a report first.", type="warning")
                return
            result_box.clear()
            fetched = await download_remote_report(panel)
            if fetched is None:
                return
            report, model, extra_reports = fetched.report_folder, fetched.model_path, fetched.extra_reports
            name = fetched.name
            if fetched.model_mode and output_input.value == suggested_output["value"]:
                output_input.value = str(Path.cwd() / "output" / name)  # named after the model
            not_included = list(fetched.skipped)  # also logged as warnings of the run
            if fetched.skipped:
                ui.notify(
                    "Not included (could not be downloaded) - 'unused' may be incomplete: "
                    + "; ".join(fetched.skipped),
                    type="warning",
                    multi_line=True,
                    timeout=30000,
                )
        else:
            extra_reports, name, not_included = [], "", []
            report = Path((report_input.value or "").strip('"'))
            model_value = (model_input.value or "").strip('"')
            if not report_input.value or not report.exists():
                ui.notify("Choose an existing report first.", type="warning")
                return
            model = Path(model_value) if model_value else find_model_for_report(report)
            if model is None or not model.exists():
                ui.notify(
                    "Choose the model (.bim, or model.tmdl of a TMDL model) that belongs to the "
                    "report.",
                    type="warning",
                )
                return

        options = ExtractionOptions(
            report_path=report,
            model_path=model,
            output_dir=Path(output_input.value or Path.cwd() / "output" / (name or report_name(report))),
            name=name,
            description_tag=description_tag.value or ExtractionOptions.description_tag,
            tabular_editor_analysis=te_analysis.value,
            tabular_editor_tsv=te_tsv.value,
            write_log_file=log_file.value,
            extra_reports=extra_reports,
            not_included=not_included,
            service_statistics=service_stats.value,
            catalog_dir=Path(catalog_input.value) if add_catalog.value and catalog_input.value else None,
        )
        remembered = stored_choices()
        remembered["catalog_enabled"] = add_catalog.value
        if catalog_input.value:
            remembered["catalog_dir"] = catalog_input.value

        events: queue.Queue = queue.Queue()

        def drain() -> None:
            while not events.empty():
                kind, first, second = events.get_nowait()
                if kind == "progress":
                    step_label.text = first
                    progress_bar.value = second
                else:
                    log_view.push(f"{first}: {second}")
                    log_view.visible = True

        run_button.disable()
        result_box.clear()
        log_view.clear()
        log_view.visible = False
        progress_bar.visible = True
        progress_bar.value = 0
        timer = ui.timer(0.2, drain)
        try:
            result: ExtractionResult = await run.io_bound(
                run_extraction,
                options,
                lambda step, fraction: events.put(("progress", step, fraction)),
                lambda level, message: events.put(("log", level, message)),
            )
        finally:
            timer.cancel()
            drain()
            run_button.enable()
            progress_bar.visible = False

        step_label.text = ""
        # The live log is only for following the run; the result shows the same messages
        log_view.visible = False
        _render_result(result_box, result, options)
        ui.notify(
            result.message if result.ok else f"Failed: {result.message}",
            type={"success": "positive", "warnings": "warning", "error": "negative"}[result.status],
        )

    run_button.on_click(start_run)


_JOB_LOCK = asyncio.Lock()


async def _open_browser_unless_reconnected(url: str, connected: asyncio.Event) -> None:
    """Open a browser tab, unless a tab from before a restart reconnects within the grace time."""
    try:
        await asyncio.wait_for(connected.wait(), timeout=BROWSER_GRACE_SECONDS)
    except asyncio.TimeoutError:
        webbrowser.open(url)


def start(port: int = 8081, open_browser: bool = True, host: Optional[str] = None) -> None:
    """
    Start the web UI (blocks until stopped).

    With open_browser, a tab is opened only when no existing tab connects within a few seconds:
    after a restart the open tab reconnects by itself, so no duplicate tab appears.
    """
    register_routes()
    add_host_check(port)
    if open_browser:
        connected = asyncio.Event()
        app.on_connect(connected.set)
        url = f"http://127.0.0.1:{port}/"
        app.on_startup(
            lambda: background_tasks.create(_open_browser_unless_reconnected(url, connected))
        )
    ui.run(
        index,
        title="PBIxtractor",
        host=host or "127.0.0.1",
        port=port,
        reload=False,
        show=False,  # opened above, unless a tab reconnects
        dark=None,  # follow the system theme
        favicon=DATA_DIR / "logo.ico",
        show_welcome_message=True,
    )
