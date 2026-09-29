"""Web UI (NiceGUI): pick a report, run the documentation, and browse the results.

    pbixtractor            # starts this UI on http://localhost:8081
    pbixtractor web --port 9000 --no-browser

Runs locally; the browser is only the front end. The lineage viewer is shown inline, and all
output files can be downloaded. The extraction itself runs in a background thread
(pipeline.run_extraction) while progress and log messages stream into the page.
"""

import os
import queue
import secrets
from collections import Counter
from pathlib import Path
from typing import Optional

from fastapi import HTTPException
from fastapi.responses import FileResponse
from nicegui import app, run, ui

from . import __version__
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
from .tabular_editor import find_tabular_editor

# Output folders of runs in this session, served read-only under /files/<token>/<file name>
_OUTPUT_DIRS: dict[str, Path] = {}

FILE_LABELS = {
    "workbook": ("Documentation workbook", "table_view"),
    "data_workbook": ("Data workbook", "dataset"),
    "lineage": ("Lineage viewer", "account_tree"),
    "json": ("JSON", "data_object"),
    "graph": ("Relationship graph", "hub"),
    "tsv": ("Tabular Editor TSV", "description"),
    "log": ("Log file", "article"),
}


def _serve_output_file(token: str, name: str) -> FileResponse:
    """Serve a file from a run's output folder (only folders created by this app)."""
    folder = _OUTPUT_DIRS.get(token)
    path = (folder / name).resolve() if folder else None
    if path is None or path.parent != folder.resolve() or not path.is_file():
        raise HTTPException(status_code=404)
    return FileResponse(path)


def register_routes() -> None:
    """Add the output file route (served to the lineage iframe)."""
    app.add_api_route("/files/{token}/{name}", _serve_output_file, methods=["GET"])


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
    unused_bookmarks = sum(1 for b in report_json["report"].get("bookmarks", []) if not b["used_by"])
    quality = Counter(v["severity"] for v in report_json["quality"] or [])
    stats = [
        ("Pages", len(pages), "primary"),
        ("Visuals", sum(1 for i in items if i["type"] in ("Visual", "Slicer")), "primary"),
        ("Tables", len(report_json["model"]["tables"]), "primary"),
        ("Measures", measures, "primary"),
        ("Unused columns", len(report_json["unused"]["columns"]), "warning"),
        ("Unused measures", len(report_json["unused"]["measures"]), "warning"),
        ("Buttons", sum(1 for i in items if i["type"] == "Button"), "primary"),
        ("Unused bookmarks", unused_bookmarks, "warning" if unused_bookmarks else "positive"),
    ]
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
                ui.code(result.logs).classes("w-full")
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
            log_tab = ui.tab("Log", icon="article")
        with ui.tab_panels(tabs, value=lineage_tab).classes("w-full"):
            with ui.tab_panel(lineage_tab).classes("p-0"):
                lineage_url = f"/files/{token}/{result.files['lineage'].name}"
                with ui.row().classes("w-full justify-end"):
                    ui.link("Open in a new tab", lineage_url, new_tab=True).classes("text-sm")
                ui.element("iframe").props(f'src="{lineage_url}"').classes(
                    "w-full h-[78vh] border rounded"
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
                    ui.code(result.logs).classes("w-full")
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
        ui.label("Power BI report documentation").classes("text-caption opacity-80")

    with ui.column().classes("w-full max-w-[1400px] mx-auto q-pa-md gap-4"):
        with ui.row().classes("w-full gap-4 items-stretch"):
            # ---------------- inputs ----------------
            with ui.card().classes("grow min-w-[420px]"):
                ui.label("Report").classes("text-subtitle1 text-weight-medium")

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
                        "Model (.bim) - found automatically when next to the report"
                    ).classes("grow").mark("model")
                    picker_button(model_input, (".bim",))
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
                    log_file = ui.switch("Write warnings to a log file", value=True)
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
                desktop_box = ui.column().classes("gap-1")

                def refresh_desktop() -> None:
                    desktop_box.clear()
                    instances = find_local_instances()
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

                ui.button("Refresh", icon="refresh", on_click=refresh_desktop).props(
                    "flat dense no-caps"
                )
                refresh_desktop()

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

        result_box = ui.column().classes("w-full gap-3")

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
        if model and not model_input.value:
            model_input.value = str(model)
        output_input.value = str(Path.cwd() / "output" / report_name(path))
        refresh_desktop()

    report_input.on_value_change(lambda _: on_report_change())

    async def start_run() -> None:
        report = Path((report_input.value or "").strip('"'))
        model_value = (model_input.value or "").strip('"')
        if not report_input.value or not report.exists():
            ui.notify("Choose an existing report first.", type="warning")
            return
        model = Path(model_value) if model_value else find_model_for_report(report)
        if model is None or not model.exists():
            ui.notify("Choose the model (.bim) that belongs to the report.", type="warning")
            return

        options = ExtractionOptions(
            report_path=report,
            model_path=model,
            output_dir=Path(output_input.value or Path.cwd() / "output" / report_name(report)),
            description_tag=description_tag.value or ExtractionOptions.description_tag,
            tabular_editor_analysis=te_analysis.value,
            tabular_editor_tsv=te_tsv.value,
            write_log_file=log_file.value,
        )

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
        _render_result(result_box, result, options)
        ui.notify(
            result.message if result.ok else f"Failed: {result.message}",
            type={"success": "positive", "warnings": "warning", "error": "negative"}[result.status],
        )

    run_button.on_click(start_run)


def start(port: int = 8081, open_browser: bool = True, host: Optional[str] = None) -> None:
    """Start the web UI (blocks until stopped)."""
    register_routes()
    ui.run(
        index,
        title="PBIxtractor",
        host=host or "127.0.0.1",
        port=port,
        reload=False,
        show=open_browser,
        dark=None,  # follow the system theme
        favicon=DATA_DIR / "logo.ico",
        show_welcome_message=True,
    )
