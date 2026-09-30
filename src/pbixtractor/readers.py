"""Readers that turn a Power BI report (legacy Layout or PBIR, zip or folder) into one format.

Supported inputs:
    - .pbix with Report/Layout (legacy format)
    - .pbix with Report/definition/ (PBIR format, default in newer Power BI Desktop)
    - PBIP projects: a .pbip file, or its <name>.Report folder (PBIR, or PBIR-Legacy report.json)

Both formats are normalised to ReportDefinition -> PageDefinition -> VisualDefinition, which is
what the extractors consume.
"""

import json
import logging
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .logger import get_logger

# ============================================================================
# Normalised report definition
# ============================================================================


@dataclass
class FieldBinding:
    """A field used by a visual."""

    role: Optional[str]  # projection role (e.g. "Values"); None if not bound to a role
    expr: dict  # query expression (Column / Measure / Aggregation / HierarchyLevel)
    query_ref: str  # Power BI queryRef, e.g. "Sales.Amount" (may be stale, see CLAUDE.md)
    display_name: Optional[str] = None  # renamed-in-visual name, if any


@dataclass
class VisualDefinition:
    """A visual container on a page."""

    name: str
    visual_type: Optional[str]
    group_name: Optional[str] = None  # set for groups (which have no visual_type)
    fields: list[FieldBinding] = field(default_factory=list)
    aliases: dict[str, str] = field(default_factory=dict)  # query alias -> table
    objects: dict = field(default_factory=dict)  # visual formatting objects
    container_objects: dict = field(default_factory=dict)  # title, visualLink (actions), ...
    filters: list = field(default_factory=list)
    sync_group: Optional[str] = None  # slicer sync group name
    hidden: bool = False  # hidden on the page (selection pane)

    @property
    def is_group(self) -> bool:
        """True for visual groups."""
        return self.group_name is not None


@dataclass
class VisualInteraction:
    """A changed "Edit interactions" setting: how a source visual affects a target visual."""

    source: str  # visual name
    target: str  # visual name
    kind: str  # Filter | Highlight | None (no effect) | Default


@dataclass
class PageDefinition:
    """A report page."""

    name: str
    display_name: str
    visuals: list[VisualDefinition] = field(default_factory=list)
    filters: list = field(default_factory=list)
    hidden: bool = False
    page_type: str = ""  # "" (normal), "Tooltip" or "Drillthrough"
    interactions: list[VisualInteraction] = field(default_factory=list)


@dataclass
class BookmarkDefinition:
    """A bookmark and what it captures."""

    name: str  # id, e.g. "Bookmark9e00a1c65dc7489350cb"
    display_name: str
    group: str = ""  # bookmark group display name
    page: str = ""  # page (section) name the bookmark opens
    captures_data: bool = True  # filters/slicers ("Data" option)
    captures_display: bool = True  # visibility, spotlight, ... ("Display" option)
    captures_page: bool = True  # navigates to its page ("Current page" option)
    target_visuals: list[str] = field(default_factory=list)  # "Selected visuals"; empty = all
    hidden_visuals: list[str] = field(default_factory=list)  # visuals/groups it hides


@dataclass
class ReportDefinition:
    """A whole report."""

    format: str  # "legacy" or "pbir"
    pages: list[PageDefinition] = field(default_factory=list)
    filters: list = field(default_factory=list)  # report-level ("All Pages") filters
    bookmarks: dict[str, str] = field(default_factory=dict)  # bookmark id -> display name
    bookmark_details: list[BookmarkDefinition] = field(default_factory=list)


# "Edit interactions" types: legacy numbers and PBIR names -> label
INTERACTION_KINDS = {
    0: "Default",
    1: "Filter",
    2: "Highlight",
    3: "None",
    "Default": "Default",
    "DataFilter": "Filter",
    "HighlightFilter": "Highlight",
    "NoFilter": "None",
}


def _page_type(binding) -> str:
    """Tooltip/Drillthrough from a page binding (only names are trusted, not codes)."""
    kind = (binding or {}).get("type") if isinstance(binding, dict) else None
    return kind if isinstance(kind, str) and kind in ("Tooltip", "Drillthrough") else ""


def _interactions(items) -> list[VisualInteraction]:
    return [
        VisualInteraction(
            source=item.get("source", ""),
            target=item.get("target", ""),
            kind=INTERACTION_KINDS.get(item.get("type"), str(item.get("type"))),
        )
        for item in items or []
        if isinstance(item, dict)
    ]


def _bookmark(data: dict, group: str = "") -> BookmarkDefinition:
    """Parse a bookmark (same structure in Report/Layout config and PBIR .bookmark.json)."""
    state = data.get("explorationState") or {}
    options = data.get("options") or {}
    hidden = []
    for section in (state.get("sections") or {}).values():
        for visual_name, container in (section.get("visualContainers") or {}).items():
            display = (container.get("singleVisual") or {}).get("display") or {}
            if display.get("mode") == "hidden":
                hidden.append(visual_name)
        for group_name, container in (section.get("visualContainerGroups") or {}).items():
            if (container or {}).get("isHidden"):
                hidden.append(group_name)
    return BookmarkDefinition(
        name=data.get("name", ""),
        display_name=data.get("displayName", ""),
        group=group,
        page=state.get("activeSection", ""),
        captures_data=not options.get("suppressData", False),
        captures_display=not options.get("suppressDisplay", False),
        captures_page=not options.get("suppressActiveSection", False),
        target_visuals=list(options.get("targetVisualNames") or []),
        hidden_visuals=hidden,
    )


# ============================================================================
# File access (zip or folder)
# ============================================================================


class ReportReadError(ValueError):
    """The report cannot be read, for a reason the message explains (no traceback needed)."""


class _ZipFiles:
    """Read report files from a .pbix zip."""

    def __init__(self, path: Path):
        self._path = path
        try:
            with zipfile.ZipFile(path, "r") as zip_file:
                self._names = zip_file.namelist()
        except zipfile.BadZipFile as exc:
            raise ReportReadError(_not_a_zip_message(path)) from exc

    def names(self) -> list[str]:
        return list(self._names)

    def read(self, name: str) -> bytes:
        # Read on demand: the zip also holds the (large) DataModel, which we never load
        with zipfile.ZipFile(self._path, "r") as zip_file:
            return zip_file.read(name)


def _not_a_zip_message(path: Path) -> str:
    """Explain why a .pbix cannot be opened as a zip file."""
    with open(path, "rb") as file:
        header = file.read(16)
    if b".pfile" in header:
        # Microsoft Purview / Information Protection wraps labelled files in a .pfile container
        return (
            f"{path.name} is encrypted by a sensitivity label (Microsoft Purview) and cannot be "
            "read. Save an unprotected copy from Power BI Desktop (or remove the label) and try "
            "again."
        )
    return f"{path.name} is not a valid .pbix file (not a zip archive)."


class _FolderFiles:
    """Read report files from a PBIP <name>.Report folder."""

    def __init__(self, root: Path):
        self._root = root

    def names(self) -> list[str]:
        return [p.relative_to(self._root).as_posix() for p in self._root.rglob("*") if p.is_file()]

    def read(self, name: str) -> bytes:
        return (self._root / name).read_bytes()


def _open_files(path: Path):
    """Open a .pbix, .pbip or .Report folder for reading."""
    if path.is_dir():
        return _FolderFiles(path)
    if path.suffix.lower() == ".pbip":
        report_folder = path.with_name(f"{path.stem}.Report")
        if not report_folder.is_dir():
            raise FileNotFoundError(f"Report folder not found for {path.name}: {report_folder}")
        return _FolderFiles(report_folder)
    return _ZipFiles(path)


def _load_json(data: bytes) -> dict:
    """Load a UTF-8 JSON file (with or without BOM)."""
    return json.loads(data.decode("utf-8-sig"))


# ============================================================================
# Entry point
# ============================================================================


def read_report(path: str | Path, logger: logging.Logger = None) -> ReportDefinition:
    """
    Read a Power BI report in any supported format.

    Args:
        path: .pbix file, .pbip file or <name>.Report folder
        logger: Logger for parse warnings

    Returns:
        Normalised ReportDefinition
    """
    logger = logger or get_logger("pbixtractor")
    path = Path(path)
    files = _open_files(path)
    names = set(files.names())

    # Legacy layout: Report/Layout in a .pbix (UTF-16), or report.json in a PBIR-Legacy folder
    if "Report/Layout" in names:
        layout = json.loads(files.read("Report/Layout").decode("utf-16-le"))
        return _read_legacy(layout, logger)

    for prefix in ("Report/definition/", "definition/"):
        if f"{prefix}report.json" in names:
            return _read_pbir(files, names, prefix, logger)

    if "report.json" in names:
        return _read_legacy(_load_json(files.read("report.json")), logger)

    raise ReportReadError(f"{path.name}: no Power BI report definition found")


# ============================================================================
# Legacy format (Report/Layout)
# ============================================================================


def _parse_embedded(value, logger: logging.Logger, context: str):
    """Parse a field stored as an embedded JSON string."""
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse {context} JSON. Error: {e}. Data: {value[:200]}")
        return None


def _read_legacy(layout: dict, logger: logging.Logger) -> ReportDefinition:
    """Normalise a legacy Report/Layout document."""
    config = _parse_embedded(layout.get("config"), logger, "report config") or {}

    bookmarks = {}
    bookmark_details = []

    def collect(items: list, group: str = ""):
        for bookmark in items:
            bookmarks[bookmark.get("name", "")] = bookmark.get("displayName", "")
            if "children" in bookmark:  # bookmark group
                collect(bookmark["children"], bookmark.get("displayName", ""))
            else:
                bookmark_details.append(_bookmark(bookmark, group))

    collect(config.get("bookmarks", []))

    pages = []
    for section in layout.get("sections", []):
        page_name = section.get("displayName", "")
        section_config = _parse_embedded(section.get("config"), logger, f"page {page_name}") or {}
        visuals = [
            _legacy_visual(container, logger, page_name)
            for container in section.get("visualContainers", [])
        ]
        pages.append(
            PageDefinition(
                name=section.get("name", ""),
                display_name=page_name,
                visuals=[visual for visual in visuals if visual is not None],
                filters=_parse_embedded(section.get("filters"), logger, f"filters on {page_name}")
                or [],
                hidden=section_config.get("visibility") == 1,
                page_type=_page_type(section_config.get("pageBinding")),
                interactions=_interactions(section_config.get("relationships")),
            )
        )

    return ReportDefinition(
        format="legacy",
        pages=pages,
        filters=_parse_embedded(layout.get("filters"), logger, "report filters") or [],
        bookmarks=bookmarks,
        bookmark_details=bookmark_details,
    )


def _legacy_visual(
    container: dict, logger: logging.Logger, page_name: str
) -> Optional[VisualDefinition]:
    """Normalise a legacy visual container."""
    config = _parse_embedded(container.get("config"), logger, f"visual on {page_name}") or {}
    if not config:
        return None

    name = config.get("name", "")
    filters = _parse_embedded(container.get("filters"), logger, f"filters of {name}") or []

    if "singleVisualGroup" in config:
        return VisualDefinition(
            name=name,
            visual_type=None,
            group_name=config["singleVisualGroup"].get("displayName", ""),
            filters=filters,
            hidden=bool(config["singleVisualGroup"].get("isHidden")),
        )

    single_visual = config.get("singleVisual") or {}
    query = single_visual.get("prototypeQuery") or {}
    column_properties = single_visual.get("columnProperties") or {}

    # queryRef -> projection role (e.g. "Values", "Category")
    roles = {}
    for role, refs in (single_visual.get("projections") or {}).items():
        for ref in refs:
            if isinstance(ref, dict) and "queryRef" in ref:
                roles.setdefault(ref["queryRef"], role)

    fields = []
    for select in query.get("Select", []):
        if not isinstance(select, dict):
            continue
        query_ref = select.get("Name", "")
        fields.append(
            FieldBinding(
                role=roles.get(query_ref),
                expr=select,
                query_ref=query_ref,
                display_name=(column_properties.get(query_ref) or {}).get("displayName")
                or select.get("NativeReferenceName"),
            )
        )

    return VisualDefinition(
        name=name,
        visual_type=single_visual.get("visualType"),
        fields=fields,
        aliases={
            source["Name"]: source.get("Entity", "")
            for source in query.get("From", [])
            if isinstance(source, dict) and source.get("Name")
        },
        objects=single_visual.get("objects") or {},
        container_objects=single_visual.get("vcObjects") or {},
        filters=filters,
        sync_group=(single_visual.get("syncGroup") or {}).get("groupName"),
        hidden=((single_visual.get("display") or {}).get("mode") == "hidden"),
    )


# ============================================================================
# PBIR format (definition/ folder with one JSON file per page/visual)
# ============================================================================


def _read_pbir(files, names: set[str], prefix: str, logger: logging.Logger) -> ReportDefinition:
    """Normalise a PBIR report definition."""

    def read(name: str) -> dict:
        try:
            return _load_json(files.read(prefix + name))
        except (KeyError, FileNotFoundError):
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse {prefix}{name}. Error: {e}")
            return {}

    report = read("report.json")

    # Pages in report order; fall back to folder order for pages not listed
    page_folders = sorted(
        {
            name[len(prefix + "pages/") :].split("/")[0]
            for name in names
            if name.startswith(prefix + "pages/") and name.endswith("/page.json")
        }
    )
    order = read("pages/pages.json").get("pageOrder", [])
    page_folders = [p for p in order if p in page_folders] + [
        p for p in page_folders if p not in order
    ]

    pages = []
    for folder in page_folders:
        page = read(f"pages/{folder}/page.json")
        visual_prefix = f"{prefix}pages/{folder}/visuals/"
        visual_files = sorted(
            name
            for name in names
            if name.startswith(visual_prefix) and name.endswith("/visual.json")
        )
        pages.append(
            PageDefinition(
                name=page.get("name", folder),
                display_name=page.get("displayName", folder),
                visuals=[_pbir_visual(read(name[len(prefix) :])) for name in visual_files],
                filters=page.get("filterConfig", {}).get("filters", []),
                hidden=page.get("visibility") == "HiddenInViewMode",
                page_type=_page_type(page.get("pageBinding")) or _page_type(page),
                interactions=_interactions(page.get("visualInteractions")),
            )
        )

    # Bookmarks: one <name>.bookmark.json per bookmark; groups live in bookmarks.json
    group_of = {}
    for item in read("bookmarks/bookmarks.json").get("items", []):
        for child in item.get("children", []):
            group_of[child] = item.get("displayName", item.get("name", ""))

    bookmarks = {}
    bookmark_details = []
    for name in sorted(names):
        if name.startswith(prefix + "bookmarks/") and name.endswith(".bookmark.json"):
            bookmark = read(name[len(prefix) :])
            bookmarks[bookmark.get("name", "")] = bookmark.get("displayName", "")
            bookmark_details.append(_bookmark(bookmark, group_of.get(bookmark.get("name"), "")))
    for item in read("bookmarks/bookmarks.json").get("items", []):
        if "children" in item and item.get("name"):
            bookmarks.setdefault(item["name"], item.get("displayName", item["name"]))

    return ReportDefinition(
        format="pbir",
        pages=pages,
        filters=report.get("filterConfig", {}).get("filters", []),
        bookmarks=bookmarks,
        bookmark_details=bookmark_details,
    )


def _pbir_visual(visual_json: dict) -> VisualDefinition:
    """Normalise a PBIR visual.json."""
    name = visual_json.get("name", "")
    filters = visual_json.get("filterConfig", {}).get("filters", [])

    if "visualGroup" in visual_json:
        return VisualDefinition(
            name=name,
            visual_type=None,
            group_name=visual_json["visualGroup"].get("displayName", ""),
            filters=filters,
            hidden=bool(visual_json.get("isHidden")),
        )

    visual = visual_json.get("visual") or {}
    query_state = (visual.get("query") or {}).get("queryState") or {}

    fields = []
    for role, state in query_state.items():
        for projection in (state or {}).get("projections", []):
            fields.append(
                FieldBinding(
                    role=role,
                    expr=projection.get("field", {}),
                    query_ref=projection.get("queryRef", ""),
                    display_name=projection.get("displayName") or projection.get("nativeQueryRef"),
                )
            )

    return VisualDefinition(
        name=name,
        visual_type=visual.get("visualType"),
        fields=fields,
        objects=visual.get("objects") or {},
        container_objects=visual.get("visualContainerObjects") or {},
        filters=filters,
        sync_group=(visual.get("syncGroup") or {}).get("groupName"),
        hidden=bool(visual_json.get("isHidden")),
    )
