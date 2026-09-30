"""The web UI's visual style: colour tokens, CSS and a few page building blocks.

The style follows the "field-station" design used by the data-platform web UI, so PBIxtractor
looks like part of it when embedded there and like its sibling when run on its own:

- one brand accent (sap green) for everything interactive and for "ok"; copper is the only
  warm colour and is spent on the one primary action / thing that needs attention;
- warm "bone" paper in light mode, "bark" in dark mode (never slate); light is the default;
- a solid deep header + slim icon rail derived from the accent (one continuous band);
- Familjen Grotesk for headings and numerals, Source Sans 3 for text - bundled (OFL), never
  fetched from a CDN.

All colours live here: pages use Quasar colour names (primary, secondary, positive, ...) and the
CSS classes below, never hex values. The class names deliberately match the host app's, so an
embedded page picks up the host's theme (including a white-label accent) without applying its
own - apply_theme() is only for the stand-alone app.

    apply_theme()                      # stand-alone only: ui.colors + CSS
    page_title("Document a report", "…", icon="description")
    stat_tile(12, "Unused measures", tone="warning")
"""

from typing import Optional

from nicegui import app, ui

from .design import (
    ACCENT_HEX,
    BARK_HEX,
    BONE_HEX,
    COPPER_HEX,
    FAIL_HEX,
    FONTS_DIR,
    INFO_HEX,
    MOSS_HEX,
    OK_HEX,
    PETROL_HEX,
    SHEET_HEX,
    WARN_HEX,
    darken,
    rgba,
)

FONTS_URL = "/pbixtractor-fonts"  # prefixed: a host app may serve its own /fonts

# Text tiers
TEXT_MUTED = "text-sm text-grey-7"
TEXT_SUBTLE = "text-grey-7"


def theme_css(accent: str = ACCENT_HEX) -> str:
    """The app-wide CSS, with the accent-derived parts computed from `accent`."""
    band = darken(accent, 0.55)  # header + rail ground (sap → pine); white text stays legible
    return f"""
/* Written casing is rendered casing: Quasar uppercases button and tab labels by default */
.q-btn, .q-tab {{ text-transform: none; }}
/* Grounds: bone paper / bark; cards are sheets with a hairline and one soft shadow step */
body:not(.body--dark) {{ background: {BONE_HEX}; }}
body {{ overflow-x: hidden; }}
.q-card {{ border-radius: 12px; background: {SHEET_HEX}; border: 1px solid #e4e1d4;
          box-shadow: 0 1px 2px rgba(23,53,43,0.05); }}
.body--dark .q-card {{ background: {MOSS_HEX}; border: 1px solid #2a2e28; box-shadow: none; }}
/* One continuous brand band: header and rail share a deep ground derived from the accent */
.q-header {{ background: {band}; }}
.q-drawer {{ background: {band}; }}
.rail {{ display: flex !important; flex-direction: column !important; align-items: center !important;
        gap: 4px !important; padding: 8px 0 !important; overflow-x: hidden !important;
        scrollbar-width: none; }}
.rail::-webkit-scrollbar {{ display: none; }}
.rail-expanded {{ align-items: stretch !important; padding: 8px !important; }}
.rail-btn {{ width: 40px; height: 40px; border-radius: 10px; color: #e9e6dc; }}
.rail-btn-x {{ width: 100%; justify-content: flex-start; border-radius: 10px; color: #e9e6dc; }}
.nav-btn {{ opacity: 0.72; }}
.nav-btn:hover {{ opacity: 1; background: rgba(255,255,255,0.08); }}
.nav-btn .q-btn__content {{ flex-wrap: nowrap; white-space: nowrap; }}
.nav-active {{ background: {rgba(accent, 0.45)} !important; color: {BONE_HEX} !important;
              font-weight: 600; opacity: 1; }}
/* Typography: display face for headings and numerals, text face for everything read */
@font-face {{ font-family: "Familjen Grotesk"; font-style: normal; font-weight: 400 700;
             font-display: swap; src: url({FONTS_URL}/familjen-grotesk-latin.woff2) format("woff2"); }}
@font-face {{ font-family: "Source Sans 3"; font-style: normal; font-weight: 200 900;
             font-display: swap; src: url({FONTS_URL}/source-sans-3-latin.woff2) format("woff2"); }}
body {{ font-family: "Source Sans 3", "Segoe UI", system-ui, sans-serif; }}
.font-disp, .text-h1, .text-h2, .text-h3, .text-h4, .text-h5, .text-h6 {{
  font-family: "Familjen Grotesk", "Source Sans 3", "Segoe UI", sans-serif; letter-spacing: -0.01em; }}
.q-table td, .q-table th, .tabnum {{ font-variant-numeric: tabular-nums; }}
/* Page masthead: the section's icon in an accent tile, display-face title, accent rule */
.page-mark {{ width: 38px; height: 38px; border-radius: 11px; flex: 0 0 38px; display: flex;
             align-items: center; justify-content: center; background: {rgba(accent, 0.12)}; }}
.body--dark .page-mark {{ background: {rgba(accent, 0.22)}; }}
.page-rule {{ height: 2px; border-radius: 2px;
             background: linear-gradient(90deg,{accent} 0 56px,{rgba(accent, 0.16)} 56px); }}
/* KPI instrument: the eyebrow names it, the numeral is the tile */
.stat-eyebrow {{ font-size: 0.68rem; letter-spacing: 0.13em; text-transform: uppercase;
                font-weight: 600; }}
.stat-numeral {{ font-family: "Familjen Grotesk", "Source Sans 3", "Segoe UI", sans-serif;
                font-variant-numeric: tabular-nums; letter-spacing: -0.01em; }}
/* Attention edges: status colours for state, copper for the one attention thing */
.edge-ok    {{ border-left: 3px solid {OK_HEX} !important; }}
.edge-warn  {{ border-left: 3px solid {WARN_HEX} !important; }}
.edge-neg   {{ border-left: 3px solid {FAIL_HEX} !important; }}
.edge-attn  {{ border-left: 3px solid {COPPER_HEX} !important; }}
.edge-quiet {{ border-left: 3px solid transparent; }}
/* Theme-aware washes for advisory panels (and the log) */
.tint-neutral  {{ background: rgba(127,127,127,0.06); }}
.tint-warning  {{ background: {rgba(WARN_HEX, 0.10)}; }}
.tint-negative {{ background: {rgba(FAIL_HEX, 0.08)}; }}
.tint-accent   {{ background: {rgba(accent, 0.10)}; }}
.body--dark .tint-neutral  {{ background: rgba(127,127,127,0.14); }}
.body--dark .tint-warning  {{ background: {rgba(WARN_HEX, 0.18)}; }}
.body--dark .tint-negative {{ background: {rgba(FAIL_HEX, 0.16)}; }}
.body--dark .tint-accent   {{ background: {rgba(accent, 0.20)}; }}
/* Tab panels sit on the page ground (Quasar paints them white / dark-grey) */
.q-tab-panels {{ background: transparent !important; }}
/* The embedded lineage viewer: a sheet edge like the cards */
.lineage-frame {{ border: 1px solid #e4e1d4; }}
.body--dark .lineage-frame {{ border-color: #2a2e28; }}
/* Drop zone (ui.upload): a quiet dashed strip, not a filled bar over an empty white list */
.drop-zone {{ width: 100%; background: transparent !important; box-shadow: none !important;
             border: 1px dashed {rgba(accent, 0.45)} !important; border-radius: 10px; }}
.drop-zone .q-uploader__header {{ background: transparent !important; color: {accent} !important; }}
.drop-zone .q-uploader__subtitle {{ display: none; }}
.drop-zone .q-uploader__list {{ min-height: 0; padding: 0; background: transparent; }}
/* Visible keyboard focus (Quasar removes outlines); mouse clicks stay ring-free */
.q-btn:focus-visible, .q-field__native:focus-visible, a:focus-visible, .q-tab:focus-visible,
[tabindex]:focus-visible, .q-toggle:has(:focus-visible), .q-checkbox:has(:focus-visible),
.q-item:has(:focus-visible), .q-select:has(:focus-visible) {{
  outline: 3px solid {accent} !important; outline-offset: 2px !important; border-radius: 8px !important; }}
"""


def serve_fonts() -> None:
    """Serve the bundled fonts (call once, before ui.run). Also needed when embedded: the
    host's own font files may live elsewhere."""
    if not any(getattr(route, "path", None) == FONTS_URL for route in app.routes):
        app.add_static_files(FONTS_URL, FONTS_DIR)


def apply_theme(accent: str = ACCENT_HEX) -> None:
    """Colours + CSS for the current page. Stand-alone only: an embedding app has its own."""
    ui.colors(
        primary=accent,
        secondary=COPPER_HEX,
        accent=PETROL_HEX,
        positive=OK_HEX,
        negative=FAIL_HEX,
        warning=WARN_HEX,
        info=INFO_HEX,
        dark=MOSS_HEX,
        dark_page=BARK_HEX,
    )
    ui.add_css(theme_css(accent))


# ── page building blocks ──────────────────────────────────────────────────────


def page_title(title: str, subtitle: str = "", *, icon: Optional[str] = None) -> None:
    """The page masthead: icon tile + display-face title (+ subtitle) + the accent rule."""
    with ui.column().classes("gap-0 mb-2 w-full"):
        with ui.row().classes("items-center gap-3 w-full no-wrap"):
            if icon:
                with ui.element("div").classes("page-mark").mark("page-mark"):
                    ui.icon(icon, size="1.35rem").classes("text-primary")
            with ui.column().classes("gap-0 flex-1 min-w-0"):
                ui.label(title).classes("text-2xl font-bold font-disp").mark("page-title")
                if subtitle:
                    ui.label(subtitle).classes(TEXT_SUBTLE)
        if icon:
            # w-full is needed: an empty div in a flex-start column collapses to zero width
            ui.element("div").classes("page-rule mt-2 w-full")


def card_header(icon: str, title: str, subtitle: str = "", *, color: str = "primary") -> None:
    """Icon + title (+ subtitle) at the top of a card; call inside a row."""
    ui.icon(icon, size="1.5rem").classes(f"text-{color}")
    with ui.column().classes("gap-0 flex-1"):
        ui.label(title).classes("font-semibold")
        if subtitle:
            ui.label(subtitle).classes(TEXT_MUTED)


# Tone of a stat → the attention edge on its tile
_EDGES = {"positive": "edge-ok", "warning": "edge-warn", "negative": "edge-neg"}


def stat_tile(value, label: str, *, tone: Optional[str] = None, caption: str = "") -> None:
    """A KPI tile: uppercase eyebrow, display-face numeral, optional caption. `tone`
    (positive/warning/negative) colours the numeral and adds the matching edge; "neutral"
    numbers stay in the text colour."""
    edge = _EDGES.get(tone or "", "edge-quiet")
    with ui.card().classes(f"q-pa-sm min-w-[128px] gap-0 {edge}").props("flat"):
        ui.label(label).classes("stat-eyebrow text-grey-7")
        ui.label(str(value)).classes(
            "stat-numeral text-2xl font-semibold" + (f" text-{tone}" if tone in _EDGES else "")
        )
        if caption:
            ui.label(caption).classes("text-xs text-grey-7")


def empty_state(icon: str, title: str, subtitle: str = "") -> None:
    """A consistent "nothing here" block: large icon, bold title, optional subtitle."""
    with ui.column().classes("items-center gap-2 w-full py-8 text-center"):
        ui.icon(icon, size="2.5rem").classes("text-grey-5")
        ui.label(title).classes("text-grey-7 font-semibold")
        if subtitle:
            ui.label(subtitle).classes("text-sm text-grey-6").style("max-width:34rem")
