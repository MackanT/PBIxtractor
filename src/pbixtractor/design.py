"""Design tokens of the "field-station" style, without NiceGUI.

Shared by the web UI (theme.py) and the generated offline HTML pages (lineage viewer,
catalog), which the CLI writes too - so this module must not import NiceGUI.
"""

from .data import DATA_DIR

# ── colour tokens ─────────────────────────────────────────────────────────────
ACCENT_HEX = "#3f7d5a"  # sap - the brand accent: interactive + ok
COPPER_HEX = "#c2703e"  # copper - the one warm accent: primary action / attention
PETROL_HEX = "#35657a"  # petrol - cool secondary accent
OK_HEX = "#3f7d5a"  # sap
FAIL_HEX = "#b4433a"  # rust - clearly apart from copper
WARN_HEX = "#b58a1e"  # lichen amber - yellower than copper
INFO_HEX = "#5b8cad"  # glacier
BONE_HEX = "#f1efe7"  # light page ground
SHEET_HEX = "#fbfaf6"  # light card
BARK_HEX = "#161915"  # dark page ground
MOSS_HEX = "#20241f"  # dark card

FONTS_DIR = DATA_DIR / "fonts"


def darken(hex_color: str, factor: float) -> str:
    """A darker shade of #rrggbb (factor 0..1 = how far toward black)."""
    r, g, b = (int(hex_color.lstrip("#")[i : i + 2], 16) for i in (0, 2, 4))
    keep = max(0.0, 1.0 - factor)
    return f"#{int(r * keep):02x}{int(g * keep):02x}{int(b * keep):02x}"


def rgba(hex_color: str, alpha: float) -> str:
    r, g, b = (int(hex_color.lstrip("#")[i : i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


# ── the generated pages (lineage viewer, catalog) ─────────────────────────────
# Self-contained offline HTML files: same tokens as the app, as CSS variables, with the fonts
# embedded (a file:// page or a sandboxed iframe cannot load the app's /pbixtractor-fonts).
# Light/dark: ?theme=dark|light in the URL, a {"pbixtractorTheme": ...} message from the app
# (its header toggle), else the system setting.

_PAGE_LIGHT = """
  --bg: #f1efe7; --panel: #fbfaf6; --text: #23281f; --muted: #6b7466; --border: #e4e1d4;
  --accent: #3f7d5a; --attn: #c2703e; --edge: #b9b6a8; --edge-hi: #2f6246; --on-color: #ffffff;
  --shadow: 0 1px 2px rgba(23,53,43,.05);
  --source: #35657a; --table: #3f7d5a; --column: #5b8cad; --measure: #b58a1e;
  --visual: #7b68b5; --report: #8a4f7d; --page: #6b7466; --model: #1c3828; --unused: #b4433a;"""
_PAGE_DARK = """
  --bg: #161915; --panel: #20241f; --text: #e8eae4; --muted: #9ba295; --border: #2a2e28;
  --accent: #6fb08a; --attn: #e0935e; --edge: #4a5048; --edge-hi: #8fc7a4; --on-color: #161915;
  --shadow: none;
  --source: #6fa3b8; --table: #6fb08a; --column: #8ab4d1; --measure: #d4a93f;
  --visual: #a596d6; --report: #c98fbb; --page: #9ba295; --model: #8fc7a4; --unused: #d9776d;"""

# Sets data-theme before the page paints; only "dark"/"light" are ever accepted
PAGE_THEME_SCRIPT = """<script>(function () {
  function set(t) { if (t === "dark" || t === "light") document.documentElement.dataset.theme = t; }
  set(new URLSearchParams(location.search).get("theme"));
  addEventListener("message", function (e) { set(e.data && e.data.pbixtractorTheme); });
})();</script>"""


def _font_data_uri(name: str) -> str:
    import base64  # noqa: PLC0415 - only the generated pages need it

    data = base64.b64encode((FONTS_DIR / name).read_bytes()).decode("ascii")
    return f"data:font/woff2;base64,{data}"


def page_css() -> str:
    """CSS variables + fonts + base typography for the generated HTML pages."""
    return f"""
@font-face {{ font-family: "Familjen Grotesk"; font-weight: 400 700; font-display: swap;
  src: url({_font_data_uri("familjen-grotesk-latin.woff2")}) format("woff2"); }}
@font-face {{ font-family: "Source Sans 3"; font-weight: 200 900; font-display: swap;
  src: url({_font_data_uri("source-sans-3-latin.woff2")}) format("woff2"); }}
:root {{{_PAGE_LIGHT}
}}
:root[data-theme="dark"] {{{_PAGE_DARK}
}}
@media (prefers-color-scheme: dark) {{ :root:not([data-theme="light"]) {{{_PAGE_DARK}
}} }}
html, body {{ font-family: "Source Sans 3", "Segoe UI", system-ui, sans-serif; }}
h1, h2, .font-disp {{ font-family: "Familjen Grotesk", "Source Sans 3", "Segoe UI", sans-serif;
  letter-spacing: -0.01em; }}
"""
