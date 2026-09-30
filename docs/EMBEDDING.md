# Embedding PBIxtractor in another NiceGUI app

PBIxtractor runs stand-alone (`pbixtractor` starts its own server) or as a page inside another
NiceGUI app. Embedded, the host keeps its own layout, theme, sign-in and host checks;
PBIxtractor adds only its page content and two read-only routes.

## 1. Add the dependency

Pin a release tag, so a PBIxtractor change never reaches the host unannounced (`<tag>` is the
release you have tested, e.g. `v0.3.0` once that is tagged):

```toml
# pyproject.toml of the host
dependencies = [
    "pbixtractor @ git+https://github.com/MackanT/PBIxtractor@<tag>",
]
```

Both apps must use NiceGUI 3.x.

## 2. Register once at startup

Before `ui.run(...)`:

```python
from pbixtractor import web_ui

web_ui.register(
    prefix="/pbixtractor",          # PBIxtractor's routes: /pbixtractor/files/..., /pbixtractor/catalog/...
    output_root="/data/pbixtractor",  # documentation, uploads, downloads (default: <cwd>/output)
    storage_prefix="pbixtractor.",  # its keys in app.storage.general
    local_machine=False,            # see "Server or user PC" below
)
```

## 3. Show the page inside the host's layout

In the host's page (or nav section), after its own header/navigation:

```python
def _power_bi_section() -> None:
    web_ui.build_page()
```

`build_page()` renders into the current container: the page masthead, report choice, options,
the run button and the results, including the lineage viewer.

## What the host is expected to provide

- **Theme.** PBIxtractor uses Quasar colour names (`primary`, `secondary`, `positive`, ...) and
  the same CSS classes as the host's design system: `page-mark`, `page-rule`, `stat-eyebrow`,
  `stat-numeral`, `edge-*`, `tint-*`, `font-disp`, and the `Familjen Grotesk` / `Source Sans 3`
  font families. If the host defines them (the data-platform web UI does), the page matches it,
  including a white-label accent. PBIxtractor applies no colours or fonts of its own when embedded.
  Two classes are PBIxtractor's own and need a rule in the host's CSS for the same look:
  `drop-zone` (the upload strip) and `lineage-frame` (the viewer's border) - see
  `pbixtractor/theme.py::theme_css`.
- **Access control.** The routes and the page have no sign-in of their own. Put the page behind
  the host's auth and roles. The two routes only serve files of runs made in this server
  process (random tokens) and the catalog folder in use.
- **Light/dark.** The embedded lineage viewer follows the page's dark mode (`body.body--dark`)
  by itself.

## Server or user PC (`local_machine`)

`local_machine=True` means every user's browser runs on the server's own PC. Only then does the
page offer things that act on the server's machine:

- typing report/model/output/catalog paths and the server-side file browser
- saving the Tabular Editor folder
- looking for reports open in Power BI Desktop (live row counts/sizes)
- "Open folder"

With `local_machine=False` (the default for `register`), those are hidden. Users upload a
`.pbix`/`.bim`, or pick a report from Fabric or Azure DevOps. Tabular Editor analysis still runs
if Tabular Editor 2 is installed on the server.

## Things to know

- One documentation run at a time per server process (a second user sees "Waiting for a run in
  another tab").
- Fabric and Azure DevOps sign-in happens on the server (`azure-identity`): an interactive
  browser sign-in opens on the server, so a hosted deployment needs `az login` on the server
  or `AZURE_DEVOPS_PAT` for DevOps.
- Run on Windows for Tabular Editor 2; everything else is portable.

## Checked by tests

`tests/test_embedding.py` embeds PBIxtractor in a minimal host page and checks the prefix, the
output root, the storage prefix, that no shell/theme is added and that nothing acts on the
server's machine.
