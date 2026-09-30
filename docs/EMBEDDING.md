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
    access=user_may_use_it,         # the host's sign-in/role check (see "Access control")
)
```

## 3. Show the page inside the host's layout

In the host's page (or nav section), after its own header/navigation:

```python
def _power_bi_section() -> None:
    web_ui.build_page()
```

`build_page()` renders into the current container: report choice, options, the run button and
the results, including the lineage viewer. Embedded it leaves the section title and the page
padding/width to the host (pass `title=True` for its own "Document a report" masthead). It works
in hosts that switch sections client-side with `ui.sub_pages`.

## What the host is expected to provide

- **Theme.** PBIxtractor uses Quasar colour names (`primary`, `secondary`, `positive`, ...) and
  the same CSS classes as the host's design system: `page-mark`, `page-rule`, `stat-eyebrow`,
  `stat-numeral`, `edge-*`, `tint-*`, `font-disp`, and the `Familjen Grotesk` / `Source Sans 3`
  font families. If the host defines them (the data-platform web UI does), the page matches it,
  including a white-label accent. PBIxtractor applies no colours or fonts of its own when embedded.
  Three classes are PBIxtractor's own and need a rule in the host's CSS for the same look:
  `drop-zone` (the upload strip), `lineage-frame` (the viewer's border) and `pbx-results`
  (the results tabs on the page ground) - see `pbixtractor/theme.py::theme_css`.
- **Access control.** PBIxtractor has no sign-in of its own. Show the page only to users who
  may use it, and pass the same check as `access=`: the two file routes (lineage files and
  the catalog, which holds model DAX) are plain HTTP routes, so a host's *page* login does not
  cover them. `access` is called for every request to them (it can read `app.storage.user`);
  False - or an error - answers 403. The file links also use random per-run tokens.
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
- Fabric and Azure DevOps sign-in happens on the server. In a container, set a service
  principal: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET` (or
  `AZURE_CLIENT_CERTIFICATE_PATH`). PBIxtractor then never tries a browser login. The service
  principal needs access to the workspaces / DevOps projects, and Fabric's tenant setting
  "Service principals can use Fabric APIs". A DevOps personal access token in
  `AZURE_DEVOPS_PAT` works for DevOps too. Without either, sign-in falls back to `az login` on
  the server, then to an interactive browser login - which only works on a desktop.
- Run on Windows for Tabular Editor 2; everything else is portable.

## Checked by tests

`tests/test_embedding.py` embeds PBIxtractor in a minimal host page and checks the prefix, the
output root, the storage prefix, that no shell/theme is added, that nothing acts on the
server's machine, and that it works inside a `ui.sub_pages` host.
