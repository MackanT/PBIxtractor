"""How the web UI is hosted: stand-alone (its own server) or embedded in another NiceGUI app.

Stand-alone keeps the defaults. An embedding app calls web_ui.register(...) once at startup,
which sets these; everything the web UI puts in shared places goes through here:

- URL paths of its routes (lineage files, catalog) get `prefix`
- keys in app.storage.general get `storage_prefix` (the host has its own keys there)
- output goes below `output_root`
- `local_machine`: the browser runs on the same PC as the server. Only then may the page browse
  the server's disk, take server paths, save the Tabular Editor folder, look for Power BI Desktop
  or open a folder. A hosted app must leave it off: those would act on the server, not the user's
  PC (uploads, Fabric and Azure DevOps still work).
"""

from collections.abc import MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from nicegui import app


@dataclass
class WebConfig:
    prefix: str = ""  # "" stand-alone, e.g. "/pbixtractor" embedded (no trailing slash)
    output_root: Optional[Path] = None  # None: <current folder>/output
    storage_prefix: str = ""  # "" stand-alone (keeps existing keys), e.g. "pbixtractor."
    local_machine: bool = True
    embedded: bool = False  # set by register(): no shell, theme or host check of our own


CONFIG = WebConfig()


def configure(**settings) -> WebConfig:
    """Replace the settings (unknown names are an error)."""
    global CONFIG
    CONFIG = WebConfig(**settings)
    return CONFIG


def url(path: str) -> str:
    """An app URL path of the web UI's own routes, e.g. url("/files/x") → "/pbixtractor/files/x"."""
    return CONFIG.prefix + path


def output_root() -> Path:
    return Path(CONFIG.output_root) if CONFIG.output_root else Path.cwd() / "output"


class _PrefixedChoices(MutableMapping):
    """app.storage.general seen through a key prefix."""

    def __init__(self, store: MutableMapping, prefix: str):
        self._store, self._prefix = store, prefix

    def __getitem__(self, key: str):
        return self._store[self._prefix + key]

    def __setitem__(self, key: str, value) -> None:
        self._store[self._prefix + key] = value

    def __delitem__(self, key: str) -> None:
        del self._store[self._prefix + key]

    def __iter__(self) -> Iterator[str]:
        return (k[len(self._prefix) :] for k in list(self._store) if k.startswith(self._prefix))

    def __len__(self) -> int:
        return sum(1 for _ in self)


def stored_choices() -> MutableMapping:
    """Remembered choices (server-side JSON in .nicegui/), under this app's key prefix; an
    empty dict if storage is unavailable."""
    try:
        store = app.storage.general
    except (RuntimeError, AttributeError):
        return {}
    return _PrefixedChoices(store, CONFIG.storage_prefix) if CONFIG.storage_prefix else store
