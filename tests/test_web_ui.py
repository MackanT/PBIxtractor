"""Tests for the NiceGUI web UI (web_ui.py), using NiceGUI's simulated user (no browser)."""

import asyncio
import json
import logging
import threading
from pathlib import Path

import pytest

pytest.importorskip("nicegui")

from fastapi import HTTPException  # noqa: E402
from nicegui import ui  # noqa: E402
from nicegui.testing.user import User  # noqa: E402
from nicegui.testing.user_notify import UserNotify  # noqa: E402
from nicegui.testing.user_simulation import user_simulation  # noqa: E402

import pbixtractor.web_sources as web_sources  # noqa: E402
import pbixtractor.web_ui as web_ui  # noqa: E402
from pbixtractor import azure_auth  # noqa: E402
from pbixtractor.azure_auth import Account, ApiError  # noqa: E402

from .sample_layout import write_sample_pbix  # noqa: E402
from .test_semantic_model import BIM  # noqa: E402

ALICE = Account("alice@contoso.com", "oid-a.contoso-id", "contoso-id", "Contoso", home=True)
ALICE_GUEST = Account("alice@contoso.com", "oid-a.contoso-id", "fabrikam-id", "Fabrikam")
BOB = Account("bob@fabrikam.com", "oid-b.fabrikam-id", "fabrikam-id", "Fabrikam", home=True)


class FakeAccounts:
    """azure_auth's account functions over an in-memory list: no MSAL, no browser. The next
    add_account() signs in as `next_user` (waiting for `browser` first, when set - meanwhile
    sign_in_in_progress() is True); ensure_account() signs in only when no entry is active,
    one caller at a time (as azure_auth's login lock does). With `unreadable` set, the list
    "exists but cannot be read": every function that reads it raises ApiError (D43)."""

    def __init__(self, monkeypatch, *entries: Account, active: str = None, encrypted: bool = True):
        self.entries = list(entries)
        self.active = active
        self.encrypted = encrypted
        self.next_user: list[Account] = [BOB]
        self.add_error = None  # ApiError text the next add_account() raises
        self.listing_warning = None  # text add_account() passes to on_warning (tenants unlisted)
        self.remove_error = None  # ApiError text remove_account() raises after removing
        self.browser = None  # threading.Event the "browser sign-in" waits for
        self.signing = threading.Event()  # set while the "browser sign-in" waits
        self.added = 0
        self.ensured = 0
        self.polls = 0  # sign_in_in_progress() calls: every tab asks every BUSY_POLL_SECONDS
        self.tenants: list = []  # the tenant each add_account() was asked for
        self.removed: list[str] = []
        self.unreadable = None  # ApiError text: the account list exists but cannot be read
        self.use_error = None  # exception use_account() raises (e.g. the list cannot be written)
        self.snapshots = 0  # account_snapshot() calls: the account list read (parsed)
        self.waits: list[bool] = []  # account_snapshot()'s wait= of each call
        self.stamps = 0  # accounts_changed_since() calls: the cheap look at the file
        self._login = threading.Lock()
        for name in ("account_snapshot", "accounts", "active_account", "accounts_changed_since",
                     "use_account", "add_account", "ensure_account", "remove_account",
                     "sign_in_in_progress", "token_cache_encrypted"):
            monkeypatch.setattr(azure_auth, name, getattr(self, name))

    def _readable(self) -> None:
        if self.unreadable:
            raise ApiError(self.unreadable)

    def _active(self):
        return next((a for a in self.entries if a.key == self.active), None)

    def account_snapshot(self, wait=True):
        self.snapshots += 1
        self.waits.append(wait)
        self._readable()
        return list(self.entries), self._active()

    def accounts(self):
        self._readable()
        return list(self.entries)

    def active_account(self):
        self._readable()
        return self._active()

    def accounts_changed_since(self, stamp):
        """The "file" changed when its content did (the real one compares time, size, id)."""
        self.stamps += 1
        new = (tuple(self.entries), self.active)
        return stamp is None or new != stamp, new

    def use_account(self, key):
        self._readable()
        account = next((a for a in self.entries if a.key == key), None)
        if account is None:
            raise ValueError(f"No account {key}")
        if self.use_error is not None:
            raise self.use_error
        self.active = key
        return account

    def add_account(self, tenant=None, timeout=azure_auth.LOGIN_TIMEOUT, on_warning=None):
        self._readable()  # as azure_auth: before the browser opens
        self.added += 1
        self.tenants.append(tenant)
        if self.browser is not None:
            self.signing.set()
            try:
                assert self.browser.wait(10), "the test never finished the sign-in"
            finally:
                self.signing.clear()
        if self.add_error:
            raise ApiError(self.add_error)
        user = self.next_user[0].home_account_id
        self.entries = [a for a in self.entries if a.home_account_id != user] + self.next_user
        self.active = self.next_user[0].key
        if self.listing_warning and on_warning is not None:
            on_warning(self.listing_warning)
        return list(self.next_user)

    def ensure_account(self, timeout=azure_auth.LOGIN_TIMEOUT):
        self.ensured += 1
        with self._login:  # a second caller waits, then finds the first one's account
            if self.active_account() is None:
                self.add_account()
        return self.active_account()

    def remove_account(self, key):
        self._readable()
        if not any(a.key == key for a in self.entries):
            raise ValueError(f"No account {key}")
        self.removed.append(key)
        self.entries = [a for a in self.entries if a.key != key]
        if self.active == key:
            self.active = self.entries[0].key if self.entries else None
        if self.remove_error:  # the entry is gone, its tokens are not
            raise ApiError(self.remove_error)

    def sign_in_in_progress(self):
        self.polls += 1
        return self.signing.is_set()

    def token_cache_encrypted(self):
        return self.encrypted


class Notifications(UserNotify):
    """A simulated tab's notifications with their type (user.notify = Notifications())."""

    def __init__(self):
        super().__init__()
        self.types: dict[str, str] = {}

    def __call__(self, message: str, **kwargs) -> None:
        super().__call__(message, **kwargs)
        self.types[message] = kwargs.get("type")


@pytest.fixture
def sample(tmp_path, monkeypatch):
    """Sample.pbix + Sample.bim; signed in as ALICE (a test that needs other accounts makes its
    own FakeAccounts)."""
    write_sample_pbix(tmp_path / "Sample.pbix")
    (tmp_path / "Sample.bim").write_text(json.dumps(BIM), encoding="utf-8")
    FakeAccounts(monkeypatch, ALICE, active=ALICE.key)
    # The catalog in use is module state: another test's catalog must not show up (its rail
    # entries would, e.g. "Lineage across models" - and "Lineage" is what tests wait for)
    monkeypatch.setitem(web_ui._CATALOG, "dir", None)
    # "Cannot read the account list" is logged once per process: not left over from a test
    monkeypatch.setattr(web_sources, "_logged_read_error", "")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _simulate(scenario) -> None:
    async def run() -> None:
        async with user_simulation(web_ui.index) as user:
            web_ui.register_routes()
            await scenario(user)

    asyncio.run(run())


def test_page_renders(sample):
    async def scenario(user):
        await user.open("/")
        await user.should_see("Create documentation")

    _simulate(scenario)


def _page_texts(user) -> str:
    """Every label, switch, input and button text of the page (also hidden ones), lower case."""
    texts = []
    for element in user.client.elements.values():
        for attribute in ("text", "label"):
            texts.append(str(getattr(element, attribute, "") or ""))
        texts.append(str(element.props.get("label", "") or ""))
        texts.append(str(element.props.get("placeholder", "") or ""))
    return "\n".join(texts).lower()


def test_page_has_no_tabular_editor_or_desktop_parts(sample):
    """Tabular Editor and Power BI Desktop detection are gone (D21, D33): no folder field,
    no switches, no found/not-found status and no "open in Power BI Desktop" line."""

    async def scenario(user):
        await user.open("/")
        await user.should_see("Create documentation")
        user.find(marker="report").type(str(sample / "Sample.pbix"))
        await asyncio.sleep(0.7)  # past the old debounce of the Desktop scan
        await user.should_not_see(marker="te_folder")
        texts = _page_texts(user)
        assert "create documentation" in texts and "row counts and distinct values" in texts
        for gone in ("tabular editor", "daxformatter", "power bi desktop", "live statistics"):
            assert gone not in texts, gone
        assert not hasattr(web_ui, "find_local_instances")
        assert not hasattr(web_ui, "find_tabular_editor")

    _simulate(scenario)


def test_old_stored_choices_for_removed_options_are_ignored(sample):
    """Choices remembered by an older version (Tabular Editor folder, switches) neither break
    the page nor reach a run, and are never written again."""
    old = {"te_folder": r"C:\Tools\TabularEditor", "tabular_editor_analysis": False,
           "tabular_editor_tsv": True, "te_analysis": False, "te_tsv": True}

    async def scenario(user):
        web_ui.stored_choices().update(old)
        await user.open("/")
        await user.should_see("Create documentation")
        assert "tabular editor" not in _page_texts(user)
        choices = web_ui.stored_choices()
        assert {key: choices.get(key) for key in old} == old  # left alone, not rewritten

    _simulate(scenario)


def test_run_passes_no_tabular_editor_option_and_logs_no_note(sample, monkeypatch, caplog):
    from pbixtractor import pipeline

    seen = []
    real = web_ui.run_extraction

    def recording(options, progress, on_log):
        seen.append(options)
        return real(options, progress, on_log)

    monkeypatch.setattr(web_ui, "run_extraction", recording)
    caplog.set_level(logging.INFO, logger="pbixtractor")

    async def scenario(user):
        web_ui.stored_choices().update({"tabular_editor_analysis": False, "te_tsv": True})
        await user.open("/")
        user.find(marker="report").type(str(sample / "Sample.pbix"))
        user.find("Create documentation").click()
        await _wait_for(lambda: bool(seen) and (sample / "output" / "Sample" / "Sample.xlsx").is_file(),
                        timeout=60)
        await user.should_see("Lineage")

    _simulate(scenario)
    assert len(seen) == 1
    assert seen[0].tabular_editor_analysis is pipeline.ExtractionOptions.tabular_editor_analysis
    assert seen[0].tabular_editor_tsv is pipeline.ExtractionOptions.tabular_editor_tsv
    assert pipeline.TABULAR_EDITOR_NOTE not in caplog.text


def test_report_path_fills_model_and_output(sample):
    async def scenario(user):
        await user.open("/")
        user.find(marker="report").type(str(sample / "Sample.pbix"))
        model = next(iter(user.find(marker="model").elements))
        output = next(iter(user.find(marker="output").elements))
        assert model.value == str(sample / "Sample.bim")
        assert output.value == str(sample / "output" / "Sample")

    _simulate(scenario)


def test_changing_the_report_replaces_the_found_model_but_not_a_chosen_one(sample):
    other = sample / "other"
    other.mkdir()
    (other / "Other.pbix").write_bytes((sample / "Sample.pbix").read_bytes())
    (other / "Other.bim").write_text((sample / "Sample.bim").read_text(encoding="utf-8"), encoding="utf-8")

    async def scenario(user):
        await user.open("/")
        report = next(iter(user.find(marker="report").elements))
        model = next(iter(user.find(marker="model").elements))
        report.value = str(sample / "Sample.pbix")
        assert model.value == str(sample / "Sample.bim")
        report.value = str(other / "Other.pbix")  # the audit found Sample.bim kept here
        assert model.value == str(other / "Other.bim")
        model.value = str(sample / "Sample.bim")  # chosen by the user: kept
        report.value = str(sample / "Sample.pbix")
        assert model.value == str(sample / "Sample.bim")

    _simulate(scenario)


def test_run_shows_results_and_serves_lineage(sample):
    async def scenario(user):
        await user.open("/")
        user.find(marker="report").type(str(sample / "Sample.pbix"))
        user.find("Create documentation").click()
        for _ in range(300):  # the extraction runs in a background thread
            if user._sees(None, ui.tabs, None, None) or user._sees("Failed", None, None, None):
                break
            await asyncio.sleep(0.1)
        await user.should_see("Lineage")
        await user.should_see("Unused measures")
        assert (sample / "output" / "Sample" / "Sample.xlsx").is_file()
        # The sample model has an RLS role: the page says the model is protected
        await user.should_see(marker="rls_notice")
        await user.should_see("RLS roles")
        await user.should_see("Security")

        # The lineage iframe is served through the /files route
        token = next(t for t, f in web_ui._OUTPUT_DIRS.items() if f == sample / "output" / "Sample")
        response = await user.http_client.get(f"/files/{token}/Sample_lineage.html")
        assert response.status_code == 200 and "<svg" in response.text.lower()
        # Only files inside the run's folder
        assert (await user.http_client.get("/files/unknown/Sample.xlsx")).status_code == 404
        assert (await user.http_client.get(f"/files/{token}/nope.txt")).status_code == 404
        with pytest.raises(HTTPException):
            web_ui._serve_output_file(token, "../Sample.pbix")

    _simulate(scenario)


def test_catalog_route_serves_only_catalog_files(tmp_path, monkeypatch):
    catalog = tmp_path / "catalog"
    (catalog / "models" / "k").mkdir(parents=True)
    (catalog / "catalog.html").write_text("<html></html>")
    (catalog / "models" / "k" / "x_lineage.html").write_text("<html></html>")
    (catalog / "secret.txt").write_text("not for the browser")
    (catalog / "notes").mkdir()
    (catalog / "notes" / "id_rsa").write_text("key")
    monkeypatch.setitem(web_ui._CATALOG, "dir", catalog)

    page = web_ui._serve_catalog_file("catalog.html")
    assert "sandbox" in page.headers["content-security-policy"]  # runs in an opaque origin
    assert web_ui._serve_catalog_file("models/k/x_lineage.html").status_code == 200
    (catalog / "lineage.html").write_text("<html></html>")  # the lineage across models
    assert web_ui._serve_catalog_file("lineage.html").status_code == 200
    for path in ("secret.txt", "notes/id_rsa", "models/../secret.txt", "../catalog/secret.txt"):
        with pytest.raises(HTTPException):
            web_ui._serve_catalog_file(path)


def test_unknown_hosts_are_refused():
    """DNS rebinding: a page on another host name pointing at 127.0.0.1 gets nothing."""
    import httpx
    from fastapi import FastAPI

    server = FastAPI()
    server.get("/")(lambda: {"ok": True})
    original, web_ui.app = web_ui.app, server
    try:
        web_ui.add_host_check(8081)
    finally:
        web_ui.app = original

    async def status(host: str) -> int:
        transport = httpx.ASGITransport(app=server)
        async with httpx.AsyncClient(transport=transport, base_url="http://x") as client:
            return (await client.get("/", headers={"host": host})).status_code

    assert asyncio.run(status("127.0.0.1:8081")) == 200
    assert asyncio.run(status("localhost:8081")) == 200
    assert asyncio.run(status("evil.example:8081")) == 421


def test_missing_report_warns(sample):
    async def scenario(user):
        await user.open("/")
        user.find(marker="report").type(str(sample / "Nope.pbix"))
        user.find("Create documentation").click()
        await user.should_see("Choose an existing report first.")

    _simulate(scenario)


def test_dropped_files_are_copied_and_filled_in(sample):
    report_bytes = (sample / "Sample.pbix").read_bytes()
    model_bytes = (sample / "Sample.bim").read_bytes()

    async def scenario(user):
        await user.open("/")
        upload = user.find(marker="upload").elements.pop()
        # The model and the report of one drop may arrive in any order
        await upload.handle_uploads([ui.upload.SmallFileUpload("Dropped.bim", "", model_bytes)])
        await upload.handle_uploads([ui.upload.SmallFileUpload("Dropped.pbix", "", report_bytes)])
        uploads = sample / "output" / "_uploads"
        await _wait_for(lambda: _value(user, "model") == str(uploads / "Dropped.bim"))
        assert (uploads / "Dropped.pbix").read_bytes() == report_bytes
        assert user.find(marker="report").elements.pop().value == str(uploads / "Dropped.pbix")
        assert user.find(marker="model").elements.pop().value == str(uploads / "Dropped.bim")
        assert user.find(marker="output").elements.pop().value.endswith("Dropped")

        await upload.handle_uploads([ui.upload.SmallFileUpload("notes.txt", "", b"x")])
        await user.should_see("drop a .pbix report or a .bim model")

    _simulate(scenario)


def _element(user, marker: str):
    return next(iter(user.find(marker=marker).elements))


def _any_element(user, marker: str):
    """An element by marker, also while it is hidden (user.find only sees visible ones)."""
    return next(e for e in list(user.client.elements.values()) if marker in e._markers)


def _value(user, marker: str):
    return _element(user, marker).value


def _options(user, marker: str) -> dict:
    return _element(user, marker).options


async def _wait_for(condition, timeout: float = 10.0) -> None:
    """Poll until an (async) handler has done its work - no fixed sleeps that are too short
    on a slow machine."""
    for _ in range(int(timeout / 0.05)):
        if condition():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")


async def _choose(user, marker: str, value, until=None) -> None:
    """Set a select/input like a user would; `until`: what its handler does (waited for)."""
    _element(user, marker).value = value
    await _wait_for(until or (lambda: True))


def test_document_a_report_from_devops(sample, monkeypatch):
    from .test_devops import FakeDevOps, _repo_files

    client = FakeDevOps(_repo_files(sample))
    monkeypatch.setattr(web_sources, "make_devops_client", lambda org: client)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "devops")
        await _choose(user, "devops_org", "contoso")
        user.find(marker="devops_load").click()
        await _wait_for(lambda: _options(user, "devops_project"))
        await _choose(user, "devops_project", "BI Team", until=lambda: _options(user, "devops_repo"))
        # the repository's default branch is preselected, and its reports listed
        await _choose(user, "devops_repo", "reports", until=lambda: _value(user, "devops_branch"))
        assert _value(user, "devops_branch") == "main"
        await _wait_for(lambda: _options(user, "devops_report"))
        await _choose(
            user, "devops_report", "/Reports/Sample.Report",
            until=lambda: len(_options(user, "devops_version")) > 1,
        )
        assert "a1b2c3d4" in list(_options(user, "devops_version").values())[1]  # commit history
        await _choose(user, "devops_version", "a1b2c3d4e5f6")
        assert user.find(marker="output").elements.pop().value.endswith("Sample")

        user.find("Create documentation").click()
        await user.should_see("Lineage", retries=100)
        assert (sample / "output" / "Sample" / "Sample.xlsx").is_file()
        zips = [q for p, q in client.calls if q.get("$format") == "zip"]
        assert zips[0]["versionDescriptor.version"] == "a1b2c3d4e5f6"

    _simulate(scenario)


def test_document_a_report_from_fabric(sample, monkeypatch):
    from .test_fabric import REPORT_ID, WS_SALES, FakeFabric, _report_parts

    client = FakeFabric(
        _report_parts(sample / "a", "Sales WS"),
        other_report_parts=_report_parts(sample / "b", "Sales WS"),
    )
    monkeypatch.setattr(web_sources, "make_fabric_client", lambda: client)
    monkeypatch.setattr("pbixtractor.service_stats.make_client", lambda: client)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        user.find("Create documentation").click()
        await user.should_see("Choose a report first.")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: _options(user, "fabric_workspace"))
        await _choose(user, "fabric_workspace", WS_SALES, until=lambda: _options(user, "fabric_report"))
        await _choose(user, "fabric_report", REPORT_ID)
        await _choose(user, "fabric_all_reports", True)  # model mode
        user.find("Create documentation").click()
        await user.should_see("Lineage", retries=100)
        # Named after the model; both reports' pages; statistics from the service
        document = json.loads(
            (sample / "output" / "Sample Model" / "Sample Model.json").read_text(encoding="utf-8")
        )
        pages = [page["name"] for page in document["report"]["pages"]]
        assert "Sample Detail › Sales" in pages
        sales = next(t for t in document["model"]["tables"] if t["name"] == "Sales")
        assert sales["rows"] == 1000

    _simulate(scenario)


def test_browser_opens_only_without_a_reconnecting_tab(monkeypatch):
    opened = []
    monkeypatch.setattr(web_ui, "BROWSER_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(web_ui.webbrowser, "open", opened.append)

    async def scenario() -> None:
        reconnected = asyncio.Event()
        reconnected.set()
        await web_ui._open_browser_unless_reconnected("http://x/", reconnected)
        assert opened == []
        await web_ui._open_browser_unless_reconnected("http://x/", asyncio.Event())
        assert opened == ["http://x/"]

    asyncio.run(scenario())


def test_tests_never_use_the_real_nicegui_storage():
    """user_simulation deletes NiceGUI's storage folder; conftest points it at a temp folder so
    running the tests does not wipe the web UI's remembered choices in the repo."""
    from nicegui.storage import Storage

    repo = Path(__file__).resolve().parent.parent
    assert repo not in Storage.path.parents and Storage.path != repo / ".nicegui"


def test_log_count_counts_messages_not_lines():
    logs = "WARNING: first\nWARNING: Tabular Editor failed:\n  details\n  more\nERROR: last\n"
    assert web_ui._log_count(logs) == 3
    assert web_ui._log_count("") == 0


def test_bookmark_filter_text():
    captured = {"captures": ["Data", "Display"], "filters": [{"text": "Sales: T[C] = 1"},
                                                             {"text": "All pages: T[D] = 2 (changed)"}]}
    assert web_ui._bookmark_filter_text(captured) == "Sales: T[C] = 1\nAll pages: T[D] = 2 (changed)"
    assert web_ui._bookmark_filter_text({"captures": ["Display"], "filters": []}) == (
        "(does not capture data)"
    )


def test_theme_fonts_are_served_and_the_band_follows_the_accent(sample):
    from pbixtractor import theme

    async def scenario(user):
        await user.open("/")
        await user.should_see("Document a report")
        for name in ("familjen-grotesk-latin.woff2", "source-sans-3-latin.woff2"):
            response = await user.http_client.get(f"{theme.FONTS_URL}/{name}")
            assert response.status_code == 200 and response.content[:4] == b"wOF2"

    _simulate(scenario)
    # A white-label accent re-derives the header/rail ground; nothing hard-codes the default
    assert theme.darken("#3f7d5a", 0.55) in theme.theme_css()
    assert theme.darken("#aa3366", 0.55) in theme.theme_css("#aa3366")
    assert "#3f7d5a" not in theme.theme_css("#aa3366").replace(theme.OK_HEX, "")


def _drop(upload, *files):
    """Drop files one by one, as a browser hands them over."""
    async def run():
        for name, content in files:
            await upload.handle_uploads([ui.upload.SmallFileUpload(name, "", content)])
    return run()


def test_dropped_reports_on_one_model_are_documented_together(sample):
    report, model = (sample / "Sample.pbix").read_bytes(), (sample / "Sample.bim").read_bytes()
    uploads = sample / "output" / "_uploads"

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"),
                    ("First.pbix", report), ("Second.pbix", report), ("Shared.bim", model))
        await _wait_for(lambda: _value(user, "model") == str(uploads / "Shared.bim"))
        assert _value(user, "report") == str(uploads / "First.pbix")
        await user.should_see(marker="together")
        user.find("Create documentation").click()
        await user.should_see("Lineage", retries=100)
        # One documentation, named after the model, with both reports
        doc = json.loads((sample / "output" / "Shared" / "Shared.json").read_text(encoding="utf-8"))
        assert [r["name"] for r in doc["reports"]] == ["First", "Second"]
        await user.should_see("Reports")  # the stat tile, shown for several reports

    _simulate(scenario)


async def _batch_done(user) -> None:
    """Wait for a batch to finish (its summary appears)."""
    await _wait_for(lambda: user._sees(None, None, "batch_summary", None), timeout=60)


def test_dropped_reports_on_different_models_are_documented_separately(sample):
    report, model = (sample / "Sample.pbix").read_bytes(), (sample / "Sample.bim").read_bytes()
    from pbixtractor.catalog import list_entries

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"), ("First.pbix", report), ("First.bim", model),
                    ("Second.pbix", report), ("Second.bim", model))
        await user.should_see("2 models: each is documented separately")  # before the run
        user.find("Create documentation").click()
        await _batch_done(user)
        await user.should_see("2 of 2 models documented")
        # Each report with the model named like it, both in the catalog
        assert (sample / "output" / "First" / "First.xlsx").is_file()
        assert (sample / "output" / "Second" / "Second.xlsx").is_file()
        assert sorted(e["name"] for e in list_entries(sample / "output" / "_catalog")) == ["First", "Second"]
        await user.should_see(marker="batch_catalog")
        await user.should_see(marker="batch_cross_lineage")  # both models in one lineage viewer
        # A model's Details: its full result, as after a single run
        user.find(marker="batch_details").click()
        await user.should_see("Unused measures")

    _simulate(scenario)


def test_dropped_reports_without_any_model_are_refused(sample):
    report = (sample / "Sample.pbix").read_bytes()  # no model of its own, no .bim dropped

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"), ("First.pbix", report), ("Second.pbix", report))
        await user.should_see("No model found for these reports")  # said before the run
        user.find("Create documentation").click()
        await user.should_see("Nothing to document")
        await user.should_not_see(marker="batch_results")
        assert not (sample / "output" / "First").exists()

    _simulate(scenario)


def test_a_whole_fabric_workspace_is_documented_model_by_model(sample, monkeypatch):
    from pbixtractor.catalog import list_entries

    from .test_batch import WS_REPORTS, BatchFabric

    client = BatchFabric(sample / "parts")
    monkeypatch.setattr(web_sources, "make_fabric_client", lambda: client)
    monkeypatch.setattr("pbixtractor.service_stats.make_client", lambda: client)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        await _choose(user, "fabric_scope", "workspace")
        user.find(marker="fabric_plan_list").click()
        await user.should_see("Choose one or more workspaces first")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: _options(user, "fabric_workspaces"))
        await _choose(user, "fabric_workspaces", [WS_REPORTS["id"]])
        user.find(marker="fabric_plan_list").click()
        await _wait_for(lambda: len(_any_element(user, "fabric_plan_table").rows) == 2)
        # What cannot be documented is listed with the reason
        assert {e.text for e in user.find(marker="fabric_plan_skipped").elements} == {"Cannot be documented (3)"}
        await user.should_see("Paginated report - not supported")
        # Nothing is ticked at first: nothing runs
        user.find("Create documentation").click()
        await user.should_see("List the models and tick the ones to document first.")
        user.find(marker="fabric_plan_all").click()
        await user.should_see("2 of 2 models selected")
        user.find("Create documentation").click()
        await _batch_done(user)
        # Two models documented (one per model, with all its reports)
        await user.should_see("2 of 2 models documented")
        doc = json.loads(
            (sample / "output" / "Sales Model" / "Sales Model.json").read_text(encoding="utf-8")
        )
        assert [r["name"] for r in doc["reports"]] == ["Sales Detail", "Sales Overview"]
        sales = next(t for t in doc["model"]["tables"] if t["name"] == "Sales")
        assert sales["rows"] == 1000  # statistics from the service, as for a single report
        assert {e["name"] for e in list_entries(sample / "output" / "_catalog")} == {"Finance", "Sales Model"}

    _simulate(scenario)


def test_a_later_drop_starts_over(sample, monkeypatch):
    monkeypatch.setattr(web_ui, "DROP_SECONDS", -1)  # every file arrives "later"
    report = (sample / "Sample.pbix").read_bytes()
    uploads = sample / "output" / "_uploads"

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"), ("First.pbix", report), ("Second.pbix", report))
        await _wait_for(lambda: _value(user, "report") == str(uploads / "Second.pbix"))
        await user.should_not_see(marker="together")

    _simulate(scenario)


def test_an_uploaded_live_connected_pbix_gets_its_model_from_fabric(sample, monkeypatch):
    import zipfile

    from .test_fabric import MODEL_ID, WS_MODELS, FakeFabric, _report_parts

    with zipfile.ZipFile(sample / "Sample.pbix", "a") as archive:
        archive.writestr("Connections", json.dumps({"Version": 3, "Connections": [{
            "ConnectionType": "pbiServiceLive", "PbiModelDatabaseName": MODEL_ID,
            "ConnectionString": "Data Source=pbiazure://api.powerbi.com"}]}))
    client = FakeFabric(_report_parts(sample / "x", "Shared Models"), model_workspace=WS_MODELS)
    monkeypatch.setattr(web_sources, "make_fabric_client", lambda: client)
    monkeypatch.setattr("pbixtractor.service_stats.make_client", lambda: client)

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"), ("Thin.pbix", (sample / "Sample.pbix").read_bytes()))
        await _wait_for(lambda: _value(user, "report").endswith("Thin.pbix"))
        assert _value(user, "model") == ""  # no model of its own
        user.find("Create documentation").click()
        await user.should_see("Lineage", retries=100)
        doc = json.loads((sample / "output" / "Thin" / "Thin.json").read_text(encoding="utf-8"))
        assert next(t for t in doc["model"]["tables"] if t["name"] == "Sales")["rows"] == 1000
        assert (sample / "output" / "_fabric" / "Shared Models" / "Sample Model.SemanticModel").is_dir()

    _simulate(scenario)


def test_a_devops_report_on_a_published_model_gets_it_from_fabric(sample, monkeypatch):
    from .test_devops import FakeDevOps, _bound_to_service, _repo_files
    from .test_fabric import MODEL_ID, WS_SALES, FakeFabric, _report_parts

    files = _bound_to_service(_repo_files(sample, bound_by_path=False), "/Reports/Sample.Report", MODEL_ID)
    devops_client = FakeDevOps(files)
    fabric_client = FakeFabric(_report_parts(sample / "x", "Sales WS"), model_workspace=WS_SALES)
    monkeypatch.setattr(web_sources, "make_devops_client", lambda org: devops_client)
    monkeypatch.setattr(web_sources, "make_fabric_client", lambda: fabric_client)
    monkeypatch.setattr("pbixtractor.service_stats.make_client", lambda: fabric_client)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "devops")
        await _choose(user, "devops_org", "contoso")
        user.find(marker="devops_load").click()
        await _wait_for(lambda: _options(user, "devops_project"))
        await _choose(user, "devops_project", "BI Team", until=lambda: _options(user, "devops_repo"))
        await _choose(user, "devops_repo", "reports", until=lambda: _value(user, "devops_branch"))
        await _wait_for(lambda: _options(user, "devops_report"))
        await _choose(user, "devops_report", "/Reports/Sample.Report")
        user.find("Create documentation").click()
        await user.should_see("Lineage", retries=100)
        doc = json.loads((sample / "output" / "Sample" / "Sample.json").read_text(encoding="utf-8"))
        assert next(t for t in doc["model"]["tables"] if t["name"] == "Sales")["rows"] == 1000

    _simulate(scenario)


def test_dropped_pbix_files_with_their_own_models_are_flagged_at_once(sample):
    import io
    import zipfile

    buffer = io.BytesIO((sample / "Sample.pbix").read_bytes())
    with zipfile.ZipFile(buffer, "a") as archive:
        archive.writestr("DataModel", b"\x00")  # a model of its own (never read here)
    report = buffer.getvalue()

    async def scenario(user):
        await user.open("/")
        await _drop(_element(user, "upload"), ("First.pbix", report), ("Second.pbix", report))
        # Before any click: the page says these are separate models, documented separately
        await user.should_see(marker="separate_models")
        await user.should_see("2 models: each is documented separately")
        await user.should_see("Dropped:")
        await user.should_not_see("Documented together:")

    _simulate(scenario)


def test_the_page_says_where_the_model_comes_from(sample):
    import io
    import zipfile

    def with_entry(name: str, content) -> bytes:
        buffer = io.BytesIO((sample / "Sample.pbix").read_bytes())
        with zipfile.ZipFile(buffer, "a") as archive:
            archive.writestr(name, content)
        return buffer.getvalue()

    thick = with_entry("DataModel", b"\x00")
    thin = with_entry("Connections", json.dumps({"Connections": [{
        "ConnectionType": "pbiServiceLive", "PbiModelDatabaseName": "44444444-4444-4444-4444-444444444444",
        "ConnectionString": "Data Source=pbiazure://api.powerbi.com"}]}))

    async def scenario(user):
        await user.open("/")
        upload = _element(user, "upload")
        await _drop(upload, ("Thick.pbix", thick))
        await user.should_see("Model: inside the report")
        _element(user, "selected")  # the report shows as a chip
        web_ui.DROP_SECONDS, saved = -1, web_ui.DROP_SECONDS  # the next file is a new drop
        try:
            await _drop(upload, ("Thin.pbix", thin))
            await user.should_see("downloaded from Fabric when you run")
            await _drop(upload, ("Plain.pbix", (sample / "Sample.pbix").read_bytes()))
            await user.should_see("Model: not found")
        finally:
            web_ui.DROP_SECONDS = saved
        # Removing the last report starts over
        chip = next(e for e in user.client.elements.values() if isinstance(e, ui.chip))
        chip.value = False
        await _wait_for(lambda: _value(user, "report") == "")
        await user.should_not_see(marker="selected")

    _simulate(scenario)


def test_only_the_ticked_models_of_a_devops_repository_are_documented(sample, monkeypatch):
    from .test_batch import BatchFabric, _devops_repo
    from .test_devops import FakeDevOps

    devops_client = FakeDevOps(_devops_repo(sample / "repo"))
    monkeypatch.setattr(web_sources, "make_devops_client", lambda org: devops_client)
    monkeypatch.setattr(web_sources, "make_fabric_client", lambda: BatchFabric(sample / "parts"))

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "devops")
        await _choose(user, "devops_scope", "repository")
        await _choose(user, "devops_org", "contoso")
        user.find(marker="devops_load").click()
        await _wait_for(lambda: _options(user, "devops_project"))
        await _choose(user, "devops_project", "BI Team", until=lambda: _options(user, "devops_repo"))
        await _choose(user, "devops_repo", "reports", until=lambda: _value(user, "devops_branch"))
        user.find(marker="devops_plan_list").click()
        await _wait_for(lambda: len(_any_element(user, "devops_plan_table").rows) == 3)
        await user.should_see("0 of 3 models selected")
        # "Select all" ticks what the filter shows
        await _choose(user, "devops_plan_filter", "other")
        user.find(marker="devops_plan_all").click()
        await user.should_see("1 of 3 models selected")
        user.find("Create documentation").click()
        await _batch_done(user)
        await user.should_see("1 of 1 model documented")
        assert (sample / "output" / "Other" / "Other.xlsx").is_file()
        assert not (sample / "output" / "Sample").exists()
        # Another scope (here: a folder): the listed models no longer apply
        await _choose(user, "devops_folder", "/Reports")
        await _wait_for(lambda: not _any_element(user, "devops_plan_table").rows)

    _simulate(scenario)


# ---------------------------------------------------------------------------
# The account control (one account for Fabric and DevOps, switchable in both panels)
# ---------------------------------------------------------------------------


def _account_clients(monkeypatch) -> dict:
    """Fake Fabric and DevOps clients bound to the account active when they are made (as
    get_credential is): their lists name that account's tenant. Returns the clients made
    (and under "loads" the tenant of each workspace list asked for)."""
    made = {"fabric": [], "devops": [], "loads": []}

    def tenant() -> str:
        return getattr(azure_auth.active_account(), "tenant_name", "nobody")

    class Fabric:
        def __init__(self):
            self.tenant = tenant()
            made["fabric"].append(self)

        def workspaces(self):
            made["loads"].append(self.tenant)
            return [{"id": f"ws-{self.tenant}", "displayName": f"{self.tenant} workspace"}]

        def reports(self, workspace):
            return [{"id": f"r-{self.tenant}", "displayName": f"{self.tenant} report"}]

    class DevOps:
        def __init__(self, org):
            self.org, self.tenant = org, tenant()
            made["devops"].append(self)

        def projects(self):
            return [f"{self.tenant} project"]

    monkeypatch.setattr(web_sources, "make_fabric_client", Fabric)
    monkeypatch.setattr(web_sources, "make_devops_client", DevOps)
    return made


def _run_button(user):
    """The "Create documentation" button (also while it is disabled)."""
    return next(iter(user.find("Create documentation").elements))


def _account_timers(user) -> list:
    """The timers an AccountState started in this tab (its poll; a once-timer while due)."""
    return [
        element
        for element in list(user.client.elements.values())
        if isinstance(element, ui.timer)
        and isinstance(getattr(element.callback, "__self__", None), web_sources.AccountState)
    ]


def _remove_entry(user, panel: str, account: Account) -> None:
    """Click an entry's remove icon in the account dropdown: the browser sends the option's
    index and label (the select's "remove_entry" event)."""
    options = _element(user, f"{panel}_account").options
    index = list(options).index(account.key)
    user.find(marker=f"{panel}_account").trigger(
        "remove_entry", {"index": index, "label": options[account.key]}
    )


def test_the_account_control_shows_the_active_account(sample, monkeypatch):
    FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, active=ALICE_GUEST.key, encrypted=False)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        await user.should_see("Signed in as alice@contoso.com · Fabrikam")
        assert _value(user, "fabric_account") == ALICE_GUEST.key
        assert list(_options(user, "fabric_account").values()) == [ALICE.label, ALICE_GUEST.label]
        await _wait_for(lambda: _any_element(user, "fabric_account_note").visible)
        await user.should_see(web_sources.UNENCRYPTED)
        # The DevOps panel shows the same account
        await _choose(user, "source", "devops")
        assert _value(user, "devops_account") == ALICE_GUEST.key
        await user.should_see(marker="devops_account_note")

    _simulate(scenario)


def test_switching_the_account_reloads_both_panels_with_new_clients(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, active=ALICE.key)
    made = _account_clients(monkeypatch)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "devops")
        await _choose(user, "devops_org", "contoso")
        user.find(marker="devops_load").click()
        await _wait_for(lambda: list(_options(user, "devops_project")) == ["Contoso project"])
        await _choose(user, "source", "fabric")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: _options(user, "fabric_workspace"))
        await _choose(user, "fabric_workspace", "ws-Contoso", until=lambda: _options(user, "fabric_report"))
        await _choose(user, "fabric_report", "r-Contoso")
        await _choose(user, "fabric_all_reports", True)

        await _choose(user, "fabric_account", ALICE_GUEST.key)
        assert accounts.active == ALICE_GUEST.key
        # What the old account loaded is gone, and loaded again with the new one
        await _wait_for(lambda: _options(user, "fabric_workspace") == {"ws-Fabrikam": "Fabrikam workspace"})
        assert _value(user, "fabric_workspace") is None and not _options(user, "fabric_report")
        assert not _value(user, "fabric_all_reports")
        await user.should_see("Signed in as alice@contoso.com · Fabrikam")
        await _choose(user, "source", "devops")
        await _wait_for(lambda: list(_options(user, "devops_project")) == ["Fabrikam project"])
        assert _value(user, "devops_account") == ALICE_GUEST.key
        assert [client.tenant for client in made["fabric"]] == ["Contoso", "Fabrikam"]
        assert [client.tenant for client in made["devops"]] == ["Contoso", "Fabrikam"]

    _simulate(scenario)


def test_add_account_waits_for_the_browser_sign_in_and_shows_failures(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch)  # nobody signed in yet
    accounts.browser = threading.Event()

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        await user.should_see("Not signed in")
        user.find(marker="fabric_account_add").click()
        await user.should_see(web_sources.WAITING_FOR_SIGN_IN)
        assert not _element(user, "fabric_account_add").enabled
        accounts.browser.set()  # the user signs in in the browser
        await user.should_see("Signed in as bob@fabrikam.com · Fabrikam")
        assert _value(user, "fabric_account") == BOB.key
        assert _any_element(user, "devops_account").value == BOB.key  # the other panel too
        assert not _any_element(user, "fabric_account_hint").visible
        # A failed (or timed-out) sign-in says why
        accounts.add_error = "Sign-in timed out after 180 s"
        user.find(marker="fabric_account_add").click()
        await user.should_see("Sign-in timed out after 180 s")
        assert accounts.added == 2 and accounts.active == BOB.key

    _simulate(scenario)


def test_sign_in_and_load_without_an_account_signs_in_first(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch)
    made = _account_clients(monkeypatch)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        accounts.add_error = "Sign-in was cancelled"
        user.find(marker="fabric_load").click()
        await user.should_see("Sign-in was cancelled")
        assert accounts.added == 1 and not made["fabric"]  # nothing is loaded without an account
        accounts.add_error = None
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: _options(user, "fabric_workspace") == {"ws-Fabrikam": "Fabrikam workspace"})
        assert accounts.added == 2 and accounts.active == BOB.key
        assert accounts.ensured == 2  # through ensure_account: tabs asking at once share one sign-in
        await user.should_see("Signed in as bob@fabrikam.com · Fabrikam")

    _simulate(scenario)


def test_removing_the_active_account_switches_to_the_next_one(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, BOB, active=ALICE.key)
    made = _account_clients(monkeypatch)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Contoso"])
        _remove_entry(user, "fabric", ALICE)
        await user.should_see("Signed in as bob@fabrikam.com · Fabrikam")
        assert accounts.removed == [ALICE.key]
        assert list(_options(user, "fabric_account")) == [BOB.key]
        assert list(_any_element(user, "devops_account").options) == [BOB.key]
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Fabrikam"])
        # The last one: not signed in, nothing loaded - and no sign-in on its own
        _remove_entry(user, "fabric", BOB)
        await user.should_see("Not signed in")
        await _wait_for(lambda: not _options(user, "fabric_workspace"))
        assert not _element(user, "fabric_account").enabled  # nothing left to choose or remove
        assert accounts.added == 0 and len(made["fabric"]) == 2

    _simulate(scenario)


def test_the_account_cannot_change_during_a_run(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, active=ALICE.key)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        await web_ui._JOB_LOCK.acquire()  # a download or documentation run, in any tab
        try:
            await _wait_for(lambda: _any_element(user, "fabric_account_hint").visible)
            await user.should_see(web_sources.WAIT_FOR_RUN)
            for marker in ("fabric_account", "fabric_account_add", "fabric_account_tenant"):
                assert not _element(user, marker).enabled
            _element(user, "fabric_account").value = ALICE_GUEST.key  # it is refused
            await user.should_see(f"{web_sources.WAIT_FOR_RUN}.")
            assert accounts.active == ALICE.key and _value(user, "fabric_account") == ALICE.key
            _remove_entry(user, "fabric", ALICE_GUEST)  # so is removing (a menu left open)
            await _wait_for(lambda: user.notify.messages.count(f"{web_sources.WAIT_FOR_RUN}.") == 2)
            assert not accounts.removed and len(_options(user, "fabric_account")) == 2
        finally:
            web_ui._JOB_LOCK.release()
        await _wait_for(lambda: _element(user, "fabric_account_add").enabled)
        assert not _any_element(user, "fabric_account_hint").visible

    _simulate(scenario)


def test_a_switch_made_in_another_tab_or_on_the_command_line_is_followed(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, active=ALICE.key)
    _account_clients(monkeypatch)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_SECONDS", 0.2)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Contoso"])
        accounts.active = ALICE_GUEST.key  # `pbixtractor account use 2`, or another tab
        await _wait_for(lambda: _value(user, "fabric_account") == ALICE_GUEST.key)
        await user.should_see("Signed in as alice@contoso.com · Fabrikam")
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Fabrikam"])

    _simulate(scenario)


def test_the_devops_panel_says_when_a_personal_access_token_is_used(sample, monkeypatch):
    monkeypatch.setenv("AZURE_DEVOPS_PAT", "token")
    accounts = FakeAccounts(monkeypatch)  # no account: the token needs none
    _account_clients(monkeypatch)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "devops")
        await user.should_see(web_sources.DEVOPS_PAT)
        await user.should_not_see(marker="devops_account")
        await _choose(user, "devops_org", "contoso")
        user.find(marker="devops_load").click()
        await _wait_for(lambda: _options(user, "devops_project"))
        assert accounts.added == 0
        await _choose(user, "source", "fabric")  # Fabric still uses the account
        await user.should_see("Not signed in")
        await user.should_see(marker="fabric_account")

    _simulate(scenario)


def test_a_service_principal_replaces_the_account_choice(sample, monkeypatch):
    for name in ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"):
        monkeypatch.setenv(name, "x")
    accounts = FakeAccounts(monkeypatch)
    _account_clients(monkeypatch)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        await user.should_see(web_sources.SERVICE_PRINCIPAL)
        await user.should_not_see(marker="fabric_account")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: _options(user, "fabric_workspace"))
        assert accounts.added == 0  # no browser sign-in

    _simulate(scenario)


def test_any_entry_is_removed_with_its_own_icon_without_choosing_it(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, BOB, active=ALICE.key)
    made = _account_clients(monkeypatch)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Contoso"])
        await _choose(user, "fabric_workspace", "ws-Contoso", until=lambda: _options(user, "fabric_report"))
        # Another entry: removed without switching to it, and the lists stay
        _remove_entry(user, "fabric", BOB)
        await _wait_for(lambda: BOB.key not in _options(user, "fabric_account"))
        await user.should_see(f"Removed {BOB.label}")
        assert accounts.removed == [BOB.key] and accounts.active == ALICE.key
        assert BOB.key not in _any_element(user, "devops_account").options
        assert _value(user, "fabric_account") == ALICE.key
        assert _value(user, "fabric_workspace") == "ws-Contoso" and _options(user, "fabric_report")
        assert len(made["fabric"]) == 1
        # The active entry: the next one is active, and the panels reload as after a switch
        _remove_entry(user, "fabric", ALICE)
        await user.should_see("Signed in as alice@contoso.com · Fabrikam")
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Fabrikam"])
        assert _value(user, "fabric_workspace") is None and not _options(user, "fabric_report")
        assert [client.tenant for client in made["fabric"]] == ["Contoso", "Fabrikam"]

    _simulate(scenario)


def test_an_entry_whose_tokens_could_not_be_deleted_is_removed_with_a_warning(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, BOB, active=ALICE.key)
    accounts.remove_error = (
        f"Removed {BOB.label}, but could not delete the tokens of bob@fabrikam.com from "
        "msal_token_cache.bin: locked"
    )

    async def scenario(user):
        notes = user.notify = Notifications()
        await user.open("/")
        await _choose(user, "source", "fabric")
        _remove_entry(user, "fabric", BOB)
        await _wait_for(lambda: BOB.key not in _options(user, "fabric_account"))
        await user.should_see("could not delete the tokens")
        assert notes.types[accounts.remove_error] == "warning"
        assert f"Removed {BOB.label}" not in notes.messages  # not shown as a plain success
        assert accounts.active == ALICE.key

    _simulate(scenario)


def test_add_account_signs_in_at_a_typed_tenant_and_warns_when_tenants_are_not_listed(
    sample, monkeypatch
):
    accounts = FakeAccounts(monkeypatch)
    accounts.listing_warning = (
        "Could not list the tenants of bob@fabrikam.com (HTTP 403): only the tenant signed in to "
        "was added or updated - add other tenants by their tenant id or domain"
    )

    async def scenario(user):
        notes = user.notify = Notifications()
        await user.open("/")
        await _choose(user, "source", "devops")  # the same control as the Fabric panel's
        await _choose(user, "devops_account_tenant", " fabrikam.onmicrosoft.com ")
        user.find(marker="devops_account_add").click()
        await user.should_see("Signed in as bob@fabrikam.com · Fabrikam")
        assert accounts.tenants == ["fabrikam.onmicrosoft.com"]
        await user.should_see("Could not list the tenants of bob@fabrikam.com")
        assert notes.types[accounts.listing_warning] == "warning"
        assert _value(user, "devops_account_tenant") == ""  # a one-off: cleared after the sign-in
        # Without a tenant: the user's home tenant
        accounts.listing_warning = None
        user.find(marker="devops_account_add").click()
        await _wait_for(lambda: accounts.added == 2)
        assert accounts.tenants[-1] is None

    _simulate(scenario)


def test_a_sign_in_in_another_tab_disables_the_control_and_refuses_runs(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, active=ALICE.key)
    accounts.browser = threading.Event()
    monkeypatch.setattr(web_sources, "BUSY_POLL_SECONDS", 0.1)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        other = User(user.http_client)  # a second tab
        await other.open("/")
        await _choose(other, "source", "fabric")
        user.find(marker="fabric_account_add").click()  # the first tab opens the browser sign-in
        await _wait_for(accounts.signing.is_set)
        # The second tab shows it, and can neither switch, add, remove nor run
        await _wait_for(lambda: _any_element(other, "fabric_account_hint").visible)
        await other.should_see(web_sources.WAITING_FOR_SIGN_IN)
        for marker in ("fabric_account", "fabric_account_add", "fabric_account_tenant"):
            assert not _element(other, marker).enabled
        await _wait_for(lambda: not _run_button(other).enabled)  # no run starts meanwhile
        await other.should_see(marker="run_sign_in")
        other.find("Create documentation").trigger("click")  # the safety net behind it
        await other.should_see(web_sources.FINISH_SIGN_IN)
        _remove_entry(other, "fabric", ALICE)
        await _wait_for(lambda: web_sources.WAITING_FOR_SIGN_IN in other.notify.messages)
        assert not accounts.removed
        # The sign-in ends: the second tab follows the new active entry and is usable again
        accounts.browser.set()
        await _wait_for(lambda: _value(other, "fabric_account") == BOB.key)
        await _wait_for(lambda: _element(other, "fabric_account_add").enabled)
        assert not _any_element(other, "fabric_account_hint").visible
        await _wait_for(lambda: _run_button(other).enabled)
        assert not _any_element(other, "run_sign_in").visible

    _simulate(scenario)


def test_two_tabs_loading_without_an_account_share_one_sign_in(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch)
    accounts.browser = threading.Event()
    made = _account_clients(monkeypatch)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        other = User(user.http_client)  # a second tab
        await other.open("/")
        await _choose(other, "source", "fabric")
        user.find(marker="fabric_load").click()  # the first tab opens the browser sign-in
        await _wait_for(accounts.signing.is_set)
        other.find(marker="fabric_load").click()  # the second waits for it
        await _wait_for(lambda: accounts.ensured == 2)
        accounts.browser.set()
        for tab in (user, other):
            await _wait_for(
                lambda: _options(tab, "fabric_workspace") == {"ws-Fabrikam": "Fabrikam workspace"}
            )
            assert _value(tab, "fabric_account") == BOB.key
        assert accounts.added == 1  # one browser sign-in for both tabs
        assert [client.tenant for client in made["fabric"]] == ["Fabrikam", "Fabrikam"]

    _simulate(scenario)


def test_a_service_principal_ignores_the_account_list_and_starts_no_timers(sample, monkeypatch):
    for name in ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"):
        monkeypatch.setenv(name, "x")
    accounts = FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, active=ALICE.key)
    made = _account_clients(monkeypatch)
    monkeypatch.setattr(web_sources, "BUSY_POLL_SECONDS", 0.05)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_SECONDS", 0.1)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Contoso"])
        await _choose(user, "fabric_workspace", "ws-Contoso", until=lambda: _options(user, "fabric_report"))
        accounts.active = ALICE_GUEST.key  # another tab or `pbixtractor account use`
        # The service principal signs in, whatever the account list says: nothing polls it,
        # nothing reads it, nothing is reloaded
        assert not _account_timers(user)
        assert accounts.stamps == 0 and accounts.snapshots == 0
        assert _value(user, "fabric_workspace") == "ws-Contoso" and _options(user, "fabric_report")
        assert len(made["fabric"]) == 1
        assert _run_button(user).enabled

    _simulate(scenario)


def test_the_account_list_is_read_again_only_when_it_changed(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, active=ALICE.key)
    monkeypatch.setattr(web_sources, "BUSY_POLL_SECONDS", 0.05)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_SECONDS", 0.1)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        assert _account_timers(user)  # the poll (no service principal)
        reads, looks = accounts.snapshots, accounts.stamps
        await _wait_for(lambda: accounts.stamps >= looks + 3)  # it looks at the file every poll,
        assert accounts.snapshots == reads  # but an unchanged file is not read again
        accounts.active = ALICE_GUEST.key  # changed: `pbixtractor account use 2`
        await _wait_for(lambda: _value(user, "fabric_account") == ALICE_GUEST.key)
        assert accounts.snapshots == reads + 1
        looks = accounts.stamps
        await _wait_for(lambda: accounts.stamps >= looks + 3)
        assert accounts.snapshots == reads + 1

    _simulate(scenario)


def test_an_unreadable_account_list_changes_nothing_and_actions_show_the_error(
    sample, monkeypatch
):
    accounts = FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, BOB, active=ALICE.key)
    made = _account_clients(monkeypatch)
    monkeypatch.setattr(web_sources, "BUSY_POLL_SECONDS", 0.05)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_SECONDS", 0.1)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_MAX_SECONDS", 0.2)  # it backs off up to this
    logged: list[str] = []
    monkeypatch.setattr(web_sources.logger, "error", lambda message, *a, **k: logged.append(message))
    damaged = "The account list accounts.json is damaged (not a JSON object) - delete it and sign in again"

    async def scenario(user):
        notes = user.notify = Notifications()
        await user.open("/")
        await _choose(user, "source", "fabric")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Contoso"])
        await _choose(user, "fabric_workspace", "ws-Contoso", until=lambda: _options(user, "fabric_report"))
        accounts.unreadable = damaged
        accounts.active = ALICE_GUEST.key  # changed meanwhile, but it cannot be read
        reads = accounts.snapshots
        await _wait_for(lambda: accounts.snapshots >= reads + 3)  # tried again by the poll
        assert logged == [f"{damaged} (the web UI keeps the accounts it showed)"]  # logged once
        # No switch, nothing cleared
        assert _value(user, "fabric_account") == ALICE.key
        await user.should_see("Signed in as alice@contoso.com · Contoso")
        assert _value(user, "fabric_workspace") == "ws-Contoso" and _options(user, "fabric_report")
        assert len(made["fabric"]) == 1
        # Switching, adding and removing show the error and change nothing
        _element(user, "fabric_account").value = BOB.key
        switch_error = f"Could not switch the account: {damaged}"
        await _wait_for(lambda: switch_error in notes.messages)
        assert notes.types[switch_error] == "negative"
        assert _value(user, "fabric_account") == ALICE.key
        user.find(marker="fabric_account_add").click()
        await _wait_for(lambda: damaged in notes.messages)
        assert notes.types[damaged] == "negative"
        _remove_entry(user, "fabric", BOB)
        await _wait_for(lambda: notes.messages.count(damaged) == 2)
        assert notes.types[damaged] == "negative"
        assert accounts.added == 0 and not accounts.removed and accounts.active == ALICE_GUEST.key
        assert len(_options(user, "fabric_account")) == 3 and _value(user, "fabric_account") == ALICE.key
        assert len(logged) == 1
        # Readable again: the next poll follows it (the switch made meanwhile)
        accounts.unreadable = None
        await _wait_for(lambda: _value(user, "fabric_account") == ALICE_GUEST.key)
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Fabrikam"])

    _simulate(scenario)


def test_a_page_opened_while_the_account_list_cannot_be_read_still_works(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, active=ALICE.key)
    accounts.unreadable = "Cannot read the account list accounts.json: locked"
    monkeypatch.setattr(web_sources, "BUSY_POLL_SECONDS", 0.05)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_SECONDS", 0.1)

    async def scenario(user):
        await user.open("/")
        await user.should_see("Create documentation")
        await _choose(user, "source", "fabric")
        await user.should_see("Not signed in")
        accounts.unreadable = None  # it can be read again: the next poll shows the account
        await _wait_for(
            lambda: _element(user, "fabric_account_status").text
            == "Signed in as alice@contoso.com · Contoso"
        )

    _simulate(scenario)


def test_an_unreadable_account_list_backs_off_and_the_poll_never_sleeps(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, active=ALICE.key)
    monkeypatch.setattr(web_sources, "BUSY_POLL_SECONDS", 0.05)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_SECONDS", 0.1)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_MAX_SECONDS", 60.0)
    monkeypatch.setattr(web_sources.logger, "error", lambda *a, **k: None)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        await user.should_see("Signed in as alice@contoso.com · Contoso")
        accounts.unreadable = "Cannot read the account list accounts.json: locked"
        accounts.active = ALICE_GUEST.key
        reads, polls, waits = accounts.snapshots, accounts.polls, len(accounts.waits)
        await _wait_for(lambda: accounts.polls >= polls + 40, timeout=20)  # 2 s of polls
        tried = accounts.snapshots - reads
        # Without the backoff the poll would read every 0.1 s (about 20 times); doubling from
        # 0.1 s it reads after 0.2, 0.4, 0.8 s...
        assert 1 <= tried <= 5, tried
        assert accounts.waits[waits:] == [False] * tried  # one attempt each: never sleeping
        # A good read resets the backoff: a switch is followed again at once
        accounts.unreadable = None
        await _wait_for(lambda: _value(user, "fabric_account") == ALICE_GUEST.key, timeout=20)
        reads = accounts.snapshots
        accounts.active = ALICE.key
        await _wait_for(lambda: _value(user, "fabric_account") == ALICE.key, timeout=1.5)
        assert accounts.snapshots == reads + 1 and accounts.waits[-1] is False

    _simulate(scenario)


def test_a_change_the_stamp_cannot_see_is_read_within_the_full_read_time(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, active=ALICE.key)
    monkeypatch.setattr(web_sources, "BUSY_POLL_SECONDS", 0.05)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_SECONDS", 0.1)
    monkeypatch.setattr(web_sources, "ACCOUNT_FULL_READ_SECONDS", 0.6)
    looks = []

    def same_stamp(stamp):  # replaced, same inode, size and modification time
        looks.append(stamp)
        return stamp is None, "the same stamp"

    monkeypatch.setattr(azure_auth, "accounts_changed_since", same_stamp)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        await user.should_see("Signed in as alice@contoso.com · Contoso")
        accounts.active = ALICE_GUEST.key  # `pbixtractor account use 2`: the stamp says nothing
        await _wait_for(lambda: _value(user, "fabric_account") == ALICE_GUEST.key)
        # The polls in between only looked (the cheap check); a full read now and then
        assert looks.count("the same stamp") > looks.count(None)
        assert accounts.waits and all(wait is False for wait in accounts.waits)

    _simulate(scenario)


def test_an_unreadable_account_list_is_logged_once_for_all_tabs(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, active=ALICE.key)
    monkeypatch.setattr(web_sources, "BUSY_POLL_SECONDS", 0.05)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_SECONDS", 0.1)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_MAX_SECONDS", 0.2)
    logged: list[str] = []
    monkeypatch.setattr(web_sources.logger, "error", lambda message, *a, **k: logged.append(message))
    locked = "Cannot read the account list accounts.json: locked"

    async def scenario(user):
        await user.open("/")
        other = User(user.http_client)  # a second tab
        await other.open("/")
        for tab in (user, other):
            await _choose(tab, "source", "fabric")
            await tab.should_see("Signed in as alice@contoso.com · Contoso")
        accounts.unreadable = locked
        accounts.entries = [ALICE, ALICE_GUEST]  # changed: the tabs read it (and cannot)
        reads = accounts.snapshots
        await _wait_for(lambda: accounts.snapshots >= reads + 6)  # both tabs tried, again
        assert logged == [f"{locked} (the web UI keeps the accounts it showed)"]
        # A good read anywhere: the same error coming back later is logged again
        accounts.unreadable = None
        await _wait_for(lambda: web_sources._logged_read_error == "")
        accounts.unreadable = locked
        accounts.entries = [ALICE]
        await _wait_for(lambda: len(logged) == 2)
        reads = accounts.snapshots
        await _wait_for(lambda: accounts.snapshots >= reads + 4)
        assert len(logged) == 2

    _simulate(scenario)


def test_sign_in_and_load_reads_the_account_list_only_when_it_changed(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, active=ALICE.key)
    made = _account_clients(monkeypatch)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        await user.should_see("Signed in as alice@contoso.com · Contoso")
        reads, looks = accounts.snapshots, accounts.stamps
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Contoso"])
        assert accounts.stamps > looks  # it looked at the file,
        assert accounts.snapshots == reads  # but an unchanged one is not read again
        assert len(made["fabric"]) == 1

    _simulate(scenario)


def test_create_documentation_asks_the_account_state_for_a_pending_sign_in(sample, monkeypatch):
    FakeAccounts(monkeypatch, ALICE, active=ALICE.key)
    # This tab's own sign-in, as AccountState.pending() sees it - before anything is shown
    monkeypatch.setattr(web_sources.AccountState, "pending", lambda self: True)
    monkeypatch.setattr(web_sources.AccountState, "_set", lambda self, blocked, signing_in: None)
    monkeypatch.setattr(web_sources, "sign_in_pending", lambda: False)

    async def scenario(user):
        await user.open("/")
        assert not _run_button(user).enabled
        await user.should_see(marker="run_sign_in", content=web_sources.WAITING_FOR_SIGN_IN)
        user.find("Create documentation").trigger("click")
        await user.should_see(web_sources.FINISH_SIGN_IN)

    _simulate(scenario)


@pytest.mark.parametrize(
    "error",
    [ApiError("Cannot read the account list accounts.json: locked"), PermissionError(13, "Permission denied")],
    ids=["unreadable", "not-writable"],
)
def test_a_failed_switch_puts_the_choice_back_on_the_active_account(sample, monkeypatch, error):
    accounts = FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, active=ALICE.key)
    accounts.use_error = error
    made = _account_clients(monkeypatch)

    async def scenario(user):
        notes = user.notify = Notifications()
        await user.open("/")
        await _choose(user, "source", "fabric")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Contoso"])
        await _choose(user, "fabric_account", ALICE_GUEST.key)
        message = f"Could not switch the account: {error}"
        await _wait_for(lambda: message in notes.messages)
        assert notes.types[message] == "negative"
        # The select (in both panels) and the status are back on the active entry
        assert _value(user, "fabric_account") == ALICE.key
        assert _any_element(user, "devops_account").value == ALICE.key
        await user.should_see("Signed in as alice@contoso.com · Contoso")
        assert accounts.active == ALICE.key
        assert list(_options(user, "fabric_workspace")) == ["ws-Contoso"] and len(made["fabric"]) == 1

    _simulate(scenario)


def test_sign_in_and_load_during_a_run_does_not_apply_a_switch_made_elsewhere(sample, monkeypatch):
    accounts = FakeAccounts(monkeypatch, ALICE, ALICE_GUEST, active=ALICE.key)
    made = _account_clients(monkeypatch)
    monkeypatch.setattr(web_sources, "ACCOUNT_POLL_SECONDS", 0.2)

    async def scenario(user):
        await user.open("/")
        await _choose(user, "source", "fabric")
        user.find(marker="fabric_load").click()
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Contoso"])
        await web_ui._JOB_LOCK.acquire()  # a download or documentation run, in any tab
        try:
            accounts.active = ALICE_GUEST.key  # `pbixtractor account use 2` during the run
            reads = accounts.snapshots
            user.find(marker="fabric_load").click()
            await _wait_for(lambda: made["loads"] == ["Contoso", "Contoso"])
            await _wait_for(lambda: not _element(user, "fabric_workspace").props.get("loading"))
            # Loaded with the client it had; the account list was not even read
            assert accounts.snapshots == reads and len(made["fabric"]) == 1
            assert _value(user, "fabric_account") == ALICE.key
            assert list(_options(user, "fabric_workspace")) == ["ws-Contoso"]
        finally:
            web_ui._JOB_LOCK.release()
        # After the run the switch is followed
        await _wait_for(lambda: _value(user, "fabric_account") == ALICE_GUEST.key)
        await _wait_for(lambda: list(_options(user, "fabric_workspace")) == ["ws-Fabrikam"])

    _simulate(scenario)


def test_a_pending_sign_in_disables_create_documentation_in_every_tab(sample, monkeypatch):
    monkeypatch.setenv("AZURE_DEVOPS_PAT", "token")  # the DevOps panel shows no account control
    accounts = FakeAccounts(monkeypatch, ALICE, active=ALICE.key)
    monkeypatch.setattr(web_sources, "BUSY_POLL_SECONDS", 0.05)

    async def scenario(user):
        await user.open("/")  # local files, the default source
        other = User(user.http_client)  # a second tab, on Azure DevOps with the token
        await other.open("/")
        await _choose(other, "source", "devops")
        await other.should_see(web_sources.DEVOPS_PAT)
        tabs = (user, other)
        for tab in tabs:
            assert _run_button(tab).enabled and not _any_element(tab, "run_sign_in").visible
        accounts.signing.set()  # a browser sign-in: another tab's, or a run's token
        for tab in tabs:
            await _wait_for(lambda: not _run_button(tab).enabled)
            await tab.should_see(marker="run_sign_in", content=web_sources.WAITING_FOR_SIGN_IN)
        user.find("Create documentation").trigger("click")  # the safety net behind it
        await user.should_see(web_sources.FINISH_SIGN_IN)
        accounts.signing.clear()  # the sign-in ended
        for tab in tabs:
            await _wait_for(lambda: _run_button(tab).enabled)
            assert not _any_element(tab, "run_sign_in").visible

    _simulate(scenario)
