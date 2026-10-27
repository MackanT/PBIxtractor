"""Web UI panels for reports in a Fabric workspace or an Azure DevOps repository.

Each panel lets the user pick a report step by step (sign-in happens on the first call) and
downloads it as a local PBIP project when the documentation is created:

    panel = FabricPanel(on_choose=lambda name: ...)   # builds the UI in the current container
    panel.ready()                                     # a report is chosen
    report_folder, model_path = panel.fetch(progress) # blocking: call via run.io_bound

Or, scope "Whole workspaces" / "Whole repository" (panel.is_batch): "List models" shows every
semantic model found (PlanPicker); the ticked ones are documented (panel.selected_jobs(),
batch.run_batch).

Stand-alone, both panels start with the same account control (AccountState + AccountControl):
which account signs in, switching between the remembered entries, adding one through the
browser sign-in (optionally at a typed tenant), removing any one (each entry has its own
remove icon). A switch - here, in another tab or with `pbixtractor account` - makes both
panels drop their clients and reload what they had loaded with the new account. While a
sign-in is going on in the process (sign_in_pending()), every tab shows it and no switch or run
starts. An account list that cannot be read changes nothing: the tab keeps what it showed.
Embedded there is no control (a service principal from the environment signs in).

API calls block (and the first one may open a browser sign-in), so they run in threads.
"""

import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable, Optional

from nicegui import run, ui

from . import azure_auth, batch, devops, fabric
from .azure_auth import ApiError
from .pipeline import model_name
from .service_stats import ServiceModel
from .web_config import output_root, stored_choices

logger = logging.getLogger("pbixtractor")


@dataclass
class Fetched:
    """What a panel downloaded: the report(s) and their model."""

    report_folder: Path
    model_path: Path
    extra_reports: list[Path] = field(default_factory=list)  # model mode: the other reports
    skipped: list[str] = field(default_factory=list)  # reports that could not be downloaded
    # A published model fetched for a report bound to the service (statistics come from it)
    service_model: Optional[ServiceModel] = None

    @property
    def model_mode(self) -> bool:
        return bool(self.extra_reports) or bool(self.skipped)

    @property
    def name(self) -> str:
        """Output name: the model's in model mode, else the report's."""
        if self.model_mode:
            return model_name(self.model_path)
        return self.report_folder.name.removesuffix(".Report")


MODEL_MODE_HELP = (
    "Document every report on this semantic model together: pages become '<report> › <page>' "
    "and 'unused' then means unused by all of them."
)


def make_fabric_client():
    """The Fabric client (tests replace this)."""
    return fabric.FabricClient()


def make_devops_client(org: str):
    """The Azure DevOps client (tests replace this)."""
    return devops.DevOpsClient(org)


async def _call(function, *args, select: Optional[ui.select] = None):
    """Run a blocking API call in a thread; show errors as a notification (returns None)."""
    if select is not None:
        select.props("loading")
    try:
        return await run.io_bound(function, *args)
    except (ApiError, ValueError, OSError) as error:
        ui.notify(str(error), type="negative", multi_line=True, timeout=15000)
        return None
    except Exception as error:  # UI boundary: never let a click end without any message
        logger.exception(f"Unexpected error: {error}")
        ui.notify(f"Unexpected error: {error}", type="negative", multi_line=True, timeout=15000)
        return None
    finally:
        if select is not None:
            select.props(remove="loading")


def _set_options(select: ui.select, options: dict, value=None) -> None:
    select.set_options(options, value=value if value in options else None)


PLAN_COLUMNS = [
    {"name": "model", "label": "Semantic model", "field": "model", "align": "left", "sortable": True},
    {"name": "reports", "label": "Reports", "field": "reports", "align": "left",
     "style": "white-space: normal"},
    {"name": "where", "label": "Where", "field": "where", "align": "left", "sortable": True},
]


class PlanPicker:
    """
    The semantic models found (in workspaces / a repository), to tick the ones to document.
    Nothing is ticked at first: only what is chosen is documented.

        picker = PlanPicker(plan=lambda progress: batch.plan_fabric(...), check=lambda: "")
        picker.selected_jobs()   # the ticked models' BatchJobs
        picker.clear()           # the scope changed: list again
    """

    def __init__(
        self,
        plan: Callable[[Callable[[str], None]], list[batch.BatchJob]],
        check: Callable[[], str],
        marker: str,
    ):
        """
        Args:
            plan: Blocking: lists the models (batch.plan_fabric / batch.plan_devops)
            check: "" when the models can be listed, else what to choose first
            marker: Prefix of the elements' markers ("fabric" -> "fabric_plan_list", ...)
        """
        self._plan = plan
        self._check = check
        self._generation = 0  # +1 on clear(): a listing still on its way is then dropped
        self.jobs: list[batch.BatchJob] = []
        with ui.row().classes("w-full items-center no-wrap gap-2"):
            self.list_button = (
                ui.button("List models", icon="checklist", on_click=self.refresh)
                .props("outline no-caps")
                .mark(f"{marker}_plan_list")
            )
            self.status = ui.label("").classes("text-sm text-grey-7").mark(f"{marker}_plan_status")
        with ui.column().classes("w-full gap-1") as self.box:
            with ui.row().classes("w-full items-center no-wrap gap-2"):
                self.filter = (
                    ui.input(placeholder="Filter models and reports")
                    .props("dense clearable")
                    .classes("grow")
                    .mark(f"{marker}_plan_filter")
                )
                ui.button("Select all", on_click=self.select_all).props("flat dense no-caps").tooltip(
                    "Every model shown (matching the filter)"
                ).mark(f"{marker}_plan_all")
                ui.button("Select none", on_click=self.select_none).props("flat dense no-caps").mark(
                    f"{marker}_plan_none"
                )
            self.table = (
                ui.table(
                    columns=PLAN_COLUMNS,
                    rows=[],
                    row_key="id",
                    selection="multiple",
                    pagination={"rowsPerPage": 0},
                    on_select=lambda _: self._show_count(),
                )
                .props("dense flat bordered hide-bottom")
                .classes("w-full")
                .style("max-height: 24rem")
                .mark(f"{marker}_plan_table")
            )
            self.filter.bind_value_to(self.table, "filter")
            self.skipped = ui.expansion("").props("dense").classes("w-full text-sm").mark(f"{marker}_plan_skipped")
            with self.skipped:
                self.skipped_list = ui.column().classes("gap-0")
        self.box.visible = False

    async def refresh(self) -> None:
        """List the models (in a thread, with progress), then show them - none ticked."""
        problem = self._check()
        if problem:
            ui.notify(problem, type="warning")
            return
        messages: queue.Queue = queue.Queue()

        def show_progress() -> None:
            while not messages.empty():
                self.status.text = messages.get_nowait()

        timer = ui.timer(0.2, show_progress)
        self.list_button.props("loading")
        generation = self._generation
        jobs = None
        try:
            jobs = await run.io_bound(self._plan, messages.put)
        except (ApiError, ValueError, OSError) as error:
            ui.notify(f"Listing failed: {error}", type="negative", multi_line=True, timeout=15000)
        except Exception as error:  # UI boundary: never let a click end without any message
            logger.exception(f"Listing failed unexpectedly: {error}")
            ui.notify(f"Listing failed: {error}", type="negative", multi_line=True, timeout=15000)
        finally:
            timer.cancel()
            self.list_button.props(remove="loading")
        if jobs is None or generation != self._generation:  # failed, or cleared meanwhile
            self.status.text = ""
            return
        self.show(jobs)

    def show(self, jobs: list[batch.BatchJob]) -> None:
        self.jobs = jobs
        self.table.rows = [
            {
                "id": index,
                "model": job.model,
                "reports": ", ".join(job.reports) or "-",
                "where": job.source.partition(" · ")[2] or job.source,
            }
            for index, job in enumerate(jobs)
            if job.prepare and not job.skip_reason
        ]
        self.table.selected = []
        self.table.update()
        unusable = [job for job in jobs if job.skip_reason or not job.prepare]
        self.skipped.text = f"Cannot be documented ({len(unusable)})"
        self.skipped.visible = bool(unusable)
        self.skipped_list.clear()
        with self.skipped_list:
            for job in unusable:
                reports = f" ({', '.join(job.reports)})" if job.reports and job.reports != [job.model] else ""
                ui.label(f"{job.model}{reports}: {job.skip_reason or 'nothing to document'}").classes(
                    "text-sm text-grey-7"
                )
        self.box.visible = True
        self._show_count()

    def _show_count(self) -> None:
        total, chosen = len(self.table.rows), len(self.table.selected)
        self.status.text = (
            f"{chosen} of {total} model{'s' if total != 1 else ''} selected" if total else "No models found"
        )

    def select_all(self) -> None:
        text = (self.filter.value or "").strip().lower()
        chosen = {row["id"] for row in self.table.selected}
        shown = [
            row for row in self.table.rows
            if not text or text in f"{row['model']} {row['reports']} {row['where']}".lower()
        ]
        self.table.selected = list(self.table.selected) + [r for r in shown if r["id"] not in chosen]
        self._show_count()

    def select_none(self) -> None:
        self.table.selected = []
        self._show_count()

    def selected_jobs(self) -> list[batch.BatchJob]:
        return [self.jobs[row["id"]] for row in self.table.selected if row["id"] < len(self.jobs)]

    def clear(self) -> None:
        """The scope (or the account) changed: what was listed no longer applies."""
        self._generation += 1
        self.jobs = []
        self.table.rows = []
        self.table.selected = []
        self.table.update()
        self.box.visible = False
        self.status.text = ""


# ============================================================================
# The sign-in account (stand-alone only: embedded, a service principal signs in)
# ============================================================================

BUSY_POLL_SECONDS = 0.5  # how soon the controls notice a run or a sign-in starting or ending
ACCOUNT_POLL_SECONDS = 3.0  # how soon a tab follows a switch made elsewhere (tab, command line):
# it then looks at accounts.json (azure_auth.accounts_changed_since) and reads it only if changed
ACCOUNT_POLL_MAX_SECONDS = 60.0  # an account list that cannot be read: the poll backs off
# (doubling from ACCOUNT_POLL_SECONDS up to this) until a read works again
ACCOUNT_FULL_READ_SECONDS = 30.0  # the poll reads accounts.json anyway when its last full read
# is this old: a changed file whose stamp did not change (inode reused, same size and mtime)
WAIT_FOR_RUN = "Wait for the run to finish"
WAITING_FOR_SIGN_IN = "Waiting for sign-in in your browser…"
FINISH_SIGN_IN = "Finish the sign-in in your browser first"
UNENCRYPTED = "Tokens stored unencrypted, readable only by your user"
SERVICE_PRINCIPAL = "Using the service principal from the environment"
DEVOPS_PAT = "Using the personal access token from AZURE_DEVOPS_PAT"
REMOVE_ENTRY = "Remove this account from the list"
LIST_CHANGED = "The account list changed meanwhile - try again."

# Each entry of the account select has its own remove icon (Quasar's option slot). The click
# must not choose the entry (.stop), and reaches Python as the select's "remove_entry" event
# with the option's index and label (the browser knows the options by index).
_OPTION_SLOT = """
<q-item v-bind="props.itemProps">
  <q-item-section><q-item-label>{{ props.opt.label }}</q-item-label></q-item-section>
  <q-item-section side>
    <q-btn flat round dense size="sm" icon="person_remove" aria-label="%(tip)s"
      @click.stop.prevent="$root.$refs['r%(id)s'].$emit('remove_entry', {index: props.opt.value, label: props.opt.label})">
      <q-tooltip>%(tip)s</q-tooltip>
    </q-btn>
  </q-item-section>
</q-item>
"""


# The last "cannot read the account list" error logged (any tab): logged once per process per
# distinct message, again after a good read anywhere
_logged_read_error = ""
_logged_read_error_lock = threading.Lock()


def _log_read_error(error: str) -> None:
    """Log an account list that cannot be read, unless this message is the one last logged."""
    global _logged_read_error
    with _logged_read_error_lock:
        if error == _logged_read_error:
            return
        _logged_read_error = error
    logger.error(f"{error} (the web UI keeps the accounts it showed)")


def _read_error_cleared() -> None:
    """A good read: the next unreadable account list is logged again, whatever its message."""
    global _logged_read_error
    with _logged_read_error_lock:
        _logged_read_error = ""


def sign_in_pending() -> bool:
    """
    A sign-in is going on in this process (azure_auth.sign_in_in_progress): a tab adding an
    account (the browser, listing its tenants, writing the entries), the first sign-in of
    "Sign in / load", or a token that needs the browser - a run's too. Every tab shows it; no
    switch and no documentation run may start meanwhile.
    """
    return azure_auth.sign_in_in_progress()


class AccountState:
    """
    The active sign-in account as one tab shows it, shared by the account controls of the
    Fabric and the DevOps panel (one account for both, for the whole app). A switch - made
    here, in another tab or with `pbixtractor account` - tells every panel that listens, which
    drops its client and reloads with the new account. With a service principal in the
    environment the accounts are not used: nothing is read or followed then (no timers).

        accounts = AccountState(busy=run_lock.locked)  # no switching while busy() is True
        FabricPanel(on_choose, accounts)               # shows a control, follows switches
        accounts.on_sign_in(lambda pending: ...)       # a sign-in starts or ends (any tab)
        await accounts.ensure_signed_in()              # False: the sign-in failed (shown)

    The poll runs on the event loop: it looks at accounts.json cheaply
    (azure_auth.accounts_changed_since) every ACCOUNT_POLL_SECONDS, reads it only when it
    changed - or anyway once its last full read is ACCOUNT_FULL_READ_SECONDS old - and reads
    with one attempt, never sleeping (account_snapshot(wait=False)). An account list that
    cannot be read (ApiError) changes nothing: the tab keeps what it showed, the error is
    logged once per process per message, and the poll backs off (doubling up to
    ACCOUNT_POLL_MAX_SECONDS, back to normal after a good read); switching, adding and
    removing read with retries, show the error and change nothing.
    """

    def __init__(self, busy: Callable[[], bool] = lambda: False):
        """
        Args:
            busy: True while a download or documentation run is in progress (in any tab)
        """
        self._busy = busy
        self._controls: list["AccountControl"] = []
        self._listeners: list[Callable[[], None]] = []
        self._sign_in_listeners: list[Callable[[bool], None]] = []
        self._own_sign_ins = 0  # this tab's: shown from the click on, before its thread starts
        self._stamp: Optional[tuple] = None  # accounts.json as last read (None: read it next)
        self._read_error = ""  # the last error reading it, "" after a good read
        self._read_at = time.monotonic()
        self._full_read_at = time.monotonic()  # the last read whatever the stamp said
        self._poll_seconds = ACCOUNT_POLL_SECONDS  # longer while the list cannot be read
        self.entries: list[azure_auth.Account] = []
        self.active: Optional[str] = None  # the active entry's key
        self.encrypted: Optional[bool] = None  # asked once there are tokens to keep
        self.signing_in = False  # as last shown (a sign-in anywhere in the process)
        self.blocked = False  # as last shown (a run in any tab)
        self.service_principal = azure_auth.service_principal_configured()
        if self.service_principal:  # it signs in, whatever accounts.json says
            return
        self.signing_in = self.pending()
        self.blocked = bool(busy())
        self._sync(wait=False)  # on the event loop: an unreadable list is retried by the poll
        ui.timer(BUSY_POLL_SECONDS, self._poll)

    @property
    def account(self) -> Optional[azure_auth.Account]:
        """The active entry (None: not signed in)."""
        return next((a for a in self.entries if a.key == self.active), None)

    def attach(self, control: "AccountControl") -> None:
        self._controls.append(control)
        control.show()

    def on_switch(self, listener: Callable[[], None]) -> None:
        """Call `listener` after the active account changed (it clears and reloads its panel)."""
        self._listeners.append(listener)

    def on_sign_in(self, listener: Callable[[bool], None]) -> None:
        """Call `listener(pending)` when a sign-in starts or ends - in this tab, another one or
        a run (e.g. "Create documentation" waits for it). Never with a service principal."""
        self._sign_in_listeners.append(listener)

    def _show(self) -> None:
        for control in self._controls:
            control.show()

    def pending(self) -> bool:
        """A sign-in is going on: this tab's (from the click on) or any other of the process
        (sign_in_pending()). No switch and no run may start meanwhile."""
        return self._own_sign_ins > 0 or sign_in_pending()

    def _set(self, blocked: bool, signing_in: bool) -> None:
        """Show a run or a sign-in starting or ending (the controls, the sign-in listeners)."""
        if (blocked, signing_in) == (self.blocked, self.signing_in):
            return
        sign_in_changed = signing_in != self.signing_in
        self.blocked, self.signing_in = blocked, signing_in
        self._show()
        if sign_in_changed:
            for listener in self._sign_in_listeners:
                listener(signing_in)

    def _sync(self, force: bool = True, wait: bool = True) -> bool:
        """
        Re-read the account list; show a change and, after a switch, tell the panels. When it
        cannot be read everything stays as it was (logged once per process per message; read
        again at a later poll, which backs off meanwhile).

        Args:
            force: Read it even if accounts.json looks unchanged (False: the cheap check)
            wait: Retry a file that cannot be read for about half a second (False: one
                attempt, no sleeping - the poll, on the event loop)

        Returns:
            Whether anything changed
        """
        if self.service_principal:  # it signs in, whatever accounts.json says
            return False
        now = self._read_at = time.monotonic()
        if force:
            self._full_read_at = now
        changed, stamp = azure_auth.accounts_changed_since(None if force else self._stamp)
        if not changed:
            return False
        try:
            # One read: never a mix of two file versions
            entries, active = azure_auth.account_snapshot(wait=wait)
        except ApiError as error:
            self._stamp = None  # read it again next time
            self._read_error = str(error)
            self._poll_seconds = min(self._poll_seconds * 2, ACCOUNT_POLL_MAX_SECONDS)
            _log_read_error(str(error))
            return False
        self._stamp, self._read_error = stamp, ""
        self._poll_seconds = ACCOUNT_POLL_SECONDS
        _read_error_cleared()
        key = active.key if active else None
        if entries == self.entries and key == self.active:
            return False
        switched = key != self.active
        self.entries, self.active = entries, key
        self._show()
        if entries and self.encrypted is None:
            ui.timer(0.01, self._check_encryption, once=True)
        if switched:
            for listener in self._listeners:
                listener()
        return True

    def _poll(self) -> None:
        blocked, signing_in = bool(self._busy()), self.pending()
        ended = self.signing_in and not signing_in  # a sign-in (maybe another tab's) is over
        self._set(blocked, signing_in)
        # A switch made elsewhere waits for a run or a sign-in to end: the lists are the old
        # account's until then
        now = time.monotonic()
        if not blocked and not signing_in and (ended or now - self._read_at >= self._poll_seconds):
            # Reads accounts.json only when it changed - or when the last full read is old (a
            # change the stamp cannot see) - with one attempt: the poll never sleeps
            full = now - self._full_read_at >= ACCOUNT_FULL_READ_SECONDS
            self._sync(force=full, wait=False)

    async def _check_encryption(self) -> None:
        if self.encrypted is not None:
            return
        try:
            # The first call may ask the OS keyring: not on the event loop
            self.encrypted = await run.io_bound(azure_auth.token_cache_encrypted)
        except Exception as error:  # the token cache cannot be opened: say nothing about it
            logger.debug(f"Token cache: {error}")
            return
        self._show()

    def _refuse(self) -> bool:
        """No switching while a run or a sign-in is going (shown; the controls are reset)."""
        refused = ""
        if self._busy():
            refused = f"{WAIT_FOR_RUN}."
        elif self.pending():
            refused = WAITING_FOR_SIGN_IN
        if refused:
            ui.notify(refused, type="warning")
            self._show()
        return bool(refused)

    async def _sign_in(self, function: Callable, *args, **kwargs) -> bool:
        """Run a blocking azure_auth sign-in in a thread; every tab shows it meanwhile
        (azure_auth.sign_in_in_progress; this tab from the click on).
        Returns False when it failed, was cancelled or timed out (the error is shown)."""
        self._own_sign_ins += 1
        self._set(bool(self._busy()), True)
        try:
            return await run.io_bound(function, *args, **kwargs) is not None
        except (ApiError, ValueError, OSError) as error:
            ui.notify(str(error), type="negative", multi_line=True, timeout=15000)
            return False
        except Exception as error:  # UI boundary: never let a click end without any message
            logger.exception(f"Sign-in failed unexpectedly: {error}")
            ui.notify(f"Sign-in failed: {error}", type="negative", multi_line=True, timeout=15000)
            return False
        finally:
            self._own_sign_ins -= 1
            self._set(bool(self._busy()), self.pending())
            if not self._sync():
                self._show()

    async def ensure_signed_in(self) -> bool:
        """
        Before a panel's first call: with no active account (and no service principal) one
        browser sign-in whose home tenant becomes active. Tabs asking at once share it
        (azure_auth.ensure_account: one sign-in, the others wait and use its account). During
        a run or a sign-in the account list is not re-read: a switch made elsewhere is applied
        once it is over (as the poll does), never in the middle of a run.

        Returns:
            False when the sign-in failed or was cancelled, or a run is going on and there is
            no account (shown)
        """
        if self.service_principal:
            return True
        if not self._busy() and not self.pending():
            self._sync(force=False)  # re-read only when accounts.json changed
        if self.active is not None:
            return True
        if self._busy():  # signing in makes an account active: not during a run
            ui.notify(f"{WAIT_FOR_RUN}.", type="warning")
            return False
        return await self._sign_in(azure_auth.ensure_account)

    async def add(self, tenant: Optional[str] = None) -> bool:
        """
        Sign in through Microsoft's sign-in page (with its account picker), in a thread: the
        user's tenants become entries, the home tenant (or `tenant`) the active one. When the
        tenants cannot be listed, a warning says so (only the tenant signed in to was added).

        Args:
            tenant: Tenant id or domain to sign in to and make active (default: the home tenant)

        Returns:
            False when the sign-in failed, was cancelled or timed out, or the account list
            cannot be read (the error is shown)
        """
        if self._refuse():
            return False
        warnings: list[str] = []  # on_warning runs in the sign-in thread: shown here afterwards
        added = await self._sign_in(
            azure_auth.add_account, (tenant or "").strip() or None, on_warning=warnings.append
        )
        for text in warnings:
            ui.notify(text, type="warning", multi_line=True, timeout=30000)
        return added

    def choose(self, key: Optional[str]) -> None:
        """Make another entry the active one (a select's choice). When that fails, the select
        and the status go back to the entry that is active."""
        if not key or key == self.active or self._refuse():
            return
        try:
            azure_auth.use_account(key)
        except ValueError as error:  # removed meanwhile (another tab, the command line)
            ui.notify(str(error), type="warning")
        except (ApiError, OSError) as error:  # the account list cannot be read or written
            ui.notify(
                f"Could not switch the account: {error}", type="negative", multi_line=True,
                timeout=15000,
            )
        if not self._sync():
            self._show()

    async def remove(self, key: Optional[str]) -> None:
        """Forget an entry (any one: it is not chosen first). When it was the active one, the
        next one is active and the panels reload as after a switch; else they keep their lists."""
        if not key or self._refuse():
            return
        removed = next((a.label for a in self.entries if a.key == key), key)
        failed = ""  # an ApiError: judged once the list has been read again
        try:
            # The user's last entry also deletes their tokens from the token cache
            await run.io_bound(azure_auth.remove_account, key)
            ui.notify(f"Removed {removed}")
        except ValueError as error:  # removed meanwhile
            ui.notify(str(error), type="warning")
        except ApiError as error:
            failed = str(error)
        except Exception as error:  # UI boundary
            logger.exception(f"Removing {removed} failed: {error}")
            ui.notify(f"Removing {removed} failed: {error}", type="negative", multi_line=True)
        if not self._sync():
            self._show()
        if failed:
            # Gone from the list: only its tokens could not be deleted. Still listed, or the
            # list cannot be read: nothing was removed
            gone = not self._read_error and all(a.key != key for a in self.entries)
            ui.notify(
                failed, type="warning" if gone else "negative", multi_line=True, timeout=30000
            )


class AccountControl:
    """Which account signs in, and switching it: one at the top of each panel, all showing
    one AccountState."""

    def __init__(self, state: AccountState, marker: str):
        """
        Args:
            state: The tab's account state (shared by the panels)
            marker: Prefix of the elements' markers ("fabric" -> "fabric_account", ...)
        """
        self._state = state
        self.select: Optional[ui.select] = None
        with ui.column().classes("w-full gap-1"):
            if state.service_principal:
                with ui.row().classes("items-center no-wrap gap-2"):
                    ui.icon("vpn_key", color="primary")
                    ui.label(SERVICE_PRINCIPAL).classes("text-sm").mark(f"{marker}_account_status")
                return
            with ui.row().classes("w-full items-center no-wrap gap-2"):
                ui.icon("account_circle", color="primary")
                self.status = ui.label("").classes("text-sm").mark(f"{marker}_account_status")
            with ui.row().classes("w-full items-center no-wrap gap-2"):
                self.select = (
                    ui.select(
                        {}, label="Account", with_input=True, on_change=lambda e: state.choose(e.value)
                    )
                    .props("dense")
                    .classes("grow")
                    .tooltip("One account for Fabric and Azure DevOps, in every tab")
                    .mark(f"{marker}_account")
                )
                self.select.add_slot("option", _OPTION_SLOT % {"id": self.select.id, "tip": REMOVE_ENTRY})
                self.select.on("remove_entry", self._remove_entry)
                self.tenant = (
                    ui.input("Tenant (id or domain)", placeholder="optional")
                    .props("dense clearable")
                    .classes("w-48")
                    .tooltip(
                        "Add account signs in at this tenant and makes it active (default: the "
                        "user's home tenant) - for a tenant that is not listed"
                    )
                    .mark(f"{marker}_account_tenant")
                )
                self.add_button = (
                    ui.button("Add account", icon="person_add", on_click=self._add)
                    .props("flat no-caps")
                    .tooltip("Sign in in your browser: the user's tenants are added")
                    .mark(f"{marker}_account_add")
                )
            self.hint = ui.label("").classes("text-sm text-grey-7").mark(f"{marker}_account_hint")
            self.note = (
                ui.label(UNENCRYPTED).classes("text-caption text-grey-7").mark(f"{marker}_account_note")
            )
        state.attach(self)

    async def _add(self) -> None:
        if await self._state.add(self.tenant.value):
            self.tenant.value = ""  # a one-off: the next "Add account" asks for the home tenant

    async def _remove_entry(self, event) -> None:
        """An entry's remove icon: the browser names the option by its index and label."""
        args = event.args if isinstance(event.args, dict) else {}
        index, label = args.get("index"), args.get("label")
        options = list(self.select.options.items())
        if isinstance(index, int) and 0 <= index < len(options) and options[index][1] == label:
            key = options[index][0]
        else:  # the list changed while the menu was open: only an unambiguous label
            keys = [k for k, text in options if text == label]
            key = keys[0] if len(keys) == 1 else None
        if key is None:
            ui.notify(LIST_CHANGED, type="warning")
            return
        await self._state.remove(key)

    def show(self) -> None:
        """Show the state: the active entry, the choices, and why they are disabled."""
        if self.select is None:  # a service principal: nothing to choose
            return
        state = self._state
        account = state.account
        self.status.text = f"Signed in as {account.label}" if account else "Not signed in"
        _set_options(self.select, {a.key: a.label for a in state.entries}, state.active)
        hint = WAITING_FOR_SIGN_IN if state.signing_in else WAIT_FOR_RUN if state.blocked else ""
        self.hint.text = hint
        self.hint.visible = bool(hint)
        locked = state.signing_in or state.blocked
        # A disabled select cannot open: its entries' remove icons are disabled with it
        self.select.set_enabled(not locked and bool(state.entries))
        if locked and self.select.is_showing_popup:
            self.select.run_method("hidePopup")
        self.tenant.set_enabled(not locked)
        self.add_button.set_enabled(not locked)
        self.note.visible = bool(state.entries) and state.encrypted is False


def _drop_old_account(
    panel, clear: Callable[[], None], reload: Callable[[], Awaitable[None]]
) -> None:
    """
    A panel (FabricPanel / DevOpsPanel) after an account switch: answers still on their way for
    the old account are dropped (its generation), its client goes with that account, `clear`
    empties what it loaded and the model list is cleared; then `reload` loads the panel's first
    list again with the new account - only if it had been loaded and an entry is active (no
    sign-in on its own).
    """
    reload_now = panel._loaded and panel._accounts.active is not None
    panel._generation += 1
    panel._client = None
    panel._loaded = False
    clear()
    panel.picker.clear()
    if reload_now:
        with panel._slot:
            ui.timer(0.01, reload, once=True)


class FabricPanel:
    """Workspace -> report; the report's semantic model is downloaded with it. Or whole
    workspaces: the models used by their reports are listed, the ticked ones documented
    (one documentation each, batch.py)."""

    def __init__(self, on_choose: Callable[[str], None], accounts: Optional[AccountState] = None):
        """
        Args:
            on_choose: Called with the chosen report's name
            accounts: The tab's account state: the panel shows its control and follows its
                switches (None, embedded: no control, a service principal signs in)
        """
        self._on_choose = on_choose
        self._accounts = accounts
        self._client = None
        self._workspaces: dict[str, str] = {}
        self._generation = 0  # +1 on an account switch: answers for the old account are dropped
        self._loaded = False  # the workspaces were loaded (a switch reloads them)
        self._slot = ui.context.slot
        if accounts is not None:
            AccountControl(accounts, "fabric")
            accounts.on_switch(self._account_switched)
        ui.label(
            "Downloads the report and its semantic model with the Fabric REST API. Needs "
            "Contributor (or higher) access; reports with an encrypting sensitivity label cannot "
            "be downloaded - use Azure DevOps for those."
        ).classes("text-caption text-grey-7")
        self.scope = (
            ui.toggle({"report": "One report", "workspace": "Whole workspaces"}, value="report")
            .props("dense unelevated")
            .mark("fabric_scope")
        )
        with ui.row().classes("w-full items-center no-wrap gap-2"):
            self.workspace = (
                ui.select({}, label="Workspace", with_input=True, on_change=self._workspace_changed)
                .classes("grow")
                .bind_visibility_from(self.scope, "value", value="report")
                .mark("fabric_workspace")
            )
            self.workspaces = (
                ui.select({}, label="Workspaces", multiple=True, with_input=True, value=[])
                .props("use-chips")
                .classes("grow")
                .bind_visibility_from(self.scope, "value", value="workspace")
                .mark("fabric_workspaces")
            )
            ui.button("Sign in / load", icon="login", on_click=self.load_workspaces).props(
                "flat"
            ).mark("fabric_load")
        with ui.column().classes("w-full gap-2") as one_report:
            self.report = (
                ui.select({}, label="Report", with_input=True, on_change=self._report_changed)
                .classes("w-full")
                .mark("fabric_report")
            )
            with ui.row().classes("items-center gap-4"):
                self.all_reports = (
                    ui.switch("All reports on its semantic model").tooltip(MODEL_MODE_HELP).mark(
                        "fabric_all_reports"
                    )
                )
                self.all_workspaces = (
                    ui.switch("Search all my workspaces (slower)")
                    .tooltip("Default: only the report's and the model's workspace")
                    .bind_visibility_from(self.all_reports, "value")
                    .mark("fabric_all_workspaces")
                )
        one_report.bind_visibility_from(self.scope, "value", value="report")
        with ui.column().classes("w-full gap-1") as whole:
            ui.label(
                "List the semantic models used by the reports in these workspaces, then tick the "
                "ones to document - one documentation per model, with all its reports, added to "
                "the catalog."
            ).classes("text-caption text-grey-7")
            self.batch_all_workspaces = (
                ui.switch("Also find their reports in my other workspaces (slower)")
                .tooltip(
                    "A model's reports in workspaces you did not choose are documented with it "
                    "too, so 'unused' is complete"
                )
                .mark("fabric_batch_all_workspaces")
            )
            self.picker = PlanPicker(
                self.plan,
                lambda: "" if self.workspaces.value else "Choose one or more workspaces first (Sign in / load).",
                "fabric",
            )
        whole.bind_visibility_from(self.scope, "value", value="workspace")
        self.workspaces.on_value_change(lambda _: self.picker.clear())
        self.batch_all_workspaces.on_value_change(lambda _: self.picker.clear())

    @property
    def is_batch(self) -> bool:
        """Whole workspaces: selected_jobs() + batch.run_batch() instead of fetch()."""
        return self.scope.value == "workspace"

    def selected_jobs(self) -> list[batch.BatchJob]:
        """The models ticked in the list (whole workspaces)."""
        return self.picker.selected_jobs()

    def _get_client(self):
        if self._client is None:
            self._client = make_fabric_client()
        return self._client

    async def load_workspaces(self) -> None:
        # No account yet: the browser sign-in first (its home tenant becomes active)
        if self._accounts is not None and not await self._accounts.ensure_signed_in():
            return
        generation = self._generation
        items = await _call(lambda: self._get_client().workspaces(), select=self.workspace)
        if items is None or generation != self._generation:  # failed, or the account changed
            return
        self._loaded = True
        self._workspaces = {
            w["id"]: w["displayName"] for w in sorted(items, key=lambda w: w["displayName"].lower())
        }
        _set_options(self.workspace, self._workspaces, stored_choices().get("fabric_workspace"))
        self.workspaces.set_options(
            self._workspaces, value=[w for w in (self.workspaces.value or []) if w in self._workspaces]
        )
        if not items:
            ui.notify("No workspaces found for this account.", type="warning")

    async def _workspace_changed(self) -> None:
        _set_options(self.report, {})
        if not self.workspace.value:
            return
        workspace, generation = self.workspace.value, self._generation
        stored_choices()["fabric_workspace"] = workspace
        items = await _call(self._get_client().reports, workspace, select=self.report)
        # Not changed meanwhile (the workspace, or the account)
        if items is not None and self.workspace.value == workspace and generation == self._generation:
            _set_options(
                self.report,
                {r["id"]: r["displayName"] for r in sorted(items, key=lambda r: r["displayName"].lower())},
            )

    def _account_switched(self) -> None:
        """Another account is active: drop the client and everything the old account loaded,
        then reload the workspaces if they had been loaded."""
        _drop_old_account(self, self._clear_lists, self.load_workspaces)

    def _clear_lists(self) -> None:
        """Empty what the old account loaded (workspaces, reports, the model-mode switches)."""
        self._workspaces = {}
        _set_options(self.workspace, {})
        self.workspaces.set_options({}, value=[])
        _set_options(self.report, {})
        self.all_reports.value = False
        self.all_workspaces.value = False

    def _report_changed(self) -> None:
        if self.report.value:
            self._on_choose(self.report.options[self.report.value])

    def ready(self) -> bool:
        if self.is_batch:
            return bool(self.selected_jobs())
        return bool(self.workspace.value and self.report.value)

    def plan(self, progress: Callable[[str], None]) -> list[batch.BatchJob]:
        """Blocking: list the chosen workspaces' reports and models, one job per model."""
        return batch.plan_fabric(
            self._get_client(),
            list(self.workspaces.value),
            self.batch_all_workspaces.value,
            output_root(),
            progress,
        )

    def fetch(self, progress: Callable[[str], None]) -> Fetched:
        workspace = self._workspaces.get(self.workspace.value, self.workspace.value)
        fetched = fabric.fetch_report(
            self._get_client(),
            self.workspace.value,
            self.report.value,
            output_root() / "_fabric" / fabric.safe_name(workspace),
            progress=progress,
            all_reports=self.all_reports.value,
            all_workspaces=self.all_workspaces.value,
        )
        return Fetched(
            fetched.report_folder, fetched.model_path, fetched.report_folders[1:], fetched.skipped
        )


class DevOpsPanel:
    """Organisation -> project -> repository -> branch -> report -> version (commit). Or the
    whole repository (optionally one folder): its models are listed, the ticked ones
    documented (one documentation each, batch.py)."""

    def __init__(self, on_choose: Callable[[str], None], accounts: Optional[AccountState] = None):
        """
        Args:
            on_choose: Called with the chosen report's name
            accounts: The tab's account state: the panel shows its control and follows its
                switches (None, embedded: no control, a service principal signs in). With
                AZURE_DEVOPS_PAT set the token is used instead, and the panel says so.
        """
        self._on_choose = on_choose
        self._accounts: Optional[AccountState] = None  # None: no account (embedded, or a PAT)
        self._client = None
        self._default_branches: dict[str, str] = {}
        self._generation = 0  # +1 on an account switch: answers for the old account are dropped
        self._loaded = False  # the projects were loaded (a switch reloads them)
        self._slot = ui.context.slot
        if accounts is not None and os.environ.get("AZURE_DEVOPS_PAT"):
            with ui.row().classes("items-center no-wrap gap-2"):
                ui.icon("key", color="primary")
                ui.label(DEVOPS_PAT).classes("text-sm").mark("devops_account_status")
        elif accounts is not None:
            self._accounts = accounts
            AccountControl(accounts, "devops")
            accounts.on_switch(self._account_switched)
        ui.label(
            "Reads a PBIP project (report + the semantic model it references) from a git "
            "repository, at the latest version of a branch or at an older commit. Same sign-in "
            "as Fabric, or a personal access token in AZURE_DEVOPS_PAT."
        ).classes("text-caption text-grey-7")
        self.scope = (
            ui.toggle({"report": "One report", "repository": "Whole repository"}, value="report")
            .props("dense unelevated")
            .mark("devops_scope")
        )
        with ui.row().classes("w-full items-center no-wrap gap-2"):
            self.org = (
                ui.input(
                    "Organisation",
                    placeholder="myorg or https://dev.azure.com/myorg",
                    value=stored_choices().get("devops_org", ""),
                )
                .classes("grow")
                .mark("devops_org")
            )
            ui.button("Sign in / load", icon="login", on_click=self.load_projects).props(
                "flat"
            ).mark("devops_load")
        with ui.row().classes("w-full no-wrap gap-2"):
            self.project = (
                ui.select({}, label="Project", with_input=True, on_change=self._project_changed)
                .classes("w-1/2")
                .mark("devops_project")
            )
            self.repo = (
                ui.select({}, label="Repository", with_input=True, on_change=self._repo_changed)
                .classes("w-1/2")
                .mark("devops_repo")
            )
        with ui.row().classes("w-full no-wrap gap-2"):
            self.branch = (
                ui.select({}, label="Branch", with_input=True, on_change=self._branch_changed)
                .classes("w-1/3")
                .mark("devops_branch")
            )
            self.report = (
                ui.select({}, label="Report", with_input=True, on_change=self._report_changed)
                .classes("w-2/3")
                .bind_visibility_from(self.scope, "value", value="report")
                .mark("devops_report")
            )
            self.folder = (
                ui.input("Only this folder (optional)", placeholder="/Reports/Finance")
                .classes("w-2/3")
                .bind_visibility_from(self.scope, "value", value="repository")
                .mark("devops_folder")
            )
        with ui.column().classes("w-full gap-2") as one_report:
            self.version = (
                ui.select({"": "Latest on the branch"}, label="Version", value="")
                .classes("w-full")
                .mark("devops_version")
            )
            self.all_reports = (
                ui.switch("All reports on its semantic model")
                .tooltip(MODEL_MODE_HELP + " (reports in this repository at the chosen version)")
                .mark("devops_all_reports")
            )
        one_report.bind_visibility_from(self.scope, "value", value="report")
        with ui.column().classes("w-full gap-1") as whole:
            ui.label(
                "List the semantic models used by the reports in this repository (latest on the "
                "branch), then tick the ones to document - one documentation per model, with all "
                "its reports, added to the catalog."
            ).classes("text-caption text-grey-7")
            self.picker = PlanPicker(
                self.plan,
                lambda: "" if self.project.value and self.repo.value and self.branch.value
                else "Choose the project, repository and branch first (Sign in / load).",
                "devops",
            )
        whole.bind_visibility_from(self.scope, "value", value="repository")
        for element in (self.project, self.repo, self.branch, self.folder):
            element.on_value_change(lambda _: self.picker.clear())

    @property
    def is_batch(self) -> bool:
        """Whole repository: selected_jobs() + batch.run_batch() instead of fetch()."""
        return self.scope.value == "repository"

    def selected_jobs(self) -> list[batch.BatchJob]:
        """The models ticked in the list (whole repository)."""
        return self.picker.selected_jobs()

    def plan(self, progress: Callable[[str], None]) -> list[batch.BatchJob]:
        """Blocking: list the repository's reports (and their models), one job per model."""
        return batch.plan_devops(
            self._get_client(),
            self.project.value,
            self.repo.value,
            self.branch.value,
            "branch",
            (self.folder.value or "").strip(),
            output_root(),
            make_fabric_client,  # reports bound to a published model
            progress,
        )

    def _get_client(self):
        org = devops.normalize_org(self.org.value or "")
        if self._client is None or self._client.org != org:
            self._client = make_devops_client(org)
        return self._client

    async def load_projects(self) -> None:
        if not (self.org.value or "").strip():
            ui.notify("Enter the organisation first.", type="warning")
            return
        # No account yet: the browser sign-in first (its home tenant becomes active)
        if self._accounts is not None and not await self._accounts.ensure_signed_in():
            return
        generation = self._generation
        names = await _call(lambda: self._get_client().projects(), select=self.project)
        if names is None or generation != self._generation:  # failed, or the account changed
            return
        self._loaded = True
        stored_choices()["devops_org"] = self.org.value.strip()
        _set_options(self.project, {n: n for n in names}, stored_choices().get("devops_project"))

    async def _project_changed(self) -> None:
        for select in (self.repo, self.branch, self.report):
            _set_options(select, {})
        if not self.project.value:
            return
        asked = self._selection(1)
        stored_choices()["devops_project"] = self.project.value
        repos = await _call(self._get_client().repositories, self.project.value, select=self.repo)
        if repos is not None and self._selection(1) == asked:  # not changed meanwhile
            self._default_branches = {r["name"]: r["defaultBranch"] for r in repos}
            _set_options(self.repo, {r["name"]: r["name"] for r in repos}, stored_choices().get("devops_repo"))

    async def _repo_changed(self) -> None:
        _set_options(self.branch, {})
        _set_options(self.report, {})
        if not self.repo.value:
            return
        asked = self._selection(2)
        stored_choices()["devops_repo"] = self.repo.value
        names = await _call(self._get_client().branches, self.project.value, self.repo.value, select=self.branch)
        if names and self._selection(2) == asked:
            default = self._default_branches.get(self.repo.value)
            _set_options(self.branch, {n: n for n in names}, default if default in names else names[0])

    async def _branch_changed(self) -> None:
        _set_options(self.report, {})
        if not self.branch.value:
            return
        asked = self._selection(3)
        paths = await _call(
            self._get_client().find_reports,
            self.project.value,
            self.repo.value,
            self.branch.value,
            select=self.report,
        )
        if paths is not None and self._selection(3) == asked:
            _set_options(self.report, {p: p.lstrip("/") for p in paths})
            if not paths:
                ui.notify("No PBIP reports (*.Report/definition.pbir) on this branch.", type="warning")

    async def _report_changed(self) -> None:
        self.version.set_options({"": f"Latest on {self.branch.value}"}, value="")
        if not self.report.value:
            return
        self._on_choose(self.report.value.rsplit("/", 1)[-1].removesuffix(".Report"))
        asked = self._selection(4)
        commits = await _call(
            self._get_client().commits,
            self.project.value,
            self.repo.value,
            self.report.value,
            self.branch.value,
            select=self.version,
        )
        if commits and self._selection(4) == asked:
            options = {"": f"Latest on {self.branch.value}"}
            options.update(
                {c["id"]: f"{c['short']} · {c['date']} · {c['author']}: {c['comment']}" for c in commits}
            )
            self.version.set_options(options, value="")

    def _selection(self, depth: int) -> tuple:
        """The account and the first `depth` choices (project, repo, branch, report): a slow
        answer is only applied if they did not change while it was on its way."""
        choices = (self.project.value, self.repo.value, self.branch.value, self.report.value)
        return (self._generation, *choices[:depth])

    def _account_switched(self) -> None:
        """Another account is active: drop the client and everything the old account loaded,
        then reload the projects if they had been loaded."""
        _drop_old_account(self, self._clear_lists, self.load_projects)

    def _clear_lists(self) -> None:
        """Empty what the old account loaded (projects, repositories, branches, reports,
        versions)."""
        self._default_branches = {}
        for select in (self.project, self.repo, self.branch, self.report):
            _set_options(select, {})
        self.version.set_options({"": "Latest on the branch"}, value="")

    def ready(self) -> bool:
        if self.is_batch:
            return bool(self.selected_jobs())
        return bool(self.project.value and self.repo.value and self.branch.value and self.report.value)

    def fetch(self, progress: Callable[[str], None]) -> Fetched:
        """Blocking download; a report bound to a published model gets it from Fabric (the
        same sign-in)."""
        connected: list[fabric.FetchedModel] = []

        def connected_model(pbir: dict) -> Path:
            model_id, workspace = fabric.model_reference(pbir)
            connected.append(
                fabric.fetch_connected_model(
                    make_fabric_client(), model_id, workspace, output_root() / "_fabric", progress
                )
            )
            return connected[-1].model_path

        commit = self.version.value or ""
        version, version_type = (commit, "commit") if commit else (self.branch.value, "branch")
        fetched = devops.fetch_report(
            self._get_client(),
            self.project.value,
            self.repo.value,
            self.report.value,
            # default_destination() is output/_devops/...: keep the layout, below output_root()
            output_root()
            / devops.default_destination(self.project.value, self.repo.value, version).relative_to(
                "output"
            ),
            version,
            version_type,
            progress=progress,
            all_reports=self.all_reports.value,
            connected_model=connected_model,
        )
        return Fetched(
            fetched.report_folder,
            fetched.model_path,
            fetched.report_folders[1:],
            fetched.skipped,
            service_model=connected_service_model(connected),
        )


def connected_service_model(connected: list) -> Optional[ServiceModel]:
    """The published model of a service-bound report, for statistics (None if there was none)."""
    if not connected:
        return None
    model = connected[-1]
    return ServiceModel(model.workspace_id, model.model_id, model.model)
