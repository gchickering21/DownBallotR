"""
Playwright-based browser client for the Louisiana SOS Graphical election results.

Source: https://voterportal.sos.la.gov/Graphical

The site is a JavaScript SPA.  All navigation is done by interacting with
a date dropdown and tab buttons — there are no separate result URLs per
election.

Interaction model
-----------------
1. Load the landing page and wait for the election dropdown to populate.
2. Select an election by option value.
3. Wait for results to render and discover which tabs are available
   (Congressional, Presidential, Statewide, Multiparish, Parish, …).
4. For each non-Parish tab: click the tab, wait for content, capture HTML.
5. For the Parish tab: enumerate the parish sub-dropdown, filter to parishes
   with > 0 precincts reporting, select each one in turn, capture HTML.

"""

from __future__ import annotations

import time

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from playwright_base import BasePlaywrightClient

LA_BASE_URL = "https://voterportal.sos.la.gov/Graphical"

# ── Selectors (all confirmed against live site HTML) ──────────────────────────

# Main election date dropdown (first <select> on page, id="ElectionId").
_ELECTION_SELECT_SEL = "select"

# Rendered race containers carry "ng-scope"; AngularJS only adds this to bound
# elements, so it reliably signals that results have loaded.
_RESULTS_CONTAINER_SEL = "div.race-container.ng-scope"

# Tab links — confirmed: <ul class="nav nav-tabs"><li><a ng-click="navigate(...)">
_TAB_SEL = "ul.nav li a, nav a, .tab, button[role='tab']"

# Non-Parish tabs in priority order; earlier tabs win when deduplicating across tabs.
# Multiparish is last because its races often duplicate Statewide/Congressional rows.
_KNOWN_STATE_TABS = (
    "Congressional",
    "Presidential",
    "Statewide",
    "Legislative",
    "Multiparish",
)

# Parish tab label as it appears on the live site.
_PARISH_TAB_LABEL = "Parish"

# Parish sub-dropdown — confirmed: <select class="form-control parish-select" ...>
_PARISH_SELECT_SEL = "select.parish-select"

# "View Results" button that must be clicked after selecting a parish.
# Confirmed from template: <input type="button" value="View Results" ng-click="viewClicked()">
_VIEW_RESULTS_BTN_SEL = "input[value='View Results']"

# "change parish" link shown in results mode; clicking it returns to the parish
# selection dropdown so the next parish can be selected.
# Confirmed from template: <a ng-click="changeParish()">change parish</a>
_CHANGE_PARISH_SEL = "a[ng-click='changeParish()']"

# How long (ms) to wait for results to appear after selecting an election or tab.
_RESULTS_TIMEOUT_MS = 20_000
# How long (ms) to wait for the parish dropdown to populate.
_PARISH_DROPDOWN_TIMEOUT_MS = 15_000


class LaPlaywrightClient(BasePlaywrightClient):
    """Browser client for the Louisiana SOS Graphical election results site.

    Parameters
    ----------
    headless : bool
        Run browser in headless mode (default True).  Set False for debugging.
    sleep_s : float
        Seconds to wait after interactions to allow JavaScript to finish
        rendering (default 3.0).
    """

    def __init__(self, headless: bool = True, sleep_s: float = 3.0):
        super().__init__(headless=headless, sleep_s=sleep_s)

    # ── Public navigation methods ──────────────────────────────────────────────

    def get_landing_page(self) -> str:
        """Load the LA SOS Graphical page and return HTML once the dropdown populates.

        Returns
        -------
        str
            Fully rendered HTML including the populated election dropdown.
        """
        self._navigate(LA_BASE_URL)
        self._wait_for_election_options()
        assert self.page is not None
        return self.page.content()

    def get_available_tabs(self, election_option_value: str) -> "list[str] | None":
        """Select an election and return the text labels of all available tabs.

        Parameters
        ----------
        election_option_value : str
            The ``<option value="...">`` string for the target election.

        Returns
        -------
        list[str] or None
            Tab labels as they appear on the page (e.g. ['Statewide', 'Parish']).
            Returns None if the results container never loaded (site has no data
            for this election). Returns an empty list if results loaded but no
            tabs were found.
        """
        self._navigate(LA_BASE_URL)
        self._wait_for_election_options()
        if not self._select_election(election_option_value):
            return None
        if not self._wait_for_results():
            return None
        assert self.page is not None
        return self._read_tab_labels()

    def get_tab_results(self, election_option_value: str, tab_label: str) -> str:
        """Select an election, click a tab, and return the rendered HTML.

        Parameters
        ----------
        election_option_value : str
            Option value for the target election.
        tab_label : str
            Visible text of the tab to click (e.g. ``'Statewide'``).

        Returns
        -------
        str
            Fully rendered HTML of the tab content.
        """
        self._navigate(LA_BASE_URL)
        self._wait_for_election_options()
        if not self._select_election(election_option_value):
            return ""
        if not self._wait_for_results():
            return ""
        self._click_tab(tab_label)
        assert self.page is not None
        return self.page.content()

    def get_parish_options(self, election_option_value: str) -> list[tuple[str, str]]:
        """Select an election, open the Parish tab, and return parishes with > 0 reporting.

        Parameters
        ----------
        election_option_value : str
            Option value for the target election.

        Returns
        -------
        list of (parish_name, parish_option_value)
            Only parishes that appear to have at least 1 precinct reporting.
            The filtering logic reads reporting info from the option text when
            present; if the site does not expose reporting counts in the dropdown
            text, all non-placeholder options are returned (see TODO below).
        """
        self._navigate(LA_BASE_URL)
        self._wait_for_election_options()
        if not self._select_election(election_option_value):
            return None
        if not self._wait_for_results():
            return None  # site has no results for this election
        self._click_tab(_PARISH_TAB_LABEL)
        self._wait_for_parish_dropdown()
        assert self.page is not None
        all_options = self._read_parish_options()
        return self._filter_reporting_parishes(all_options)

    def get_parish_results(
        self, election_option_value: str, parish_option_value: str
    ) -> str:
        """Select an election and a parish; return the rendered parish results HTML.

        Reuses the existing election selection: navigates to the Parish tab once,
        then selects the parish from the sub-dropdown.

        Parameters
        ----------
        election_option_value : str
            Option value for the target election.
        parish_option_value : str
            Option value for the target parish in the parish sub-dropdown.

        Returns
        -------
        str
            Fully rendered HTML with parish-level candidate results.
        """
        self._navigate(LA_BASE_URL)
        self._wait_for_election_options()
        if not self._select_election(election_option_value):
            return ""
        self._wait_for_results()
        self._click_tab(_PARISH_TAB_LABEL)
        self._wait_for_parish_dropdown()
        self._select_parish(parish_option_value)
        assert self.page is not None
        return self.page.content()

    def get_all_state_tabs_html(
        self,
        election_option_value: str,
        tab_labels: list[str],
    ) -> list[tuple[str, str]]:
        """Navigate once, select an election, then capture HTML for each state tab.

        More efficient than calling ``get_tab_results`` per tab because the
        election is selected only once and the browser stays on the same page.

        Parameters
        ----------
        election_option_value : str
            Option value for the target election.
        tab_labels : list[str]
            Tab labels to capture (should be non-Parish tabs, e.g. from
            ``get_available_tabs()``).

        Returns
        -------
        list of (tab_label, html)
            One entry per tab, in the order given.
        """
        self._navigate(LA_BASE_URL)
        self._wait_for_election_options()
        if not self._select_election(election_option_value):
            return []
        if not self._wait_for_results():
            return []

        assert self.page is not None
        results: list[tuple[str, str]] = []
        for tab_label in tab_labels:
            print(f"[LA]       Tab: {tab_label}")
            self._click_tab(tab_label)
            results.append((tab_label, self.page.content()))
        return results

    def get_all_parishes_results(
        self,
        election_option_value: str,
        parishes: list[tuple[str, str]],
    ) -> list[tuple[str, str]]:
        """Open the Parish tab once and scrape all given parishes in one session.

        More efficient than calling ``get_parish_results`` per parish because
        the election is selected only once.

        Parameters
        ----------
        election_option_value : str
            Option value for the target election.
        parishes : list of (parish_name, parish_option_value)
            Parishes to scrape (typically the output of ``get_parish_options``).

        Returns
        -------
        list of (parish_name, html)
            One entry per parish.
        """
        self._navigate(LA_BASE_URL)
        self._wait_for_election_options()
        if not self._select_election(election_option_value):
            return []
        if not self._wait_for_results():
            return []
        self._click_tab(_PARISH_TAB_LABEL)
        self._wait_for_parish_dropdown()

        assert self.page is not None
        results: list[tuple[str, str]] = []

        for parish_name, parish_value in parishes:
            print(f"[LA]       Parish: {parish_name}")
            self._select_parish(parish_value)
            html = self.page.content()
            results.append((parish_name, html))

        return results

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _wait_for_election_options(self) -> None:
        """Wait until the election dropdown has at least one non-placeholder option."""
        assert self.page is not None
        try:
            self.page.wait_for_function(
                "sel => document.querySelectorAll(sel).length > 1",
                arg=f"{_ELECTION_SELECT_SEL} option",
                timeout=_RESULTS_TIMEOUT_MS,
            )
        except PlaywrightTimeoutError:
            print(
                "[LA] WARNING: Election dropdown did not populate within timeout. "
                "Run inspect_landing.py to check the selector."
            )
        if self.sleep_s:
            time.sleep(self.sleep_s)

    def _wait_for_specific_option(
        self, option_value: str, timeout_ms: int = 20_000
    ) -> bool:
        """Wait until a specific option value exists in the election dropdown.

        Returns True when found, False on timeout.  Calling this before
        ``select_option`` avoids a 30-second Playwright timeout when the
        option is simply not yet rendered (AngularJS loads options
        asynchronously) or genuinely absent (future / no-results election).
        """
        assert self.page is not None
        try:
            self.page.wait_for_function(
                "([sel, val]) => { "
                "  const s = document.querySelector(sel); "
                "  if (!s) return false; "
                "  return Array.from(s.options).some(o => o.value === val); "
                "}",
                arg=[_ELECTION_SELECT_SEL, option_value],
                timeout=timeout_ms,
            )
            return True
        except PlaywrightTimeoutError:
            return False

    def _select_election(self, option_value: str) -> bool:
        """Select an election from the main dropdown by option value.

        Returns True on success, False if the option is not found in the
        dropdown within the wait timeout (election may not have results yet).
        """
        assert self.page is not None
        if not self._wait_for_specific_option(option_value):
            print(
                f"[LA] NOTE: Option {option_value!r} not found in election dropdown "
                "— election may not have results published yet."
            )
            return False
        selects = self.page.query_selector_all(_ELECTION_SELECT_SEL)
        if not selects:
            print("[LA] WARNING: No <select> found — cannot select election.")
            return False
        selects[0].select_option(value=option_value)
        time.sleep(self.sleep_s)
        return True

    def _wait_for_results(self) -> bool:
        """Wait for result content to appear after selecting an election.

        Returns True if results loaded, False if the container never appeared
        (e.g. the site has no results for this election date).
        """
        assert self.page is not None
        try:
            self.page.wait_for_selector(_RESULTS_CONTAINER_SEL, timeout=_RESULTS_TIMEOUT_MS)
            if self.sleep_s:
                time.sleep(self.sleep_s)
            return True
        except PlaywrightTimeoutError:
            return False

    def _read_tab_labels(self) -> list[str]:
        """Return the visible text labels of all tab elements currently on the page."""
        assert self.page is not None
        for sel in _TAB_SEL.split(","):
            sel = sel.strip()
            elements = self.page.query_selector_all(sel)
            if elements:
                labels = []
                for el in elements:
                    text = (el.inner_text() or "").strip()
                    if text:
                        labels.append(text)
                if labels:
                    return labels
        print(
            "[LA] WARNING: No tab elements found with selector "
            f"{_TAB_SEL!r}. Update _TAB_SEL in client.py."
        )
        return []

    def _click_tab(self, tab_label: str) -> None:
        """Click the tab whose visible text exactly matches ``tab_label`` (case-insensitive)."""
        assert self.page is not None
        label_lower = tab_label.strip().lower()
        for sel in _TAB_SEL.split(","):
            sel = sel.strip()
            elements = self.page.query_selector_all(sel)
            for el in elements:
                text = (el.inner_text() or "").strip().lower()
                if text == label_lower:
                    el.click()
                    time.sleep(self.sleep_s)
                    return
        print(
            f"[LA] WARNING: Tab {tab_label!r} not found. "
            "Run inspect_landing.py to check available tab labels."
        )

    def _wait_for_parish_dropdown(self) -> None:
        """Wait for the parish sub-dropdown to populate after clicking the Parish tab.

        The dropdown is populated by an AngularJS ng-repeat binding; it may take
        a second or two after the tab click for the options to render.
        """
        assert self.page is not None
        try:
            self.page.wait_for_function(
                "sel => { const s = document.querySelector(sel); "
                "return s && s.options.length > 1; }",
                arg=_PARISH_SELECT_SEL,
                timeout=_PARISH_DROPDOWN_TIMEOUT_MS,
            )
        except PlaywrightTimeoutError:
            print(
                "[LA] WARNING: Parish dropdown did not populate. "
                f"Selector used: {_PARISH_SELECT_SEL!r}. "
                "Run inspect_landing.py --tab Parish to check the live HTML."
            )
        if self.sleep_s:
            time.sleep(self.sleep_s)

    def _read_parish_options(self) -> list[tuple[str, str]]:
        """Return all (text, value) pairs from the parish sub-dropdown."""
        assert self.page is not None
        return self.page.evaluate(
            "sel => { "
            "  const s = document.querySelector(sel); "
            "  if (!s) return []; "
            "  return Array.from(s.options).map(o => [o.text.trim(), o.value]); "
            "}",
            _PARISH_SELECT_SEL,
        )

    def _filter_reporting_parishes(
        self, options: list[tuple[str, str]]
    ) -> list[tuple[str, str]]:
        """Filter parish options to those with > 0 precincts reporting.

        Strategy
        --------
        1. Skip blank / placeholder options (no value, starts with "--" / "Select").
        2. Look for a reporting fraction in the option text, e.g. "(150/200)" or
           "150 of 200".  If found, keep only options where the numerator > 0.
        3. If no reporting fraction is found in the text, keep all options —
           the pipeline will scrape them all and the parser can skip empty pages.

        TODO: After live inspection, confirm whether the parish dropdown text
        includes reporting counts.  If it does not, and you want to skip
        zero-reporting parishes, you may need to select each parish and check
        a "precincts reporting" indicator on the page itself — update this
        method and ``get_all_parishes_results`` accordingly.
        """
        import re
        # Matches patterns like "(5/200)", "5 of 200", "5/200"
        _REPORTING_RE = re.compile(r"(\d+)\s*/\s*(\d+)|(\d+)\s+of\s+(\d+)")

        result: list[tuple[str, str]] = []
        has_reporting_info = False

        for name, value in options:
            # Skip placeholder / blank options
            if not value or not name or name.startswith("--") or name.lower().startswith("select"):
                continue

            m = _REPORTING_RE.search(name)
            if m:
                has_reporting_info = True
                numerator = int(m.group(1) or m.group(3))
                if numerator > 0:
                    result.append((name, value))
            else:
                result.append((name, value))

        if has_reporting_info:
            total = len([o for o in options if o[1] and not o[0].startswith("--")])
            print(
                f"[LA]   Parish dropdown: {len(result)}/{total} parishes have > 0 reporting."
            )
        else:
            print(
                "[LA]   Parish dropdown: no reporting counts found in option text — "
                "including all non-placeholder parishes."
            )

        return result

    def _select_parish(self, parish_option_value: str) -> None:
        """Select a parish from the sub-dropdown and click 'View Results'.

        After viewing results for one parish, the AngularJS view switches to
        ``resultsMode=true``, hiding the dropdown.  We must click "change parish"
        to return to selection mode before selecting the next parish.
        """
        assert self.page is not None

        # If already in results mode, click "change parish" to get back to the dropdown.
        change_link = self.page.query_selector(_CHANGE_PARISH_SEL)
        if change_link and change_link.is_visible():
            change_link.click()
            # Wait for options to re-populate, not just for the element to be visible.
            # AngularJS re-binds options asynchronously after clicking "change parish".
            self._wait_for_parish_dropdown()

        sel = self.page.query_selector(_PARISH_SELECT_SEL)
        if not sel:
            print(f"[LA] WARNING: Parish dropdown not found ({_PARISH_SELECT_SEL!r}).")
            return
        sel.select_option(value=parish_option_value)
        time.sleep(0.5)  # brief pause for AngularJS model update

        # Click "View Results" to trigger viewClicked() and load parish results.
        btn = self.page.query_selector(_VIEW_RESULTS_BTN_SEL)
        if btn:
            btn.click()
        else:
            print(
                f"[LA] WARNING: 'View Results' button not found ({_VIEW_RESULTS_BTN_SEL!r}). "
                "Results may not load correctly."
            )
        # Wait for results container to (re-)appear with parish data.
        try:
            self.page.wait_for_selector(
                _RESULTS_CONTAINER_SEL, timeout=_RESULTS_TIMEOUT_MS
            )
        except PlaywrightTimeoutError:
            print(f"[LA] WARNING: Results did not appear after selecting parish.")
        if self.sleep_s:
            time.sleep(self.sleep_s)
