"""
Playwright-based browser client for the Connecticut CTEMS election results site.

The site (https://ctemspublic.tgstg.net/#/home) is an AngularJS single-page
application.  All navigation is done by interacting with dropdown menus and
links on the same base URL — there are no separate result URLs per election.

Two-phase interaction model
---------------------------
**Phase 1 — Statewide:**
  1. Load the home page and wait for the election dropdown to populate.
  2. Select an election by option value.
  3. Wait for statewide race panels to render.
  4. Return the fully rendered HTML.

**Phase 2 — Town-level:**
  1. Load the home page, select the election.
  2. Click the "Select Town" navigation link.
  3. Wait for the county and town dropdowns to appear.
  4. Collect the full list of county + town option pairs.
  5. For each (county, town) pair: select county → select town → wait → capture HTML.

Selector notes
--------------
The selectors below were inferred from the AngularJS template structure observed
via static HTML fetch.  **They should be verified against the live rendered page.**
To inspect the actual selectors, run the client with ``headless=False`` and open
browser DevTools, or save the rendered HTML to disk::

    with CtPlaywrightClient(headless=False) as client:
        html = client.get_landing_page()
    with open("/tmp/ct_landing.html", "w") as f:
        f.write(html)
"""

from __future__ import annotations

import time

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from playwright_base import BasePlaywrightClient

CT_BASE_URL = "https://ctemspublic.tgstg.net/#/home"

# ── CSS / text selectors ───────────────────────────────────────────────────────
# TODO: verify these by inspecting the live rendered page.

# The main election dropdown.  AngularJS usually renders it as the first <select>
# on the page; we use the label text as a fallback locator.
_ELECTION_SELECT_SEL = "select"          # first select on the page

# Selector for a rendered race/contest container (statewide view).
# AngularJS apps often repeat items in a div with class "race", "contest", or
# "ng-scope".  Update this once you've inspected the live HTML.
_RACE_CONTAINER_SEL = ".race, .contest, tr.ng-scope, table"

# The "Select Town" navigation tab/link.
_TOWN_TAB_SEL = "a"   # narrowed further by text match in the client method

# After clicking "Select Town", the page has 4 <select> elements:
#   [0]  election dropdown (visible)
#   [1]  duplicate election dropdown (hidden, ng-hide)
#   [2]  county dropdown  ng-model='selectedCounty'  ← after town tab click
#   [3]  town dropdown    ng-model='selectedTown'    ← filtered by county selection
_COUNTY_SELECT_INDEX = 2
_TOWN_SELECT_INDEX   = 3

# How long to wait (ms) for race containers to appear after selecting an election.
_RESULTS_TIMEOUT_MS = 45_000
# How long to wait (ms) for county/town dropdowns to appear.
_DROPDOWN_TIMEOUT_MS = 20_000


class CtPlaywrightClient(BasePlaywrightClient):
    """Browser client for the Connecticut CTEMS election results site.

    Parameters
    ----------
    headless : bool
        Run browser in headless mode (default True).  Set False for debugging.
    sleep_s : float
        Seconds to wait after interactions to allow AngularJS to finish
        rendering (default 3.0).
    """

    def __init__(self, headless: bool = True, sleep_s: float = 3.0):
        super().__init__(headless=headless, sleep_s=sleep_s)

    # ── Public navigation methods ──────────────────────────────────────────────

    def get_landing_page(self) -> str:
        """Load the CTEMS home page and return HTML once the election dropdown populates.

        Returns
        -------
        str
            Fully rendered HTML including the populated election dropdown.
        """
        self._navigate(CT_BASE_URL)
        self._wait_for_election_options()
        assert self.page is not None
        return self.page.content()

    def get_statewide_results(self, election_option_value: str) -> str:
        """Select an election and return the statewide results HTML.

        Navigates to the home page, selects the election from the dropdown,
        waits for AngularJS to render the race panels, then returns the page HTML.

        Parameters
        ----------
        election_option_value : str
            The ``<option value="...">`` string for the target election.

        Returns
        -------
        str
            Fully rendered HTML with all statewide race results.
        """
        self._navigate(CT_BASE_URL)
        self._wait_for_election_options()
        self._select_election(election_option_value)
        self._wait_for_results()
        assert self.page is not None
        return self.page.content()

    def get_county_town_options(
        self, election_option_value: str
    ) -> list[tuple[str, str, list[tuple[str, str]]]]:
        """Select an election and return all (county, town) option pairs.

        Navigates to town view for the given election and enumerates every
        county + its towns from the dropdowns.

        Parameters
        ----------
        election_option_value : str
            The ``<option value="...">`` string for the target election.

        Returns
        -------
        list of (county_name, county_value, [(town_name, town_value), ...])
        """
        self._navigate(CT_BASE_URL)
        self._wait_for_election_options()
        self._select_election(election_option_value)
        self._wait_for_results()
        self._click_town_tab()
        self._wait_for_county_dropdown()

        assert self.page is not None
        county_options = self._read_select_options(index=_COUNTY_SELECT_INDEX)
        result = []
        for county_name, county_value in county_options:
            if not county_value or county_name.startswith("--"):
                continue
            self._select_by_index(index=_COUNTY_SELECT_INDEX, value=county_value)
            time.sleep(self.sleep_s)
            town_options = self._read_select_options(index=_TOWN_SELECT_INDEX)
            towns = [
                (tn, tv)
                for tn, tv in town_options
                if tv and not tn.startswith("--")
            ]
            result.append((county_name, county_value, towns))
        return result

    def get_all_towns_for_county(
        self,
        election_option_value: str,
        county_name: str,
        county_option_value: str,
        towns: list[tuple[str, str]],
    ) -> list[tuple[str, str]]:
        """Scrape all towns in one county within a single browser session.

        Navigates to the town view once, selects the county, then iterates
        through every town in ``towns`` by changing only the town dropdown —
        far more efficient than opening a new browser per town.

        Designed to be called from a parallel worker: one worker per county,
        each with its own ``CtPlaywrightClient`` context.

        Parameters
        ----------
        election_option_value : str
            Option value for the target election.
        county_name : str
            Human-readable county name (used only for log messages).
        county_option_value : str
            Option value for the county in the county dropdown.
        towns : list of (town_name, town_option_value)
            All towns to scrape for this county, in order.

        Returns
        -------
        list of (town_name, html)
            One entry per town that returned non-empty HTML.
        """
        self._navigate(CT_BASE_URL)
        self._wait_for_election_options()
        self._select_election(election_option_value)
        self._wait_for_results()
        self._click_town_tab()
        self._wait_for_county_dropdown()

        # Select county once — this filters the town dropdown to this county's towns.
        self._select_by_index(index=_COUNTY_SELECT_INDEX, value=county_option_value)
        self._wait_for_town_dropdown()

        assert self.page is not None
        results: list[tuple[str, str]] = []

        for town_name, town_value in towns:
            print(f"[CT]       {county_name} / {town_name}")
            self._select_by_index(index=_TOWN_SELECT_INDEX, value=town_value)
            time.sleep(self.sleep_s)
            html = self.page.content()
            results.append((town_name, html))

        return results

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _wait_for_election_options(self) -> None:
        """Wait until the election dropdown has at least one non-placeholder option."""
        assert self.page is not None
        try:
            # Wait for the select to contain at least two options (placeholder + ≥1 election)
            self.page.wait_for_function(
                f"() => document.querySelectorAll('{_ELECTION_SELECT_SEL} option').length > 1",
                timeout=_RESULTS_TIMEOUT_MS,
            )
        except PlaywrightTimeoutError:
            print("[CT] WARNING: Election dropdown did not populate — selector may need updating.")
        if self.sleep_s:
            time.sleep(self.sleep_s)

    def _select_election(self, option_value: str) -> None:
        """Select an election from the main dropdown by option value."""
        assert self.page is not None
        selects = self.page.query_selector_all(_ELECTION_SELECT_SEL)
        if not selects:
            print(f"[CT] WARNING: No <select> found on page — cannot select election.")
            return
        selects[0].select_option(value=option_value)
        time.sleep(self.sleep_s)

    def _wait_for_results(self) -> None:
        """Wait for race/contest content to appear after selecting an election."""
        assert self.page is not None
        try:
            self.page.wait_for_selector(_RACE_CONTAINER_SEL, timeout=_RESULTS_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            print(
                f"[CT] WARNING: Timed out waiting for race containers "
                f"(selector: {_RACE_CONTAINER_SEL!r}). "
                "The selector may need updating — save the HTML for inspection."
            )
        if self.sleep_s:
            time.sleep(self.sleep_s)

    def _click_town_tab(self) -> None:
        """Click the 'Select Town' navigation link to switch to town view."""
        assert self.page is not None
        # Use text matching to find the correct navigation link.
        try:
            link = self.page.get_by_text("Select Town", exact=False).first
            link.click()
            time.sleep(self.sleep_s)
        except Exception as exc:
            print(f"[CT] WARNING: Could not click 'Select Town' tab: {exc}")

    def _wait_for_county_dropdown(self) -> None:
        """Wait for the county dropdown (index 2) to be visible after clicking Select Town."""
        assert self.page is not None
        try:
            # County dropdown is the 3rd select (index 2) and must be visible
            self.page.wait_for_function(
                f"""() => {{
                    const s = document.querySelectorAll('{_ELECTION_SELECT_SEL}');
                    return s.length > {_COUNTY_SELECT_INDEX} && s[{_COUNTY_SELECT_INDEX}].offsetParent !== null;
                }}""",
                timeout=_DROPDOWN_TIMEOUT_MS,
            )
        except PlaywrightTimeoutError:
            print("[CT] WARNING: County dropdown did not become visible.")
        if self.sleep_s:
            time.sleep(self.sleep_s)

    def _wait_for_town_dropdown(self) -> None:
        """Wait for the town dropdown (index 3) to be populated after selecting a county."""
        assert self.page is not None
        try:
            # Town dropdown should have > 1 option (placeholder + at least one town)
            self.page.wait_for_function(
                f"""() => {{
                    const s = document.querySelectorAll('{_ELECTION_SELECT_SEL}');
                    return s.length > {_TOWN_SELECT_INDEX} && s[{_TOWN_SELECT_INDEX}].options.length > 1;
                }}""",
                timeout=_DROPDOWN_TIMEOUT_MS,
            )
        except PlaywrightTimeoutError:
            print("[CT] WARNING: Town dropdown did not populate after county selection.")
        if self.sleep_s:
            time.sleep(self.sleep_s)

    def _read_select_options(self, index: int) -> list[tuple[str, str]]:
        """Return all (text, value) pairs from the select at the given 0-based index."""
        assert self.page is not None
        return self.page.evaluate(
            f"""
            () => {{
                const selects = document.querySelectorAll('{_ELECTION_SELECT_SEL}');
                if (selects.length <= {index}) return [];
                return Array.from(selects[{index}].options).map(o => [o.text.trim(), o.value]);
            }}
            """
        )

    def _select_by_index(self, index: int, value: str) -> None:
        """Select an option in the select element at the given 0-based index."""
        assert self.page is not None
        selects = self.page.query_selector_all(_ELECTION_SELECT_SEL)
        if len(selects) <= index:
            print(f"[CT] WARNING: Only {len(selects)} <select> element(s) found; cannot select index {index}.")
            return
        selects[index].select_option(value=value)
