"""
Playwright-based browser client for the Georgia Secretary of State election
results site (results.sos.ga.gov).

The site is JavaScript-rendered and sits behind Cloudflare bot protection.
Browser lifecycle and stealth settings are handled by BasePlaywrightClient
(playwright_base.py); this subclass adds Georgia-specific navigation methods.

Virtual scrolling
-----------------
Election pages use Angular's virtual scroller: panels are only rendered as the
user scrolls down.  ``get_election_page`` and ``get_county_page`` scroll to the
bottom in a loop until no new panels appear, ensuring ALL contests are captured.

Vote-method breakdown
---------------------
Each contest panel has a "Vote Method" toggle button.  When clicked, Angular
replaces the bar-chart view with a table showing votes split by method
(Advanced Voting / Election Day / Absentee by Mail / Provisional).
``get_election_page_with_vote_methods`` and
``get_county_page_with_vote_methods`` perform this interaction before returning
the HTML.

Typical usage
-------------
>>> with GaPlaywrightClient() as client:
...     landing_html = client.get_landing_page()
...     election_html = client.get_election_page(
...         "https://results.sos.ga.gov/results/public/Georgia/elections/2024NovGen"
...     )
"""

from __future__ import annotations

import time

from playwright_base import BasePlaywrightClient

GA_BASE_URL = "https://results.sos.ga.gov/results/public/Georgia"

# CSS selector for ballot-item panels
_PANEL_SEL = "p-panel.ballot-item"
# Per-panel Vote Method toggle button selector
_VOTE_METHOD_BTN_SEL = "button.p-panel-header-icon[role='checkbox']"


class GaPlaywrightClient(BasePlaywrightClient):
    """Browser client for the Georgia SOS election results site.

    Parameters
    ----------
    headless : bool
        Run browser in headless mode (default True).  Set False for debugging.
    sleep_s : float
        Seconds to wait after a page loads to allow JS rendering to complete.
    """

    def __init__(self, headless: bool = True, sleep_s: float = 3.0):
        super().__init__(headless=headless, sleep_s=sleep_s)

    # ── Public navigation methods ──────────────────────────────────────────────

    def get_landing_page(self) -> str:
        """Render the Georgia elections landing page and return the HTML.

        The landing page at ``GA_BASE_URL`` lists all elections grouped by year.
        Each election has a link to its results page.

        Returns
        -------
        str
            Fully rendered HTML after JavaScript execution.
        """
        self._navigate(GA_BASE_URL)
        self._wait_and_sleep("a[href*='/results/public/Georgia/']")
        assert self.page is not None
        return self.page.content()

    def get_election_page(self, url: str) -> str:
        """Render an individual election results page and return the HTML.

        Scrolls to the bottom in a loop until no new panels appear so that
        Angular's virtual scroller renders ALL contests, not just the first
        viewport's worth.

        Parameters
        ----------
        url : str
            Full URL to a Georgia SOS election results page.

        Returns
        -------
        str
            Fully rendered HTML of all contests.
        """
        self._navigate(url)
        self._wait_and_sleep(_PANEL_SEL)
        self._scroll_to_load_all()
        assert self.page is not None
        return self.page.content()

    def get_election_page_with_vote_methods(self, url: str) -> str:
        """Render an election page with all vote-method breakdowns expanded.

        Loads the page, scrolls to render all panels, then clicks every panel's
        "Vote Method" toggle so Angular replaces bar-chart views with per-method
        vote tables (Advanced Voting / Election Day / Absentee by Mail /
        Provisional).

        Parameters
        ----------
        url : str
            Full URL to a Georgia SOS election results page.

        Returns
        -------
        str
            Fully rendered HTML with vote-method tables for every contest.
        """
        self._navigate(url)
        self._wait_and_sleep(_PANEL_SEL)
        self._scroll_to_load_all()
        self._click_all_vote_method_buttons()
        assert self.page is not None
        return self.page.content()

    def get_county_page(self, url: str) -> str:
        """Render a per-county election results page and return the HTML.

        County pages share the same Angular structure as the main election page
        but are filtered to a single county.  Virtual scrolling may still apply
        for counties with many local contests.

        Parameters
        ----------
        url : str
            Full URL to a county election page, e.g.
            ``https://results.sos.ga.gov/results/public/fulton-county-ga/elections/2024NovGen``

        Returns
        -------
        str
            Fully rendered HTML after JavaScript execution.
        """
        self._navigate(url)
        self._wait_and_sleep(_PANEL_SEL)
        self._scroll_to_load_all()
        assert self.page is not None
        return self.page.content()

    def get_county_page_with_vote_methods(self, url: str) -> str:
        """Render a per-county election page with all vote-method breakdowns.

        Parameters
        ----------
        url : str
            Full URL to a county election page.

        Returns
        -------
        str
            Fully rendered HTML with vote-method tables for every contest.
        """
        self._navigate(url)
        self._wait_and_sleep(_PANEL_SEL)
        self._scroll_to_load_all()
        self._click_all_vote_method_buttons()
        assert self.page is not None
        return self.page.content()

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _scroll_to_load_all(
        self,
        panel_sel: str = _PANEL_SEL,
        max_rounds: int = 30,
        settle_s: float = 1.5,
    ) -> None:
        """Scroll to the bottom repeatedly until no new ballot-item panels appear.

        Angular's virtual scroller only renders panels in the viewport.  This
        method scrolls to the bottom in a loop, waiting ``settle_s`` seconds
        between each scroll, stopping when the panel count stabilises.

        Parameters
        ----------
        panel_sel : str
            CSS selector used to count rendered panels (default
            ``"p-panel.ballot-item"``).
        max_rounds : int
            Safety cap on scroll iterations (default 30 ≈ 1 500 panels).
        settle_s : float
            Seconds to wait after each scroll before re-counting (default 1.5).
        """
        assert self.page is not None
        prev_count = -1
        for _ in range(max_rounds):
            count = len(self.page.query_selector_all(panel_sel))
            if count == prev_count:
                break
            prev_count = count
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(settle_s)
        # Scroll back to top so any subsequent interactions start from the top
        self.page.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.5)

    def _click_all_vote_method_buttons(self, settle_s: float = 3.0) -> None:
        """Click every per-panel Vote Method toggle and wait for re-renders.

        Each ballot-item panel header has a ``button[role='checkbox']`` that
        toggles between the bar-chart view and a per-vote-method breakdown
        table.  This method clicks all of them and waits for Angular to finish
        updating the DOM.

        Parameters
        ----------
        settle_s : float
            Seconds to wait after all clicks for Angular to finish rendering
            (default 3.0).
        """
        assert self.page is not None
        btns = self.page.query_selector_all(_VOTE_METHOD_BTN_SEL)
        if not btns:
            print("[GA] WARNING: No vote-method buttons found; HTML unchanged.")
            return
        print(f"[GA]   Expanding vote-method breakdowns for {len(btns)} contest(s)...")
        for btn in btns:
            btn.click()
        time.sleep(settle_s)
