"""
Generic Playwright client for Angular/PrimeNG Secretary of State election
results sites.

All SOS sites using this framework share the same Angular component structure
(virtual scrolling, p-panel.ballot-item, vote-method toggle buttons).  This
client handles all navigation and interaction; state-specific details (base URL,
log prefix) are passed at construction time.

Subclasses for individual states (GaPlaywrightClient, UtPlaywrightClient) set
state-specific defaults and are the recommended import for state-specific code.

Typical usage via a state subclass
-----------------------------------
>>> from Georgia.client import GaPlaywrightClient
>>> with GaPlaywrightClient() as client:
...     landing_html = client.get_landing_page()
...     election_html = client.get_election_page(url)

Direct usage with explicit parameters
--------------------------------------
>>> from Clarity.client import ClarityPlaywrightClient
>>> with ClarityPlaywrightClient(
...     base_url="https://electionresults.utah.gov/results/public/Utah",
...     log_prefix="[UT]",
... ) as client:
...     landing_html = client.get_landing_page()
"""

from __future__ import annotations

import time
from urllib.parse import urlparse

from playwright_base import BasePlaywrightClient

# CSS selector for ballot-item panels — same across all SOS sites
_PANEL_SEL = "p-panel.ballot-item"
# Per-panel Vote Method toggle button selector
_VOTE_METHOD_BTN_SEL = "button.p-panel-header-icon[role='checkbox']"


class ClarityPlaywrightClient(BasePlaywrightClient):
    """Generic browser client for Angular/PrimeNG SOS election results sites.

    Parameters
    ----------
    base_url : str
        Landing page URL, e.g.
        ``"https://results.sos.ga.gov/results/public/Georgia"`` or
        ``"https://electionresults.utah.gov/results/public/Utah"``.
        Also used as the prefix for individual election page navigation.
    log_prefix : str
        Short prefix for console messages, e.g. ``"[GA]"`` or ``"[UT]"``.
    headless : bool
        Run browser in headless mode (default True).
    sleep_s : float
        Seconds to wait after a page loads to allow JS rendering to complete.
    """

    def __init__(
        self,
        base_url: str,
        log_prefix: str = "",
        headless: bool = True,
        sleep_s: float = 3.0,
    ):
        super().__init__(headless=headless, sleep_s=sleep_s)
        self.base_url = base_url
        self.log_prefix = log_prefix
        # Derive the URL path segment used in the landing page wait selector.
        # e.g. "https://results.sos.ga.gov/results/public/Georgia"
        #   → _landing_path = "/results/public/Georgia/"
        parsed = urlparse(base_url)
        self._landing_path = parsed.path.rstrip("/") + "/"

    # ── Public navigation methods ──────────────────────────────────────────────

    def get_landing_page(self) -> str:
        """Render the elections landing page and return the HTML."""
        self._navigate(self.base_url)
        self._wait_and_sleep(f"a[href*='{self._landing_path}']")
        assert self.page is not None
        return self.page.content()

    def get_election_page(self, url: str) -> str:
        """Render an individual election results page and return the HTML.

        Scrolls to the bottom in a loop until no new panels appear so that
        Angular's virtual scroller renders ALL contests.
        """
        self._navigate(url)
        self._wait_and_sleep(_PANEL_SEL)
        self._scroll_to_load_all()
        assert self.page is not None
        return self.page.content()

    def get_election_page_with_vote_methods(self, url: str) -> str:
        """Render an election page with all vote-method breakdowns expanded."""
        self._navigate(url)
        self._wait_and_sleep(_PANEL_SEL)
        self._scroll_to_load_all()
        self._click_all_vote_method_buttons()
        assert self.page is not None
        return self.page.content()

    def get_county_page(self, url: str) -> str:
        """Render a per-county election results page and return the HTML.

        Timeout waiting for ballot-item panels is silenced because some
        counties legitimately have no contests for a given election.
        """
        self._navigate(url)
        self._wait_and_sleep(_PANEL_SEL, warn_on_timeout=False)
        self._scroll_to_load_all()
        assert self.page is not None
        return self.page.content()

    def get_county_page_with_vote_methods(self, url: str) -> str:
        """Render a per-county election page with all vote-method breakdowns."""
        self._navigate(url)
        self._wait_and_sleep(_PANEL_SEL, warn_on_timeout=False)
        self._scroll_to_load_all()
        self._click_all_vote_method_buttons()
        assert self.page is not None
        return self.page.content()

    def get_county_page_with_precinct_links(self, url: str) -> str:
        """Render a county election page and return its HTML for precinct link extraction.

        The "View results by precinct" entries are plain ``<a>`` tags already
        present in the county page HTML (one per contest/ballot item), so no
        extra interaction is required beyond a normal page render.
        """
        return self.get_county_page(url)

    def get_precinct_page(self, url: str) -> str:
        """Render a per-precinct election results page and return the HTML."""
        self._navigate(url)
        self._wait_and_sleep(_PANEL_SEL, warn_on_timeout=False)
        self._scroll_to_load_all()
        assert self.page is not None
        return self.page.content()

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _scroll_to_load_all(
        self,
        panel_sel: str = _PANEL_SEL,
        max_rounds: int = 30,
        settle_s: float = 1.5,
    ) -> None:
        """Scroll to the bottom repeatedly until no new ballot-item panels appear."""
        assert self.page is not None
        prev_count = -1
        for _ in range(max_rounds):
            count = len(self.page.query_selector_all(panel_sel))
            if count == prev_count:
                break
            prev_count = count
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(settle_s)
        self.page.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.5)

    def _click_all_vote_method_buttons(self, settle_s: float = 3.0) -> None:
        """Click every per-panel Vote Method toggle and wait for re-renders."""
        assert self.page is not None
        panels = self.page.query_selector_all(_PANEL_SEL)
        if not panels:
            return
        btns = self.page.query_selector_all(_VOTE_METHOD_BTN_SEL)
        if not btns:
            print(f"{self.log_prefix} WARNING: No vote-method buttons found; HTML unchanged.")
            return
        prefix = f"{self.log_prefix} " if self.log_prefix else ""
        print(f"{prefix}  Expanding vote-method breakdowns for {len(btns)} contest(s)...")
        for btn in btns:
            btn.click()
        time.sleep(settle_s)
