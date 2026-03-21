"""
Playwright-based browser automation client for v2 ElectionStats states (SC, NM, NY).

These states use React/Material-UI apps with client-side JavaScript rendering,
requiring browser automation instead of simple HTTP requests.

NY additionally sits behind Cloudflare bot protection, so a realistic browser
context (user-agent, no-automation flags, webdriver patch) is used for all
v2 states to ensure compatibility.

Inherits browser lifecycle from BasePlaywrightClient (playwright_base.py).
"""

from __future__ import annotations

import time
from typing import Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from playwright_base import BasePlaywrightClient


class PlaywrightClient(BasePlaywrightClient):
    """Browser automation client for v2 states (SC, NM, NY) that use React/JS.

    Launches a headless browser to render JavaScript and extract election data
    from dynamically-loaded React applications.  Uses realistic browser
    fingerprinting to bypass Cloudflare bot protection (required for NY).

    Parameters
    ----------
    state_key : str
        State identifier (e.g., 'south_carolina', 'new_mexico', 'new_york')
    base_url : str
        Base URL for the state's ElectionStats site
    headless : bool, optional
        Whether to run browser in headless mode (default: True)
    sleep_s : float, optional
        Sleep duration after page loads to ensure content stability (default: 2.0)

    Examples
    --------
    >>> with PlaywrightClient("south_carolina", "https://electionhistory.scvotes.gov") as client:
    ...     html = client.get_search_page(2024, 2024)
    ...     # Process rendered HTML...
    """

    def __init__(
        self,
        state_key: str,
        base_url: str,
        headless: bool = True,
        sleep_s: float = 2.0,
    ):
        super().__init__(headless=headless, sleep_s=sleep_s)
        self.state_key = state_key
        self.base_url = base_url.rstrip("/")

    def get_search_page(
        self, year_from: Optional[int] = None, year_to: Optional[int] = None
    ) -> str:
        """Navigate to search page, wait for table to load, return rendered HTML.

        URL patterns for SC/NM/NY:
        - Single year: /search?df=2009&dt=2009&t=table
        - From year to present: /search?df=2024&t=table (no dt)
        - From beginning to year: /search?dt=2000&t=table (no df)
        - All years: /search?t=table (no df or dt)

        Parameters
        ----------
        year_from : int, optional
            Start year for date range (df parameter). If None, search from beginning.
        year_to : int, optional
            End year for date range (dt parameter). If None, search to present.

        Returns
        -------
        str
            Rendered HTML content after JavaScript execution

        Raises
        ------
        TimeoutError
            If results table doesn't appear within 45 seconds
        """
        if self.page is None:
            raise RuntimeError("Browser not initialized. Use context manager (with statement).")

        # Build URL with optional year parameters
        params = ["t=table"]
        if year_from is not None:
            params.append(f"df={year_from}")
        if year_to is not None:
            params.append(f"dt={year_to}")

        url = f"{self.base_url}/search?{'&'.join(params)}"
        self._navigate(url)

        # Wait for either a results table or a "No Results Found" message.
        # This prevents a 45-second timeout on years with no elections.
        try:
            self.page.wait_for_selector(
                "table#contestCollectionTable, table.MuiTable-root, "
                "span.MuiTypography-root.MuiTypography-body1",
                timeout=45000,
            )
        except PlaywrightTimeoutError:
            raise

        # If the page loaded a "No Results Found" span rather than a table, return early.
        no_results = self.page.locator(
            "span.MuiTypography-root.MuiTypography-body1"
        ).filter(has_text="No Results Found")
        if no_results.count() > 0:
            return self.page.content()

        if self.sleep_s:
            time.sleep(self.sleep_s)

        return self.page.content()

    def get_detail_page(self, election_id: int) -> str:
        """Navigate to election detail page and return HTML after JS loads.

        Parameters
        ----------
        election_id : int
            Election ID to fetch detailed results for

        Returns
        -------
        str
            Rendered HTML content with county/locality vote breakdowns

        Raises
        ------
        TimeoutError
            If detail page table doesn't appear within 45 seconds

        Notes
        -----
        V2 states (SC/NM/NY) use /contest/{election_id} URL pattern.
        """
        if self.page is None:
            raise RuntimeError("Browser not initialized. Use context manager (with statement).")

        url = f"{self.base_url}/contest/{election_id}"
        self._navigate(url)

        self._wait_and_sleep(
            "#content table, table#contestCollectionTable, table.MuiTable-root"
        )

        return self.page.content()
