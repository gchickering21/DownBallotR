"""
Playwright-based browser automation client for v2 ElectionStats states (SC, NM).

These states use React/Material-UI apps with client-side JavaScript rendering,
requiring browser automation instead of simple HTTP requests.
"""

from __future__ import annotations
import time
from typing import Optional

from playwright.sync_api import sync_playwright, Page, Browser, Playwright


class PlaywrightClient:
    """Browser automation client for v2 states (SC, NM) that use React/JS.

    Launches a headless browser to render JavaScript and extract election data
    from dynamically-loaded React applications.

    Parameters
    ----------
    state_key : str
        State identifier (e.g., 'south_carolina', 'new_mexico')
    base_url : str
        Base URL for the state's ElectionStats site
    headless : bool, optional
        Whether to run browser in headless mode (default: True)
    sleep_s : float, optional
        Sleep duration after page loads to ensure content stability (default: 0.5)

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
        sleep_s: float = 0.5,
    ):
        self.state_key = state_key
        self.base_url = base_url.rstrip("/")
        self.headless = headless
        self.sleep_s = sleep_s
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None

    def __enter__(self):
        """Context manager entry - launch browser."""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        self.page = self.browser.new_page()
        return self

    def __exit__(self, *args):
        """Context manager exit - close browser and clean up resources."""
        if self.page:
            self.page.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    def get_search_page(
        self, year_from: Optional[int] = None, year_to: Optional[int] = None
    ) -> str:
        """Navigate to search page, wait for table to load, return rendered HTML.

        URL patterns for SC/NM:
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
            If results table doesn't appear within 30 seconds
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
        self.page.goto(url, wait_until="networkidle")

        # Wait for the results table to appear (React rendering)
        # Try multiple possible selectors to handle different table structures
        self.page.wait_for_selector(
            "table#search_results_table, table[role='table'], table.MuiTable-root",
            timeout=30000,
        )

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
            If detail page table doesn't appear within 30 seconds

        Notes
        -----
        V2 states (SC/NM) use /contest/{election_id} URL pattern.
        """
        if self.page is None:
            raise RuntimeError("Browser not initialized. Use context manager (with statement).")

        # V2 states use /contest/ID pattern
        url = f"{self.base_url}/contest/{election_id}"
        self.page.goto(url, wait_until="networkidle")

        # Wait for table to load (use a more flexible selector)
        try:
            self.page.wait_for_selector("table", timeout=30000)
        except Exception:
            # Some pages may not have tables or may load differently
            pass

        if self.sleep_s:
            time.sleep(self.sleep_s)

        return self.page.content()
