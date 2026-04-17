"""
Playwright-based browser automation client for ElectionStats states that require
browser rendering (SC, NM, NY, VA, ID).

SC/NM/NY/VA use React/Material-UI apps with client-side JavaScript rendering.
ID (Idaho) uses a classic v1 table layout but the search page is AJAX-rendered
via DataTables, so Playwright is needed to capture the populated table HTML.
County detail pages for Idaho are server-rendered and fetched separately via requests.

NY additionally sits behind Cloudflare bot protection, so a realistic browser
context (user-agent, no-automation flags, webdriver patch) is used for all
Playwright states to ensure compatibility.

Inherits browser lifecycle from BasePlaywrightClient (playwright_base.py).
"""

from __future__ import annotations

import time
from typing import Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from playwright_base import BasePlaywrightClient
from ElectionStats.state_config import get_state_config


class PlaywrightClient(BasePlaywrightClient):
    """Browser automation client for states that need JS rendering.

    Supports both v2 states (SC, NM, NY, VA — React apps) and classic states that
    use AJAX-rendered tables (Idaho — DataTables on a classic v1 layout).

    Parameters
    ----------
    state_key : str
        State identifier (e.g., 'south_carolina', 'idaho')
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
        config = get_state_config(state_key)
        self._url_style = config.get("url_style", "path_params")
        self._search_path = config.get("search_path", "/search").lstrip("/")

    def _build_search_url(
        self, year_from: Optional[int], year_to: Optional[int]
    ) -> str:
        """Build the search URL for this state.

        Classic/path-params states (e.g. Idaho):
            {base_url}/search/year_from:{year_from}/year_to:{year_to}

        v2/query-params states (SC, NM, NY, VA):
            {base_url}/search?df={year_from}&dt={year_to}&t=table
        """
        search_base = f"{self.base_url}/{self._search_path}".rstrip("/")

        if self._url_style == "path_params":
            # Classic path-params style (same as requests-based states like NH/VA).
            # Build the full path regardless of whether years are None.
            yf = year_from if year_from is not None else 1789
            yt = year_to if year_to is not None else 9999
            return f"{search_base}/year_from:{yf}/year_to:{yt}"

        # query_params / v2 style (SC, NM, NY, VA)
        params = ["t=table"]
        if year_from is not None:
            params.append(f"df={year_from}")
        if year_to is not None:
            params.append(f"dt={year_to}")
        return f"{search_base}?{'&'.join(params)}"

    def get_search_page(
        self, year_from: Optional[int] = None, year_to: Optional[int] = None
    ) -> str:
        """Navigate to search page, wait for table to load, return rendered HTML.

        Supports two URL styles automatically based on state configuration:

        path_params (Idaho/classic):
            /search/year_from:{year_from}/year_to:{year_to}
            Waits for #search_results_table to be populated.

        query_params (SC/NM/NY/VA):
            /search?df={year_from}&dt={year_to}&t=table
            Waits for #contestCollectionTable or MuiTable.

        Parameters
        ----------
        year_from : int, optional
            Start year. If None, defaults to 1789 (path-params) or omitted (query-params).
        year_to : int, optional
            End year. If None, defaults to 9999 (path-params) or omitted (query-params).

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

        url = self._build_search_url(year_from, year_to)
        self._navigate(url)

        if self._url_style == "path_params":
            # Classic states (e.g. Idaho): wait for the DataTables-managed classic
            # table to be populated.  DataTables fires its draw callback once rows
            # are injected — we wait for the first contest row to appear.
            try:
                self.page.wait_for_selector(
                    "table#search_results_table tbody tr[id]",
                    timeout=45000,
                )
            except PlaywrightTimeoutError:
                # No rows appeared — could be a genuinely empty year.
                pass
        else:
            # v2 states (SC, NM, NY, VA): React/MUI table or "No Results" span.
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

    def get_html(self, url: str) -> str:
        """Fetch a contest detail page by URL, extracting election_id from the path.

        Implements the same interface as StateHttpClient.get_html so this client
        can be passed to the standard county/precinct parsers.

        Expected URL form: {base_url}/contest/{election_id}
        """
        import re
        m = re.search(r"/contest/(\d+)", url)
        if not m:
            raise ValueError(f"Cannot extract election_id from URL: {url}")
        return self.get_detail_page(int(m.group(1)))

    def fetch_csv_text(self, url: str) -> str:
        """Fetch a CSV URL using the browser session's cookies/Cloudflare clearance.

        Uses Playwright's APIRequestContext so the request inherits the browser
        context's cookies (including any Cloudflare clearance tokens), bypassing
        bot-protection that would block a plain ``requests.get()`` call.

        Parameters
        ----------
        url : str
            Full URL to fetch (e.g. the contest CSV download endpoint).

        Returns
        -------
        str
            Response body as text.
        """
        if self.context is None:
            raise RuntimeError("Browser not initialized. Use context manager (with statement).")
        response = self.context.request.get(url)
        return response.text()

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
