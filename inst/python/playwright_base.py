"""
Shared base Playwright client used by all browser-based scrapers in DownBallotR.

Subclasses add site-specific navigation methods on top of the shared browser
lifecycle (launch, stealth context, retry-goto, selector wait).

Current subclasses
------------------
- ElectionStats.playwright_client.PlaywrightClient  (SC, NM, NY)
- Georgia.client.GaPlaywrightClient                 (GA SOS)
"""

from __future__ import annotations

import time
from typing import Optional

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Page,
    Browser,
    BrowserContext,
    Playwright,
)

_STEALTH_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

_SELECTOR_TIMEOUT_MS = 45_000


class BasePlaywrightClient:
    """Shared headless browser infrastructure for DownBallotR scrapers.

    Handles browser launch with stealth / anti-bot settings, context
    manager lifecycle, and common navigation helpers.  Subclasses add
    site-specific navigation methods.

    Parameters
    ----------
    headless : bool
        Run browser in headless mode (default True).  Set False for debugging.
    sleep_s : float
        Seconds to wait after a page load to allow JS rendering to settle.
    """

    def __init__(self, headless: bool = True, sleep_s: float = 2.0):
        self.headless = headless
        self.sleep_s = sleep_s
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    # ── Context manager ────────────────────────────────────────────────────────

    def __enter__(self):
        """Launch browser with stealth settings to bypass bot detection."""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        self.context = self.browser.new_context(
            user_agent=_STEALTH_UA,
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            timezone_id="America/New_York",
        )
        self.context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        self.page = self.context.new_page()
        return self

    def __exit__(self, *args) -> None:
        """Close browser and release all Playwright resources."""
        if self.page:
            self.page.close()
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    # ── Navigation helpers ─────────────────────────────────────────────────────

    def _navigate(self, url: str) -> None:
        """Navigate to *url*, retrying up to 3 times on timeout.

        Parameters
        ----------
        url : str
            Full URL to navigate to.

        Raises
        ------
        PlaywrightTimeoutError
            If all 3 attempts time out.
        RuntimeError
            If called outside a context manager (browser not started).
        """
        if self.page is None:
            raise RuntimeError(
                f"Browser not started. Use the client as a context manager: "
                f"'with {type(self).__name__}() as client:'"
            )
        for attempt in range(1, 4):
            try:
                self.page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                self._wait_for_cloudflare()
                return
            except PlaywrightTimeoutError:
                if attempt == 3:
                    raise
                print(f"  [WARN] goto timeout for {url!r}, retry {attempt}/3")
                time.sleep(5 * attempt)

    def _wait_for_cloudflare(self, timeout_ms: int = 15_000) -> None:
        """Wait for a Cloudflare challenge to resolve if one is present.

        Cloudflare's JS challenge temporarily serves a "Just a moment..." page
        while the browser solves a proof-of-work puzzle.  This method detects
        that page by title and waits until the real page loads.

        Called automatically by ``_navigate``; subclasses rarely need to invoke
        it directly.

        Parameters
        ----------
        timeout_ms : int
            Maximum milliseconds to wait for the challenge to clear (default
            15 000).  A warning is printed if the challenge does not resolve in
            time, but execution continues so callers can inspect whatever loaded.
        """
        assert self.page is not None
        try:
            title = self.page.title()
        except Exception:
            return  # can't read title — proceed

        if "just a moment" not in title.lower():
            return  # no challenge present

        print(
            f"  [CF] Cloudflare challenge detected — waiting up to "
            f"{timeout_ms / 1000:.0f}s for it to resolve..."
        )
        try:
            self.page.wait_for_function(
                "() => document.title.toLowerCase().indexOf('just a moment') === -1",
                timeout=timeout_ms,
            )
            print("  [CF] Challenge resolved.")
        except PlaywrightTimeoutError:
            print(
                "  [CF] WARNING: Challenge did not resolve in time — "
                "proceeding with current page content."
            )

    def _wait_and_sleep(
        self,
        selector: str,
        timeout_ms: int = _SELECTOR_TIMEOUT_MS,
        warn_on_timeout: bool = True,
    ) -> None:
        """Wait for *selector* to appear, then sleep to let JS settle.

        On selector timeout execution continues so the caller can inspect
        whatever HTML loaded.

        Parameters
        ----------
        selector : str
            CSS or XPath selector to wait for.
        timeout_ms : int
            Milliseconds before giving up on the selector (default 45 000).
        warn_on_timeout : bool
            When True (default) print a ``[WARN]`` line on timeout.  Pass
            False for callers where an empty page is an expected outcome (e.g.
            county pages with no contests) so that routine empty-county loads
            don't flood the log.
        """
        assert self.page is not None
        try:
            self.page.wait_for_selector(selector, timeout=timeout_ms)
        except PlaywrightTimeoutError:
            if warn_on_timeout:
                print(f"  [WARN] Timed out waiting for selector: {selector!r}")
        if self.sleep_s:
            time.sleep(self.sleep_s)
