"""
Shared helpers for Ballotpedia scrapers.

Provides:
  - Module-level constants (_BASE_URL, _DEFAULT_USER_AGENT)
  - _current_year() convenience function
  - BallotpediaBaseScraper — base class with shared HTTP and parsing helpers
    inherited by SchoolBoardScraper and StateElectionsScraper.
"""

from __future__ import annotations

import datetime
import re
import time
from typing import List, Optional

import requests


# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

_BASE_URL = "https://ballotpedia.org"
_DEFAULT_USER_AGENT = "DownBallotR (+https://github.com/gchickering21/DownBallotR)"


def _current_year() -> int:
    """Return the current calendar year."""
    return datetime.date.today().year


# ---------------------------------------------------------------------------
# Base scraper
# ---------------------------------------------------------------------------

class BallotpediaBaseScraper:
    """Shared HTTP client and parsing utilities for Ballotpedia scrapers.

    Subclasses (SchoolBoardScraper, StateElectionsScraper) inherit the
    session setup, WAF-bypass logic, and common static parsing helpers so
    neither file needs to repeat them.

    Parameters
    ----------
    sleep_s : float, optional
        Polite delay (seconds) between consecutive HTTP requests (default: 1.0).
    timeout_s : int, optional
        Per-request timeout in seconds (default: 30).
    user_agent : str, optional
        HTTP ``User-Agent`` header sent with every request.
    """

    def __init__(
        self,
        sleep_s: float = 1.0,
        timeout_s: int = 30,
        user_agent: str = _DEFAULT_USER_AGENT,
    ) -> None:
        self.sleep_s = sleep_s
        self.timeout_s = timeout_s

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "text/html,*/*",
            }
        )

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _is_waf_challenge(self, resp) -> bool:
        """Return True if Ballotpedia returned an AWS WAF bot-challenge page."""
        return (
            resp.headers.get("x-amzn-waf-action", "") == "challenge"
            or (resp.status_code == 202 and len(resp.text) < 10_000)
        )

    def _sync_cookies_from_playwright(self, pw_context) -> None:
        """Copy cookies and User-Agent from a Playwright context into the requests session.

        cf_clearance is tied to the UA that solved the challenge, so we also
        update the session's User-Agent header to match Playwright's Chromium UA.
        """
        import urllib.parse
        for cookie in pw_context.cookies():
            self.session.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain", "").lstrip("."),
            )
        # Keep UA in sync so Cloudflare accepts the clearance cookie
        ua = pw_context.pages[0].evaluate("() => navigator.userAgent") if pw_context.pages else None
        if ua:
            self.session.headers["User-Agent"] = ua

    def _get_html_playwright(self, url: str) -> Optional[str]:
        """Fetch *url* using a headless Chromium browser (handles WAF challenges).

        After a successful fetch the Cloudflare clearance cookies are synced back
        into the requests session so subsequent requests skip the challenge.
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            print("  WARNING: playwright not installed — cannot bypass WAF challenge")
            return None

        print(f"  [playwright] fetching {url}")
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                context = browser.new_context()
                page = context.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                try:
                    page.wait_for_selector(
                        "div.votebox, table.sortable, table.wikitable, "
                        "div.widget-table-container",
                        timeout=15_000,
                    )
                except Exception:
                    pass  # proceed even if selector times out
                if self.sleep_s:
                    time.sleep(self.sleep_s)
                content = page.content()
                # Sync clearance cookies back so requests can reuse them
                self._sync_cookies_from_playwright(context)
                browser.close()
            return content
        except Exception as exc:
            print(f"  WARNING: playwright failed for {url}: {exc}")
            return None

    def _get_html(self, url: str, _retries: int = 3) -> Optional[str]:
        """Fetch *url* and return the response body, or ``None`` on 404/5xx.

        If Ballotpedia's AWS WAF returns a bot-challenge (202 + JS puzzle),
        automatically retries the same URL via a headless Playwright browser.
        Retries up to *_retries* times on read timeouts with exponential backoff.
        """
        for attempt in range(1, _retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout_s)
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                if attempt < _retries:
                    wait = 2 ** attempt
                    print(f"  WARNING: timeout on {url} (attempt {attempt}/{_retries}) — retrying in {wait}s")
                    time.sleep(wait)
                    continue
                print(f"  WARNING: timeout on {url} after {_retries} attempts — skipping")
                return None
            if resp.status_code == 404:
                return None
            if resp.status_code >= 500:
                if attempt < _retries:
                    wait = 2 ** attempt
                    print(f"  WARNING: {resp.status_code} on {url} (attempt {attempt}/{_retries}) — retrying in {wait}s")
                    time.sleep(wait)
                    continue
                print(f"  WARNING: {resp.status_code} on {url} after {_retries} attempts — skipping")
                return None
            if self._is_waf_challenge(resp):
                print(f"  WAF challenge on {url} — retrying with Playwright")
                return self._get_html_playwright(url)
            resp.raise_for_status()
            if self.sleep_s:
                time.sleep(self.sleep_s)
            return resp.text
        return None

    # ------------------------------------------------------------------
    # Shared static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean(node) -> str:
        """Whitespace-normalised text content of an lxml element."""
        try:
            return re.sub(r"\s+", " ", node.text_content() or "").strip()
        except Exception:
            return ""

    @staticmethod
    def _infer_election_type(heading: str) -> str:
        """Infer election type from a race heading string.

        Returns one of: ``"Primary Runoff"``, ``"Primary"``,
        ``"General (RCV)"``, ``"General"``, or ``"Other"``.
        """
        h = heading.lower()
        if "primary runoff" in h or "primary run-off" in h:
            return "Primary Runoff"
        if "primary" in h:
            return "Primary"
        if "ranked" in h or "rcv" in h or "round" in h:
            return "General (RCV)"
        if "general" in h:
            return "General"
        return "Other"

    @staticmethod
    def _parse_candidate_cell(td) -> List[tuple]:
        """Parse a candidates ``<td>`` from a wikitable row.

        The cell contains interleaved ``<img>`` (green checkmark = winner),
        ``<a>`` (candidate link), and ``<br/>`` elements.  A checkmark
        immediately preceding a candidate link marks that candidate as the
        winner.  Incumbent status is signalled by ``(i)`` in the tail text
        after the ``<a>``.

        Returns
        -------
        List of ``(name, candidate_url, is_winner, is_incumbent)`` tuples.
        """
        results = []
        pending_winner = False

        for child in td:
            tag = child.tag
            if tag == "img":
                alt = (child.get("alt") or "").lower()
                if "green check mark" in alt or "check" in alt:
                    pending_winner = True
                # Candidate Connection logo — ignore, don't reset flag
            elif tag == "a":
                href = child.get("href", "")
                target = child.get("target", "")
                # Skip Candidate Connection / survey links
                if target == "_blank" or "#Campaign_themes" in href:
                    continue
                name = child.text_content().strip().rstrip("*").strip()
                if not name:
                    continue
                candidate_url = (
                    href if href.startswith("http") else f"{_BASE_URL}{href}"
                )
                tail = child.tail or ""
                is_incumbent = "(i)" in tail
                results.append((name, candidate_url, pending_winner, is_incumbent))
                pending_winner = False
            # <br>, <p>, text nodes — do not affect pending_winner

        return results
