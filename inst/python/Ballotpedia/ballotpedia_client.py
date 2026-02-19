"""
Ballotpedia search client.

Uses Ballotpedia's MediaWiki search endpoint (Special:Search) to query
the site and return structured results, mirroring the patterns used in
the ElectionStats scrapers elsewhere in this project.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Iterable, List, Optional
from urllib.parse import urlencode

import requests
from lxml import html


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE_URL = "https://ballotpedia.org"
_SEARCH_PATH = "/wiki/index.php"
_DEFAULT_USER_AGENT = "DownBallotR (+https://github.com/gchickering21/DownBallotR)"
_DEFAULT_LIMIT = 20  # results per page (MediaWiki default)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class BallotpediaSearchResult:
    """A single result returned by the Ballotpedia search bar.

    Attributes
    ----------
    title : str
        Article title as it appears on Ballotpedia.
    url : str
        Absolute URL to the article (e.g. https://ballotpedia.org/Joe_Smith).
    snippet : str
        Short text excerpt highlighting why the article matched the query.
    metadata : str
        Size / last-updated string shown beneath each result (may be empty).
    """

    title: str
    url: str
    snippet: str = ""
    metadata: str = ""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class BallotpediaClient:
    """HTTP client for querying the Ballotpedia search bar.

    Submits queries to Ballotpedia's ``Special:Search`` endpoint and parses
    the resulting MediaWiki HTML into :class:`BallotpediaSearchResult` objects.

    Parameters
    ----------
    sleep_s : float, optional
        Polite delay (seconds) between consecutive HTTP requests (default: 1.0).
    timeout_s : int, optional
        Per-request timeout in seconds (default: 30).
    user_agent : str, optional
        HTTP ``User-Agent`` header sent with every request.

    Examples
    --------
    >>> client = BallotpediaClient()
    >>> results = client.search("mayor Chicago")
    >>> for r in results:
    ...     print(r.title, r.url)

    >>> # Fetch multiple pages
    >>> all_results = client.search_all("school board election", max_pages=5)
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
    # URL helpers
    # ------------------------------------------------------------------

    def build_search_url(
        self,
        query: str,
        limit: int = _DEFAULT_LIMIT,
        offset: int = 0,
    ) -> str:
        """Return a fully-formed Ballotpedia search URL.

        Parameters
        ----------
        query : str
            The search term(s) to look up.
        limit : int, optional
            Number of results per page (default: 20; max: 500).
        offset : int, optional
            Zero-based result offset for pagination (default: 0).

        Returns
        -------
        str
            Absolute URL ready to be fetched.
        """
        params = urlencode(
            {
                "title": "Special:Search",
                "search": query,
                "profile": "advanced",
                "fulltext": "1",
                "limit": limit,
                "offset": offset,
            }
        )
        return f"{_BASE_URL}{_SEARCH_PATH}?{params}"

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _get_html(self, url: str) -> str:
        """Fetch *url* and return the response body as a string."""
        resp = self.session.get(url, timeout=self.timeout_s)
        resp.raise_for_status()
        if self.sleep_s:
            time.sleep(self.sleep_s)
        return resp.text

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean(node) -> str:
        """Return whitespace-normalised text content of an lxml node."""
        try:
            import re
            return re.sub(r"\s+", " ", node.text_content() or "").strip()
        except Exception:
            return ""

    def _parse_results(self, page_html: str) -> List[BallotpediaSearchResult]:
        """Parse a Ballotpedia Special:Search HTML page.

        MediaWiki search results live in::

            <ul class="mw-search-results">
              <li>
                <div class="mw-search-result-heading">
                  <a href="/Article_Title">Article Title</a>
                </div>
                <div class="searchresult">…snippet…</div>
                <div class="mw-search-result-data">3 KB – …</div>
              </li>
              …
            </ul>

        Parameters
        ----------
        page_html : str
            Raw HTML returned by the search endpoint.

        Returns
        -------
        List[BallotpediaSearchResult]
        """
        doc = html.fromstring(page_html)
        items = doc.xpath("//ul[contains(@class,'mw-search-results')]/li")

        results: List[BallotpediaSearchResult] = []
        for li in items:
            # Title + link
            heading_links = li.xpath(
                ".//div[contains(@class,'mw-search-result-heading')]//a"
            )
            if not heading_links:
                continue
            anchor = heading_links[0]
            title = self._clean(anchor)
            href = anchor.get("href", "")
            url = href if href.startswith("http") else f"{_BASE_URL}{href}"

            # Snippet
            snippet_nodes = li.xpath(".//div[contains(@class,'searchresult')]")
            snippet = self._clean(snippet_nodes[0]) if snippet_nodes else ""

            # Metadata (size / date line)
            meta_nodes = li.xpath(
                ".//div[contains(@class,'mw-search-result-data')]"
            )
            metadata = self._clean(meta_nodes[0]) if meta_nodes else ""

            results.append(
                BallotpediaSearchResult(
                    title=title,
                    url=url,
                    snippet=snippet,
                    metadata=metadata,
                )
            )

        return results

    # ------------------------------------------------------------------
    # Public search API
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        limit: int = _DEFAULT_LIMIT,
        offset: int = 0,
    ) -> List[BallotpediaSearchResult]:
        """Fetch one page of Ballotpedia search results.

        Parameters
        ----------
        query : str
            The search term(s) to submit to the Ballotpedia search bar.
        limit : int, optional
            Results per page (default: 20).
        offset : int, optional
            Zero-based starting position for pagination (default: 0).

        Returns
        -------
        List[BallotpediaSearchResult]
            Parsed results for this page (empty list if none found).
        """
        url = self.build_search_url(query, limit=limit, offset=offset)
        page_html = self._get_html(url)
        return self._parse_results(page_html)

    def iter_search(
        self,
        query: str,
        limit: int = _DEFAULT_LIMIT,
        max_pages: int = 10,
    ) -> Iterable[BallotpediaSearchResult]:
        """Iterate over multiple pages of search results, yielding each result.

        Parameters
        ----------
        query : str
            The search term(s) to submit.
        limit : int, optional
            Results per page (default: 20).
        max_pages : int, optional
            Maximum number of pages to fetch (default: 10).

        Yields
        ------
        BallotpediaSearchResult
        """
        for page in range(max_pages):
            offset = page * limit
            results = self.search(query, limit=limit, offset=offset)
            if not results:
                break
            yield from results

    def search_all(
        self,
        query: str,
        limit: int = _DEFAULT_LIMIT,
        max_pages: int = 10,
    ) -> List[BallotpediaSearchResult]:
        """Collect all paginated results into a single list.

        Parameters
        ----------
        query : str
            The search term(s) to submit.
        limit : int, optional
            Results per page (default: 20).
        max_pages : int, optional
            Maximum number of pages to fetch (default: 10).

        Returns
        -------
        List[BallotpediaSearchResult]
        """
        return list(self.iter_search(query, limit=limit, max_pages=max_pages))
