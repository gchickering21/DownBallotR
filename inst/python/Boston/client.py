"""HTTP client for the City of Boston elections page.

The boston.gov elections page is a static Drupal site — no JavaScript
rendering is required.  A plain requests.Session is sufficient.

All PDF downloads are also plain HTTP GETs.  The client uses the shared
DownBallotR User-Agent and fetch_with_retry for transient-error resilience.
"""

from __future__ import annotations

import requests

from http_utils import fetch_with_retry, DOWNBALLOT_UA

BOSTON_BASE_URL = "https://www.boston.gov"
BOSTON_LANDING_URL = (
    f"{BOSTON_BASE_URL}/departments/elections/"
    "state-and-city-boston-election-results"
)


class BostonHttpClient:
    """Requests-based client for the City of Boston elections page.

    Parameters
    ----------
    session : requests.Session | None
        Optional pre-configured session.  A new session is created when None.
    retries : int
        Number of fetch attempts before giving up (default 3).
    backoff_s : float
        Initial retry back-off in seconds, doubled each attempt (default 5.0).
    """

    def __init__(
        self,
        session: "requests.Session | None" = None,
        retries: int = 3,
        backoff_s: float = 5.0,
    ):
        self._session = session or requests.Session()
        self._session.headers["User-Agent"] = DOWNBALLOT_UA
        self._retries = retries
        self._backoff_s = backoff_s

    def _get_text(self, url: str) -> str:
        def _fetch(u: str) -> str:
            r = self._session.get(u, timeout=30)
            r.raise_for_status()
            return r.text

        return fetch_with_retry(_fetch, url, retries=self._retries, backoff_s=self._backoff_s)

    def _get_bytes(self, url: str) -> bytes:
        def _fetch(u: str) -> bytes:
            r = self._session.get(u, timeout=60)
            r.raise_for_status()
            return r.content

        return fetch_with_retry(_fetch, url, retries=self._retries, backoff_s=self._backoff_s)

    def get_landing_page(self) -> str:
        """Fetch and return the HTML of the Boston elections landing page."""
        return self._get_text(BOSTON_LANDING_URL)

    def get_pdf(self, pdf_url: str) -> bytes:
        """Fetch and return the raw bytes of a PDF result file."""
        return self._get_bytes(pdf_url)
