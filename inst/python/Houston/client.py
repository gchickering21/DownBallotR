"""HTTP client for the Harris County (harrisvotes.com) elections archive.

The harrisvotes.com site is a DNN (DotNetNuke) CMS — no JavaScript rendering
required.  A plain requests.Session is sufficient.

Canvass PDFs can be very large (40–50 MB), so the PDF fetch uses a 120-second
timeout.
"""

from __future__ import annotations

import requests

from http_utils import fetch_with_retry, DOWNBALLOT_UA

HARRIS_BASE_URL = "https://www.harrisvotes.com"
HARRIS_ARCHIVE_URL = f"{HARRIS_BASE_URL}/Election-Results/Archives"


class HoustonHttpClient:
    """Requests-based HTTP client for harrisvotes.com.

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
            r = self._session.get(u, timeout=120)
            r.raise_for_status()
            return r.content
        return fetch_with_retry(_fetch, url, retries=self._retries, backoff_s=self._backoff_s)

    def get_archive_page(self, page_num: int = 1) -> str:
        """Fetch and return the HTML of one archive listing page."""
        if page_num <= 1:
            return self._get_text(HARRIS_ARCHIVE_URL)
        return self._get_text(f"{HARRIS_ARCHIVE_URL}/page/{page_num}")

    def get_pdf(self, pdf_url: str) -> bytes:
        """Fetch and return the raw bytes of a PDF result file."""
        return self._get_bytes(pdf_url)
