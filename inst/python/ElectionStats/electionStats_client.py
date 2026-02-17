from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Optional
import requests
from urllib.parse import urlencode

@dataclass(frozen=True)
class HttpConfig:
    timeout_s: int = 60
    sleep_s: float = 0.0  # polite delay between requests
    user_agent: str = "DownBallotR (+https://github.com/gchickering21/DownBallotR)"

class BaseHttpClient:
    def __init__(
        self,
        base_url: str,
        config: Optional[HttpConfig] = None,
    ) -> None:
        self.base_url = base_url
        self.config = config or HttpConfig()

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.config.user_agent,
                "Accept": "text/html,*/*",
            }
        )

    def get_html(self, url: str) -> str:
        resp = self.session.get(url, timeout=self.config.timeout_s)
        resp.raise_for_status()
        if self.config.sleep_s:
            time.sleep(self.config.sleep_s)
        return resp.text

    def build_search_url(
        self,
        year_from: int,
        year_to: int,
        page: int = 1,
    ) -> str:
        return (
            f"{self.base_url}/elections/search/"
            f"year_from:{year_from}/year_to:{year_to}"
        )

@dataclass
class StateHttpClient:
    state: str
    base_url: str
    config: "HttpConfig"
    search_path: str = "/search"   # default for VA, MA, etc.

    # ---------------------------
    # URL Builders
    # ---------------------------
    def _normalize_base(self) -> str:
        return self.base_url.rstrip("/")

    def _normalize_path(self) -> str:
        if not self.search_path:
            return ""
        path = self.search_path.strip()
        if not path.startswith("/"):
            path = "/" + path
        return path

    def build_search_url(
        self,
        year_from: int,
        year_to: int,
        page: int = 1,
    ) -> str:
        """
        Builds a search URL.
        If search_path == "", the base URL is used directly (Colorado case).
        """
        base = self._normalize_base()
        path = self._normalize_path()

        url = f"{base}{path}" if path else base

        query = urlencode(
            {
                "year_from": year_from,
                "year_to": year_to,
                "page": page,
            }
        )

        return f"{url}?{query}"

    def build_detail_url(self, election_id: int) -> str:
        """
        Builds detail page URL.
        Adjust here later if a state uses a different detail pattern.
        """
        base = self._normalize_base()
        detail_url = f"{base}/view/{election_id}/"
        return detail_url

    # ---------------------------
    # HTTP Fetch
    # ---------------------------
    def get_html(self, url: str) -> str:
        """
        Fetch HTML from a URL using this client's timeout and sleep config.
        """
        resp = requests.get(url, timeout=self.config.timeout_s)
        resp.raise_for_status()

        if self.config.sleep_s:
            time.sleep(self.config.sleep_s)

        return resp.text

