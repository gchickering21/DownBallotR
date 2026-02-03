from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import requests


@dataclass(frozen=True)
class HttpConfig:
    timeout_s: int = 60
    sleep_s: float = 0.0  # polite delay between requests
    user_agent: str = "DownBallotR (+https://github.com/gchickering21/DownBallotR)"


class VaHttpClient:
    BASE = "https://historical.elections.virginia.gov"

    def __init__(self, config: Optional[HttpConfig] = None) -> None:
        self.config = config or HttpConfig()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.config.user_agent,
                "Accept": "text/html,*/*",
            }
        )

    def get_html(self, url: str) -> str:
        """GET a URL and return HTML text (raises on HTTP errors)."""
        resp = self.session.get(url, timeout=self.config.timeout_s)
        resp.raise_for_status()
        if self.config.sleep_s:
            time.sleep(self.config.sleep_s)
        return resp.text

    def build_search_url(self, year_from: int, year_to: int, page: int = 1) -> str:
        # Site uses path filters like /year_from:1789/year_to:2025/page:1
        return f"{self.BASE}/elections/search/year_from:{year_from}/year_to:{year_to}/page:{page}"
