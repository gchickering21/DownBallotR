"""
HTTP client for the Indiana FirstTuesday election archive.

All data is served as flat JSON files under:
  {archive_base_url}/data/settings.json
  {archive_base_url}/data/statewideElectionsC_A.json
  {archive_base_url}/data/OffCatC_{id}_A.json

No browser automation is needed.
"""

from __future__ import annotations

import re
import time
from typing import Any

import requests

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
}
_TIMEOUT = 30
_MAX_RETRIES = 3


def _get_json(url: str) -> Any:
    """Fetch *url* and return parsed JSON, retrying up to 3 times on failure."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            if attempt == _MAX_RETRIES:
                raise
            print(f"  [WARN] GET failed (attempt {attempt}): {type(exc).__name__} — retrying...")
            time.sleep(2 * attempt)


class InElectionClient:
    """Thin HTTP client for Indiana archive JSON endpoints.

    Parameters
    ----------
    archive_base_url : str
        Base URL for one election archive, e.g.
        ``"https://enr.indianavoters.in.gov/archive/2020General"``.
    """

    def __init__(self, archive_base_url: str):
        self.base = archive_base_url.rstrip("/")

    def get_settings(self) -> dict:
        """Fetch settings.json (election date, certification status, etc.)."""
        return _get_json(f"{self.base}/data/settings.json")

    def get_office_categories(self) -> dict:
        """Fetch statewideElectionsC_A.json (list of office category IDs)."""
        return _get_json(f"{self.base}/data/statewideElectionsC_A.json")

    def get_office_category(self, category_id: str) -> dict:
        """Fetch OffCatC_{category_id}_A.json (race results + county breakdowns)."""
        if not re.match(r'^[A-Za-z0-9_]+$', category_id):
            raise ValueError(f"Invalid category_id {category_id!r}: must be alphanumeric/underscore only.")
        return _get_json(f"{self.base}/data/OffCatC_{category_id}_A.json")
