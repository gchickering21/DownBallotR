"""
Stage 1: Discover Indiana General Election archive URLs.

Fetches the Indiana SOS election results landing page and extracts all links
that point to ``enr.indianavoters.in.gov/archive/{YEAR}General/`` for years
within the requested range.

For elections more recent than what the landing page lists (i.e. the current
or most recently completed election cycle), the archive URL is constructed
directly from the known pattern rather than relying on the landing page.
"""

from __future__ import annotations

import re
from datetime import date

import requests
from bs4 import BeautifulSoup

from http_utils import DOWNBALLOT_UA
from .client import InElectionClient
from .models import InElectionInfo

_SOS_LANDING_URL = (
    "https://www.in.gov/sos/elections/election-commission/election-results/"
)
_ARCHIVE_BASE = "https://enr.indianavoters.in.gov/archive"

# Pattern: enr.indianavoters.in.gov/archive/2020General/index.html
_ARCHIVE_RE = re.compile(
    r"enr\.indianavoters\.in\.gov/archive/(\d{4})General/",
    re.IGNORECASE,
)


def _fetch_html(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": DOWNBALLOT_UA}, timeout=30)
    resp.raise_for_status()
    return resp.text


def _years_from_landing_page(html: str) -> set[int]:
    """Extract all General Election years linked from the SOS landing page."""
    soup = BeautifulSoup(html, "html.parser")
    years: set[int] = set()
    for a in soup.find_all("a", href=True):
        m = _ARCHIVE_RE.search(a["href"])
        if m:
            years.add(int(m.group(1)))
    return years


def _get_election_info(year: int) -> InElectionInfo:
    """Fetch settings.json for *year* and return an InElectionInfo."""
    slug = f"{year}General"
    base = f"{_ARCHIVE_BASE}/{slug}"
    client = InElectionClient(base)
    settings = client.get_settings()
    root = settings["Root"]
    return InElectionInfo(
        year=year,
        archive_slug=slug,
        archive_base_url=base,
        election_date=root.get("CurrentElection") or None,
        certified=root.get("Certified", "") == "T",
    )


def discover_general_elections(
    year_from: int | None = None,
    year_to: int | None = None,
) -> list[InElectionInfo]:
    """Return InElectionInfo objects for Indiana General Elections in range.

    Strategy:
    1. Fetch the SOS landing page to find all years that are explicitly linked.
    2. Fill in any years in [year_from, year_to] that aren't on the landing
       page by probing the archive URL directly (handles elections newer than
       the last landing-page update).
    3. Filter to [year_from, year_to].

    Parameters
    ----------
    year_from : int | None
        Earliest year to include (inclusive).  Default: 2020.
    year_to : int | None
        Latest year to include (inclusive).  Default: current calendar year.
    """
    if year_from is None:
        year_from = 2020
    if year_to is None:
        year_to = date.today().year

    print(f"[IN] Discovering General Elections {year_from}–{year_to}...")

    # Step 1: years explicitly listed on the landing page
    try:
        html = _fetch_html(_SOS_LANDING_URL)
        listed_years = _years_from_landing_page(html)
    except Exception as exc:
        print(f"[IN] WARNING: could not fetch landing page: {exc} — probing directly")
        listed_years = set()

    candidate_years = set(range(year_from, year_to + 1))

    # Step 2: probe years not on the landing page
    confirmed_years: set[int] = set()
    for year in candidate_years:
        if year in listed_years:
            confirmed_years.add(year)
        else:
            # Probe: try to fetch settings.json for this year
            try:
                client = InElectionClient(f"{_ARCHIVE_BASE}/{year}General")
                client.get_settings()
                confirmed_years.add(year)
                print(f"[IN]   {year}General confirmed via direct probe.")
            except Exception:
                pass  # archive doesn't exist for this year

    elections: list[InElectionInfo] = []
    for year in sorted(confirmed_years):
        try:
            info = _get_election_info(year)
            elections.append(info)
            cert = "certified" if info.certified else "not certified"
            print(f"[IN]   Found: {year}General  ({info.election_date}, {cert})")
        except Exception as exc:
            print(f"[IN]   WARNING: could not load settings for {year}General: {exc}")

    print(f"[IN] Discovered {len(elections)} General Election(s).")
    return elections
