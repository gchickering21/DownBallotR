"""
Ballotpedia school board election scraper.

Iterates year-by-year (default 2013 to present) and state-by-state,
extracting school board election data from pages like:
  https://ballotpedia.org/School_board_elections,_2026

Each year page contains per-state ``sortable`` tables whose caption row
reads "YEAR STATE School Board Elections".  The scraper identifies state
name and columns dynamically from these rows, so it handles both the
pre-2025 and post-2024 Ballotpedia page layouts without branching.

Optionally, each district page can be followed to scrape candidate names,
parties, vote counts, and percentages from Ballotpedia's votebox tables.

Usage
-----
>>> scraper = SchoolBoardScraper()

# District-level metadata only (fast — one request per year-page)
>>> rows = scraper.scrape_year(2024, state="Alabama")

# Full candidate/results for a specific year+state (one request per district)
>>> df = scraper.scrape_with_results_to_dataframe(year=2024, state="Alabama")

# Iterate across years (district metadata only)
>>> for row in scraper.iter_years(start_year=2020, end_year=2024, state="Texas"):
...     print(row.year, row.state, row.district)
"""

from __future__ import annotations

import datetime
import re
import time
from dataclasses import asdict, dataclass
from typing import Iterable, List, Optional

import requests
from lxml import html

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE_URL = "https://ballotpedia.org"
_EARLIEST_YEAR = 2013  # earliest year with a dedicated page
_DEFAULT_USER_AGENT = "DownBallotR (+https://github.com/gchickering21/DownBallotR)"

# Matches "2024 Alabama School Board Elections" → group 1 = "Alabama"
# Year prefix is optional to handle captions like "New Hampshire School Board Elections"
_CAPTION_RE = re.compile(r"^(?:\d{4}\s+)?(.+?)\s+School Board", re.IGNORECASE)


def _current_year() -> int:
    return datetime.date.today().year


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class SchoolBoardElectionRow:
    """District-level metadata from a Ballotpedia school board elections page.

    Attributes
    ----------
    year : int
        Election year (from the page URL).
    state : str
        State name extracted from the table caption.
    district : str
        School district name.
    district_url : str
        Absolute URL to the district's Ballotpedia election page.
    primary : str
        Primary election date string or "-" if not listed.
    primary_runoff : str
        Primary runoff date or "-" if not listed.
    general_election : str
        General election date or "-" if not listed.
    general_runoff : str
        General runoff date or "-" if not listed.
    term_length : str
        Regular board member term length (years), if present.
    seats_up : str
        Number of seats up for election.
    total_board_seats : str
        Total number of board seats.
    enrollment : str
        Student enrollment figure for the most recent reported school year.
    """

    year: int
    state: str
    district: str
    district_url: str
    primary: str = "-"
    primary_runoff: str = "-"
    general_election: str = "-"
    general_runoff: str = "-"
    term_length: str = "-"
    seats_up: str = ""
    total_board_seats: str = ""
    enrollment: str = ""


@dataclass
class SchoolBoardCandidateResult:
    """One candidate row scraped from a district's Ballotpedia election page.

    Combines district-level metadata (carried forward from
    :class:`SchoolBoardElectionRow`) with candidate-level results.

    Attributes
    ----------
    year : int
        Election year.
    state : str
        State name.
    district : str
        School district name.
    district_url : str
        Absolute URL to the district's Ballotpedia page.
    race : str
        Full heading of the individual race, e.g.
        ``"General election for Jefferson County School District, District 1"``.
    election_type : str
        Inferred election type: ``"General"``, ``"Primary"``,
        ``"Primary Runoff"``, or ``"Other"``.
    candidate : str
        Candidate's full name.
    candidate_url : str
        Absolute URL to the candidate's Ballotpedia page (may be empty).
    party : str
        Party abbreviation or name as shown on Ballotpedia (may be empty).
    is_winner : bool
        ``True`` if the candidate is marked as winner (green checkmark or
        votebox winner row).
    is_incumbent : bool
        ``True`` if the candidate is marked as an incumbent with ``(i)``.
    pct : str
        Vote percentage string (e.g. ``"69.4%"``).
    votes : str
        Raw vote count string (e.g. ``"17,259"``).
    primary : str
        Primary date carried from the district row.
    primary_runoff : str
        Primary runoff date carried from the district row.
    general_election : str
        General election date carried from the district row.
    general_runoff : str
        General runoff date carried from the district row.
    term_length : str
        Term length carried from the district row.
    seats_up : str
        Seats up for election carried from the district row.
    total_board_seats : str
        Total board seats carried from the district row.
    enrollment : str
        Student enrollment carried from the district row.
    """

    year: int
    state: str
    district: str
    district_url: str
    race: str
    election_type: str
    candidate: str
    candidate_url: str
    party: str
    is_winner: bool
    is_incumbent: bool
    pct: str
    votes: str
    primary: str = "-"
    primary_runoff: str = "-"
    general_election: str = "-"
    general_runoff: str = "-"
    term_length: str = "-"
    seats_up: str = ""
    total_board_seats: str = ""
    enrollment: str = ""


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class SchoolBoardScraper:
    """Scrape Ballotpedia school board election pages year-by-year, state-by-state.

    Submits plain HTTP GET requests (no JavaScript needed — these are standard
    MediaWiki pages) and parses the HTML with lxml.

    The parser identifies per-state ``sortable`` tables by their caption row
    ("YEAR STATE School Board Elections") and maps columns dynamically from
    the header row, so it handles Ballotpedia layout changes without branching.

    Two scraping modes:

    * **District metadata only** (``scrape_year`` / ``iter_years`` /
      ``scrape_all``) — fast, one request per year-page.
    * **Full candidate results** (``scrape_with_results`` /
      ``scrape_with_results_to_dataframe``) — follows each district URL and
      parses the votebox candidate tables; one additional request per district.

    Parameters
    ----------
    sleep_s : float, optional
        Polite delay (seconds) between consecutive page fetches (default: 1.0).
    timeout_s : int, optional
        Per-request timeout in seconds (default: 30).
    user_agent : str, optional
        HTTP ``User-Agent`` header sent with every request.

    Examples
    --------
    >>> scraper = SchoolBoardScraper()

    >>> # District metadata for one year + state
    >>> rows = scraper.scrape_year(2024, state="Alabama")

    >>> # Full candidate results as a DataFrame
    >>> df = scraper.scrape_with_results_to_dataframe(year=2024, state="Alabama")

    >>> # All years, all states (district metadata)
    >>> df = scraper.scrape_all_to_dataframe(start_year=2020, end_year=2024)
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
    # URL builder
    # ------------------------------------------------------------------

    def build_year_url(self, year: int) -> str:
        """Return the Ballotpedia URL for a given school board election year."""
        return f"{_BASE_URL}/School_board_elections,_{year}"

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _is_waf_challenge(self, resp) -> bool:
        """Return True if Ballotpedia returned an AWS WAF bot-challenge page."""
        return (
            resp.headers.get("x-amzn-waf-action", "") == "challenge"
            or (resp.status_code == 202 and len(resp.text) < 10_000)
        )

    def _get_html_playwright(self, url: str) -> Optional[str]:
        """Fetch *url* using a headless Chromium browser (handles WAF challenges)."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            print("  WARNING: playwright not installed — cannot bypass WAF challenge")
            return None

        print(f"  [playwright] fetching {url}")
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                # Wait for the main content to appear
                try:
                    page.wait_for_selector(
                        "table.sortable, div.votebox, table.wikitable",
                        timeout=15_000,
                    )
                except Exception:
                    pass  # proceed even if selector times out
                if self.sleep_s:
                    time.sleep(self.sleep_s)
                content = page.content()
                browser.close()
            return content
        except Exception as exc:
            print(f"  WARNING: playwright failed for {url}: {exc}")
            return None

    def _get_html(self, url: str) -> Optional[str]:
        """Fetch *url* and return the response body, or ``None`` on 404/5xx.

        If Ballotpedia's AWS WAF returns a bot-challenge (202 + JS puzzle),
        automatically retries the same URL via a headless Playwright browser.
        """
        resp = self.session.get(url, timeout=self.timeout_s)
        if resp.status_code == 404:
            return None
        if resp.status_code >= 500:
            print(f"  WARNING: {resp.status_code} on {url} — skipping")
            return None
        if self._is_waf_challenge(resp):
            print(f"  WAF challenge on {url} — retrying with Playwright")
            return self._get_html_playwright(url)
        resp.raise_for_status()
        if self.sleep_s:
            time.sleep(self.sleep_s)
        return resp.text

    # ------------------------------------------------------------------
    # Text helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean(node) -> str:
        """Whitespace-normalised text content of an lxml element."""
        try:
            return re.sub(r"\s+", " ", node.text_content() or "").strip()
        except Exception:
            return ""

    @staticmethod
    def _cell_text(td) -> str:
        """Cleaned text for a <td>, returning '-' when empty."""
        raw = re.sub(r"\s+", " ", td.text_content() or "").strip()
        return raw if raw else "-"

    # ------------------------------------------------------------------
    # Column-header index resolver
    # ------------------------------------------------------------------

    @staticmethod
    def _col_index(headers: list[str], *keywords: str) -> Optional[int]:
        """Return the index of the first header matching any of *keywords*.

        Matching is case-insensitive substring search.  Returns ``None`` if
        no header matches.

        For "primary" specifically, callers that want to exclude "runoff"
        should call with ``"primary"`` after having already excluded the
        runoff column via a more specific call.
        """
        kws = [k.lower() for k in keywords]
        for i, h in enumerate(headers):
            h_lower = h.lower()
            if any(kw in h_lower for kw in kws):
                return i
        return None

    # ------------------------------------------------------------------
    # Year-page parser (district metadata)
    # ------------------------------------------------------------------

    def _parse_year_page(
        self, page_html: str, year: int
    ) -> List[SchoolBoardElectionRow]:
        """Parse one year's school board elections page into district rows.

        Finds every ``<table class="... sortable ...">`` whose first row
        caption matches "YEAR STATE School Board Elections", then maps
        columns dynamically from the second header row.  Works for all
        Ballotpedia page layouts from 2013 onward.
        """
        doc = html.fromstring(page_html)
        rows: List[SchoolBoardElectionRow] = []

        for table in doc.xpath("//table[contains(@class,'sortable')]"):
            trs = table.xpath(".//tr")
            if len(trs) < 3:
                # Need at least: caption row, header row, one data row
                continue

            # --- Row 0: caption -------------------------------------------
            caption_text = self._clean(trs[0])
            m = _CAPTION_RE.match(caption_text)
            if not m:
                continue
            state_name = m.group(1).strip()

            # --- Row 1: column headers ------------------------------------
            header_cells = trs[1].xpath("./th | ./td")
            headers = [self._clean(h) for h in header_cells]

            # Resolve column indices by header keyword
            # Primary runoff must be resolved before "primary" to avoid overlap
            primary_runoff_idx = self._col_index(headers, "primary runoff", "primary run-off")
            primary_idx = next(
                (i for i, h in enumerate(headers)
                 if "primary" in h.lower()
                 and "runoff" not in h.lower()
                 and "run-off" not in h.lower()),
                None,
            )
            general_idx = self._col_index(headers, "general election")
            general_runoff_idx = self._col_index(headers, "general runoff")
            term_length_idx = self._col_index(headers, "term length")
            seats_up_idx = self._col_index(headers, "seats up")
            total_seats_idx = self._col_index(headers, "total board")
            enrollment_idx = self._col_index(headers, "enrollment")

            # --- Rows 2+: data rows ---------------------------------------
            for tr in trs[2:]:
                tds = tr.xpath("./td")
                if not tds:
                    continue

                # District name and URL (always column 0)
                district_links = tds[0].xpath(".//a")
                if district_links:
                    district = self._clean(district_links[0])
                    href = district_links[0].get("href", "")
                    district_url = (
                        href if href.startswith("http")
                        else f"{_BASE_URL}{href}"
                    )
                else:
                    district = self._cell_text(tds[0])
                    district_url = ""

                def _col(idx: Optional[int]) -> str:
                    if idx is None or idx >= len(tds):
                        return "-"
                    return self._cell_text(tds[idx])

                rows.append(
                    SchoolBoardElectionRow(
                        year=year,
                        state=state_name,
                        district=district,
                        district_url=district_url,
                        primary=_col(primary_idx),
                        primary_runoff=_col(primary_runoff_idx),
                        general_election=_col(general_idx),
                        general_runoff=_col(general_runoff_idx),
                        term_length=_col(term_length_idx),
                        seats_up=_col(seats_up_idx),
                        total_board_seats=_col(total_seats_idx),
                        enrollment=_col(enrollment_idx),
                    )
                )

        return rows

    # ------------------------------------------------------------------
    # District-page parser (candidate / results)
    # ------------------------------------------------------------------

    @staticmethod
    def _infer_election_type(heading: str) -> str:
        """Infer 'General', 'Primary', 'Primary Runoff', or 'Other'."""
        h = heading.lower()
        if "primary runoff" in h or "primary run-off" in h:
            return "Primary Runoff"
        if "primary" in h:
            return "Primary"
        if "general" in h:
            return "General"
        return "Other"

    def _parse_candidate_cell(self, td) -> List[tuple]:
        """Parse a candidates <td> from the Office/Candidates table format.

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

    def _parse_district_page(
        self,
        page_html: str,
        district_row: SchoolBoardElectionRow,
    ) -> List[SchoolBoardCandidateResult]:
        """Parse a district election page and return one row per candidate.

        Handles two Ballotpedia page formats:

        **Format A — votebox** (e.g. NJ 2022):
        Uses ``<div class="votebox">`` containers with ``<table
        class="results_table">`` inside.  Race heading is in
        ``<h5 class="votebox-header-election-type">``.  Winner rows have
        class ``results_row winner``.

        **Format B — Office/Candidates table** (e.g. NH 2025):
        Uses ``<table class="wikitable sortable collapsible">``.  First row
        contains an ``<h4>`` with the election title; subsequent rows have
        an Office cell and a Candidates cell.  Winners are indicated by a
        green checkmark ``<img>`` immediately preceding the candidate
        ``<a>`` link; incumbents by ``(i)`` in the tail text.

        Pages with no data yet (future elections) return an empty list.
        """
        doc = html.fromstring(page_html)
        results: List[SchoolBoardCandidateResult] = []

        def _make_row(
            race, election_type, candidate, candidate_url,
            party, is_winner, is_incumbent, pct, votes,
        ) -> SchoolBoardCandidateResult:
            return SchoolBoardCandidateResult(
                year=district_row.year,
                state=district_row.state,
                district=district_row.district,
                district_url=district_row.district_url,
                race=race,
                election_type=election_type,
                candidate=candidate,
                candidate_url=candidate_url,
                party=party,
                is_winner=is_winner,
                is_incumbent=is_incumbent,
                pct=pct,
                votes=votes,
                primary=district_row.primary,
                primary_runoff=district_row.primary_runoff,
                general_election=district_row.general_election,
                general_runoff=district_row.general_runoff,
                term_length=district_row.term_length,
                seats_up=district_row.seats_up,
                total_board_seats=district_row.total_board_seats,
                enrollment=district_row.enrollment,
            )

        # ------------------------------------------------------------------
        # Format A: votebox (NJ 2022 style)
        # Select inner voteboxes only — exclude scroll-container wrappers by
        # requiring a direct child div.race_header (the scroll-container has
        # a div.votebox as its direct child, not a div.race_header).
        # ------------------------------------------------------------------
        voteboxes = doc.xpath(
            "//div[contains(@class,'votebox') and "
            "div[contains(@class,'race_header')]]"
        )
        for votebox in voteboxes:
            # Race heading: check h5.votebox-header-election-type first
            heading = ""
            for h_xpath in [
                ".//h5[contains(@class,'votebox-header-election-type')]",
                ".//div[contains(@class,'votebox-heading')]",
                ".//div[contains(@class,'votebox-title')]",
                ".//h3", ".//h4",
            ]:
                nodes = votebox.xpath(h_xpath)
                if nodes:
                    heading = self._clean(nodes[0])
                    break

            election_type = self._infer_election_type(heading)

            for table in votebox.xpath(
                ".//table[contains(@class,'results_table')]"
            ):
                for tr in table.xpath(
                    ".//tr[contains(@class,'results_row')]"
                ):
                    is_winner = "winner" in (tr.get("class") or "")

                    # Candidate name: prefer the <a> link text inside the cell
                    text_cells = tr.xpath(
                        ".//td[contains(@class,'votebox-results-cell--text')]"
                    )
                    if not text_cells:
                        continue

                    name_links = text_cells[0].xpath(".//a")
                    if name_links:
                        candidate = self._clean(name_links[0])
                        href = name_links[0].get("href", "")
                        candidate_url = (
                            href if href.startswith("http")
                            else f"{_BASE_URL}{href}"
                        )
                    else:
                        candidate = self._clean(text_cells[0])
                        candidate_url = ""

                    # Strip trailing party "(Party)" to isolate name
                    candidate = re.sub(r"\s*\([^)]+\)\s*$", "", candidate).strip()
                    if not candidate:
                        continue

                    # Party: extract "(Party)" from full cell text
                    full_text = self._clean(text_cells[0])
                    party_m = re.search(r"\(([^)]+)\)\s*$", full_text)
                    party = party_m.group(1).strip() if party_m else ""

                    # Incumbent: "(i)" anywhere in the cell text
                    is_incumbent = "(i)" in full_text

                    # Percentage: use the inner percentage_number div
                    pct_nodes = tr.xpath(
                        ".//div[contains(@class,'percentage_number')]"
                    )
                    pct = pct_nodes[0].text_content().strip() if pct_nodes else ""

                    # Votes: last votebox-results-cell--number cell
                    num_cells = tr.xpath(
                        ".//td[contains(@class,'votebox-results-cell--number')]"
                    )
                    votes = self._clean(num_cells[-1]) if num_cells else ""

                    results.append(_make_row(
                        heading, election_type, candidate, candidate_url,
                        party, is_winner, is_incumbent, pct, votes,
                    ))

        # ------------------------------------------------------------------
        # Format B: Office/Candidates wikitable (NH 2025 style)
        # Table class contains "collapsible"; title in h3/h4 in first row.
        # ------------------------------------------------------------------
        for table in doc.xpath(
            "//table[contains(@class,'wikitable') and "
            "contains(@class,'collapsible')]"
        ):
            trs = table.xpath(".//tr")
            if not trs:
                continue

            # Election title from h3 or h4 in row 0
            title_nodes = trs[0].xpath(".//h4 | .//h3")
            if not title_nodes:
                continue
            election_type = self._infer_election_type(
                self._clean(title_nodes[0])
            )

            for tr in trs:
                tds = tr.xpath("./td")
                if len(tds) != 2:
                    continue  # skip title, legend, and header rows
                office_text = self._clean(tds[0])
                if not office_text or office_text.lower() == "office":
                    continue

                for name, cand_url, is_winner, is_incumbent in \
                        self._parse_candidate_cell(tds[1]):
                    results.append(_make_row(
                        office_text, election_type, name, cand_url,
                        "", is_winner, is_incumbent, "", "",
                    ))

        return results

    # ------------------------------------------------------------------
    # Public API — district metadata
    # ------------------------------------------------------------------

    def scrape_year(
        self,
        year: int,
        state: Optional[str] = None,
    ) -> List[SchoolBoardElectionRow]:
        """Scrape district-level metadata for one year, optionally one state.

        Parameters
        ----------
        year : int
            Election year to fetch (e.g. 2024).
        state : str, optional
            If given, only return rows for this state (case-insensitive).

        Returns
        -------
        List[SchoolBoardElectionRow]
            Empty list if the page does not exist (404).
        """
        url = self.build_year_url(year)
        page_html = self._get_html(url)
        if page_html is None:
            return []
        rows = self._parse_year_page(page_html, year)
        if state:
            target = state.strip().lower()
            rows = [r for r in rows if r.state.lower() == target]
        return rows

    def iter_years(
        self,
        start_year: int = _EARLIEST_YEAR,
        end_year: Optional[int] = None,
        state: Optional[str] = None,
    ) -> Iterable[SchoolBoardElectionRow]:
        """Yield district rows year-by-year, optionally filtered by state.

        Parameters
        ----------
        start_year : int, optional
            First year to scrape (default: 2013).
        end_year : int, optional
            Last year, inclusive (default: current calendar year).
        state : str, optional
            If given, only yield rows for this state.

        Yields
        ------
        SchoolBoardElectionRow
        """
        if end_year is None:
            end_year = _current_year()
        for year in range(start_year, end_year + 1):
            yield from self.scrape_year(year, state=state)

    def scrape_all(
        self,
        start_year: int = _EARLIEST_YEAR,
        end_year: Optional[int] = None,
        state: Optional[str] = None,
    ) -> List[SchoolBoardElectionRow]:
        """Collect all district rows across years into a single list."""
        return list(
            self.iter_years(start_year=start_year, end_year=end_year, state=state)
        )

    def scrape_all_to_dataframe(
        self,
        start_year: int = _EARLIEST_YEAR,
        end_year: Optional[int] = None,
        state: Optional[str] = None,
    ):
        """Scrape district metadata and return as a pandas DataFrame."""
        import pandas as pd

        rows = self.scrape_all(
            start_year=start_year, end_year=end_year, state=state
        )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame.from_records([asdict(r) for r in rows])

    # ------------------------------------------------------------------
    # Public API — full candidate results
    # ------------------------------------------------------------------

    def scrape_with_results(
        self,
        year: int,
        state: Optional[str] = None,
    ) -> List[SchoolBoardCandidateResult]:
        """Scrape district metadata then follow each district URL for candidates.

        For every district from ``scrape_year(year, state)``, fetches the
        district's Ballotpedia page and parses candidate/results votebox tables.
        Future elections with no data yet silently contribute zero rows.

        Parameters
        ----------
        year : int
            Election year (e.g. 2024).
        state : str, optional
            If given, only scrape districts in this state.

        Returns
        -------
        List[SchoolBoardCandidateResult]
            One row per candidate per race.
        """
        district_rows = self.scrape_year(year, state=state)
        all_results: List[SchoolBoardCandidateResult] = []

        for district_row in district_rows:
            if not district_row.district_url:
                continue
            page_html = self._get_html(district_row.district_url)
            if page_html is None:
                continue
            all_results.extend(
                self._parse_district_page(page_html, district_row)
            )

        return all_results

    def scrape_with_results_to_dataframe(
        self,
        year: int,
        state: Optional[str] = None,
    ):
        """Scrape candidate results and return as a pandas DataFrame.

        Parameters
        ----------
        year : int
            Election year (e.g. 2024).
        state : str, optional
            If given, only scrape districts in this state.

        Returns
        -------
        pandas.DataFrame
            One row per candidate per race.
        """
        import pandas as pd

        results = self.scrape_with_results(year=year, state=state)
        if not results:
            return pd.DataFrame()
        return pd.DataFrame.from_records([asdict(r) for r in results])

    def scrape_joined_to_dataframe(
        self,
        year: int,
        state: Optional[str] = None,
    ):
        """Scrape districts and candidates then return a single joined DataFrame.

        Every district row appears at least once.  Districts with candidates
        produce one row per candidate per race; districts whose pages have no
        candidate data yet (future elections) produce a single row with ``NaN``
        in all candidate-specific columns.

        This avoids having to separately load and merge the districts and
        candidates CSVs — the result is a ready-to-use flat table.

        Parameters
        ----------
        year : int
            Election year (e.g. 2024).
        state : str, optional
            If given, only scrape districts in this state.

        Returns
        -------
        pandas.DataFrame
            Columns: all district metadata fields followed by all
            candidate-specific fields (``race``, ``election_type``,
            ``candidate``, ``candidate_url``, ``party``, ``is_winner``,
            ``is_incumbent``, ``pct``, ``votes``).
        """
        import pandas as pd

        district_rows = self.scrape_year(year, state=state)
        if not district_rows:
            return pd.DataFrame()

        districts_df = pd.DataFrame.from_records([asdict(r) for r in district_rows])

        # Fetch candidate results (one HTTP request per district)
        all_results: List[SchoolBoardCandidateResult] = []
        for district_row in district_rows:
            if not district_row.district_url:
                continue
            page_html = self._get_html(district_row.district_url)
            if page_html is None:
                continue
            all_results.extend(self._parse_district_page(page_html, district_row))

        if not all_results:
            # No candidates found — return districts with empty candidate cols
            for col in ("race", "election_type", "candidate", "candidate_url",
                        "party", "is_winner", "is_incumbent", "pct", "votes"):
                districts_df[col] = pd.NA
            return districts_df

        candidates_df = pd.DataFrame.from_records([asdict(r) for r in all_results])

        # Candidate-specific columns (not present in districts_df)
        join_keys = ["year", "state", "district", "district_url"]
        cand_only_cols = [c for c in candidates_df.columns if c not in districts_df.columns]

        # Left join: every district row is preserved; districts with candidates
        # fan out to one row per candidate.
        joined = districts_df.merge(
            candidates_df[join_keys + cand_only_cols],
            on=join_keys,
            how="left",
        )
        return joined
