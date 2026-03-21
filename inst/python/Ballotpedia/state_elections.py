"""
Ballotpedia state-level elections scraper.

Scrapes per-state, per-year election pages (e.g.
  https://ballotpedia.org/Maine_elections,_2024
  https://ballotpedia.org/Maine_elections,_2025)
and, optionally, the individual election pages linked from them (e.g.
  https://ballotpedia.org/United_States_Senate_election_in_Maine,_2024).

The state+year page contains a "List of candidates" section with up to three
subsections — "Federal Candidates", "State Candidates", "Local Candidates" —
each backed by a wikitable with columns: Candidate | Office | Party | Status.

Each Office cell links to an individual election page that uses the same
votebox format as Ballotpedia school board district pages.

Two scraping modes
------------------
**Listings mode** (fast — one HTTP request per year):
    Parses the state+year page only.  Returns one row per candidate with
    candidate name, office, party, and result status ("Won General", etc.)
    but no vote counts or percentages.

**Results mode** (full — one additional request per unique contest):
    After parsing the state+year page, deduplicates the contest URLs and
    follows each one to scrape vote counts, percentages, and election type
    (General / Primary) from the votebox tables.

Usage
-----
>>> scraper = StateElectionsScraper()

# Fast listing for one state + year
>>> rows = scraper.scrape_listings(2024, "Maine")

# Fast listing, federal only
>>> df = scraper.scrape_listings_to_dataframe(2024, "Pennsylvania", level="federal")

# Full results (follows contest links)
>>> df = scraper.scrape_with_results_to_dataframe(2024, "Maine", level="federal")

# Multi-year, all levels
>>> df = scraper.scrape_all_to_dataframe(2022, 2024, state="Maine")
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

from lxml import html

from .helpers import (
    BallotpediaBaseScraper,
    _BASE_URL,
    _current_year,
)
from text_utils import strip_trailing_parens, extract_party_from_parens, ensure_percent_suffix

# ---------------------------------------------------------------------------
# NOTE: The widget-table-container layout used by this scraper was introduced
# in 2024 and is also present in 2025. Not all states have pages for these
# years. Older year pages use a different structure not yet supported.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Known multi-word office-type suffixes (checked in order — longest first)
# ---------------------------------------------------------------------------

_MULTI_WORD_OFFICES = [
    "Board of Supervisors",
    "Board of Education",
    "Board of Aldermen",
    "Board of Commissioners",
    "Board of Directors",
    "Board of Trustees",
    "Select Board",
    "Board of Selectmen",
    "City Council",
    "Town Council",
    "County Board",
    "County Commission",
    "City Commission",
    "Town Commission",
    "State Senate",
    "State House",
    "State Assembly",
    "State Legislature",
    "State Representative",
    "State Senator",
    "State Treasurer",
    "State Auditor",
    "State Controller",
    "Attorney General",
    "Secretary of State",
    "Lieutenant Governor",
    "Comptroller General",
    "Solicitor General",
    "Court of Appeals",
    "Supreme Court",
    "Superior Court",
    "District Court",
    "Circuit Court",
    "Probate Court",
    "County Clerk",
    "County Treasurer",
    "County Auditor",
    "County Sheriff",
    "County Assessor",
    "County Recorder",
    "County Coroner",
    "County Surveyor",
    "Public Defender",
    "District Attorney",
    "Town Manager",
    "City Manager",
    "Town Clerk",
    "City Clerk",
]

# Single-word office types that can appear at the start of a contest name
_LEADING_OFFICES = [
    "Governor",
    "Comptroller",
    "Treasurer",
    "Auditor",
    "Commissioner",
    "Mayor",
    "Sheriff",
    "Judge",
    "Clerk",
    "Registrar",
    "Assessor",
    "Coroner",
    "Constable",
    "Surveyor",
    "Alderman",
    "Selectman",
    "Selectwoman",
    "Trustee",
    "Director",
    "Supervisor",
    "Chancellor",
    "Controller",
]

# Sub-race patterns that go into the `district` column.
# The leading comma is optional — state races like "Maine State Senate District 1"
# use a space separator; local races like "Augusta City Council, Ward 2" use comma.
_DISTRICT_RE = re.compile(
    r",?\s*("
    r"at[\s\-]large"
    r"|district\s+\d+"
    r"|\d+(?:st|nd|rd|th)\s+district"
    r"|ward\s+\d+"
    r"|position\s+\d+"
    r"|seat\s+\d+"
    r"|place\s+\d+"
    r"|section\s+\d+"
    r")\s*$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class StateElectionCandidateRow:
    """One candidate row from a Ballotpedia state elections page.

    Attributes
    ----------
    year : int
        Election year.
    state : str
        State name as passed to the scraper (e.g. "Maine").
    level : str
        Election level: ``"federal"``, ``"state"``, or ``"local"``.
    contest_name : str
        Raw text from the Office cell, e.g. ``"Augusta City Council, At-large"``.
        Never modified by the parser.
    contest_url : str
        Absolute URL to the individual election page on Ballotpedia.
    jurisdiction : str
        Parsed geographic area extracted from *contest_name*,
        e.g. ``"Augusta"`` or ``"Maine"``.
    office : str
        Parsed role/title extracted from *contest_name*,
        e.g. ``"City Council"`` or ``"U.S. Senate"``.
    district : str
        Parsed sub-race qualifier, e.g. ``"At-large"`` or ``"District 1"``.
        Empty string if not present.
    candidate : str
        Candidate's full name.
    ballotpedia_url : str
        Absolute URL to the candidate's Ballotpedia profile page.
    party : str
        Party affiliation as listed on the state+year page.
    status : str
        Result status from the state+year page listing, e.g.
        ``"Won General"``, ``"Lost Primary"``, ``"Won Round 1"``.
    election_type : str
        Populated in results mode: ``"General"``, ``"Primary"``,
        ``"Primary Runoff"``, ``"General (RCV)"``, or ``"Other"``.
    is_winner : bool
        Populated in results mode: ``True`` if marked as winner.
    is_incumbent : bool
        Populated in results mode: ``True`` if marked as incumbent (``(i)``).
    pct : str
        Vote percentage string, e.g. ``"51.7%"`` (results mode only).
    votes : str
        Raw vote count string, e.g. ``"427,331"`` (results mode only).
    """

    year: int
    state: str
    level: str
    contest_name: str
    contest_url: str
    jurisdiction: str
    office: str
    district: str
    candidate: str
    ballotpedia_url: str
    party: str
    status: str
    election_type: str = ""
    is_winner: bool = False
    is_incumbent: bool = False
    pct: str = ""
    votes: str = ""


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------


class StateElectionsScraper(BallotpediaBaseScraper):
    """Scrape Ballotpedia state elections pages for federal, state, and local races.

    Fetches pages like ``https://ballotpedia.org/Maine_elections,_2024`` and
    parses the "List of candidates" section, which contains subsections for
    Federal, State, and Local candidates in ``wikitable sortable`` tables.

    Optionally follows each unique contest URL to scrape full vote counts and
    percentages from the votebox tables (same format as school board pages).

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
    >>> scraper = StateElectionsScraper()
    >>> df = scraper.scrape_listings_to_dataframe(2024, "Maine", level="federal")
    >>> df = scraper.scrape_with_results_to_dataframe(2024, "Maine", level="local")
    """

    # ------------------------------------------------------------------
    # URL builder
    # ------------------------------------------------------------------

    def build_state_year_url(self, state: str, year: int) -> str:
        """Return the Ballotpedia URL for a given state and election year."""
        slug = state.strip().replace(" ", "_")
        return f"{_BASE_URL}/{slug}_elections,_{year}"

    # ------------------------------------------------------------------
    # Contest-name parser
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_contest_name(text: str) -> tuple:
        """Parse raw contest text into (jurisdiction, office, district).

        The *text* argument is the raw contest_name string and is never
        modified — this function works on a local working copy.

        Parameters
        ----------
        text : str
            Raw Office cell text, e.g. ``"Augusta City Council, At-large"``.

        Returns
        -------
        tuple of (jurisdiction, office, district)
        """
        work = text.strip()
        district = ""

        # 1. District suffix — capture and remove from working copy only
        m = _DISTRICT_RE.search(work)
        if m:
            district = m.group(1).strip()
            work = work[: m.start()].strip()

        # 2. "X of Y" pattern — e.g. "Mayor of Augusta", "Governor of Maine"
        of_m = re.match(r"^(.+?)\s+of\s+(.+)$", work, re.IGNORECASE)
        if of_m:
            return of_m.group(2).strip(), of_m.group(1).strip(), district

        # 3. U.S. / United States federal races
        #    e.g. "U.S. Senate Maine", "United States House Maine 2nd District"
        us_m = re.match(
            r"^(U\.S\.|United States)\s+(Senate|House(?:\s+of\s+Representatives)?|"
            r"Representative|Congress(?:man|woman)?|Presidential\s+[Ee]lectors?)\s*(.*)?$",
            work,
        )
        if us_m:
            role_raw = us_m.group(2).strip()
            remainder = (us_m.group(3) or "").strip()
            # Normalise role
            role = "U.S. " + role_raw.split()[0].capitalize()
            if "House" in role_raw or "Representative" in role_raw:
                role = "U.S. House"
            elif "Senate" in role_raw:
                role = "U.S. Senate"
            elif "President" in role_raw:
                role = "Presidential Electors"
            # Remainder is typically the state name (which we already know)
            # but capture it as jurisdiction if present
            jurisdiction = remainder if remainder else ""
            return jurisdiction, role, district

        # 4. Known multi-word office-type suffixes
        #    e.g. "Augusta City Council", "Cumberland County Select Board"
        work_lower = work.lower()
        for mw_office in _MULTI_WORD_OFFICES:
            mw_lower = mw_office.lower()
            if work_lower.endswith(mw_lower):
                prefix = work[: len(work) - len(mw_office)].strip().rstrip(",").strip()
                return prefix, mw_office, district

        # 5. Known single-word leading offices
        #    e.g. "Governor John Smith" → unlikely; more likely just "Governor"
        for leading in _LEADING_OFFICES:
            if re.match(rf"^{re.escape(leading)}\b", work, re.IGNORECASE):
                remainder = work[len(leading):].strip().lstrip(",").strip()
                return remainder, leading, district

        # 6. Default — treat entire text as office, no jurisdiction parsed
        return "", work, district

    # ------------------------------------------------------------------
    # State-year page parser
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_status_cell(td) -> str:
        """Extract status text from a status cell.

        Ballotpedia renders two patterns:
        - Winners: ``<span style="font-weight:700">Won</span>
                    <span class="sub-detail"> General</span>``
        - Losers:  ``Lost<span class="sub-detail"> General</span>``
          where "Lost" is a bare text node, not inside a span.

        Using full ``text_content()`` on the inner div captures both patterns.
        """
        # Prefer the inner <div> to avoid stray whitespace from the td itself
        inner_divs = td.xpath("./div")
        node = inner_divs[0] if inner_divs else td
        return re.sub(r"\s+", " ", node.text_content() or "").strip()

    def _parse_state_year_page(
        self,
        page_html: str,
        year: int,
        state: str,
        level_filter: Optional[str] = None,
    ) -> List[StateElectionCandidateRow]:
        """Parse the 'List of candidates' section of a Ballotpedia state+year page.

        The section contains ``div.widget-table-container`` wrappers, each
        holding a ``table.bp-table.widget-table`` with a ``<caption>`` that
        reads "Federal Candidates", "State Candidates", or "Local Candidates".
        Columns are identified via ``td[data-cell=...]`` attributes.

        Parameters
        ----------
        page_html : str
            Raw HTML of the state+year page.
        year : int
            Election year.
        state : str
            State name (carried into each row unchanged).
        level_filter : str, optional
            If given, only return rows whose level matches (case-insensitive).

        Returns
        -------
        List[StateElectionCandidateRow]
        """
        doc = html.fromstring(page_html)
        rows: List[StateElectionCandidateRow] = []

        # Find all widget-table-container divs that contain a bp-table
        containers = doc.xpath(
            "//div[contains(@class,'widget-table-container')]"
            "[.//table[contains(@class,'bp-table') or contains(@class,'widget-table')]]"
        )

        for container in containers:
            # Level from the table caption
            captions = container.xpath(".//caption")
            if not captions:
                continue
            caption_text = self._clean(captions[0]).lower()
            if "federal" in caption_text:
                current_level = "federal"
            elif "state" in caption_text:
                current_level = "state"
            elif "local" in caption_text:
                current_level = "local"
            else:
                continue  # not a candidates table we recognise

            # Apply level filter early
            if level_filter and level_filter != "all":
                if current_level != level_filter:
                    continue

            # Get the table
            tables = container.xpath(
                ".//table[contains(@class,'bp-table') or contains(@class,'widget-table')]"
            )
            if not tables:
                continue
            table = tables[0]

            # Identify the header row to skip it
            header_trs = table.xpath(".//tr[.//th]")
            header_set = set(id(tr) for tr in header_trs)

            # Data rows
            for tr in table.xpath(".//tr"):
                if id(tr) in header_set:
                    continue  # skip header row

                # Use data-cell attributes for reliable column identification
                office_td = tr.xpath("./td[@data-cell='office']")
                candidate_td = tr.xpath("./td[@data-cell='candidate']")
                party_td = tr.xpath("./td[@data-cell='party']")
                status_td = tr.xpath("./td[@data-cell='status']")

                if not office_td:
                    # Fallback: positional (office is col index 1)
                    all_tds = tr.xpath("./td")
                    if len(all_tds) < 2:
                        continue
                    office_td = [all_tds[1]]
                    candidate_td = [all_tds[0]] if all_tds else []
                    party_td = [all_tds[2]] if len(all_tds) > 2 else []
                    status_td = [all_tds[3]] if len(all_tds) > 3 else []

                # Office cell — single <a> inside a <div>
                office_links = office_td[0].xpath(".//a")
                if not office_links:
                    continue
                contest_name = self._clean(office_links[0])
                contest_href = office_links[0].get("href", "")
                contest_url = (
                    contest_href if contest_href.startswith("http")
                    else f"{_BASE_URL}{contest_href}"
                )

                # Candidate cell — <a> inside div.widget-candidate-info
                candidate_name = ""
                bp_url = ""
                if candidate_td:
                    # Prefer links inside the candidate-info div (avoids image links)
                    cand_links = candidate_td[0].xpath(
                        ".//div[contains(@class,'widget-candidate-info')]//a"
                    )
                    if not cand_links:
                        # Fallback: any non-external <a>
                        cand_links = [
                            a for a in candidate_td[0].xpath(".//a")
                            if a.get("target") != "_blank"
                            and a.text_content().strip()
                        ]
                    if cand_links:
                        a = cand_links[0]
                        candidate_name = a.text_content().strip().rstrip("*").strip()
                        href = a.get("href", "")
                        bp_url = (
                            href if href.startswith("http")
                            else f"{_BASE_URL}{href}"
                        )

                # Party cell — text inside <span class="party-affiliation ...">
                party = ""
                if party_td:
                    spans = party_td[0].xpath(".//span[contains(@class,'party-affiliation')]")
                    party = self._clean(spans[0]) if spans else self._clean(party_td[0])

                # Status cell — combine bold span + sub-detail span
                status = ""
                if status_td:
                    status = self._parse_status_cell(status_td[0])

                # Parse contest name into structured components
                jurisdiction, office_type, district = self._parse_contest_name(
                    contest_name
                )

                rows.append(StateElectionCandidateRow(
                    year=year,
                    state=state,
                    level=current_level,
                    contest_name=contest_name,
                    contest_url=contest_url,
                    jurisdiction=jurisdiction,
                    office=office_type,
                    district=district,
                    candidate=candidate_name,
                    ballotpedia_url=bp_url,
                    party=party,
                    status=status,
                ))

        return rows

    # ------------------------------------------------------------------
    # Election-page parser (votebox format)
    # ------------------------------------------------------------------

    def _parse_election_page(
        self,
        page_html: str,
        listing_rows: List[StateElectionCandidateRow],
    ) -> List[StateElectionCandidateRow]:
        """Parse an individual election page and return fully populated rows.

        Handles two Ballotpedia page formats:

        **Format A — votebox** (most races):
        Uses ``<div class="votebox">`` containers with
        ``<table class="results_table">`` inside.

        **Format B — Office/Candidates wikitable** (some races):
        Uses ``<table class="wikitable collapsible">``.

        For each candidate found, tries to match by name (case-insensitive)
        to an existing listing row to carry forward ``status``, ``party``,
        and ``ballotpedia_url``.  Write-in or unlisted candidates are added
        as new rows.

        Parameters
        ----------
        page_html : str
            Raw HTML of the individual election page.
        listing_rows : List[StateElectionCandidateRow]
            Candidate rows from the state+year page for this contest.

        Returns
        -------
        List[StateElectionCandidateRow]
            One fully populated row per candidate per election type.
        """
        doc = html.fromstring(page_html)
        results: List[StateElectionCandidateRow] = []

        # Build lookup: lowercased name → listing row
        listing_by_name: Dict[str, StateElectionCandidateRow] = {
            r.candidate.lower(): r for r in listing_rows
        }

        # Shared fields from the first listing row (office, jurisdiction, etc.)
        template = listing_rows[0] if listing_rows else None

        def _make_row(
            election_type: str,
            candidate: str,
            candidate_url: str,
            party: str,
            is_winner: bool,
            is_incumbent: bool,
            pct: str,
            votes: str,
        ) -> StateElectionCandidateRow:
            # Try to match to a listing row
            listing = listing_by_name.get(candidate.lower())
            if listing:
                base = listing
            elif template:
                # New candidate (write-in, etc.) — clone template with overrides
                base = template
            else:
                # No template available — return minimal row
                return StateElectionCandidateRow(
                    year=0, state="", level="", contest_name="",
                    contest_url="", jurisdiction="", office="",
                    district="", candidate=candidate,
                    ballotpedia_url=candidate_url, party=party,
                    status="",
                    election_type=election_type,
                    is_winner=is_winner, is_incumbent=is_incumbent,
                    pct=pct, votes=votes,
                )

            return StateElectionCandidateRow(
                year=base.year,
                state=base.state,
                level=base.level,
                contest_name=base.contest_name,
                contest_url=base.contest_url,
                jurisdiction=base.jurisdiction,
                office=base.office,
                district=base.district,
                candidate=candidate,
                ballotpedia_url=listing.ballotpedia_url if listing else candidate_url,
                party=listing.party if listing else party,
                status=listing.status if listing else "",
                election_type=election_type,
                is_winner=is_winner,
                is_incumbent=is_incumbent,
                pct=pct,
                votes=votes,
            )

        # ------------------------------------------------------------------
        # Format A: votebox (standard and RCV)
        # ------------------------------------------------------------------
        voteboxes = doc.xpath(
            "//div[contains(@class,'votebox') and "
            "div[contains(@class,'race_header')]]"
        )
        for votebox in voteboxes:
            # Race heading
            heading = ""
            for h_xpath in [
                ".//h5[contains(@class,'votebox-header-election-type')]",
                ".//h5[contains(@class,'rcvrace_header')]",
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

                    candidate = strip_trailing_parens(candidate)
                    if not candidate:
                        continue

                    full_text = self._clean(text_cells[0])
                    party = extract_party_from_parens(full_text)
                    is_incumbent = "(i)" in full_text

                    pct_nodes = tr.xpath(
                        ".//div[contains(@class,'percentage_number')] | "
                        ".//span[contains(@class,'percentage_number')]"
                    )
                    pct = pct_nodes[0].text_content().strip() if pct_nodes else ""
                    pct = ensure_percent_suffix(pct)

                    num_cells = tr.xpath(
                        ".//td[contains(@class,'votebox-results-cell--number')]"
                    )
                    votes = self._clean(num_cells[-1]) if num_cells else ""

                    results.append(_make_row(
                        election_type, candidate, candidate_url,
                        party, is_winner, is_incumbent, pct, votes,
                    ))

        # ------------------------------------------------------------------
        # Format B: Office/Candidates wikitable (collapsible)
        # ------------------------------------------------------------------
        for table in doc.xpath(
            "//table[contains(@class,'wikitable') and "
            "contains(@class,'collapsible')]"
        ):
            trs = table.xpath(".//tr")
            if not trs:
                continue

            title_nodes = trs[0].xpath(".//h4 | .//h3")
            if not title_nodes:
                continue
            election_type = self._infer_election_type(
                self._clean(title_nodes[0])
            )

            for tr in trs:
                tds = tr.xpath("./td")
                if len(tds) != 2:
                    continue
                office_text = self._clean(tds[0])
                if not office_text or office_text.lower() == "office":
                    continue

                for name, cand_url, is_winner, is_incumbent in \
                        self._parse_candidate_cell(tds[1]):
                    results.append(_make_row(
                        election_type, name, cand_url,
                        "", is_winner, is_incumbent, "", "",
                    ))

        return results

    # ------------------------------------------------------------------
    # Public API — listings (fast, no vote counts)
    # ------------------------------------------------------------------

    def scrape_listings(
        self,
        year: int,
        state: str,
        level: str = "all",
    ) -> List[StateElectionCandidateRow]:
        """Scrape the state+year page and return one row per listed candidate.

        One HTTP request.  Returns candidate names, offices, parties, and
        result statuses ("Won General", etc.) but no vote counts.

        Parameters
        ----------
        year : int
            Election year (e.g. 2024).
        state : str
            State name (e.g. ``"Maine"``).
        level : str, optional
            ``"federal"``, ``"state"``, ``"local"``, or ``"all"`` (default).

        Returns
        -------
        List[StateElectionCandidateRow]
        """
        url = self.build_state_year_url(state, year)
        page_html = self._get_html(url)
        if page_html is None:
            print(f"  No data available for {state} {year}.")
            return []
        level_arg = None if level == "all" else level.lower()
        return self._parse_state_year_page(page_html, year, state, level_arg)

    def scrape_listings_to_dataframe(
        self,
        year: int,
        state: str,
        level: str = "all",
    ):
        """Scrape the state+year listing and return as a pandas DataFrame.

        Parameters
        ----------
        year : int
            Election year.
        state : str
            State name.
        level : str, optional
            ``"federal"``, ``"state"``, ``"local"``, or ``"all"`` (default).

        Returns
        -------
        pandas.DataFrame
        """
        import pandas as pd

        rows = self.scrape_listings(year=year, state=state, level=level)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame.from_records([asdict(r) for r in rows])

    # ------------------------------------------------------------------
    # Public API — full results (follows contest links)
    # ------------------------------------------------------------------

    def scrape_with_results(
        self,
        year: int,
        state: str,
        level: str = "all",
    ) -> List[StateElectionCandidateRow]:
        """Scrape the state+year page then follow each unique contest URL.

        Step 1: Parses the state+year page to build the candidate listing.
        Step 2: Deduplicates contest URLs (many candidates share one page).
        Step 3: Fetches each unique contest page and parses vote counts.
        Step 4: Merges results back with listing data (status, party, etc.).

        Parameters
        ----------
        year : int
            Election year (e.g. 2024).
        state : str
            State name (e.g. ``"Maine"``).
        level : str, optional
            ``"federal"``, ``"state"``, ``"local"``, or ``"all"`` (default).

        Returns
        -------
        List[StateElectionCandidateRow]
            One row per candidate per election type (General, Primary, etc.).
        """
        listing_rows = self.scrape_listings(year=year, state=state, level=level)
        if not listing_rows:
            return []

        # Group listing rows by contest URL
        by_contest: Dict[str, List[StateElectionCandidateRow]] = defaultdict(list)
        for row in listing_rows:
            if row.contest_url:
                by_contest[row.contest_url].append(row)

        all_results: List[StateElectionCandidateRow] = []

        for contest_url, contest_listing_rows in by_contest.items():
            page_html = self._get_html(contest_url)
            if page_html is None:
                # No election page — return listing rows as-is (no vote data)
                all_results.extend(contest_listing_rows)
                continue

            parsed = self._parse_election_page(page_html, contest_listing_rows)
            if parsed:
                all_results.extend(parsed)
            else:
                # Election page exists but no votebox data yet (future election)
                all_results.extend(contest_listing_rows)

        return all_results

    def scrape_with_results_to_dataframe(
        self,
        year: int,
        state: str,
        level: str = "all",
    ):
        """Scrape with full results and return as a pandas DataFrame.

        Parameters
        ----------
        year : int
            Election year.
        state : str
            State name.
        level : str, optional
            ``"federal"``, ``"state"``, ``"local"``, or ``"all"`` (default).

        Returns
        -------
        pandas.DataFrame
            Columns: year, state, level, contest_name, contest_url,
            jurisdiction, office, district, candidate, ballotpedia_url,
            party, status, election_type, is_winner, is_incumbent, pct, votes.
        """
        import pandas as pd

        results = self.scrape_with_results(year=year, state=state, level=level)
        if not results:
            return pd.DataFrame()
        return pd.DataFrame.from_records([asdict(r) for r in results])

    # ------------------------------------------------------------------
    # Public API — multi-year
    # ------------------------------------------------------------------

    def scrape_all_to_dataframe(
        self,
        start_year: int = 2024,
        end_year: Optional[int] = None,
        state: Optional[str] = None,
        level: str = "all",
        full_results: bool = False,
    ):
        """Scrape multiple years for a single state and return a DataFrame.

        Parameters
        ----------
        start_year : int, optional
            First year to scrape (default: 2024).
        end_year : int, optional
            Last year, inclusive (default: current calendar year).
        state : str
            State name (required for this scraper).
        level : str, optional
            ``"federal"``, ``"state"``, ``"local"``, or ``"all"`` (default).
        full_results : bool, optional
            If ``True``, follows each contest URL for vote counts (slower).
            Default ``False`` returns listings only.

        Returns
        -------
        pandas.DataFrame
        """
        import pandas as pd

        if not state:
            raise ValueError("state is required for StateElectionsScraper")

        if end_year is None:
            end_year = _current_year()

        all_rows: List[StateElectionCandidateRow] = []
        for year in range(start_year, end_year + 1):
            if full_results:
                rows = self.scrape_with_results(year=year, state=state, level=level)
            else:
                rows = self.scrape_listings(year=year, state=state, level=level)
            all_rows.extend(rows)
            print(f"  {year}: {len(rows)} rows")

        if not all_rows:
            return pd.DataFrame()
        return pd.DataFrame.from_records([asdict(r) for r in all_rows])
