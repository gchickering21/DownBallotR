"""
Ballotpedia municipal and mayoral elections scraper.

Scrapes two annual US election index pages:
  - Mayoral:   https://ballotpedia.org/United_States_mayoral_elections,_{year}
  - Municipal: https://ballotpedia.org/United_States_municipal_elections,_{year}

Both index pages contain a "elections across the United States" section listing
city and county sub-URLs.  This module supports two modes via ``race_type``:

  race_type="mayoral"
      Uses the mayoral index (2020–2025).  All sub-URLs are
      ``/Mayoral_election_in_*`` pages with a single race per page.

  race_type="all"  (default)
      Uses the municipal index (2014–2025).  Sub-URLs include
      ``/City_elections_in_*``, ``/Mayoral_election_in_*``,
      ``/Municipal_elections_in_*`` (county), and other types.

Phase 1 — index discovery (fast, one request per year):
    Returns ``MunicipalElectionLink`` records (location metadata + sub-URL).

Phase 2 — full results (one additional request per sub-URL):
    Returns ``MunicipalElectionRow`` records with candidate names, vote counts,
    percentages, winner/incumbent flags, office, and election type.

    Sub-pages use two formats:
      Format A (votebox, ~2018+): div.votebox + table.results_table
      Format B (wikitable, older): table.wikitable under a "Results" heading

Usage
-----
>>> s = MunicipalElectionsScraper()

# Phase 1 — discover sub-URLs for one year
>>> links = s.get_election_links(2022, race_type="mayoral")
>>> df   = s.get_all_years_links_to_dataframe(2020, 2022, race_type="all")

# Phase 2 — full candidate results
>>> rows = s.scrape_location(2022, "Austin", "Texas", race_type="mayoral")
>>> df   = s.scrape_all_to_dataframe(2022, race_type="mayoral")
>>> df   = s.scrape_years_to_dataframe(2020, 2022, race_type="mayoral")
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import List, Optional
import pandas as pd
from lxml import html as lhtml

from .helpers import (
    BallotpediaBaseScraper,
    _BASE_URL,
    _current_year,
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class MunicipalElectionLink:
    """One row per discovered sub-URL from the index page (Phase 1 output)."""

    year: int
    location: str       # city or county name
    state: str          # from h3/h4 state heading on index page
    location_type: str  # "mayoral" | "city" | "county" | "other"
    url: str            # full URL of the individual election page


@dataclass
class MunicipalElectionRow:
    """One row per candidate per race per sub-URL (Phase 2 output)."""

    year: int
    location: str
    state: str
    location_type: str
    election_url: str    # URL of the individual election page
    office: str          # race/office name (votebox h3 or wikitable heading)
    election_type: str   # "General" | "Primary" | "Primary Runoff" | "General (RCV)" | "Other"
    candidate: str
    ballotpedia_url: str # candidate's Ballotpedia profile URL
    party: str
    is_winner: bool
    is_incumbent: bool
    pct: str             # "62.3%" or ""
    votes: str           # "12,345" or ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STRUCTURAL_HEADINGS = frozenset({
    "by state", "by date", "by election date", "by election type",
    "see also", "external links", "references", "notes",
})

# h2 headings that signal the end of the elections section we want.
# Any h2 NOT in this set (and not the "elections across" trigger) is
# treated as a state-name heading and updates current_state.
_STOP_H2 = frozenset({
    "see also", "references", "external links", "notes", "footnotes",
    "campaign finance", "historical election data", "recent news",
    "battleground", "contents", "by date", "by election date",
    "ballot measures", "quick facts", "by election type", "by city",
    "navigation menu", "additional resources", "sources and notes",
})

# Href substring → location_type (new URL format: *_election_in_*)
_HREF_TYPE_MAP = [
    ("Mayoral_election_in_",    "mayoral"),
    ("City_elections_in_",      "city"),
    ("Municipal_elections_in_", "county"),
]

# Matches both new format (/Mayoral_election_in_City,_State_(year))
# and old format (/City,_State_municipal_elections,_year used pre-2016)
_ELECTION_LINK_RE = re.compile(
    r"/_?(?:Mayoral_election_in_|City_elections_in_|Municipal_elections_in_|"
    r"[A-Za-z_]+_election_in_|[A-Za-z_]+_elections_in_)"
    r"|/[^/]+,_[A-Za-z_]+_(?:municipal|mayoral)_elections?,"
)


def _href_location_type(href: str) -> str:
    """Infer location_type from href pattern (handles both old and new formats)."""
    # New format
    for prefix, ltype in _HREF_TYPE_MAP:
        if prefix in href:
            return ltype
    # Old format: /City,_State_mayoral_election,_year or _municipal_elections,_year
    if "_mayoral_election," in href:
        return "mayoral"
    if "_municipal_elections," in href:
        return "city"
    return "other"


def _location_from_text(link_text: str) -> str:
    """Extract city/county name from link text like 'Austin, Texas'.

    Returns empty string when text doesn't look like 'City, State' so the
    caller can fall back to href-based parsing.
    """
    if ", " in link_text:
        return link_text.split(", ", 1)[0].strip()
    return ""  # not a "City, State" string — signal to use href fallback


def _location_from_href(href: str) -> str:
    """Fallback: parse location from href when link text is uninformative.

    Handles both URL formats:
      New: /Mayoral_election_in_Austin,_Texas_(2022)
      Old: /Austin,_Texas_municipal_elections,_2015
    """
    # New format: _in_City,_State_(year)
    m = re.search(r"_in_([^,]+),", href)
    if m:
        return m.group(1).replace("_", " ").strip()
    # Old format: /City,_State_type_elections,_year
    m = re.search(r"/([^/,]+),", href)
    if m:
        return m.group(1).replace("_", " ").strip()
    return ""


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class MunicipalElectionsScraper(BallotpediaBaseScraper):
    """Scraper for Ballotpedia municipal and mayoral election pages.

    Inherits HTTP client, WAF-bypass, and static parsing helpers from
    ``BallotpediaBaseScraper``.

    Parameters
    ----------
    sleep_s : float
        Polite delay between requests (default: 1.0 s).
    timeout_s : int
        Per-request timeout (default: 30 s).
    user_agent : str
        HTTP User-Agent header.
    """

    # ------------------------------------------------------------------
    # URL builder
    # ------------------------------------------------------------------

    def build_index_url(self, year: int, race_type: str = "all") -> str:
        """Return the Ballotpedia index URL for the given year and race_type."""
        if race_type == "mayoral":
            return f"{_BASE_URL}/United_States_mayoral_elections,_{year}"
        return f"{_BASE_URL}/United_States_municipal_elections,_{year}"

    def _build_location_url(
        self, year: int, location: str, state: str, race_type: str = "mayoral"
    ) -> str:
        """Build the individual election page URL from location parts."""
        loc_slug = location.strip().replace(" ", "_")
        st_slug  = state.strip().replace(" ", "_")
        if race_type == "mayoral":
            return f"{_BASE_URL}/Mayoral_election_in_{loc_slug},_{st_slug}_({year})"
        # Generic city elections URL as fallback
        return f"{_BASE_URL}/City_elections_in_{loc_slug},_{st_slug}_({year})"

    # ------------------------------------------------------------------
    # Phase 1 — index page parsing
    # ------------------------------------------------------------------

    def _extract_links(
        self, page_html: str, year: int
    ) -> List[MunicipalElectionLink]:
        """Parse the index page and return all discovered sub-URL records.

        Handles three page structures seen across years:

        * 2022-style: "elections across" is h2, state names are h3.
        * 2024-style: "elections across" is h2, state names are also h2.
        * 2015-style: no "elections across" heading; falls back to "By state" h2
          section where state names appear as h3.

        For h2 siblings after the start heading, we stop only when the h2 text
        is in ``_STOP_H2`` (structural page sections); any other h2 is treated
        as a state-name update so that 2024-style pages work correctly.
        """

        doc = lhtml.fromstring(page_html)
        links: List[MunicipalElectionLink] = []
        seen_hrefs: set = set()
        current_state = ""

        # --- Shared link appender -----------------------------------------
        def _add_link(href: str, location: str, state: str) -> None:
            if href in seen_hrefs:
                return
            seen_hrefs.add(href)
            full_url = (
                href if href.startswith("http") else f"{_BASE_URL}{href}"
            )
            if not location:
                location = _location_from_href(href)
            links.append(MunicipalElectionLink(
                year=year,
                location=location,
                state=state,
                location_type=_href_location_type(href),
                url=full_url,
            ))

        # --- marqueetable parser (2021 mayoral style) --------------------
        def _parse_marqueetable(table) -> None:
            """Parse a table.marqueetable.sortable: cols are City, State, …"""
            rows = table.xpath(".//tr")
            # Find the header row to locate City and State column indices
            city_col = state_col = None
            for tr in rows[:3]:
                cells = tr.xpath("./th | ./td")
                for i, cell in enumerate(cells):
                    t = self._clean(cell).lower()
                    if t == "city":
                        city_col = i
                    elif t == "state":
                        state_col = i
                if city_col is not None:
                    break

            if city_col is None:
                return  # can't locate columns

            for tr in rows:
                tds = tr.xpath("./td")
                if not tds:
                    continue
                if city_col >= len(tds):
                    continue
                city_links = tds[city_col].xpath(".//a")
                if not city_links:
                    continue
                href = city_links[0].get("href", "")
                if not _ELECTION_LINK_RE.search(href):
                    continue
                loc = self._clean(city_links[0])
                st = (
                    self._clean(tds[state_col]) if state_col is not None and state_col < len(tds)
                    else ""
                )
                _add_link(href, loc, st)

        # --- Find start heading -------------------------------------------
        # Primary: "elections across the United States" (2022/2024 style)
        target_heading = None
        for h2 in doc.xpath("//h2"):
            text = self._clean(h2).lower()
            if "elections across the united states" in text:
                target_heading = h2
                break

        # Fallback 1: "By state" as h2 (2014/2015 style) or h1 (2017 style)
        if target_heading is None:
            for heading in doc.xpath("//h2 | //h1"):
                text = self._clean(heading).lower()
                if text == "by state":
                    target_heading = heading
                    break

        # Fallback 2: marqueetable (2021 mayoral style — no state-list section)
        if target_heading is None:
            for tbl in doc.xpath("//table[contains(@class,'marqueetable')]"):
                _parse_marqueetable(tbl)
            return links  # done — marqueetable is self-contained

        parent = target_heading.getparent()
        if parent is None:
            return links

        siblings = list(parent)
        try:
            start_idx = siblings.index(target_heading)
        except ValueError:
            return links

        # --- Link collector for ul/ol elements ---------------------------
        def _collect_from_list(elem) -> None:
            for li in elem.xpath("./li"):
                for a in li.xpath(".//a"):
                    href = a.get("href", "")
                    if not _ELECTION_LINK_RE.search(href):
                        continue
                    link_text = self._clean(a)
                    location = _location_from_text(link_text)
                    _add_link(href, location, current_state)

        # --- Recursive walker for non-h2 elements ------------------------
        def _walk(elem) -> None:
            nonlocal current_state
            tag = elem.tag
            text = self._clean(elem).strip()

            if tag in ("h3", "h4", "h5"):
                lower = text.lower()
                if lower not in _STRUCTURAL_HEADINGS and text:
                    current_state = text
            elif tag in ("ul", "ol"):
                _collect_from_list(elem)
            elif tag not in ("h1", "h2"):
                # Recurse into div/section wrappers
                for child in elem:
                    _walk(child)

        # --- Walk siblings after the start heading -----------------------
        for elem in siblings[start_idx + 1:]:
            tag = elem.tag
            if tag == "h2":
                h2_text = self._clean(elem).lower()
                if h2_text in _STOP_H2:
                    break  # end of elections section
                # State-name h2 (2024 style) — update current_state
                state_text = self._clean(elem).strip()
                if state_text:
                    current_state = state_text
            else:
                _walk(elem)

        return links

    # ------------------------------------------------------------------
    # Phase 2 — sub-page parsing
    # ------------------------------------------------------------------

    def _parse_election_page(
        self,
        page_html: str,
        location: str,
        state: str,
        location_type: str,
        election_url: str,
        year: int,
    ) -> List[MunicipalElectionRow]:
        """Parse an individual election page and return candidate rows.

        Tries Format A (votebox) first; falls back to Format B (wikitable).
        """

        doc = lhtml.fromstring(page_html)
        results: List[MunicipalElectionRow] = []

        def _make_row(
            office: str,
            election_type: str,
            candidate: str,
            candidate_url: str,
            party: str,
            is_winner: bool,
            is_incumbent: bool,
            pct: str,
            votes: str,
        ) -> MunicipalElectionRow:
            return MunicipalElectionRow(
                year=year,
                location=location,
                state=state,
                location_type=location_type,
                election_url=election_url,
                office=office,
                election_type=election_type,
                candidate=candidate,
                ballotpedia_url=candidate_url,
                party=party,
                is_winner=is_winner,
                is_incumbent=is_incumbent,
                pct=pct,
                votes=votes,
            )

        # --------------------------------------------------------------
        # Format A: votebox (div.votebox + div.race_header)
        # Same structure as state_elections._parse_election_page
        # --------------------------------------------------------------
        voteboxes = doc.xpath(
            "//div[contains(@class,'votebox') and "
            "div[contains(@class,'race_header')]]"
        )

        for votebox in voteboxes:
            # The h5.votebox-header-election-type contains the full race title,
            # e.g. "General election for Mayor of Austin" or
            #      "Democratic primary for Hillsborough County Sheriff".
            # Office name is everything after " for "; election type is inferred
            # from the prefix words.
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

            # Parse office from heading: split on " for " and take right side.
            # e.g. "General election for Mayor of Austin" -> "Mayor of Austin"
            if " for " in heading:
                office = heading.split(" for ", 1)[1].strip()
            else:
                office = heading

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

                    # Strip trailing "(Party)" to isolate name
                    candidate = re.sub(r"\s*\([^)]+\)\s*$", "", candidate).strip()
                    if not candidate:
                        continue

                    full_text = self._clean(text_cells[0])
                    party_m = re.search(r"\(([^)]+)\)\s*$", full_text)
                    party = party_m.group(1).strip() if party_m else ""
                    is_incumbent = "(i)" in full_text

                    pct_nodes = tr.xpath(
                        ".//div[contains(@class,'percentage_number')] | "
                        ".//span[contains(@class,'percentage_number')]"
                    )
                    pct = pct_nodes[0].text_content().strip() if pct_nodes else ""
                    if pct and "%" not in pct:
                        pct = pct + "%"

                    num_cells = tr.xpath(
                        ".//td[contains(@class,'votebox-results-cell--number')]"
                    )
                    votes = self._clean(num_cells[-1]) if num_cells else ""

                    results.append(_make_row(
                        office, election_type, candidate, candidate_url,
                        party, is_winner, is_incumbent, pct, votes,
                    ))

        if results:
            return results

        # --------------------------------------------------------------
        # Format B/C: older pages — bptable or plain wikitable
        #
        # Page-wide scan: search the entire document for vote-result tables
        # regardless of what section heading they appear under.  This handles
        # pages that put results under "Results", "City council", "Elections",
        # "General election", or no named heading at all.
        #
        # Table classes:
        #   Format B: table.wikitable  — plain wikitable (Candidate|Vote%|Votes)
        #   Format C: table.bptable    — collapsible table with title row
        #
        # Finance tables (same CSS classes) are excluded via column-header check.
        # Partisan-matrix wikitables (Office|Democratic|Republican|Other) are
        # excluded by detecting "democratic"/"republican" column headers.
        # --------------------------------------------------------------

        def _is_vote_table(table) -> bool:
            """Return True when the table looks like a candidate vote-result table.

            Exclusions:
            - Navigation/sidebar boxes: class ends with ";" (e.g. "wikitable;").
            - Campaign finance tables: column header contains "contribut",
              "expendit", or "disburs".
            - Partisan-matrix tables (Office | Democratic | Republican | Other):
              a column header cell text is exactly "democratic" or "republican".
            """
            cls = (table.get("class") or "").lower()
            if "bptable" not in cls and "wikitable" not in cls:
                return False
            # Navigation/sidebar boxes have class="wikitable;" (trailing semicolon)
            if cls.strip().endswith(";"):
                return False
            for tr in table.xpath(".//tr")[:3]:
                for cell in tr.xpath("./th | ./td"):
                    t = self._clean(cell).lower()
                    if "contribut" in t or "expendit" in t or "disburs" in t:
                        return False  # campaign finance table
                    if t in ("democratic", "republican", "democrat", "gop"):
                        return False  # partisan-matrix table
            return True

        def _parse_candidate_cell(td) -> tuple:
            """Extract (candidate, url, is_winner, is_incumbent) from a table cell."""
            pending_winner = False
            candidate = ""
            candidate_url = ""
            is_incumbent = False
            is_winner = False

            for child in td.iter():
                ctag = child.tag
                if ctag == "img":
                    alt = (child.get("alt") or "").lower()
                    if "green check" in alt or "check mark" in alt:
                        pending_winner = True
                elif ctag == "a":
                    href = child.get("href", "")
                    if href == "/Won" or href.endswith("/Won"):
                        continue  # bptable winner-wrapper link
                    name = self._clean(child)
                    if not name:
                        continue
                    candidate = name
                    candidate_url = (
                        href if href.startswith("http")
                        else f"{_BASE_URL}{href}"
                    )
                    tail = child.tail or ""
                    is_incumbent = "(i)" in tail
                    is_winner = pending_winner
                    break

            if not candidate:
                candidate = self._clean(td).strip()
            return candidate, candidate_url, is_winner, is_incumbent

        def _parse_data_row(
            tr, office: str, election_type: str
        ) -> Optional[MunicipalElectionRow]:
            """Parse one <tr> from a bptable or wikitable."""
            tds = tr.xpath("./td")
            if len(tds) < 2:
                return None
            cell0_text = self._clean(tds[0])
            if not cell0_text or cell0_text.lower() in ("candidate", "name"):
                return None  # column-label row

            candidate, candidate_url, is_winner, is_incumbent = \
                _parse_candidate_cell(tds[0])
            if not candidate:
                return None

            pct = self._clean(tds[1]) if len(tds) > 1 else ""
            if pct and "%" not in pct:
                try:
                    float(pct.replace(",", ""))
                    pct = pct + "%"
                except ValueError:
                    pct = ""

            votes = self._clean(tds[2]) if len(tds) > 2 else ""
            return _make_row(
                office, election_type, candidate, candidate_url,
                "", is_winner, is_incumbent, pct, votes,
            )

        def _parse_bptable(table) -> None:
            """Parse a bptable.gray.collapsible (title row + column labels + data)."""
            trs = table.xpath(".//tr")
            if not trs:
                return
            title_cells = trs[0].xpath("./th | ./td[@colspan]")
            if not title_cells:
                return
            title_text = self._clean(title_cells[0])
            election_type = self._infer_election_type(title_text)
            for tr in trs[1:]:
                row = _parse_data_row(tr, title_text, election_type)
                if row is not None:
                    results.append(row)

        def _preceding_office(elem) -> str:
            """Return the nearest preceding h3–h5 text in the document."""
            for tag in ("h5", "h4", "h3"):
                prev = elem.xpath(f"preceding-sibling::{tag}")
                if prev:
                    return self._clean(prev[-1])
            parent = elem.getparent()
            if parent is not None:
                for tag in ("h5", "h4", "h3"):
                    prev = parent.xpath(f"preceding-sibling::{tag}")
                    if prev:
                        return self._clean(prev[-1])
            return ""

        for tbl in doc.xpath(
            "//table[contains(@class,'bptable') or contains(@class,'wikitable')]"
        ):
            if not _is_vote_table(tbl):
                continue
            if "bptable" in (tbl.get("class") or ""):
                _parse_bptable(tbl)
            else:
                office = _preceding_office(tbl)
                election_type = self._infer_election_type(office)
                for tr in tbl.xpath(".//tr"):
                    row = _parse_data_row(tr, office, election_type)
                    if row is not None:
                        results.append(row)

        return results

    # ------------------------------------------------------------------
    # Phase 1 — public API
    # ------------------------------------------------------------------

    def get_election_links(
        self,
        year: int,
        race_type: str = "all",
        state: Optional[str] = None,
    ) -> List[MunicipalElectionLink]:
        """Fetch the index page and return all discovered sub-URLs.

        Parameters
        ----------
        year : int
            Election year.
        race_type : str
            ``"mayoral"`` → United_States_mayoral_elections page (2020–2025).
            ``"all"`` → United_States_municipal_elections page (2014–2025).
        state : str, optional
            If given, return only links whose state matches (case-insensitive).

        Returns
        -------
        List[MunicipalElectionLink]
        """
        url = self.build_index_url(year, race_type)
        page_html = self._get_html(url)
        if page_html is None:
            print(f"  No data available for {race_type} elections {year}.")
            return []

        links = self._extract_links(page_html, year)

        if state:
            state_lower = state.strip().lower()
            links = [l for l in links if l.state.lower() == state_lower]

        return links

    def get_election_links_to_dataframe(
        self,
        year: int,
        race_type: str = "all",
        state: Optional[str] = None,
    ):
        """Same as ``get_election_links`` but returns a pandas DataFrame."""

        links = self.get_election_links(year=year, race_type=race_type, state=state)
        if not links:
            return pd.DataFrame()
        return pd.DataFrame.from_records([asdict(l) for l in links])

    def get_all_years_links(
        self,
        start_year: int,
        end_year: int,
        race_type: str = "all",
        state: Optional[str] = None,
    ) -> List[MunicipalElectionLink]:
        """Iterate year by year and concatenate all discovered links.

        Parameters
        ----------
        start_year : int
            First year to scrape (inclusive).
        end_year : int
            Last year to scrape (inclusive).
        race_type : str
            ``"mayoral"`` or ``"all"`` (default).
        state : str, optional
            Optional state filter.

        Returns
        -------
        List[MunicipalElectionLink]
        """
        all_links: List[MunicipalElectionLink] = []
        for year in range(start_year, end_year + 1):
            print(f"  [{race_type}] Fetching index for {year}...")
            year_links = self.get_election_links(
                year=year, race_type=race_type, state=state
            )
            print(f"    → {len(year_links)} links")
            all_links.extend(year_links)
        return all_links

    def get_all_years_links_to_dataframe(
        self,
        start_year: int,
        end_year: int,
        race_type: str = "all",
        state: Optional[str] = None,
    ):
        """Same as ``get_all_years_links`` but returns a pandas DataFrame."""

        links = self.get_all_years_links(
            start_year=start_year, end_year=end_year,
            race_type=race_type, state=state,
        )
        if not links:
            return pd.DataFrame()
        return pd.DataFrame.from_records([asdict(l) for l in links])

    # ------------------------------------------------------------------
    # Phase 2 — public API
    # ------------------------------------------------------------------

    def scrape_location(
        self,
        year: int,
        location: str,
        state: str,
        race_type: str = "mayoral",
    ) -> List[MunicipalElectionRow]:
        """Scrape one location's election results without fetching the index.

        Builds the sub-URL from parts and parses the election page directly.

        Parameters
        ----------
        year : int
        location : str
            City or county name (e.g. ``"Austin"``).
        state : str
            State name (e.g. ``"Texas"``).
        race_type : str
            Determines the URL pattern (``"mayoral"`` or ``"all"``).

        Returns
        -------
        List[MunicipalElectionRow]
        """
        loc_slug = location.strip().replace(" ", "_")
        st_slug  = state.strip().replace(" ", "_")

        if race_type == "mayoral":
            url_candidates = [
                (f"{_BASE_URL}/Mayoral_election_in_{loc_slug},_{st_slug}_({year})", "mayoral"),
            ]
        else:
            # For race_type="all", try several URL patterns in priority order:
            # county-level (Municipal_elections_in_) first, then city-level,
            # then mayoral-specific.
            url_candidates = [
                (f"{_BASE_URL}/Municipal_elections_in_{loc_slug},_{st_slug}_({year})", "county"),
                (f"{_BASE_URL}/City_elections_in_{loc_slug},_{st_slug}_({year})", "city"),
                (f"{_BASE_URL}/Mayoral_election_in_{loc_slug},_{st_slug}_({year})", "mayoral"),
            ]

        for url, location_type in url_candidates:
            page_html = self._get_html(url)
            if page_html is not None:
                return self._parse_election_page(
                    page_html, location, state, location_type, url, year
                )

        print(f"  No data available for {location}, {state} {year}.")
        return []

    def scrape_all(
        self,
        year: int,
        race_type: str = "all",
        state: Optional[str] = None,
    ) -> List[MunicipalElectionRow]:
        """Fetch index page and scrape all discovered sub-URLs.

        Parameters
        ----------
        year : int
        race_type : str
            ``"mayoral"`` or ``"all"`` (default).
        state : str, optional
            If given, only scrape locations in that state.

        Returns
        -------
        List[MunicipalElectionRow]
        """
        links = self.get_election_links(year=year, race_type=race_type, state=state)
        if not links:
            return []

        all_rows: List[MunicipalElectionRow] = []
        total = len(links)
        for i, link in enumerate(links, 1):
            print(f"  [{i}/{total}] {link.location}, {link.state} ...")
            page_html = self._get_html(link.url)
            if page_html is None:
                print(f"    No data available.")
                continue
            rows = self._parse_election_page(
                page_html,
                link.location,
                link.state,
                link.location_type,
                link.url,
                year,
            )
            print(f"    → {len(rows)} candidate rows")
            all_rows.extend(rows)

        return all_rows

    def scrape_all_to_dataframe(
        self,
        year: int,
        race_type: str = "all",
        state: Optional[str] = None,
    ):
        """Same as ``scrape_all`` but returns a pandas DataFrame."""

        rows = self.scrape_all(year=year, race_type=race_type, state=state)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame.from_records([asdict(r) for r in rows])

    def scrape_years_to_dataframe(
        self,
        start_year: int,
        end_year: int,
        race_type: str = "all",
        state: Optional[str] = None,
    ):
        """Scrape multiple years and concatenate results into a DataFrame.

        Parameters
        ----------
        start_year : int
        end_year : int
        race_type : str
            ``"mayoral"`` or ``"all"`` (default).
        state : str, optional

        Returns
        -------
        pandas.DataFrame
        """

        frames = []
        for year in range(start_year, end_year + 1):
            print(f"\n{'=' * 50}")
            print(f"  Scraping {race_type} elections {year}")
            print(f"{'=' * 50}")
            df = self.scrape_all_to_dataframe(
                year=year, race_type=race_type, state=state
            )
            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
