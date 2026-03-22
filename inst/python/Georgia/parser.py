"""
Parse Georgia SOS election results pages into DataFrames.

Both the statewide election page and per-county election pages share the same
Angular component structure (``p-panel.ballot-item`` → ``div.ballot-option``).
This module provides:

  parse_state_results(html_str, election_info)
      Parses the main election page.
      Returns (state_df, county_urls).  If the HTML was rendered with vote-method
      tables expanded (via ``GaPlaywrightClient.get_election_page_with_vote_methods``),
      also returns a non-empty ``vote_method_df``.

  parse_county_results(html_str, county_name, election_info)
      Parses one county-level election page.
      Returns (county_df, vote_method_df) in the same fashion.

HTML structure (both page types)
---------------------------------
Page header (election-level metadata)::

  <div class="election-info">
    <div class="election-header">
      <h1 class="h4">November General Election</h1>
      <span class="h6">November 5, 2024</span>          ← election_date
    </div>
    <div class="status-info">
      <h4 class="h6 text-danger">OFFICIAL RESULTS</h4>  ← result_status
    </div>
  </div>

Per-contest panel (normal / bar-chart view)::

  <p-panel class="ballot-item …" id="<uuid>">
    <div class="contest-header">
      <h1 class="panel-header h3">
        <span>President of the US</span>      ← office name (state page)
        <a href="…">Fulton County</a>         ← county name (county page)
      </h1>
      <div class="h6"><span>Vote for 1</span></div>    ← vote_for
    </div>
    <!-- repeated once per candidate -->
    <div class="ballot-option">
      <div class="me-2">Donald J. Trump (Rep)</div>    ← name + optional "(I)" + party
      <div class="text-muted small">Rep</div>           ← party only
      <div class="percentage …"><span> 50.73% </span></div>
      <div class="vote-total …"><span> 2,663,117 </span></div>
    </div>
    <!-- panel footer -->
    <div class="footer-container">
      <span class="units-reporting">Localities reporting</span>
      <span class="fw-bold"> 159/159 </span>           ← localities_reporting
    </div>
  </p-panel>

Per-contest panel (vote-method expanded view)::

  <p-panel class="ballot-item …">
    …contest-header same as above…
    <table class="table contest-table">
      <thead>
        <tr>
          <th>Candidate</th>
          <th>Advance in Person</th>
          <th>Election Day</th>
          <th>Absentee by Mail</th>
          <th>Provisional</th>
          <th>Total Votes</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td><div class="candidate">Donald J. Trump (Rep)</div>…</td>
          <td><span> 1,916,442 </span></td>   ← Advance in Person
          <td><span>   647,080 </span></td>   ← Election Day
          <td><span>    98,151 </span></td>   ← Absentee by Mail
          <td><span>     1,444 </span></td>   ← Provisional
          <td><span> 2,663,117 </span></td>   ← Total Votes
        </tr>
        …
      </tbody>
      <tfoot><tr><td>Totals</td>…</tr></tfoot>  ← skipped
    </table>
  </p-panel>
"""

from __future__ import annotations

import re

import pandas as pd
from lxml import html as lhtml

from .models import GaElectionInfo

# ---------------------------------------------------------------------------
# Output column definitions
# ---------------------------------------------------------------------------
_STATE_COLS = [
    "election_name",
    "election_year",
    "election_slug",
    "election_date",
    "result_status",
    "office",
    "vote_for",
    "localities_reporting",
    "candidate",
    "party",
    "is_winner",
    "is_incumbent",
    "votes",
    "pct",
]

_COUNTY_COLS = [
    "election_name",
    "election_year",
    "election_slug",
    "election_date",
    "result_status",
    "county",
    "office",
    "vote_for",
    "localities_reporting",
    "candidate",
    "party",
    "is_winner",
    "is_incumbent",
    "votes",
    "pct",
]

# Vote-method breakdown DataFrame columns (state-level)
_VM_STATE_COLS = [
    "election_name",
    "election_year",
    "election_slug",
    "election_date",
    "result_status",
    "office",
    "vote_for",
    "localities_reporting",
    "candidate",
    "party",
    "is_incumbent",
    "votes_advance_in_person",
    "votes_election_day",
    "votes_absentee",
    "votes_provisional",
    "votes_total",
]

# Vote-method breakdown DataFrame columns (county-level)
_VM_COUNTY_COLS = [
    "election_name",
    "election_year",
    "election_slug",
    "election_date",
    "result_status",
    "county",
    "office",
    "vote_for",
    "localities_reporting",
    "candidate",
    "party",
    "is_incumbent",
    "votes_advance_in_person",
    "votes_election_day",
    "votes_absentee",
    "votes_provisional",
    "votes_total",
]

# Regex to strip trailing " (Party)" suffix from candidate name strings
_PARTY_SUFFIX_RE = re.compile(r"\s*\([^)]+\)\s*$")
# Regex to capture trailing " - Party" suffix (e.g. "Star Black - Rep")
_DASH_PARTY_RE = re.compile(r"\s+-\s+(\S+)\s*$")
# Regex to parse "X/Y" localities-reporting string
_REPORTING_RE = re.compile(r"(\d+)\s*/\s*(\d+)")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _clean(text: str | None) -> str:
    return " ".join((text or "").split())


def _parse_votes(text: str) -> int | None:
    cleaned = re.sub(r"[^\d]", "", text)
    return int(cleaned) if cleaned else None


def _parse_pct(text: str) -> float | None:
    cleaned = re.sub(r"[^\d.]", "", text)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _parse_page_meta(doc) -> dict:
    """Extract election-level metadata from the page header.

    Returns a dict with keys ``election_date`` and ``result_status``.
    Both default to ``None`` if not found.
    """
    date_spans = doc.xpath(
        "//*[contains(@class,'election-header')]"
        "//*[contains(@class,'h6')]"
    )
    election_date = _clean(date_spans[0].text_content()) if date_spans else None

    status_h4s = doc.xpath(
        "//*[contains(@class,'status-info')]//*[self::h4 or self::h3 or self::h2]"
    )
    result_status = _clean(status_h4s[0].text_content()) if status_h4s else None

    return {"election_date": election_date, "result_status": result_status}


def _panel_office(panel) -> str:
    """Extract the office/contest name from a ballot-item panel's h1."""
    spans = panel.xpath(
        ".//h1[contains(@class,'panel-header')]"
        "/span[not(contains(@class,'visually-hidden'))]"
    )
    if spans:
        return _clean(spans[0].text_content())
    h1s = panel.xpath(".//h1[contains(@class,'panel-header')]")
    return _clean(h1s[0].text_content()) if h1s else ""


def _panel_vote_for(panel) -> int | None:
    """Extract the number of seats to fill from 'Vote for N' in the panel header."""
    spans = panel.xpath(
        ".//*[contains(@class,'contest-header')]"
        "//*[contains(@class,'h6')]"
        "/span[contains(text(),'Vote for')]"
    )
    if not spans:
        spans = panel.xpath(
            ".//*[contains(@class,'h6')]/span[contains(text(),'Vote for')]"
        )
    if spans:
        m = re.search(r"Vote for\s+(\d+)", spans[0].text_content(), re.I)
        if m:
            return int(m.group(1))
    return None


def _panel_localities_reporting(panel) -> str | None:
    """Extract 'X/Y' localities-reporting string from the panel footer."""
    bold_spans = panel.xpath(
        ".//*[contains(@class,'footer-container')]"
        "//*[contains(@class,'fw-bold')]"
    )
    for span in bold_spans:
        txt = _clean(span.text_content())
        if _REPORTING_RE.search(txt):
            return txt
    return None


def _parse_candidate_name(raw_name: str) -> tuple[str, str, bool]:
    """Return (clean_candidate_name, inline_party, is_incumbent) from a raw name string.

    Georgia marks incumbents with "(I)" inline, e.g. "Earl Carter (I) (Rep)".
    Party may appear as a parenthesised suffix "(Rep)" or a dash suffix "- Rep".
    The cleaned name has both the party suffix and any trailing "(I)" stripped.
    ``inline_party`` is the party extracted from the name string, or ``""`` if
    none was found (use the dedicated HTML party element in that case).
    """
    is_incumbent = "(I)" in raw_name
    inline_party = ""

    # Try "(Party)" suffix first
    m_paren = _PARTY_SUFFIX_RE.search(raw_name)
    if m_paren:
        candidate = _PARTY_SUFFIX_RE.sub("", raw_name).strip()
    else:
        # Try " - Party" suffix (e.g. "Star Black - Rep")
        m_dash = _DASH_PARTY_RE.search(raw_name)
        if m_dash:
            inline_party = m_dash.group(1)
            candidate = raw_name[: m_dash.start()].strip()
        else:
            candidate = raw_name

    candidate = re.sub(r"\s*\(I\)\s*$", "", candidate).strip()
    return candidate, inline_party, is_incumbent


def _parse_ballot_options(panel) -> list[dict]:
    """Return candidate dicts from the bar-chart (ballot-option) view.

    ``is_winner`` is always ``None`` — the GA SOS site does not expose a winner
    marker in its rendered HTML.
    """
    results = []
    for opt in panel.xpath(".//*[contains(@class,'ballot-option')]"):
        name_divs = opt.xpath(
            ".//*[contains(@class,'me-2') and not(contains(@class,'party-marker'))]"
        )
        raw_name = _clean(name_divs[0].text_content()) if name_divs else ""
        candidate, inline_party, is_incumbent = _parse_candidate_name(raw_name)
        if not candidate:
            continue

        party_divs = opt.xpath(
            ".//*[contains(@class,'text-muted') and contains(@class,'small')]"
        )
        party = _clean(party_divs[0].text_content()) if party_divs else ""
        if not party:
            party = inline_party

        pct_spans = opt.xpath(
            ".//*[contains(@class,'percentage')]"
            "/span[not(contains(@class,'visually-hidden'))]"
        )
        pct = _parse_pct(_clean(pct_spans[0].text_content())) if pct_spans else None

        vote_spans = opt.xpath(
            ".//*[contains(@class,'vote-total')]"
            "/span[not(contains(@class,'visually-hidden'))]"
        )
        votes = _parse_votes(_clean(vote_spans[0].text_content())) if vote_spans else None

        results.append({
            "candidate":    candidate,
            "party":        party,
            "is_winner":    None,
            "is_incumbent": is_incumbent,
            "votes":        votes,
            "pct":          pct,
        })
    return results


def _parse_contest_table(panel) -> list[dict]:
    """Return candidate dicts from the vote-method expanded table view.

    Expected columns (in order): Candidate, Advance in Person, Election Day,
    Absentee by Mail, Provisional, [optional extra], Total Votes.

    The ``<tfoot>`` Totals row is skipped.
    """
    tables = panel.xpath(".//*[contains(@class,'contest-table')]")
    if not tables:
        return []

    table = tables[0]

    # Parse column order from <thead> — keyed lowercase for case-insensitive lookup
    headers = [_clean(th.text_content()) for th in table.xpath(".//thead//th")]
    col_map = {h.lower(): i for i, h in enumerate(headers)}

    def _get_col(cells, name: str) -> int | None:
        idx = col_map.get(name.lower())
        if idx is None or idx >= len(cells):
            return None
        return _parse_votes(_clean(cells[idx].text_content()))

    results = []
    for tr in table.xpath(".//tbody/tr"):
        cells = tr.xpath("td")
        if not cells:
            continue
        # Skip the tfoot Totals row (it's in tbody in some renders)
        first_cell_text = _clean(cells[0].text_content())
        if first_cell_text.lower() == "totals":
            continue

        # Candidate name and party from first cell.
        # Use word-boundary check to avoid matching "candidate-info".
        name_div = cells[0].xpath(
            ".//*[contains(concat(' ',normalize-space(@class),' '),' candidate ')]"
        )
        raw_name = _clean(name_div[0].text_content()) if name_div else first_cell_text
        candidate, inline_party, is_incumbent = _parse_candidate_name(raw_name)
        if not candidate:
            continue

        party_div = cells[0].xpath(
            ".//*[contains(@class,'text-muted') and contains(@class,'small')]"
        )
        party = _clean(party_div[0].text_content()) if party_div else ""
        if not party:
            party = inline_party

        results.append({
            "candidate":        candidate,
            "party":            party,
            "is_incumbent":     is_incumbent,
            "votes_advance_in_person":   _get_col(cells, "Advance in Person"),
            "votes_election_day": _get_col(cells, "Election Day"),
            "votes_absentee":   _get_col(cells, "Absentee by Mail"),
            "votes_provisional": _get_col(cells, "Provisional"),
            "votes_total":      _get_col(cells, "Total Votes"),
        })
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_state_results(
    html_str: str,
    election_info: GaElectionInfo,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """Parse the main election results page.

    Handles both normal (bar-chart) and vote-method-expanded HTML.  If the page
    was fetched via ``get_election_page_with_vote_methods``, panels will have a
    ``contest-table`` instead of ``ballot-option`` divs and ``vote_method_df``
    will be populated.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the election's main results page.
    election_info : GaElectionInfo
        Election metadata (name, year, slug, url).

    Returns
    -------
    state_df : pd.DataFrame
        One row per candidate per contest (bar-chart data).  Columns:
        ``election_name``, ``election_year``, ``election_slug``,
        ``election_date``, ``result_status``, ``office``, ``vote_for``,
        ``localities_reporting``, ``candidate``, ``party``, ``is_winner``
        (always ``None``), ``is_incumbent``, ``votes``, ``pct``.
        Empty when all panels are in vote-method-expanded mode.
    vote_method_df : pd.DataFrame
        One row per candidate per contest from vote-method tables.  Columns:
        ``election_name``, ``election_year``, ``election_slug``,
        ``election_date``, ``result_status``, ``office``, ``vote_for``,
        ``localities_reporting``, ``candidate``, ``party``, ``is_incumbent``,
        ``votes_advance_in_person``, ``votes_election_day``, ``votes_absentee``,
        ``votes_provisional``, ``votes_total``.
        Empty when the page was fetched without vote-method expansion.
    county_urls : list[str]
        Ordered list of per-county page URLs from the locality dropdown.
    """
    doc = lhtml.fromstring(html_str)
    page_meta = _parse_page_meta(doc)
    state_rows: list[dict] = []
    vm_rows: list[dict] = []

    panels = doc.xpath("//p-panel[contains(@class,'ballot-item')]")
    if not panels:
        print(
            f"[GA parser] WARNING: No ballot-item panels found for "
            f"'{election_info.name}'. The page structure may have changed."
        )
        return (
            pd.DataFrame(columns=_STATE_COLS),
            pd.DataFrame(columns=_VM_STATE_COLS),
            [],
        )

    for panel in panels:
        office = _panel_office(panel)
        if not office:
            continue
        vote_for = _panel_vote_for(panel)
        localities_reporting = _panel_localities_reporting(panel)
        base = {
            "election_name":        election_info.name,
            "election_year":        election_info.year,
            "election_slug":        election_info.slug,
            "election_date":        page_meta["election_date"],
            "result_status":        page_meta["result_status"],
            "office":               office,
            "vote_for":             vote_for,
            "localities_reporting": localities_reporting,
        }

        # Vote-method table takes priority when present
        vm_cands = _parse_contest_table(panel)
        if vm_cands:
            for cand in vm_cands:
                vm_rows.append({**base, **cand})
        else:
            for cand in _parse_ballot_options(panel):
                state_rows.append({**base, **cand})

    state_df = (
        pd.DataFrame(state_rows, columns=_STATE_COLS)
        if state_rows else pd.DataFrame(columns=_STATE_COLS)
    )
    vote_method_df = (
        pd.DataFrame(vm_rows, columns=_VM_STATE_COLS)
        if vm_rows else pd.DataFrame(columns=_VM_STATE_COLS)
    )

    # Extract county URLs from the locality dropdown
    county_links = doc.xpath(
        "//a[contains(@class,'dropdown-item') and contains(@href,'/elections/')]"
    )
    county_urls = [
        (f"https://results.sos.ga.gov{a.get('href')}"
         if a.get("href", "").startswith("/") else a.get("href", ""))
        for a in county_links
    ]

    if not county_urls:
        print(
            f"[GA parser] WARNING: No county dropdown links found for "
            f"'{election_info.name}'. County-level scraping will be skipped."
        )

    return state_df, vote_method_df, county_urls


def parse_county_results(
    html_str: str,
    county_name: str,
    election_info: GaElectionInfo,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse one county's election results page.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of a county-level election page.
    county_name : str
        Human-readable county name (e.g. ``"Fulton County"``).
    election_info : GaElectionInfo
        Election metadata shared with the state-level parse.

    Returns
    -------
    county_df : pd.DataFrame
        Bar-chart candidate rows.  Empty when all panels are vote-method expanded.
    vote_method_df : pd.DataFrame
        Vote-method breakdown rows.  Empty when not vote-method expanded.
    """
    doc = lhtml.fromstring(html_str)
    page_meta = _parse_page_meta(doc)
    county_rows: list[dict] = []
    vm_rows: list[dict] = []

    panels = doc.xpath("//p-panel[contains(@class,'ballot-item')]")
    for panel in panels:
        office = _panel_office(panel)
        if not office:
            continue
        vote_for = _panel_vote_for(panel)
        localities_reporting = _panel_localities_reporting(panel)
        base = {
            "election_name":        election_info.name,
            "election_year":        election_info.year,
            "election_slug":        election_info.slug,
            "election_date":        page_meta["election_date"],
            "result_status":        page_meta["result_status"],
            "county":               county_name,
            "office":               office,
            "vote_for":             vote_for,
            "localities_reporting": localities_reporting,
        }

        vm_cands = _parse_contest_table(panel)
        if vm_cands:
            for cand in vm_cands:
                vm_rows.append({**base, **cand})
        else:
            for cand in _parse_ballot_options(panel):
                county_rows.append({**base, **cand})

    county_df = (
        pd.DataFrame(county_rows, columns=_COUNTY_COLS)
        if county_rows else pd.DataFrame(columns=_COUNTY_COLS)
    )
    vote_method_df = (
        pd.DataFrame(vm_rows, columns=_VM_COUNTY_COLS)
        if vm_rows else pd.DataFrame(columns=_VM_COUNTY_COLS)
    )
    return county_df, vote_method_df


def county_name_from_url(url: str) -> str:
    """Derive a human-readable county name from a GA SOS county URL.

    Examples
    --------
    >>> county_name_from_url(".../results/public/fulton-county-ga/elections/...")
    'Fulton County'
    >>> county_name_from_url(".../results/public/jeff-davis-county-ga/elections/...")
    'Jeff Davis County'
    """
    m = re.search(r"/results/public/([^/]+)-ga/", url)
    if not m:
        return ""
    slug = m.group(1)
    return slug.replace("-", " ").title()
