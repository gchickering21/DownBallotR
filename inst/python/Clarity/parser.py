"""
Parse Angular/PrimeNG SOS election results pages into DataFrames.

Both the statewide election page and per-county election pages share the same
component structure (``p-panel.ballot-item`` → ``div.ballot-option``) across
all SOS sites built on this framework.

This module provides:

  parse_state_results(html_str, election_info)
      Parses the main election page.
      Returns (state_df, vote_method_df, county_urls).

  parse_county_results(html_str, county_name, election_info, url)
      Parses one county-level election page.
      Returns (county_df, vote_method_df).

  county_name_from_url(url, county_suffix)
      Derives a human-readable county name from a county page URL.
      ``county_suffix`` is the state-specific suffix (e.g. ``"-ga"`` or ``"-ut"``).

HTML structure (both page types)
---------------------------------
Page header::

  <div class="election-info">
    <div class="election-header">
      <h1 class="h4">Election Name</h1>
      <span class="h6">November 5, 2024</span>   ← election_date
    </div>
    <div class="status-info">
      <h4 class="h6 text-danger">OFFICIAL RESULTS</h4>  ← result_status
    </div>
  </div>

Per-contest panel (bar-chart view)::

  <p-panel class="ballot-item …">
    <div class="contest-header">
      <h1 class="panel-header h3"><span>Office Name</span></h1>
    </div>
    <div class="ballot-option">
      <div class="me-2">Candidate Name (Party)</div>
      <div class="text-muted small">Party</div>
      <div class="percentage …"><span> 50.73% </span></div>
      <div class="vote-total …"><span> 123,456 </span></div>
    </div>
    <div class="footer-container">
      <span class="fw-bold"> 29/29 </span>   ← localities_reporting
    </div>
  </p-panel>

Per-contest panel (vote-method expanded view)::

  <p-panel class="ballot-item …">
    …contest-header…
    <table class="table contest-table">
      <thead><tr>
        <th>Candidate</th><th>Advance in Person</th><th>Election Day</th>
        <th>Absentee by Mail</th><th>Provisional</th><th>Total Votes</th>
      </tr></thead>
      <tbody><tr>
        <td><div class="candidate">Name (Party)</div>…</td>
        <td><span> 1,234 </span></td>…
      </tr></tbody>
    </table>
  </p-panel>
"""

from __future__ import annotations

import re
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
from lxml import html as lhtml

from .models import ClarityElectionInfo
from office_level_utils import classify_office_level
from column_schemas import (
    CLARITY_STATE_COLS, CLARITY_COUNTY_COLS,
    CLARITY_VM_STATE_COLS, CLARITY_VM_COUNTY_COLS,
    compute_vote_pct,
)
from text_utils import normalize_party


# ---------------------------------------------------------------------------
# Output column definitions
# ---------------------------------------------------------------------------
# Partial schemas (no "state") for intermediate DataFrame construction.
_STATE_COLS    = [c for c in CLARITY_STATE_COLS    if c != "state"]
_COUNTY_COLS   = [c for c in CLARITY_COUNTY_COLS   if c != "state"]
_VM_STATE_COLS = [c for c in CLARITY_VM_STATE_COLS if c != "state"]
_VM_COUNTY_COLS = [c for c in CLARITY_VM_COUNTY_COLS if c != "state"]

_PARTY_SUFFIX_RE = re.compile(r"\s*\([^)]+\)\s*$")
_DASH_PARTY_RE   = re.compile(r"\s+-\s+(\S+)\s*$")
_REPORTING_RE    = re.compile(r"(\d+)\s*/\s*(\d+)")
_DATE_RE         = re.compile(r"\d{1,2}/\d{1,2}/\d{2,4}")

# Matches district info at the end of an office string, e.g.:
#   "State House of Representatives - District 119"
#   "State House District 75"  /  "State House Dist 68"
#   "State House 172 (Special)"  /  "State Senate 11 (Special)"
_OFFICE_DISTRICT_RE = re.compile(
    r"\s*(?:-\s*)?(?:District|Dist\.?)\s+(\d+)(\s*\([^)]+\))?\s*$"
    r"|"
    r"\s+(\d+)(\s*\([^)]+\))?\s*$",
    re.I,
)

# Normalize shortened office names to their canonical form
_OFFICE_ALIASES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^state\s+house$", re.I), "State House of Representatives"),
]


def _split_office_district(raw: str) -> tuple[str, str | None]:
    """Split a raw office string into (office, district).

    Examples
    --------
    'State House of Representatives - District 119' → ('State House of Representatives', 'District 119')
    'State House District 75'  → ('State House of Representatives', 'District 75')
    'State House Dist 68'      → ('State House of Representatives', 'District 68')
    'State House 172 (Special)'→ ('State House of Representatives', 'District 172 (Special)')
    'State Senate 11 (Special)'→ ('State Senate', 'District 11 (Special)')
    """
    name = raw.strip()
    district = None
    m = _OFFICE_DISTRICT_RE.search(name)
    if m:
        num     = m.group(1) or m.group(3)
        special = (m.group(2) or m.group(4) or "").strip()
        district = f"District {num}" + (f" {special}" if special else "")
        name = name[: m.start()].strip()
    for alias_re, canonical in _OFFICE_ALIASES:
        if alias_re.match(name):
            name = canonical
            break
    return name, district or None

# ---------------------------------------------------------------------------
# Election type classification
# ---------------------------------------------------------------------------
_ELECTION_TYPE_RULES: list[tuple[re.Pattern, str]] = [
    # Most specific special-election combinations first
    (re.compile(r"special.{0,30}primary.{0,30}runoff",              re.I), "special_primary_runoff"),
    (re.compile(r"special.{0,30}runoff|runoff.{0,30}special",       re.I), "special_runoff"),
    (re.compile(r"special.{0,30}primary",                           re.I), "special_primary"),
    (re.compile(r"special",                                         re.I), "special"),
    # Presidential primary before generic primary
    (re.compile(r"presidential.{0,30}(preference|primary)",         re.I), "presidential_primary"),
    # Primary+Runoff before either alone
    (re.compile(r"primary.{0,50}runoff|runoff.{0,50}primary",       re.I), "primary_runoff"),
    (re.compile(r"general.{0,30}runoff|runoff.{0,30}general",       re.I), "general_runoff"),
    (re.compile(r"primary",                                         re.I), "primary"),
    (re.compile(r"general",                                         re.I), "general"),
    # Catch-all for bare "Runoff" (e.g. district-specific runoffs)
    (re.compile(r"runoff",                                          re.I), "runoff"),
    (re.compile(r"recount",                                         re.I), "recount"),
]


def _classify_election_type(name: str) -> str:
    for pattern, label in _ELECTION_TYPE_RULES:
        if pattern.search(name):
            return label
    return "other"


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


def _format_election_date(raw: str | None) -> str | None:
    """Parse a date string like 'November 5, 2024' and return 'MM/DD/YY'."""
    if not raw:
        return None
    # Normalize multi-space and strip so "November  5, 2024" works too.
    normalized = re.sub(r"\s+", " ", raw).strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(normalized, fmt).strftime("%m/%d/%y")
        except ValueError:
            continue
    return raw  # fall back to raw if unparseable


def _parse_page_meta(doc) -> dict:
    """Extract election_date and result_status from the page header."""
    date_spans = doc.xpath(
        "//*[contains(@class,'election-header')]//*[contains(@class,'h6')]"
    )
    raw_date = _clean(date_spans[0].text_content()) if date_spans else None
    election_date = _format_election_date(raw_date)

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


def _panel_localities_reporting(panel) -> str | None:
    """Extract 'X/Y' localities-reporting string from the panel footer."""
    bold_spans = panel.xpath(
        ".//*[contains(@class,'footer-container')]//*[contains(@class,'fw-bold')]"
    )
    for span in bold_spans:
        txt = _clean(span.text_content())
        if _DATE_RE.search(txt):
            continue
        if _REPORTING_RE.search(txt):
            return txt
    return None


def _parse_candidate_name(raw_name: str) -> tuple[str, str]:
    """Return (clean_candidate_name, inline_party)."""
    inline_party = ""

    m_paren = _PARTY_SUFFIX_RE.search(raw_name)
    if m_paren:
        candidate = _PARTY_SUFFIX_RE.sub("", raw_name).strip()
    else:
        m_dash = _DASH_PARTY_RE.search(raw_name)
        if m_dash:
            inline_party = m_dash.group(1)
            candidate = raw_name[: m_dash.start()].strip()
        else:
            candidate = raw_name

    candidate = re.sub(r"\s*\(I\)\s*$", "", candidate).strip()
    return candidate, inline_party


def _parse_ballot_options(panel) -> list[dict]:
    """Return candidate dicts from the bar-chart (ballot-option) view."""
    results = []
    for opt in panel.xpath(".//*[contains(@class,'ballot-option')]"):
        name_divs = opt.xpath(
            ".//*[contains(@class,'me-2') and not(contains(@class,'party-marker'))]"
        )
        raw_name = _clean(name_divs[0].text_content()) if name_divs else ""
        candidate, inline_party = _parse_candidate_name(raw_name)
        if not candidate:
            continue

        party_divs = opt.xpath(
            ".//*[contains(@class,'text-muted') and contains(@class,'small')]"
        )
        party = normalize_party(_clean(party_divs[0].text_content()) if party_divs else "") or normalize_party(inline_party)

        pct_spans = opt.xpath(
            ".//*[contains(@class,'percentage')]"
            "/span[not(contains(@class,'visually-hidden'))]"
        )
        vote_pct = _parse_pct(_clean(pct_spans[0].text_content())) if pct_spans else None

        vote_spans = opt.xpath(
            ".//*[contains(@class,'vote-total')]"
            "/span[not(contains(@class,'visually-hidden'))]"
        )
        votes = _parse_votes(_clean(vote_spans[0].text_content())) if vote_spans else None

        results.append({
            "candidate": candidate,
            "party":     party,
            "winner":    None,
            "votes":     votes,
            "vote_pct":  vote_pct,
        })
    return results


def _parse_contest_table(panel) -> list[dict]:
    """Return candidate dicts from the vote-method expanded table view."""
    tables = panel.xpath(".//*[contains(@class,'contest-table')]")
    if not tables:
        return []

    table = tables[0]
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
        first_cell_text = _clean(cells[0].text_content())
        if first_cell_text.lower() == "totals":
            continue

        name_div = cells[0].xpath(
            ".//*[contains(concat(' ',normalize-space(@class),' '),' candidate ')]"
        )
        raw_name = _clean(name_div[0].text_content()) if name_div else first_cell_text
        candidate, inline_party = _parse_candidate_name(raw_name)
        if not candidate:
            continue

        party_div = cells[0].xpath(
            ".//*[contains(@class,'text-muted') and contains(@class,'small')]"
        )
        party = normalize_party(_clean(party_div[0].text_content()) if party_div else "") or normalize_party(inline_party)

        results.append({
            "candidate": candidate,
            "party":     party,
            "votes_advance_in_person": _get_col(cells, "Advance in Person"),
            "votes_election_day":      _get_col(cells, "Election Day"),
            "votes_absentee":          _get_col(cells, "Absentee by Mail"),
            "votes_provisional":       _get_col(cells, "Provisional"),
            "votes_total":             _get_col(cells, "Total Votes"),
        })
    return results


def _fix_clarity_winners(
    df: pd.DataFrame, group_cols: list[str], col: str = "winner"
) -> pd.DataFrame:
    """Derive winner: mark the top-voted candidate per contest as True.

    Clarity HTML does not expose a winner flag or num_seats, so we default to
    1 winner per contest (the candidate with the most votes).  Ties share the
    winner flag (rank method='min').
    """
    if df.empty or "votes" not in df.columns:
        return df
    valid = df["votes"].notna()
    if not valid.any():
        return df
    df = df.copy()
    # Use pandas nullable boolean so rows with no vote data stay NA (not False).
    # reticulate converts this to R logical, which supports NA natively.
    df[col] = pd.NA
    df[col] = df[col].astype("boolean")
    ranked = (
        df.loc[valid]
        .groupby(group_cols, dropna=False)["votes"]
        .rank(method="min", ascending=False)
    )
    df.loc[valid, col] = (ranked <= 1).astype("boolean")
    return df


def _fill_pct(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing vote_pct values within the correct contest+geography scope.

    Includes county and district in the grouping when present so that county-level
    calls use the per-county denominator rather than a statewide total.
    """
    group_cols = [
        c for c in ("election_name", "election_year", "office", "district", "county")
        if c in df.columns
    ]
    return compute_vote_pct(df, group_cols, fill_missing_only=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_state_results(
    html_str: str,
    election_info: ClarityElectionInfo,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """Parse the main election results page.

    The server base URL (for resolving relative county hrefs) is derived
    automatically from ``election_info.url``.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the election's main results page.
    election_info : ClarityElectionInfo
        Election metadata (name, year, slug, url).

    Returns
    -------
    state_df : pd.DataFrame
        One row per candidate per contest.
    vote_method_df : pd.DataFrame
        One row per candidate per contest from vote-method tables (empty when
        the page was not fetched with vote-method expansion).
    county_urls : list[str]
        Ordered list of per-county page URLs from the locality dropdown.
    """
    # Derive server base from the election URL for county URL resolution.
    parsed = urlparse(election_info.url)
    server_base = f"{parsed.scheme}://{parsed.netloc}"
    state_name = parsed.path.split("/")[3] if parsed.path.count("/") >= 3 else "SOS"

    doc = lhtml.fromstring(html_str)
    page_meta = _parse_page_meta(doc)
    state_rows: list[dict] = []
    vm_rows: list[dict] = []

    panels = doc.xpath("//p-panel[contains(@class,'ballot-item')]")
    if not panels:
        print(
            f"[{state_name} parser] WARNING: No ballot-item panels found for "
            f"'{election_info.name}'. The page structure may have changed."
        )
        return (
            pd.DataFrame(columns=_STATE_COLS),
            pd.DataFrame(columns=_VM_STATE_COLS),
            [],
        )

    for panel in panels:
        raw_office = _panel_office(panel)
        if not raw_office:
            continue
        office, district = _split_office_district(raw_office)
        base = {
            "election_name":        election_info.name,
            "election_type":        _classify_election_type(election_info.name),
            "election_year":        election_info.year,
            "election_date":        page_meta["election_date"],
            "office_level":         classify_office_level(raw_office),
            "office":               office,
            "district":             district,
            "url":                  election_info.url,
        }

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
    state_df = _fill_pct(state_df)
    state_df = _fix_clarity_winners(state_df, ["election_name", "election_year", "office", "district"])
    vote_method_df = (
        pd.DataFrame(vm_rows, columns=_VM_STATE_COLS)
        if vm_rows else pd.DataFrame(columns=_VM_STATE_COLS)
    )

    # Extract county URLs from the locality dropdown.
    county_links = doc.xpath(
        "//a[contains(@class,'dropdown-item') and contains(@href,'/elections/')]"
    )
    county_urls = [
        (f"{server_base}{a.get('href')}"
         if a.get("href", "").startswith("/") else a.get("href", ""))
        for a in county_links
    ]

    if not county_urls:
        print(
            f"[{state_name} parser] WARNING: No county dropdown links found for "
            f"'{election_info.name}'. County-level scraping will be skipped."
        )

    return state_df, vote_method_df, county_urls


def parse_county_results(
    html_str: str,
    county_name: str,
    election_info: ClarityElectionInfo,
    url: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse one county's election results page.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of a county-level election page.
    county_name : str
        Human-readable county name (e.g. ``"Fulton County"``).
    election_info : ClarityElectionInfo
        Election metadata shared with the state-level parse.
    url : str
        The county page URL (stored in the output DataFrame).

    Returns
    -------
    county_df : pd.DataFrame
        Bar-chart candidate rows.
    vote_method_df : pd.DataFrame
        Vote-method breakdown rows.
    """
    doc = lhtml.fromstring(html_str)
    page_meta = _parse_page_meta(doc)
    county_rows: list[dict] = []
    vm_rows: list[dict] = []

    panels = doc.xpath("//p-panel[contains(@class,'ballot-item')]")
    for panel in panels:
        raw_office = _panel_office(panel)
        if not raw_office:
            continue
        office, district = _split_office_district(raw_office)
        base = {
            "election_name":        election_info.name,
            "election_type":        _classify_election_type(election_info.name),
            "election_year":        election_info.year,
            "election_date":        page_meta["election_date"],
            "county":               county_name,
            "office_level":         classify_office_level(raw_office),
            "office":               office,
            "district":             district,
            "url":                  url,
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
    county_df = _fill_pct(county_df)
    county_df = _fix_clarity_winners(county_df, ["election_name", "election_year", "office", "district", "county"], col="county_winner")
    vote_method_df = (
        pd.DataFrame(vm_rows, columns=_VM_COUNTY_COLS)
        if vm_rows else pd.DataFrame(columns=_VM_COUNTY_COLS)
    )
    return county_df, vote_method_df


def county_name_from_url(url: str, county_suffix: str) -> str:
    """Derive a human-readable county name from a SOS county URL.

    Parameters
    ----------
    url : str
        County page URL, e.g.
        ``".../results/public/fulton-county-ga/elections/..."`` or
        ``".../results/public/salt-lake-county-ut/elections/..."``.
    county_suffix : str
        State-specific suffix used in county slugs, e.g. ``"-ga"`` or ``"-ut"``.

    Examples
    --------
    >>> county_name_from_url(".../results/public/fulton-county-ga/elections/...", "-ga")
    'Fulton County'
    >>> county_name_from_url(".../results/public/salt-lake-county-ut/elections/...", "-ut")
    'Salt Lake County'
    """
    suffix_escaped = re.escape(county_suffix)
    m = re.search(rf"/results/public/([^/]+){suffix_escaped}/", url)
    if not m:
        return ""
    slug = m.group(1)
    return slug.replace("-", " ").title()
