"""
Parse HTML from the Louisiana SOS Graphical election results pages.

HTML structure (confirmed from live site inspection):

  div.race-container.ng-scope          ← one rendered race (unrendered templates lack ng-scope)
    span.race-title-text               ← office / race title
    div.reporting-header
      span.ng-binding                  ← "Early & Absentee Reporting - N of N parishes"
      span.ng-binding                  ← "Election Day Reporting - N of N precincts"
    div.choice-container               ← one per candidate / ballot choice
      div.choice-outcome-votes
        div.choice-votes               ← vote total; commas in <span class="hidden-trans">
        div.choice-outcome             ← winner: has inline style + glyphicon-ok-sign
          span.visible-trans           ← winner has class "glyphicon glyphicon-ok-sign"
      div.choice-name                  ← "Candidate Name Party" (name + party combined)
      span.choice-percent              ← e.g. "60%"

Voter turnout
-------------
Turnout percentage is a page-level stat (not per-race).  The site typically
renders it in an element whose class contains "turnout", e.g.:

  <div class="voter-turnout ...">
    <span class="ng-binding">Voter Turnout - 42.50%</span>
  </div>

``_parse_turnout_pct(doc)`` tries multiple strategies and returns None when the
page does not publish turnout data.  The value is broadcast to every candidate
row from that page.  At the state-tab level the column is ``voter_turnout_pct``;
at the parish level it is ``parish_voter_turnout_pct``.

Winner detection
----------------
Winners are marked by AngularJS adding ``glyphicon-ok-sign`` to ``span.visible-trans``.
For races where no candidate has this icon (e.g. Presidential Electors slate), the
candidate with the highest vote count is marked as winner.

Entry points
------------
- ``parse_tab_results(html, tab_label, election)``      → statewide DataFrame
- ``parse_parish_results(html, parish_name, election)`` → parish-level DataFrame
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd
from lxml import html as lhtml

from .models import LaElectionInfo

# ── Party name list ────────────────────────────────────────────────────────────
# Sorted longest-first so the endswith check matches the most specific name.
# Add parties here as new ones are encountered; the fallback is the last word.
_KNOWN_PARTIES: list[str] = sorted(
    [
        "American Solidarity Party",
        "Constitution Party",
        "Godliness, Truth, Justice",
        "Justice For All",
        "Natural Law Party",
        "No Party Affiliation",
        "Reform Party",
        "Socialism and Liberation",
        "Socialist Workers Party",
        "We The People",
        "Democratic",
        "Democrat",
        "Green",
        "Independent",
        "Libertarian",
        "No Party",
        "Republican",
    ],
    key=len,
    reverse=True,
)

from office_level_utils import classify_office_level as _classify_office_level_by_name
from column_schemas import LA_STATE_COLS, LA_PARISH_COLS
from text_utils import normalize_party


def _classify_election_level(tab_label: Optional[str], office: str) -> str:
    """Return 'Federal', 'State', or 'Local' for a given tab + office combination.

    Tab-based rules (fast path):
      Congressional, Presidential → Federal
      Multiparish, Parish         → Local (parish = county-level)

    Office-name fallback (applied for Statewide and unknown tabs):
      Uses the shared classify_office_level from office_level_utils.
    """
    if tab_label:
        t = tab_label.lower().strip()
        if t.startswith("congressional") or t.startswith("presidential"):
            return "Federal"
        if t.startswith("multiparish") or t.startswith("parish"):
            return "Local"
        # "Statewide" and other tabs fall through to office-name check below.

    return _classify_office_level_by_name(office)


# ── Output column schemas ──────────────────────────────────────────────────────
# Partial schemas (no "state") used for intermediate DataFrame construction.
# The pipeline adds "state" and reindexes to LA_STATE_COLS/LA_PARISH_COLS via finalize_df().

_STATE_COLS  = [c for c in LA_STATE_COLS  if c != "state"]
_PARISH_COLS = [c for c in LA_PARISH_COLS if c != "state"]

# ── Party normalization ────────────────────────────────────────────────────────

def _normalize_party(party: Optional[str]) -> Optional[str]:
    """Normalize a party string, returning None for blank-ballot markers."""
    if not party:
        return party
    stripped = party.strip()
    if stripped.startswith("(") and stripped.endswith(")"):
        stripped = stripped[1:-1].strip()
    if stripped.upper() in ("BLANKS", "BLANK"):
        return None  # blank ballots — not a candidate party
    result = normalize_party(stripped)
    return result if result else None


# ── Voter turnout ──────────────────────────────────────────────────────────────

# Matches the exact site pattern: "Turnout: 42.5%"
# The colon and space are fixed; the percentage may have 1–2 decimal places.
_TURNOUT_RE = re.compile(r"Turnout:\s*(\d+(?:\.\d+)?)\s*%")


def _parse_turnout_pct(doc: "lhtml.HtmlElement") -> Optional[float]:
    """Return the voter-turnout percentage from a rendered LA SOS page, or None.

    The site renders turnout as ``"Turnout: XX.X%"`` somewhere on the page.
    We do a single full-page text sweep for that pattern; returns None when
    not found (column will be null for that page).
    """
    full_text = " ".join(doc.text_content().split())
    m = _TURNOUT_RE.search(full_text)
    if m:
        try:
            return round(float(m.group(1)), 4)
        except ValueError:
            pass
    return None


# ── Low-level helpers ──────────────────────────────────────────────────────────

def _clean_int(text: str) -> Optional[int]:
    """Strip commas/whitespace and convert to int; return None on failure."""
    cleaned = re.sub(r"[,\s]", "", text or "")
    try:
        return int(cleaned)
    except ValueError:
        return None


def _clean_pct(text: str) -> Optional[float]:
    """Strip '%' and whitespace and convert to float; return None on failure."""
    cleaned = re.sub(r"[%\s]", "", text or "")
    try:
        return round(float(cleaned), 4)
    except ValueError:
        return None


def _split_candidate_party(text: str) -> tuple[str, Optional[str]]:
    """Split a combined candidate+party string into (candidate, party).

    The live site renders both in one div, e.g.:
      "Donald J. Trump, 'JD' Vance Republican"
      "Peter Sonski, Lauren Onak American Solidarity Party"
      "YES" / "NO"  (ballot referenda — party will be None)

    Strategy: check known party list (longest-first), then fall back to last word.
    """
    text = " ".join(text.split())
    if not text:
        return "", None

    for party in _KNOWN_PARTIES:
        if text.endswith(party):
            candidate = text[: -len(party)].strip()
            return candidate, _normalize_party(party)

    # Fallback: treat last whitespace-separated token as party.
    parts = text.rsplit(None, 1)
    if len(parts) == 2 and len(parts[1]) > 1:
        return parts[0], _normalize_party(parts[1])

    return text, None


def _parse_reporting(reporting_div) -> tuple[Optional[int], Optional[int]]:
    """Extract (precincts_reporting, precincts_expected) from the reporting div."""
    if reporting_div is None:
        return None, None
    text = " ".join(reporting_div.text_content().split())
    m = re.search(
        r"Election Day Reporting\s*-\s*(\d[\d,]*)\s+of\s+(\d[\d,]*)",
        text,
        re.IGNORECASE,
    )
    if m:
        return _clean_int(m.group(1)), _clean_int(m.group(2))
    return None, None


# ── Core HTML parser ───────────────────────────────────────────────────────────

def _parse_results_from_doc(
    doc: "lhtml.HtmlElement",
    election: LaElectionInfo,
    tab: Optional[str],
    parish: Optional[str],
) -> list[dict]:
    """Extract all candidate rows from a rendered results page.

    Winner detection
    ----------------
    First checks for ``glyphicon-ok-sign`` on ``span.visible-trans`` inside
    each choice.  If no choice in a race has that icon (e.g. Presidential
    Electors slate), the candidate with the highest vote total is marked winner.

    Parameters
    ----------
    doc : lhtml.HtmlElement
    election : LaElectionInfo
    tab : str | None   — tab label for statewide rows; None for parish rows
    parish : str | None — parish name for parish rows; None for statewide rows

    Returns
    -------
    list of dicts, one per candidate / ballot choice.
    """
    all_rows: list[dict] = []

    # Rendered race containers carry "ng-scope"; unrendered AngularJS templates do not.
    race_divs = doc.xpath(
        ".//div[contains(@class,'race-container') and contains(@class,'ng-scope')]"
    )

    for race_div in race_divs:
        # ── Race title ───────────────────────────────────────────────────────
        title_spans = race_div.xpath(".//span[contains(@class,'race-title-text')]")
        if not title_spans:
            continue
        raw_office = " ".join(title_spans[0].text_content().split())
        if not raw_office:
            continue

        if " -- " in raw_office:
            office, district = raw_office.split(" -- ", 1)
            office = office.strip()
            district = district.strip()
        else:
            office = raw_office
            district = None

        election_level = _classify_election_level(tab, office)

        # ── Reporting ────────────────────────────────────────────────────────
        reporting_divs = race_div.xpath(".//div[contains(@class,'reporting-header')]")
        reporting_div = reporting_divs[0] if reporting_divs else None
        precincts_reporting, precincts_expected = _parse_reporting(reporting_div)

        # ── Choices / candidates ─────────────────────────────────────────────
        choice_divs = race_div.xpath(".//div[contains(@class,'choice-container')]")
        race_rows: list[dict] = []

        for choice_div in choice_divs:
            # Votes — text_content() collapses hidden-trans spans' commas correctly.
            vote_divs = choice_div.xpath(".//div[contains(@class,'choice-votes')]")
            votes: Optional[int] = None
            if vote_divs:
                votes = _clean_int(vote_divs[0].text_content())

            # Candidate name + party (combined in one div on the live site).
            name_divs = choice_div.xpath(".//div[contains(@class,'choice-name')]")
            candidate_party = (
                " ".join(name_divs[0].text_content().split()) if name_divs else ""
            )
            candidate, party = _split_candidate_party(candidate_party)

            # Vote percentage.
            pct_spans = choice_div.xpath(".//span[contains(@class,'choice-percent')]")
            vote_pct: Optional[float] = None
            if pct_spans:
                vote_pct = _clean_pct(pct_spans[0].text_content())

            # Winner marker: AngularJS adds "glyphicon-ok-sign" to span.visible-trans
            # for the winning choice.  Confirmed from live HTML inspection.
            has_winner_icon = bool(
                choice_div.xpath(".//span[contains(@class,'glyphicon-ok-sign')]")
            )

            if not candidate:
                continue

            row: dict = {
                "election_name":      election.name,
                "election_year":      election.year,
                "election_date":      election.election_date,
                "office_level":       election_level,
                "office":             office,
                "district":           district,
                "candidate":          candidate,
                "party":              party,
                "votes":              votes,
                "vote_pct":           vote_pct,
                "_has_winner_icon":   has_winner_icon,
                "precincts_reporting": precincts_reporting,
                "precincts_expected": precincts_expected,
            }
            if tab is not None:
                row["tab"] = tab
            if parish is not None:
                row["parish"] = parish
            race_rows.append(row)

        # ── Resolve winner for this race ─────────────────────────────────────
        any_icon = any(r["_has_winner_icon"] for r in race_rows)

        for row in race_rows:
            if any_icon:
                # At least one candidate has the winner icon — use it directly.
                row["winner"] = row["_has_winner_icon"]
            else:
                # No icon found (e.g. Presidential Electors slate, unresolved race).
                # Fall back: mark the highest vote-getter as winner.
                valid_votes = [r["votes"] for r in race_rows if r["votes"] is not None]
                if valid_votes:
                    max_v = max(valid_votes)
                    row["winner"] = (row["votes"] == max_v and max_v > 0)
                else:
                    row["winner"] = False
            del row["_has_winner_icon"]

        all_rows.extend(race_rows)

    return all_rows


# ── Public entry points ────────────────────────────────────────────────────────

def parse_tab_results(html_str: str, tab_label: str, election: LaElectionInfo) -> pd.DataFrame:
    """Parse the rendered HTML for a non-Parish tab and return a DataFrame.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the tab content.
    tab_label : str
        Tab name (e.g. ``'Statewide'``, ``'Congressional'``).
    election : LaElectionInfo
        Election metadata.

    Returns
    -------
    pd.DataFrame with columns matching ``_STATE_COLS``.
    ``voter_turnout_pct`` is the same value for every row from this page
    (it is a page-level stat).  Null when not published by the site.
    """
    if not html_str:
        return pd.DataFrame(columns=_STATE_COLS)

    doc = lhtml.fromstring(html_str)
    turnout_pct = _parse_turnout_pct(doc)
    rows = _parse_results_from_doc(doc, election, tab=tab_label, parish=None)

    if not rows:
        return pd.DataFrame(columns=_STATE_COLS)

    for row in rows:
        row["voter_turnout_pct"] = turnout_pct

    df = pd.DataFrame(rows)
    for col in _STATE_COLS:
        if col not in df.columns:
            df[col] = None
    return df[_STATE_COLS]


def parse_parish_results(
    html_str: str, parish_name: str, election: LaElectionInfo
) -> pd.DataFrame:
    """Parse the rendered HTML for one parish and return a DataFrame.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the Parish tab after selecting a specific parish.
    parish_name : str
        Human-readable parish name (e.g. ``'Acadia Parish'``).
    election : LaElectionInfo
        Election metadata.

    Returns
    -------
    pd.DataFrame with columns matching ``_PARISH_COLS``.
    ``parish_voter_turnout_pct`` is the turnout for this specific parish page.
    It is the same value for every row from the page (page-level stat).
    Null when not published by the site.
    """
    if not html_str:
        return pd.DataFrame(columns=_PARISH_COLS)

    doc = lhtml.fromstring(html_str)
    turnout_pct = _parse_turnout_pct(doc)
    rows = _parse_results_from_doc(doc, election, tab=None, parish=parish_name)

    if not rows:
        return pd.DataFrame(columns=_PARISH_COLS)

    for row in rows:
        row["parish_voter_turnout_pct"] = turnout_pct

    df = pd.DataFrame(rows)
    if "winner" in df.columns:
        df = df.rename(columns={"winner": "parish_winner"})
    for col in _PARISH_COLS:
        if col not in df.columns:
            df[col] = None
    return df[_PARISH_COLS]
