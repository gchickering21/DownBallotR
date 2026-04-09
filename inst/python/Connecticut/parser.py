"""
HTML parsers for the Connecticut CTEMS election results pages.

Verified HTML structure (from live site inspection)
-----------------------------------------------------
The CTEMS AngularJS app renders results using these CSS classes:

**Race/contest structure (statewide Summary view):**
  Each visible race has:
    <span class="resultssummarytitle">Presidential Electors for</span>
    ...inside a non-hidden ng-scope div...
    <table>
      <tr><td class="resultssummaryheader">CANDIDATE PARTY / NAME</td>
          <td class="resultssummaryheader">VOTES</td>
          <td class="resultssummaryheader">VOTES %</td></tr>
      <tr><td class="bggrey1" colspan="3"><strong>Connecticut 28</strong></td></tr>
      <tbody data-ng-repeat>
        <tr data-ng-repeat class="ng-scope">
          <td class="resultssummaryvalues">
            <img title="Democratic Party" .../>
            <span class="ng-binding">Harris and Walz</span>
          </td>
          <td class="resultssummaryvalues ng-binding">992,053</td>
          <td style="font-weight: bold" class="resultssummaryvalues ng-binding">56.40%</td>
        </tr>
      </tbody>
    </table>

**Key selectors:**
  - Race name:    span.resultssummarytitle  (skip if ancestor has class containing ng-hide)
  - District:     td.bggrey1 strong  (optional; empty string if absent)
  - Party:        td.resultssummaryvalues img @title  (e.g. "Democratic Party")
  - Candidate:    td.resultssummaryvalues span.ng-binding
  - Votes:        td.resultssummaryvalues.ng-binding  (2nd td in row)
  - Pct:          td[style*=font-weight]              (3rd td in row)

**Town view:**
  Same CSS classes — the page content changes when a town is selected but the
  HTML structure is identical.

Election-level classification
------------------------------
``classify_election_level(office)`` assigns each race to:
  - ``'Federal'`` — Presidential Electors, US Senator, Rep in Congress, etc.
  - ``'State'``   — Governor, Lt Gov, AG, State Senator, General Assembly, etc.
  - ``'Local'``   — everything else (Mayor, Selectman, Town Council, etc.)
"""

from __future__ import annotations

import re

import pandas as pd
from lxml import html as lhtml

from .models import CtElectionInfo

# ── Election-level classification ─────────────────────────────────────────────

_FEDERAL_RE = re.compile(
    r"presidential\s+elector"
    r"|united\s+states\s+senator"
    r"|u\.?\s*s\.?\s+senator"
    r"|senator\s+in\s+congress"
    r"|representative\s+in\s+congress"
    r"|u\.?\s*s\.?\s+representative"
    r"|congressman|congresswoman",
    re.IGNORECASE,
)

_STATE_RE = re.compile(
    r"\bgovernor\b"
    r"|lieutenant\s+governor|lt\.?\s+governor"
    r"|attorney\s+general"
    r"|secretary\s+of\s+(the\s+)?state"
    r"|state\s+treasurer|\btreasurer\b"
    r"|state\s+comptroller|\bcomptroller\b"
    r"|state\s+senator|senator\s+in\s+(the\s+)?general\s+assembly"
    r"|state\s+representative"
    r"|representative\s+to\s+(the\s+)?general\s+assembly"
    r"|general\s+assembly"
    r"|judge\s+of\s+probate",
    re.IGNORECASE,
)


def classify_election_level(office: str) -> str:
    """Classify an office name as ``'Federal'``, ``'State'``, or ``'Local'``."""
    if _FEDERAL_RE.search(office):
        return "Federal"
    if _STATE_RE.search(office):
        return "State"
    return "Local"


# ── Output column definitions ─────────────────────────────────────────────────

_STATE_COLS = [
    "election_name",
    "election_year",
    "election_date",
    "election_level",
    "office",
    "candidate",
    "party",
    "votes",
    "vote_pct",
    "contest_outcome",
]

_TOWN_COLS = [
    "election_name",
    "election_year",
    "election_date",
    "county",
    "town",
    "election_level",
    "office",
    "candidate",
    "party",
    "votes",
    "vote_pct",
]

# Columns that define a unique contest at the state level.
_CONTEST_STATE_COLS = ["election_name", "office"]

# ── Internal helpers ──────────────────────────────────────────────────────────

_COMMA_RE = re.compile(r",")
_PCT_RE   = re.compile(r"[\s%]+$")


def _clean(s: str | None) -> str:
    return " ".join((s or "").split()).strip()


def _parse_votes(s: str) -> int | None:
    c = _COMMA_RE.sub("", _clean(s))
    if not c or c == "-":
        return None
    try:
        return int(float(c))
    except ValueError:
        return None


def _parse_pct(s: str) -> float | None:
    c = _PCT_RE.sub("", _clean(s))
    if not c or c == "-":
        return None
    try:
        return float(c)
    except ValueError:
        return None


def _is_hidden(el) -> bool:
    """Return True if any ancestor element has class containing 'ng-hide'."""
    for ancestor in el.iterancestors():
        if "ng-hide" in (ancestor.get("class") or ""):
            return True
    return False


def _clean_party(img_title: str) -> str:
    """Strip trailing ' Party' from img title attributes (e.g. 'Democratic Party' → 'Democratic')."""
    return re.sub(r"\s+Party\s*$", "", img_title, flags=re.IGNORECASE).strip()


def _add_contest_outcome(df: pd.DataFrame, contest_cols: list[str]) -> pd.DataFrame:
    """Add ``contest_outcome`` column: ``'Won'`` for the max-votes candidate per
    contest, ``'Lost'`` for all others.  Ties both receive ``'Won'``.
    Candidates with no valid vote data receive ``None``.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with at least ``votes`` and the columns in *contest_cols*.
    contest_cols : list[str]
        Columns that together identify a unique contest (e.g. election + office).
    """
    if df.empty:
        df = df.copy()
        df["contest_outcome"] = pd.Series(dtype="object")
        return df
    df = df.copy()
    # transform("max") always returns a Series with the same index as df,
    # so boolean masks derived from it are always index-aligned.
    max_votes = df.groupby(contest_cols, dropna=False)["votes"].transform("max")
    has_votes = df["votes"].notna()
    df["contest_outcome"] = pd.NA  # Start as NA; fill below only for rows with data
    df.loc[has_votes, "contest_outcome"] = "Lost"
    df.loc[has_votes & (df["votes"] == max_votes), "contest_outcome"] = "Won"
    return df


def _extract_races(doc) -> list[tuple[str, list[dict]]]:
    """Extract all visible races from a rendered CTEMS page.

    Returns
    -------
    list of (race_name, candidate_rows)
        where each candidate_row dict has keys:
        district, candidate, party, votes_raw, pct_raw
    """
    race_title_spans = doc.xpath('//span[@class="resultssummarytitle"]')
    results = []

    for title_span in race_title_spans:
        if _is_hidden(title_span):
            continue

        race_name = _clean(title_span.text_content())
        if not race_name:
            continue

        # Navigate up to the nearest non-hidden ng-scope ancestor that contains
        # candidate tables.
        ancestor = title_span.getparent()
        container = None
        for _ in range(15):
            if ancestor is None:
                break
            cls = ancestor.get("class") or ""
            if "ng-scope" in cls and "ng-hide" not in cls:
                # Check it actually has a candidate table
                if ancestor.xpath('.//td[@class="resultssummaryheader"]'):
                    container = ancestor
                    break
            ancestor = ancestor.getparent()

        if container is None:
            continue

        # Find candidate tables in this container that are not hidden.
        cand_tables = [
            t for t in container.xpath('.//table[.//td[@class="resultssummaryheader"]]')
            if not _is_hidden(t)
        ]

        for ct in cand_tables:
            # District name from the bggrey1 row (may be empty).
            district_cells = ct.xpath('.//td[@class="bggrey1"]//text()')
            district = _clean(" ".join(district_cells))

            # Candidate rows: tr elements that have resultssummaryvalues tds.
            cand_rows = ct.xpath('.//tr[td[@class="resultssummaryvalues"]]')

            for tr in cand_rows:
                tds = tr.xpath('./td')
                if len(tds) < 2:
                    continue

                # Party from img title in first td.
                party_imgs = tds[0].xpath('.//img/@title')
                party = _clean_party(party_imgs[0]) if party_imgs else ""

                # Candidate name from span.ng-binding in first td.
                name_spans = tds[0].xpath('.//span[@class="ng-binding"]/text()')
                candidate = _clean(name_spans[0]) if name_spans else _clean(tds[0].text_content())

                votes_raw = _clean(tds[1].text_content()) if len(tds) > 1 else ""
                pct_raw   = _clean(tds[2].text_content()) if len(tds) > 2 else ""

                if not candidate:
                    continue

                results.append((race_name, {
                    "district":   district,
                    "candidate":  candidate,
                    "party":      party,
                    "votes_raw":  votes_raw,
                    "pct_raw":    pct_raw,
                }))

    return results


def _build_df(rows: list[dict], cols: list[str]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=cols)
    df["votes"]    = pd.to_numeric(df["votes"],    errors="coerce").astype("Int64")
    df["vote_pct"] = pd.to_numeric(df["vote_pct"], errors="coerce")
    return df


# ── Public parsers ────────────────────────────────────────────────────────────

def parse_statewide_results(
    html_str: str,
    election: CtElectionInfo,
) -> pd.DataFrame:
    """Parse statewide election results from the CTEMS Summary page HTML.

    The Summary page shows federal races only (Presidential Electors, US Senator,
    Representative in Congress).  All rows are tagged ``election_level='Federal'``.

    An **empty DataFrame is normal** for elections with no federal races on the
    ballot (e.g. off-year municipal elections).  The pipeline then builds the
    statewide output entirely from town aggregation.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the CTEMS statewide Summary page.
    election : CtElectionInfo
        Election metadata.

    Returns
    -------
    pd.DataFrame
        Columns: election_name, election_year, election_date, election_level,
        office, candidate, party, votes, vote_pct.
    """
    doc = lhtml.fromstring(html_str)
    race_rows = _extract_races(doc)

    if not race_rows:
        return pd.DataFrame(columns=_STATE_COLS)

    election_date_str = (
        election.election_date.isoformat() if election.election_date else None
    )
    rows: list[dict] = []

    for race_name, cr in race_rows:
        # Include district in office name when present.
        office = f"{race_name} — {cr['district']}" if cr["district"] else race_name

        rows.append({
            "election_name":  election.name,
            "election_year":  election.year,
            "election_date":  election_date_str,
            "election_level": classify_election_level(race_name),
            "office":         office,
            "candidate":      cr["candidate"],
            "party":          cr["party"],
            "votes":          _parse_votes(cr["votes_raw"]),
            "vote_pct":       _parse_pct(cr["pct_raw"]),
        })

    if not rows:
        return pd.DataFrame(columns=_STATE_COLS)

    return _add_contest_outcome(_build_df(rows, _STATE_COLS), _CONTEST_STATE_COLS)


def parse_town_results(
    html_str: str,
    town_name: str,
    county_name: str,
    election: CtElectionInfo,
) -> pd.DataFrame:
    """Parse town-level election results from the CTEMS town page HTML.

    Uses the same HTML structure as the statewide parser; each row's
    ``election_level`` is classified from the office name.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the CTEMS town results page.
    town_name : str
        Town name (populates the ``town`` column).
    county_name : str
        County name (populates the ``county`` column).
    election : CtElectionInfo
        Election metadata.

    Returns
    -------
    pd.DataFrame
        Columns: election_name, election_year, election_date, county, town,
        election_level, office, candidate, party, votes, vote_pct.
    """
    doc = lhtml.fromstring(html_str)
    race_rows = _extract_races(doc)

    if not race_rows:
        return pd.DataFrame(columns=_TOWN_COLS)

    election_date_str = (
        election.election_date.isoformat() if election.election_date else None
    )
    rows: list[dict] = []

    for race_name, cr in race_rows:
        office = f"{race_name} — {cr['district']}" if cr["district"] else race_name

        rows.append({
            "election_name":  election.name,
            "election_year":  election.year,
            "election_date":  election_date_str,
            "county":         county_name,
            "town":           town_name,
            "election_level": classify_election_level(race_name),
            "office":         office,
            "candidate":      cr["candidate"],
            "party":          cr["party"],
            "votes":          _parse_votes(cr["votes_raw"]),
            "vote_pct":       _parse_pct(cr["pct_raw"]),
        })

    if not rows:
        return pd.DataFrame(columns=_TOWN_COLS)

    return _build_df(rows, _TOWN_COLS)
