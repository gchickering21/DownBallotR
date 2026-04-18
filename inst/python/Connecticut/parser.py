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
from datetime import date
from datetime import datetime as _datetime

import pandas as pd
from lxml import html as lhtml

from .models import CtElectionInfo

from office_level_utils import classify_office_level as classify_election_level
from column_schemas import CT_STATE_COLS, CT_TOWN_COLS
from text_utils import normalize_party


_CT_DATE_FMTS = ("%m/%d/%Y", "%B %d, %Y", "%B %Y", "%Y")


def _parse_ct_election_type(election_name: str) -> "str | None":
    """Extract the election type from a CT election name.

    "November 8, 2022 -- General Election" → "General Election"
    Returns None if the ' -- ' separator is absent.
    """
    if " -- " in election_name:
        return election_name.split(" -- ", 1)[1].strip() or None
    return None


def _parse_ct_election_date(election_name: str) -> "date | None":
    """Extract and parse the date portion from a CT election name.

    Election names look like "November 8, 2022 -- General Election".
    Returns a ``datetime.date`` on success, None if unparseable.
    """
    raw = (
        election_name.split(" -- ")[0].strip()
        if " -- " in election_name
        else election_name.strip()
    )
    for fmt in _CT_DATE_FMTS:
        try:
            return _datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


# ── Output column definitions ─────────────────────────────────────────────────
# Partial schemas (no "state") used for intermediate DataFrame construction.
# The pipeline adds "state" and reindexes to the full CT_STATE_COLS/CT_TOWN_COLS
# from column_schemas via finalize_df().

_STATE_COLS = [c for c in CT_STATE_COLS if c != "state"]

_TOWN_COLS = [c for c in CT_TOWN_COLS if c != "state"]

# Columns that define a unique contest at the state level.
_CONTEST_STATE_COLS = ["election_name", "office", "district", "town"]
# Columns that define a unique contest at the town level.
_CONTEST_TOWN_COLS = ["election_name", "district", "town", "office"]

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



def _add_winner(
    df: pd.DataFrame, contest_cols: list[str], col: str = "winner"
) -> pd.DataFrame:
    """Add a boolean winner column: ``True`` for the max-votes candidate per
    contest, ``False`` for all others.  Ties both receive ``True``.
    Candidates with no valid vote data receive ``pd.NA``.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with at least ``votes`` and the columns in *contest_cols*.
    contest_cols : list[str]
        Columns that together identify a unique contest (e.g. election + office).
    col : str
        Name of the output column (default ``'winner'``).
    """
    if df.empty:
        df = df.copy()
        df[col] = pd.Series(dtype="boolean")
        return df
    df = df.copy()
    # transform("max") always returns a Series with the same index as df,
    # so boolean masks derived from it are always index-aligned.
    max_votes = df.groupby(contest_cols, dropna=False)["votes"].transform("max")
    has_votes = df["votes"].notna()
    df[col] = pd.NA
    df[col] = df[col].astype("boolean")
    df.loc[has_votes, col] = False
    df.loc[has_votes & (df["votes"] == max_votes), col] = True
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
                party = normalize_party(party_imgs[0]) if party_imgs else ""

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

    election_date_str = _parse_ct_election_date(election.name)
    election_type_str = _parse_ct_election_type(election.name)
    rows: list[dict] = []

    for race_name, cr in race_rows:
        rows.append({
            "election_name": election.name,
            "election_year": election.year,
            "election_date": election_date_str,
            "election_type": election_type_str,
            "office_level":  classify_election_level(race_name),
            "office":        race_name,
            "district":      cr["district"] or None,
            "town":          None,
            "candidate":     cr["candidate"],
            "party":         cr["party"],
            "votes":         _parse_votes(cr["votes_raw"]),
            "vote_pct":      _parse_pct(cr["pct_raw"]),
        })

    if not rows:
        return pd.DataFrame(columns=_STATE_COLS)

    return _add_winner(_build_df(rows, _STATE_COLS), _CONTEST_STATE_COLS)


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

    election_date_str = _parse_ct_election_date(election.name)
    election_type_str = _parse_ct_election_type(election.name)
    rows: list[dict] = []

    for race_name, cr in race_rows:
        rows.append({
            "election_name": election.name,
            "election_year": election.year,
            "election_date": election_date_str,
            "election_type": election_type_str,
            "district":      county_name,
            "town":          town_name,
            "office_level":  classify_election_level(race_name),
            "office":        race_name,
            "candidate":     cr["candidate"],
            "party":         cr["party"],
            "votes":         _parse_votes(cr["votes_raw"]),
            "vote_pct":      _parse_pct(cr["pct_raw"]),
        })

    if not rows:
        return pd.DataFrame(columns=_TOWN_COLS)

    return _add_winner(_build_df(rows, _TOWN_COLS), _CONTEST_TOWN_COLS, col="town_winner")
