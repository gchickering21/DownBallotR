"""
Parse Indiana FirstTuesday archive JSON into DataFrames.

JSON quirk: single-item collections are dicts; multi-item collections are
lists.  All access goes through ``_ensure_list`` to normalise this.

State DataFrame columns
-----------------------
election_year, election_date, election_type, office_level, office,
candidate, party, votes, vote_pct, winner, num_seats

County DataFrame columns
------------------------
election_year, election_date, election_type, office_level, office,
county_name, county_fips, candidate, party, votes, vote_pct, county_winner,
num_seats
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from .models import InElectionInfo
from column_schemas import IN_STATE_COLS, IN_COUNTY_COLS, compute_vote_pct
from text_utils import normalize_party

# ── Output column schemas ──────────────────────────────────────────────────────
# Partial schemas (no "state") used for intermediate DataFrame construction.

_STATE_COLS  = [c for c in IN_STATE_COLS  if c != "state"]
_COUNTY_COLS = [c for c in IN_COUNTY_COLS if c != "state"]


# ── Helpers ────────────────────────────────────────────────────────────────────



def _fix_winners(df: pd.DataFrame, group_cols: list[str], col: str = "winner") -> pd.DataFrame:
    """Re-derive winner flag: top num_seats candidates by votes within each contest.

    The JSON marks all candidates as isWinner='F' for local races in the
    statewide summary.  We override by ranking candidates within each contest
    and marking the top num_seats as winners.
    """
    if df.empty:
        return df
    rank = df.groupby(group_cols, dropna=False)["votes"].rank(method="min", ascending=False)
    df[col] = rank <= df["num_seats"]
    return df


_JURISDICTION_RE = re.compile(r'^(.+?\s+(?:County|Township|Town|City))\b', re.IGNORECASE)
_OF_JURISDICTION_RE = re.compile(r'\bOf\s+(.+)$', re.IGNORECASE)

def _extract_jurisdiction(office_title: str) -> "str | None":
    """Extract a leading jurisdiction name (e.g. 'Bartholomew County') from OFFICE_TITLE."""
    m = _JURISDICTION_RE.match(office_title)
    return m.group(1).strip() if m else None


def _extract_of_jurisdiction(office_title: str) -> "str | None":
    """Extract jurisdiction from 'Mayor Of Decatur' style titles → 'Decatur'."""
    m = _OF_JURISDICTION_RE.search(office_title)
    return m.group(1).strip() or None if m else None


def _extract_district(office_title: str) -> "str | None":
    """Return everything after the first ', ' in OFFICE_TITLE, or None.

    Examples
    --------
    'State Representative, District 001' → 'District 001'
    'Adams County Commissioner, District 1' → 'District 1'
    'Adams County Council, At Large' → 'At Large'
    'Attorney General' → None
    """
    if ", " in office_title:
        return office_title.split(", ", 1)[1].strip() or None
    return None


def _ensure_list(obj: Any) -> list:
    """Wrap a dict in a list; return lists unchanged; return [] for None."""
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        return [obj]
    return []


def _safe_int(v: Any) -> int:
    """Convert *v* to int, returning 0 on failure."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _safe_float(v: Any) -> float:
    """Convert *v* to float, returning 0.0 on failure."""
    try:
        return round(float(v), 4)
    except (TypeError, ValueError):
        return 0.0


def _extract_office_categories(data: dict) -> list[dict]:
    """Return flat list of office-category dicts from statewideElectionsC_A.json."""
    cats = []
    for group in _ensure_list(data.get("Root", {}).get("List")):
        heading = group.get("Heading", "")
        items = group.get("Items", {})
        if not items:
            continue
        for item in _ensure_list(items.get("Item")):
            item["_heading"] = heading
            cats.append(item)
    return cats


# ── Main parsers ───────────────────────────────────────────────────────────────

def parse_office_categories(data: dict) -> list[dict]:
    """Extract category metadata from statewideElectionsC_A.json.

    Returns a list of dicts with keys:
      OFFICECATEGORYID, OFFICE_CATEGORY_NAME, _heading (Federal/State/Local)
    """
    return _extract_office_categories(data)


def parse_state_results(
    offcat_data: dict,
    election: InElectionInfo,
    category_name: str,
    office_level: str,
) -> pd.DataFrame:
    """Parse statewide candidate totals from one OffCatC_{id}_A.json response.

    Parameters
    ----------
    offcat_data : dict
        Parsed JSON from OffCatC_{id}_A.json.
    election : InElectionInfo
        Election metadata (year, date, etc.).
    category_name : str
        Human-readable office category name (e.g. "County Treasurer").
    office_level : str
        One of ``'Federal'``, ``'State'``, or ``'Local'``.

    Returns
    -------
    pd.DataFrame with columns matching ``_STATE_COLS``.
    """
    rows = []
    root = offcat_data.get("Root", {})
    summary = root.get("StatewideSummary", {})

    for race in _ensure_list(summary.get("Race")):
        office_title = race.get("OFFICE_TITLE", "")
        district     = (_extract_district(office_title)
                        or (office_level == "Local" and _extract_jurisdiction(office_title))
                        or (office_level == "Local" and _extract_of_jurisdiction(office_title))
                        or None)
        num_seats    = _safe_int(race.get("NumofSeats", 1))

        for cand in _ensure_list(race.get("Candidates", {}).get("Candidate")):
            rows.append({
                "election_year":  election.year,
                "election_date":  election.election_date,
                "election_type":  "General",
                "office_level":   office_level,
                "office":         category_name,
                "district":       district,
                "candidate":      cand.get("NAME_ON_BALLOT", "") or cand.get("CandidateName", ""),
                "party":          normalize_party(cand.get("PARTY", "")),
                "votes":          _safe_int(cand.get("TOTAL", 0)),
                "vote_pct":       0.0,
                "winner":         cand.get("isWinner", "") == "T",
                "num_seats":      num_seats,
            })

    if not rows:
        return pd.DataFrame(columns=_STATE_COLS)
    df = pd.DataFrame(rows)
    df = compute_vote_pct(df, ["election_year", "election_date", "office", "district"])
    df = _fix_winners(df, ["election_year", "election_date", "office", "district"])
    return df[_STATE_COLS]


def parse_county_results(
    offcat_data: dict,
    election: InElectionInfo,
    category_name: str,
    office_level: str,
) -> pd.DataFrame:
    """Parse county-level candidate totals from one OffCatC_{id}_A.json response.

    Parameters
    ----------
    offcat_data : dict
        Parsed JSON from OffCatC_{id}_A.json.
    election : InElectionInfo
        Election metadata.
    category_name : str
        Human-readable office category name.
    office_level : str
        One of ``'Federal'``, ``'State'``, or ``'Local'``.

    Returns
    -------
    pd.DataFrame with columns matching ``_COUNTY_COLS``.
    """
    rows = []
    root = offcat_data.get("Root", {})
    # County data lives at Root.OfficeCategory.Regions.Region
    regions_container = root.get("OfficeCategory", {}).get("Regions", {})

    for region in _ensure_list(regions_container.get("Region")):
        # Skip district-type regions (congressional/legislative races) — those
        # are not county breakdowns and would be misclassified.
        if region.get("MAP_JURISDICTION_TYPE", "").lower() != "locality":
            continue
        county_name = region.get("MAP_JURISDICTION_NAME", "")
        county_fips = region.get("MAP_FIPS", "")

        # Prefer Races (TOTAL_VOTES) — gives true per-county breakdown for both
        # statewide and local offices.  Fall back to RegionSummary (TOTAL) for
        # regions that only carry a summary (no Races key).
        if "Races" in region:
            races = _ensure_list(region["Races"].get("Race"))
            vote_field = "TOTAL_VOTES"
        else:
            races = _ensure_list(region.get("RegionSummary", {}).get("Race"))
            vote_field = "TOTAL"

        for race in races:
            office_title = race.get("OFFICE_TITLE", "")
            district     = (_extract_district(office_title)
                            or (office_level == "Local" and _extract_jurisdiction(office_title))
                            or (office_level == "Local" and _extract_of_jurisdiction(office_title))
                            or None)
            num_seats    = _safe_int(race.get("NumofSeats", 1))

            for cand in _ensure_list(race.get("Candidates", {}).get("Candidate")):
                rows.append({
                    "election_year":   election.year,
                    "election_date":   election.election_date,
                    "election_type":   "General",
                    "office_level":    office_level,
                    "office":          category_name,
                    "district":        district,
                    "county_name":     county_name,
                    "candidate":       cand.get("NAME_ON_BALLOT", "") or cand.get("CandidateName", ""),
                    "party":           normalize_party(cand.get("PARTY") or cand.get("PARTY_ABBREV", "")),
                    "votes":           _safe_int(cand.get(vote_field, 0)),
                    "vote_pct":        0.0,
                    "county_winner":   cand.get("isWinner", "") == "T",
                    "num_seats":       num_seats,
                })

    if not rows:
        return pd.DataFrame(columns=_COUNTY_COLS)
    df = pd.DataFrame(rows)
    df = compute_vote_pct(df, ["election_year", "election_date", "office", "district", "county_name"])
    df = _fix_winners(df, ["election_year", "election_date", "office", "district", "county_name"], col="county_winner")
    return df[_COUNTY_COLS]
