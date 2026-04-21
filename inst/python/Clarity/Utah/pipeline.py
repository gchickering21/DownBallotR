"""
Utah election results pipeline.

Thin wrapper around :mod:`Clarity.pipeline` with Utah-specific
configuration (base URL, county suffix, log prefix).

Public entry points
-------------------
``get_ut_election_results(year_from, year_to)``
    Returns a dict (or individual DataFrames) — called by the registry.
"""

from __future__ import annotations

import re

import pandas as pd

from Clarity.pipeline import get_clarity_election_results

# Matches a geographic sub-qualifier trailing the office name after the
# base office type.  These are sub-area or school-district names that the
# Utah SOS site appends to the contest title, e.g.:
#   "School Board Alpine"        → base="School Board",  geo="Alpine"
#   "County Council Logan"       → base="County Council", geo="Logan"
#   "Cache Water District North" → base="Cache Water District", geo="North"
_UT_GEO_SUFFIX_RE = re.compile(
    r"^(School Board|County Council|Cache Water District)\s+(.+)$",
    re.I,
)


def _normalize_ut_office_district(df: pd.DataFrame) -> pd.DataFrame:
    """Extract trailing geographic sub-qualifiers from Utah office names.

    When a geographic sub-area name is appended to the base office
    (e.g. 'School Board Alpine'), move it to the district column,
    prepending to any existing district value:
      office='School Board Alpine', district='District 3'
        → office='School Board',    district='Alpine, District 3'
    """
    if df.empty or "office" not in df.columns:
        return df

    df = df.copy()
    m = df["office"].str.extract(_UT_GEO_SUFFIX_RE, expand=True)
    matched = m[0].notna()
    if not matched.any():
        return df

    base_office = m.loc[matched, 0]
    geo_suffix  = m.loc[matched, 1]
    existing    = df.loc[matched, "district"].fillna("").str.strip()

    df.loc[matched, "office"]    = base_office
    df.loc[matched, "district"]  = geo_suffix.str.strip().where(
        existing == "",
        geo_suffix.str.strip() + ", " + existing,
    )
    return df


def _drop_is_incumbent(result):
    """Drop the is_incumbent column from all DataFrames in a result."""
    if isinstance(result, pd.DataFrame):
        return result.drop(columns=["is_incumbent"], errors="ignore")
    if isinstance(result, dict):
        return {k: v.drop(columns=["is_incumbent"], errors="ignore") for k, v in result.items()}
    return result

UT_BASE_URL      = "https://electionresults.utah.gov/results/public/Utah"
UT_COUNTY_SUFFIX = "-ut"
UT_LOG_PREFIX    = "[UT]"


def get_ut_election_results(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_county_workers: int = 2,
):
    """Return Utah election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.  ``None`` applies no lower bound.
    year_to : int | None
        End year, inclusive.  ``None`` applies no upper bound.
    level : str
        What to return:
          - ``'all'``      (default) dict with keys ``'state'``, ``'county'``,
                          and ``'precinct'``; reticulate converts this to a
                          named R list.
          - ``'state'``    statewide totals only (skips county/precinct scraping).
          - ``'county'``   county-level only (skips precinct scraping).
          - ``'precinct'`` precinct-level only; navigates each county page,
                          clicks "View results by precinct", then scrapes every
                          individual precinct page.
    max_county_workers : int
        Parallel Chromium browsers for county scraping (default 2).
    include_vote_methods : bool
        When True, expands each contest's vote-method breakdown table and
        returns per-method vote counts (Advance in Person, Election Day,
        Absentee by Mail, Provisional) in addition to the normal totals.
        Adds ``vote_method_state`` / ``vote_method_county`` to the result dict.
        Default False.
    """
    result = get_clarity_election_results(
        base_url=UT_BASE_URL,
        county_suffix=UT_COUNTY_SUFFIX,
        state_abbrev="UT",
        log_prefix=UT_LOG_PREFIX,
        year_from=year_from,
        year_to=year_to,
        level=level,
        max_county_workers=max_county_workers,
    )
    result = _drop_is_incumbent(result)
    if isinstance(result, pd.DataFrame):
        return _normalize_ut_office_district(result)
    if isinstance(result, dict):
        return {k: _normalize_ut_office_district(v) for k, v in result.items()}
    return result
