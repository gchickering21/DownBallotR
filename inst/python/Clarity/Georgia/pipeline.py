"""
Georgia SOS election results pipeline.

Thin wrapper around :mod:`Clarity.pipeline` with Georgia-specific
configuration (base URL, county suffix, log prefix).

Public entry points
-------------------
``get_ga_election_results(year_from, year_to)``
    Returns a dict (or individual DataFrames) — called by the registry.
"""

from __future__ import annotations

import re

import pandas as pd

from Clarity.pipeline import get_clarity_election_results

# Party tokens that appear as standalone prefixes or suffixes around " - "
_GA_PARTY_PREFIX_RE = re.compile(
    r"^(?:DEM|REP|Dem|Rep|Democrat|Republican)\s*-\s*",
    re.I,
)
_GA_PARTY_TOKEN_RE = re.compile(
    r"^(?:DEM|REP|Dem|Rep|Democrat|Republican|Nonpartisan|NonPartisan|NP)$",
    re.I,
)

# All known spellings of US House of Representatives
_GA_US_HOUSE_RE = re.compile(
    r"^U\.?S\.?\s+House(?:\s+of\s+Representatives)?$",
    re.I,
)


def _normalize_ga_office_district(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Georgia office and district columns.

    Handles four patterns found in the Georgia SOS Clarity data:

    1. Party prefix  — ``"REP - State House"`` → office="State House"
    2. Party suffix  — ``"Attorney General - Rep"`` → office="Attorney General"
    3. Three-segment — ``"US House of Representatives - District 1 - Dem"``
                       → office="US House", district="District 1"
    4. Two-segment   — ``"Court of Appeals - Brown"`` → office="Court of Appeals",
                       district="Brown" (only when district column is empty)

    When a district value is already present, the trailing segment is stripped
    from the office name but the existing district is left unchanged.

    All US House / US House of Representatives variants are normalised to
    ``"US House"``.
    """
    if df.empty or "office" not in df.columns:
        return df

    df = df.copy()

    def _fix_row(row):
        office   = str(row.get("office",   "") or "").strip()
        district = str(row.get("district", "") or "").strip()

        # Step 1: strip leading party token
        office = _GA_PARTY_PREFIX_RE.sub("", office).strip()

        # Step 2: handle " - " separators
        if " - " in office:
            parts = [p.strip() for p in office.split(" - ")]
            base  = parts[0]

            if len(parts) >= 3:
                # "Office - Sub/District - Party/Other": second segment → district
                second = parts[1]
                if not district:
                    district = second
                office = base
            else:
                # Two segments: second may be party suffix or district/qualifier
                second = parts[1]
                if _GA_PARTY_TOKEN_RE.match(second):
                    office = base          # pure party suffix — drop it
                else:
                    if not district:
                        district = second  # geographic/district qualifier
                    office = base

        # Step 3: normalise US House spellings
        if _GA_US_HOUSE_RE.match(office):
            office = "US House of Representatives"

        row = row.copy()
        row["office"] = office if office else None
        if "district" in row.index:
            row["district"] = district if district else None
        return row

    return df.apply(_fix_row, axis=1)


GA_BASE_URL      = "https://results.sos.ga.gov/results/public/Georgia"
GA_COUNTY_SUFFIX = "-ga"
GA_LOG_PREFIX    = "[GA]"


def get_ga_election_results(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_county_workers: int = 2,
    include_vote_methods: bool = False,
):
    """Return Georgia election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.  ``None`` applies no lower bound.
    year_to : int | None
        End year, inclusive.  ``None`` applies no upper bound.
    level : str
        What to return:
          - ``'all'``      (default) dict with keys ``'state'``, ``'county'``,
                          and ``'precinct'`` (plus ``'vote_method_state'`` /
                          ``'vote_method_county'`` when
                          ``include_vote_methods=True``);
                          reticulate converts this to a named R list.
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
        base_url=GA_BASE_URL,
        county_suffix=GA_COUNTY_SUFFIX,
        state_abbrev="GA",
        log_prefix=GA_LOG_PREFIX,
        year_from=year_from,
        year_to=year_to,
        level=level,
        max_county_workers=max_county_workers,
        include_vote_methods=include_vote_methods,
    )
    if isinstance(result, pd.DataFrame):
        return _normalize_ga_office_district(result)
    if isinstance(result, dict):
        return {k: _normalize_ga_office_district(v) for k, v in result.items()}
    return result
