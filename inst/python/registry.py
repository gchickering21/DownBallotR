# inst/python/registry.py
#
# Central Python registry for all DownBallotR scrapers.
# Imported by R via reticulate; each _scrape_* function uses lazy imports
# so only the deps needed for the requested source are loaded.

from __future__ import annotations

import datetime
from typing import List

import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ──────────────────────────────────────────────────────────────────────────────

def _to_year(v) -> "int | None":
    """Coerce *v* to an integer year, accepting int/float/str/None."""
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        raise ValueError(f"Cannot convert {v!r} to a year integer.")


# ──────────────────────────────────────────────────────────────────────────────
# Internal scraper functions
# ──────────────────────────────────────────────────────────────────────────────

def _scrape_nc(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    **_,
) -> pd.DataFrame:
    """Scrape North Carolina local election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.  Elections on or after Jan 1 of this year.
    year_to : int | None
        End year, inclusive.  Elections on or before Dec 31 of this year.
    """
    year_from = _to_year(year_from)
    year_to   = _to_year(year_to)

    label = (
        f"{year_from}–{year_to}" if year_from and year_to
        else f"{year_from}–" if year_from
        else f"–{year_to}" if year_to
        else "all years"
    )
    print(f"[NC] Starting scrape | {label}")

    # Mirror year_from/year_to as pipeline date guards so only elections
    # within the requested range are attempted.
    min_date = datetime.date(year_from, 1,  1)  if year_from is not None else None
    max_date = datetime.date(year_to,   12, 31) if year_to   is not None else None

    from NorthCarolina.pipeline import get_nc_election_results
    return get_nc_election_results(
        year_from=year_from,
        year_to=year_to,
        min_supported_date=min_date,
        max_supported_date=max_date,
    )


def _scrape_election_stats(
    state: str,
    year_from: "int | None" = 1789,
    year_to: "int | None" = None,
    level: str = "all",
    parallel: bool = True,
    **_,
) -> "pd.DataFrame | dict":
    """Scrape ElectionStats data for a given state and year range.

    Parameters
    ----------
    state : str
        State key (e.g. 'virginia', 'south_carolina').
        Call list_states('election_stats') for supported values.
    year_from : int
        Start year (default: 1789).
    year_to : int | None
        End year, inclusive (default: current calendar year).
    level : str
        What to return:
          - 'all'    (default) dict with keys 'state' and 'county'
          - 'state'  candidate/state-level DataFrame
          - 'county' county vote breakdown DataFrame
          - 'joined' county rows merged with statewide metadata
    parallel : bool
        Enable parallel county scraping for classic (requests-based) states.
    """
    year_from = _to_year(year_from) if year_from is not None else 1789
    year_to   = _to_year(year_to)

    from ElectionStats.state_config import get_state_config, STATE_CONFIGS
    from ElectionStats.run_scrape_yearly import (
        scrape_one_year,
        _normalize_state,
        _concat_or_empty,
        _join_county_with_state,
    )

    state_key = _normalize_state(state)
    if state_key not in STATE_CONFIGS:
        raise ValueError(
            f"Unknown state: {state!r}. "
            f"Available: {sorted(STATE_CONFIGS.keys())}"
        )

    if year_to is None:
        year_to = datetime.date.today().year

    n_years = year_to - year_from + 1
    method  = get_state_config(state_key)["scraping_method"]
    print(
        f"[ElectionStats] Starting: {state_key} | "
        f"{year_from}–{year_to} ({n_years} year(s)) | "
        f"method={method}"
    )

    config = get_state_config(state_key)

    state_frames: list[pd.DataFrame] = []
    county_frames: list[pd.DataFrame] = []

    for year in range(year_from, year_to + 1):
        print(f"[ElectionStats] Scraping {state_key} {year}...", flush=True)
        s_df, c_df = scrape_one_year(
            state_key=state_key,
            state_name=state_key,
            base_url=config["base_url"],
            search_path=config["search_path"],
            year=year,
            parallel=parallel,
            scraping_method=config["scraping_method"],
        )
        print(
            f"[ElectionStats] {year}: "
            f"{len(s_df):,} election rows, {len(c_df):,} county rows"
        )
        if not s_df.empty:
            state_frames.append(s_df)
        if not c_df.empty:
            county_frames.append(c_df)

    state_all = _concat_or_empty(state_frames)
    county_all = _concat_or_empty(county_frames)

    print(
        f"[ElectionStats] Done. "
        f"{len(state_all):,} total election rows, "
        f"{len(county_all):,} total county rows."
    )

    if level == "state":
        return state_all
    if level == "county":
        return county_all
    if level == "joined":
        return _join_county_with_state(county_all=county_all, state_all=state_all)
    # "all" — return both as a dict; reticulate converts to a named R list
    return {"state": state_all, "county": county_all}


def _scrape_ballotpedia(
    year: int | None = None,
    state: str | None = None,
    mode: str = "districts",
    start_year: int = 2013,
    end_year: int | None = None,
    **_,
) -> pd.DataFrame:
    """Scrape Ballotpedia school board election data.

    Parameters
    ----------
    year : int | None
        Election year. If provided with mode='districts', scrapes that year only.
        Required for mode='results' or mode='joined'.
    state : str | None
        Filter to one state (e.g. 'Alabama'), or None for all states.
    mode : str
        - 'districts' (default) district-level metadata; fast, one request per year-page.
        - 'results'   follows each district URL for candidate/vote data (one extra
                      request per district). Requires year.
        - 'joined'    districts + candidates merged into a single DataFrame. Requires year.
    start_year : int
        Earliest year when year is None (default: 2013).
    end_year : int | None
        Latest year when year is None (default: current calendar year).
    """
    # Reticulate passes R numerics as Python floats; coerce to int where needed.
    year       = int(year)       if year       is not None else None
    start_year = int(start_year) if start_year is not None else 2013
    end_year   = int(end_year)   if end_year   is not None else None

    from Ballotpedia.school_board_elections import SchoolBoardScraper

    scraper = SchoolBoardScraper()

    if mode == "results":
        if year is None:
            raise ValueError("'year' is required for mode='results'")
        return scraper.scrape_with_results_to_dataframe(year=year, state=state)

    if mode == "joined":
        if year is None:
            raise ValueError("'year' is required for mode='joined'")
        return scraper.scrape_joined_to_dataframe(year=year, state=state)

    # mode == "districts"
    if year is not None:
        # Single-year district metadata: reuse scrape_all_to_dataframe with year as both bounds
        return scraper.scrape_all_to_dataframe(
            start_year=year, end_year=year, state=state
        )
    # Multi-year district metadata
    return scraper.scrape_all_to_dataframe(
        start_year=start_year, end_year=end_year, state=state
    )


# ──────────────────────────────────────────────────────────────────────────────
# Year availability registry
# ──────────────────────────────────────────────────────────────────────────────

# (start_year, end_year) tuples per source / state.
# end_year of None means "through current calendar year" (open-ended).
_YEAR_RANGES: dict = {
    "election_stats": {
        "vermont":        (1789, 2024),
        "virginia":       (1789, 2025),
        "colorado":       (1902, 2024),
        "massachusetts":  (1970, 2026),
        "new_hampshire":  (1970, 2024),
        "new_york":       (1994, 2024),
        "new_mexico":     (2000, 2024),
        "south_carolina": (2008, 2025),
    },
    "nc_results": {
        "NC": (2000, 2025),
    },
    "ballotpedia": {
        # Ballotpedia covers all US states from 2013 onward (open-ended).
        "_all": (2013, None),
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# Registry
# ──────────────────────────────────────────────────────────────────────────────

def _list_election_stats_states() -> List[str]:
    from ElectionStats.state_config import STATE_CONFIGS
    return sorted(STATE_CONFIGS.keys())


_SOURCES: dict = {
    "nc_results": {
        "description": "North Carolina local election results (NC State Board of Elections)",
        "scrape_fn": _scrape_nc,
        "states": ["NC"],
    },
    "election_stats": {
        "description": (
            "Multi-state ElectionStats scraper "
            "(VA, MA, CO, NH, SC, NM, NY)"
        ),
        "scrape_fn": _scrape_election_stats,
        # Stored as a callable so deps are not imported at registry load time
        "states": _list_election_stats_states,
    },
    "ballotpedia": {
        "description": "Ballotpedia school board elections (all US states, 2013–present)",
        "scrape_fn": _scrape_ballotpedia,
        "states": [],  # Ballotpedia covers all states; use state= param to filter
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def list_sources() -> List[str]:
    """Return names of all registered scraper sources."""
    return sorted(_SOURCES.keys())


def list_states(source: str) -> List[str]:
    """Return supported state keys for a given source.

    Parameters
    ----------
    source : str
        One of the names returned by list_sources().
    """
    if source not in _SOURCES:
        raise ValueError(
            f"Unknown source: {source!r}. Available: {list_sources()}"
        )
    states = _SOURCES[source]["states"]
    return states() if callable(states) else list(states)


def get_available_years(source: str, state: "str | None" = None) -> dict:
    """Return the earliest and latest available year for a source/state.

    Parameters
    ----------
    source : str
        One of 'nc_results', 'election_stats', 'ballotpedia'.
    state : str | None
        State key for 'election_stats' (e.g. 'virginia').
        Pass None to get the earliest year across all ElectionStats states.
        Ignored for 'nc_results' and 'ballotpedia'.

    Returns
    -------
    dict with keys 'start_year' (int) and 'end_year' (int, current calendar year).
    """
    if source not in _YEAR_RANGES:
        raise ValueError(
            f"Unknown source: {source!r}. Available: {sorted(_YEAR_RANGES.keys())}"
        )
    ranges = _YEAR_RANGES[source]
    current_year = datetime.date.today().year

    if source == "ballotpedia":
        start, end = ranges["_all"]
        return {"start_year": start, "end_year": end or current_year}

    if source == "nc_results":
        start, end = ranges["NC"]
        return {"start_year": start, "end_year": end or current_year}

    # election_stats
    if state is None:
        start_year = min(v[0] for v in ranges.values())
        end_year   = max(v[1] or current_year for v in ranges.values())
        return {"start_year": start_year, "end_year": end_year}

    state_key = state.strip().lower().replace(" ", "_")
    if state_key not in ranges:
        raise ValueError(
            f"No year range recorded for state {state!r} "
            f"(looked up as {state_key!r}). "
            f"Available: {sorted(ranges.keys())}"
        )
    start, end = ranges[state_key]
    return {"start_year": start, "end_year": end or current_year}


def scrape(source: str, **kwargs) -> "pd.DataFrame | dict":
    """Dispatch a scrape call to the registered source handler.

    Parameters
    ----------
    source : str
        One of 'nc_results', 'election_stats', 'ballotpedia'.
        Call list_sources() to see all options.
    **kwargs
        Passed through to the source's scrape function.
        See the individual _scrape_* functions for parameter details.

    Returns
    -------
    pd.DataFrame, or dict of DataFrames when level='all' for election_stats.
    """
    if source not in _SOURCES:
        raise ValueError(
            f"Unknown source: {source!r}. Available: {list_sources()}"
        )
    return _SOURCES[source]["scrape_fn"](**kwargs)
