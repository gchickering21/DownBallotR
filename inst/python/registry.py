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
# Internal scraper functions
# ──────────────────────────────────────────────────────────────────────────────

def _scrape_nc(date: str | None = None, **_) -> pd.DataFrame:
    """Scrape North Carolina local election results."""
    from NorthCarolina.pipeline import get_nc_election_results
    return get_nc_election_results(date=date)


def _scrape_election_stats(
    state: str,
    year_from: int = 1789,
    year_to: int | None = None,
    level: str = "all",
    parallel: bool = False,
    **_,
) -> pd.DataFrame | dict:
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

    config = get_state_config(state_key)

    state_frames: list[pd.DataFrame] = []
    county_frames: list[pd.DataFrame] = []

    for year in range(year_from, year_to + 1):
        s_df, c_df = scrape_one_year(
            state_key=state_key,
            state_name=state_key,
            base_url=config["base_url"],
            search_path=config["search_path"],
            year=year,
            parallel=parallel,
            scraping_method=config["scraping_method"],
        )
        if not s_df.empty:
            state_frames.append(s_df)
        if not c_df.empty:
            county_frames.append(c_df)

    state_all = _concat_or_empty(state_frames)
    county_all = _concat_or_empty(county_frames)

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
