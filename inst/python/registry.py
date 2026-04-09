# inst/python/registry.py
#
# Central Python registry for all DownBallotR scrapers.
# Imported by R via reticulate; each _scrape_* function uses lazy imports
# so only the deps needed for the requested source are loaded.

from __future__ import annotations

import datetime
from typing import List

import pandas as pd

from date_utils import validate_year_range, year_to_date_range
from df_utils import concat_or_empty as _concat_or_empty


# ──────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ──────────────────────────────────────────────────────────────────────────────

_VALID_LEVELS = ("all", "state", "county")


def _to_year(v) -> "int | None":
    """Coerce *v* to an integer year, accepting int/float/str/None."""
    if v is None:
        return None
    try:
        year = int(float(v))
    except (TypeError, ValueError):
        raise ValueError(f"Cannot convert {v!r} to a year integer.")
    if not (1900 <= year <= 2100):
        raise ValueError(f"Year must be between 1900 and 2100; got {year}.")
    return year


def _validate_level(level: str) -> None:
    if level not in _VALID_LEVELS:
        raise ValueError(f"level must be one of {_VALID_LEVELS}; got {level!r}.")


# ──────────────────────────────────────────────────────────────────────────────
# Internal scraper functions
# ──────────────────────────────────────────────────────────────────────────────

def _scrape_ut(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_county_workers: int = 4,
    include_vote_methods: bool = False,
    **_,
):
    """Scrape Utah election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.
    year_to : int | None
        End year, inclusive.
    level : str
        ``'all'`` (default) — dict with ``'state'`` and ``'county'`` DataFrames;
        ``'state'`` — statewide totals only (no county scraping, much faster);
        ``'county'`` — county-level DataFrame only.
    max_county_workers : int
        Parallel Chromium browsers for county scraping (default 4).
    include_vote_methods : bool
        When True, expands each contest's vote-method breakdown table and adds
        ``'vote_method_state'`` / ``'vote_method_county'`` to the result dict
        (default False).
    """
    _validate_level(level)
    year_from = _to_year(year_from)
    year_to   = _to_year(year_to)
    year_from, year_to = _clamp_year_range(year_from, year_to, "utah_results")

    label = (
        f"{year_from}–{year_to}" if year_from and year_to
        else f"{year_from}–" if year_from
        else f"–{year_to}" if year_to
        else "all years"
    )
    print(f"[UT] Starting scrape | {label} | level={level!r} | vote_methods={include_vote_methods}")

    from Clarity.Utah.pipeline import get_ut_election_results
    return get_ut_election_results(
        year_from=year_from,
        year_to=year_to,
        level=level,
        max_county_workers=max_county_workers,
        include_vote_methods=include_vote_methods,
    )


def _scrape_ga(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_county_workers: int = 4,
    include_vote_methods: bool = False,
    **_,
):
    """Scrape Georgia Secretary of State election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.
    year_to : int | None
        End year, inclusive.
    level : str
        ``'all'`` (default) — dict with ``'state'`` and ``'county'`` DataFrames;
        ``'state'`` — statewide totals only (no county scraping, much faster);
        ``'county'`` — county-level DataFrame only.
    max_county_workers : int
        Parallel Chromium browsers for county scraping (default 4).
    include_vote_methods : bool
        When True, expands each contest's vote-method breakdown table and adds
        ``'vote_method_state'`` / ``'vote_method_county'`` to the result dict
        (default False).
    """
    _validate_level(level)
    year_from = _to_year(year_from)
    year_to   = _to_year(year_to)
    year_from, year_to = _clamp_year_range(year_from, year_to, "georgia_results")

    label = (
        f"{year_from}–{year_to}" if year_from and year_to
        else f"{year_from}–" if year_from
        else f"–{year_to}" if year_to
        else "all years"
    )
    print(f"[GA] Starting scrape | {label} | level={level!r} | vote_methods={include_vote_methods}")

    from Clarity.Georgia.pipeline import get_ga_election_results
    return get_ga_election_results(
        year_from=year_from,
        year_to=year_to,
        level=level,
        max_county_workers=max_county_workers,
        include_vote_methods=include_vote_methods,
    )


def _scrape_in(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    **_,
):
    """Scrape Indiana General Election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive (default: 2020).
    year_to : int | None
        End year, inclusive (default: current calendar year).
    level : str
        ``'all'`` (default) — dict with ``'state'`` and ``'county'`` DataFrames;
        ``'state'`` — statewide candidate totals only;
        ``'county'`` — county-level totals only.
    """
    _validate_level(level)
    year_from = _to_year(year_from)
    year_to   = _to_year(year_to)
    year_from, year_to = _clamp_year_range(year_from, year_to, "indiana_results")

    label = (
        f"{year_from}–{year_to}" if year_from and year_to
        else f"{year_from}–" if year_from
        else f"–{year_to}" if year_to
        else "all years"
    )
    print(f"[IN] Starting scrape | {label} | level={level!r}")

    from Indiana.pipeline import get_in_election_results
    return get_in_election_results(
        year_from=year_from,
        year_to=year_to,
        level=level,
    )


def _scrape_ct(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_town_workers: int = 2,
    **_,
):
    """Scrape Connecticut CTEMS election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.
    year_to : int | None
        End year, inclusive.
    level : str
        ``'all'`` (default) — dict with ``'state'`` and ``'town'`` DataFrames;
        ``'state'`` — statewide totals only (no town scraping, much faster);
        ``'town'`` — town-level DataFrame only.
    max_town_workers : int
        Parallel Chromium browsers for town scraping (default 2).
    """
    year_from = _to_year(year_from)
    year_to   = _to_year(year_to)

    label = (
        f"{year_from}–{year_to}" if year_from and year_to
        else f"{year_from}–" if year_from
        else f"–{year_to}" if year_to
        else "all years"
    )
    print(f"[CT] Starting scrape | {label} | level={level!r}")

    from Connecticut.pipeline import get_ct_election_results
    return get_ct_election_results(
        year_from=year_from,
        year_to=year_to,
        level=level,
        max_town_workers=max_town_workers,
    )


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

    min_date, max_date = year_to_date_range(year_from, year_to)

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
    failed_years: list[int] = []

    for year in range(year_from, year_to + 1):
        print(f"[ElectionStats] Scraping {state_key} {year}...", flush=True)
        try:
            s_df, c_df = scrape_one_year(
                state_key=state_key,
                state_name=state_key,
                base_url=config["base_url"],
                search_path=config["search_path"],
                year=year,
                parallel=parallel,
                scraping_method=config["scraping_method"],
                url_style=config.get("url_style", "path_params"),
            )
        except Exception as exc:
            print(f"[ElectionStats] ERROR {state_key} {year}: {exc} — skipping year", flush=True)
            failed_years.append(year)
            continue
        print(
            f"[ElectionStats] {year}: "
            f"{len(s_df):,} election rows, {len(c_df):,} county rows"
        )
        if not s_df.empty:
            state_frames.append(s_df)
        if not c_df.empty:
            county_frames.append(c_df)

    if failed_years:
        print(f"[ElectionStats] WARNING: {len(failed_years)} year(s) failed for {state_key}: {failed_years}")

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


def _scrape_ballotpedia_elections(
    year: "int | None" = None,
    state: "str | None" = None,
    mode: str = "listings",
    election_level: str = "all",
    start_year: int = 2024,
    end_year: "int | None" = None,
    **_,
) -> pd.DataFrame:
    """Scrape Ballotpedia state-level election candidate data.

    Parameters
    ----------
    year : int | None
        Election year (e.g. 2024). Required for mode='results'.
        If provided with mode='listings', scrapes that single year.
    state : str | None
        State name (e.g. 'Maine'). Required — state pages are state-specific.
    mode : str
        - 'listings' (default) candidate listing from state+year page; fast,
          one HTTP request. Returns candidate names, offices, parties, status
          but no vote counts.
        - 'results'   follows each unique contest URL for vote counts and
                      percentages. Requires year. Slower.
    election_level : str
        Filter by candidate level:
          - 'all'     (default) all levels
          - 'federal' U.S. House, Senate, Presidential Electors
          - 'state'   state-level races (Governor, state legislature, etc.)
          - 'local'   local races (mayor, city council, etc.)
    start_year : int
        Earliest year for multi-year listing scrape when year is None (default: 2024).
    end_year : int | None
        Latest year for multi-year listing scrape when year is None (default: current year).
    """
    year       = int(year)       if year       is not None else None
    start_year = int(start_year) if start_year is not None else 2024
    end_year   = int(end_year)   if end_year   is not None else None

    if state is None:
        raise ValueError("'state' is required for Ballotpedia state elections scraper.")

    # ── Year validation ──────────────────────────────────────────────────────
    _SUPPORTED_FROM = 2024
    _effective_years = (
        [year] if year is not None
        else list(range(start_year, (end_year or datetime.date.today().year) + 1))
    )
    _unsupported = validate_year_range(
        _effective_years, _SUPPORTED_FROM, "Ballotpedia state elections"
    )
    if _unsupported and all(y < _SUPPORTED_FROM for y in _effective_years):
        return pd.DataFrame()

    from Ballotpedia.state_elections import StateElectionsScraper

    scraper = StateElectionsScraper()

    if mode == "results":
        if year is None:
            raise ValueError("'year' is required for mode='results'")
        return scraper.scrape_with_results_to_dataframe(
            year=year, state=state, level=election_level
        )

    # mode == "listings"
    if year is not None:
        return scraper.scrape_all_to_dataframe(
            start_year=year, end_year=year, state=state, level=election_level
        )
    return scraper.scrape_all_to_dataframe(
        start_year=start_year, end_year=end_year, state=state, level=election_level
    )


def _scrape_ballotpedia_municipal(
    year: "int | None" = None,
    state: "str | None" = None,
    race_type: str = "all",
    mode: str = "links",
    start_year: int = 2014,
    end_year: "int | None" = None,
    **_,
) -> pd.DataFrame:
    """Scrape Ballotpedia municipal and mayoral election data.

    Parameters
    ----------
    year : int | None
        Election year (e.g. 2022). If provided, scrapes that single year.
        If None, scrapes start_year through end_year.
    state : str | None
        Filter to one state (e.g. 'Texas'), or None for all states.
    race_type : str
        ``'all'`` (default) — United_States_municipal_elections (2014–present);
        includes city, county, and mayoral races.
        ``'mayoral'`` — United_States_mayoral_elections (2020–present).
    mode : str
        ``'links'`` (default) — Phase 1: index discovery only (one request per
        year); returns location metadata and sub-URLs, no vote data.
        ``'results'`` — Phase 2: follows every sub-URL for candidate and vote
        data. One extra request per location; slower.
    start_year : int
        Earliest year when year is None (default: 2014).
    end_year : int | None
        Latest year when year is None (default: current calendar year).
    """
    year       = int(year)       if year       is not None else None
    start_year = int(start_year) if start_year is not None else 2014
    end_year   = int(end_year)   if end_year   is not None else None

    _min_year = 2020 if race_type == "mayoral" else 2014
    _effective_years = (
        [year] if year is not None
        else list(range(start_year, (end_year or datetime.date.today().year) + 1))
    )
    _unsupported = validate_year_range(
        _effective_years, _min_year, f"Ballotpedia municipal (race_type='{race_type}')"
    )
    if _unsupported and all(y < _min_year for y in _effective_years):
        return pd.DataFrame()

    from Ballotpedia.municipal_elections import MunicipalElectionsScraper

    scraper = MunicipalElectionsScraper()

    if mode == "results":
        if year is not None:
            return scraper.scrape_all_to_dataframe(
                year=year, race_type=race_type, state=state
            )
        return scraper.scrape_years_to_dataframe(
            start_year=start_year,
            end_year=end_year or datetime.date.today().year,
            race_type=race_type,
            state=state,
        )

    # mode == "links" — Phase 1 index only
    if year is not None:
        return scraper.get_election_links_to_dataframe(
            year=year, race_type=race_type, state=state
        )
    return scraper.get_all_years_links_to_dataframe(
        start_year=start_year,
        end_year=end_year or datetime.date.today().year,
        race_type=race_type,
        state=state,
    )


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
    "northcarolina_results": {
        "NC": (2000, 2025),
    },
    "indiana_results": {
        "IN": (2019, None),  # open-ended; new elections added as they occur
    },
    "connecticut_results": {
        "CT": (2016, None),  # open-ended; discovery determines actual availability
    },
    "georgia_results": {
        "GA": (2000, None),
    },
    "utah_results": {
        "UT": (2023, 2025),
    },
    "ballotpedia": {
        # Ballotpedia covers all US states from 2013 onward (open-ended).
        "_all": (2013, None),
    },
    "ballotpedia_elections": {
        # State-level election pages use the widget-table-container layout
        # introduced in 2024; not all states have pages for every year.
        "_all": (2024, None),
    },
    "ballotpedia_municipal": {
        # Municipal index (race_type="all") covers 2014–present.
        # Mayoral-only index (race_type="mayoral") covers 2020–present.
        "_all": (2014, None),
    },
}


def _clamp_year_range(
    year_from: "int | None",
    year_to: "int | None",
    source: str,
) -> "tuple[int | None, int | None]":
    """Clamp year_from/year_to to the available range for *source* in _YEAR_RANGES.

    Looks up all state ranges under *source*, derives the union
    (earliest start, latest end), then clamps the requested range to that
    window.  Prints a warning for any clamping applied.  Returns unchanged
    values if *source* is not in _YEAR_RANGES.
    """
    ranges = _YEAR_RANGES.get(source)
    if not ranges:
        return year_from, year_to

    current_year = datetime.datetime.now().year
    starts = [r[0] for r in ranges.values()]
    ends   = [r[1] if r[1] is not None else current_year for r in ranges.values()]
    min_start = min(starts)
    max_end   = max(ends)

    if year_from is not None and year_to is not None and year_from > year_to:
        raise ValueError(
            f"year_from ({year_from}) cannot be greater than year_to ({year_to})."
        )

    new_from = year_from
    new_to   = year_to

    if year_from is not None and year_from < min_start:
        print(f"  [WARNING] year_from={year_from} is before the earliest available year "
              f"for '{source}' ({min_start}). Clamping to {min_start}.")
        new_from = min_start

    if year_to is not None and year_to > max_end:
        print(f"  [WARNING] year_to={year_to} is after the latest available year "
              f"for '{source}' ({max_end}). Clamping to {max_end}.")
        new_to = max_end

    return new_from, new_to


# ──────────────────────────────────────────────────────────────────────────────
# Registry
# ──────────────────────────────────────────────────────────────────────────────

def _list_election_stats_states() -> List[str]:
    from ElectionStats.state_config import STATE_CONFIGS
    return sorted(STATE_CONFIGS.keys())


_SOURCES: dict = {
    "georgia_results": {
        "description": "Georgia Secretary of State election results (results.sos.ga.gov)",
        "scrape_fn": _scrape_ga,
        "states": ["GA"],
    },
    "utah_results": {
        "description": "Utah election results (electionresults.utah.gov)",
        "scrape_fn": _scrape_ut,
        "states": ["UT"],
    },
    "indiana_results": {
        "description": "Indiana General Election results (enr.indianavoters.in.gov, 2020–present)",
        "scrape_fn": _scrape_in,
        "states": ["IN"],
    },
    "connecticut_results": {
        "description": "Connecticut CTEMS election results (ctemspublic.tgstg.net)",
        "scrape_fn": _scrape_ct,
        "states": ["CT"],
    },
    "northcarolina_results": {
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
    "ballotpedia_elections": {
        "description": (
            "Ballotpedia state elections — federal, state, and local candidates "
            "(all US states, 2024–present)"
        ),
        "scrape_fn": _scrape_ballotpedia_elections,
        "states": [],  # state= param required; covers all US states
    },
    "ballotpedia_municipal": {
        "description": (
            "Ballotpedia municipal and mayoral elections "
            "(all US states, 2014–present)"
        ),
        "scrape_fn": _scrape_ballotpedia_municipal,
        "states": [],  # use state= param to filter; covers all US states
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
        One of 'northcarolina_results', 'election_stats', 'ballotpedia',
        'ballotpedia_elections'.
    state : str | None
        State key for 'election_stats' (e.g. 'virginia').
        Pass None to get the earliest year across all ElectionStats states.
        Ignored for 'northcarolina_results', 'ballotpedia', and 'ballotpedia_elections'.

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

    if source in ("ballotpedia", "ballotpedia_elections", "ballotpedia_municipal"):
        start, end = ranges["_all"]
        return {"start_year": start, "end_year": end or current_year}

    if source == "northcarolina_results":
        start, end = ranges["NC"]
        return {"start_year": start, "end_year": end or current_year}

    if source == "indiana_results":
        start, end = ranges["IN"]
        return {"start_year": start, "end_year": end or current_year}

    if source == "georgia_results":
        start, end = ranges["GA"]
        return {"start_year": start, "end_year": end or current_year}

    if source == "utah_results":
        start, end = ranges["UT"]
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
        One of 'northcarolina_results', 'election_stats', 'ballotpedia',
        'ballotpedia_elections'. Call list_sources() to see all options.
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
