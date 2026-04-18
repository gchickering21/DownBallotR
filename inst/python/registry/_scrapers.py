# registry/_scrapers.py
#
# Internal dispatch functions — one per data source.  Each function validates
# its inputs, then delegates to the relevant pipeline module via a lazy import
# so only the dependencies needed for the requested source are loaded.
#
# These are private (_-prefixed) and called only through registry.scrape().
# To add a new source: write a _scrape_<name>() here, register it in
# __init__._SOURCES, and add routing in R/scrape_elections.R.

from __future__ import annotations

import datetime

import pandas as pd

from date_utils import validate_year_range, year_to_date_range
from df_utils import concat_or_empty as _concat_or_empty

from ._validators import (
    _to_year,
    _validate_level,
    _validate_level_ct,
    _validate_level_la,
    _validate_workers,
)
from ._year_ranges import _clamp_year_range


def _prep_years(
    year_from: "int | None",
    year_to: "int | None",
    source_key: "str | None" = None,
) -> "tuple[int | None, int | None]":
    year_from = _to_year(year_from)
    year_to   = _to_year(year_to)
    if source_key is not None:
        year_from, year_to = _clamp_year_range(year_from, year_to, source_key)
    return year_from, year_to


def _format_year_label(year_from: "int | None", year_to: "int | None") -> str:
    if year_from and year_to:
        return f"{year_from}–{year_to}"
    if year_from:
        return f"{year_from}–"
    if year_to:
        return f"–{year_to}"
    return "all years"


# ── State-portal scrapers (Clarity / Playwright-based) ────────────────────────

def _scrape_ut(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_county_workers: int = 4,
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
    """
    _validate_level(level)
    max_county_workers = _validate_workers(max_county_workers, "max_county_workers")
    year_from, year_to = _prep_years(year_from, year_to, "utah_results")

    label = _format_year_label(year_from, year_to)
    print(f"[UT] Starting scrape | {label} | level={level!r}")

    from Clarity.Utah.pipeline import get_ut_election_results
    return get_ut_election_results(
        year_from=year_from,
        year_to=year_to,
        level=level,
        max_county_workers=max_county_workers
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
    max_county_workers = _validate_workers(max_county_workers, "max_county_workers")
    year_from, year_to = _prep_years(year_from, year_to, "georgia_results")

    label = _format_year_label(year_from, year_to)
    print(f"[GA] Starting scrape | {label} | level={level!r} | vote_methods={include_vote_methods}")

    from Clarity.Georgia.pipeline import get_ga_election_results
    return get_ga_election_results(
        year_from=year_from,
        year_to=year_to,
        level=level,
        max_county_workers=max_county_workers,
        include_vote_methods=include_vote_methods,
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
    _validate_level_ct(level)
    max_town_workers = _validate_workers(max_town_workers, "max_town_workers")
    year_from, year_to = _prep_years(year_from, year_to)

    label = _format_year_label(year_from, year_to)
    print(f"[CT] Starting scrape | {label} | level={level!r}")

    from Connecticut.pipeline import get_ct_election_results
    return get_ct_election_results(
        year_from=year_from,
        year_to=year_to,
        level=level,
        max_town_workers=max_town_workers,
    )


def _scrape_la(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_parish_workers: int = 2,
    **_,
):
    """Scrape Louisiana Secretary of State Graphical election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive (default: no lower bound; data goes back to 1982).
    year_to : int | None
        End year, inclusive (default: current calendar year).
    level : str
        ``'all'`` (default) — dict with ``'state'`` and ``'parish'`` DataFrames;
        ``'state'`` — statewide tab results only (no parish scraping, much faster);
        ``'parish'`` — parish-level DataFrame only.
    max_parish_workers : int
        Parallel Chromium browsers for parish scraping (default 2).
    """
    _validate_level_la(level)
    max_parish_workers = _validate_workers(max_parish_workers, "max_parish_workers")
    year_from, year_to = _prep_years(year_from, year_to, "louisiana_results")

    label = _format_year_label(year_from, year_to)
    print(f"[LA] Starting scrape | {label} | level={level!r}")

    from Louisiana.pipeline import get_la_election_results
    return get_la_election_results(
        year_from=year_from,
        year_to=year_to,
        level=level,
        max_parish_workers=max_parish_workers,
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
    year_from, year_to = _prep_years(year_from, year_to, "indiana_results")

    label = _format_year_label(year_from, year_to)
    print(f"[IN] Starting scrape | {label} | level={level!r}")

    from Indiana.pipeline import get_in_election_results
    return get_in_election_results(
        year_from=year_from,
        year_to=year_to,
        level=level,
    )


# ── Requests-based state scrapers ─────────────────────────────────────────────

def _scrape_nc(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    **_,
) -> "pd.DataFrame | dict":
    """Scrape North Carolina local election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.  Elections on or after Jan 1 of this year.
    year_to : int | None
        End year, inclusive.  Elections on or before Dec 31 of this year.
    level : str
        ``'all'`` (default) — dict with keys ``'precinct'``, ``'county'``,
        and ``'state'``; reticulate converts this to a named R list.
        ``'precinct'`` — precinct-level DataFrame only.
        ``'county'``   — county-level DataFrame only.
        ``'state'``    — statewide totals DataFrame only.
    """
    year_from, year_to = _prep_years(year_from, year_to)

    label = _format_year_label(year_from, year_to)
    print(f"[NC] Starting scrape | {label} | level={level!r}")

    min_date, max_date = year_to_date_range(year_from, year_to)

    from NorthCarolina.pipeline import get_nc_election_results
    result = get_nc_election_results(
        year_from=year_from,
        year_to=year_to,
        min_supported_date=min_date,
        max_supported_date=max_date,
    )

    if level == "all":
        return result
    return result[level]


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
          - 'all'      (default) dict with keys 'state', 'county', and (when
                       available) 'precinct'
          - 'state'    candidate/state-level DataFrame
          - 'county'   county vote breakdown DataFrame
          - 'precinct' precinct-level vote breakdown DataFrame
    parallel : bool
        Enable parallel county scraping for classic (requests-based) states.
    """
    year_from = _to_year(year_from) if year_from is not None else 1789
    year_to   = _to_year(year_to)

    from ElectionStats.state_config import get_state_config, STATE_CONFIGS
    from ElectionStats.run_scrape_yearly import (
        scrape_one_year,
        _normalize_state,
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
    precinct_frames: list[pd.DataFrame] = []
    failed_years: list[int] = []

    for year in range(year_from, year_to + 1):
        print(f"[ElectionStats] Scraping {state_key} {year}...", flush=True)
        try:
            s_df, c_df, p_df = scrape_one_year(
                state_key=state_key,
                state_name=state_key,
                base_url=config["base_url"],
                search_path=config["search_path"],
                year=year,
                parallel=parallel,
                scraping_method=config["scraping_method"],
                url_style=config.get("url_style", "path_params"),
                level=level,
            )
        except Exception as exc:
            print(f"[ElectionStats] ERROR {state_key} {year}: {exc} — skipping year", flush=True)
            failed_years.append(year)
            continue
        print(
            f"[ElectionStats] {year}: "
            f"{len(s_df):,} election rows, {len(c_df):,} county rows, "
            f"{len(p_df):,} precinct rows"
        )
        if not s_df.empty:
            state_frames.append(s_df)
        if not c_df.empty:
            county_frames.append(c_df)
        if not p_df.empty:
            precinct_frames.append(p_df)

    if failed_years:
        if len(failed_years) == n_years:
            raise RuntimeError(
                f"All {n_years} requested year(s) failed to scrape for '{state_key}'. "
                f"This usually means the site is unreachable or its structure has changed. "
                f"Years attempted: {failed_years}. "
                f"See the error messages printed above for details."
            )
        print(f"[ElectionStats] WARNING: {len(failed_years)}/{n_years} year(s) failed for {state_key}: {failed_years}")

    state_all   = _concat_or_empty(state_frames)
    county_all  = _concat_or_empty(county_frames)
    precinct_all = _concat_or_empty(precinct_frames)

    print(
        f"[ElectionStats] Done. "
        f"{len(state_all):,} total election rows, "
        f"{len(county_all):,} total county rows, "
        f"{len(precinct_all):,} total precinct rows."
    )

    if level == "state":
        return state_all
    if level == "county":
        return county_all
    if level == "precinct":
        return precinct_all
    # "all" — return all three as a dict; reticulate converts to a named R list
    result = {"state": state_all, "county": county_all}
    if not precinct_all.empty:
        result["precinct"] = precinct_all
    return result

