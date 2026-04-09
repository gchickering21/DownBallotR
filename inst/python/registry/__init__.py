# registry/__init__.py
#
# Public API for the DownBallotR Python registry.
# Imported by R via reticulate::import("registry") — the interface is identical
# to the old single-file registry.py, so no R-side changes are needed.
#
# Internal layout:
#   _validators.py   — input validation helpers (_to_year, _validate_*, etc.)
#   _year_ranges.py  — _YEAR_RANGES dict and _clamp_year_range()
#   _scrapers.py     — one _scrape_*() function per data source

from __future__ import annotations

from typing import List

from ._scrapers import (
    _scrape_ballotpedia,
    _scrape_ballotpedia_elections,
    _scrape_ballotpedia_municipal,
    _scrape_ct,
    _scrape_election_stats,
    _scrape_ga,
    _scrape_in,
    _scrape_la,
    _scrape_nc,
    _scrape_ut,
)
from ._year_ranges import _YEAR_RANGES, _clamp_year_range

import datetime


# ── Source registry ───────────────────────────────────────────────────────────

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
    "louisiana_results": {
        "description": (
            "Louisiana Secretary of State Graphical election results "
            "(voterportal.sos.la.gov/Graphical, 1982–present)"
        ),
        "scrape_fn": _scrape_la,
        "states": ["LA"],
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


# ── Public API ────────────────────────────────────────────────────────────────

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
        One of the source names returned by list_sources().
    state : str | None
        State key for 'election_stats' (e.g. 'virginia').
        Pass None to get the earliest year across all ElectionStats states.
        Ignored for all other sources.

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

    if source == "louisiana_results":
        start, end = ranges["LA"]
        return {"start_year": start, "end_year": end or current_year}

    # election_stats — aggregate or per-state lookup
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


def scrape(source: str, **kwargs) -> "object":
    """Dispatch a scrape call to the registered source handler.

    Parameters
    ----------
    source : str
        One of the names returned by list_sources().
    **kwargs
        Passed through to the source's scrape function.
        See the individual _scrape_* functions in _scrapers.py for details.

    Returns
    -------
    pd.DataFrame, or dict of DataFrames when level='all'.
    """
    if source not in _SOURCES:
        raise ValueError(
            f"Unknown source: {source!r}. Available: {list_sources()}"
        )
    return _SOURCES[source]["scrape_fn"](**kwargs)
