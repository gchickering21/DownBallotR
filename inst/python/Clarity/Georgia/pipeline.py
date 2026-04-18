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

from Clarity.pipeline import get_clarity_election_results

GA_BASE_URL     = "https://results.sos.ga.gov/results/public/Georgia"
GA_COUNTY_SUFFIX = "-ga"
GA_LOG_PREFIX   = "[GA]"


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
    return get_clarity_election_results(
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
