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

from Clarity.pipeline import get_clarity_election_results

UT_BASE_URL      = "https://electionresults.utah.gov/results/public/Utah"
UT_COUNTY_SUFFIX = "-ut"
UT_LOG_PREFIX    = "[UT]"


def get_ut_election_results(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_county_workers: int = 2,
    include_vote_methods: bool = False,
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
          - ``'all'``    (default) dict with keys ``'state'`` and ``'county'``
                        (plus ``'vote_method_state'`` / ``'vote_method_county'``
                        when ``include_vote_methods=True``);
                        reticulate converts this to a named R list.
          - ``'state'``  statewide totals only (skips county scraping).
          - ``'county'`` county-level only.
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
        base_url=UT_BASE_URL,
        county_suffix=UT_COUNTY_SUFFIX,
        log_prefix=UT_LOG_PREFIX,
        year_from=year_from,
        year_to=year_to,
        level=level,
        max_county_workers=max_county_workers,
        include_vote_methods=include_vote_methods,
    )
