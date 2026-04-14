# registry/_year_ranges.py
#
# Confirmed data availability per source and the year-range clamping logic.
# Edit _YEAR_RANGES when a new state or year is confirmed working.

from __future__ import annotations

import datetime

_ISSUES_URL = "https://github.com/gchickering21/DownBallotR/issues"

# (start_year, end_year) tuples per source / state.
# end_year of None means "through current calendar year" (open-ended).
_YEAR_RANGES: dict = {
    "election_stats": {
        "vermont":        (1789, 2024),
        "virginia":       (1789, 2025),
        "colorado":       (1902, 2024),
        "massachusetts":  (1970, 2026),
        "new_hampshire":  (1970, 2024),
        "idaho":          (1990, 2024),
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
    "louisiana_results": {
        "LA": (1982, None),  # open-ended; dropdown goes back to 1982
    },
}


def _clamp_year_range(
    year_from: "int | None",
    year_to: "int | None",
    source: str,
) -> "tuple[int | None, int | None]":
    """Validate year_from/year_to against the registered range for *source*.

    Lower bound (year_from < min_start): hard-clamped — data genuinely does
    not exist before this point.

    Upper bound (year_to > max_end): allowed through with an informational
    notice.  New election cycles are published continuously, so a year beyond
    the last *confirmed* end may still work.  If it fails, the caller's error
    handler will direct the user to file a report.

    Returns unchanged values if *source* is not in _YEAR_RANGES.
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

    if year_from is not None and year_from < min_start:
        print(
            f"  [WARNING] year_from={year_from} is before the earliest confirmed year "
            f"for '{source}' ({min_start}). Clamping to {min_start}."
        )
        new_from = min_start

    if year_to is not None and year_to > max_end:
        print(
            f"  [NOTE] year_to={year_to} is beyond the last confirmed year for "
            f"'{source}' ({max_end}). Attempting scrape anyway — this year has not "
            f"been verified and results are not guaranteed.\n"
            f"  If the scrape fails or data looks wrong, please report it at:\n"
            f"  {_ISSUES_URL}"
        )
        # Do NOT clamp — let the attempt proceed

    return new_from, year_to
