"""
Date / year-range helpers shared across DownBallotR scrapers.
"""

from __future__ import annotations

from datetime import date


def year_to_date_range(
    year_from: int | None,
    year_to: int | None,
) -> tuple[date | None, date | None]:
    """Convert year integers to a (start_date, end_date) inclusive date range.

    Jan 1 of *year_from* through Dec 31 of *year_to*.  Either bound may be
    ``None`` to indicate no limit.

    Examples
    --------
    >>> year_to_date_range(2022, 2024)
    (datetime.date(2022, 1, 1), datetime.date(2024, 12, 31))
    >>> year_to_date_range(None, 2024)
    (None, datetime.date(2024, 12, 31))
    """
    start = date(int(year_from), 1,  1)  if year_from is not None else None
    end   = date(int(year_to),   12, 31) if year_to   is not None else None
    return start, end


def validate_year_range(
    effective_years: list[int],
    min_supported: int,
    scraper_name: str,
) -> list[int]:
    """Validate *effective_years* against *min_supported* and print warnings.

    Parameters
    ----------
    effective_years : list[int]
        The full list of years being requested.
    min_supported : int
        The earliest year for which data is available.
    scraper_name : str
        Human-readable label used in warning messages.

    Returns
    -------
    list[int]
        The unsupported years (empty list if all years are supported).
        Callers should bail early if the returned list equals *effective_years*
        (i.e. ``all(y < min_supported for y in effective_years)``).
    """
    unsupported = [y for y in effective_years if y < min_supported]
    if not unsupported:
        return []

    available_msg = (
        f"Available years for the {scraper_name}: {min_supported}–present."
    )

    if all(y < min_supported for y in effective_years):
        print(
            f"[{scraper_name}] No data for year(s) {sorted(unsupported)} — "
            f"data begins in {min_supported}.\n{available_msg}"
        )
    else:
        print(
            f"[{scraper_name}] WARNING: year(s) {sorted(unsupported)} are before "
            f"{min_supported}. Those years will return no data.\n{available_msg}"
        )
    return unsupported
