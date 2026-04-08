"""Georgia SOS parser — thin wrapper around Clarity.parser."""

from Clarity.parser import (
    parse_state_results,
    parse_county_results,
    county_name_from_url as _county_name_from_url,
)

_GA_COUNTY_SUFFIX = "-ga"

__all__ = ["parse_state_results", "parse_county_results", "county_name_from_url"]


def county_name_from_url(url: str) -> str:
    """Derive a human-readable county name from a Georgia SOS county URL.

    Thin wrapper around :func:`Clarity.parser.county_name_from_url` with
    the Georgia county suffix ``"-ga"`` pre-filled.

    Examples
    --------
    >>> county_name_from_url(".../results/public/fulton-county-ga/elections/...")
    'Fulton County'
    >>> county_name_from_url(".../results/public/jeff-davis-county-ga/elections/...")
    'Jeff Davis County'
    """
    return _county_name_from_url(url, county_suffix=_GA_COUNTY_SUFFIX)
