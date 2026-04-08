"""Utah election results parser — thin wrapper around Clarity.parser."""

from Clarity.parser import (
    parse_state_results,
    parse_county_results,
    county_name_from_url as _county_name_from_url,
)

_UT_COUNTY_SUFFIX = "-ut"

__all__ = ["parse_state_results", "parse_county_results", "county_name_from_url"]


def county_name_from_url(url: str) -> str:
    """Derive a human-readable county name from a Utah county URL.

    Thin wrapper around :func:`Clarity.parser.county_name_from_url` with
    the Utah county suffix ``"-ut"`` pre-filled.

    Examples
    --------
    >>> county_name_from_url(".../results/public/salt-lake-county-ut/elections/...")
    'Salt Lake County'
    >>> county_name_from_url(".../results/public/san-juan-county-ut/elections/...")
    'San Juan County'
    """
    return _county_name_from_url(url, county_suffix=_UT_COUNTY_SUFFIX)
