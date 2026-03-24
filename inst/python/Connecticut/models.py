"""Data models for the Connecticut CTEMS election results scraper."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Optional

# Regex patterns for extracting dates from election names.
# CT election names take forms like:
#   "2024 November General Election"
#   "August 13, 2024 Primary Election"
#   "November 7, 2023 Municipal Election"
#   "November 2022 General Election"
_FULL_DATE_RE = re.compile(
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),?\s+(?P<year>20\d{2}|19\d{2})"
)
_MONTH_YEAR_RE = re.compile(
    r"(?P<year>20\d{2}|19\d{2})\s+(?P<month>[A-Za-z]+)"
    r"|(?P<month2>[A-Za-z]+)\s+(?P<year2>20\d{2}|19\d{2})"
)
_YEAR_ONLY_RE = re.compile(r"\b(20\d{2}|19\d{2})\b")

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_election_date(name: str) -> tuple[int | None, date | None]:
    """Extract (year, date) from an election name string.

    Returns
    -------
    tuple of (year: int | None, election_date: date | None)
        ``election_date`` is only set when a full month+day+year is parseable;
        otherwise it is None and only ``year`` is set.
    """
    # Try full date: "August 13, 2024"
    m = _FULL_DATE_RE.search(name)
    if m:
        month_str = m.group("month").lower()
        month = _MONTH_MAP.get(month_str)
        year = int(m.group("year"))
        day = int(m.group("day"))
        if month:
            try:
                return year, date(year, month, day)
            except ValueError:
                return year, None

    # Try year + month or month + year: "2024 November" / "November 2024"
    m = _MONTH_YEAR_RE.search(name)
    if m:
        year_str = m.group("year") or m.group("year2")
        month_str = (m.group("month") or m.group("month2") or "").lower()
        if year_str:
            year = int(year_str)
            month = _MONTH_MAP.get(month_str)
            if month:
                # Use the 1st of the month as a proxy date for ordering
                try:
                    return year, date(year, month, 1)
                except ValueError:
                    return year, None
            return year, None

    # Fallback: year only
    m = _YEAR_ONLY_RE.search(name)
    if m:
        return int(m.group(1)), None

    return None, None


@dataclass
class CtElectionInfo:
    """Metadata for a single Connecticut election discovered from the CTEMS dropdown.

    Attributes
    ----------
    name : str
        Human-readable election name as shown in the dropdown
        (e.g. "2024 November General Election").
    year : int
        Calendar year of the election.
    election_date : date | None
        Parsed election date when a full month/day/year is available;
        None when only a year (or year+month) could be extracted.
    option_value : str
        The ``<option value="...">`` attribute used to select this election
        in the CTEMS dropdown.
    """

    name: str
    year: int
    election_date: Optional[date]
    option_value: str

    @classmethod
    def from_option(cls, name: str, option_value: str) -> "CtElectionInfo":
        """Construct from a dropdown option name + value, parsing the date."""
        year, election_date = _parse_election_date(name)
        if year is None:
            raise ValueError(
                f"Could not extract a year from election name: {name!r}"
            )
        return cls(
            name=name,
            year=year,
            election_date=election_date,
            option_value=option_value,
        )
