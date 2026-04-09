"""Data models for the Louisiana Secretary of State election results scraper.

Source: https://voterportal.sos.la.gov/Graphical
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Optional

# Louisiana election option labels from the dropdown look like:
#   "Sat May 16 2026"
#   "Tues Nov 5 2024"
#   "Tues Mar 9 2004"
# Format: {DayOfWeek} {Month} {Day} {Year}
# The day-of-week abbreviation may vary ("Sat", "Tues", "Mon", etc.)
_DOW_DATE_RE = re.compile(
    r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*\s+([A-Za-z]+)\s+(\d{1,2})\s+(\d{4})",
    re.IGNORECASE,
)
_YEAR_ONLY_RE = re.compile(r"\b(19|20)\d{2}\b")

_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_election_date(name: str) -> tuple[int | None, date | None]:
    """Extract (year, election_date) from a Louisiana election option label.

    Handles the live site format "Sat May 16 2026" / "Tues Nov 5 2024".
    Falls back to extracting a bare 4-digit year if no full date is found.

    Returns
    -------
    tuple of (year: int | None, election_date: date | None)
    """
    m = _DOW_DATE_RE.search(name)
    if m:
        month_str = m.group(1)[:3].lower()
        day = int(m.group(2))
        year = int(m.group(3))
        month = _MONTH_MAP.get(month_str)
        if month:
            try:
                return year, date(year, month, day)
            except ValueError:
                return year, None
        return year, None

    m = _YEAR_ONLY_RE.search(name)
    if m:
        return int(m.group(0)), None

    return None, None


@dataclass
class LaElectionInfo:
    """Metadata for a single Louisiana election discovered from the SOS dropdown.

    Attributes
    ----------
    name : str
        Human-readable election name as shown in the dropdown
        (e.g. "Tues Nov 5 2024", "Sat Dec 7 2024").
    year : int
        Calendar year of the election.
    election_date : date | None
        Parsed election date; None when the full date cannot be extracted.
    option_value : str
        The ``<option value="...">`` attribute used to select this election
        in the dropdown.
    """

    name: str
    year: int
    election_date: Optional[date]
    option_value: str

    @classmethod
    def from_option(cls, name: str, option_value: str) -> "LaElectionInfo":
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
