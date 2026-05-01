"""Data models for the Harris County (harrisvotes.com) election results scraper.

Source: https://www.harrisvotes.com/Election-Results/Archives
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

_FULL_DATE_RE = re.compile(
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),?\s+(?P<year>20\d{2}|19\d{2})"
)
_YEAR_ONLY_RE = re.compile(r"\b(20\d{2}|19\d{2})\b")

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Ordered list: earlier patterns take priority so "primary runoff" beats "primary".
_ELECTION_TYPE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"primary\s+runoff",    re.I), "Primary Runoff"),
    (re.compile(r"general\s+runoff",    re.I), "General Runoff"),
    (re.compile(r"\brunoff\b",          re.I), "Runoff"),
    (re.compile(r"\bprimary\b",         re.I), "Primary"),
    (re.compile(r"\bgeneral\b",         re.I), "General"),
    (re.compile(r"\bspecial\b",         re.I), "Special"),
    (re.compile(r"\bconstitutional\b",  re.I), "Constitutional"),
    (re.compile(r"\bcanvass\b",         re.I), "Canvass"),
]


def _classify_election_type(text: str) -> str:
    for pat, label in _ELECTION_TYPE_PATTERNS:
        if pat.search(text):
            return label
    return "Election"


def _parse_election_info(text: str) -> "tuple[int | None, date | None, str]":
    """Extract (year, election_date, election_type) from a combined date+name string."""
    m = _FULL_DATE_RE.search(text)
    if m:
        month_str = m.group("month").lower()[:3]
        month = _MONTH_MAP.get(month_str)
        year = int(m.group("year"))
        day = int(m.group("day"))
        try:
            parsed_date = date(year, month, day) if month else None
        except ValueError:
            parsed_date = None
    else:
        ym = _YEAR_ONLY_RE.search(text)
        year = int(ym.group(1)) if ym else None
        parsed_date = None

    election_type = _classify_election_type(text)
    return year, parsed_date, election_type


@dataclass
class HoustonElectionInfo:
    """Metadata for one Harris County election discovered from the archive page.

    Attributes
    ----------
    name : str
        Election name from the archive, e.g. "2022 General Election".
    year : int
        Calendar year.
    election_date : date | None
        Parsed election date (None when only the year is available).
    election_type : str
        Classified type: "General", "Primary", "Runoff", "Primary Runoff", "Special".
    cumulative_url : str | None
        URL to the cumulative (county-wide totals) results PDF.
    canvass_url : str | None
        URL to the canvass (precinct-level) results PDF.
    """

    name: str
    year: int
    election_date: Optional[date]
    election_type: str
    cumulative_url: Optional[str]
    canvass_url: Optional[str]

    @classmethod
    def from_archive_row(
        cls,
        date_text: str,
        name_text: str,
        cumulative_url: Optional[str],
        canvass_url: Optional[str],
    ) -> "HoustonElectionInfo":
        """Construct from raw discovery data."""
        combined = f"{date_text} {name_text}".strip()
        year, election_date, election_type = _parse_election_info(combined)
        if year is None:
            raise ValueError(f"Could not extract year from: {combined!r}")
        name = name_text.strip() or date_text.strip()
        return cls(
            name=name,
            year=year,
            election_date=election_date,
            election_type=election_type,
            cumulative_url=cumulative_url,
            canvass_url=canvass_url,
        )
