"""Data models for the City of Boston election results scraper.

Source: https://www.boston.gov/departments/elections/state-and-city-boston-election-results
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

# Matches link text that names a party rather than an office.
# e.g. "Democratic party", "Republican party", "Libertarian party"
_PARTY_LINK_RE = re.compile(
    r"\b(democratic|republican|libertarian|green|socialist|constitution"
    r"|working\s+families|unenrolled|independent)\b",
    re.IGNORECASE,
)

# Maps raw party strings from link text to canonical names.
_PARTY_CANONICAL = {
    "democratic": "Democratic",
    "republican": "Republican",
    "libertarian": "Libertarian",
    "green": "Green",
    "socialist": "Socialist",
    "constitution": "Constitution",
    "working families": "Working Families",
    "unenrolled": "Unenrolled",
    "independent": "Independent",
}

# Office name normalizations (lowercased key → canonical string).
_OFFICE_MAP = {
    "mayor": "Mayor",
    "mayoral": "Mayor",
    "city council": "City Councillor",
    "city councillor": "City Councillor",
    "city councilor": "City Councillor",
    "school committee": "School Committee",
}

# Suffix patterns that denote a district within an office name.
# Captures the full district string (e.g. "District 1", "Ward 5").
_DISTRICT_RE = re.compile(
    r"(?:\s*[-–]\s*|\s+)((?:District|Ward)\s+\S+(?:\s+\S+)*)\s*$",
    re.IGNORECASE,
)

# Trailing " Results" or " Result" from group labels like "City Council Results".
_RESULTS_SUFFIX_RE = re.compile(r"\s+Results?\s*:?\s*$", re.IGNORECASE)


def _parse_election_name(name: str) -> tuple[int | None, date | None, str]:
    """Extract (year, election_date, election_type) from a drawer label.

    Drawer labels look like:
      "November 4, 2025: General Municipal Election"
      "September 3, 2024: State Primary"
      "November 5, 2024: State Election"

    Returns
    -------
    (year, election_date, election_type)
        election_date is None when only the year can be extracted.
        election_type is the text after the colon, or "" if no colon present.
    """
    election_type = ""
    colon_idx = name.find(":")
    if colon_idx >= 0:
        election_type = name[colon_idx + 1:].strip()
        date_part = name[:colon_idx]
    else:
        date_part = name

    m = _FULL_DATE_RE.search(date_part)
    if m:
        month_str = m.group("month").lower()[:3]
        month = _MONTH_MAP.get(month_str)
        year = int(m.group("year"))
        day = int(m.group("day"))
        if month:
            try:
                return year, date(year, month, day), election_type
            except ValueError:
                return year, None, election_type
        return year, None, election_type

    m = _YEAR_ONLY_RE.search(date_part)
    if m:
        return int(m.group(1)), None, election_type

    return None, None, election_type


def _normalize_party(text: str) -> str:
    """Return a canonical party name from link text, or '' if not a party label."""
    m = _PARTY_LINK_RE.search(text)
    if not m:
        return ""
    key = m.group(1).lower().replace("-", " ")
    return _PARTY_CANONICAL.get(key, m.group(1).title())


def _normalize_office(raw: str) -> tuple[str, str]:
    """Return (office, district) from a raw office string.

    Handles patterns like:
      "City Council District 1"   → ("City Councillor", "District 1")
      "City Council At-Large"     → ("City Councillor At-Large", "")
      "Mayor"                     → ("Mayor", "")
      "U.S. Senate"               → ("U.S. Senate", "")
      "State Senator"             → ("State Senator", "")
    """
    raw = _RESULTS_SUFFIX_RE.sub("", raw).strip()

    # Extract trailing district
    dm = _DISTRICT_RE.search(raw)
    if dm:
        district = dm.group(1).strip()
        base = raw[: dm.start()].strip().rstrip("-–").strip()
    else:
        district = ""
        base = raw

    # Normalize "At Large" / "At-Large" attached to the office
    at_large_suffix = ""
    al_m = re.search(r"\s+[Aa][Tt][-\s][Ll][Aa][Rr][Gg][Ee]\s*$", base)
    if al_m:
        at_large_suffix = " At-Large"
        base = base[: al_m.start()].strip()

    key = base.lower().strip()
    office = _OFFICE_MAP.get(key, base.title())
    return office + at_large_suffix, district


@dataclass
class BostonResultLink:
    """One PDF result file discovered from the Boston.gov elections page.

    Attributes
    ----------
    link_text : str
        Anchor text of the link, e.g. "Mayor", "City Council District 1",
        "Democratic party".
    group_label : str
        The <h5> section heading under which this link appears, e.g.
        "Mayoral Results", "City Council Results", "U.S. Senate".
        Used as the office source when link_text is a party name.
    href : str
        Relative href from the page, e.g. "/sites/default/files/...pdf".
    pdf_url : str
        Absolute URL to the PDF.
    office : str
        Normalised office name, e.g. "City Councillor", "Mayor".
    district : str
        District string, e.g. "District 1", "District 9", or "".
    party : str
        Party name when the PDF covers a single-party primary, e.g.
        "Democratic".  Empty string for general/nonpartisan races.
    """

    link_text: str
    group_label: str
    href: str
    pdf_url: str
    office: str
    district: str
    party: str

    @classmethod
    def from_link(
        cls,
        link_text: str,
        group_label: str,
        href: str,
        base_url: str,
    ) -> "BostonResultLink":
        """Build a BostonResultLink from raw discovery data."""
        pdf_url = href if href.startswith("http") else f"{base_url}{href}"
        party = _normalize_party(link_text)

        # Office/district from link_text when it names an office;
        # fall back to group_label when link_text is a party name.
        raw_office_text = group_label if party else link_text
        office, district = _normalize_office(raw_office_text)

        return cls(
            link_text=link_text,
            group_label=group_label,
            href=href,
            pdf_url=pdf_url,
            office=office,
            district=district,
            party=party,
        )


@dataclass
class BostonElectionInfo:
    """Metadata for one election discovered from the Boston.gov page.

    Attributes
    ----------
    name : str
        Full drawer label, e.g. "November 4, 2025: General Municipal Election".
    year : int
        Calendar year of the election.
    election_date : date | None
        Parsed election date; None when only year could be extracted.
    election_type : str
        Text after the colon in the name, e.g. "General Municipal Election".
    result_links : list[BostonResultLink]
        All PDF result links found in the election's drawer.
    """

    name: str
    year: int
    election_date: Optional[date]
    election_type: str
    result_links: list[BostonResultLink] = field(default_factory=list)

    @classmethod
    def from_drawer(
        cls,
        name: str,
        result_links: list[BostonResultLink],
    ) -> "BostonElectionInfo":
        """Construct from a drawer title and its pre-built result links."""
        year, election_date, election_type = _parse_election_name(name)
        if year is None:
            raise ValueError(f"Could not extract year from election name: {name!r}")
        return cls(
            name=name,
            year=year,
            election_date=election_date,
            election_type=election_type,
            result_links=result_links,
        )
