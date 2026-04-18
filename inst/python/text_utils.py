"""
Text / string helpers shared across DownBallotR scrapers.
"""

from __future__ import annotations

import re
from typing import Optional


def clean_text(s: str | None) -> str:
    """Normalise whitespace in a plain string."""
    return re.sub(r"\s+", " ", s or "").strip()


def clean_node(node) -> str:
    """Normalise whitespace in the text content of an lxml element."""
    try:
        return clean_text(node.text_content() or "")
    except Exception:
        return ""


def parse_int(s: str | None) -> Optional[int]:
    """Parse an integer from a string, stripping commas (e.g. '12,345' → 12345)."""
    s = (s or "").strip().replace(",", "")
    return int(s) if s.isdigit() else None


def parse_percentage(p: str | None) -> Optional[float]:
    """Parse a percentage string (e.g. '48.3%' or '48.3') to a float."""
    if not p:
        return None
    try:
        return float(str(p).replace("%", "").strip())
    except ValueError:
        return None


# ── Candidate-cell helpers ────────────────────────────────────────────────────

def strip_trailing_parens(name: str) -> str:
    """Remove a trailing parenthetical suffix from a candidate name.

    Examples
    --------
    >>> strip_trailing_parens("Jane Smith (R)")
    'Jane Smith'
    >>> strip_trailing_parens("John Doe (i)")
    'John Doe'
    """
    return re.sub(r"\s*\([^)]+\)\s*$", "", name).strip()


def extract_party_from_parens(text: str) -> str:
    """Extract party abbreviation from the last parenthetical in *text*.

    Examples
    --------
    >>> extract_party_from_parens("Jane Smith (Republican)")
    'Republican'
    >>> extract_party_from_parens("No party here")
    ''
    """
    m = re.search(r"\(([^)]+)\)\s*$", text)
    return m.group(1).strip() if m else ""


def is_incumbent(text: str) -> bool:
    """Return True if *text* contains the incumbent marker ``(i)``."""
    return "(i)" in text


# ── Party normalization ───────────────────────────────────────────────────────

# Keys are uppercase; values are canonical full names.
_PARTY_CANONICAL: dict[str, str] = {
    # Single-letter codes (Indiana / other compact sources)
    "D": "Democratic", "R": "Republican", "I": "Independent",
    "L": "Libertarian", "G": "Green", "C": "Constitution",
    "N": "Nonpartisan", "O": "Other", "P": "Progressive", "W": "Workers",
    # Common multi-letter abbreviations
    "DEM": "Democratic", "REP": "Republican", "IND": "Independent",
    "LIB": "Libertarian", "GRN": "Green", "CON": "Constitution",
    "NOP": "No Party",   "NOPTY": "No Party",
    "WRI": "Write-In",
    # Full-name variants (Democrat vs. Democratic, etc.)
    "DEMOCRAT": "Democratic",      "DEMOCRATIC": "Democratic",
    "REPUBLICAN": "Republican",    "INDEPENDENT": "Independent",
    "LIBERTARIAN": "Libertarian",  "GREEN": "Green",
    "CONSTITUTION": "Constitution",
    "NONPARTISAN": "Nonpartisan",  "NON-PARTISAN": "Nonpartisan",
    "NONPARTISAN OFFICE": "Nonpartisan",
    "NO PARTY": "No Party",        "NO PARTY AFFILIATION": "No Party",
    "OTHER": "Other",              "PROGRESSIVE": "Progressive",
    "WORKERS": "Workers",
    "WRITE-IN": "Write-In",        "WRITE IN": "Write-In",
    "WRITEIN": "Write-In",
    # Indiana-specific abbreviations
    "CP": "Citizens Party",        "LB": "Long Beach Party",
    "AMER SOLID": "American Solidarity",
    "COM": "Communist",
    "EP": "Elkhart Party",         "PP": "People's Party",
    "PI": "Pirate",                "SP": "Socialist",
    "T": "Taxpayers",
    # Blank-ballot markers — not a party; normalize to empty string
    "BLANKS": "",                  "BLANK": "",
}


def normalize_party(raw: "str | None") -> str:
    """Normalize a raw party string to a canonical full name.

    Handles parenthetical wrappers ``(DEM)``, common abbreviations ``D`` /
    ``DEM``, trailing ``" Party"`` suffixes, and full-name variants
    (``"Democrat"`` → ``"Democratic"``).  Unknown values are returned as-is
    after stripping parentheses and the ``" Party"`` suffix.  Returns ``""``
    for blank / None input.

    Examples
    --------
    >>> normalize_party("(DEM)")
    'Democratic'
    >>> normalize_party("Democratic Party")
    'Democratic'
    >>> normalize_party("D")
    'Democratic'
    >>> normalize_party("Write-In")
    'Write-In'
    """
    if not raw:
        return ""
    s = raw.strip()
    if not s:
        return ""

    # Strip surrounding parentheses: "(DEM)" → "DEM"
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1].strip()

    # Direct case-insensitive lookup (preserves e.g. "Citizens Party" as-is)
    result = _PARTY_CANONICAL.get(s.upper())
    if result is not None:
        return result

    # Try stripping trailing " Party" / " party": "Democratic Party" → "Democratic"
    stripped = re.sub(r"\s+Party\s*$", "", s, flags=re.IGNORECASE).strip()
    if stripped != s:
        result = _PARTY_CANONICAL.get(stripped.upper())
        if result is not None:
            return result

    return stripped if stripped else s


def ensure_percent_suffix(pct: str) -> str:
    """Ensure a percentage string ends with ``%``.

    Examples
    --------
    >>> ensure_percent_suffix("48.3")
    '48.3%'
    >>> ensure_percent_suffix("48.3%")
    '48.3%'
    """
    if pct and "%" not in pct:
        return pct + "%"
    return pct
