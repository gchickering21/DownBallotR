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
