"""
Shared office-level classifier: Federal / State / Local.

Usage
-----
    from office_level_utils import classify_office_level

    classify_office_level("US Senate")                  # -> "Federal"
    classify_office_level("Governor")                   # -> "State"
    classify_office_level("Mayor")                      # -> "Local"
    classify_office_level("Unknown Special Commission") # -> "Local"

Design
------
Three-tier classification applied in order:

  1. Federal  — US Congress, President, Presidential Electors, US Senate
  2. State    — governor, lieutenant governor, all state executive officers,
                state legislature (both chambers), state courts, state boards
  3. Local    — everything else (mayor, county/city/town/municipal offices,
                school boards, sheriffs, local judges, special districts, etc.)

The regexes are deliberately broad: they match on key words rather than
requiring full phrase matches, so they work across the varied formatting
conventions of all supported scrapers (NC raw CSV names, LA SOS labels,
ElectionStats office strings, etc.).
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Federal offices
# ---------------------------------------------------------------------------
_FEDERAL_RE = re.compile(
    r"presidential\s+elector"
    r"|president\s+of\s+the\s+united\s+states"
    r"|u\.?\s*s\.?\s+president"
    r"|us\s+president"
    r"|\bpresident\b"                           # Idaho: "President"

    r"|united\s+states\s+senator"
    r"|u\.?\s*s\.?\s+senator"
    r"|u\.?\s*s\.?\s+senate"                    # Idaho: "U.S. Senate"
    r"|us\s+senate"
    r"|senator\s+in\s+congress"

    r"|united\s+states\s+representative"        # Idaho: "United States Representative"
    r"|representative\s+in\s+congress"
    r"|u\.?\s*s\.?\s+representative"
    r"|us\s+house\s+of\s+representatives"
    r"|us\s+house"
    r"|u\.?\s*s\.?\s+house"
    r"|congressman|congresswoman"
    r"|congressional",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# State offices
# ---------------------------------------------------------------------------
_STATE_RE = re.compile(
    # Executive
    r"\bgovernor\b"
    r"|lieutenant\s+governor|lt\.?\s*gov(?:ernor)?"
    r"|attorney\s+general"
    r"|secretary\s+of\s+(the\s+)?state"
    r"|state\s+treasurer|\btreasurer\b"
    r"|state\s+comptroller|\bcomptroller\b"
    r"|state\s+controller|\bcontroller\b"       # Idaho: "State Controller"
    r"|state\s+auditor|\bauditor\b"
    r"|superintendent\s+of\s+(public\s+)?instruction"
    r"|commissioner\s+of\s+(agriculture|insurance|labor)"

    # Legislature
    r"|state\s+senator"
    r"|state\s+senate"
    r"|state\s+representative"
    r"|state\s+house"
    r"|general\s+assembly"
    r"|house\s+of\s+representatives"       # catches "NC House of Representatives"
    r"|state\s+senate"

    # Courts (state level)
    r"|supreme\s+court"
    r"|court\s+of\s+appeals"
    r"|superior\s+court"
    r"|district\s+court\s+judge"
    r"|\bdistrict\s+judge\b"                    # Idaho: "District Judge"
    r"|circuit\s+court"
    r"|appellate\s+court"

    # Other statewide
    r"|state\s+school\s+board"
    r"|insurance\s+commissioner"
    r"|labor\s+commissioner"
    r"|agriculture\s+commissioner",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Local offices  (explicit list; everything not Federal/State falls here anyway)
# ---------------------------------------------------------------------------
# These are checked *after* Federal and State, so they only fire when the
# office did not match either of those patterns.  Listed for documentation.
# The catch-all "Local" is the default when neither _FEDERAL_RE nor _STATE_RE
# match — no separate _LOCAL_RE is needed.

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_office_level(office: str | None) -> str:
    """Return ``'Federal'``, ``'State'``, or ``'Local'`` for an office string.

    Parameters
    ----------
    office : str | None
        Office name as it appears in any scraper output.  May be None or empty.

    Returns
    -------
    str
        ``'Federal'``, ``'State'``, or ``'Local'``.
        ``'Local'`` is the default when no pattern matches.
    """
    if not office:
        return "Local"
    if _FEDERAL_RE.search(office):
        return "Federal"
    if _STATE_RE.search(office):
        return "State"
    return "Local"
