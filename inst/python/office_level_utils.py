"""
Shared office-level classifier: Federal / State / Local.

Public API
----------
classify_office_level(office)
    Regex-only classification — fast, no state context needed.
    Suitable for scrapers that don't carry a state_key (CT, IN, LA, GA, UT, NC).

lookup_office_level(office, state_key=None)
    Registry-first classification with regex fallback.
    When state_key is provided, checks the state-specific exact-match registry
    before falling back to regex.  Used by ElectionStats; available to all scrapers.

Three-tier classification
-------------------------
  1. Federal  — US Congress, President, Presidential Electors, US Senate
  2. State    — governor, lieutenant governor, all state executive officers,
                state legislature (both chambers), state courts, state boards
  3. Local    — everything else (mayor, county/city/town/municipal offices,
                school boards, sheriffs, local judges, special districts, etc.)

Adding a new state registry
---------------------------
Populate a dict below (same pattern as the existing states) and add it to
STATE_OFFICE_REGISTRIES.  Source: the state's ElectionStats /search page
"Office" dropdown, which groups offices into Federal / State / Local sections.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Federal offices  (regex)
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
# State offices  (regex)
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
# State-specific exact-match registries
# Source: each state's ElectionStats /search page "Office" dropdown.
# ---------------------------------------------------------------------------

# Idaho  —  https://canvass.sos.idaho.gov/eng/contests/search
_IDAHO: dict[str, str] = {
    # Federal
    "President":                                    "Federal",
    "U.S. Senate":                                  "Federal",
    "United States Representative":                 "Federal",

    # State
    "Governor":                                     "State",
    "Lieutenant Governor":                          "State",
    "Secretary of State":                           "State",
    "State Controller":                             "State",
    "State Treasurer":                              "State",
    "State Auditor":                                "State",
    "Attorney General":                             "State",
    "Superintendent of Public Instruction":         "State",
    "Supreme Court Justice":                        "State",
    "Appellate Court Judge":                        "State",
    "District Judge":                               "State",
    "State Senate":                                 "State",
    "State Senate A":                               "State",
    "State Senate B":                               "State",
    "State Senate C":                               "State",
    "State Representative A":                       "State",
    "State Representative B":                       "State",
    "State Representative C":                       "State",
    "State Representative D":                       "State",
    "State Representative E":                       "State",
    "State Representative F":                       "State",

    # Local — educational
    "North Idaho College Trustee":                  "Local",
    "College of Eastern Idaho Trustee":             "Local",
    "College of Western Idaho Trustee":             "Local",
    "College of Southern Idaho Trustee":            "Local",

    # Local — county
    "County Soil and Water Conservation District Supervisor": "Local",
    "County Clerk":                                 "Local",
    "County Prosecuting Attorney":                  "Local",
    "County Sheriff":                               "Local",
    "County Commissioner":                          "Local",
    "County Treasurer":                             "Local",
    "County Assessor":                              "Local",
    "County Coroner":                               "Local",
    "Clerk of the District Court":                  "Local",

    # Local — special districts
    "Ada County Highway District Commissioner":     "Local",
    "Port of Lewiston Commissioner":                "Local",
    "Central Fire District Commissioner":           "Local",
    "Richfield Fire District":                      "Local",
    "Richfield Fire District Commissioner":         "Local",

    # Local — municipal / other
    "Mayor":                                        "Local",
    "Alderman":                                     "Local",
    "Precinct Committeeman":                        "Local",
    "Water and Sewer District Director":            "Local",
    "Board of Directors":                           "Local",
}


# Colorado  —  https://co.elstats2.civera.com/eng/contests/search
_COLORADO: dict[str, str] = {
    # Federal
    "President":                                    "Federal",
    "Presidential Electors":                        "Federal",
    "United States Senator":                        "Federal",
    "United States Congressperson":                 "Federal",

    # State — executive
    "Governor":                                     "State",
    "Lieutenant Governor":                          "State",
    "Secretary of State":                           "State",
    "State Auditor":                                "State",
    "State Treasurer":                              "State",
    "Attorney General":                             "State",
    "Superintendent of Public Instruction":         "State",
    "State Board of Education":                     "State",
    "State Railroad Commissioner":                  "State",

    # State — legislature
    "State Senate":                                 "State",
    "State Representative":                         "State",

    # State — courts
    "Supreme Court":                                "State",
    "District Court":                               "State",

    # State — boards/regents
    "Regent of the University of Colorado":         "State",

    # Local
    "District Attorney":                            "Local",
    "Director Of Regional Transit District":        "Local",
    "Moffat Tunnel Commission":                     "Local",
    "Director of the Caddoa Reservoir and Arkansas Valley": "Local",
}


# New Hampshire  —  https://nh.electionstats.com/elections/search
_NEW_HAMPSHIRE: dict[str, str] = {
    # Federal
    "President":                                    "Federal",
    "Vice President":                               "Federal",
    "U.S. Senate":                                  "Federal",
    "U.S. House":                                   "Federal",

    # State
    "Governor":                                     "State",
    "Executive Council":                            "State",
    "State Senator":                                "State",
    "State Representative":                         "State",

    # Local — county
    "Sheriff":                                      "Local",
    "Attorney":                                     "Local",   # County Attorney
    "Treasurer":                                    "Local",   # County Treasurer
    "Register of Deeds":                            "Local",
    "Register of Probate":                          "Local",
    "County Commissioner":                          "Local",
}


# Vermont  —  https://electionarchive.vermont.gov/elections/search
_VERMONT: dict[str, str] = {
    # Federal
    "President":                                    "Federal",
    "U.S. Senate":                                  "Federal",
    "U.S. House":                                   "Federal",

    # State — executive
    "Governor":                                     "State",
    "Lieutenant Governor":                          "State",
    "Treasurer":                                    "State",
    "Secretary of State":                           "State",
    "Auditor":                                      "State",
    "Attorney General":                             "State",

    # State — legislature
    "State Senator":                                "State",
    "State Representative":                         "State",

    # Local — county
    "Sheriff":                                      "Local",
    "High Bailiff":                                 "Local",
    "State's Attorney":                             "Local",
    "Probate Judge":                                "Local",
    "Assistant Judge":                              "Local",

    # Local — town/municipal
    "Selectman":                                    "Local",
    "Select Board Member":                          "Local",
    "Town Clerk":                                   "Local",
    "Town Treasurer":                               "Local",
    "Town Agent":                                   "Local",
    "Town Constable":                               "Local",
    "Justice of the Peace":                         "Local",
    "Lister":                                       "Local",
    "School Director":                              "Local",
    "Mayor":                                        "Local",
    "City Councilor":                               "Local",
    "Alderman":                                     "Local",
}


# Virginia  —  https://historical.elections.virginia.gov/search?t=table
# Dropdown sections: Federal | State | Party Position | County/City | Local.
# Party Position and County/City entries → "Local".
_VIRGINIA: dict[str, str] = {
    # Federal
    "President":                                    "Federal",
    "U.S. Senate":                                  "Federal",
    "U.S. House":                                   "Federal",

    # State — executive
    "Governor":                                     "State",
    "Lieutenant Governor":                          "State",
    "Attorney General":                             "State",
    "Member of State Corporation Commission":       "State",

    # State — legislature
    "State Senate":                                 "State",
    "State Representative":                         "State",
    "Constitutional Convention":                    "State",

    # State — courts
    "Judge of Circuit Court":                       "State",
    "Justice of the Supreme Court of Virginia":     "State",
    "Judge of the Court of Appeals of Virginia":    "State",

    # County/City (→ Local)
    "County Board Member":                          "Local",
    "Chairman of the Board of Supervisors":         "Local",
    "Mayor":                                        "Local",
    "Board of Supervisors":                         "Local",
    "Vice Mayor":                                   "Local",
    "Chairman of the School Board":                 "Local",
    "School Board":                                 "Local",
    "City Council":                                 "Local",
    "Clerk of Court":                               "Local",
    "Commonwealth's Attorney":                      "Local",
    "Sheriff":                                      "Local",
    "Treasurer":                                    "Local",
    "Commissioner of the Revenue":                  "Local",
    "Town Recorder":                                "Local",
    "Town Council":                                 "Local",
    "Town Committee":                               "Local",
    "Soil and Water Conservation Director":         "Local",

    # Party Position (→ Local)
    "Democratic National Committeeman":             "Local",
    "Democratic National Committeewoman":           "Local",
    "Republican National Committeeman":             "Local",
    "Republican National Committeewoman":           "Local",
    "Precinct Captain":                             "Local",
}


# Massachusetts  —  https://electionstats.state.ma.us/elections/search
_MASSACHUSETTS: dict[str, str] = {
    # Federal
    "President":                                    "Federal",
    "United States Senator":                        "Federal",
    "Representative in Congress":                   "Federal",

    # State — executive
    "Governor":                                     "State",
    "Lieutenant Governor":                          "State",
    "Attorney General":                             "State",
    "Secretary of State":                           "State",
    "Treasurer and Receiver-General":               "State",
    "Auditor of the Commonwealth":                  "State",
    "Governor's Council":                           "State",

    # State — legislature
    "Senator in General Court":                     "State",
    "Representative in General Court":              "State",

    # Local
    "Sheriff":                                      "Local",
    "Register of Probate":                          "Local",
    "Clerk of Courts":                              "Local",
    "District Attorney":                            "Local",
    "County Commissioner":                          "Local",
}


# South Carolina  —  https://electionhistory.scvotes.gov/search?t=table
# Dropdown sections: Federal | State | Local | Other.  Local and Other → "Local".
_SOUTH_CAROLINA: dict[str, str] = {
    # Federal
    "President of the United States":               "Federal",
    "Presidential Preference":                      "Federal",
    "U.S. Senate":                                  "Federal",
    "U.S. House":                                   "Federal",

    # State — executive
    "Governor":                                     "State",
    "Lieutenant Governor":                          "State",
    "Secretary of State":                           "State",
    "Attorney General":                             "State",
    "State Treasurer":                              "State",
    "Comptroller General":                          "State",
    "Adjutant General":                             "State",
    "Commissioner of Agriculture":                  "State",
    "State Superintendent of Public Education":     "State",

    # State — legislature
    "State Senate":                                 "State",
    "State House":                                  "State",

    # Local
    "Register of Deeds":                            "Local",
    "Clerk of Court":                               "Local",
    "County Auditor":                               "Local",
    "County Treasurer":                             "Local",
    "County Council Member":                        "Local",
    "County Council Chairman":                      "Local",
    "County Supervisor":                            "Local",
    "County Coroner":                               "Local",
    "City Mayor":                                   "Local",
    "City Council":                                 "Local",
    "Town Mayor":                                   "Local",
    "Town Council":                                 "Local",
    "Fire District Trustee":                        "Local",
    "Probate Judge":                                "Local",
    "Register of Mesne Conveyance":                 "Local",
    "Sheriff":                                      "Local",

    # Other (special districts, boards, misc → Local)
    "Solicitor":                                    "Local",
    "Member of the Board of Education":             "Local",
    "School Board Chairman":                        "Local",
    "School Board Member":                          "Local",
    "School Trustee":                               "Local",
    "Board of Education Chair":                     "Local",
    "Consolidated School Board":                    "Local",
    "Constituent School Board":                     "Local",
    "Combined Utilities Commissioner":              "Local",
    "Commissioner of Public Works":                 "Local",
    "Fire District Commissioner":                   "Local",
    "Public Service District Commissioner":         "Local",
    "Soil-Water District Commissioner":             "Local",
    "Water District Commissioner":                  "Local",
    "Sanity Sewer District Commissioner":           "Local",
    "Water-Sewer Commissioner":                     "Local",
    "Watershed Commissioner":                       "Local",
    "Waterworks Commissioner":                      "Local",
    "Straight Party":                               "Local",
}


# New Mexico  —  https://electionstats.sos.nm.gov/search?t=table
# Dropdown sections: Federal | State | County | Local | Other.
# County, Local, and Other entries all → "Local".
_NEW_MEXICO: dict[str, str] = {
    # Federal
    "President of the United States":               "Federal",
    "United States Senator":                        "Federal",
    "United States Representative":                 "Federal",

    # State — executive
    "Governor":                                     "State",
    "Governor and Lieutenant Governor":             "State",
    "Lieutenant Governor":                          "State",
    "Secretary of State":                           "State",
    "Attorney General":                             "State",
    "State Auditor":                                "State",
    "State Treasurer":                              "State",
    "Commissioner of Public Lands":                 "State",
    "Public Education Commissioner":                "State",
    "Public Regulation Commissioner":               "State",

    # State — legislature
    "State Senate":                                 "State",
    "State Representative":                         "State",

    # State — other statewide
    "State Board of Education":                     "State",
    "District Attorney":                            "State",

    # State — courts
    "Justice of The Supreme Court":                 "State",
    "Judge of the Court of Appeals":                "State",
    "District Judge":                               "State",

    # County (→ Local)
    "Magistrate Judge":                             "Local",
    "Probate Judge":                                "Local",
    "County Commissioner":                          "Local",
    "County Council":                               "Local",
    "County Sheriff":                               "Local",
    "County Assessor":                              "Local",
    "County Clerk":                                 "Local",
    "County Treasurer":                             "Local",
    "County Surveyor":                              "Local",

    # Local
    "Metropolitan Court Judge":                     "Local",
    "Municipal Judge":                              "Local",
    "Mayor":                                        "Local",
    "City Mayor":                                   "Local",
    "City Commissioner":                            "Local",
    "City Councilor":                               "Local",
    "City Trustee":                                 "Local",
    "Town Mayor":                                   "Local",
    "Town Commissioner":                            "Local",
    "Town Councilor":                               "Local",
    "Town Trustee":                                 "Local",
    "Village Mayor":                                "Local",
    "Village Councilor":                            "Local",
    "Village Trustee":                              "Local",
    "Soil-Water Supervisor":                        "Local",

    # Other (special districts, boards → Local)
    "School Board Member":                          "Local",
    "College Board Member":                         "Local",
    "Flood Control Authority Board of Directors":   "Local",
    "Hospital District Board of Directors":         "Local",
    "Public Improvement Director":                  "Local",
    "Public Water Works Authority Director":        "Local",
    "Special Zoning Board Member":                  "Local",
    "Tax Increment Development Director":           "Local",
    "Water-Sanitation Director":                    "Local",
    "Watershed Supervisor":                         "Local",
    "Watershed Treasurer":                          "Local",
}


# New York  —  https://results.elections.ny.gov/search?t=table
# Dropdown sections: Federal | State | Party Position | County | Local.
# Party Position, County, and Local entries → "Local".
_NEW_YORK: dict[str, str] = {
    # Federal
    "President of the United States":               "Federal",
    "United States Senator":                        "Federal",
    "Representative in Congress":                   "Federal",

    # State — executive
    "Governor":                                     "State",
    "Lieutenant Governor":                          "State",
    "State Comptroller":                            "State",
    "Attorney General":                             "State",

    # State — legislature
    "State Senator":                                "State",
    "Member of Assembly":                           "State",

    # State — courts
    "Supreme Court Justice":                        "State",
    "Associate Judge of Court of Appeals":          "State",
    "Judge of Court of Appeals":                    "State",

    # State — other
    "State Committee":                              "State",

    # County (→ Local)
    "Sheriff":                                      "Local",
    "County Clerk":                                 "Local",
    "County Judge":                                 "Local",
    "Surrogate":                                    "Local",
    "District Attorney":                            "Local",
    "County Legislator":                            "Local",
    "Member of County Legislature":                 "Local",
    "Coroner":                                      "Local",

    # Local
    "Mayor":                                        "Local",
    "City Council Member":                          "Local",
    "Alderperson":                                  "Local",
    "Town Supervisor":                              "Local",
    "Town Justice":                                 "Local",
    "Town Clerk":                                   "Local",
    "Village Mayor":                                "Local",
    "Village Trustee":                              "Local",

    # Party Position (→ Local)
    "Presidential Delegate":                        "Local",
    "Alternate Presidential Delegate":              "Local",
    "Judicial Delegate":                            "Local",
    "Alternate Judicial Delegate":                  "Local",
    "Democratic District Leader":                   "Local",
    "Republican District Leader":                   "Local",
    "Male Democratic District Leader":              "Local",
    "Female Democratic District Leader":            "Local",
    "National Committeeman":                        "Local",
    "National Committeewoman":                      "Local",
}


# Master registry — add new states here
STATE_OFFICE_REGISTRIES: dict[str, dict[str, str]] = {
    "idaho":          _IDAHO,
    "colorado":       _COLORADO,
    "new_hampshire":  _NEW_HAMPSHIRE,
    "vermont":        _VERMONT,
    "virginia":       _VIRGINIA,
    "massachusetts":  _MASSACHUSETTS,
    "south_carolina": _SOUTH_CAROLINA,
    "new_mexico":     _NEW_MEXICO,
    "new_york":       _NEW_YORK,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_office_level(office: str | None) -> str:
    """Return ``'Federal'``, ``'State'``, or ``'Local'`` using regex only.

    Suitable for scrapers that don't carry a state_key (CT, IN, LA, GA, UT, NC).
    For state-aware classification use :func:`lookup_office_level`.
    """
    if not office:
        return "Local"
    if _FEDERAL_RE.search(office):
        return "Federal"
    if _STATE_RE.search(office):
        return "State"
    return "Local"


def lookup_office_level(office: str | None, state_key: str | None = None) -> str:
    """Return ``'Federal'``, ``'State'``, or ``'Local'`` for an office string.

    Checks the state-specific registry first (exact match, case-insensitive),
    then falls back to :func:`classify_office_level`.

    Parameters
    ----------
    office : str | None
        Office name as it appears in the scraper output.
    state_key : str | None
        State key (e.g. ``'idaho'``, ``'colorado'``).  When provided,
        the state-specific registry is consulted first.
    """
    if not office:
        return "Local"

    if state_key:
        registry = STATE_OFFICE_REGISTRIES.get(
            state_key.strip().lower().replace(" ", "_"), {}
        )
        office_stripped = office.strip()
        level = registry.get(office_stripped)
        if level is None:
            office_lower = office_stripped.lower()
            level = next(
                (v for k, v in registry.items() if k.lower() == office_lower),
                None,
            )
        if level is not None:
            return level

    return classify_office_level(office)
