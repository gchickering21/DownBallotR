"""
office_registry.py
==================
State-specific office → office_level lookup tables for ElectionStats scrapers.

Each registry is derived from the "Office" dropdown on that state's ElectionStats
search page.  The dropdown groups offices into sections (Federal / State / Local /
Other); those sections map directly to the three-tier classification used throughout
DownBallotR:

    Federal  → offices of the U.S. government
    State    → state-level executive, legislative, and judicial offices
    Local    → county, municipal, district, and special-district offices

Usage
-----
    from ElectionStats.office_registry import lookup_office_level

    lookup_office_level("U.S. Senate",    "idaho")      # -> "Federal"
    lookup_office_level("District Judge", "idaho")      # -> "State"
    lookup_office_level("County Sheriff", "idaho")      # -> "Local"
    lookup_office_level("Governor",       "virginia")   # -> "State" (regex fallback)

Design
------
1. If the office string (exact match, case-insensitive) appears in the
   state's registry → return the registered level.
2. Otherwise fall back to the regex-based ``classify_office_level`` from
   ``office_level_utils``.

To add a new state: populate a dict below and add it to STATE_OFFICE_REGISTRIES.
Source: each state's ElectionStats /search page "Office" dropdown.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Idaho  —  https://canvass.sos.idaho.gov/eng/contests/search
# Dropdown groups: Federal | State | Local (county, district, municipal, other)
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Colorado  —  https://co.elstats2.civera.com/eng/contests/search
# Dropdown shows a flat list; groups inferred from office type.
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# New Hampshire  —  https://nh.electionstats.com/elections/search
# Flat dropdown; groups inferred from office type.
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Vermont  —  https://electionarchive.vermont.gov/elections/search
# Dropdown sections: Federal | State | Local (county + town offices).
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Virginia  —  https://historical.elections.virginia.gov/search?t=table
# Civera v2 React platform (same as SC/NM/NY).
# Dropdown sections: Federal | State | Party Position | County/City | Local.
# Party Position and County/City entries → "Local".
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Massachusetts  —  https://electionstats.state.ma.us/elections/search
# Dropdown loads via JavaScript; full office list requires browser interaction.
# Dropdown sections: Federal Offices | State Offices | Other Offices
# Stub populated from known MA office structure; regex fallback handles the rest.
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# South Carolina  —  https://electionhistory.scvotes.gov/search?t=table
# Civera v2 platform.  Dropdown sections: Federal | State | Local | Other.
# Local and Other entries → "Local".
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# New Mexico  —  https://electionstats.sos.nm.gov/search?t=table
# Civera v2 platform.  Dropdown sections: Federal | State | County | Local |
# Other.  County, Local, and Other entries all → "Local".
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# New York  —  https://results.elections.ny.gov/search?t=table
# Civera v2 platform.  Dropdown sections: Federal | State | Party Position |
# County | Local.  Party Position, County, and Local entries → "Local".
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Master registry
# ---------------------------------------------------------------------------
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

def lookup_office_level(office: str | None, state_key: str | None = None) -> str:
    """Return ``'Federal'``, ``'State'``, or ``'Local'`` for an office string.

    Checks the state-specific registry first (exact match, case-insensitive),
    then falls back to the regex-based ``classify_office_level``.

    Parameters
    ----------
    office : str | None
        Office name as it appears in the scraper output.
    state_key : str | None
        State key (e.g. ``'idaho'``, ``'colorado'``).  When provided,
        the state-specific registry is consulted first.

    Returns
    -------
    str
        ``'Federal'``, ``'State'``, or ``'Local'``.
    """
    from office_level_utils import classify_office_level

    if not office:
        return "Local"

    if state_key:
        key = state_key.strip().lower().replace(" ", "_")
        registry = STATE_OFFICE_REGISTRIES.get(key, {})
        # Case-insensitive lookup
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
