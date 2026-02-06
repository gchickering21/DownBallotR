import pandas as pd

import re

_CONTEST_RE = re.compile(
    r"""
    ^
    # Optional municipal prefix
    (?:(?P<unit>CITY|TOWN|VILLAGE)\s+OF\s+)?

    # Jurisdiction/entity name (allow periods for "ST.")
    (?P<jurisdiction>[A-Z0-9\s&\-\.\u2013\u2014]+?)\s+

    # Optional extra municipal token before office (e.g., "TOWN COUNCIL")
    (?:(?:CITY|TOWN|VILLAGE)\s+)?

    # Office phrase
    (?P<office>
        MAYOR|

        # Council variants
        COUNCIL\s+MEMBER(?:S)?|
        COUNCILMAN|
        COUNCILMEN|
        COUNCIL(?:\s+MEMBER(?:S)?)?|
        (?:CITY|TOWN|VILLAGE)\s+COUNCIL(?:\s+MEMBER(?:S)?)?|
        (?:CITY|TOWN|VILLAGE)\s+COUNCILMEN|

        # Boards
        BOARD|
        BOARD\s+MEMBER(?:S)?|
        BOARD\s+OF\s+DIRECTORS|
        BOARD\s+OF\s+EDUCATION|
        BOARD\s+OF\s+TRUSTEES|
        BOARD\s+OF\s+ALDERMEN|
        BOARD\s+OF\s+COMMISSIONERS|

        # Commissioners / trustees / alder
        COMMISSIONER(?:S)?|
        TRUSTEE(?:S)?|
        ALDERMAN|
        ALDERMEN|
        ALDERMAN(?:S)?

        
    )

    # Optional trailing qualifier(s)
    (?:\s+
        (?P<district>
            # AT-LARGE optionally followed by another seat label (e.g., "AT-LARGE 1ST WARD")
            AT[-\s]?LARGE
            (?:\s+
                (?:
                    \d+(?:ST|ND|RD|TH)\s+WARD|
                    (?:FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH|SEVENTH|EIGHTH|NINTH|TENTH|ELEVENTH|TWELFTH)\s+WARD|
                    WARD\s+(?:\d+|[A-Z]|[IVXLCDM]+)|
                    (?:NORTH|SOUTH|EAST|WEST)\s+WARD
                )
            )?
            |

            # District numeric or letter
            DISTRICT\s+\d+|
            DISTRICT\s+[A-Z]|

            # Ward numeric, letter, or Roman
            WARD\s+(?:\d+|[A-Z]|[IVXLCDM]+)|

            # Ordinal ward numeric (1ST WARD) or spelled (THIRD WARD)
            \d+(?:ST|ND|RD|TH)\s+WARD|
            (?:FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH|SEVENTH|EIGHTH|NINTH|TENTH|ELEVENTH|TWELFTH)\s+WARD|

            # Directional wards / ends
            (?:NORTH|SOUTH|EAST|WEST)\s+WARD|
            (?:NORTH|SOUTH|EAST|WEST)\s+END|

            # Named district like "WOODVILLE DISTRICT" (allow 1–2 words before DISTRICT)
            [A-Z0-9&\.\-]+(?:\s+[A-Z0-9&\.\-]+)?\s+DISTRICT
        )
    )?

    # Optional trailing party in parentheses
    (?:\s*\([A-Z]+\))?
    \s*$
    """,
    re.VERBOSE,
)

_CONTEST_RE2 = re.compile(
    r"""
    ^
    # Optional state prefix (kept separate so it doesn't steal "NC" from the office)
    (?:(?P<state_prefix>NC)\s+)?


    # Optional jurisdiction/entity prefix (counties, school systems, districts, etc.)
    (?:
        (?P<jurisdiction>
            [A-Z0-9&\.\-]+(?:\s+[A-Z0-9&\.\-]+)*\s+COUNTY(?:\s+PUBLIC\s+SCHOOLS)?|
            [A-Z0-9&\.\-]+(?:\s+[A-Z0-9&\.\-]+)*\s+CITY\s+SCHOOLS|
            [A-Z0-9&\.\-]+(?:\s+[A-Z0-9&\.\-]+)*\s+SANITARY\s+DISTRICT|
            [A-Z0-9&\.\-]+(?:\s+[A-Z0-9&\.\-]+)*\s+SOIL\s+AND\s+WATER\s+CONSERVATION\s+DISTRICT
        )
        \s+
    )?

    # Office
    (?P<office>
        # Federal / national
        US\s+HOUSE\s+OF\s+REPRESENTATIVES|
        US\s+SENATE|
        US\s+PRESIDENT|

        # Ballot type
        PRESIDENTIAL\s+PREFERENCE|

        # NC legislative (allow optional "NC" here too)
        (?:NC\s+)?HOUSE\s+OF\s+REPRESENTATIVES|
        (?:NC\s+)?STATE\s+SENATE|

        # NC statewide executive (allow optional "NC")
        (?:NC\s+)?ATTORNEY\s+GENERAL|
        (?:NC\s+)?AUDITOR|
        (?:NC\s+)?COMMISSIONER\s+OF\s+AGRICULTURE|
        (?:NC\s+)?COMMISSIONER\s+OF\s+LABOR|
        (?:NC\s+)?COMMISSIONER\s+OF\s+INSURANCE|
        (?:NC\s+)?GOVERNOR|
        (?:NC\s+)?LIEUTENANT\s+GOVERNOR|
        (?:NC\s+)?SECRETARY\s+OF\s+STATE|
        (?:NC\s+)?TREASURER|
        (?:NC\s+)?SUPERINTENDENT\s+OF\s+PUBLIC\s+INSTRUCTION|


        # Courts (allow optional "NC")
        (?:NC\s+)?SUPREME\s+COURT\s+CHIEF\s+JUSTICE|
        (?:NC\s+)?COURT\s+OF\s+APPEALS\s+JUDGE|
        (?:NC\s+)?SUPERIOR\s+COURT\s+JUDGE|
        (?:NC\s+)?DISTRICT\s+COURT\s+JUDGE|
        (?:NC\s+)?SUPREME\s+COURT\s+ASSOCIATE\s+JUSTICE|


        # Prosecutor
        DISTRICT\s+ATTORNEY|

        # County / local offices
        SHERIFF|
        CLERK\s+OF\s+SUPERIOR\s+COURT|
        REGISTER\s+OF\s+DEEDS|

        # Boards / education
        BOARD\s+OF\s+COMMISSIONERS|
        BOARD\s+OF\s+EDUCATION(?:\s+MEMBER)?|

        # Conservation
        SUPERVISOR
    )

    # Qualifiers (0+)
    (?P<qualifiers>
        (?:
            \s+
            (?:
                # DIST / DISTRICT / DISTRICT spelled out; allow leading zeros + optional letter (024, 27B)
                DIST(?:RICT)?\s+(?:\d{1,3}[A-Z]?|[IVXLCDM]+)|
                DISTRICT\s+(?:\d{1,3}[A-Z]?|[IVXLCDM]+)|

                # SEAT with leading zeros
                SEAT\s+\d{1,3}|

                AT[-\s]?LARGE|
                COUNTY[-\s]?WIDE|
                AREA\s+(?:[IVXLCDM]+|[A-Z]|\d+)|
                CHAIRMAN|
                MEMBER
            )
        )*
    )

    # Optional trailing party
    (?:\s*\((?P<party>[A-Z]+)\))?
    \s*$
    """,
    re.VERBOSE,
)





def extract_jurisdiction_office_and_district(contest_name: str):
    if not isinstance(contest_name, str):
        return None, None, None

    # --- normalize input ---
    s = contest_name.upper().strip()
    s = re.sub(r"^[,\s]+", "", s)
    s = re.sub(r"[,\s]+$", "", s)
    s = re.sub(r"\s+", " ", s)

    for rx in (_CONTEST_RE, _CONTEST_RE2):
        m = rx.match(s)
        if not m:
            continue

        gd = m.groupdict()

        office_raw = (gd.get("office") or "").strip()
        jurisdiction_raw = gd.get("jurisdiction")
        state_prefix = gd.get("state_prefix")

        # -------------------------
        # Jurisdiction
        # -------------------------
        if jurisdiction_raw:
            jurisdiction = jurisdiction_raw.title()
        elif office_raw.startswith("US "):
            jurisdiction = "US"
        elif state_prefix == "NC" or office_raw.startswith("NC "):
            jurisdiction = "NC"
        else:
            jurisdiction = None

        # -------------------------
        # Office (strip US / NC prefixes)
        # -------------------------
        if office_raw.startswith("US "):
            office_raw = office_raw[3:]
        elif office_raw.startswith("NC "):
            office_raw = office_raw[3:]

        office = office_raw.title().replace("  ", " ").strip() if office_raw else None

        # -------------------------
        # District / qualifiers
        # -------------------------
        district_raw = gd.get("district") or gd.get("qualifiers")
        if district_raw:
            district = (
                re.sub(r"\s+", " ", district_raw.replace("-", " "))
                .title()
                .strip()
            )
        else:
            district = None

        return jurisdiction, office, district

    return None, None, None






