"""
Parse the Louisiana SOS Graphical landing page to discover available elections.

The page (https://voterportal.sos.la.gov/Graphical) is a JavaScript SPA.
After it loads, an election dropdown is populated with one <option> per
election date going back to 1982.  This module parses the rendered HTML to
extract those options into LaElectionInfo objects.

Typical usage (in combination with the client)::

    with LaPlaywrightClient() as client:
        html = client.get_landing_page()
    elections = parse_election_options(html)

Inspection tip
--------------
If no elections are found, run the inspect script::

    python -m Louisiana.inspect_landing

This saves the raw rendered HTML to /tmp/la_landing.html so you can inspect
the actual <select> / dropdown structure and update selectors here.
"""

from __future__ import annotations

from datetime import date
from lxml import html as lhtml

from .models import LaElectionInfo

# 0-based index of the election <select> among all <select> elements on the page.
# Confirmed: the election dropdown is the first <select> (id="ElectionId").
_ELECTION_SELECT_INDEX = 0


def parse_election_options(html_str: str) -> list[LaElectionInfo]:
    """Parse the rendered Louisiana SOS landing page and return all elections.

    Reads all ``<option>`` elements from the election dropdown and converts
    each to a ``LaElectionInfo`` (skipping blank placeholder options).

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the Louisiana SOS Graphical page after JavaScript
        has populated the election dropdown.

    Returns
    -------
    list[LaElectionInfo]
        Elections sorted descending by date (most recent first), matching the
        typical dropdown ordering on the SOS site.
        Returns an empty list if the dropdown cannot be found — inspect the raw
        HTML via inspect_landing.py to update selectors.
    """
    doc = lhtml.fromstring(html_str)
    selects = doc.xpath("//select")

    if not selects:
        print(
            "[LA discovery] WARNING: No <select> element found on the page. "
            "The election dropdown may not have loaded — confirm the HTML was "
            "captured after JavaScript rendered the page. "
            "Run inspect_landing.py to save the raw HTML for inspection."
        )
        return []

    if _ELECTION_SELECT_INDEX >= len(selects):
        print(
            f"[LA discovery] WARNING: Expected a <select> at index "
            f"{_ELECTION_SELECT_INDEX} but only {len(selects)} were found. "
            f"Update _ELECTION_SELECT_INDEX in discovery.py."
        )
        return []

    election_select = selects[_ELECTION_SELECT_INDEX]
    options = election_select.xpath(".//option")

    elections: list[LaElectionInfo] = []
    skipped = 0

    for opt in options:
        value: str = (opt.get("value") or "").strip()
        name: str = " ".join((opt.text_content() or "").split()).strip()

        # Skip blank placeholder options ("-- Select --", empty, etc.)
        if not value or not name or name.startswith("--") or name.lower().startswith("select"):
            skipped += 1
            continue

        try:
            info = LaElectionInfo.from_option(name=name, option_value=value)
        except ValueError as exc:
            print(f"[LA discovery] WARNING: Skipping option {name!r}: {exc}")
            skipped += 1
            continue

        elections.append(info)

    if not elections:
        print(
            "[LA discovery] WARNING: Election dropdown was found but contained "
            f"no parseable options ({skipped} option(s) skipped). "
            "Run inspect_landing.py to inspect the rendered HTML."
        )
        return []

    # Sort descending by date (most recent first).
    return sorted(
        elections,
        key=lambda e: e.election_date or date(e.year, 1, 1),
        reverse=True,
    )
