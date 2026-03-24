"""
Parse the Connecticut CTEMS home page to discover available elections.

The CTEMS site (ctemspublic.tgstg.net/#/home) is an AngularJS SPA.  After
the page loads, the election dropdown is populated by AngularJS with one
``<option>`` per election.  This module parses the rendered HTML to extract
those options into ``CtElectionInfo`` objects.

Typical usage (in combination with the client)::

    with CtPlaywrightClient() as client:
        html = client.get_landing_page()
    elections = parse_election_options(html)

Inspection tip
--------------
If no elections are found, inspect the rendered HTML::

    with open("/tmp/ct_landing.html", "w") as f:
        f.write(html)

Then open it in a browser or text editor to verify the ``<select>`` element
structure and update the selectors in this file if needed.
"""

from __future__ import annotations

from lxml import html as lhtml

from .models import CtElectionInfo

# Index of the election dropdown among all <select> elements on the page (0-based).
# The home page has only one <select> (the election picker) before an election is
# selected.  If the selectors change, update this constant.
_ELECTION_SELECT_INDEX = 0


def parse_election_options(html_str: str) -> list[CtElectionInfo]:
    """Parse the rendered CTEMS home page HTML and return all available elections.

    Reads all ``<option>`` elements from the election dropdown and converts
    each to a ``CtElectionInfo`` (skipping the blank placeholder option).

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the CTEMS home page after AngularJS has
        populated the election dropdown.

    Returns
    -------
    list[CtElectionInfo]
        Elections sorted ascending by year, then by name within a year.
        Returns an empty list if the dropdown cannot be found or has no
        options (likely means the selector assumptions need updating —
        inspect the raw HTML).
    """
    doc = lhtml.fromstring(html_str)

    # Find all <select> elements and take the first one (election dropdown).
    selects = doc.xpath("//select")
    if not selects:
        print(
            "[CT discovery] WARNING: No <select> element found on the page. "
            "The election dropdown may not have loaded — check that the HTML "
            "was captured after AngularJS rendered the page."
        )
        return []

    if _ELECTION_SELECT_INDEX >= len(selects):
        print(
            f"[CT discovery] WARNING: Expected a <select> at index "
            f"{_ELECTION_SELECT_INDEX} but only {len(selects)} were found."
        )
        return []

    election_select = selects[_ELECTION_SELECT_INDEX]
    options = election_select.xpath(".//option")

    elections: list[CtElectionInfo] = []
    skipped = 0

    for opt in options:
        value: str = (opt.get("value") or "").strip()
        name: str = " ".join((opt.text_content() or "").split()).strip()

        # Skip the blank placeholder option ("-- Select Election --")
        if not value or not name or name.startswith("--") or name.startswith("Select"):
            skipped += 1
            continue

        try:
            info = CtElectionInfo.from_option(name=name, option_value=value)
        except ValueError as exc:
            print(f"[CT discovery] WARNING: Skipping option {name!r}: {exc}")
            skipped += 1
            continue

        elections.append(info)

    if not elections:
        print(
            "[CT discovery] WARNING: Election dropdown was found but contained "
            f"no parseable options ({skipped} option(s) skipped). "
            "Inspect the rendered HTML to verify option text/value structure."
        )
        return []

    return sorted(elections, key=lambda e: (e.year, e.name))
