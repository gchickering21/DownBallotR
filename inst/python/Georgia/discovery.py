"""
Parse the Georgia SOS elections landing page to discover available elections.

The landing page (``results.sos.ga.gov/results/public/Georgia``) is a
JavaScript-rendered app that, once loaded, contains a list of elections grouped
by year.  Each election links to a sub-page at:

    /results/public/Georgia/elections/{Slug}

This module is intentionally decoupled from the browser client so it can be
unit-tested against saved HTML fixtures.

NOTE on selectors
-----------------
The XPath / CSS selectors below are initial guesses based on the URL structure
the user described.  **Run the client interactively and inspect the rendered
HTML to verify / update these selectors before relying on this module.**

Quick inspection snippet::

    from Georgia.client import GaPlaywrightClient
    with GaPlaywrightClient(headless=False) as client:
        html = client.get_landing_page()
    with open("/tmp/ga_landing.html", "w") as f:
        f.write(html)
    # Then open /tmp/ga_landing.html in a browser or text editor.
"""

from __future__ import annotations

import re
from urllib.parse import urljoin

from lxml import html as lhtml

from .models import GaElectionInfo

# Base URL used to resolve relative hrefs found on the landing page.
_BASE_URL = "https://results.sos.ga.gov"

# Regex to extract a 4-digit year from an election name or nearby heading.
_YEAR_RE = re.compile(r"\b(20\d{2}|19\d{2})\b")

# Regex to extract the election slug from a /results/public/Georgia/{Slug} href.
_SLUG_RE = re.compile(r"/results/public/Georgia/([^/?#]+)")


def _extract_year_from_text(text: str) -> int | None:
    """Return the first 4-digit year found in *text*, or None."""
    m = _YEAR_RE.search(text)
    return int(m.group(1)) if m else None


def parse_election_links(html_str: str) -> list[GaElectionInfo]:
    """Parse the rendered landing page HTML and return all elections found.

    The function looks for anchor elements whose ``href`` attribute contains
    ``/elections/``.  It then tries to determine the election year from:

    1. A 4-digit year embedded in the link text itself (e.g. "November **2024**
       General Election").
    2. A nearby heading element (``<h1>``–``<h4>``) that contains a year —
       this covers layouts where elections are grouped under a year header.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the Georgia elections landing page.

    Returns
    -------
    list[GaElectionInfo]
        Elections sorted ascending by year, then by name within a year.
        Returns an empty list if no matching links are found (likely means the
        selector assumptions need updating — inspect the raw HTML).
    """
    doc = lhtml.fromstring(html_str)

    # The page renders one list-row link per election, using class
    # "col-12 col-sm-12 d-flex".  Each link contains:
    #   span.col-2.row-text  → date text  (e.g. "November 5, 2024")
    #   span.col-9.row-text  → election name (e.g. "November General Election")
    # Recent elections also have a "primary-card-body" card link, but the
    # list-row links are present for ALL years so we use only those.
    anchors = doc.xpath(
        "//a[contains(@class,'col-12') and contains(@href,'/results/public/Georgia/')]"
    )

    if not anchors:
        print(
            "[GA discovery] WARNING: No election links found on the landing page. "
            "The CSS/XPath selector may need updating — inspect the rendered HTML."
        )
        return []

    elections: list[GaElectionInfo] = []

    for anchor in anchors:
        href: str = anchor.get("href", "").strip()
        slug_match = _SLUG_RE.search(href)
        if not slug_match:
            continue

        slug = slug_match.group(1)
        full_url = urljoin(_BASE_URL, href)

        # Name: second child span (col-9 row-text).
        name_spans = anchor.xpath(".//span[contains(@class,'col-9')]")
        if name_spans:
            name = " ".join((name_spans[0].text_content() or "").split()).strip()
        else:
            name = " ".join((anchor.text_content() or "").split()).strip()

        if not name:
            # Convert CamelCase slug to a readable name as last resort.
            name = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", slug)

        # Year: first child span holds the date ("November 5, 2024").
        date_spans = anchor.xpath(".//span[contains(@class,'col-2')]")
        year = None
        if date_spans:
            year = _extract_year_from_text(date_spans[0].text_content() or "")

        # Fallback: scan the name text and then the slug.
        if year is None:
            year = _extract_year_from_text(name)
        if year is None:
            year = _extract_year_from_text(slug)

        if year is None:
            print(
                f"[GA discovery] WARNING: Could not determine year for election "
                f"'{name}' (slug={slug!r}). Skipping."
            )
            continue

        elections.append(
            GaElectionInfo(name=name, year=year, slug=slug, url=full_url)
        )

    # Deduplicate by URL (same election may appear multiple times in the DOM).
    seen: set[str] = set()
    unique: list[GaElectionInfo] = []
    for e in elections:
        if e.url not in seen:
            seen.add(e.url)
            unique.append(e)

    return sorted(unique, key=lambda e: (e.year, e.name))
