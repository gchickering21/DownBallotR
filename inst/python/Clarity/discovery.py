"""
Parse a SOS elections landing page to discover available elections.

Works with any Angular/PrimeNG SOS site that follows the same URL structure:

    {base_url}/{Slug}

where ``base_url`` is e.g. ``"https://results.sos.ga.gov/results/public/Georgia"``
or ``"https://electionresults.utah.gov/results/public/Utah"``.

This module is intentionally decoupled from the browser client so it can be
unit-tested against saved HTML fixtures.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse, urljoin

from lxml import html as lhtml

from .models import ClarityElectionInfo

# Regex to extract a 4-digit year from an election name or nearby heading.
_YEAR_RE = re.compile(r"\b(20\d{2}|19\d{2})\b")


def _extract_year_from_text(text: str) -> int | None:
    """Return the first 4-digit year found in *text*, or None."""
    m = _YEAR_RE.search(text)
    return int(m.group(1)) if m else None


def parse_election_links(html_str: str, base_url: str) -> list[ClarityElectionInfo]:
    """Parse the rendered landing page HTML and return all elections found.

    The function looks for anchor elements whose ``href`` attribute contains
    the election path prefix derived from ``base_url``.  It then tries to
    determine the election year from the link text and slug.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the SOS elections landing page.
    base_url : str
        The landing page URL, e.g.
        ``"https://results.sos.ga.gov/results/public/Georgia"``.
        Used to derive the server base (for resolving relative hrefs) and the
        path prefix (for the XPath anchor filter and slug extraction).

    Returns
    -------
    list[ClarityElectionInfo]
        Elections sorted ascending by year, then by name within a year.
        Returns an empty list if no matching links are found.
    """
    parsed = urlparse(base_url)
    server_base = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.rstrip("/")           # e.g. "/results/public/Georgia"
    path_prefix = path + "/"                 # e.g. "/results/public/Georgia/"
    state_name = path.split("/")[-1]         # e.g. "Georgia"

    slug_re = re.compile(rf"{re.escape(path)}/([^/?#]+)")

    doc = lhtml.fromstring(html_str)

    anchors = doc.xpath(
        f"//a[contains(@class,'col-12') and contains(@href,'{path_prefix}')]"
    )

    if not anchors:
        print(
            f"[{state_name} discovery] WARNING: No election links found on the landing page. "
            "The CSS/XPath selector may need updating — inspect the rendered HTML."
        )
        return []

    elections: list[ClarityElectionInfo] = []

    for anchor in anchors:
        href: str = anchor.get("href", "").strip()
        slug_match = slug_re.search(href)
        if not slug_match:
            continue

        slug = slug_match.group(1)
        full_url = urljoin(server_base, href)

        # Name: second child span (col-9 row-text).
        name_spans = anchor.xpath(".//span[contains(@class,'col-9')]")
        if name_spans:
            name = " ".join((name_spans[0].text_content() or "").split()).strip()
        else:
            name = " ".join((anchor.text_content() or "").split()).strip()

        if not name:
            name = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", slug)

        # Year: first child span holds the date string.
        date_spans = anchor.xpath(".//span[contains(@class,'col-2')]")
        year = None
        if date_spans:
            year = _extract_year_from_text(date_spans[0].text_content() or "")

        if year is None:
            year = _extract_year_from_text(name)
        if year is None:
            year = _extract_year_from_text(slug)

        if year is None:
            print(
                f"[{state_name} discovery] WARNING: Could not determine year for election "
                f"'{name}' (slug={slug!r}). Skipping."
            )
            continue

        elections.append(ClarityElectionInfo(name=name, year=year, slug=slug, url=full_url))

    # Deduplicate by URL.
    seen: set[str] = set()
    unique: list[ClarityElectionInfo] = []
    for e in elections:
        if e.url not in seen:
            seen.add(e.url)
            unique.append(e)

    return sorted(unique, key=lambda e: (e.year, e.name))
