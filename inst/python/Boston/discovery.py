"""Parse the Boston.gov elections landing page to discover elections and PDF links.

The page (boston.gov/departments/elections/state-and-city-boston-election-results)
is a static Drupal site.  Year sections (2005–present) each contain one or more
election drawers.  Each drawer lists PDF result files grouped under <h5> headings.

Typical structure
-----------------
<div class="subnav-anchor" data-text="2025">
  <div class="paragraphs-item-drawers">
    <div class="paragraphs-item-drawer">
      <label class="dr-h">
        <div class="field-name-field-title">
          November 4, 2025: General Municipal Election
        </div>
      </label>
      <div class="dr-c">
        <h5>Mayoral Results:</h5>
        <ul>
          <li><a href="/sites/.../2025-11-04-Mayor.pdf">Mayor</a></li>
        </ul>
        <h5>City Council Results:</h5>
        <ul>
          <li><a href="...">City Council At-Large</a></li>
          <li><a href="...">City Council District 1</a></li>
        </ul>
      </div>
    </div>
  </div>
</div>

Inspection tip
--------------
If discovery returns 0 elections, save the HTML and check the structure::

    with open("/tmp/boston_landing.html", "w") as f:
        f.write(html)
"""

from __future__ import annotations

import re

from lxml import html as lhtml

from .client import BOSTON_BASE_URL
from .models import BostonElectionInfo, BostonResultLink

# Minimum year the page is known to cover.
_MIN_YEAR = 2005

# Drawer class names — these are the Drupal paragraph items that hold each election.
_DRAWER_XPATH = '//div[contains(@class,"paragraphs-item-drawer")]'

# The election title lives in a Drupal field inside the drawer label.
_TITLE_XPATH = './/div[contains(@class,"field-name-field-title")]'

# The drawer content container holds <h5> group headings and <a> PDF links.
_CONTENT_XPATH = './/div[contains(@class,"dr-c")]'

# A simple guard: skip drawers whose title doesn't look like an election.
_ELECTION_TITLE_RE = re.compile(
    r"\b(election|primary|preliminary|results|municipal|state|special)\b",
    re.IGNORECASE,
)


def _parse_result_links(content_el) -> list[BostonResultLink]:
    """Walk the drawer content element and collect all PDF links with their group heading.

    We traverse the element tree in document order.  Each time we encounter
    an <h5>, we update ``current_group`` so that subsequent <a> tags are
    labelled with that section heading.
    """
    links: list[BostonResultLink] = []
    current_group = ""

    for el in content_el.iter():
        tag = el.tag if isinstance(el.tag, str) else ""

        if tag == "h5":
            current_group = el.text_content().strip().rstrip(":").strip()
            continue

        if tag == "a":
            href = (el.get("href") or "").strip()
            if not href.lower().endswith(".pdf"):
                continue
            link_text = el.text_content().strip()
            if not link_text:
                continue
            try:
                link = BostonResultLink.from_link(
                    link_text=link_text,
                    group_label=current_group,
                    href=href,
                    base_url=BOSTON_BASE_URL,
                )
                links.append(link)
            except Exception as exc:
                print(f"[Boston discovery] WARNING: skipping link {href!r}: {exc}")

    return links


def parse_landing_page(html_str: str) -> list[BostonElectionInfo]:
    """Parse the Boston elections landing page and return all discovered elections.

    Finds every election drawer on the page, extracts the election date/type
    from the drawer title, and collects all PDF result links inside the drawer.

    The year is parsed from the election title string (not the page section
    anchor) so that elections are correctly labelled even if a section anchor
    and an election title year ever diverge.

    Parameters
    ----------
    html_str : str
        Raw HTML of the Boston elections landing page.

    Returns
    -------
    list[BostonElectionInfo]
        Elections sorted ascending by year, then by name within a year.
        Returns an empty list if no drawers are found (structure may have changed).
    """
    doc = lhtml.fromstring(html_str)
    drawers = doc.xpath(_DRAWER_XPATH)

    if not drawers:
        print(
            "[Boston discovery] WARNING: No election drawers found on the page. "
            "The site structure may have changed — check that "
            f"'{_DRAWER_XPATH}' still matches the rendered HTML."
        )
        return []

    elections: list[BostonElectionInfo] = []
    skipped = 0

    for drawer in drawers:
        # ── Extract election title ──────────────────────────────────────────
        title_els = drawer.xpath(_TITLE_XPATH)
        if not title_els:
            skipped += 1
            continue
        title = " ".join(title_els[0].text_content().split()).strip()

        if not title or not _ELECTION_TITLE_RE.search(title):
            skipped += 1
            continue

        # ── Extract PDF links from the drawer content ───────────────────────
        content_els = drawer.xpath(_CONTENT_XPATH)
        if not content_els:
            skipped += 1
            continue

        result_links = _parse_result_links(content_els[0])
        if not result_links:
            # Drawer with no PDFs yet (e.g. future election placeholder).
            skipped += 1
            continue

        # ── Build the election info object ──────────────────────────────────
        try:
            info = BostonElectionInfo.from_drawer(title, result_links)
        except ValueError as exc:
            print(f"[Boston discovery] WARNING: skipping drawer {title!r}: {exc}")
            skipped += 1
            continue

        if info.year < _MIN_YEAR:
            skipped += 1
            continue

        elections.append(info)

    if not elections:
        print(
            "[Boston discovery] WARNING: Discovery found drawers but parsed 0 elections. "
            f"({skipped} drawer(s) skipped — check title format and PDF link structure.)"
        )
        return []

    result = sorted(elections, key=lambda e: (e.year, e.name))
    print(f"[Boston discovery] Found {len(result)} election(s) ({skipped} skipped).")
    return result
