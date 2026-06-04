"""Parse the harrisvotes.com elections archive to discover elections and PDF links.

The archive (harrisvotes.com/Election-Results/Archives) is a DNN CMS site.
Each election is represented by an "EAL-Item" div containing:
  1. A cell with the election date text
  2. A cell with the election name text
  3. A cell with a link to the cumulative results PDF
  4. A cell with a link to the canvass results PDF

Pagination: page 1 is at /Election-Results/Archives; subsequent pages are at
/Election-Results/Archives/page/N.  Discovery stops when a page returns no items.

Inspection tip
--------------
If discovery returns 0 elections, save an archive page and inspect the structure::

    with open("/tmp/harris_archive.html", "w") as f:
        f.write(html)
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from lxml import html as lhtml

from .client import HoustonHttpClient, HARRIS_BASE_URL
from .models import HoustonElectionInfo

_thread_local = threading.local()


def _get_thread_client() -> HoustonHttpClient:
    """Return a per-thread HTTP client for discovery page fetches."""
    if not hasattr(_thread_local, "disc_client"):
        _thread_local.disc_client = HoustonHttpClient()
    return _thread_local.disc_client

_LOG = "[Houston discovery]"

# XPath: match class "EAL-Item" with word-boundary to avoid matching "EAL-Items".
_ITEM_XPATH = (
    '//div[contains(concat(" ", normalize-space(@class), " "), " EAL-Item ")]'
)

_MAX_PAGES = 25  # safety cap against infinite pagination loops


def _full_url(href: str) -> str:
    """Resolve a relative href to an absolute URL."""
    href = href.strip()
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return f"{HARRIS_BASE_URL}{href}"
    return f"{HARRIS_BASE_URL}/{href}"


def _parse_item(item_el) -> "HoustonElectionInfo | None":
    """Parse one EAL-Item element into a HoustonElectionInfo.

    Strategy:
    - Collect all PDF links (<a href="*.pdf">) and classify as cumulative/canvass.
    - Collect text from child elements that contain no PDF links (date and name cells).
    """
    # ── Collect PDF links ────────────────────────────────────────────────────
    cumulative_url: "str | None" = None
    canvass_url:    "str | None" = None
    unclassified_pdfs: list[str] = []

    for a in item_el.xpath('.//a[@href]'):
        href = (a.get("href") or "").strip()
        # Azure blob URLs carry query-string tokens after ".pdf" — strip them first.
        href_path = href.split("?")[0].lower()
        if not href_path.endswith(".pdf"):
            continue
        link_text = a.text_content().strip().lower()
        url_lower  = href_path   # classify by the path portion only
        full = href if href.startswith("http") else _full_url(href)

        if "cumulative" in link_text or "cumulative" in url_lower:
            cumulative_url = full
        elif "canvass" in link_text or "canvass" in url_lower:
            canvass_url = full
        else:
            unclassified_pdfs.append(full)

    # Fall back: assign unclassified PDFs positionally
    if cumulative_url is None and unclassified_pdfs:
        cumulative_url = unclassified_pdfs.pop(0)
    if canvass_url is None and unclassified_pdfs:
        canvass_url = unclassified_pdfs.pop(0)

    # Skip items with no PDFs at all (placeholder entries, future elections, etc.)
    if cumulative_url is None and canvass_url is None:
        return None

    # ── Collect date and name text ───────────────────────────────────────────
    # Walk immediate children; cells that contain no PDF links carry text (date/name).
    texts: list[str] = []
    for child in item_el:
        # Azure blob URLs: ".pdf" may appear before a query string — substring match suffices.
        if child.xpath('.//a[contains(translate(@href,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),".pdf")]'):
            continue  # skip link-bearing cells
        text = child.text_content().strip()
        if text:
            texts.append(text)

    # If we got no text from cells, fall back to all non-link text
    if not texts:
        link_set = {a.text_content().strip() for a in item_el.xpath('.//a')}
        texts = [
            t.strip() for t in item_el.itertext()
            if t.strip() and t.strip() not in link_set
        ]

    date_text = texts[0] if len(texts) > 0 else ""
    name_text = texts[1] if len(texts) > 1 else texts[0] if texts else ""

    try:
        return HoustonElectionInfo.from_archive_row(
            date_text=date_text,
            name_text=name_text,
            cumulative_url=cumulative_url,
            canvass_url=canvass_url,
        )
    except ValueError as exc:
        print(f"{_LOG} WARNING: skipping item ({exc})")
        return None


def parse_archive_page(html_str: str) -> "list[HoustonElectionInfo]":
    """Parse one archive page and return its election items."""
    doc = lhtml.fromstring(html_str)
    items = doc.xpath(_ITEM_XPATH)
    elections: list[HoustonElectionInfo] = []
    for item in items:
        info = _parse_item(item)
        if info is not None:
            elections.append(info)
    return elections


def discover_all_elections(
    client: HoustonHttpClient,
    max_workers: int = 5,
) -> "list[HoustonElectionInfo]":
    """Fetch all archive pages in parallel and return all discovered elections.

    All pages up to _MAX_PAGES are requested concurrently.  Results are then
    collected in page-number order; collection stops at the first page that
    returns no items (end of archive) or raises an error.
    """
    def _fetch_page(page_num: int):
        return page_num, _get_thread_client().get_archive_page(page_num)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_fetch_page, n): n
            for n in range(1, _MAX_PAGES + 1)
        }
        page_results: dict[int, "list[HoustonElectionInfo] | None"] = {}
        for future in as_completed(futures):
            page_num = futures[future]
            try:
                _, html = future.result()
                page_results[page_num] = parse_archive_page(html)
            except Exception as exc:
                if page_num == 1:
                    raise
                print(f"{_LOG} Page {page_num} fetch failed ({exc}).")
                page_results[page_num] = None

    all_elections: list[HoustonElectionInfo] = []
    for page_num in range(1, _MAX_PAGES + 1):
        page_elections = page_results.get(page_num)
        if not page_elections:
            print(f"{_LOG} Page {page_num} returned no elections — stopping.")
            break
        print(f"{_LOG}   Page {page_num}: {len(page_elections)} election(s).")
        all_elections.extend(page_elections)

    all_elections.sort(key=lambda e: (e.year, e.name))
    print(f"{_LOG} Total: {len(all_elections)} election(s) discovered.")
    return all_elections
