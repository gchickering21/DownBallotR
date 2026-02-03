from __future__ import annotations

from dataclasses import asdict
import re
from typing import Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

from lxml import html
import pandas as pd

from va_client import VaHttpClient
from va_models import VaElectionSearchRow

# Rows look like: <tr id="election-id-165954" ...>
_ROW_ID_RE = re.compile(r"^election-id-(\d+)$")


def _clean_ws(s: str) -> str:
    """Collapse all whitespace to single spaces and trim."""
    return re.sub(r"\s+", " ", s or "").strip()


def _safe_text(node) -> str:
    """Best-effort text_content() with whitespace cleanup."""
    try:
        return _clean_ws(node.text_content())
    except Exception:
        return ""


def _extract_candidates_nested_text(candidates_td) -> Optional[str]:
    """
    The 'Candidates »' UI on the site is a JS toggle; in the server HTML the
    candidates cell looks like:

      <td class="candidates_container_cell" data-order="...">
        <div class="candidates_preview expand_toggle">...</div>
        <table class="candidates" style="display:none"> ... </table>
      </td>

    There is often *no* <a href="...">Candidates</a> link to extract.
    So we extract a reasonable summary from the cell itself and (if present)
    the hidden nested table.
    """
    if candidates_td is None:
        return None

    # Prefer nested table text if it exists
    nested_tables = candidates_td.xpath(".//table[contains(concat(' ', normalize-space(@class), ' '), ' candidates ')]")
    if nested_tables:
        txt = _safe_text(nested_tables[0])
        return txt or None

    # Fall back to all text in the TD
    txt = _safe_text(candidates_td)
    return txt or None


def _extract_candidates_url(candidates_td, client: VaHttpClient) -> Optional[str]:
    if candidates_td is None:
        return None

    hrefs = candidates_td.xpath(".//a[contains(normalize-space(.), 'Candidates')]/@href")
    if not hrefs:
        return None

    href = (hrefs[0] or "").strip()
    if not href or href.lower().startswith("javascript:"):
        return None

    return urljoin(client.BASE, href)



def parse_search_results(page_html: str, client: VaHttpClient) -> List[VaElectionSearchRow]:
    """
    Parse the VA elections search results table into structured rows.

    Expects the server-rendered table:
      <table id="search_results_table"> ... <tr id="election-id-123"> ... </tr>

    Columns (as observed):
      Year | Office | District | Stage | Candidates
    """
    doc = html.fromstring(page_html)

    trs = doc.xpath("//table[@id='search_results_table']//tr[starts-with(@id, 'election-id-')]")

    out: List[VaElectionSearchRow] = []
    for tr in trs:
        tr_id = tr.get("id") or ""
        m = _ROW_ID_RE.match(tr_id)
        if not m:
            continue

        election_id = int(m.group(1))

        tds = tr.xpath("./td")
        if len(tds) < 4:
            # defensive: should be 5, but don't crash if markup changes
            continue

        year_txt = _safe_text(tds[0])
        office = _safe_text(tds[1])
        district = _safe_text(tds[2])
        stage = _safe_text(tds[3])

        try:
            year = int(year_txt)
        except ValueError:
            # Sometimes tables have odd rows; skip
            continue

        candidates_td = tds[4] if len(tds) >= 5 else None
        candidates_summary = _extract_candidates_nested_text(candidates_td)
        candidates_url = _extract_candidates_url(candidates_td, client)

        out.append(
            VaElectionSearchRow(
                election_id=election_id,
                year=year,
                office=office,
                district=district,
                stage=stage,
                candidates_summary=candidates_summary,
                candidates_url=candidates_url,
            )
        )

    return out


def fetch_search_results(
    client: VaHttpClient,
    year_from: int = 1789,
    year_to: int = 2025,
    page: int = 1,
) -> List[VaElectionSearchRow]:
    """Fetch and parse one search results page."""
    url = client.build_search_url(year_from=year_from, year_to=year_to, page=page)
    page_html = client.get_html(url)
    return parse_search_results(page_html, client)


def fetch_search_results_dicts(
    client: VaHttpClient,
    year_from: int = 1789,
    year_to: int = 2025,
    page: int = 1,
) -> List[dict]:
    """
    Convenience wrapper returning dicts (including computed detail_url).
    """
    rows = fetch_search_results(client, year_from=year_from, year_to=year_to, page=page)
    return [asdict(r) | {"detail_url": r.detail_url} for r in rows]


def iter_search_results(
    client: VaHttpClient,
    year_from: int = 1789,
    year_to: int = 2025,
    start_page: int = 1,
    max_pages: int = 200,
) -> Iterable[VaElectionSearchRow]:
    """
    Iterate search results across pages until we hit an empty page
    or max_pages.

    Note: the site often caps the number of results (e.g., "first 1,200 results").
    Paging beyond that may repeat or stop returning new rows.
    """
    seen_ids: set[int] = set()
    page = start_page

    for _ in range(max_pages):
        rows = fetch_search_results(client, year_from=year_from, year_to=year_to, page=page)
        if not rows:
            break

        # Stop if we’re no longer seeing anything new (common when capped)
        new_rows = [r for r in rows if r.election_id not in seen_ids]
        if not new_rows:
            break

        for r in new_rows:
            seen_ids.add(r.election_id)
            yield r

        page += 1


def fetch_all_search_results(
    client: VaHttpClient,
    year_from: int = 1789,
    year_to: int = 2025,
    start_page: int = 1,
    max_pages: int = 200,
) -> List[VaElectionSearchRow]:
    """Materialize iter_search_results() into a list."""
    return list(
        iter_search_results(
            client,
            year_from=year_from,
            year_to=year_to,
            start_page=start_page,
            max_pages=max_pages,
        )
    )

def rows_to_dataframe(rows: list[VaElectionSearchRow]) -> pd.DataFrame:
    """
    Convert parsed VaElectionSearchRow objects into a pandas DataFrame.
    Includes computed detail_url.
    """
    records = [asdict(r) | {"detail_url": r.detail_url} for r in rows]
    return pd.DataFrame.from_records(records)
