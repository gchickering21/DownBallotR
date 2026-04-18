from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Tuple, Dict

from lxml import html
import pandas as pd

from .electionStats_county_search import (
    LEADING_IGNORE_HEADERS,
    _extract_candidate_names_from_thead,
    _count_trailing_ignored_columns_from_thead,
    _extract_vote_text_from_td,
    _extract_county_name_from_row,
    _get_all_header_labels,
    parse_county_votes_from_detail_html,
    _build_candidate_id_map_from_state_df,
    _PRECINCT_COLS,
)

from http_utils import fetch_with_retry
from text_utils import parse_int as _parse_int


# =============================================================================
# Precinct HTML helpers
# =============================================================================

def _build_locality_id_map(table) -> Dict[str, str]:
    """Map {locality/division id string → county name} from the county rows."""
    mapping: Dict[str, str] = {}
    for tr in table.xpath(".//tbody/tr[starts-with(@id,'locality-id-')]"):
        loc_id = tr.get("id", "").replace("locality-id-", "").strip()
        name = _extract_county_name_from_row(tr)
        if loc_id and name:
            mapping[loc_id] = name
    for tr in table.xpath(".//tbody/tr[starts-with(@id,'division-id-')]"):
        div_id = tr.get("id", "").replace("division-id-", "").strip()
        name = _extract_county_name_from_row(tr)
        if div_id and name:
            mapping[div_id] = name
    return mapping


def _iter_precinct_row_pairs(table) -> List[Tuple]:
    """
    Return list of (tr, style) for all precinct-level rows in the table.

    ``style`` is one of:
      ``'precinct_id'``    — rows have ``id="precinct-id-*"`` and
                             ``class="precinct-for-{parent_id}"``
                             (NH / MA / VT style)
      ``'child_division'`` — rows have ``class`` containing
                             ``"child-division-of-{parent_id}"``
                             (Idaho / CO / VA style)
    """
    rows = table.xpath(".//tbody/tr[starts-with(@id,'precinct-id-')]")
    if rows:
        return [(tr, "precinct_id") for tr in rows]
    rows = table.xpath(".//tbody/tr[contains(@class,'child-division-of-')]")
    if rows:
        return [(tr, "child_division") for tr in rows]
    return []


def _parent_county_for_precinct(
    tr, locality_id_map: Dict[str, str], style: str
) -> Optional[str]:
    """Resolve the parent county name for a precinct row via CSS class."""
    cls = tr.get("class") or ""
    prefix = "precinct-for-" if style == "precinct_id" else "child-division-of-"
    for part in cls.split():
        if part.startswith(prefix):
            parent_id = part[len(prefix):]
            return locality_id_map.get(parent_id)
    return None


def _extract_precinct_label(
    tr, all_headers: List[str], style: str
) -> Optional[str]:
    """
    Build a human-readable precinct name from a precinct row.

    ``child_division`` style (ID/CO/VA):
      The first <td> holds the precinct name directly.

    ``precinct_id`` style (NH/MA/VT):
      Ward and Pct columns are combined into e.g. ``"Ward 1 Pct 3"``
      or just ``"3"`` when no Ward is present.
    """
    tds = tr.xpath("./td")
    if not tds:
        return None

    if style == "child_division":
        # For Idaho/CO style the name lives in <th scope="row"><a/span class="label">,
        # same structure as county rows.  Mirror _extract_county_name_from_row here.
        txt = tr.xpath(".//a[contains(@class,'label')]/text()")
        if txt:
            return txt[0].strip() or None
        txt = tr.xpath(".//span[contains(@class,'label')]/text()")
        if txt:
            return txt[0].strip() or None
        # Fallback: first td text (for child_division states where name IS in a <td>)
        text = " ".join(tds[0].xpath(".//text()")).strip()
        return text or None

    # precinct_id style — use Ward / Pct column indices from the header
    ward_idx = next((i for i, h in enumerate(all_headers) if h == "Ward"), None)
    pct_idx  = next(
        (i for i, h in enumerate(all_headers) if h in ("Pct", "Precinct")), None
    )

    parts: List[str] = []
    if ward_idx is not None and ward_idx < len(tds):
        ward = " ".join(tds[ward_idx].xpath(".//text()")).strip()
        if ward and ward not in ("-", "—"):
            parts.append(f"Ward {ward}")
    if pct_idx is not None and pct_idx < len(tds):
        pct = " ".join(tds[pct_idx].xpath(".//text()")).strip()
        if pct and pct not in ("-", "—"):
            parts.append(pct)

    if parts:
        return " ".join(parts)

    # fallback: first non-empty td
    for td in tds:
        text = " ".join(td.xpath(".//text()")).strip()
        if text and text not in ("-", "—"):
            return text
    return None


def _extract_precinct_vote_tds(
    tr,
    all_headers: List[str],
    candidate_count: int,
    trailing_ignore_n: int,
    style: str,
) -> Optional[List]:
    """
    Slice the <td>s that contain candidate votes from a precinct row.

    ``child_division``: first td is the precinct name; votes start at td[1].
    ``precinct_id``:    leading locality columns (City/Town, Ward, Pct …)
                        are counted from the header and skipped.
    """
    tds = tr.xpath("./td")
    if not tds:
        return None

    if style == "child_division":
        # When the locality name lives in a <th scope="row"> (Idaho/CO style),
        # all ./td elements are vote data — don't skip the first one.
        if tr.xpath("./th"):
            data_tds = tds
        else:
            data_tds = tds[1:]
    else:
        leading_count = sum(1 for h in all_headers if h in LEADING_IGNORE_HEADERS)
        data_tds = tds[leading_count:]

    if trailing_ignore_n > 0 and len(data_tds) >= trailing_ignore_n:
        data_tds = data_tds[:-trailing_ignore_n]

    if len(data_tds) < candidate_count:
        return None

    return data_tds[-candidate_count:]


# =============================================================================
# Precinct HTML parsing
# =============================================================================

def parse_precinct_votes_from_detail_html(
    detail_html: str,
    election_id: int,
    state: str,
    candidate_id_map: Optional[Dict[str, int]] = None,
) -> pd.DataFrame:
    """
    Parse precinct-level vote totals from an election detail HTML page.

    Works for both layout styles:
      * ``precinct_id``    — NH / MA / VT (rows ``id="precinct-id-*"``)
      * ``child_division`` — ID / CO / VA (rows with ``class="child-division-of-*"``)

    Returns an empty DataFrame (with correct columns) if the page has no
    precinct rows.

    Parameters
    ----------
    detail_html : str
        Raw HTML of the election detail page.
    election_id : int
        Election identifier to attach to every row.
    state : str
        State key to attach to every row.
    candidate_id_map : dict | None
        ``{candidate_name: candidate_id}``.  Built positionally if not provided.

    Returns
    -------
    pd.DataFrame
        Columns: state, election_id, candidate_id, county, precinct, candidate, votes
    """
    EMPTY = pd.DataFrame(columns=_PRECINCT_COLS)

    doc = html.fromstring(detail_html)

    tables = doc.xpath(
        "//table[.//th["
        "normalize-space()='County/City' "
        "or normalize-space()='City/Town' "
        "or normalize-space()='County'"
        "]] | //table[@id='precinct_data']"
    )
    if not tables:
        return EMPTY

    table = tables[0]

    precinct_pairs = _iter_precinct_row_pairs(table)
    if not precinct_pairs:
        return EMPTY

    candidate_names = _extract_candidate_names_from_thead(table)
    if not candidate_names:
        return EMPTY

    trailing_ignore_n = _count_trailing_ignored_columns_from_thead(table)
    all_headers       = _get_all_header_labels(table)

    if candidate_id_map is None:
        candidate_id_map = {name: i + 1 for i, name in enumerate(candidate_names)}

    locality_id_map = _build_locality_id_map(table)

    records: List[Dict] = []

    for tr, style in precinct_pairs:
        county = _parent_county_for_precinct(tr, locality_id_map, style)

        # fallback: read county from City/Town / County header column
        if not county:
            for hdr in ("City/Town", "County/City", "County"):
                idx = next((i for i, h in enumerate(all_headers) if h == hdr), None)
                if idx is not None:
                    tds = tr.xpath("./td")
                    if idx < len(tds):
                        county = " ".join(tds[idx].xpath(".//text()")).strip() or None
                    break

        precinct = _extract_precinct_label(tr, all_headers, style)

        if not county or not precinct:
            continue
        if county.lower() in ("totals", "total"):
            continue
        if precinct.lower() in ("totals", "total"):
            continue

        vote_tds = _extract_precinct_vote_tds(
            tr, all_headers, len(candidate_names), trailing_ignore_n, style
        )
        if vote_tds is None:
            continue

        for cand_name, td in zip(candidate_names, vote_tds):
            votes = _parse_int(_extract_vote_text_from_td(td))
            if votes is None:
                continue
            cand_id = candidate_id_map.get(cand_name)
            if cand_id is None:
                continue
            records.append({
                "state":        state,
                "election_id":  election_id,
                "candidate_id": cand_id,
                "county":       county,
                "precinct":     precinct,
                "candidate":    cand_name,
                "votes":        votes,
            })

    return pd.DataFrame(records) if records else EMPTY


# =============================================================================
# Combined county + precinct builders (single fetch per detail page)
# =============================================================================

def _fetch_and_parse_county_and_precinct(
    st: Optional[str],
    election_id: int,
    url: str,
    candidate_id_map: Optional[Dict[str, int]],
    client_factory,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Worker: fetch one detail page and return ``(county_df, precinct_df)``.
    Creates its own client via ``client_factory()`` for thread-safety.
    """
    try:
        client = client_factory()
        detail_html = fetch_with_retry(client.get_html, url)

        county_df = parse_county_votes_from_detail_html(
            detail_html, election_id=election_id, state=st
        )
        if candidate_id_map and "candidate" in county_df.columns:
            county_df["candidate_id"] = county_df["candidate"].map(candidate_id_map)

        precinct_df = parse_precinct_votes_from_detail_html(
            detail_html,
            election_id=election_id,
            state=st,
            candidate_id_map=candidate_id_map,
        )
        return county_df, precinct_df

    except Exception as e:
        print(
            f"[ERROR] Failed to parse detail "
            f"(state={st}, election_id={election_id})\n"
            f"URL: {url}\n"
            f"Error: {type(e).__name__}: {e}\n"
        )
        return None, None


def build_county_and_precinct_dataframe_sequential(
    state_df: pd.DataFrame,
    client,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Sequential (single-thread) version that fetches each detail page once
    and returns ``(county_df, precinct_df)``.

    Use this instead of the parallel version when the client is not thread-safe
    (e.g. Playwright's sync API, which requires all calls on the same greenlet).

    Parameters
    ----------
    state_df : pd.DataFrame
        Must include ``['election_id', 'url']``.
        ``'state'`` and ``'candidate_id'`` columns are used when present.
    client : object
        Must implement ``client.get_html(url) -> str``.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        ``(county_df, precinct_df)`` — either may be empty.
    """
    required = {"election_id", "url"}
    missing = required - set(state_df.columns)
    if missing:
        raise ValueError(f"state_df missing columns: {sorted(missing)}")

    has_state = "state" in state_df.columns
    county_cols = (
        ["state", "election_id", "candidate_id", "county_or_city", "candidate", "votes"]
        if has_state
        else ["election_id", "candidate_id", "county_or_city", "candidate", "votes"]
    )

    jobs: List[Tuple[Optional[str], int, str]] = []
    for _, r in state_df.iterrows():
        url = str(r["url"]).strip()
        if not url:
            continue
        st = str(r["state"]) if has_state else None
        jobs.append((st, int(r["election_id"]), url))

    if not jobs:
        return pd.DataFrame(columns=county_cols), pd.DataFrame(columns=_PRECINCT_COLS)

    candidate_id_map = (
        _build_candidate_id_map_from_state_df(state_df)
        if "candidate_id" in state_df.columns
        else None
    )

    county_frames: List[pd.DataFrame] = []
    precinct_frames: List[pd.DataFrame] = []

    for st, election_id, url in jobs:
        try:
            detail_html = fetch_with_retry(client.get_html, url)

            c_df = parse_county_votes_from_detail_html(
                detail_html, election_id=election_id, state=st
            )
            if candidate_id_map and "candidate" in c_df.columns:
                c_df["candidate_id"] = c_df["candidate"].map(candidate_id_map)

            p_df = parse_precinct_votes_from_detail_html(
                detail_html,
                election_id=election_id,
                state=st,
                candidate_id_map=candidate_id_map,
            )

            if not c_df.empty:
                county_frames.append(c_df)
            if not p_df.empty:
                precinct_frames.append(p_df)

        except Exception as e:
            print(
                f"[ERROR] Failed to parse detail "
                f"(state={st}, election_id={election_id})\n"
                f"URL: {url}\n"
                f"Error: {type(e).__name__}: {e}\n"
            )

    c_out = pd.concat(county_frames, ignore_index=True) if county_frames else pd.DataFrame(columns=county_cols)
    p_out = pd.concat(precinct_frames, ignore_index=True) if precinct_frames else pd.DataFrame(columns=_PRECINCT_COLS)
    return c_out, p_out


def build_county_and_precinct_dataframe_parallel(
    state_df: pd.DataFrame,
    client_factory,
    max_workers: int = 6,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parallel version that fetches each detail page **once** and returns both
    ``(county_df, precinct_df)``.

    Parameters
    ----------
    state_df : pd.DataFrame
        Must include ``['election_id', 'url']``.
        ``'state'`` and ``'candidate_id'`` columns are used when present.
    client_factory : callable
        Returns a new client instance per thread.
    max_workers : int
        Thread pool size.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        ``(county_df, precinct_df)`` — either may be empty.
    """
    required = {"election_id", "url"}
    missing = required - set(state_df.columns)
    if missing:
        raise ValueError(f"state_df missing columns: {sorted(missing)}")

    has_state = "state" in state_df.columns
    county_cols = (
        ["state", "election_id", "candidate_id", "county_or_city", "candidate", "votes"]
        if has_state
        else ["election_id", "candidate_id", "county_or_city", "candidate", "votes"]
    )

    jobs: List[Tuple[Optional[str], int, str]] = []
    for _, r in state_df.iterrows():
        url = str(r["url"]).strip()
        if not url:
            continue
        st = str(r["state"]) if has_state else None
        jobs.append((st, int(r["election_id"]), url))

    if not jobs:
        return pd.DataFrame(columns=county_cols), pd.DataFrame(columns=_PRECINCT_COLS)

    candidate_id_map = (
        _build_candidate_id_map_from_state_df(state_df)
        if "candidate_id" in state_df.columns
        else None
    )

    county_frames: List[pd.DataFrame] = []
    precinct_frames: List[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(
                _fetch_and_parse_county_and_precinct,
                st, election_id, url, candidate_id_map, client_factory,
            ): election_id
            for st, election_id, url in jobs
        }
        for fut in as_completed(futures):
            eid = futures[fut]
            try:
                c_df, p_df = fut.result()
                if c_df is not None and not c_df.empty:
                    county_frames.append(c_df)
                if p_df is not None and not p_df.empty:
                    precinct_frames.append(p_df)
            except Exception as exc:
                print(f"  [ElectionStats] WARN: county/precinct failed for election_id={eid}: {exc}", flush=True)

    county_df   = pd.concat(county_frames,   ignore_index=True) if county_frames   else pd.DataFrame(columns=county_cols)
    precinct_df = pd.concat(precinct_frames, ignore_index=True) if precinct_frames else pd.DataFrame(columns=_PRECINCT_COLS)
    return county_df, precinct_df
