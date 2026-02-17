from __future__ import annotations

import re
from dataclasses import asdict
from typing import Iterable, List, Optional, Tuple

import pandas as pd
from lxml import html

from .electionStats_client import StateHttpClient
from .electionStats_models import ElectionSearchRow

# Accept both VA/MA: election-id-#### and CO: contest-id-####
_ROW_ID_RE = re.compile(r"^(?:election|contest)-id-(\d+)$")


# =============================
# Text + parsing helpers
# =============================
def _clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _safe_text(node) -> str:
    try:
        return _clean_ws(node.text_content())
    except Exception:
        return ""


def _parse_int(s: str) -> Optional[int]:
    s = (s or "").strip().replace(",", "")
    return int(s) if s.isdigit() else None


# =============================
# Party inference / normalization
# =============================
def _infer_party_from_stage(stage: str) -> Optional[str]:
    s = _clean_ws(stage).lower()

    if "democratic" in s and "primary" in s:
        return "Democratic"
    if "republican" in s and "primary" in s:
        return "Republican"
    if "libertarian" in s and "primary" in s:
        return "Libertarian"

    return None


def _is_write_in_party(party: str) -> bool:
    """
    True for values like "(Write-In)", "Write-In", "write in", etc.
    """
    if not party:
        return False
    p = party.strip().lower()
    p = re.sub(r"[()\[\]{}]", "", p)
    p = re.sub(r"[\s\-]+", "", p)  # "write-in"/"write in" -> "writein"
    return "writein" in p


def _normalize_party(party: Optional[str], stage: str) -> str:
    """
    If party missing OR party is write-in marker, infer from stage when possible.
    Returns a non-null string ("" if nothing available).
    """
    party_clean = (party or "").strip()
    inferred = _infer_party_from_stage(stage)

    if inferred and (not party_clean or _is_write_in_party(party_clean)):
        return inferred

    return party_clean


# =============================
# Candidate table helpers (works for VA/MA + CO)
# =============================
def _find_candidates_table(candidates_cell):
    """
    Return the first nested <table ... class contains 'candidates'> node, or None.
    """
    if candidates_cell is None:
        return None
    tables = candidates_cell.xpath(
        ".//table[contains(concat(' ', normalize-space(@class), ' '), ' candidates ')]"
    )
    return tables[0] if tables else None


def _iter_candidate_rows(candidates_table):
    if candidates_table is None:
        return []
    return candidates_table.xpath(".//tbody/tr")


def _is_special_candidate_row(tr) -> bool:
    """
    Skip summary/utility rows.
    Handles VA/MA and Colorado.
    """
    cls = (tr.get("class") or "").lower()

    # VA/MA
    if "more_info" in cls or "n_total_votes" in cls or "n_all_other_votes" in cls:
        return True

    # Colorado
    if "and-n-more" in cls:
        return True
    if "total-votes-cast" in cls:
        return True
    if "non_candidate" in cls:
        return True

    return False


def _extract_candidate_name(tr) -> Optional[str]:
    """
    Works for both:
      - <th class="candidate"><div class="name"><a>NAME</a>...
      - <td class="candidate"><div class="name"><a>NAME</a>...
    """
    name_nodes = tr.xpath(".//div[contains(@class,'name')]/a")
    if not name_nodes:
        return None
    name = _safe_text(name_nodes[0])
    return name or None


def _extract_party(tr) -> Optional[str]:
    party_nodes = tr.xpath(".//div[contains(@class,'party')]")
    party = _safe_text(party_nodes[0]) if party_nodes else ""
    return party or None


def _extract_vote_count(tr) -> Optional[int]:
    """
    Colorado candidate rows are: <th candidate> + <td votes> + <td pct>
    VA/MA are often:            <td candidate> + <td votes> + <td pct>

    So: read cells as (th|td) and take index 1 for votes.
    """
    cells = tr.xpath("./th|./td")
    if len(cells) < 2:
        return None
    return _parse_int(_safe_text(cells[1]))


def _extract_vote_percentage(tr) -> Optional[str]:
    cells = tr.xpath("./th|./td")
    if len(cells) < 3:
        return None
    pct = _safe_text(cells[2])
    return pct or None


def _extract_contest_outcome(tr) -> str:
    cls = (tr.get("class") or "")
    return "Winner" if "is_winner" in cls else "Loser"


def _extract_candidate_record(tr) -> Optional[Tuple[str, Optional[str], int, Optional[str], str]]:
    """
    Return:
      (candidate_name, party, vote_count, vote_percentage, contest_outcome)
    """
    if _is_special_candidate_row(tr):
        return None

    candidate_name = _extract_candidate_name(tr)
    if not candidate_name:
        return None

    total_vote_count = _extract_vote_count(tr)
    if total_vote_count is None:
        return None

    party = _extract_party(tr)
    vote_percentage = _extract_vote_percentage(tr)
    contest_outcome = _extract_contest_outcome(tr)

    return (candidate_name, party, total_vote_count, vote_percentage, contest_outcome)


def _extract_candidates_table(candidates_cell) -> List[Tuple[str, Optional[str], int, Optional[str], str]]:
    """
    Returns list of:
      (candidate_name, party, vote_count, vote_percentage, contest_outcome)
    """
    table = _find_candidates_table(candidates_cell)
    if table is None:
        return []

    out: List[Tuple[str, Optional[str], int, Optional[str], str]] = []
    for tr in _iter_candidate_rows(table):
        rec = _extract_candidate_record(tr)
        if rec is not None:
            out.append(rec)
    return out


# =============================
# Row parsers: VA/MA vs Colorado
# =============================
def _extract_year_from_colorado_year_th(th_node) -> Optional[int]:
    """
    Colorado uses:
      <th class="year ..."><span class="date-year">2024</span>...
    """
    if th_node is None:
        return None

    y = th_node.xpath(".//span[contains(@class,'date-year')]/text()")
    if y:
        try:
            return int(y[0].strip())
        except ValueError:
            pass

    txt = _safe_text(th_node)
    m = re.search(r"\b(19|20)\d{2}\b", txt)
    return int(m.group(0)) if m else None


def _parse_search_row_colorado(tr) -> Optional[Tuple[int, int, str, str, str, object]]:
    """
    Returns:
      (election_id, year, stage, office, district, candidates_cell)
    """
    tr_id = tr.get("id") or ""
    m = _ROW_ID_RE.match(tr_id)
    if not m:
        return None
    election_id = int(m.group(1))

    year_th = (tr.xpath("./th[contains(@class,'year')]") or [None])[0]
    year = _extract_year_from_colorado_year_th(year_th)
    if year is None:
        return None

    stage_node = (tr.xpath("./td[contains(@class,'party_border_top')]") or [None])[0]
    office_node = (tr.xpath("./td[contains(@class,'office')]") or [None])[0]
    district_node = (tr.xpath("./td[contains(@class,'division')]") or [None])[0]
    candidates_cell = (tr.xpath("./td[contains(@class,'candidates_container_cell')]") or [None])[0]

    stage = _safe_text(stage_node)
    office = _safe_text(office_node)
    district = _safe_text(district_node)

    if not stage or not office or candidates_cell is None:
        return None

    return election_id, year, stage, office, district, candidates_cell


def _parse_search_row_vama(tr) -> Optional[Tuple[int, int, str, str, str, object]]:
    """
    VA/MA row structure:
      td[0]=year, td[1]=office, td[2]=district, td[3]=stage, td[4]=candidates
    Returns:
      (election_id, year, stage, office, district, candidates_cell)
    """
    tr_id = tr.get("id") or ""
    m = _ROW_ID_RE.match(tr_id)
    if not m:
        return None
    election_id = int(m.group(1))

    tds = tr.xpath("./td")
    if len(tds) < 5:
        return None

    year_txt = _safe_text(tds[0])
    office = _safe_text(tds[1])
    district = _safe_text(tds[2])
    stage = _safe_text(tds[3])
    candidates_cell = tds[4]

    try:
        year = int(year_txt)
    except ValueError:
        m_year = re.search(r"\b(19|20)\d{2}\b", year_txt)
        if not m_year:
            return None
        year = int(m_year.group(0))

    if not office or not stage:
        return None

    return election_id, year, stage, office, district, candidates_cell


def _choose_row_parser(state_key: str):
    """
    Choose a specialized row parser by state key.
    """
    s = (state_key or "").strip().lower()
    if s == "colorado":
        return _parse_search_row_colorado
    return _parse_search_row_vama


# =============================
# Main parser
# =============================
def parse_search_results(page_html: str, client: StateHttpClient, state_name: str, url:str) -> List[ElectionSearchRow]:
    doc = html.fromstring(page_html)

    trs = doc.xpath(
        "//table[@id='search_results_table']//tr["
        "starts-with(@id,'election-id-') or starts-with(@id,'contest-id-')"
        "]"
    )

    parse_row = _choose_row_parser(state_name)

    out: List[ElectionSearchRow] = []

    for tr in trs:
        parsed = parse_row(tr)
        if parsed is None:
            continue

        election_id, year, stage, office, district, candidates_cell = parsed

        candidate_rows = _extract_candidates_table(candidates_cell)
        if not candidate_rows:
            continue

        for candidate_id, (
            candidate_name,
            party,
            total_vote_count,
            vote_percentage,
            contest_outcome,
        ) in enumerate(candidate_rows, start=1):
            out.append(
                ElectionSearchRow(
                    state=state_name or client.state,  # keep client as source of truth for state label
                    election_id=election_id,
                    year=year,
                    office=office,
                    district=district,
                    stage=stage,
                    candidate_id=candidate_id,
                    candidate=candidate_name,
                    party=_normalize_party(party, stage),
                    total_vote_count=total_vote_count,
                    vote_percentage=(vote_percentage or "").strip(),
                    contest_outcome=contest_outcome,
                )
            )

    return out


# =============================
# Fetch helpers
# =============================
def fetch_search_results(
    client: StateHttpClient,
    year_from: int = 1789,
    year_to: int = 2025,
    page: int = 1,
    state_name: str | None = None,   # ✅ added
) -> List[ElectionSearchRow]:

    url = client.build_search_url(
        year_from=year_from,
        year_to=year_to,
        page=page,
    )

    page_html = client.get_html(url)

    return parse_search_results(
        page_html,
        client,
        state_name=state_name,
        url = url  
    )



def fetch_search_results_dicts(
    client: StateHttpClient,
    state_key: str,
    year_from: int = 1789,
    year_to: int = 2025,
    page: int = 1,
) -> List[dict]:
    rows = fetch_search_results(client, state_key=state_key, year_from=year_from, year_to=year_to, page=page)
    return [asdict(r) | {"detail_url": client.build_detail_url(r.election_id)} for r in rows]


def iter_search_results(
    client: StateHttpClient,
    year_from: int = 1789,
    year_to: int = 2025,
    start_page: int = 1,
    max_pages: int = 200,
    state_name: str | None = None,   # ✅ added
) -> Iterable[ElectionSearchRow]:

    seen_keys: set[tuple[int, int]] = set()
    page = start_page

    for _ in range(max_pages):
        rows = fetch_search_results(
            client,
            year_from=year_from,
            year_to=year_to,
            page=page,
            state_name=state_name,   # ✅ pass through
        )

        if not rows:
            break

        new_rows = [
            r for r in rows
            if (r.election_id, r.candidate_id) not in seen_keys
        ]

        if not new_rows:
            break

        for r in new_rows:
            seen_keys.add((r.election_id, r.candidate_id))
            yield r

        page += 1



def fetch_all_search_results(
    client: StateHttpClient,
    year_from: int = 1789,
    year_to: int = 2025,
    start_page: int = 1,
    max_pages: int = 200,
    state_name: str | None = None,   
) -> List[ElectionSearchRow]:
    return list(
        iter_search_results(
            client,
            year_from=year_from,
            year_to=year_to,
            start_page=start_page,
            max_pages=max_pages,
            state_name=state_name,   
        )
    )



def rows_to_dataframe(rows: list[ElectionSearchRow], client: StateHttpClient) -> pd.DataFrame:
    """
    Convert parsed ElectionSearchRow objects into a pandas DataFrame.
    Includes computed detail_url.
    """
    records = [asdict(r) | {"detail_url": client.build_detail_url(r.election_id)} for r in rows]
    return pd.DataFrame.from_records(records)
