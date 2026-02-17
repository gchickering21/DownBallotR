from __future__ import annotations

from dataclasses import asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from lxml import html
from typing import Optional, List, Tuple, Dict

import pandas as pd

from .electionStats_models import CountyVotes


TRAILING_IGNORE_HEADERS = {
    "All Others",
    "Blanks",
    "No Preference",
    "Total Votes Cast",
}

LEADING_IGNORE_HEADERS = {
    "County/City",
    "City/Town",
    "County",
    "Ward",
    "Pct",
    "Precinct",
}


def _extract_candidate_names_from_thead(table) -> List[str]:
    """
    Extract candidate names from the header in the same order the vote <td>s appear.

    Works for:
      - VA/MA: <a class="tooltip-above" oldtitle="Full Name">Short</a>
      - CO:    <span class="tooltip-above" oldtitle="Full Name (Party)">Short</span>

    Stops when it hits trailing summary columns (Total Votes Cast, All Others, Blanks, No Preference).
    Ignores locality columns like County/Ward/Pct.
    """
    ths = table.xpath(".//thead//tr[1]//th")
    names: List[str] = []

    for th in ths:
        # Fast stop conditions for trailing pseudo-candidate / totals
        th_class = (th.get("class") or "").lower()
        if "is_pseudocandidate" in th_class or "is-total-votes" in th_class:
            break

        label = " ".join(th.xpath(".//text()")).strip()

        # Skip leading locality columns
        if label in LEADING_IGNORE_HEADERS:
            continue

        # Stop when we reach trailing summary columns
        if label in TRAILING_IGNORE_HEADERS:
            break

        # Candidate identifier node: VA/MA uses <a>, CO uses <span>
        node = None
        a = th.xpath(".//a[contains(@class,'tooltip-above')]")
        if a:
            node = a[0]
        else:
            sp = th.xpath(".//span[contains(@class,'tooltip-above')]")
            if sp:
                node = sp[0]

        if node is None:
            # If it's not a tooltip element, it's not a candidate header
            continue

        oldtitle = (node.get("oldtitle") or "").strip()
        text = " ".join(node.xpath(".//text()")).strip()
        nm = oldtitle or text

        # Extra safety: ignore trailing items even if they appear via tooltip span
        if nm in TRAILING_IGNORE_HEADERS:
            break

        if nm:
            names.append(nm)

    return names




def _count_trailing_ignored_columns_from_thead(table) -> int:
    """
    Count how many trailing summary columns appear at the end of the header row.
    This lets us drop those columns from each tbody row before slicing candidate vote cells.
    """
    ths = table.xpath(".//thead/tr/th")
    labels = [" ".join(th.xpath(".//text()")).strip() for th in ths]

    n = 0
    for lbl in reversed(labels):
        if lbl in TRAILING_IGNORE_HEADERS:
            n += 1
        else:
            break
    return n


def _parse_int(s: str) -> Optional[int]:
    s = (s or "").strip().replace(",", "")
    return int(s) if s.isdigit() else None


def _extract_vote_text_from_td(td) -> str:
    div_txt = td.xpath(".//div/text()")
    if div_txt:
        return div_txt[0].strip()
    return "".join(td.xpath(".//text()")).strip()


def _iter_locality_rows(table):
    return table.xpath(".//tbody/tr[starts-with(@id,'locality-id-')]")


def _find_locality_td_index(tr) -> Optional[int]:
    """
    Returns the td index containing the locality anchor (<a class='label'>...).
    This is robust to extra columns like Ward/Pct.
    """
    tds = tr.xpath("./td")
    for i, td in enumerate(tds):
        if td.xpath(".//a[contains(@class,'label')]"):
            return i
    return None


def _extract_county_name_from_row(tr) -> Optional[str]:
    """
    Extract locality name (County/City) from anywhere in the row.
    Robust to Ward/Pct columns that may appear before/after the locality cell.
    """
    a = tr.xpath(".//a[contains(@class,'label')]/text()")
    if not a:
        return None
    return a[0].strip() or None


def _extract_candidate_vote_tds(
    tr,
    candidate_count: int,
    trailing_ignore_n: int,
) -> Optional[List]:
    """
    Given a tbody <tr>, return the list of <td> elements corresponding to candidate vote columns.

    Strategy:
      - Find locality td index (the cell containing the locality label link)
      - Consider all tds AFTER locality
      - Drop trailing summary tds (All Others / Blanks / No Preference / Total Votes Cast)
      - Ignore Ward/Pct and any other extra columns by taking the LAST N cells as candidate votes
    """
    tds = tr.xpath("./td")
    loc_idx = _find_locality_td_index(tr)
    if loc_idx is None:
        return None

    data_tds = tds[loc_idx + 1 :]

    if trailing_ignore_n > 0 and len(data_tds) >= trailing_ignore_n:
        data_tds = data_tds[:-trailing_ignore_n]

    if len(data_tds) < candidate_count:
        return None

    return data_tds[-candidate_count:]


# -----------------------------
# Public parsing API
# -----------------------------
def parse_county_votes_from_detail_html(
    detail_html: str,
    election_id: int,
    state: Optional[str] = None,
    candidate_id_map: Optional[Dict[str, int]] = None,
) -> pd.DataFrame:
    """
    Parse county/city vote totals from an election detail HTML page.

    Handles:
      - Optional Ward / Pct columns (ignored automatically)
      - Trailing summary columns ignored:
        'All Others', 'Blanks', 'No Preference', 'Total Votes Cast'

    Requires `state` to produce CountyVotes dataclass rows (enforced).
    """

    # ---------------------------------------------------
    # 1️⃣ Parse the raw HTML into an lxml document
    # ---------------------------------------------------
    doc = html.fromstring(detail_html)

    # ---------------------------------------------------
    # 2️⃣ Locate the correct results table
    #    We search for a table that contains a header
    #    cell labeled County/City, City/Town, or County.
    #    This makes it robust across VA/MA/CO formats.
    # ---------------------------------------------------
    tables = doc.xpath(
        "//table[.//th["
        "normalize-space()='County/City' "
        "or normalize-space()='City/Town' "
        "or normalize-space()='County'"
        "]]"
    )

    if not tables:
        raise ValueError("Could not find county/city results table.")

    # Use the first matching table
    table = tables[0]

    # ---------------------------------------------------
    # 3️⃣ Extract candidate names from the table header
    #    This ensures vote columns line up correctly.
    # ---------------------------------------------------
    candidate_names = _extract_candidate_names_from_thead(table)

    if not candidate_names:
        raise ValueError("Could not extract candidate names from table header.")

    # ---------------------------------------------------
    # 4️⃣ Count trailing summary columns
    #    (e.g., Total Votes Cast, Blanks, All Others)
    #    so we can ignore them when slicing vote cells.
    # ---------------------------------------------------
    trailing_ignore_n = _count_trailing_ignored_columns_from_thead(table)

    # ---------------------------------------------------
    # 5️⃣ Build fallback candidate_id mapping if not provided
    #    Ideally this comes from state-level results to ensure
    #    stable IDs across pages.
    # ---------------------------------------------------
    if candidate_id_map is None:
        candidate_id_map = {name: i + 1 for i, name in enumerate(candidate_names)}

    # ---------------------------------------------------
    # 6️⃣ Enforce required state parameter
    #    We need this to build CountyVotes dataclass rows.
    # ---------------------------------------------------
    if state is None:
        raise ValueError("state is required to build CountyVotes rows (got None).")

    # ---------------------------------------------------
    # 7️⃣ Iterate through each locality row (county/city)
    # ---------------------------------------------------
    rows: List[CountyVotes] = []

    for tr in _iter_locality_rows(table):

        # Extract county/city name from the row
        county = _extract_county_name_from_row(tr)
        if not county:
            continue  # skip malformed rows

        # ---------------------------------------------------
        # Extract the <td> elements that correspond ONLY to
        # candidate vote columns (ignoring locality + summary)
        # ---------------------------------------------------
        vote_tds = _extract_candidate_vote_tds(
            tr,
            candidate_count=len(candidate_names),
            trailing_ignore_n=trailing_ignore_n,
        )

        if vote_tds is None:
            continue

        # ---------------------------------------------------
        # 8️⃣ Pair each candidate name with its vote cell
        # ---------------------------------------------------
        for cand_name, td in zip(candidate_names, vote_tds):

            # Extract and parse vote count
            votes = _parse_int(_extract_vote_text_from_td(td))
            if votes is None:
                continue

            # Map candidate name → candidate_id
            cand_id = candidate_id_map.get(cand_name)
            if cand_id is None:
                continue  # safety: skip if mapping missing

            # ---------------------------------------------------
            # 9️⃣ Build CountyVotes dataclass row
            # ---------------------------------------------------
            rows.append(
                CountyVotes(
                    state=state,
                    election_id=election_id,
                    county_or_city=county,
                    candidate_id=cand_id,
                    candidate_name=cand_name,
                    votes=votes,
                )
            )

    # ---------------------------------------------------
    # 🔟 Convert dataclass rows to pandas DataFrame
    # ---------------------------------------------------
    df = pd.DataFrame([asdict(r) for r in rows])
    return df 



# -----------------------------
# Dataframe builders
# -----------------------------
def _build_candidate_id_map_from_state_df(state_df: pd.DataFrame) -> Dict[str, int]:
    """
    Map candidate name -> candidate_id using the state_df.
    Requires columns: ['candidate', 'candidate_id'].
    If there are duplicates, first one wins.
    """
    if "candidate" not in state_df.columns or "candidate_id" not in state_df.columns:
        raise ValueError("state_df must include columns ['candidate', 'candidate_id'] to build candidate_id_map.")

    m: Dict[str, int] = {}
    for _, r in state_df[["candidate", "candidate_id"]].dropna().iterrows():
        name = str(r["candidate"]).strip()
        cid = int(r["candidate_id"])
        if name and name not in m:
            m[name] = cid
    return m


def build_county_dataframe(state_df: pd.DataFrame, client) -> pd.DataFrame:
    required = {"election_id", "detail_url"}
    missing = required - set(state_df.columns)
    if missing:
        raise ValueError(f"state_df missing columns: {sorted(missing)}")

    has_state = "state" in state_df.columns
    out_cols = (
        ["state", "election_id", "candidate_id", "county_or_city", "candidate_name", "votes"]
        if has_state
        else ["election_id", "candidate_id", "county_or_city", "candidate_name", "votes"]
    )

    jobs: List[Tuple[Optional[str], int, str]] = []
    for _, r in state_df.iterrows():
        url = str(r["detail_url"]).strip()
        if not url:
            continue
        st = str(r["state"]) if has_state else None
        jobs.append((st, int(r["election_id"]), url))

    if not jobs:
        return pd.DataFrame(columns=out_cols)

    candidate_id_map = _build_candidate_id_map_from_state_df(state_df) if "candidate_id" in state_df.columns else None

    frames: List[pd.DataFrame] = []

    for st, election_id, url in jobs:
        try:
            detail_html = client.get_html(url)
            df_one = parse_county_votes_from_detail_html(
                detail_html,
                election_id=election_id,
                state=st if has_state else None,
                candidate_id_map=candidate_id_map,
            )
            if not df_one.empty:
                frames.append(df_one)
        except Exception as e:
            print(
                f"[ERROR] Failed to parse county detail "
                f"(state={st}, election_id={election_id})\n"
                f"URL: {url}\n"
                f"Error: {type(e).__name__}: {e}\n"
            )

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=out_cols)


def _fetch_and_parse_one_parallel(
    st: Optional[str],
    election_id: int,
    url: str,
    candidate_id_map: Optional[Dict[str, int]],
    client_factory,
) -> Optional[pd.DataFrame]:
    try:
        client = client_factory()
        detail_html = client.get_html(url)
        return parse_county_votes_from_detail_html(
            detail_html,
            election_id=election_id,
            state=st,
            candidate_id_map=candidate_id_map,
        )
    except Exception as e:
        print(
            f"[ERROR] Failed to parse county detail "
            f"(state={st}, election_id={election_id})\n"
            f"URL: {url}\n"
            f"Error: {type(e).__name__}: {e}\n"
        )
        return None


def build_county_dataframe_parallel(
    state_df: pd.DataFrame,
    client_factory,
    max_workers: int = 6,
) -> pd.DataFrame:
    required = {"election_id", "detail_url"}
    missing = required - set(state_df.columns)
    if missing:
        raise ValueError(f"state_df missing columns: {sorted(missing)}")

    has_state = "state" in state_df.columns
    out_cols = (
        ["state", "election_id", "candidate_id", "county_or_city", "candidate_name", "votes"]
        if has_state
        else ["election_id", "candidate_id", "county_or_city", "candidate_name", "votes"]
    )

    jobs: List[Tuple[Optional[str], int, str]] = []
    for _, r in state_df.iterrows():
        url = str(r["detail_url"]).strip()
        if not url:
            continue
        st = str(r["state"]) if has_state else None
        jobs.append((st, int(r["election_id"]), url))

    if not jobs:
        return pd.DataFrame(columns=out_cols)

    candidate_id_map = _build_candidate_id_map_from_state_df(state_df) if "candidate_id" in state_df.columns else None

    frames: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [
            ex.submit(_fetch_and_parse_one_parallel, st, election_id, url, candidate_id_map, client_factory)
            for st, election_id, url in jobs
        ]
        for fut in as_completed(futures):
            df_one = fut.result()
            if df_one is not None and not df_one.empty:
                frames.append(df_one)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=out_cols)
