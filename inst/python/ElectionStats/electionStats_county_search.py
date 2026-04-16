from __future__ import annotations  # Enables postponed evaluation of type hints (Python 3.7+); helps with forward refs.

# Standard library imports
from dataclasses import asdict  # Convert dataclass instances into plain dicts (easy -> DataFrame).
from concurrent.futures import ThreadPoolExecutor, as_completed  # Thread-based parallelism utilities.
from typing import Optional, List, Tuple, Dict  # Type annotations for readability + static checking.
import time

# Third-party imports
from lxml import html  # HTML parsing + XPath support.
import pandas as pd  # DataFrame construction/concatenation.

# Local imports
from .electionStats_models import CountyVotes  # Dataclass representing one county/city vote record.
from .state_config import get_scraper_type  # Check if state is classic or v2

import requests  # for requests.exceptions

from http_utils import fetch_with_retry
from text_utils import parse_int as _parse_int


# -----------------------------------------------------------------------------
# Header labels we want to ignore when parsing tables
# -----------------------------------------------------------------------------

# Some tables have trailing "summary" columns that are NOT candidates (totals, blanks, etc.).
TRAILING_IGNORE_HEADERS = {
    "All Others",
    "Blank",
    "Blanks",
    "No Preference",
    "Scattering",
    "Total Votes",
    "Total Votes Cast",
    "Void",
}

# Many tables start with locality identifier columns (county/city/precinct/ward/etc.)
# These are not candidate columns either.
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

    Parameters
    ----------
    table : lxml element
        The <table> element containing the county/city results.

    Returns
    -------
    List[str]
        Candidate names as they appear in header order (aligned with vote cells in tbody).
    """
    # Grab all <th> elements from the first header row.
    ths = table.xpath(".//thead//tr[1]//th")
    names: List[str] = []

    for th in ths:
        # Some sites explicitly mark "pseudo-candidate" or total-votes columns via CSS classes.
        # If we hit those, we can stop early.
        th_class = (th.get("class") or "").lower()
        if "is_pseudocandidate" in th_class or "is-total-votes" in th_class:
            break

        # Build a label from all text contained in the header cell.
        label = " ".join(th.xpath(".//text()")).strip()

        # Skip the locality-identification headers (County/City, Ward, etc.)
        if label in LEADING_IGNORE_HEADERS:
            continue

        # If we hit known trailing summary headers, stop collecting candidate columns.
        if label in TRAILING_IGNORE_HEADERS:
            break

        # Candidate identifiers often appear as "tooltip-above" elements.
        # VA/MA uses <a>, CO uses <span>.
        node = None

        a = th.xpath(".//a[contains(@class,'tooltip-above')]")
        if a:
            node = a[0]
        else:
            sp = th.xpath(".//span[contains(@class,'tooltip-above')]")
            if sp:
                node = sp[0]

        if node is None:
            # Fallback: use plain th text directly (Idaho/Civera-style: no tooltip
            # wrapper). The cell has already passed both ignore-set checks above,
            # so if we reach here it is safe to treat the label as a candidate name.
            nm = label
        else:
            # Prefer oldtitle (usually the full candidate name), fallback to visible text.
            oldtitle = (node.get("oldtitle") or "").strip()
            text = " ".join(node.xpath(".//text()")).strip()
            nm = oldtitle or text

        # Extra safety: if we somehow got a trailing summary label via tooltip, stop.
        if nm in TRAILING_IGNORE_HEADERS:
            break

        if nm:
            names.append(nm)

    return names


def _count_trailing_ignored_columns_from_thead(table) -> int:
    """
    Count how many trailing summary columns appear at the end of the header row.
    This lets us drop those columns from each tbody row before slicing candidate vote cells.

    Example: if headers end with ["All Others", "Blanks", "Total Votes Cast"] => returns 3.

    Parameters
    ----------
    table : lxml element
        The results table.

    Returns
    -------
    int
        Number of trailing summary columns to drop from each tbody row.
    """
    # Note: XPath here assumes a simple <thead><tr><th> structure.
    ths = table.xpath(".//thead/tr/th")
    labels = [" ".join(th.xpath(".//text()")).strip() for th in ths]

    n = 0
    # Walk backwards through the header labels; count how many are in our ignore set.
    for lbl in reversed(labels):
        if lbl in TRAILING_IGNORE_HEADERS:
            n += 1
        else:
            break

    return n




def _extract_vote_text_from_td(td) -> str:
    """
    Extract vote text from a vote <td> cell.

    Some pages wrap vote counts inside a <div>, others are plain text.

    Parameters
    ----------
    td : lxml element
        The <td> element containing votes.

    Returns
    -------
    str
        Cleaned vote text.
    """
    # Prefer direct <div> text if present (common pattern).
    div_txt = td.xpath(".//div/text()")
    if div_txt:
        return div_txt[0].strip()

    # Fallback: gather all text contained in the cell.
    return "".join(td.xpath(".//text()")).strip()


def _iter_locality_rows(table):
    """
    Yield the tbody rows corresponding to localities (counties/cities).

    Preferred pattern:
      - rows with ids like: locality-id-12345

    Fallback pattern (some pages use divisions instead of localities):
      - rows with ids like: division-id-12345

    Parameters
    ----------
    table : lxml element
        The results table.

    Returns
    -------
    List[lxml element]
        List of matching <tr> elements.
    """
    # Primary: locality rows
    rows = table.xpath(".//tbody/tr[starts-with(@id,'locality-id-')]")
    if rows:
        return rows

    # Fallback: division rows
    return table.xpath(".//tbody/tr[starts-with(@id,'division-id-')]")


def _find_locality_td_index(tr) -> Optional[int]:
    """
    Return the index of the cell containing the locality label.

    Supports multiple layouts:
      - Many states: locality is in a <td> containing <a class="label">...</a>
      - CO-style:    locality is in a <th scope="row"> containing <span class="label">...</span>
      - Idaho-style: locality is in a plain first <td> with no .label wrapper

    Returns an index relative to the row's "data cells" list used by callers.
    For <td>-based rows, that's index in ./td.
    For <th>-based rows, we return 0 to indicate locality is the first logical cell,
    and callers should treat the vote cells as coming from ./td (see note below).
    """
    # --- Primary (original) behavior: locality lives in a <td> with <a class="label"> ---
    tds = tr.xpath("./td")
    for i, td in enumerate(tds):
        if td.xpath(".//a[contains(@class,'label')]"):
            return i

        # Added: some pages use <span class="label"> inside a <td>
        if td.xpath(".//span[contains(@class,'label')]"):
            return i

    # --- Fallback: locality lives in a <th scope="row"> (CO-style), not in <td> ---
    ths = tr.xpath("./th")
    if ths:
        # Look for <a class="label"> or <span class="label"> inside the row header cell
        for th in ths:
            if th.xpath(".//a[contains(@class,'label')]") or th.xpath(".//span[contains(@class,'label')]"):
                # In this layout, vote cells are typically the row's <td>s that follow the <th>.
                # Return 0 to indicate "locality is present before vote <td>s".
                return 0

    # --- Final fallback: plain first <td> (Idaho/Civera-style, no .label wrapper) ---
    # Only reached when no .label element is found anywhere in the row.  Safe because
    # this function is only called for rows already identified as locality rows
    # (division-id-* / locality-id-*), so the first <td> is the locality name.
    if tds:
        return 0

    return None


def _extract_county_name_from_row(tr) -> Optional[str]:
    """
    Extract locality name (County/City/etc.) from a row.

    Supports:
      - <a class="label">Name</a>              (VA/MA/NH classic style)
      - <span class="label">Name</span>        (CO-style)
      - plain first <td> text content          (Idaho/Civera-style: no .label wrapper)
    """
    # Original pattern (VA/MA/NH)
    txt = tr.xpath(".//a[contains(@class,'label')]/text()")
    if txt:
        name = txt[0].strip()
        return name or None

    # CO-style pattern
    txt = tr.xpath(".//span[contains(@class,'label')]/text()")
    if txt:
        name = txt[0].strip()
        return name or None

    # Fallback: plain text in the first <td> (Idaho/Civera-style).
    # Only reached when the above patterns both fail.  These rows are already
    # identified as locality rows (division-id-* / locality-id-*), so the
    # first cell is virtually always the locality name.
    tds = tr.xpath("./td")
    if tds:
        name = " ".join(tds[0].xpath(".//text()")).strip()
        return name or None

    return None


def _extract_candidate_vote_tds(
    tr,
    candidate_count: int,
    trailing_ignore_n: int,
) -> Optional[List]:
    """
    Return vote <td>s aligned to candidates.

    Handles:
      - locality in <td> (common)
      - locality in <th scope="row"> (CO-style)
    """
    tds = tr.xpath("./td")
    loc_idx = _find_locality_td_index(tr)
    if loc_idx is None:
        return None

    # If locality is in <th>, then ALL ./td are data cells (votes + summaries).
    # If locality is in <td>, we slice after that index.
    has_row_th = bool(tr.xpath("./th"))
    if has_row_th:
        data_tds = tds
    else:
        data_tds = tds[loc_idx + 1 :]

    # Drop trailing summary columns (e.g., Total Votes Cast, Blanks, etc.)
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

    Parameters
    ----------
    detail_html : str
        Raw HTML of the election detail page.
    election_id : int
        Election identifier to attach to each row.
    state : Optional[str]
        State abbreviation/name to attach to each row. Required here because CountyVotes requires it.
    candidate_id_map : Optional[Dict[str, int]]
        Mapping from candidate_name -> candidate_id (stable across pages). If None, a fallback map is created.

    Returns
    -------
    pd.DataFrame
        Long-form DataFrame with one row per (locality, candidate).
    """
    # ---------------------------------------------------
    # 1) Parse raw HTML into an lxml document object
    # ---------------------------------------------------
    doc = html.fromstring(detail_html)

    # ---------------------------------------------------
    # 2) Locate the results table.
    #    We search for a <table> that contains a header cell with one of:
    #    County/City, City/Town, or County (robust across states).
    # ---------------------------------------------------
    tables = doc.xpath(
        "//table[.//th["
        "normalize-space()='County/City' "
        "or normalize-space()='City/Town' "
        "or normalize-space()='County'"
        "]] | //table[@id='precinct_data']"
    )

    if not tables:
        # If we can't find the expected table, fail early with a clear error.
        raise ValueError("Could not find county/city results table.")

    # Use the first matching table; if multiple exist, this assumes the first is correct.
    table = tables[0]

    # ---------------------------------------------------
    # 3) Extract candidate names from the header row
    #    This defines the expected ordering of vote cells in tbody.
    # ---------------------------------------------------
    candidate_names = _extract_candidate_names_from_thead(table)
    if not candidate_names:
        raise ValueError("Could not extract candidate names from table header.")

    # ---------------------------------------------------
    # 4) Count trailing summary columns to drop from each row
    #    e.g., "Total Votes Cast", "Blanks", etc.
    # ---------------------------------------------------
    trailing_ignore_n = _count_trailing_ignored_columns_from_thead(table)

    # ---------------------------------------------------
    # 5) Build a fallback candidate_id mapping if one isn't provided
    #    NOTE: This fallback is only positional and may not match statewide IDs.
    # ---------------------------------------------------
    if candidate_id_map is None:
        candidate_id_map = {name: i + 1 for i, name in enumerate(candidate_names)}

    # ---------------------------------------------------
    # 6) Enforce required `state` parameter
    #    Your CountyVotes dataclass requires state (and you want it in output).
    # ---------------------------------------------------
    if state is None:
        raise ValueError("state is required to build CountyVotes rows (got None).")

    # ---------------------------------------------------
    # 7) Detect whether a "Total Votes" / "Total Votes Cast" column is present.
    #    It is always the last column when present, so we check the last header label
    #    and grab the last <td> from each data row.
    # ---------------------------------------------------
    _TOTAL_VOTES_LABELS = {"Total Votes", "Total Votes Cast"}
    all_header_labels = _get_all_header_labels(table)
    has_total_votes_col = bool(all_header_labels) and all_header_labels[-1] in _TOTAL_VOTES_LABELS

    # ---------------------------------------------------
    # 8) Iterate through each locality row (county/city)
    # ---------------------------------------------------
    rows: List[CountyVotes] = []
    county_total_votes: Dict[str, Optional[int]] = {}  # county_or_city -> total votes cast

    for tr in _iter_locality_rows(table):
        # Extract county/city name from the row.
        county = _extract_county_name_from_row(tr)
        if not county or county.lower() == "totals":
            continue  # Skip rows that don't look like a valid locality row.

        # Extract total votes cast for this locality (last <td> when column exists).
        if has_total_votes_col:
            all_tds = tr.xpath("./td")
            if all_tds:
                county_total_votes[county] = _parse_int(
                    _extract_vote_text_from_td(all_tds[-1])
                )

        # Extract ONLY the candidate vote <td> cells (ignore locality cols + summary cols).
        vote_tds = _extract_candidate_vote_tds(
            tr,
            candidate_count=len(candidate_names),
            trailing_ignore_n=trailing_ignore_n,
        )
        if vote_tds is None:
            continue  # Skip malformed rows.

        # ---------------------------------------------------
        # 9) Pair each candidate name with its corresponding vote cell
        # ---------------------------------------------------
        for cand_name, td in zip(candidate_names, vote_tds):
            # Extract vote count string and parse into int.
            votes = _parse_int(_extract_vote_text_from_td(td))
            if votes is None:
                continue  # Skip blanks/non-numeric entries.

            # Resolve candidate_id; skip if missing from mapping.
            cand_id = candidate_id_map.get(cand_name)
            if cand_id is None:
                continue

            # ---------------------------------------------------
            # 10) Create a CountyVotes dataclass instance for this (locality, candidate)
            # ---------------------------------------------------
            rows.append(
                CountyVotes(
                    state=state,
                    election_id=election_id,
                    county_or_city=county,
                    candidate_id=cand_id,
                    candidate=cand_name,
                    votes=votes,
                )
            )

    # ---------------------------------------------------
    # 11) Convert dataclass rows into a pandas DataFrame; attach total_votes when available.
    # ---------------------------------------------------
    df = pd.DataFrame([asdict(r) for r in rows])
    if not df.empty and county_total_votes:
        df["total_votes"] = df["county_or_city"].map(county_total_votes)
    return df


# -----------------------------
# DataFrame builders
# -----------------------------
def _build_candidate_id_map_from_state_df(state_df: pd.DataFrame) -> Dict[str, int]:
    """
    Build a candidate-name -> candidate_id mapping from a statewide results dataframe.

    Requires columns:
      - 'candidate'
      - 'candidate_id'

    If duplicates exist, the first occurrence wins.

    Parameters
    ----------
    state_df : pd.DataFrame
        Statewide results dataframe.

    Returns
    -------
    Dict[str, int]
        Mapping from candidate name to stable candidate_id.
    """
    if "candidate" not in state_df.columns or "candidate_id" not in state_df.columns:
        raise ValueError("state_df must include columns ['candidate', 'candidate_id'] to build candidate_id_map.")

    m: Dict[str, int] = {}
    # Iterate only over the two needed columns, ignoring missing rows.
    for _, r in state_df[["candidate", "candidate_id"]].dropna().iterrows():
        name = str(r["candidate"]).strip()
        cid = int(r["candidate_id"])
        # Keep the first ID we see for a given name to avoid accidental remaps.
        if name and name not in m:
            m[name] = cid

    return m


# =============================
# V2 CSV-based county + precinct parsing (SC/NM/VA)
# =============================

_COUNTY_COLS_V2 = ["state", "election_id", "candidate_id", "county_or_city", "candidate", "votes"]

# Row-type labels that represent a county/locality-level aggregate row.
# Virginia uses "Locality"; other states may use "County", "City", or "County/City".
_COUNTY_ROW_TYPES = {"county", "locality", "city", "county/city"}


def _parse_contest_csv_v2(
    csv_text: str,
    election_id: int,
    state: str,
    candidate_id_map: Optional[Dict[str, int]] = None,
) -> "tuple[pd.DataFrame, pd.DataFrame]":
    """
    Parse the flat contest CSV returned by /api/download_contest/{id}_table.csv.

    CSV format
    ----------
    Row 0 : ``,,Candidate1,Candidate2,...,Total Votes Cast,Total Ballots Cast``
    Row 1 : ``,,Party1,Party2,...``
    Row 2+: ``RowType,Name,votes1,votes2,...,total,total_ballots``

    RowType values
    --------------
    ``"County"`` / ``"Locality"`` / ``"City"`` → county-level aggregate row
    ``"Precinct"`` → precinct-level row; county context tracked from the last
                     county-equivalent row seen above it in the file
    anything else  → district-level totals (skipped)

    Returns
    -------
    (county_df, precinct_df)
        county_df   columns: state, election_id, candidate_id, county_or_city, candidate, party, votes
        precinct_df columns: state, election_id, candidate_id, county, precinct, candidate, votes
    """
    import csv as _csv
    import io as _io

    rows = list(_csv.reader(_io.StringIO(csv_text)))
    if len(rows) < 3:
        return (
            pd.DataFrame(columns=_COUNTY_COLS_V2),
            pd.DataFrame(columns=_PRECINCT_COLS),
        )

    # ── Candidate columns from header row ─────────────────────────────────────
    header = rows[0]
    cand_indices: List[tuple[int, str]] = []
    total_votes_col_idx: Optional[int] = None
    for i, col in enumerate(header):
        if i < 2:
            continue
        col_clean = col.strip()
        if not col_clean:
            break
        if col_clean.lower().startswith("total votes"):
            total_votes_col_idx = i
            break
        cand_indices.append((i, col_clean))

    if not cand_indices:
        return (
            pd.DataFrame(columns=_COUNTY_COLS_V2),
            pd.DataFrame(columns=_PRECINCT_COLS),
        )

    if candidate_id_map is None:
        candidate_id_map = {name: idx + 1 for idx, (_, name) in enumerate(cand_indices)}

    # ── Party row (row index 1): same column positions as the candidate header ──
    party_map: Dict[str, str] = {}
    if len(rows) >= 2:
        party_row = rows[1]
        for col_idx, cand_name in cand_indices:
            if col_idx < len(party_row):
                party = party_row[col_idx].strip()
                if party:
                    party_map[cand_name] = party

    county_records: List[Dict] = []
    precinct_records: List[Dict] = []
    current_county: Optional[str] = None

    for row in rows[1:]:
        if len(row) < 3:
            continue
        row_type = row[0].strip()
        name     = row[1].strip()

        if row_type.lower() in _COUNTY_ROW_TYPES:
            current_county = name
            county_tv: Optional[int] = None
            if total_votes_col_idx is not None and total_votes_col_idx < len(row):
                try:
                    county_tv = int(row[total_votes_col_idx].strip().replace(",", ""))
                except ValueError:
                    pass
            for col_idx, cand_name in cand_indices:
                if col_idx >= len(row):
                    continue
                try:
                    votes = int(row[col_idx].strip().replace(",", ""))
                except ValueError:
                    continue
                county_records.append({
                    "state":          state,
                    "election_id":    election_id,
                    "candidate_id":   candidate_id_map.get(cand_name, 0),
                    "county_or_city": name,
                    "candidate":      cand_name,
                    "party":          party_map.get(cand_name, ""),
                    "votes":          votes,
                    "total_votes":    county_tv,
                })

        elif row_type == "Precinct":
            precinct_label = f"Precinct {name}"
            for col_idx, cand_name in cand_indices:
                if col_idx >= len(row):
                    continue
                try:
                    votes = int(row[col_idx].strip().replace(",", ""))
                except ValueError:
                    continue
                precinct_records.append({
                    "state":        state,
                    "election_id":  election_id,
                    "candidate_id": candidate_id_map.get(cand_name, 0),
                    "county":       current_county or "",
                    "precinct":     precinct_label,
                    "candidate":    cand_name,
                    "votes":        votes,
                })
        # else: district/statewide totals row — skip

    county_df   = pd.DataFrame(county_records)   if county_records   else pd.DataFrame(columns=_COUNTY_COLS_V2)
    precinct_df = pd.DataFrame(precinct_records) if precinct_records else pd.DataFrame(columns=_PRECINCT_COLS)
    return county_df, precinct_df


def build_county_and_precinct_dataframe_v2(
    state_df: pd.DataFrame,
    base_url: str,
    max_workers: int = 6,
) -> "tuple[pd.DataFrame, pd.DataFrame]":
    """
    Download and parse county + precinct votes for v2 states (SC, NM, VA)
    using the CSV download API — no Playwright interaction required.

    For each unique election_id in *state_df* the function calls::

        GET {base_url}/api/download_contest/{election_id}_table.csv?split_party=false

    and parses county rows (first column == ``"County"``) and precinct rows
    (first column == ``"Precinct"``) from the flat CSV.

    Parameters
    ----------
    state_df : pd.DataFrame
        Must include columns ``['state', 'election_id']``.
        If ``'candidate_id'`` and ``'candidate'`` are present, a stable
        candidate-ID map is built from them.
    base_url : str
        Public base URL for the state (e.g. ``'https://electionstats.sos.nm.gov'``).
    max_workers : int
        Number of parallel download threads (default 6).

    Returns
    -------
    (county_df, precinct_df)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

    required = {"state", "election_id"}
    missing = required - set(state_df.columns)
    if missing:
        raise ValueError(f"state_df missing columns: {sorted(missing)}")

    candidate_id_map: Optional[Dict[str, int]] = None
    if "candidate_id" in state_df.columns and "candidate" in state_df.columns:
        candidate_id_map = {}
        for _, row in state_df[["candidate", "candidate_id"]].dropna().iterrows():
            name = str(row["candidate"]).strip()
            cid  = int(row["candidate_id"])
            if name and name not in candidate_id_map:
                candidate_id_map[name] = cid

    import requests as _requests

    def _fetch(u: str) -> str:
        return _requests.get(u, timeout=30).text

    def _fetch_one(election_id: int, state: str) -> "tuple[pd.DataFrame, pd.DataFrame]":
        url = f"{base}/api/download_contest/{election_id}_table.csv?split_party=false"
        csv_text = fetch_with_retry(_fetch, url)
        return _parse_contest_csv_v2(csv_text, election_id, state, candidate_id_map)

    base = base_url.rstrip("/")
    county_frames:   List[pd.DataFrame] = []
    precinct_frames: List[pd.DataFrame] = []

    unique = state_df[["state", "election_id"]].drop_duplicates()
    unique_rows = [(int(row["election_id"]), str(row["state"])) for _, row in unique.iterrows()]
    n_elections = len(unique_rows)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_fetch_one, eid, state): eid
            for eid, state in unique_rows
        }
        for future in _as_completed(futures):
            eid = futures[future]
            try:
                c_df, p_df = future.result()
                if not c_df.empty:
                    county_frames.append(c_df)
                if not p_df.empty:
                    precinct_frames.append(p_df)
            except Exception as exc:
                print(f"  [ElectionStats] WARN: CSV download failed for election_id={eid}: {exc}", flush=True)

    county_df   = pd.concat(county_frames,   ignore_index=True) if county_frames   else pd.DataFrame(columns=_COUNTY_COLS_V2)
    precinct_df = pd.concat(precinct_frames, ignore_index=True) if precinct_frames else pd.DataFrame(columns=_PRECINCT_COLS)
    return county_df, precinct_df


def build_county_dataframe(state_df: pd.DataFrame, client) -> pd.DataFrame:
    """
    Sequentially fetch + parse county/city vote details for each election in state_df.

    Parameters
    ----------
    state_df : pd.DataFrame
        Must include at least ['election_id', 'url'].
        If 'state' exists, it will be passed through and included in output.
        If 'candidate_id' exists, it will be used to create a stable candidate_id_map.
    client : object
        Must implement client.get_html(url) -> str.

    Returns
    -------
    pd.DataFrame
        Concatenated county/city vote dataframe (long form).
    """
    # Validate required columns exist.
    required = {"election_id", "url"}
    missing = required - set(state_df.columns)
    if missing:
        raise ValueError(f"state_df missing columns: {sorted(missing)}")

    # Determine whether we can include state in output.
    has_state = "state" in state_df.columns

    # Define output schema based on whether state exists.
    out_cols = (
        ["state", "election_id", "candidate_id", "county_or_city", "candidate", "votes"]
        if has_state
        else ["election_id", "candidate_id", "county_or_city", "candidate", "votes"]
    )

    # Build a list of jobs (state, election_id, url) to run.
    jobs: List[Tuple[Optional[str], int, str]] = []
    for _, r in state_df.iterrows():
        url = str(r["url"]).strip()
        if not url:
            continue  # Skip missing/blank urls.
        st = str(r["state"]) if has_state else None
        jobs.append((st, int(r["election_id"]), url))

    # If no work to do, return an empty dataframe with expected columns.
    if not jobs:
        return pd.DataFrame(columns=out_cols)

    # # Prefer stable candidate IDs derived from state_df if available.
    # candidate_id_map = (
    #     _build_candidate_id_map_from_state_df(state_df) if "candidate_id" in state_df.columns else None
    # )

    frames: List[pd.DataFrame] = []

    # Fetch + parse each detail page sequentially.
    for st, election_id, url in jobs:
        try:
            # Fetch the detail page HTML.
            detail_html = fetch_with_retry(client.get_html, url)

            # Parse into long-form county votes dataframe.
            df_one = parse_county_votes_from_detail_html(
                detail_html,
                election_id=election_id,
                state=st if has_state else None,
                # candidate_id_map=candidate_id_map,
            )

            # Keep only non-empty results.
            if not df_one.empty:
                frames.append(df_one)

        except Exception as e:
            # Log errors but continue processing other elections.
            print(
                f"[ERROR] Failed to parse county detail "
                f"(state={st}, election_id={election_id})\n"
                f"URL: {url}\n"
                f"Error: {type(e).__name__}: {e}\n"
            )

    # Concatenate results if any; otherwise return an empty df with expected schema.
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=out_cols)


def _fetch_and_parse_one_parallel(
    st: Optional[str],
    election_id: int,
    url: str,
    candidate_id_map: Optional[Dict[str, int]],
    client_factory,
) -> Optional[pd.DataFrame]:
    """
    Worker function used by the parallel builder.

    Creates its own client via client_factory() so each thread can have an independent client
    (important if the client is not thread-safe).

    Returns a parsed DataFrame or None on failure.
    """
    try:
        # Create a per-thread client instance.
        client = client_factory()

        # Fetch HTML and parse.
        detail_html = fetch_with_retry(client.get_html, url)
        return parse_county_votes_from_detail_html(
            detail_html,
            election_id=election_id,
            state=st,
            candidate_id_map=candidate_id_map,
        )

    except Exception as e:
        # Log and return None so caller can ignore failures.
        print(
            f"[ERROR] Failed to parse county detail "
            f"(state={st}, election_id={election_id})\n"
            f"URL: {url}\n"
            f"Error: {type(e).__name__}: {e}\n"
        )
        return None


# =============================================================================
# Precinct parsing
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


def _get_all_header_labels(table) -> List[str]:
    """Return all <th> labels in order from the first header row."""
    ths = table.xpath(".//thead//tr[1]//th")
    return [" ".join(th.xpath(".//text()")).strip() for th in ths]


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
        data_tds = tds[1:]
    else:
        leading_count = sum(1 for h in all_headers if h in LEADING_IGNORE_HEADERS)
        data_tds = tds[leading_count:]

    if trailing_ignore_n > 0 and len(data_tds) >= trailing_ignore_n:
        data_tds = data_tds[:-trailing_ignore_n]

    if len(data_tds) < candidate_count:
        return None

    return data_tds[-candidate_count:]


_PRECINCT_COLS = [
    "state", "election_year", "election_type", "election_id", "candidate_id", "office", "office_level", "district", "county", "precinct", "candidate", "party", "votes", "precinct_winner", "url"
]


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
# Combined county + precinct builder (single fetch per detail page)
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
            # Re-map candidate_ids to match statewide IDs
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


def build_county_dataframe_parallel(
    state_df: pd.DataFrame,
    client_factory,
    max_workers: int = 6,
) -> pd.DataFrame:
    """
    Parallel version of build_county_dataframe using ThreadPoolExecutor.

    Parameters
    ----------
    state_df : pd.DataFrame
        Must include at least ['election_id', 'url'].
        If 'state' exists, it will be passed through and included in output.
        If 'candidate_id' exists, it will be used to create a stable candidate_id_map.
    client_factory : callable
        Function that returns a new client instance with get_html(url) -> str.
        Used to ensure thread safety.
    max_workers : int
        Number of threads.

    Returns
    -------
    pd.DataFrame
        Concatenated county/city vote dataframe (long form).
    """
    # Validate required columns exist.
    required = {"election_id", "url"}
    missing = required - set(state_df.columns)
    if missing:
        raise ValueError(f"state_df missing columns: {sorted(missing)}")

    # Determine whether we can include state in output.
    has_state = "state" in state_df.columns

    # Define output schema based on whether state exists.
    out_cols = (
        ["state", "election_id", "candidate_id", "county_or_city", "candidate", "votes"]
        if has_state
        else ["election_id", "candidate_id", "county_or_city", "candidate", "votes"]
    )

    # Build a list of jobs (state, election_id, url).
    jobs: List[Tuple[Optional[str], int, str]] = []
    for _, r in state_df.iterrows():
        url = str(r["url"]).strip()
        if not url:
            continue
        st = str(r["state"]) if has_state else None
        jobs.append((st, int(r["election_id"]), url))

    # If nothing to do, return empty dataframe with expected columns.
    if not jobs:
        return pd.DataFrame(columns=out_cols)

    # Prefer stable candidate IDs derived from state_df if available.
    candidate_id_map = (
        _build_candidate_id_map_from_state_df(state_df) if "candidate_id" in state_df.columns else None
    )

    frames: List[pd.DataFrame] = []

    # ThreadPoolExecutor schedules IO-bound tasks well (HTTP fetches + parsing).
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        # Submit all jobs to the thread pool.
        futures = [
            ex.submit(_fetch_and_parse_one_parallel, st, election_id, url, candidate_id_map, client_factory)
            for st, election_id, url in jobs
        ]

        # as_completed yields futures as they finish (not necessarily submission order).
        for fut in as_completed(futures):
            df_one = fut.result()
            if df_one is not None and not df_one.empty:
                frames.append(df_one)

    # Combine successful results, preserving schema on empty.
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=out_cols)


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

    n_jobs = len(jobs)
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
