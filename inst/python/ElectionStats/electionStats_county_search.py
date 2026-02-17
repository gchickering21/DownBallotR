from __future__ import annotations  # Enables postponed evaluation of type hints (Python 3.7+); helps with forward refs.

# Standard library imports
from dataclasses import asdict  # Convert dataclass instances into plain dicts (easy -> DataFrame).
from concurrent.futures import ThreadPoolExecutor, as_completed  # Thread-based parallelism utilities.
from typing import Optional, List, Tuple, Dict  # Type annotations for readability + static checking.

# Third-party imports
from lxml import html  # HTML parsing + XPath support.
import pandas as pd  # DataFrame construction/concatenation.

# Local imports
from .electionStats_models import CountyVotes  # Dataclass representing one county/city vote record.
from .state_config import get_scraper_type  # Check if state is classic or v2


# -----------------------------------------------------------------------------
# Header labels we want to ignore when parsing tables
# -----------------------------------------------------------------------------

# Some tables have trailing "summary" columns that are NOT candidates (totals, blanks, etc.).
TRAILING_IGNORE_HEADERS = {
    "All Others",
    "Blanks",
    "No Preference",
    "Total Votes Cast",
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
            # If there is no tooltip node, treat this header as "not a candidate" and skip.
            continue

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


def _parse_int(s: str) -> Optional[int]:
    """
    Parse a vote string into an int.

    - Strips whitespace
    - Removes commas (e.g., "12,345")
    - Returns None if not purely digits after cleaning

    Parameters
    ----------
    s : str
        Input vote string.

    Returns
    -------
    Optional[int]
        Parsed integer or None if parsing fails.
    """
    s = (s or "").strip().replace(",", "")
    return int(s) if s.isdigit() else None


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

    return None


def _extract_county_name_from_row(tr) -> Optional[str]:
    """
    Extract locality name (County/City/etc.) from a row.

    Supports:
      - <a class="label">Name</a>
      - <span class="label">Name</span>
    """
    # Original pattern
    txt = tr.xpath(".//a[contains(@class,'label')]/text()")
    if txt:
        name = txt[0].strip()
        return name or None

    # CO-style pattern
    txt = tr.xpath(".//span[contains(@class,'label')]/text()")
    if txt:
        name = txt[0].strip()
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
        "]]"
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
    # 7) Iterate through each locality row (county/city)
    # ---------------------------------------------------
    rows: List[CountyVotes] = []

    for tr in _iter_locality_rows(table):
        # Extract county/city name from the row.
        county = _extract_county_name_from_row(tr)
        if not county or county.lower() == "totals":
            continue  # Skip rows that don't look like a valid locality row.

        # Extract ONLY the candidate vote <td> cells (ignore locality cols + summary cols).
        vote_tds = _extract_candidate_vote_tds(
            tr,
            candidate_count=len(candidate_names),
            trailing_ignore_n=trailing_ignore_n,
        )
        if vote_tds is None:
            continue  # Skip malformed rows.

        # ---------------------------------------------------
        # 8) Pair each candidate name with its corresponding vote cell
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
            # 9) Create a CountyVotes dataclass instance for this (locality, candidate)
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
    # 10) Convert dataclass rows into a pandas DataFrame
    # ---------------------------------------------------
    df = pd.DataFrame([asdict(r) for r in rows])
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
# V2-specific county parsing (SC/NM)
# =============================
def _extract_candidate_names_v2(table) -> List[str]:
    """
    Extract candidate names from v2 table headers (plain text, no tooltips).

    V2 structure:
      Header row: County | Candidate1 | Candidate2 | Total Votes Cast | Total Ballots Cast

    Parameters
    ----------
    table : lxml element
        The county results table

    Returns
    -------
    List[str]
        Candidate names in order
    """
    ths = table.xpath(".//thead//tr[1]//th | .//tr[1]//th")
    names: List[str] = []

    for th in ths:
        label = " ".join(th.xpath(".//text()")).strip()

        # Skip locality columns
        if label in LEADING_IGNORE_HEADERS:
            continue

        # Stop at trailing summary columns
        if label in TRAILING_IGNORE_HEADERS or "Total" in label:
            break

        if label:
            names.append(label)

    return names


def parse_county_votes_v2(
    detail_html: str,
    election_id: int,
    state: str,
    candidate_id_map: Optional[Dict[str, int]] = None,
) -> pd.DataFrame:
    """
    Parse county votes from v2 state detail pages (SC/NM).

    V2 table structure is simpler than classic:
    - Plain text headers (no tooltips)
    - Header: County | Candidate1 | Candidate2 | Total Votes Cast | ...
    - Data rows: County name | vote count | vote count | total | ...

    Parameters
    ----------
    detail_html : str
        Rendered HTML from Playwright
    election_id : int
        Election ID
    state : str
        State identifier
    candidate_id_map : Optional[Dict[str, int]]
        Mapping from candidate name to ID

    Returns
    -------
    pd.DataFrame
        Long-form county votes
    """
    doc = html.fromstring(detail_html)

    # Find any table (v2 typically has just one main table)
    tables = doc.xpath("//table")
    if not tables:
        raise ValueError("No table found in detail page")

    table = tables[0]

    # Extract candidate names from header
    candidate_names = _extract_candidate_names_v2(table)
    if not candidate_names:
        raise ValueError("Could not extract candidate names from v2 table header")

    # Build candidate ID map if not provided
    if candidate_id_map is None:
        candidate_id_map = {name: i + 1 for i, name in enumerate(candidate_names)}

    # Parse data rows
    data_rows = table.xpath(".//tbody/tr | .//tr[position()>1]")
    records: List[Dict] = []

    for tr in data_rows:
        cells = tr.xpath("./td | ./th")
        if len(cells) < 2:
            continue

        # First cell is county/locality name
        county = " ".join(cells[0].xpath(".//text()")).strip()
        if not county or county in LEADING_IGNORE_HEADERS:
            continue

        # Remaining cells are vote counts for each candidate
        for i, candidate_name in enumerate(candidate_names):
            if i + 1 >= len(cells):
                break

            vote_text = " ".join(cells[i + 1].xpath(".//text()")).strip()
            vote_text = vote_text.replace(",", "")

            try:
                votes = int(vote_text) if vote_text.isdigit() else 0
            except ValueError:
                votes = 0

            records.append({
                "state": state,
                "election_id": election_id,
                "county_or_city": county,
                "candidate_id": candidate_id_map.get(candidate_name, i + 1),
                "candidate_name": candidate_name,
                "votes": votes,
            })

    return pd.DataFrame(records)


def build_county_dataframe_v2(
    state_df: pd.DataFrame,
    playwright_client,
) -> pd.DataFrame:
    """
    Fetch and parse county votes for v2 states using Playwright.

    Parameters
    ----------
    state_df : pd.DataFrame
        Must include ['state', 'election_id', 'detail_url']
        If 'candidate_id' exists, used to create stable candidate_id_map
    playwright_client : PlaywrightClient
        Playwright client in context manager

    Returns
    -------
    pd.DataFrame
        Concatenated county vote data
    """
    required = {"state", "election_id", "detail_url"}
    missing = required - set(state_df.columns)
    if missing:
        raise ValueError(f"state_df missing columns: {sorted(missing)}")

    # Build candidate ID map from state_df if available
    candidate_id_map = None
    if "candidate_id" in state_df.columns and "candidate" in state_df.columns:
        candidate_id_map = {}
        for _, row in state_df.iterrows():
            name = str(row.get("candidate", "")).strip()
            cid = int(row["candidate_id"])
            if name and name not in candidate_id_map:
                candidate_id_map[name] = cid

    frames: List[pd.DataFrame] = []

    for _, row in state_df.iterrows():
        election_id = int(row["election_id"])
        state = str(row["state"])
        detail_url = str(row["detail_url"])

        try:
            # Navigate to detail page
            playwright_client.page.goto(detail_url, wait_until="networkidle")

            # Wait for table to load
            import time
            time.sleep(1)

            detail_html = playwright_client.page.content()

            # Parse county votes
            df = parse_county_votes_v2(
                detail_html,
                election_id,
                state,
                candidate_id_map,
            )

            if not df.empty:
                frames.append(df)

        except Exception as e:
            print(f"Warning: Failed to parse county votes for election_id={election_id}: {e}")
            continue

    if not frames:
        return pd.DataFrame(columns=["state", "election_id", "candidate_id", "county_or_city", "candidate_name", "votes"])

    return pd.concat(frames, ignore_index=True)


def build_county_dataframe(state_df: pd.DataFrame, client) -> pd.DataFrame:
    """
    Sequentially fetch + parse county/city vote details for each election in state_df.

    Parameters
    ----------
    state_df : pd.DataFrame
        Must include at least ['election_id', 'detail_url'].
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
    required = {"election_id", "detail_url"}
    missing = required - set(state_df.columns)
    if missing:
        raise ValueError(f"state_df missing columns: {sorted(missing)}")

    # Determine whether we can include state in output.
    has_state = "state" in state_df.columns

    # Define output schema based on whether state exists.
    out_cols = (
        ["state", "election_id", "candidate_id", "county_or_city", "candidate_name", "votes"]
        if has_state
        else ["election_id", "candidate_id", "county_or_city", "candidate_name", "votes"]
    )

    # Build a list of jobs (state, election_id, url) to run.
    jobs: List[Tuple[Optional[str], int, str]] = []
    for _, r in state_df.iterrows():
        url = str(r["detail_url"]).strip()
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
            detail_html = client.get_html(url)

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
        detail_html = client.get_html(url)
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
        Must include at least ['election_id', 'detail_url'].
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
    required = {"election_id", "detail_url"}
    missing = required - set(state_df.columns)
    if missing:
        raise ValueError(f"state_df missing columns: {sorted(missing)}")

    # Determine whether we can include state in output.
    has_state = "state" in state_df.columns

    # Define output schema based on whether state exists.
    out_cols = (
        ["state", "election_id", "candidate_id", "county_or_city", "candidate_name", "votes"]
        if has_state
        else ["election_id", "candidate_id", "county_or_city", "candidate_name", "votes"]
    )

    # Build a list of jobs (state, election_id, url).
    jobs: List[Tuple[Optional[str], int, str]] = []
    for _, r in state_df.iterrows():
        url = str(r["detail_url"]).strip()
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
