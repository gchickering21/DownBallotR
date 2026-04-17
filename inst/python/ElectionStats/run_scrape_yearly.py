# ElectionStats/run_scrape_yearly.py
from __future__ import annotations

from dataclasses import fields
from pathlib import Path
from typing import Callable

import pandas as pd
import pyreadr  

from ElectionStats.electionStats_client import HttpConfig, StateHttpClient
from ElectionStats.electionStats_models import ElectionSearchRow
from ElectionStats.electionStats_search import (
    fetch_all_search_results,
    fetch_all_search_results_v2,
    rows_to_dataframe,
)
from ElectionStats.electionStats_county_search import (
    build_county_dataframe_parallel,
    build_county_dataframe,
    build_county_and_precinct_dataframe_parallel,
    build_county_and_precinct_dataframe_v2,
    _PRECINCT_COLS,
)
from ElectionStats.state_config import get_state_config
from ElectionStats.playwright_client import PlaywrightClient
from df_utils import concat_or_empty as _concat_or_empty


# ---------------------------
# State configuration now managed in state_config.py
# Use get_state_config() to access state URLs and scraping methods
# ---------------------------


# ---------------------------
# Defaults (kept fixed for now)
# ---------------------------
_TIMEOUT_S = 120
_SLEEP_S_STATE = 0.10
_SLEEP_S_COUNTY = 0.10
_MAX_WORKERS = 6
_SAMPLE_N = 5000

JOIN_KEYS = ["state", "election_id", "candidate_id"]
COUNTY_COLS = ["state", "election_year", "election_type", "election_id", "candidate_id", "office", "office_level", "district", "county_or_city", "candidate", "party", "votes", "vote_pct", "county_winner", "url"]


# ---------------------------
# Output helpers
# ---------------------------
def _ensure_outdir(outdir: str | Path) -> Path:
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)
    return out


def _save_rds(df: pd.DataFrame, path: Path) -> None:
    pyreadr.write_rds(str(path), df, compress="gzip")


def _save_outputs(df: pd.DataFrame, outdir: Path, base_name: str, sample_n: int = _SAMPLE_N) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    sample_csv = outdir / f"{base_name}_sample.csv"
    df.head(sample_n).to_csv(sample_csv, index=False)

    rds_path = outdir / f"{base_name}.rds"
    try:
        _save_rds(df, rds_path)
    except ImportError:
        print(f"[WARN] pyreadr not installed; skipping RDS: {rds_path}")
    except Exception as e:
        print(f"[WARN] Failed to write RDS {rds_path}: {e}")


# ---------------------------
# Client helpers
# ---------------------------
def _normalize_state(state: str) -> str:
    return state.strip().lower().replace(" ", "_")


def _make_client(state_key: str, base_url: str, sleep_s: float, search_path: str, url_style: str = "path_params") -> StateHttpClient:
    return StateHttpClient(
        state=state_key,
        base_url=base_url,
        config=HttpConfig(timeout_s=_TIMEOUT_S, sleep_s=sleep_s),
        search_path=search_path,
        url_style=url_style,
    )


def _make_client_factory(state_key: str, base_url: str, sleep_s: float, search_path: str, url_style: str = "path_params"):
    def factory() -> StateHttpClient:
        return _make_client(state_key, base_url, sleep_s, search_path, url_style)
    return factory


# ---------------------------
# Core scrape
# ---------------------------
def scrape_one_year(
    state_key: str,
    state_name: str,
    base_url: str,
    search_path: str,
    year: int,
    parallel: bool,
    scraping_method: str,
    url_style: str = "path_params",
    level: str = "all",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns (state_df, county_df, precinct_df) for a single year.
    Dispatches to requests-based or Playwright-based scraping.

    state_df:    candidate-exploded search results (includes url)
    county_df:   long county votes (includes candidate_id)
    precinct_df: long precinct votes; empty DataFrame for states/years without
                 precinct rows on the detail pages.

    When level='state', county/precinct fetching is skipped entirely.
    """
    _need_subunit = level != "state"

    if scraping_method == "playwright":
        # =============================
        # Playwright states: use browser to render the search page.
        # V2 states (SC, NM, NY, VA) are React SPAs requiring Playwright.
        # =============================
        from ElectionStats.state_config import get_state_config as _get_cfg
        _state_cfg = _get_cfg(state_key)
        _scraper_type = _state_cfg.get("scraper_type", "v2")

        with PlaywrightClient(state_key, base_url) as pw_client:
            rows = fetch_all_search_results_v2(
                pw_client,
                year_from=year,
                year_to=year,
                state_name=state_name
            )
            state_df = rows_to_dataframe(rows, client=pw_client)

            if state_df.empty:
                return state_df, pd.DataFrame(columns=COUNTY_COLS), pd.DataFrame(columns=_PRECINCT_COLS)

            # Ensure state column
            if "state" not in state_df.columns:
                state_df.insert(0, "state", state_name)
            else:
                state_df["state"] = state_name

            if "election_year" not in state_df.columns:
                state_df.insert(0, "election_year", year)

            # Only hit each election detail page once
            unique_elections = state_df[["state", "election_id", "url"]].drop_duplicates()

            if not _need_subunit:
                county_df   = pd.DataFrame(columns=COUNTY_COLS)
                precinct_df = pd.DataFrame(columns=_PRECINCT_COLS)
            elif _scraper_type == "classic":
                n_unique = len(unique_elections)
                print(
                    f"  [ElectionStats] Found {len(state_df):,} state-level rows; "
                    f"fetching county/precinct for {n_unique:,} election(s)...",
                    flush=True,
                )
                county_df, precinct_df = build_county_and_precinct_dataframe_parallel(
                    state_df=unique_elections,
                    client_factory=_make_client_factory(
                        state_key=state_key,
                        base_url=base_url,
                        sleep_s=_SLEEP_S_COUNTY,
                        search_path=search_path,
                        url_style=url_style,
                    ),
                    max_workers=_MAX_WORKERS,
                )
            else:
                n_unique = len(unique_elections)
                print(
                    f"  [ElectionStats] Found {len(state_df):,} state-level rows; "
                    f"fetching county/precinct for {n_unique:,} election(s)...",
                    flush=True,
                )
                _county_method = _state_cfg.get("county_method", "csv")
                # Use positional candidate IDs from the CSV header (1, 2, 3 ...).
                # Candidate names from v2 text summaries often differ from CSV header
                # names, so a global name→id map built from state_df is unreliable.
                # Passing unique_elections (no candidate columns) forces each CSV to
                # assign its own sequential IDs, which are then consistent across
                # county_df, precinct_df, and the rebuilt state_df below.
                #
                # For states like NY whose CSV API is behind Cloudflare, pass
                # pw_client.fetch_csv_text as the fetcher so downloads go through
                # the already-open browser session (which has Cloudflare clearance).
                # This also forces sequential execution (required for Playwright).
                _csv_fetcher = (
                    pw_client.fetch_csv_text
                    if _state_cfg.get("csv_requires_browser", False)
                    else None
                )
                county_df, precinct_df = build_county_and_precinct_dataframe_v2(
                    state_df=unique_elections,
                    base_url=base_url,
                    max_workers=_MAX_WORKERS,
                    fetcher=_csv_fetcher,
                )

            # ── Rebuild state_df from CSV data (v2 states only) ──────────────
            # The v2 search page provides election metadata (office, district,
            # stage, year) but only text-parsed candidate info (votes=0, no party,
            # winner pct only).  Rebuild the candidate rows from county CSV sums so
            # that votes, party, vote_pct, and candidate_id are all accurate and
            # consistent with county_df / precinct_df.
            if _scraper_type == "v2" and not county_df.empty:
                # Preserve per-election metadata from search results
                meta_cols = [c for c in ["election_id", "election_year", "office",
                                         "office_level", "district", "election_type",
                                         "state", "url"]
                             if c in state_df.columns]
                election_meta = (
                    state_df[meta_cols]
                    .drop_duplicates(subset=["election_id"])
                    .reset_index(drop=True)
                )

                # Statewide vote totals = sum of locality (county) votes per candidate
                statewide = (
                    county_df
                    .groupby(
                        ["election_id", "candidate_id", "candidate", "party"],
                        as_index=False,
                    )["votes"]
                    .sum()
                )

                # vote_pct denominator: use total_votes (includes blanks/scattering/void)
                # when available, otherwise fall back to summing candidate votes.
                if "total_votes" in county_df.columns and county_df["total_votes"].notna().any():
                    elec_total = (
                        county_df
                        .drop_duplicates(subset=["election_id", "county_or_city"])
                        .groupby("election_id", as_index=False)["total_votes"]
                        .sum()
                    )
                    statewide = statewide.merge(elec_total, on="election_id", how="left")
                    statewide["vote_pct"] = (
                        (statewide["votes"] / statewide["total_votes"].replace(0, pd.NA) * 100)
                        .round(2)
                        .apply(lambda x: f"{x}%" if pd.notna(x) else "")
                    )
                else:
                    elec_total = (
                        statewide
                        .groupby("election_id", as_index=False)["votes"]
                        .sum()
                        .rename(columns={"votes": "total_votes"})
                    )
                    statewide = statewide.merge(elec_total, on="election_id", how="left")
                    statewide["vote_pct"] = (
                        (statewide["votes"] / statewide["total_votes"].replace(0, pd.NA) * 100)
                        .round(2)
                        .apply(lambda x: f"{x}%" if pd.notna(x) else "")
                    )

                # Winner flag: candidate with the most votes per election
                max_votes = statewide.groupby("election_id")["votes"].transform("max")
                statewide["winner"] = statewide["votes"] == max_votes

                # Combine CSV candidate rows with search-result election metadata
                state_df = statewide.merge(election_meta, on="election_id", how="left")
                state_df["state"] = state_name

                # Move state to first column
                cols = ["state"] + [c for c in state_df.columns if c != "state"]
                state_df = state_df[cols]

    else:
        # =============================
        # Classic states: use requests
        # =============================
        state_client = _make_client(
            state_key=state_key,
            base_url=base_url,
            sleep_s=_SLEEP_S_STATE,
            search_path=search_path,
            url_style=url_style,
        )

        rows = fetch_all_search_results(
            state_client,
            year_from=year,
            year_to=year,
            start_page=1,
            state_name=state_name,
        )
        state_df = rows_to_dataframe(rows, client=state_client)

        if state_df.empty:
            return state_df, pd.DataFrame(columns=COUNTY_COLS), pd.DataFrame(columns=_PRECINCT_COLS)

        # Ensure the state column is set consistently
        if "state" not in state_df.columns:
            state_df.insert(0, "state", state_name)
        else:
            state_df["state"] = state_name

        # Ensure a year column exists
        if "election_year" not in state_df.columns:
            state_df.insert(0, "election_year", year)

        # Only hit each election detail page once
        unique_elections = state_df[["state", "election_id", "url"]].drop_duplicates()

        if not _need_subunit:
            county_df   = pd.DataFrame(columns=COUNTY_COLS)
            precinct_df = pd.DataFrame(columns=_PRECINCT_COLS)
        else:
            n_unique = len(unique_elections)
            print(
                f"  [ElectionStats] Found {len(state_df):,} state-level rows; "
                f"fetching county/precinct for {n_unique:,} election(s)...",
                flush=True,
            )
            if parallel:
                county_df, precinct_df = build_county_and_precinct_dataframe_parallel(
                    state_df=unique_elections,
                    client_factory=_make_client_factory(
                        state_key=state_key,
                        base_url=base_url,
                        sleep_s=_SLEEP_S_COUNTY,
                        search_path=search_path,
                        url_style=url_style,
                    ),
                    max_workers=_MAX_WORKERS,
                )
            else:
                county_client = _make_client(
                    state_key=state_key,
                    base_url=base_url,
                    sleep_s=_SLEEP_S_COUNTY,
                    search_path=search_path,
                    url_style=url_style,
                )
                county_df = build_county_dataframe(
                    state_df=unique_elections,
                    client=county_client,
                )
                precinct_df = pd.DataFrame(columns=_PRECINCT_COLS)

    # Common cleanup for both paths
    if county_df.empty:
        return state_df, pd.DataFrame(columns=COUNTY_COLS), precinct_df

    # Ensure county df also uses the same state label
    if "state" not in county_df.columns:
        county_df.insert(0, "state", state_name)
    else:
        county_df["state"] = state_name

    if "election_year" not in county_df.columns:
        county_df.insert(0, "election_year", year)

    # Derive county_winner: top vote-getter per election+county
    if not county_df.empty and "votes" in county_df.columns:
        max_votes = county_df.groupby(
            ["election_id", "county_or_city"], dropna=False
        )["votes"].transform("max")
        county_df["county_winner"] = county_df["votes"] == max_votes

    # Derive vote_pct per (election_id, county_or_city): use total_votes when present
    # (v2 states include blanks/scattering in total_votes), else sum candidate votes.
    if not county_df.empty and "votes" in county_df.columns:
        if "total_votes" in county_df.columns and county_df["total_votes"].notna().any():
            denom = county_df["total_votes"].replace(0, pd.NA)
        else:
            denom = county_df.groupby(
                ["election_id", "county_or_city"], dropna=False
            )["votes"].transform("sum").replace(0, pd.NA)
        county_df["vote_pct"] = (
            (county_df["votes"] / denom * 100)
            .pipe(lambda s: pd.to_numeric(s, errors="coerce"))
            .round(2)
            .apply(lambda x: f"{x}%" if pd.notna(x) else "")
        )

    # Derive precinct_winner: top vote-getter per election+county+precinct
    if not precinct_df.empty and "votes" in precinct_df.columns:
        max_votes = precinct_df.groupby(
            ["election_id", "county", "precinct"], dropna=False
        )["votes"].transform("max")
        precinct_df["precinct_winner"] = precinct_df["votes"] == max_votes

    # Enrich county/precinct with election_type and party from state_df.
    # election_year is already on county_df (added above) but not precinct_df.
    if not state_df.empty:
        _url_cols = ["url"] if "url" in state_df.columns else []
        _type_party = (
            state_df[["election_id", "candidate_id", "election_type", "office", "office_level", "district", "party"] + _url_cols]
            .drop_duplicates(subset=["election_id", "candidate_id"])
        )
        _year_type_party = (
            state_df[["election_id", "candidate_id", "election_year", "election_type", "office", "office_level", "district", "party"] + _url_cols]
            .drop_duplicates(subset=["election_id", "candidate_id"])
        )
        if not county_df.empty:
            _join_keys = {"election_id", "candidate_id"}
            _overlap = [c for c in _type_party.columns if c not in _join_keys and c in county_df.columns]
            county_df = county_df.drop(columns=_overlap)
            county_df = county_df.merge(
                _type_party, on=["election_id", "candidate_id"], how="left"
            )
        if not precinct_df.empty:
            _join_keys = {"election_id", "candidate_id"}
            _overlap = [c for c in _year_type_party.columns if c not in _join_keys and c in precinct_df.columns]
            precinct_df = precinct_df.drop(columns=_overlap)
            precinct_df = precinct_df.merge(
                _year_type_party, on=["election_id", "candidate_id"], how="left"
            )

    # Normalize/ensure expected output columns exist
    for c in COUNTY_COLS:
        if c not in county_df.columns:
            county_df[c] = pd.NA
    county_df = county_df[COUNTY_COLS]

    for c in _PRECINCT_COLS:
        if c not in precinct_df.columns:
            precinct_df[c] = pd.NA
    if not precinct_df.empty:
        precinct_df = precinct_df[_PRECINCT_COLS]

    return state_df, county_df, precinct_df



def _join_county_with_state(county_all: pd.DataFrame, state_all: pd.DataFrame) -> pd.DataFrame:
    """
    Join county votes with statewide metadata on (state, election_id, candidate_id).
    Uses the ElectionSearchRow dataclass to validate expected statewide columns.
    """
    if county_all.empty or state_all.empty:
        return pd.DataFrame()

    # Validate join keys exist in both
    missing_left = [k for k in JOIN_KEYS if k not in county_all.columns]
    missing_right = [k for k in JOIN_KEYS if k not in state_all.columns]
    if missing_left or missing_right:
        raise ValueError(
            f"Missing join keys. county missing={missing_left}, state missing={missing_right}"
        )

    # Ensure statewide df includes expected model fields (future-proof)
    model_cols = [f.name for f in fields(ElectionSearchRow)]
    missing_model = [c for c in model_cols if c not in state_all.columns]
    if missing_model:
        raise ValueError(f"state_all missing expected model columns: {missing_model}")

    # One statewide row per (state, election_id, candidate_id)
    state_subset = state_all.drop_duplicates(subset=JOIN_KEYS)

    # Merge: many county rows to one statewide row
    joined = county_all.merge(
        state_subset,
        on=JOIN_KEYS,
        how="left",
        validate="many_to_one",
    )
    return joined


# ---------------------------
# Main runner (edit only 3 vars)
# ---------------------------
def main() -> None:
    # -------------------------
    # ✏️ EDIT THESE ONLY
    # -------------------------
    state = "south_carolina"  # Can be: virginia, massachusetts, colorado, south_carolina, new_mexico
    year_from = 2024
    year_to = 2024
    parallel = True
    # -------------------------

    state_key = _normalize_state(state)

    # Get state configuration from state_config.py
    config = get_state_config(state_key)
    base_url = config["base_url"]
    search_path = config["search_path"]
    scraping_method = config["scraping_method"]
    url_style = config.get("url_style", "path_params")

    print(f"State: {state_key}")
    print(f"Scraping method: {scraping_method}")
    print(f"URL style: {url_style}")
    print(f"Base URL: {base_url}")

    out = _ensure_outdir(f"{state_key}_outputs")

    state_frames: list[pd.DataFrame] = []
    county_frames: list[pd.DataFrame] = []

    for year in range(year_from, year_to + 1):
        print(f"\n=== Scraping {state_key} year {year} ===")

        state_df, county_df, precinct_df = scrape_one_year(
            state_key=state_key,
            state_name=state,
            base_url=base_url,
            search_path=search_path,
            year=year,
            parallel=parallel,
            scraping_method=scraping_method,
            url_style=url_style,
        )

        print(f"[{year}] state rows:  {len(state_df):,}")
        print(f"[{year}] county rows: {len(county_df):,}")

        if not state_df.empty:
            state_frames.append(state_df)

        if not county_df.empty:
            county_frames.append(county_df)

    state_all = _concat_or_empty(state_frames)
    county_all = _concat_or_empty(county_frames)

    if not state_all.empty:
        _save_outputs(
            state_all,
            out,
            base_name=f"{state_key}_state_{year_from}_{year_to}",
        )
        print(f"[OK] Saved state sample+RDS to {out}")

    if not county_all.empty:
        _save_outputs(
            county_all,
            out,
            base_name=f"{state_key}_county_{year_from}_{year_to}",
        )
        print(f"[OK] Saved county sample+RDS to {out}")

    joined_df = _join_county_with_state(
        county_all=county_all,
        state_all=state_all,
    )

    if not joined_df.empty:
        _save_outputs(
            joined_df,
            out,
            base_name=f"{state_key}_joined_{year_from}_{year_to}",
        )
        print(f"[OK] Saved joined sample+RDS to {out}")

    print("\n✅ Done.")




if __name__ == "__main__":
    main()
