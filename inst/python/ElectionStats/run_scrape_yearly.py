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
    build_county_dataframe_v2,
)
from ElectionStats.state_config import get_state_config
from ElectionStats.playwright_client import PlaywrightClient


# ---------------------------
# State configuration now managed in state_config.py
# Use get_state_config() to access state URLs and scraping methods
# ---------------------------


# ---------------------------
# Defaults (kept fixed for now)
# ---------------------------
_TIMEOUT_S = 60
_SLEEP_S_STATE = 0.10
_SLEEP_S_COUNTY = 0.10
_MAX_WORKERS = 6
_SAMPLE_N = 5000

JOIN_KEYS = ["state", "election_id", "candidate_id"]
COUNTY_COLS = ["state", "year", "election_id", "candidate_id", "county_or_city", "candidate_name", "votes"]


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


def _make_client(state_key: str, base_url: str, sleep_s: float, search_path: str) -> StateHttpClient:
    return StateHttpClient(
        state=state_key,
        base_url=base_url,
        config=HttpConfig(timeout_s=_TIMEOUT_S, sleep_s=sleep_s),
        search_path=search_path,
    )


def _make_client_factory(state_key: str, base_url: str, sleep_s: float, search_path: str):
    def factory() -> StateHttpClient:
        return _make_client(state_key, base_url, sleep_s, search_path)
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
    scraping_method: str,  # NEW: "requests" or "playwright"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (state_df, county_df) for a single year.
    Dispatches to requests-based or Playwright-based scraping.

    state_df: candidate-exploded search results (includes detail_url)
    county_df: long county votes (includes candidate_id)
    """

    if scraping_method == "playwright":
        # =============================
        # V2 states: use Playwright
        # =============================
        with PlaywrightClient(state_key, base_url) as pw_client:
            rows = fetch_all_search_results_v2(
                pw_client,
                year_from=year,
                year_to=year,
                state_name=state_name
            )
            state_df = rows_to_dataframe(rows, client=pw_client)

            if state_df.empty:
                return state_df, pd.DataFrame(columns=COUNTY_COLS)

            # Ensure state column
            if "state" not in state_df.columns:
                state_df.insert(0, "state", state_name)
            else:
                state_df["state"] = state_name

            if "year" not in state_df.columns:
                state_df.insert(0, "year", year)

            # Only hit each election detail page once
            unique_elections = state_df[["state", "election_id", "detail_url"]].drop_duplicates()

            # County scraping with Playwright (no parallel support yet for v2)
            county_df = build_county_dataframe_v2(
                state_df=unique_elections,
                playwright_client=pw_client,
            )

    else:
        # =============================
        # Classic states: use requests
        # =============================
        state_client = _make_client(
            state_key=state_key,
            base_url=base_url,
            sleep_s=_SLEEP_S_STATE,
            search_path=search_path,
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
            return state_df, pd.DataFrame(columns=COUNTY_COLS)

        # Ensure the state column is set consistently
        if "state" not in state_df.columns:
            state_df.insert(0, "state", state_name)
        else:
            state_df["state"] = state_name

        # Ensure a year column exists
        if "year" not in state_df.columns:
            state_df.insert(0, "year", year)

        # Only hit each election detail page once
        unique_elections = state_df[["state", "election_id", "detail_url"]].drop_duplicates()

        if parallel == True:
            county_df = build_county_dataframe_parallel(
                state_df=unique_elections,
                client_factory=_make_client_factory(
                    state_key=state_key,
                    base_url=base_url,
                    sleep_s=_SLEEP_S_COUNTY,
                    search_path=search_path,
                ),
                max_workers=_MAX_WORKERS,
            )
        else:
            county_client = _make_client(
                state_key=state_key,
                base_url=base_url,
                sleep_s=_SLEEP_S_COUNTY,
                search_path=search_path,
            )

            county_df = build_county_dataframe(
                state_df=unique_elections,
                client=county_client,
            )

    # Common cleanup for both paths
    if county_df.empty:
        return state_df, pd.DataFrame(columns=COUNTY_COLS)

    # Ensure county df also uses the same state label
    if "state" not in county_df.columns:
        county_df.insert(0, "state", state_name)
    else:
        county_df["state"] = state_name

    if "year" not in county_df.columns:
        county_df.insert(0, "year", year)

    # Normalize/ensure expected output columns exist
    for c in COUNTY_COLS:
        if c not in county_df.columns:
            county_df[c] = pd.NA
    county_df = county_df[COUNTY_COLS]

    return state_df, county_df

def _concat_or_empty(frames: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


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

    print(f"State: {state_key}")
    print(f"Scraping method: {scraping_method}")
    print(f"Base URL: {base_url}")

    out = _ensure_outdir(f"{state_key}_outputs")

    state_frames: list[pd.DataFrame] = []
    county_frames: list[pd.DataFrame] = []

    for year in range(year_from, year_to + 1):
        print(f"\n=== Scraping {state_key} year {year} ===")

        state_df, county_df = scrape_one_year(
            state_key=state_key,
            state_name=state,
            base_url=base_url,
            search_path=search_path,
            year=year,
            parallel=parallel,
            scraping_method=scraping_method,  # NEW: dispatch to correct scraper
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
