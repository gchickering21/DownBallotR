# ElectionStats/run_scrape_yearly.py
from __future__ import annotations

from dataclasses import fields
from pathlib import Path
from typing import Callable

import pandas as pd
import pyreadr  

from ElectionStats.electionStats_client import HttpConfig, StateHttpClient
from ElectionStats.electionStats_models import ElectionSearchRow
from ElectionStats.electionStats_search import fetch_all_search_results, rows_to_dataframe
from ElectionStats.electionStats_county_search  import build_county_dataframe_parallel, build_county_dataframe


# ---------------------------
# Edit-friendly registry
# ---------------------------
STATE_REGISTRY: dict[str, dict[str, str]] = {
    "virginia": {"base_url": "https://historical.elections.virginia.gov/elections", "search_path": "/search"},
    "massachusetts": {"base_url": "https://electionstats.state.ma.us/elections", "search_path": "/search"},
    # Colorado: do NOT include /search
    "colorado": {"base_url": "https://co.elstats2.civera.com/eng/contests", "search_path": ""},
}


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
    state_name: str,   # ✅ added
    base_url: str,
    search_path: str,
    year: int,
    parallel: True
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (state_df, county_df) for a single year.

    state_df: candidate-exploded search results (includes detail_url)
    county_df: long county votes (includes candidate_id)
    """
    # Use state_key for registry lookup / file naming, but carry state_name through
    # if you want a nicer label (e.g., "Colorado") downstream.
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

    # ✅ ensure the state column is set consistently (useful if parsers differ)
    if "state" not in state_df.columns:
        state_df.insert(0, "state", state_name)
    else:
        state_df["state"] = state_name

    # Ensure a year column exists (useful when concatenating multiple years)
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

    if county_df.empty:
        return state_df, pd.DataFrame(columns=COUNTY_COLS)

    # ✅ ensure county df also uses the same state label
    if "state" not in county_df.columns:
        county_df.insert(0, "state", state_name)
    else:
        county_df["state"] = state_name

    if "year" not in county_df.columns:
        county_df.insert(0, "year", year)

    # Normalize/ensure expected output columns exist (helpful if upstream changes)
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
    state = "massachusetts"
    year_from = 2026
    year_to = 2026
    parallel = True
    # -------------------------

    state_key = _normalize_state(state)

    if state_key not in STATE_REGISTRY:
        raise ValueError(
            f"Unknown state {state!r}. Available: {sorted(STATE_REGISTRY.keys())}"
        )

    cfg = STATE_REGISTRY[state_key]
    base_url = cfg["base_url"]
    search_path = cfg.get("search_path", "/search")

    out = _ensure_outdir(f"{state_key}_outputs")

    state_frames: list[pd.DataFrame] = []
    county_frames: list[pd.DataFrame] = []

    for year in range(year_from, year_to + 1):
        print(f"\n=== Scraping {state_key} year {year} ===")

        # ✅ pass state_name as well
        state_df, county_df = scrape_one_year(
            state_key=state_key,
            state_name=state,          
            base_url=base_url,
            search_path=search_path,
            year=year,
            parallel = parallel
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
