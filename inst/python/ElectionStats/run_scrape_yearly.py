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
    build_county_and_precinct_dataframe_v2,
    _PRECINCT_COLS,
)
from ElectionStats.electionStats_precinct_search import (
    build_county_and_precinct_dataframe_parallel,
)
from ElectionStats.state_config import get_state_config
from ElectionStats.playwright_client import PlaywrightClient
from df_utils import concat_or_empty as _concat_or_empty
from column_schemas import ES_STATE_COLS, ES_COUNTY_COLS, finalize_df, compute_vote_pct


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
COUNTY_COLS = ES_COUNTY_COLS


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
# Scraping sub-routines
# ---------------------------

def _rebuild_state_from_county_v2(
    county_df: pd.DataFrame,
    state_name: str,
    year: int,
    base_url: str,
) -> pd.DataFrame:
    """Aggregate county-level CSV data back up to statewide totals for v2 states."""
    from office_level_utils import lookup_office_level as _lookup_level

    group_cols = ["election_id", "candidate_id", "candidate", "party"]
    for col in ("election_type", "office", "district"):
        if col in county_df.columns:
            group_cols.append(col)

    statewide = county_df.groupby(group_cols, as_index=False, dropna=False)["votes"].sum()

    # vote_pct denominator: sum of per-county total_votes, de-duped to avoid double-counting.
    if "total_votes" in county_df.columns and county_df["total_votes"].notna().any():
        election_total = (
            county_df
            .drop_duplicates(subset=["election_id", "county_or_city"])
            .groupby("election_id", as_index=False)["total_votes"]
            .sum()
        )
    else:
        election_total = (
            statewide
            .groupby("election_id", as_index=False)["votes"]
            .sum()
            .rename(columns={"votes": "total_votes"})
        )
    statewide = statewide.merge(election_total, on="election_id", how="left")
    statewide = compute_vote_pct(statewide, ["election_id"], total_col="total_votes")

    max_votes = statewide.groupby("election_id")["votes"].transform("max")
    statewide["winner"] = statewide["votes"] == max_votes

    statewide["state"] = state_name
    statewide["election_year"] = year
    statewide["url"] = statewide["election_id"].apply(lambda eid: f"{base_url}/contest/{eid}")
    if "office" in statewide.columns:
        statewide["office_level"] = statewide["office"].apply(
            lambda o: _lookup_level(o, state_name) if pd.notna(o) and o else ""
        )

    return statewide


def _scrape_playwright_year(
    state_key: str,
    state_name: str,
    base_url: str,
    search_path: str,
    year: int,
    url_style: str,
    need_subunit: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fetch one year's data for a Playwright (React SPA) state."""
    from ElectionStats.state_config import get_state_config as _get_cfg
    state_cfg = _get_cfg(state_key)
    scraper_type = state_cfg.get("scraper_type", "v2")

    with PlaywrightClient(state_key, base_url) as pw_client:
        rows = fetch_all_search_results_v2(pw_client, year_from=year, year_to=year, state_name=state_name)
        state_df = rows_to_dataframe(rows, client=pw_client)

        if state_df.empty:
            return state_df, pd.DataFrame(columns=COUNTY_COLS), pd.DataFrame(columns=_PRECINCT_COLS)

        state_df["state"] = state_name
        if "election_year" not in state_df.columns:
            state_df["election_year"] = year

        unique_elections = state_df[["state", "election_id", "url"]].drop_duplicates()

        if not need_subunit:
            county_df = pd.DataFrame(columns=COUNTY_COLS)
            precinct_df = pd.DataFrame(columns=_PRECINCT_COLS)
        elif scraper_type == "classic":
            n_unique = len(unique_elections)
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
            # For states like NY whose CSV API is behind Cloudflare, route downloads
            # through the already-open browser session (which has Cloudflare clearance).
            # This also forces sequential execution (required for Playwright).
            meta_fetcher = (
                pw_client.fetch_contest_csv_and_type
                if state_cfg.get("csv_requires_browser", False)
                else None
            )
            county_df, precinct_df = build_county_and_precinct_dataframe_v2(
                state_df=unique_elections,
                base_url=base_url,
                max_workers=_MAX_WORKERS,
                meta_fetcher=meta_fetcher,
            )

        if scraper_type == "v2" and not county_df.empty:
            # Save per-election metadata from the Playwright search results.
            # county_df for non-NY v2 states (VA, SC, NM) doesn't carry
            # election_type/office/district, so the rebuilt statewide loses them.
            # Capture here and merge back after rebuild.
            _META_COLS = ["election_type", "office", "district", "office_level"]
            _search_meta = (
                state_df[[c for c in ["election_id"] + _META_COLS if c in state_df.columns]]
                .drop_duplicates("election_id")
            )

            state_df = _rebuild_state_from_county_v2(county_df, state_name, year, base_url)

            # Backfill any column that ended up entirely empty in statewide.
            for col in [c for c in _META_COLS if c in _search_meta.columns]:
                col_empty = (
                    col not in state_df.columns
                    or (state_df[col].isna() | state_df[col].astype(str).str.strip().eq("")).all()
                )
                if col_empty:
                    state_df = state_df.drop(columns=[col], errors="ignore").merge(
                        _search_meta[["election_id", col]], on="election_id", how="left"
                    )

    return state_df, county_df, precinct_df


def _scrape_classic_year(
    state_key: str,
    state_name: str,
    base_url: str,
    search_path: str,
    year: int,
    parallel: bool,
    url_style: str,
    need_subunit: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fetch one year's data for a classic (requests-based) state."""
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

    state_df["state"] = state_name
    if "election_year" not in state_df.columns:
        state_df["election_year"] = year

    if "vote_pct" in state_df.columns:
        state_df["vote_pct"] = (
            pd.to_numeric(state_df["vote_pct"].astype(str).str.rstrip("%"), errors="coerce")
            .astype("float64")
            .round(2)
        )
    state_df = compute_vote_pct(state_df, ["election_id"], fill_missing_only=True)

    unique_elections = state_df[["state", "election_id", "url"]].drop_duplicates()

    if not need_subunit:
        county_df = pd.DataFrame(columns=COUNTY_COLS)
        precinct_df = pd.DataFrame(columns=_PRECINCT_COLS)
    else:
        n_unique = len(unique_elections)
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
            county_df = build_county_dataframe(state_df=unique_elections, client=county_client)
            precinct_df = pd.DataFrame(columns=_PRECINCT_COLS)

    return state_df, county_df, precinct_df


def _postprocess_scrape_outputs(
    state_df: pd.DataFrame,
    county_df: pd.DataFrame,
    precinct_df: pd.DataFrame,
    state_name: str,
    year: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Apply winner flags, vote_pct, cross-df enrichment, and schema finalization."""
    if county_df.empty:
        return state_df, pd.DataFrame(columns=COUNTY_COLS), precinct_df

    county_df["state"] = state_name
    if "election_year" not in county_df.columns:
        county_df["election_year"] = year

    if "votes" in county_df.columns:
        max_votes = county_df.groupby(["election_id", "county_or_city"], dropna=False)["votes"].transform("max")
        county_df["county_winner"] = county_df["votes"] == max_votes

        total_col = (
            "total_votes"
            if "total_votes" in county_df.columns and county_df["total_votes"].notna().any()
            else None
        )
        county_df = compute_vote_pct(county_df, ["election_id", "county_or_city"], total_col=total_col)

    if not precinct_df.empty and "votes" in precinct_df.columns:
        precinct_df = compute_vote_pct(precinct_df, ["election_id", "county", "precinct"])
        max_votes = precinct_df.groupby(["election_id", "county", "precinct"], dropna=False)["votes"].transform("max")
        precinct_df["precinct_winner"] = precinct_df["votes"] == max_votes

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
        _join_keys = {"election_id", "candidate_id"}
        if not county_df.empty:
            _overlap = [c for c in _type_party.columns if c not in _join_keys and c in county_df.columns]
            county_df = county_df.drop(columns=_overlap).merge(_type_party, on=["election_id", "candidate_id"], how="left")
        if not precinct_df.empty:
            _overlap = [c for c in _year_type_party.columns if c not in _join_keys and c in precinct_df.columns]
            precinct_df = precinct_df.drop(columns=_overlap).merge(_year_type_party, on=["election_id", "candidate_id"], how="left")

    state_df = finalize_df(state_df, ES_STATE_COLS)
    county_df = finalize_df(county_df, COUNTY_COLS)
    precinct_df = finalize_df(precinct_df, _PRECINCT_COLS) if not precinct_df.empty else pd.DataFrame(columns=_PRECINCT_COLS)

    return state_df, county_df, precinct_df


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
    need_subunit = level != "state"

    if scraping_method == "playwright":
        state_df, county_df, precinct_df = _scrape_playwright_year(
            state_key=state_key,
            state_name=state_name,
            base_url=base_url,
            search_path=search_path,
            year=year,
            url_style=url_style,
            need_subunit=need_subunit,
        )
    else:
        state_df, county_df, precinct_df = _scrape_classic_year(
            state_key=state_key,
            state_name=state_name,
            base_url=base_url,
            search_path=search_path,
            year=year,
            parallel=parallel,
            url_style=url_style,
            need_subunit=need_subunit,
        )

    if state_df.empty:
        return state_df, pd.DataFrame(columns=COUNTY_COLS), pd.DataFrame(columns=_PRECINCT_COLS)

    if state_key == "colorado" and "candidate_id" in state_df.columns:
        state_df    = state_df[state_df["candidate_id"] <= 9]
        if not county_df.empty and "candidate_id" in county_df.columns:
            county_df   = county_df[county_df["candidate_id"] <= 9]
        if not precinct_df.empty and "candidate_id" in precinct_df.columns:
            precinct_df = precinct_df[precinct_df["candidate_id"] <= 9]

    if state_key == "idaho" and "candidate_id" in state_df.columns:
        state_df    = state_df[state_df["candidate_id"] <= 8]
        if not county_df.empty and "candidate_id" in county_df.columns:
            county_df   = county_df[county_df["candidate_id"] <= 8]
        if not precinct_df.empty and "candidate_id" in precinct_df.columns:
            precinct_df = precinct_df[precinct_df["candidate_id"] <= 8]

    return _postprocess_scrape_outputs(state_df, county_df, precinct_df, state_name, year)



def _join_county_with_state(county_all: pd.DataFrame, state_all: pd.DataFrame) -> pd.DataFrame:
    """
    Join county votes with statewide metadata on (state, election_id, candidate_id).
    Uses the ElectionSearchRow dataclass to validate expected statewide columns.
    """
    if county_all.empty or state_all.empty:
        return pd.DataFrame(columns=COUNTY_COLS)

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
