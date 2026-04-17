#!/usr/bin/env python3
"""
download_all_data.py
====================
Bulk-download script for all DownBallotR data sources.

Usage (run from inst/python/):
    python download_all_data.py [options]

Sections:
    election_stats      ElectionStats (VT, VA, CO, MA, NH, ID, NY, NM, SC)
    nc                  North Carolina (NC State Board of Elections, 2000-2025)
    georgia             Georgia Secretary of State election results (2012-present)
    utah                Utah election results (2023-present)
    connecticut         Connecticut CTEMS election results (2016-present)
    louisiana           Louisiana SOS Graphical election results (1982-present)
    indiana             Indiana General Election results (2019-present)
    all                 All of the above (default)

Options:
    --output-dir PATH       Root output directory (default: <repo>/data/)
    --section NAME          Run only one section (default: all)
    --dry-run               Print tasks without scraping

ElectionStats-specific options:
    --es-year-from INT      Override start year for all election_stats states
    --es-year-to   INT      Override end year for all election_stats states

Georgia-specific options:
    --ga-year-from INT      First year to download (default: 2012)
    --ga-year-to   INT      Last year to download (default: current year)
    --ga-level     LEVEL    State-only, county-only, or both (default: all)
                            Choices: all, state, county
    --ga-vote-methods       Also capture per-contest vote-method breakdowns
                            (Advance in Person / Election Day / Absentee / Provisional).
                            Adds extra Playwright clicks per page; significantly
                            slower but produces richer data.
    --ga-county-workers INT Parallel Chromium browsers for GA county scraping (default: 2)

Utah-specific options:
    --ut-year-from INT      First year to download (default: 2023)
    --ut-year-to   INT      Last year to download (default: current year)
    --ut-level     LEVEL    State-only, county-only, or both (default: all)
                            Choices: all, state, county
    --ut-vote-methods       Also capture per-contest vote-method breakdowns.
    --ut-county-workers INT Parallel Chromium browsers for UT county scraping (default: 2)

Connecticut-specific options:
    --ct-year-from INT      First year to download (default: 2000)
    --ct-year-to   INT      Last year to download (default: current year)
    --ct-level     LEVEL    What to download: all, state, or town (default: all)
    --ct-town-workers INT   Parallel Chromium browsers for town scraping (default: 2)

Output layout:
    data/
      election_stats/
        {state}/{state}_{year_from}_{year_to}_state.csv
        {state}/{state}_{year_from}_{year_to}_county.csv
        {state}/{state}_{year_from}_{year_to}_precinct.csv  (CO, MA, ID, VA, NH, VT only)
      northcarolina/
        nc_{year_from}_{year_to}_precinct.csv
        nc_{year_from}_{year_to}_county.csv
        nc_{year_from}_{year_to}_state.csv
      georgia/
        ga_{year}_state.csv               (statewide candidate totals)
        ga_{year}_county.csv              (per-county candidate totals)
        ga_{year}_vote_method_state.csv   (--ga-vote-methods: per-method statewide)
        ga_{year}_vote_method_county.csv  (--ga-vote-methods: per-method by county)
      utah/
        ut_{year}_state.csv               (statewide candidate totals)
        ut_{year}_county.csv              (per-county candidate totals)
        ut_{year}_vote_method_state.csv   (--ut-vote-methods: per-method statewide)
        ut_{year}_vote_method_county.csv  (--ut-vote-methods: per-method by county)
      connecticut/
        ct_{year}_state.csv               (statewide totals: federal + aggregated state/local)
        ct_{year}_town.csv                (per-town candidate totals with election_level)
      louisiana/
        la_{year}_state.csv               (statewide tab results: all non-Parish tabs combined)
        la_{year}_parish.csv              (per-parish candidate totals)

Louisiana-specific options:
    --la-year-from INT      First year to download (default: 2024)
    --la-year-to   INT      Last year to download (default: 2024)
                            Full history goes back to 1982; use --la-year-from 1982
                            once the scraper is validated.
    --la-level     LEVEL    State-only, parish-only, or both (default: all)
                            Choices: all, state, parish
    --la-parish-workers INT Parallel Chromium browsers for parish scraping (default: 2)
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable

import pandas as pd

# Ensure inst/python/ is on the path when invoked from any directory
sys.path.insert(0, str(Path(__file__).parent))

import registry


# ── Constants ──────────────────────────────────────────────────────────────────

CURRENT_YEAR = datetime.date.today().year

# States that produce precinct-level output.
# Classic (requests-based): CO, MA, ID — precinct rows on HTML detail pages.
# v2 (Playwright + CSV API): SC, NM, VA — precinct rows from download_contest CSV.
# NH and VT only have state + county levels; NY has no precinct data at all.
ELECTION_STATS_PRECINCT_STATES: frozenset[str] = frozenset({
    "colorado", "massachusetts", "idaho",
    "south_carolina", "new_mexico", "virginia",
})

# States that produce only a state-level file (no county output).
ELECTION_STATS_NO_COUNTY_STATES: frozenset[str] = frozenset({
    "new_york",
})

# Year ranges mirror registry._YEAR_RANGES / STATE_CONFIGS
ELECTION_STATS_STATES: dict[str, tuple[int, int]] = {
    "vermont":        (1789, 2024),
    "virginia":       (1789, 2025),
    "colorado":       (1902, 2024),
    "massachusetts":  (1970, 2026),
    "new_hampshire":  (1970, 2024),
    "idaho":          (1990, 2024),
    "new_york":       (1994, 2024),
    "new_mexico":     (2000, 2024),
    "south_carolina": (2008, 2025),
}

NC_YEAR_RANGE              = (2000, 2025)
IN_YEAR_RANGE              = (2019, CURRENT_YEAR)
GA_YEAR_RANGE              = (2012, CURRENT_YEAR)
UT_YEAR_RANGE              = (2023, CURRENT_YEAR)
CT_YEAR_RANGE              = (2016, CURRENT_YEAR)
# Default to 2024 only until the scraper is validated; full history goes back to 1982.
LA_YEAR_RANGE              = (1982, 2024)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _slug(s: str) -> str:
    """Convert a label to a filesystem-safe slug."""
    return s.lower().replace(" ", "_").replace("/", "_")


_FORMULA_CHARS = ("=", "+", "-", "@", "\t", "\r")


def _sanitize_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Escape string cells that start with formula-injection characters.

    Spreadsheet apps (Excel, Google Sheets) treat cells starting with
    =, +, -, @, tab, or CR as formulas.  Prepending a single quote
    prevents execution without altering the visible cell value.
    """
    df = df.copy()
    for col in df.select_dtypes(include="str").columns:
        df[col] = df[col].apply(
            lambda x: f"'{x}" if isinstance(x, str) and x[:1] in _FORMULA_CHARS else x
        )
    return df


def _save(df: pd.DataFrame, path: Path) -> None:
    """Write *df* to *path* as a CSV file, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    _sanitize_csv(df).to_csv(path, index=False)
    # Show path relative to the repo root (4 levels up from inst/python/)
    try:
        display = path.relative_to(Path(__file__).parent.parent.parent.parent)
    except ValueError:
        display = path
    print(f"  → saved {len(df):,} rows  →  {display}")


def _is_valid_csv(path: Path) -> bool:
    """Return True if *path* exists and contains non-trivial content.

    A file that exists but is empty or header-only (< 50 bytes) is treated as
    corrupt — it is deleted so the next run re-scrapes rather than silently
    skipping.  This handles the case where a previous run crashed mid-write.
    """
    if not path.exists():
        return False
    size = path.stat().st_size
    if size < 50:
        print(f"  ⚠ file exists but appears empty/corrupt ({size} bytes): {path.name} — will re-scrape")
        path.unlink()
        return False
    return True


def _already_exists(path: Path) -> bool:
    """Return True and print a skip message when *path* is a valid CSV."""
    if _is_valid_csv(path):
        print(f"  ↷ already exists, skipping: {path.name}")
        return True
    return False


def _all_valid(paths: "dict[str, Path]") -> bool:
    """Return True when every path in *paths* is a valid (non-empty) CSV."""
    return all(_is_valid_csv(p) for p in paths.values())


def _run_task(
    label: str,
    path: Path,
    fn: Callable[[], "pd.DataFrame | dict[str, pd.DataFrame]"],
    dry_run: bool,
) -> bool:
    """
    Run *fn()*, save the result to *path*, and return True on success.

    - Skips silently when *path* already exists.
    - When *fn()* returns a dict (election_stats level='all'), saves each
      value as a separate file using the dict key as a suffix.
    - Prints a traceback and returns False on any exception.
    """
    print(f"\n[{label}]")
    if _already_exists(path):
        return True
    if dry_run:
        print(f"  (dry-run) → {path.name}")
        return True

    try:
        result = fn()
    except Exception:
        print("  ✗ ERROR during scrape:")
        traceback.print_exc()
        return False

    try:
        if isinstance(result, dict):
            # election_stats level='all' → {"state": df, "county": df[, "precinct": df]}
            for key, df in result.items():
                suffix_path = path.parent / f"{path.stem}_{key}{path.suffix}"
                if df.empty:
                    print(f"  ⚠ empty result for key '{key}' — skipping write")
                else:
                    _save(df, suffix_path)
        elif isinstance(result, pd.DataFrame):
            if result.empty:
                print("  ⚠ empty result — skipping write")
            else:
                _save(result, path)
        else:
            print(f"  ✗ unexpected result type {type(result).__name__} — skipping")
            return False
    except Exception:
        print("  ✗ ERROR while saving result:")
        traceback.print_exc()
        return False

    return True


def _run_tasks(
    tasks: list[tuple[str, Path, Callable]],
    *,
    dry_run: bool,
    workers: int = 1,
) -> list[bool]:
    """Run a list of (label, path, fn) tasks, optionally in parallel.

    workers=1 (default) runs sequentially.
    workers>1 uses ThreadPoolExecutor.

    Note: Playwright-based scrapers (SC, NM, NY, VA)
    are not thread-safe — use workers=1 for those sections.
    """
    if workers <= 1:
        return [_run_task(label, path, fn, dry_run) for label, path, fn in tasks]

    results = [False] * len(tasks)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_idx = {
            executor.submit(_run_task, label, path, fn, dry_run): i
            for i, (label, path, fn) in enumerate(tasks)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except Exception:
                traceback.print_exc()
                results[idx] = False
    return results


def _save_scrape_result(
    result: "pd.DataFrame | dict",
    paths: "dict[str, Path]",
    level: str,
) -> bool:
    """Save a scrape result (DataFrame or dict of DataFrames) to *paths*."""
    try:
        if isinstance(result, dict):
            for key, df in result.items():
                if key not in paths:
                    continue
                if df.empty:
                    print(f"  ⚠ empty result for '{key}' — skipping write")
                else:
                    _save(df, paths[key])
        elif isinstance(result, pd.DataFrame):
            if result.empty:
                print("  ⚠ empty result — skipping write")
            elif level in paths:
                _save(result, paths[level])
        else:
            print(f"  ✗ unexpected result type {type(result).__name__} — skipping")
            return False
    except Exception:
        print("  ✗ ERROR while saving result:")
        traceback.print_exc()
        return False
    return True


def _download_yearly(
    output_dir: Path,
    *,
    base_subdir: str,
    source: str,
    year_from: int,
    year_to: int,
    level: str,
    dry_run: bool,
    make_label: "Callable[[int], str]",
    make_paths: "Callable[[Path, int], dict[str, Path]]",
    scrape_kwargs: "dict | None" = None,
) -> "list[bool]":
    """Year-loop boilerplate shared by all single-state yearly downloaders."""
    base = output_dir / base_subdir
    scrape_kwargs = scrape_kwargs or {}
    results: list[bool] = []

    for year in range(year_from, year_to + 1):
        print(f"\n[{make_label(year)}]")
        paths = make_paths(base, year)

        if _all_valid(paths):
            print("  ↷ all output files exist, skipping")
            results.append(True)
            continue

        if dry_run:
            for key, p in paths.items():
                status = "exists" if p.exists() else "would write"
                print(f"  (dry-run) [{key}] → {p.name}  ({status})")
            results.append(True)
            continue

        try:
            result = registry.scrape(
                source, year_from=year, year_to=year, level=level, **scrape_kwargs
            )
        except Exception:
            print("  ✗ ERROR during scrape:")
            traceback.print_exc()
            results.append(False)
            continue

        results.append(_save_scrape_result(result, paths, level))

    return results


# ── Section downloaders ────────────────────────────────────────────────────────

def download_election_stats(
    output_dir: Path,
    *,
    dry_run: bool,
    state: "str | None" = None,
    workers: int = 1,
    es_year_from: "int | None" = None,
    es_year_to: "int | None" = None,
    **_,
) -> list[bool]:
    """
    ElectionStats: one download per state, full year range, level='all'.
    Each state goes into its own subdirectory:
      data/election_stats/{state}/{state}_{year_from}_{year_to}_state.csv
      data/election_stats/{state}/{state}_{year_from}_{year_to}_county.csv
      data/election_stats/{state}/{state}_{year_from}_{year_to}_precinct.csv  (CO, MA, ID, VA, NH, VT)

    Pass state= to restrict to a single state (e.g. state='new_york').
    Pass es_year_from/es_year_to to override the default year range.
    Note: SC, NM, NY, VA use Playwright — set workers=1 for those states.
    """
    # Normalize the filter to the same slug format used as dict keys
    state_filter = _slug(state) if state else None
    if state_filter and state_filter not in ELECTION_STATS_STATES:
        valid = ", ".join(sorted(ELECTION_STATS_STATES))
        raise ValueError(f"Unknown state {state!r}. Available: {valid}")

    states_to_run = (
        {state_filter: ELECTION_STATS_STATES[state_filter]}
        if state_filter
        else ELECTION_STATS_STATES
    )

    def _scrape_one(s: str, year_from: int, year_to: int) -> bool:
        state_dir   = output_dir / "election_stats" / s
        stem        = f"{s}_{year_from}_{year_to}"
        state_path  = state_dir / f"{stem}_state.csv"
        county_path = state_dir / f"{stem}_county.csv"
        label       = f"ElectionStats  {s}  {year_from}–{year_to}"

        precinct_path = state_dir / f"{stem}_precinct.csv"
        has_county   = s not in ELECTION_STATS_NO_COUNTY_STATES
        has_precinct = s in ELECTION_STATS_PRECINCT_STATES

        print(f"\n[{label}]")
        state_ok  = _is_valid_csv(state_path)
        county_ok = (not has_county) or _is_valid_csv(county_path)
        if state_ok and county_ok:
            files = f"{stem}_state.csv"
            if has_county:
                files += f", {stem}_county.csv"
            print(f"  ↷ already exists, skipping: {files}")
            return True

        if dry_run:
            print(f"  (dry-run) → {stem}_state.csv, {stem}_county.csv[, {stem}_precinct.csv]")
            return True

        try:
            result = registry.scrape(
                "election_stats",
                state=s, year_from=year_from, year_to=year_to,
                level="all", parallel=True,
            )
        except Exception:
            print("  ✗ ERROR during scrape:")
            traceback.print_exc()
            return False

        try:
            if not isinstance(result, dict):
                print(f"  ✗ unexpected result type {type(result).__name__} — skipping")
                return False
            for key, df in result.items():
                out_path = state_dir / f"{stem}_{key}.csv"
                if df.empty:
                    print(f"  ⚠ empty result for key '{key}' — skipping write")
                else:
                    _save(df, out_path)
        except Exception:
            print("  ✗ ERROR while saving result:")
            traceback.print_exc()
            return False

        return True

    jobs = [
        (s,
         es_year_from if es_year_from is not None else default_from,
         es_year_to   if es_year_to   is not None else default_to)
        for s, (default_from, default_to) in states_to_run.items()
    ]

    if workers <= 1:
        return [_scrape_one(s, yf, yt) for s, yf, yt in jobs]

    results = [False] * len(jobs)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_idx = {
            executor.submit(_scrape_one, s, yf, yt): i
            for i, (s, yf, yt) in enumerate(jobs)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except Exception:
                traceback.print_exc()
                results[idx] = False
    return results


def download_nc(
    output_dir: Path,
    *,
    dry_run: bool,
    nc_year_from: int = NC_YEAR_RANGE[0],
    nc_year_to: int = NC_YEAR_RANGE[1],
    **_,
) -> "list[bool]":
    """NC State Board of Elections, all three aggregation levels in a single pipeline run."""
    from datetime import date

    base  = output_dir / "northcarolina_results"
    stem  = f"nc_{nc_year_from}_{nc_year_to}"
    label = f"NC State Board of Elections  {nc_year_from}–{nc_year_to}  (precinct + county + state)"
    paths = {
        "precinct": base / f"{stem}_precinct.csv",
        "county":   base / f"{stem}_county.csv",
        "state":    base / f"{stem}_state.csv",
    }

    print(f"\n[{label}]")
    if _all_valid(paths):
        print("  ↷ all three output files exist, skipping")
        return [True]
    if dry_run:
        for key, p in paths.items():
            status = "exists" if p.exists() else "would write"
            print(f"  (dry-run) [{key}] → {p.name}  ({status})")
        return [True]

    try:
        from NorthCarolina.pipeline import NcElectionPipeline

        start = date(nc_year_from, 1, 1)
        end   = date(nc_year_to, 12, 31)
        pipeline = NcElectionPipeline()
        precinct_df, county_df, state_df = pipeline.run(
            start_date=start, end_date=end,
            min_supported_date=start, max_supported_date=end,
        )
    except Exception:
        print("  ✗ ERROR during scrape:")
        traceback.print_exc()
        return [False]

    ok = True
    for key, df in [("precinct", precinct_df), ("county", county_df), ("state", state_df)]:
        try:
            if df.empty:
                print(f"  ⚠ empty result for '{key}' — skipping write")
            else:
                _save(df, paths[key])
        except Exception:
            print(f"  ✗ ERROR saving '{key}':")
            traceback.print_exc()
            ok = False
    return [ok]


def download_indiana(
    output_dir: Path,
    *,
    dry_run: bool,
    in_year_from: int = IN_YEAR_RANGE[0],
    in_year_to: int = IN_YEAR_RANGE[1],
    in_level: str = "all",
    **_,
) -> "list[bool]":
    """Indiana General Election results, one set of files per year."""
    def make_label(year: int) -> str:
        return f"Indiana  {year}General  level={in_level}"

    def make_paths(base: Path, year: int) -> "dict[str, Path]":
        paths: dict[str, Path] = {}
        if in_level in ("all", "state"):
            paths["state"]  = base / f"in_{year}_state.csv"
        if in_level in ("all", "county"):
            paths["county"] = base / f"in_{year}_county.csv"
        return paths

    return _download_yearly(
        output_dir,
        base_subdir="indiana",
        source="indiana_results",
        year_from=in_year_from,
        year_to=in_year_to,
        level=in_level,
        dry_run=dry_run,
        make_label=make_label,
        make_paths=make_paths,
    )


def download_georgia(
    output_dir: Path,
    *,
    dry_run: bool,
    ga_year_from: int = GA_YEAR_RANGE[0],
    ga_year_to: int = GA_YEAR_RANGE[1],
    ga_level: str = "all",
    ga_vote_methods: bool = False,
    ga_county_workers: int = 2,
    **_,
) -> "list[bool]":
    """Georgia Secretary of State election results, one set of files per year."""
    vm_suffix = " +vote-methods" if ga_vote_methods else ""

    def make_label(year: int) -> str:
        return f"Georgia SOS  {year}  level={ga_level}{vm_suffix}"

    def make_paths(base: Path, year: int) -> "dict[str, Path]":
        paths: dict[str, Path] = {}
        if ga_level in ("all", "state"):
            paths["state"]  = base / f"ga_{year}_state.csv"
        if ga_level in ("all", "county"):
            paths["county"] = base / f"ga_{year}_county.csv"
        if ga_vote_methods:
            if ga_level in ("all", "state"):
                paths["vote_method_state"]  = base / f"ga_{year}_vote_method_state.csv"
            if ga_level in ("all", "county"):
                paths["vote_method_county"] = base / f"ga_{year}_vote_method_county.csv"
        return paths

    return _download_yearly(
        output_dir,
        base_subdir="georgia",
        source="georgia_results",
        year_from=ga_year_from,
        year_to=ga_year_to,
        level=ga_level,
        dry_run=dry_run,
        make_label=make_label,
        make_paths=make_paths,
        scrape_kwargs=dict(
            include_vote_methods=ga_vote_methods,
            max_county_workers=ga_county_workers,
        ),
    )


def download_utah(
    output_dir: Path,
    *,
    dry_run: bool,
    ut_year_from: int = UT_YEAR_RANGE[0],
    ut_year_to: int = UT_YEAR_RANGE[1],
    ut_level: str = "all",
    ut_vote_methods: bool = False,
    ut_county_workers: int = 2,
    **_,
) -> "list[bool]":
    """Utah election results, one set of files per year."""
    vm_suffix = " +vote-methods" if ut_vote_methods else ""

    def make_label(year: int) -> str:
        return f"Utah  {year}  level={ut_level}{vm_suffix}"

    def make_paths(base: Path, year: int) -> "dict[str, Path]":
        paths: dict[str, Path] = {}
        if ut_level in ("all", "state"):
            paths["state"]  = base / f"ut_{year}_state.csv"
        if ut_level in ("all", "county"):
            paths["county"] = base / f"ut_{year}_county.csv"
        if ut_vote_methods:
            if ut_level in ("all", "state"):
                paths["vote_method_state"]  = base / f"ut_{year}_vote_method_state.csv"
            if ut_level in ("all", "county"):
                paths["vote_method_county"] = base / f"ut_{year}_vote_method_county.csv"
        return paths

    return _download_yearly(
        output_dir,
        base_subdir="utah",
        source="utah_results",
        year_from=ut_year_from,
        year_to=ut_year_to,
        level=ut_level,
        dry_run=dry_run,
        make_label=make_label,
        make_paths=make_paths,
        scrape_kwargs=dict(
            include_vote_methods=ut_vote_methods,
            max_county_workers=ut_county_workers,
        ),
    )


def download_connecticut(
    output_dir: Path,
    *,
    dry_run: bool,
    ct_year_from: int = CT_YEAR_RANGE[0],
    ct_year_to: int = CT_YEAR_RANGE[1],
    ct_level: str = "all",
    ct_town_workers: int = 2,
    **_,
) -> "list[bool]":
    """Connecticut CTEMS election results, one set of files per year."""
    def make_label(year: int) -> str:
        return f"Connecticut CTEMS  {year}  level={ct_level}"

    def make_paths(base: Path, year: int) -> "dict[str, Path]":
        paths: dict[str, Path] = {}
        if ct_level in ("all", "state"):
            paths["state"] = base / f"ct_{year}_state.csv"
        if ct_level in ("all", "town"):
            paths["town"]  = base / f"ct_{year}_town.csv"
        return paths

    return _download_yearly(
        output_dir,
        base_subdir="connecticut",
        source="connecticut_results",
        year_from=ct_year_from,
        year_to=ct_year_to,
        level=ct_level,
        dry_run=dry_run,
        make_label=make_label,
        make_paths=make_paths,
        scrape_kwargs=dict(max_town_workers=ct_town_workers),
    )


def download_louisiana(
    output_dir: Path,
    *,
    dry_run: bool,
    la_year_from: int = LA_YEAR_RANGE[0],
    la_year_to: int = LA_YEAR_RANGE[1],
    la_level: str = "all",
    la_parish_workers: int = 2,
    **_,
) -> "list[bool]":
    """Louisiana SOS Graphical election results, one set of files per year."""
    def make_label(year: int) -> str:
        return f"Louisiana  {year}  level={la_level}"

    def make_paths(base: Path, year: int) -> "dict[str, Path]":
        paths: dict[str, Path] = {}
        if la_level in ("all", "state"):
            paths["state"]  = base / f"la_{year}_state.csv"
        if la_level in ("all", "parish"):
            paths["parish"] = base / f"la_{year}_parish.csv"
        return paths

    return _download_yearly(
        output_dir,
        base_subdir="louisiana",
        source="louisiana_results",
        year_from=la_year_from,
        year_to=la_year_to,
        level=la_level,
        dry_run=dry_run,
        make_label=make_label,
        make_paths=make_paths,
        scrape_kwargs=dict(max_parish_workers=la_parish_workers),
    )


# ── Trial run ─────────────────────────────────────────────────────────────────

# One representative recent year per section used by trial_run().
TRIAL_YEARS: dict[str, int] = {
    "vermont":        2024,
    "virginia":       2024,
    "colorado":       2024,
    "massachusetts":  2024,
    "new_hampshire":  2024,
    "idaho":          2024,
    "new_york":       2024,
    "new_mexico":     2024,
    "south_carolina": 2024,
    "nc":             2025,
    "indiana":        2024,
    "georgia":        2023,
    "utah":           2024,
    "connecticut":    2023,
    "louisiana":      2026,
}


def trial_run(output_dir: Path, *, dry_run: bool = False, workers: int = os.cpu_count() or 1) -> list[bool]:
    """Run one year for every state/section as a quick end-to-end smoke test.

    Uses a fixed recent year per section (see TRIAL_YEARS).  Output files are
    written under ``output_dir/trial/`` so they never collide with a full run.
    All sections use ``level='all'`` to exercise the full pipeline.
    """
    trial_dir = output_dir / "trial"
    print(f"Trial output directory: {trial_dir}")
    all_results: list[bool] = []

    # ── ElectionStats ──────────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SECTION: ELECTION_STATS (trial — one year each)")
    print(f"{'─'*70}")
    for state_key, year in {
        k: TRIAL_YEARS[k] for k in ELECTION_STATS_STATES
    }.items():
        results = download_election_stats(
            trial_dir,
            dry_run=dry_run,
            state=state_key,
            workers=1,
            es_year_from=year,
            es_year_to=year,
        )
        all_results.extend(results)

    # ── North Carolina ─────────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SECTION: NC (trial)")
    print(f"{'─'*70}")
    year = TRIAL_YEARS["nc"]
    all_results.extend(download_nc(trial_dir, dry_run=dry_run, nc_year_from=year, nc_year_to=year))

    # ── Indiana ────────────────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SECTION: INDIANA (trial)")
    print(f"{'─'*70}")
    all_results.extend(download_indiana(
        trial_dir, dry_run=dry_run,
        in_year_from=TRIAL_YEARS["indiana"], in_year_to=TRIAL_YEARS["indiana"],
    ))

    # ── Georgia ────────────────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SECTION: GEORGIA (trial)")
    print(f"{'─'*70}")
    all_results.extend(download_georgia(
        trial_dir, dry_run=dry_run,
        ga_year_from=TRIAL_YEARS["georgia"], ga_year_to=TRIAL_YEARS["georgia"],
        ga_county_workers=workers,
    ))

    # ── Utah ───────────────────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SECTION: UTAH (trial)")
    print(f"{'─'*70}")
    all_results.extend(download_utah(
        trial_dir, dry_run=dry_run,
        ut_year_from=TRIAL_YEARS["utah"], ut_year_to=TRIAL_YEARS["utah"],
        ut_county_workers=workers,
    ))

    # ── Connecticut ────────────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SECTION: CONNECTICUT (trial)")
    print(f"{'─'*70}")
    all_results.extend(download_connecticut(
        trial_dir, dry_run=dry_run,
        ct_year_from=TRIAL_YEARS["connecticut"], ct_year_to=TRIAL_YEARS["connecticut"],
        ct_town_workers=workers,
    ))

    # ── Louisiana ──────────────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SECTION: LOUISIANA (trial)")
    print(f"{'─'*70}")
    all_results.extend(download_louisiana(
        trial_dir, dry_run=dry_run,
        la_year_from=TRIAL_YEARS["louisiana"], la_year_to=TRIAL_YEARS["louisiana"],
        la_parish_workers=workers,
    ))

    return all_results


# ── Dispatch table ─────────────────────────────────────────────────────────────

SECTIONS: dict[str, Callable] = {
    "election_stats":   download_election_stats,
    "nc":               download_nc,
    "georgia":          download_georgia,
    "utah":             download_utah,
    "indiana":          download_indiana,
    "connecticut":      download_connecticut,
    "louisiana":        download_louisiana,
}


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    default_output = str(Path(__file__).parent.parent.parent / "data")

    parser = argparse.ArgumentParser(
        description="Bulk-download all DownBallotR election data to CSV files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--output-dir",
        default=default_output,
        metavar="PATH",
        help=f"Root output directory (default: {default_output})",
    )
    parser.add_argument(
        "--section",
        choices=[*SECTIONS.keys(), "all"],
        default="all",
        metavar="SECTION",
        help=(
            "Which section to download (default: all). "
            f"Choices: {', '.join(SECTIONS)} or all."
        ),
    )
    parser.add_argument(
        "--state",
        default=None,
        metavar="STATE",
        help=(
            "Restrict election_stats section to one state "
            f"(e.g. new_york). Available: {', '.join(sorted(ELECTION_STATS_STATES))}."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count() or 1,
        metavar="N",
        help=(
            f"Number of parallel download workers (default: {os.cpu_count() or 1} = all available CPUs). "
            "Keep at 1 for Playwright sections (election_stats SC/NM/NY/VA)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be downloaded without actually scraping.",
    )
    parser.add_argument(
        "--trial-run",
        action="store_true",
        help=(
            "Run one representative year per state/section as a smoke test. "
            "Output goes to <output-dir>/trial/ so it never collides with a full run. "
            "Ignores --section and all year-range flags."
        ),
    )

    # ElectionStats-specific options
    es_group = parser.add_argument_group("election_stats options")
    es_group.add_argument(
        "--es-year-from",
        type=int,
        default=None,
        metavar="YEAR",
        help=(
            "Override the start year for the election_stats section. "
            "Applies to all states (or the single state set by --state). "
            "Default: each state's earliest available year."
        ),
    )
    es_group.add_argument(
        "--es-year-to",
        type=int,
        default=None,
        metavar="YEAR",
        help=(
            "Override the end year for the election_stats section. "
            "Applies to all states (or the single state set by --state). "
            "Default: each state's latest available year."
        ),
    )

    # Georgia-specific options
    ga_group = parser.add_argument_group("georgia options")
    ga_group.add_argument(
        "--ga-year-from",
        type=int,
        default=GA_YEAR_RANGE[0],
        metavar="YEAR",
        help=f"First year to download for Georgia (default: {GA_YEAR_RANGE[0]}).",
    )
    ga_group.add_argument(
        "--ga-year-to",
        type=int,
        default=GA_YEAR_RANGE[1],
        metavar="YEAR",
        help=f"Last year to download for Georgia (default: current year).",
    )
    ga_group.add_argument(
        "--ga-level",
        choices=["all", "state", "county"],
        default="all",
        help="What to scrape for Georgia: state totals, county totals, or both (default: all).",
    )
    ga_group.add_argument(
        "--ga-vote-methods",
        action="store_true",
        help=(
            "Capture per-contest vote-method breakdowns for Georgia "
            "(Advance in Person / Election Day / Absentee / Provisional). "
            "Requires extra Playwright clicks per page; significantly slower."
        ),
    )
    ga_group.add_argument(
        "--ga-county-workers",
        type=int,
        default=2,
        metavar="N",
        help=(
            "Parallel Chromium browsers for Georgia county scraping (default: 2). "
            "Each worker is a separate process — keep ≤ 6 to avoid exhausting memory."
        ),
    )

    # Utah-specific options
    ut_group = parser.add_argument_group("utah options")
    ut_group.add_argument(
        "--ut-year-from",
        type=int,
        default=UT_YEAR_RANGE[0],
        metavar="YEAR",
        help=f"First year to download for Utah (default: {UT_YEAR_RANGE[0]}).",
    )
    ut_group.add_argument(
        "--ut-year-to",
        type=int,
        default=UT_YEAR_RANGE[1],
        metavar="YEAR",
        help="Last year to download for Utah (default: current year).",
    )
    ut_group.add_argument(
        "--ut-level",
        choices=["all", "state", "county"],
        default="all",
        help="What to scrape for Utah: state totals, county totals, or both (default: all).",
    )
    ut_group.add_argument(
        "--ut-vote-methods",
        action="store_true",
        help=(
            "Capture per-contest vote-method breakdowns for Utah "
            "(Advance in Person / Election Day / Absentee / Provisional). "
            "Requires extra Playwright clicks per page; significantly slower."
        ),
    )
    ut_group.add_argument(
        "--ut-county-workers",
        type=int,
        default=2,
        metavar="N",
        help=(
            "Parallel Chromium browsers for Utah county scraping (default: 2). "
            "Each worker is a separate process — keep ≤ 6 to avoid exhausting memory."
        ),
    )

    # Indiana-specific options
    in_group = parser.add_argument_group("indiana options")
    in_group.add_argument(
        "--in-year-from",
        type=int,
        default=IN_YEAR_RANGE[0],
        metavar="YEAR",
        help=f"First year to download for Indiana (default: {IN_YEAR_RANGE[0]}).",
    )
    in_group.add_argument(
        "--in-year-to",
        type=int,
        default=IN_YEAR_RANGE[1],
        metavar="YEAR",
        help="Last year to download for Indiana (default: current year).",
    )
    in_group.add_argument(
        "--in-level",
        choices=["all", "state", "county"],
        default="all",
        help="What to scrape for Indiana: state totals, county totals, or both (default: all).",
    )

    # Louisiana-specific options
    la_group = parser.add_argument_group("louisiana options")
    la_group.add_argument(
        "--la-year-from",
        type=int,
        default=LA_YEAR_RANGE[0],
        metavar="YEAR",
        help=f"First year to download for Louisiana (default: {LA_YEAR_RANGE[0]}). Full history starts at 1982.",
    )
    la_group.add_argument(
        "--la-year-to",
        type=int,
        default=LA_YEAR_RANGE[1],
        metavar="YEAR",
        help=f"Last year to download for Louisiana (default: {LA_YEAR_RANGE[1]}).",
    )
    la_group.add_argument(
        "--la-level",
        choices=["all", "state", "parish"],
        default="all",
        help="What to scrape for Louisiana: state tabs, parish results, or both (default: all).",
    )
    la_group.add_argument(
        "--la-parish-workers",
        type=int,
        default=2,
        metavar="N",
        help="Parallel Chromium browsers for Louisiana parish scraping (default: 2).",
    )

    # Connecticut-specific options
    ct_group = parser.add_argument_group("connecticut options")
    ct_group.add_argument(
        "--ct-year-from",
        type=int,
        default=CT_YEAR_RANGE[0],
        metavar="YEAR",
        help=f"First year to download for Connecticut (default: {CT_YEAR_RANGE[0]}).",
    )
    ct_group.add_argument(
        "--ct-year-to",
        type=int,
        default=CT_YEAR_RANGE[1],
        metavar="YEAR",
        help="Last year to download for Connecticut (default: current year).",
    )
    ct_group.add_argument(
        "--ct-level",
        choices=["all", "state", "town"],
        default="all",
        help=(
            "What to scrape for Connecticut: statewide totals only (state), "
            "town-level only (town), or both (all, default)."
        ),
    )
    ct_group.add_argument(
        "--ct-town-workers",
        type=int,
        default=2,
        metavar="N",
        help=(
            "Parallel Chromium browsers for Connecticut town scraping (default: 2). "
            "One browser per county — keep ≤ 4 to avoid memory exhaustion."
        ),
    )

    args = parser.parse_args()

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("DownBallotR bulk data download")
    print("=" * 70)
    print(f"Output directory : {output_dir}")
    print(f"Section          : {args.section}")
    print(f"State filter     : {args.state or 'all'}")
    print(f"Workers          : {args.workers}")
    print(f"Dry run          : {args.dry_run}")
    if args.section in ("election_stats", "all"):
        yf = args.es_year_from or "default (per state)"
        yt = args.es_year_to   or "default (per state)"
        print(f"ES year range    : {yf}–{yt}")
    if args.section in ("georgia", "all"):
        print(f"GA year range    : {args.ga_year_from}–{args.ga_year_to}")
        print(f"GA level         : {args.ga_level}")
        print(f"GA vote methods  : {args.ga_vote_methods}")
        print(f"GA county workers: {args.ga_county_workers}")
    if args.section in ("indiana", "all"):
        print(f"IN year range    : {args.in_year_from}–{args.in_year_to}")
        print(f"IN level         : {args.in_level}")
    if args.section in ("utah", "all"):
        print(f"UT year range    : {args.ut_year_from}–{args.ut_year_to}")
        print(f"UT level         : {args.ut_level}")
        print(f"UT vote methods  : {args.ut_vote_methods}")
        print(f"UT county workers: {args.ut_county_workers}")
    if args.section in ("connecticut", "all"):
        print(f"CT year range    : {args.ct_year_from}–{args.ct_year_to}")
        print(f"CT level         : {args.ct_level}")
        print(f"CT town workers  : {args.ct_town_workers}")
    if args.section in ("louisiana", "all"):
        print(f"LA year range    : {args.la_year_from}–{args.la_year_to}")
        print(f"LA level         : {args.la_level}")
        print(f"LA parish workers: {args.la_parish_workers}")
    print("=" * 70)

    if args.trial_run:
        print("\nMode: TRIAL RUN (one year per state, output → <output-dir>/trial/)")
        print("=" * 70)
        all_results = trial_run(output_dir, dry_run=args.dry_run, workers=args.workers)
        n_total   = len(all_results)
        n_success = sum(all_results)
        n_failed  = n_total - n_success
        print(f"\n{'='*70}")
        print(f"DONE  |  {n_success}/{n_total} tasks succeeded", end="")
        if n_failed:
            print(f"  |  {n_failed} failed (see errors above)")
        else:
            print()
        print("=" * 70)
        if n_failed:
            sys.exit(1)
        return

    to_run = SECTIONS if args.section == "all" else {args.section: SECTIONS[args.section]}

    all_results: list[bool] = []
    for name, fn in to_run.items():
        print(f"\n{'─'*70}")
        print(f"  SECTION: {name.upper()}")
        print(f"{'─'*70}")
        section_results = fn(
            output_dir,
            dry_run=args.dry_run,
            state=args.state,
            workers=args.workers,
            # ElectionStats-specific
            es_year_from=args.es_year_from,
            es_year_to=args.es_year_to,
            # Georgia-specific
            ga_year_from=args.ga_year_from,
            ga_year_to=args.ga_year_to,
            ga_level=args.ga_level,
            ga_vote_methods=args.ga_vote_methods,
            ga_county_workers=args.ga_county_workers,
            # Indiana-specific
            in_year_from=args.in_year_from,
            in_year_to=args.in_year_to,
            in_level=args.in_level,
            # Utah-specific
            ut_year_from=args.ut_year_from,
            ut_year_to=args.ut_year_to,
            ut_level=args.ut_level,
            ut_vote_methods=args.ut_vote_methods,
            ut_county_workers=args.ut_county_workers,
            # Connecticut-specific
            ct_year_from=args.ct_year_from,
            ct_year_to=args.ct_year_to,
            ct_level=args.ct_level,
            ct_town_workers=args.ct_town_workers,
            # Louisiana-specific
            la_year_from=args.la_year_from,
            la_year_to=args.la_year_to,
            la_level=args.la_level,
            la_parish_workers=args.la_parish_workers,
        )
        all_results.extend(section_results)

    n_total   = len(all_results)
    n_success = sum(all_results)
    n_failed  = n_total - n_success

    print(f"\n{'='*70}")
    print(f"DONE  |  {n_success}/{n_total} tasks succeeded", end="")
    if n_failed:
        print(f"  |  {n_failed} failed (see errors above)")
    else:
        print()
    print("=" * 70)

    if n_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
