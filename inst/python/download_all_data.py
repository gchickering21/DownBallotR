#!/usr/bin/env python3
"""
download_all_data.py
====================
Bulk-download script for all DownBallotR data sources.

Usage (run from inst/python/):
    python download_all_data.py [options]

Sections:
    election_stats      ElectionStats (VA, MA, CO, NH, SC, NM, NY)
    nc                  North Carolina (NC State Board of Elections, 2000-2025)
    school_board        Ballotpedia school board elections (2013-present)
    state_elections     Ballotpedia state elections (2024-present, per state)
    municipal           Ballotpedia municipal/mayoral elections (2014-present)
    all                 All of the above (default)

Options:
    --output-dir PATH   Root output directory (default: <repo>/data/)
    --section NAME      Run only one section (default: all)
    --fast              Use fast/lightweight modes (districts/listings/links)
                        instead of the default detailed modes (joined/results).
                        Much quicker but omits vote counts and candidate detail.
    --dry-run           Print tasks without scraping

Output layout:
    data/
      election_stats/
        {state}_{year_from}_{year_to}_state.csv
        {state}_{year_from}_{year_to}_county.csv
      nc_results/
        nc_{year_from}_{year_to}.csv
      school_board/
        school_board_{year}.csv           (fast: district metadata)
        school_board_{year}_joined.csv    (--full: districts + candidates)
      state_elections/
        {state_slug}_{year}.csv
      municipal/
        municipal_all_{year}.csv
        municipal_mayoral_{year}.csv
"""

from __future__ import annotations

import argparse
import datetime
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

# Year ranges mirror registry._YEAR_RANGES / STATE_CONFIGS
ELECTION_STATS_STATES: dict[str, tuple[int, int]] = {
    "virginia":       (1789, 2025),
    "massachusetts":  (1970, 2026),
    "colorado":       (1902, 2024),
    "new_hampshire":  (1970, 2024),
    "south_carolina": (2008, 2025),
    "new_mexico":     (2000, 2024),
    "new_york":       (1994, 2024),
}

NC_YEAR_RANGE              = (2000, 2025)
SCHOOL_BOARD_YEAR_RANGE    = (2013, CURRENT_YEAR)
STATE_ELECTIONS_YEAR_RANGE = (2024, CURRENT_YEAR)
MUNICIPAL_YEAR_RANGE       = (2014, CURRENT_YEAR)
MAYORAL_YEAR_RANGE         = (2020, CURRENT_YEAR)

# All 50 US states + DC (title-case full names expected by Ballotpedia scrapers)
ALL_STATES: list[str] = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming", "District of Columbia",
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _slug(s: str) -> str:
    """Convert a label to a filesystem-safe slug."""
    return s.lower().replace(" ", "_").replace("/", "_")


def _save(df: pd.DataFrame, path: Path) -> None:
    """Write *df* to *path* as a CSV file, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    # Show path relative to the repo root (4 levels up from inst/python/)
    try:
        display = path.relative_to(Path(__file__).parent.parent.parent.parent)
    except ValueError:
        display = path
    print(f"  → saved {len(df):,} rows  →  {display}")


def _already_exists(path: Path) -> bool:
    """Return True and print a skip message when *path* already exists."""
    if path.exists():
        print(f"  ↷ already exists, skipping: {path.name}")
        return True
    return False


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
            # election_stats level='all' → {"state": df, "county": df}
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

    Note: Playwright-based scrapers (SC, NM, NY, school_board WAF fallback)
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


# ── Section downloaders ────────────────────────────────────────────────────────

def download_election_stats(
    output_dir: Path, *, dry_run: bool, state: "str | None" = None, workers: int = 1, **_
) -> list[bool]:
    """
    ElectionStats: one download per state, full year range, level='all'.
    Each state goes into its own subdirectory:
      data/election_stats/{state}/{state}_{year_from}_{year_to}_state.csv
      data/election_stats/{state}/{state}_{year_from}_{year_to}_county.csv

    Pass state= to restrict to a single state (e.g. state='new_york').
    Note: SC, NM, NY use Playwright — set workers=1 for those states.
    """
    # Normalise the filter to the same slug format used as dict keys
    state_filter = _slug(state) if state else None
    if state_filter and state_filter not in ELECTION_STATS_STATES:
        valid = ", ".join(sorted(ELECTION_STATS_STATES))
        raise ValueError(f"Unknown state {state!r}. Available: {valid}")

    states_to_run = (
        {state_filter: ELECTION_STATS_STATES[state_filter]}
        if state_filter
        else ELECTION_STATS_STATES
    )

    tasks = []
    for s, (year_from, year_to) in states_to_run.items():
        state_dir = output_dir / "election_stats" / s
        label = f"ElectionStats  {s}  {year_from}–{year_to}"
        path = state_dir / f"{s}_{year_from}_{year_to}.csv"
        tasks.append((label, path, lambda s=s, yf=year_from, yt=year_to: registry.scrape(
            "election_stats",
            state=s, year_from=yf, year_to=yt,
            level="all", parallel=True,
        )))

    return _run_tasks(tasks, dry_run=dry_run, workers=workers)


def download_nc(output_dir: Path, *, dry_run: bool, **_) -> list[bool]:
    """
    NC State Board of Elections: full 2000–2025 range, all three aggregation levels.

    Calls NcElectionPipeline directly (instead of the registry wrapper) so that
    all three outputs are captured in a single pipeline run:
      nc_{year_from}_{year_to}_precinct.csv  — raw precinct-level results
      nc_{year_from}_{year_to}_county.csv    — aggregated to county level
      nc_{year_from}_{year_to}_state.csv     — aggregated to state level
    """
    import datetime as _dt

    year_from, year_to = NC_YEAR_RANGE
    base  = output_dir / "nc_results"
    stem  = f"nc_{year_from}_{year_to}"
    label = f"NC State Board of Elections  {year_from}–{year_to}  (precinct + county + state)"

    # Use a sentinel path for skip-detection; check all three files
    paths = {
        "precinct": base / f"{stem}_precinct.csv",
        "county":   base / f"{stem}_county.csv",
        "state":    base / f"{stem}_state.csv",
    }
    print(f"\n[{label}]")
    if all(p.exists() for p in paths.values()):
        print("  ↷ all three output files exist, skipping")
        return [True]
    if dry_run:
        for key, p in paths.items():
            status = "exists" if p.exists() else "would write"
            print(f"  (dry-run) [{key}] → {p.name}  ({status})")
        return [True]

    try:
        from NorthCarolina.pipeline import NcElectionPipeline
        from datetime import date

        start = date(year_from, 1, 1)
        end   = date(year_to, 12, 31)

        pipeline = NcElectionPipeline()
        precinct_df, county_df, state_df = pipeline.run(
            start_date=start,
            end_date=end,
            min_supported_date=start,
            max_supported_date=end,
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


def download_school_board(
    output_dir: Path, *, dry_run: bool, full: bool = False, workers: int = 1, **_
) -> list[bool]:
    """
    Ballotpedia school board elections, one file per year (all US states).

    Default (full): mode='joined'    — districts + candidate results; one
                    extra request per district (thousands of HTTP calls/year).
    --fast (quick): mode='districts' — district metadata only, ~1 request/year.
    Note: WAF Playwright fallback is not thread-safe — use workers=1.
    """
    base = output_dir / "Ballotpedia" / "school_board"
    mode = "joined" if full else "districts"
    start, end = SCHOOL_BOARD_YEAR_RANGE
    suffix = "" if full else "_districts"

    tasks = [
        (
            f"Ballotpedia school board  {year}  mode={mode}",
            base / f"school_board_{year}{suffix}.csv",
            lambda y=year: registry.scrape("ballotpedia", year=y, state=None, mode=mode),
        )
        for year in range(start, end + 1)
    ]
    return _run_tasks(tasks, dry_run=dry_run, workers=workers)


def download_state_elections(
    output_dir: Path, *, dry_run: bool, full: bool = False, workers: int = 1, **_
) -> list[bool]:
    """
    Ballotpedia state elections, one file per (state, year) (all US states).

    Default (full): mode='results'  — follows each contest URL for vote counts
                    and percentages; many extra requests per state/year.
    --fast (quick): mode='listings' — candidate names, offices, parties, status;
                    no vote counts; ~1 request per state/year.
    """
    base  = output_dir / "Ballotpedia" / "state_elections"
    mode  = "results" if full else "listings"
    start, end = STATE_ELECTIONS_YEAR_RANGE

    tasks = [
        (
            f"Ballotpedia state elections  {state}  {year}  mode={mode}",
            base / f"{_slug(state)}_{year}.csv",
            lambda s=state, y=year: registry.scrape(
                "ballotpedia_elections",
                state=s, year=y, mode=mode, election_level="all",
            ),
        )
        for state in ALL_STATES
        for year in range(start, end + 1)
    ]
    return _run_tasks(tasks, dry_run=dry_run, workers=workers)


def download_municipal(
    output_dir: Path, *, dry_run: bool, full: bool = False, workers: int = 1, **_
) -> list[bool]:
    """
    Ballotpedia municipal and mayoral elections, one file per (race_type, year).

    Default (full): mode='results' — follows every sub-URL for candidate and
                    vote data; one extra request per location.
    --fast (quick): mode='links'   — index discovery only; location metadata
                    and sub-URLs, no vote data; 1 request/year.

    race_type='all'    covers municipal elections 2014–present.
    race_type='mayoral' covers mayoral-only elections 2020–present.
    """
    base = output_dir / "Ballotpedia" / "municipal"
    mode = "results" if full else "links"

    tasks = [
        (
            f"Ballotpedia municipal (all)    {year}  mode={mode}",
            base / f"municipal_all_{year}.csv",
            lambda y=year: registry.scrape(
                "ballotpedia_municipal", year=y, state=None, race_type="all", mode=mode,
            ),
        )
        for year in range(MUNICIPAL_YEAR_RANGE[1], MUNICIPAL_YEAR_RANGE[0] - 1, -1)
    ] + [
        (
            f"Ballotpedia municipal (mayoral) {year}  mode={mode}",
            base / f"municipal_mayoral_{year}.csv",
            lambda y=year: registry.scrape(
                "ballotpedia_municipal", year=y, state=None, race_type="mayoral", mode=mode,
            ),
        )
        for year in range(MAYORAL_YEAR_RANGE[1], MAYORAL_YEAR_RANGE[0] - 1, -1)
    ]
    return _run_tasks(tasks, dry_run=dry_run, workers=workers)


# ── Dispatch table ─────────────────────────────────────────────────────────────

SECTIONS: dict[str, Callable] = {
    "election_stats":   download_election_stats,
    "nc":               download_nc,
    "school_board":     download_school_board,
    "state_elections":  download_state_elections,
    "municipal":        download_municipal,
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
        "--fast",
        action="store_true",
        help=(
            "Use fast/lightweight scrape modes: school_board→districts, "
            "state_elections→listings, municipal→links. "
            "Much faster but omits vote counts."
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
        default=1,
        metavar="N",
        help=(
            "Number of parallel download workers (default: 1 = sequential). "
            "Recommended: 4–8 for HTTP-only sections (state_elections, municipal --fast). "
            "Keep at 1 for Playwright sections (election_stats SC/NM/NY, school_board full)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be downloaded without actually scraping.",
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
    print(f"Fast mode        : {args.fast}")
    print(f"Workers          : {args.workers}")
    print(f"Dry run          : {args.dry_run}")
    print("=" * 70)

    to_run = SECTIONS if args.section == "all" else {args.section: SECTIONS[args.section]}

    all_results: list[bool] = []
    for name, fn in to_run.items():
        print(f"\n{'─'*70}")
        print(f"  SECTION: {name.upper()}")
        print(f"{'─'*70}")
        section_results = fn(output_dir, dry_run=args.dry_run, full=not args.fast, state=args.state, workers=args.workers)
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
