"""
School board elections scraper — command-line interface.

Usage
-----
    python scrape_school_boards.py --state "Wisconsin" --start 2014 --end 2025
    python scrape_school_boards.py --state "New Hampshire" --start 2025
    python scrape_school_boards.py --state "New Jersey" --start 2022 --end 2022

Output
------
Three CSVs are written to Ballotpedia/output_data/:
    {state_slug}_{start}_{end}_districts.csv
    {state_slug}_{start}_{end}_candidates.csv
    {state_slug}_{start}_{end}_joined.csv
"""

import argparse
import datetime
import os
import sys

# Allow running from anywhere inside the project
_HERE = os.path.dirname(os.path.abspath(__file__))
_PYTHON_DIR = os.path.dirname(_HERE)
sys.path.insert(0, _PYTHON_DIR)

from Ballotpedia import SchoolBoardScraper
from dataclasses import asdict
import pandas as pd

# Ballotpedia's school board pages start at 2013; nothing earlier exists.
_FIRST_AVAILABLE_YEAR = 2013


def state_slug(state: str) -> str:
    """Convert 'New Hampshire' → 'new_hampshire' for use in filenames."""
    return state.strip().lower().replace(" ", "_")


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Ballotpedia school board election data for a given state and year range."
    )
    parser.add_argument(
        "--state", required=True,
        help="State name (e.g. 'Wisconsin', 'New Hampshire')"
    )
    parser.add_argument(
        "--start", type=int, required=True,
        help="First year to scrape (e.g. 2014)"
    )
    parser.add_argument(
        "--end", type=int, default=None,
        help="Last year to scrape, inclusive (default: same as --start)"
    )
    parser.add_argument(
        "--sleep", type=float, default=1.0,
        help="Seconds to wait between requests (default: 1.0)"
    )
    parser.add_argument(
        "--out-dir", default=None,
        help="Output directory (default: Ballotpedia/output_data/ next to this script)"
    )
    args = parser.parse_args()

    end_year = args.end if args.end is not None else args.start
    if end_year < args.start:
        parser.error("--end must be >= --start")

    # Clamp to the range Ballotpedia actually covers
    current_year = datetime.date.today().year
    effective_start = max(args.start, _FIRST_AVAILABLE_YEAR)
    effective_end = min(end_year, current_year)

    if effective_start != args.start:
        print(
            f"Note: Ballotpedia data begins at {_FIRST_AVAILABLE_YEAR}. "
            f"Skipping {args.start}–{effective_start - 1}."
        )
    if effective_end != end_year:
        print(
            f"Note: {end_year} is beyond {current_year}. "
            f"Capping end year at {effective_end}."
        )
    if effective_start > effective_end:
        print("No years to scrape in the available range. Exiting.")
        sys.exit(0)

    out_dir = args.out_dir or os.path.join(_HERE, "output_data")
    os.makedirs(out_dir, exist_ok=True)

    slug = state_slug(args.state)
    prefix = f"{slug}_{effective_start}_{effective_end}"

    scraper = SchoolBoardScraper(sleep_s=args.sleep)

    # ------------------------------------------------------------------
    # Districts
    # ------------------------------------------------------------------
    print(f"\n=== Districts: {args.state} {effective_start}–{effective_end} ===")
    all_districts = []
    for year in range(effective_start, effective_end + 1):
        rows = scraper.scrape_year(year, state=args.state)
        if rows:
            all_districts.extend(rows)
            print(f"  {year}: {len(rows)} districts")
        else:
            print(f"  {year}: 0 districts")

    districts_df = (
        pd.DataFrame([asdict(r) for r in all_districts])
        if all_districts else pd.DataFrame()
    )
    dist_path = os.path.join(out_dir, f"{prefix}_districts.csv")
    districts_df.to_csv(dist_path, index=False)
    print(f"\nSaved {dist_path}  ({len(districts_df)} rows)")

    # ------------------------------------------------------------------
    # Candidates
    # ------------------------------------------------------------------
    print(f"\n=== Candidates: {args.state} {effective_start}–{effective_end} ===")
    all_cands = []
    for year in range(effective_start, effective_end + 1):
        try:
            rows = scraper.scrape_with_results(year, state=args.state)
            if rows:
                all_cands.extend(rows)
                print(f"  {year}: {len(rows)} candidate rows")
            else:
                print(f"  {year}: 0 candidates")
        except Exception as exc:
            print(f"  {year}: ERROR — {exc}")

    candidates_df = (
        pd.DataFrame([asdict(r) for r in all_cands])
        if all_cands else pd.DataFrame()
    )
    cand_path = os.path.join(out_dir, f"{prefix}_candidates.csv")
    candidates_df.to_csv(cand_path, index=False)
    print(f"\nSaved {cand_path}  ({len(candidates_df)} rows)")

    # ------------------------------------------------------------------
    # Joined
    # ------------------------------------------------------------------
    if not candidates_df.empty and not districts_df.empty:
        join_keys = ["year", "state", "district", "district_url"]
        cand_extra = [c for c in candidates_df.columns if c not in districts_df.columns]
        joined = districts_df.merge(
            candidates_df[join_keys + cand_extra],
            on=join_keys,
            how="left",
        )
        joined_path = os.path.join(out_dir, f"{prefix}_joined.csv")
        joined.to_csv(joined_path, index=False)
        print(f"Saved {joined_path}  ({len(joined)} rows)")
    else:
        print("\nNo candidates found — joined file not written.")

    print("\nDone.")


if __name__ == "__main__":
    main()
