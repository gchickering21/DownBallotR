"""
One-shot script to inspect the Georgia SOS landing page.

Run from inst/python/ with:
    python -m Clarity.Georgia.inspect_landing
    python -m Clarity.Georgia.inspect_landing --year 2024
    python -m Clarity.Georgia.inspect_landing --year-from 2020 --year-to 2024

What it does:
  1. Renders https://results.sos.ga.gov/results/public/Georgia in a real browser.
  2. Saves the raw HTML to /tmp/ga_landing.html for manual inspection.
  3. Runs parse_election_links() and prints every election found.
  4. If --year / --year-from / --year-to are given, also prints the filtered list.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .client import GaPlaywrightClient
from .discovery import parse_election_links


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect the Georgia SOS elections landing page."
    )
    parser.add_argument("--year", type=int, default=None,
                        help="Filter to a single year.")
    parser.add_argument("--year-from", type=int, default=None, dest="year_from",
                        help="Filter: start year (inclusive).")
    parser.add_argument("--year-to", type=int, default=None, dest="year_to",
                        help="Filter: end year (inclusive).")
    parser.add_argument("--headless", action="store_true", default=False,
                        help="Run browser headlessly (default: visible window).")
    parser.add_argument("--save", default="/tmp/ga_landing.html",
                        help="Path to save raw HTML (default: /tmp/ga_landing.html).")
    args = parser.parse_args()

    print(f"[GA inspect] Fetching landing page (headless={args.headless})...")
    with GaPlaywrightClient(headless=args.headless, sleep_s=3.0) as client:
        html = client.get_landing_page()

    # Save raw HTML for offline inspection.
    out_path = Path(args.save)
    out_path.write_text(html, encoding="utf-8")
    print(f"[GA inspect] Saved raw HTML → {out_path}  ({len(html):,} bytes)")

    # Parse all elections.
    elections = parse_election_links(html)

    if not elections:
        print(
            "\n[GA inspect] WARNING: parse_election_links() returned 0 elections.\n"
            "  Open the saved HTML and look for the election link structure.\n"
            "  Then update the XPath in Clarity/Georgia/discovery.py accordingly."
        )
        sys.exit(1)

    print(f"\n[GA inspect] Found {len(elections)} election(s) total:\n")
    years_seen = sorted({e.year for e in elections})
    for year in years_seen:
        year_elections = [e for e in elections if e.year == year]
        print(f"  {year}  ({len(year_elections)} election(s))")
        for e in year_elections:
            print(f"    {e.name:<50}  {e.slug}")

    # Apply year filter if requested.
    year_from = args.year or args.year_from
    year_to   = args.year or args.year_to

    if year_from or year_to:
        filtered = [
            e for e in elections
            if (year_from is None or e.year >= year_from)
            and (year_to   is None or e.year <= year_to)
        ]
        lo = year_from or "–"
        hi = year_to   or "–"
        print(f"\n[GA inspect] Filtered to {lo}–{hi}: {len(filtered)} election(s)\n")
        for e in filtered:
            print(f"  {e.year}  {e.name:<50}  {e.url}")


if __name__ == "__main__":
    main()
