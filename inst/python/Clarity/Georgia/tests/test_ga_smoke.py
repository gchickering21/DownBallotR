"""
Smoke tests for the Georgia SOS election results scraper.

These are live integration tests that hit the real website.  They require:
  - A working internet connection
  - Playwright installed with Chromium: ``playwright install chromium``

Run from ``inst/python/`` with::

    python -m Georgia.tests.test_ga_smoke
    python -m Georgia.tests.test_ga_smoke --year 2024
    python -m Georgia.tests.test_ga_smoke --discovery-only
    python -m Georgia.tests.test_ga_smoke --state-only   # skip county scraping
    python -m Georgia.tests.test_ga_smoke --vote-methods # include vote-method breakdown
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pass(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")


def _warn(msg: str) -> None:
    print(f"  WARN  {msg}")


# ── Individual test functions ─────────────────────────────────────────────────

def test_discovery() -> bool:
    """Render the landing page and check that at least one election is found."""
    print("\n[test_discovery] Rendering Georgia SOS landing page...")
    from Clarity.Georgia.client import GaPlaywrightClient
    from Clarity.Georgia.discovery import parse_election_links

    with GaPlaywrightClient() as client:
        html = client.get_landing_page()

    elections = parse_election_links(html)

    if not elections:
        _fail("No elections discovered — the landing page selector likely needs updating.")
        print("  TIP: Save the rendered HTML and inspect it:")
        print("       with open('/tmp/ga_landing.html', 'w') as f: f.write(html)")
        return False

    years = sorted({e.year for e in elections})
    _pass(f"Discovered {len(elections)} election(s) across years {years[0]}–{years[-1]}.")
    for e in elections[:5]:
        print(f"       {e.year}  {e.name}  ({e.slug})")
    if len(elections) > 5:
        print(f"       ... and {len(elections) - 5} more.")
    return True


def test_single_election(year: int, level: str = "all") -> bool:
    """Discover elections, pick the first one matching *year*, and scrape it."""
    print(f"\n[test_single_election] year={year}, level={level!r}")
    from Clarity.Georgia.client import GaPlaywrightClient
    from Clarity.Georgia.discovery import parse_election_links
    from Clarity.Georgia.parser import parse_state_results, parse_county_results, county_name_from_url

    # Phase 1: discovery
    with GaPlaywrightClient() as client:
        landing_html = client.get_landing_page()

    elections = parse_election_links(landing_html)
    matching = [e for e in elections if e.year == year]

    if not matching:
        _warn(f"No elections found for year {year}. Available years: "
              f"{sorted({e.year for e in elections})}")
        return True  # not a failure — just no data

    target = matching[0]
    print(f"  Scraping state: {target.name} ({target.url})")

    # Phase 2a: state scrape
    with GaPlaywrightClient() as client:
        election_html = client.get_election_page(target.url)

    state_df, county_urls = parse_state_results(election_html, target)

    if state_df.empty:
        _fail(f"State parser returned empty DataFrame for '{target.name}'.")
        print("  TIP: with open('/tmp/ga_election.html', 'w') as f: f.write(election_html)")
        return False

    _pass(f"State: {len(state_df)} candidate row(s), {len(county_urls)} county URLs.")
    print(state_df.head(8).to_string(index=False))

    if level == "state" or not county_urls:
        return True

    # Phase 2b: one county scrape (first county only for smoke test)
    sample_url = county_urls[0]
    county = county_name_from_url(sample_url)
    print(f"\n  Scraping county sample: {county} ({sample_url})")
    with GaPlaywrightClient() as client:
        county_html = client.get_county_page(sample_url)

    county_df = parse_county_results(county_html, county, target)

    if county_df.empty:
        _fail(f"County parser returned empty DataFrame for {county}.")
        return False

    _pass(f"County ({county}): {len(county_df)} candidate row(s).")
    print(county_df.head(5).to_string(index=False))
    return True


def test_pipeline_year_range(
    year_from: int,
    year_to: int,
    level: str = "all",
    include_vote_methods: bool = False,
) -> bool:
    """Run the full pipeline for a year range and check the output shape."""
    print(f"\n[test_pipeline_year_range] {year_from}–{year_to}, "
          f"level={level!r}, vote_methods={include_vote_methods}")
    from Clarity.Georgia.pipeline import get_ga_election_results

    result = get_ga_election_results(
        year_from=year_from,
        year_to=year_to,
        level=level,
        include_vote_methods=include_vote_methods,
    )

    ok = True

    # Pipeline returns a dict for level="all", a DataFrame otherwise.
    if isinstance(result, dict):
        state_df  = result.get("state",  pd.DataFrame())
        county_df = result.get("county", pd.DataFrame())
    else:
        state_df  = result if level == "state" else pd.DataFrame()
        county_df = result if level == "county" else pd.DataFrame()

    if not isinstance(state_df, pd.DataFrame):
        _fail(f"Expected state_df to be a DataFrame, got {type(state_df)}")
        ok = False
    else:
        _pass(f"state_df: {len(state_df)} rows × {len(state_df.columns)} cols.")
        if not state_df.empty:
            print(state_df.head(5).to_string(index=False))

    if level in ("all", "county"):
        if not isinstance(county_df, pd.DataFrame):
            _fail(f"Expected county_df to be a DataFrame, got {type(county_df)}")
            ok = False
        else:
            _pass(f"county_df: {len(county_df)} rows × {len(county_df.columns)} cols.")
            if not county_df.empty:
                print(county_df.head(5).to_string(index=False))

    if include_vote_methods and isinstance(result, dict):
        for key in ("vote_method_state", "vote_method_county"):
            df = result.get(key, pd.DataFrame())
            if isinstance(df, pd.DataFrame) and not df.empty:
                _pass(f"{key}: {len(df)} rows × {len(df.columns)} cols.")
            else:
                _warn(f"{key}: empty or missing.")

    return ok


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Smoke tests for the Georgia SOS election scraper."
    )
    parser.add_argument(
        "--year", type=int, default=None,
        help="Scrape a specific year (default: run a multi-year pipeline test).",
    )
    parser.add_argument(
        "--discovery-only", action="store_true",
        help="Only run the discovery test (no full scrape).",
    )
    parser.add_argument(
        "--state-only", action="store_true",
        help="Skip county scraping (faster, state totals only).",
    )
    parser.add_argument(
        "--vote-methods", action="store_true",
        help="Include vote-method breakdown (Advanced / Election Day / Absentee / Provisional).",
    )
    args = parser.parse_args()

    level = "state" if args.state_only else "all"
    results: list[bool] = []

    results.append(test_discovery())

    if not args.discovery_only:
        if args.year is not None:
            results.append(test_single_election(args.year, level=level))
        else:
            import datetime
            current_year = datetime.date.today().year
            results.append(
                test_pipeline_year_range(
                    current_year - 1, current_year,
                    level=level,
                    include_vote_methods=args.vote_methods,
                )
            )

    passed = sum(results)
    total  = len(results)
    print(f"\n{'='*50}")
    print(f"Results: {passed}/{total} tests passed.")
    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
