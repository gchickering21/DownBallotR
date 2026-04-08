"""
Smoke tests for the Connecticut CTEMS election results scraper.

These are live integration tests that hit the real website.  They require:
  - A working internet connection
  - Playwright installed with Chromium: ``playwright install chromium``

Run from ``inst/python/`` with::

    python -m Connecticut.tests.test_ct_smoke
    python -m Connecticut.tests.test_ct_smoke --year 2024
    python -m Connecticut.tests.test_ct_smoke --discovery-only
    python -m Connecticut.tests.test_ct_smoke --state-only    # skip town scraping
    python -m Connecticut.tests.test_ct_smoke --save-html     # dump HTML to /tmp for inspection

Selector debugging
------------------
If tests fail with "No race containers found" or empty DataFrames, the HTML
selectors in ``parser.py`` and ``client.py`` may need updating.  Use
``--save-html`` to dump the rendered page to ``/tmp/ct_*.html`` and inspect
the actual DOM structure::

    python -m Connecticut.tests.test_ct_smoke --discovery-only --save-html
    # then open /tmp/ct_landing.html in a browser
"""

from __future__ import annotations

import argparse
import sys


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pass(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")


def _warn(msg: str) -> None:
    print(f"  WARN  {msg}")


# ── Individual test functions ─────────────────────────────────────────────────

def test_discovery(save_html: bool = False) -> bool:
    """Render the CTEMS home page and check that at least one election is found."""
    print("\n[test_discovery] Rendering CTEMS home page...")
    from Connecticut.client import CtPlaywrightClient
    from Connecticut.discovery import parse_election_options

    with CtPlaywrightClient() as client:
        html = client.get_landing_page()

    if save_html:
        path = "/tmp/ct_landing.html"
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"  [debug] Saved landing page HTML to {path}")

    elections = parse_election_options(html)

    if not elections:
        _fail(
            "No elections discovered — the election dropdown selector likely "
            "needs updating.  Use --save-html to inspect the rendered HTML."
        )
        return False

    years = sorted({e.year for e in elections})
    _pass(
        f"Discovered {len(elections)} election(s) across years "
        f"{years[0]}–{years[-1]}."
    )
    for e in elections[:5]:
        print(f"       {e.year}  {e.name!r}  (option_value={e.option_value!r})")
    if len(elections) > 5:
        print(f"       ... and {len(elections) - 5} more.")
    return True


def test_single_election(
    year: int,
    scrape_towns: bool = True,
    save_html: bool = False,
) -> bool:
    """Discover elections, pick the first one matching *year*, and scrape it."""
    print(f"\n[test_single_election] year={year}, scrape_towns={scrape_towns}")
    from Connecticut.client import CtPlaywrightClient
    from Connecticut.discovery import parse_election_options
    from Connecticut.parser import parse_statewide_results, parse_town_results

    # Phase 1: discovery
    with CtPlaywrightClient() as client:
        landing_html = client.get_landing_page()

    elections = parse_election_options(landing_html)
    matching = [e for e in elections if e.year == year]

    if not matching:
        _warn(
            f"No elections found for year {year}. "
            f"Available years: {sorted({e.year for e in elections})}"
        )
        return True  # not a failure — just no data for this year

    target = matching[0]
    print(f"  Target: {target.name!r} (option_value={target.option_value!r})")

    # Phase 2a: statewide scrape
    print(f"  Scraping statewide results...")
    with CtPlaywrightClient() as client:
        state_html = client.get_statewide_results(target.option_value)

    if save_html:
        path = "/tmp/ct_statewide.html"
        with open(path, "w", encoding="utf-8") as f:
            f.write(state_html)
        print(f"  [debug] Saved statewide HTML to {path}")

    state_df = parse_statewide_results(state_html, target)

    if state_df.empty:
        _warn(
            f"Statewide parser returned empty DataFrame for '{target.name}'. "
            "This is normal for special elections or elections with no federal races "
            "on the statewide Summary page — town aggregation will supply the data. "
            "If unexpected, use --save-html and inspect /tmp/ct_statewide.html."
        )
    else:
        levels = state_df["election_level"].value_counts().to_dict() if "election_level" in state_df.columns else {}
        _pass(f"Statewide: {len(state_df)} candidate row(s). Levels: {levels}")
    print(state_df.head(8).to_string(index=False))

    if not scrape_towns:
        return True

    # Phase 2b: enumerate county/town options
    print(f"\n  Enumerating county/town options...")
    with CtPlaywrightClient() as client:
        county_town_tree = client.get_county_town_options(target.option_value)

    total_towns = sum(len(towns) for _, _, towns in county_town_tree)
    if not county_town_tree:
        _warn("No county/town options found — 'Select Town' navigation may need updating.")
        return True

    _pass(
        f"Found {len(county_town_tree)} county/counties, "
        f"{total_towns} total town(s)."
    )
    for county_name, _, towns in county_town_tree:
        print(f"       {county_name}: {len(towns)} town(s)")

    # Phase 2c: scrape first county (all its towns in one browser session)
    first_county_name, first_county_value, first_towns = county_town_tree[0]
    if not first_towns:
        _warn(f"No towns found in county '{first_county_name}'.")
        return True

    print(f"\n  Scraping all {len(first_towns)} town(s) in {first_county_name}...")
    with CtPlaywrightClient() as client:
        town_htmls = client.get_all_towns_for_county(
            election_option_value=target.option_value,
            county_name=first_county_name,
            county_option_value=first_county_value,
            towns=first_towns,
        )

    town_frames = []
    for town_name, html in town_htmls:
        if save_html and town_name == first_towns[0][0]:
            path = "/tmp/ct_town.html"
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"  [debug] Saved first town HTML to {path}")
        df = parse_town_results(html, town_name, first_county_name, target)
        if not df.empty:
            town_frames.append(df)

    if not town_frames:
        _warn(
            f"All towns in {first_county_name} returned empty DataFrames. "
            "This may be normal for primaries with no contested races in this county."
        )
        return True

    import pandas as pd
    county_df = pd.concat(town_frames, ignore_index=True)
    levels = county_df["election_level"].value_counts().to_dict()
    _pass(
        f"{first_county_name}: {len(town_frames)}/{len(first_towns)} towns with results, "
        f"{len(county_df)} total rows. Levels: {levels}"
    )
    print(county_df.head(8).to_string(index=False))
    return True


def test_pipeline_year_range(
    year_from: int,
    year_to: int,
    scrape_towns: bool = True,
) -> bool:
    """Run the full pipeline for a year range and check the output shape."""
    print(
        f"\n[test_pipeline_year_range] {year_from}–{year_to}, "
        f"scrape_towns={scrape_towns}"
    )
    import pandas as pd
    from Connecticut.pipeline import get_ct_election_results

    level = "all" if scrape_towns else "state"
    result = get_ct_election_results(
        year_from=year_from,
        year_to=year_to,
        level=level,
    )

    ok = True

    if level == "state":
        if not isinstance(result, pd.DataFrame):
            _fail(f"Expected a DataFrame for level='state', got {type(result)}")
            ok = False
        else:
            _pass(f"state_df: {len(result)} rows × {len(result.columns)} cols.")
            if not result.empty:
                print(result.head(5).to_string(index=False))
    else:
        if not isinstance(result, dict):
            _fail(f"Expected a dict for level='all', got {type(result)}")
            ok = False
        else:
            state_df = result.get("state", pd.DataFrame())
            town_df  = result.get("town",  pd.DataFrame())

            if not isinstance(state_df, pd.DataFrame):
                _fail(f"result['state'] is not a DataFrame: {type(state_df)}")
                ok = False
            else:
                _pass(f"state_df: {len(state_df)} rows × {len(state_df.columns)} cols.")
                if not state_df.empty:
                    print(state_df.head(5).to_string(index=False))

            if not isinstance(town_df, pd.DataFrame):
                _fail(f"result['town'] is not a DataFrame: {type(town_df)}")
                ok = False
            else:
                _pass(f"town_df: {len(town_df)} rows × {len(town_df.columns)} cols.")
                if not town_df.empty:
                    print(town_df.head(5).to_string(index=False))

    return ok


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Smoke tests for the Connecticut CTEMS election scraper."
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
        help="Skip town scraping (faster, statewide totals only).",
    )
    parser.add_argument(
        "--save-html", action="store_true",
        help="Save rendered HTML to /tmp/ct_*.html for selector debugging.",
    )
    args = parser.parse_args()

    scrape_towns = not args.state_only
    results: list[bool] = []

    results.append(test_discovery(save_html=args.save_html))

    if not args.discovery_only:
        if args.year is not None:
            results.append(
                test_single_election(
                    args.year,
                    scrape_towns=scrape_towns,
                    save_html=args.save_html,
                )
            )
        else:
            import datetime
            current_year = datetime.date.today().year
            results.append(
                test_pipeline_year_range(
                    current_year - 1,
                    current_year,
                    scrape_towns=scrape_towns,
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
