"""
Smoke test: verify that every supported state/source can reach its data endpoint
and parse at least one election result row.

Sources tested:
  Classic states (requests-based): VA, MA, NH, CO
  V2 states    (playwright-based): SC, NM, NY  — requires playwright install
  NC (NCSBE ZIP pipeline)         : discovers elections, scrapes most recent one

Run:
    python -m ElectionStats.tests.integration.test_all_states_smoke
    python -m ElectionStats.tests.integration.test_all_states_smoke --classic-only
    python -m ElectionStats.tests.integration.test_all_states_smoke --v2-only
    python -m ElectionStats.tests.integration.test_all_states_smoke --nc-only
"""

from __future__ import annotations

import sys
from typing import List, Tuple

from ElectionStats.electionStats_client import HttpConfig, StateHttpClient
from ElectionStats.electionStats_search import (
    fetch_search_results,
    fetch_all_search_results_v2,
)
from ElectionStats.state_config import STATE_CONFIGS, get_state_config

# Default test year — should have data for all currently supported states
TEST_YEAR = 2024


# ---------------------------------------------------------------------------
# Per-state smoke helpers
# ---------------------------------------------------------------------------

def smoke_classic_state(state_key: str, year: int = TEST_YEAR) -> List:
    """Fetch first page for a requests-based state and return parsed rows."""
    config = get_state_config(state_key)
    client = StateHttpClient(
        state=state_key,
        base_url=config["base_url"],
        config=HttpConfig(timeout_s=30, sleep_s=0.1),
        search_path=config["search_path"],
        url_style=config["url_style"],
    )
    url = client.build_search_url(year_from=year, year_to=year)
    print(f"  URL: {url}")
    rows = fetch_search_results(client, year_from=year, year_to=year, page=1, state_name=state_key)
    assert len(rows) > 0, f"No rows parsed for {state_key} {year}"
    r = rows[0]
    print(f"  ✓ {len(rows)} row(s) on first page — {r.office} | {r.district} | {r.candidate}")
    return rows


def smoke_v2_state(state_key: str, year: int = TEST_YEAR) -> List:
    """Fetch first page for a playwright-based state and return parsed rows."""
    from ElectionStats.playwright_client import PlaywrightClient

    config = get_state_config(state_key)
    with PlaywrightClient(state_key, config["base_url"]) as pw:
        rows = fetch_all_search_results_v2(pw, year_from=year, year_to=year, state_name=state_key)
    assert len(rows) > 0, f"No rows parsed for {state_key} {year}"
    r = rows[0]
    print(f"  ✓ {len(rows)} row(s) — {r.office} | {r.district} | {r.candidate}")
    return rows


def smoke_nc() -> None:
    """Discover NC elections from NCSBE and scrape the most recent one."""
    from NorthCarolina.discovery import discover_nc_results_zips
    from NorthCarolina.pipeline import NcElectionPipeline

    # Step 1: verify the NCSBE discovery endpoint is reachable
    elections = discover_nc_results_zips()
    assert len(elections) > 0, "No NC elections discovered from NCSBE index"
    print(f"  Discovered {len(elections)} NC elections")

    # Step 2: scrape only the single most recent election as a pipeline check
    most_recent = elections[-1]  # list is sorted ascending by date
    d = most_recent.election_date
    print(f"  Scraping most recent: {d} — {most_recent.label}")

    pipeline = NcElectionPipeline()
    precinct_df, county_df, state_df = pipeline.run(start_date=d, end_date=d)

    assert len(precinct_df) > 0, f"No precinct rows returned for NC {d}"
    print(
        f"  ✓ {len(precinct_df):,} precinct rows | "
        f"{len(county_df):,} county rows | "
        f"{len(state_df):,} state rows"
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def _run_states(
    states: List[str],
    label: str,
    smoke_fn,
) -> Tuple[List[str], List[str]]:
    """Run smoke_fn for each state; return (passed, failed) lists."""
    passed, failed = [], []
    sep = "=" * 60
    print(f"\n{sep}\n{label} ({TEST_YEAR})\n{sep}")
    for state in sorted(states):
        print(f"\n[{state}]")
        try:
            smoke_fn(state)
            passed.append(state)
        except Exception as exc:
            print(f"  ✗ FAILED: {exc}")
            failed.append(state)
    return passed, failed


def main(run_classic: bool = True, run_v2: bool = True, run_nc: bool = True) -> None:
    classic_states = [
        k for k, v in STATE_CONFIGS.items() if v["scraping_method"] == "requests"
    ]
    v2_states = [
        k for k, v in STATE_CONFIGS.items() if v["scraping_method"] == "playwright"
    ]

    all_passed: List[str] = []
    all_failed: List[str] = []

    if run_classic:
        p, f = _run_states(classic_states, "Classic states (requests)", smoke_classic_state)
        all_passed.extend(p)
        all_failed.extend(f)

    if run_v2:
        p, f = _run_states(v2_states, "V2 states (playwright)", smoke_v2_state)
        all_passed.extend(p)
        all_failed.extend(f)

    if run_nc:
        sep = "=" * 60
        print(f"\n{sep}\nNorth Carolina (NCSBE ZIP pipeline)\n{sep}\n[nc]")
        try:
            smoke_nc()
            all_passed.append("nc")
        except Exception as exc:
            print(f"  ✗ FAILED: {exc}")
            all_failed.append("nc")

    sep = "=" * 60
    print(f"\n{sep}")
    print(f"Results: {len(all_passed)} passed, {len(all_failed)} failed")
    if all_failed:
        print(f"Failed: {', '.join(all_failed)}")
        print(sep)
        sys.exit(1)
    else:
        print("✓ All states passed!")
        print(sep)


if __name__ == "__main__":
    classic_only = "--classic-only" in sys.argv
    v2_only = "--v2-only" in sys.argv
    nc_only = "--nc-only" in sys.argv
    main(
        run_classic=not v2_only and not nc_only,
        run_v2=not classic_only and not nc_only,
        run_nc=not classic_only and not v2_only,
    )
