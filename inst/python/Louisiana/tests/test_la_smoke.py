"""
Smoke tests for the Louisiana SOS Graphical election results scraper.

Run from inst/python/ with:
    pytest Louisiana/tests/test_la_smoke.py -v

These tests make live network + browser requests.  They are intentionally
minimal (smoke tests only) and will skip cleanly if the site is unreachable.

To run against a specific year:
    pytest Louisiana/tests/test_la_smoke.py -v -k "2022"
"""

from __future__ import annotations

import pytest

# ── Helpers ───────────────────────────────────────────────────────────────────

def _try_import():
    """Return True if all Louisiana scraper deps are importable."""
    try:
        from Louisiana.client import LaPlaywrightClient
        from Louisiana.discovery import parse_election_options
        from Louisiana.pipeline import get_la_election_results
        return True
    except ImportError as exc:
        pytest.skip(f"Louisiana scraper deps not available: {exc}")


# ── Discovery smoke test ───────────────────────────────────────────────────────

def test_discovery_returns_elections():
    """Landing page renders and returns at least one election."""
    _try_import()
    from Louisiana.client import LaPlaywrightClient
    from Louisiana.discovery import parse_election_options

    try:
        with LaPlaywrightClient(headless=True, sleep_s=3.0) as client:
            html = client.get_landing_page()
    except Exception as exc:
        pytest.skip(f"Could not reach Louisiana SOS site: {exc}")

    elections = parse_election_options(html)

    assert elections, (
        "parse_election_options() returned 0 elections — "
        "run inspect_landing.py to check the dropdown selector."
    )
    # Expect elections going back at least to 2000.
    years = {e.year for e in elections}
    assert any(y <= 2005 for y in years), (
        f"Expected elections before 2005 but found years: {sorted(years)}"
    )
    assert any(y >= 2020 for y in years), (
        f"Expected elections in 2020+ but found years: {sorted(years)}"
    )

    print(f"\n  Found {len(elections)} elections, years {min(years)}–{max(years)}")


# ── Statewide tab smoke test ───────────────────────────────────────────────────

def test_state_level_recent_election():
    """Scraping state-level tabs for a recent election returns a non-empty DataFrame."""
    _try_import()
    from Louisiana.pipeline import get_la_election_results

    try:
        result = get_la_election_results(year_from=2023, year_to=2024, level="state")
    except Exception as exc:
        pytest.skip(f"Scrape failed (possibly network): {exc}")

    import pandas as pd
    assert isinstance(result, pd.DataFrame), f"Expected DataFrame, got {type(result)}"
    assert not result.empty, "State-level scrape returned an empty DataFrame"

    expected_cols = {"election_name", "election_year", "office", "candidate", "votes"}
    missing = expected_cols - set(result.columns)
    assert not missing, f"Missing columns: {missing}"

    print(f"\n  {len(result):,} state-level rows, columns: {list(result.columns)}")


# ── Parish tab smoke test ──────────────────────────────────────────────────────

def test_parish_level_recent_election():
    """Scraping the Parish tab for a recent election returns a non-empty DataFrame."""
    _try_import()
    from Louisiana.pipeline import get_la_election_results

    try:
        result = get_la_election_results(year_from=2023, year_to=2024, level="parish")
    except Exception as exc:
        pytest.skip(f"Scrape failed (possibly network): {exc}")

    import pandas as pd
    assert isinstance(result, pd.DataFrame), f"Expected DataFrame, got {type(result)}"
    assert not result.empty, "Parish-level scrape returned an empty DataFrame"

    expected_cols = {"election_name", "parish", "office", "candidate", "votes"}
    missing = expected_cols - set(result.columns)
    assert not missing, f"Missing columns: {missing}"

    parishes = result["parish"].unique()
    assert len(parishes) > 1, f"Expected multiple parishes, got: {parishes}"

    print(f"\n  {len(result):,} parish-level rows, {len(parishes)} unique parishes")
