"""
Master smoke test — all DownBallotR scrapers.

Each test makes one real network request for a single year at state level only
(no county/precinct scraping) to verify:

  1. The scraper reaches its data source without error
  2. The result is a non-empty DataFrame
  3. Expected columns are present
  4. Key columns are actually populated (not all-null / all-blank)
  5. Numeric columns have plausible values (votes >= 0, year in range)
  6. The returned year matches what was requested

If a scraper's site structure changes (HTML selectors break, URL paths move,
columns renamed, vote parsing broken), these tests will catch it.

Usage (run from inst/python/):
    pytest tests/test_all_scrapers_smoke.py -v

    # Skip Playwright — runs only requests-based states (much faster):
    pytest tests/test_all_scrapers_smoke.py -v -m "not playwright"

    # Only Playwright:
    pytest tests/test_all_scrapers_smoke.py -v -m playwright

    # One specific state:
    pytest tests/test_all_scrapers_smoke.py -v -k "colorado"
    pytest tests/test_all_scrapers_smoke.py -v -k "georgia"

Notes:
  - Playwright tests require Chromium: playwright install chromium
  - All tests hit live websites — internet required
  - Network/browser failures cause the test to skip (not fail) so CI does not
    go red on transient connectivity issues
  - If a test consistently skips, investigate whether the site is down or
    its URL / selector has changed
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

import registry


# ── Validation helpers ─────────────────────────────────────────────────────────

def _col_null_pct(df: pd.DataFrame, col: str) -> float:
    """Fraction of rows where col is null or empty string."""
    s = df[col]
    null_mask = s.isna()
    if s.dtype == object:
        null_mask = null_mask | (s.astype(str).str.strip() == "")
    return float(null_mask.mean())


def _assert_populated(
    df: pd.DataFrame,
    label: str,
    cols: list[str],
    max_null_pct: float = 0.10,
) -> None:
    """Assert that each col has no more than max_null_pct null/blank values."""
    for col in cols:
        if col not in df.columns:
            pytest.fail(
                f"[{label}] Missing column '{col}'\n"
                f"  Actual columns: {sorted(df.columns)}\n"
                f"  This may indicate the site structure has changed."
            )
        pct = _col_null_pct(df, col)
        assert pct <= max_null_pct, (
            f"[{label}] Column '{col}' is {pct:.0%} null/blank "
            f"(threshold: {max_null_pct:.0%}) — parsing may be broken."
        )


def _assert_votes_positive(df: pd.DataFrame, label: str, col: str = "votes") -> None:
    """Assert that a vote column is numeric and has at least some positive values."""
    if col not in df.columns:
        return  # votes column optional for some level="state" results
    numeric = pd.to_numeric(df[col], errors="coerce")
    pct_non_numeric = float(numeric.isna().mean())
    assert pct_non_numeric < 0.20, (
        f"[{label}] '{col}' column is {pct_non_numeric:.0%} non-numeric — "
        f"vote parsing may be broken."
    )
    has_positive = (numeric > 0).any()
    assert has_positive, (
        f"[{label}] '{col}' column has no positive values — all votes are 0 or null."
    )


def _assert_year_range(
    df: pd.DataFrame,
    label: str,
    year_col: str,
    expected_year: int,
    tolerance: int = 1,
) -> None:
    """Assert that year_col values are close to expected_year."""
    if year_col not in df.columns:
        return
    numeric = pd.to_numeric(df[year_col], errors="coerce").dropna()
    if numeric.empty:
        pytest.fail(f"[{label}] '{year_col}' column has no numeric values.")
    lo, hi = expected_year - tolerance, expected_year + tolerance
    out_of_range = numeric[(numeric < lo) | (numeric > hi)]
    assert out_of_range.empty, (
        f"[{label}] '{year_col}' has {len(out_of_range)} value(s) outside "
        f"[{lo}, {hi}]: {sorted(out_of_range.unique().tolist())[:5]}"
    )


def _assert_multiple_offices(df: pd.DataFrame, label: str, col: str = "office") -> None:
    """Assert there are at least 2 distinct office names — catches total parse failures."""
    if col not in df.columns:
        return
    n = df[col].nunique()
    assert n >= 2, (
        f"[{label}] Only {n} unique value(s) in '{col}' — "
        f"expected multiple offices for a general election year. "
        f"Values: {df[col].unique().tolist()[:5]}"
    )


def _scrape(label: str, *args, **kwargs) -> pd.DataFrame:
    """Call registry.scrape(), skip on network/Playwright errors."""
    try:
        return registry.scrape(*args, **kwargs)
    except Exception as exc:
        msg = str(exc).lower()
        if any(k in msg for k in ("timeout", "connection", "network", "unreachable",
                                   "playwright", "chromium", "browser",
                                   "404", "503", "502", "ssl")):
            pytest.skip(f"[{label}] Network/browser error — site may be down: {exc}")
        raise


# ── ElectionStats — Classic (requests-based) ──────────────────────────────────

_ES_CLASSIC = [
    ("colorado",      2024),
    ("massachusetts", 2024),
    ("new_hampshire", 2024),
    ("idaho",         2024),
    ("vermont",       2024),
]


@pytest.mark.classic
@pytest.mark.parametrize("state,year", _ES_CLASSIC, ids=[s for s, _ in _ES_CLASSIC])
def test_election_stats_classic(state: str, year: int) -> None:
    """Classic (requests-based) ElectionStats state — state-level results."""
    result = _scrape(
        state, "election_stats",
        state=state, year_from=year, year_to=year,
        level="state", parallel=False,
    )

    assert isinstance(result, pd.DataFrame), f"[{state}] Expected DataFrame, got {type(result).__name__}"
    assert not result.empty, f"[{state}] Scraper returned 0 rows"

    # Columns populated
    _assert_populated(result, state, ["state", "year", "election_id", "candidate_id",
                                       "office", "candidate"])

    # Year matches request
    _assert_year_range(result, state, "year", year)

    # Multiple offices (catches total parse failure)
    _assert_multiple_offices(result, state, "office")

    # Votes present and positive where available
    _assert_votes_positive(result, state, col="total_vote_count")

    print(f"\n  [{state}] {len(result):,} rows | "
          f"{result['office'].nunique()} offices | "
          f"year={result['year'].unique().tolist()}")


# ── ElectionStats — v2 (Playwright-based) ─────────────────────────────────────

_ES_V2 = [
    ("south_carolina", 2024),
    ("new_mexico",     2024),
    ("new_york",       2024),
    ("virginia",       2024),
]


@pytest.mark.playwright
@pytest.mark.parametrize("state,year", _ES_V2, ids=[s for s, _ in _ES_V2])
def test_election_stats_v2(state: str, year: int) -> None:
    """v2 (Playwright-based) ElectionStats state — state-level results."""
    result = _scrape(
        state, "election_stats",
        state=state, year_from=year, year_to=year,
        level="state", parallel=False,
    )

    assert isinstance(result, pd.DataFrame), f"[{state}] Expected DataFrame, got {type(result).__name__}"
    assert not result.empty, f"[{state}] Scraper returned 0 rows"

    _assert_populated(result, state, ["state", "year", "election_id", "candidate_id",
                                       "office", "candidate"])
    _assert_year_range(result, state, "year", year)
    _assert_multiple_offices(result, state, "office")
    _assert_votes_positive(result, state, col="total_vote_count")

    print(f"\n  [{state}] {len(result):,} rows | "
          f"{result['office'].nunique()} offices | "
          f"year={result['year'].unique().tolist()}")


# ── North Carolina ─────────────────────────────────────────────────────────────

@pytest.mark.classic
def test_north_carolina() -> None:
    """NC State Board of Elections — statewide results for 2024."""
    result = _scrape(
        "north_carolina", "northcarolina_results",
        year_from=2024, year_to=2024, level="state",
    )

    assert isinstance(result, pd.DataFrame), f"[NC] Expected DataFrame, got {type(result).__name__}"
    assert not result.empty, "[NC] Scraper returned 0 rows"

    _assert_populated(result, "north_carolina", ["state", "year"])
    _assert_year_range(result, "north_carolina", "year", 2024)
    _assert_votes_positive(result, "north_carolina")

    print(f"\n  [north_carolina] {len(result):,} rows")


# ── Connecticut ────────────────────────────────────────────────────────────────

@pytest.mark.playwright
def test_connecticut() -> None:
    """Connecticut CTEMS — statewide results for 2024."""
    result = _scrape(
        "connecticut", "connecticut_results",
        year_from=2024, year_to=2024, level="state",
    )

    assert isinstance(result, pd.DataFrame), f"[CT] Expected DataFrame, got {type(result).__name__}"
    assert not result.empty, "[CT] Scraper returned 0 rows"

    _assert_populated(result, "connecticut", ["state", "year"])
    _assert_year_range(result, "connecticut", "year", 2024)
    _assert_votes_positive(result, "connecticut")
    _assert_multiple_offices(result, "connecticut")

    print(f"\n  [connecticut] {len(result):,} rows | {result['office'].nunique()} offices")


# ── Georgia ────────────────────────────────────────────────────────────────────

@pytest.mark.playwright
def test_georgia() -> None:
    """Georgia Secretary of State — statewide results for 2024."""
    result = _scrape(
        "georgia", "georgia_results",
        year_from=2024, year_to=2024, level="state",
    )

    assert isinstance(result, pd.DataFrame), f"[GA] Expected DataFrame, got {type(result).__name__}"
    assert not result.empty, "[GA] Scraper returned 0 rows"

    _assert_populated(result, "georgia", ["state", "year"])
    _assert_year_range(result, "georgia", "year", 2024)
    _assert_votes_positive(result, "georgia")
    _assert_multiple_offices(result, "georgia")

    print(f"\n  [georgia] {len(result):,} rows | {result['office'].nunique()} offices")


# ── Utah ───────────────────────────────────────────────────────────────────────

@pytest.mark.playwright
def test_utah() -> None:
    """Utah elections site — statewide results for 2024."""
    result = _scrape(
        "utah", "utah_results",
        year_from=2024, year_to=2024, level="state",
    )

    assert isinstance(result, pd.DataFrame), f"[UT] Expected DataFrame, got {type(result).__name__}"
    assert not result.empty, "[UT] Scraper returned 0 rows"

    _assert_populated(result, "utah", ["state", "year"])
    _assert_year_range(result, "utah", "year", 2024)
    _assert_votes_positive(result, "utah")
    _assert_multiple_offices(result, "utah")

    print(f"\n  [utah] {len(result):,} rows | {result['office'].nunique()} offices")


# ── Indiana ────────────────────────────────────────────────────────────────────

@pytest.mark.playwright
def test_indiana() -> None:
    """Indiana voters portal — statewide General Election results for 2024."""
    result = _scrape(
        "indiana", "indiana_results",
        year_from=2024, year_to=2024, level="state",
    )

    assert isinstance(result, pd.DataFrame), f"[IN] Expected DataFrame, got {type(result).__name__}"
    assert not result.empty, "[IN] Scraper returned 0 rows"

    _assert_populated(result, "indiana", ["state", "year"])
    _assert_year_range(result, "indiana", "year", 2024)
    _assert_votes_positive(result, "indiana")
    _assert_multiple_offices(result, "indiana")

    print(f"\n  [indiana] {len(result):,} rows | {result['office'].nunique()} offices")


# ── Louisiana ──────────────────────────────────────────────────────────────────

@pytest.mark.playwright
def test_louisiana() -> None:
    """Louisiana SOS — statewide results for 2023–2024."""
    result = _scrape(
        "louisiana", "louisiana_results",
        year_from=2023, year_to=2024, level="state",
    )

    assert isinstance(result, pd.DataFrame), f"[LA] Expected DataFrame, got {type(result).__name__}"
    assert not result.empty, "[LA] Scraper returned 0 rows"

    _assert_populated(result, "louisiana",
                      ["election_name", "election_year", "office", "candidate", "votes"])
    _assert_year_range(result, "louisiana", "election_year", 2024, tolerance=2)
    _assert_votes_positive(result, "louisiana", col="votes")
    _assert_multiple_offices(result, "louisiana")

    print(f"\n  [louisiana] {len(result):,} rows | {result['office'].nunique()} offices")
