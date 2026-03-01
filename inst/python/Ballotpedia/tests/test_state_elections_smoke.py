"""
Smoke tests for the Ballotpedia state elections scraper.

Verifies that:
  - Supported years (2024, 2025) return candidate rows for representative states
  - Unsupported years (e.g. 2023, 1980) print an informative message and
    return an empty result rather than crashing
  - States without a Ballotpedia election page for a given year print a
    "no data available" message and return an empty result
  - Level filtering (federal / state / local) works correctly
  - Column schema matches the expected StateElectionCandidateRow fields

Run from inst/python/:
    python -m Ballotpedia.tests.test_state_elections_smoke
    python -m Ballotpedia.tests.test_state_elections_smoke --year 2024
    python -m Ballotpedia.tests.test_state_elections_smoke --year 2025
    python -m Ballotpedia.tests.test_state_elections_smoke --unsupported-only
"""

from __future__ import annotations

import sys
from dataclasses import fields
from typing import List, Tuple

from Ballotpedia.state_elections import StateElectionCandidateRow, StateElectionsScraper

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# States confirmed to have 2024 pages; used for the supported-year tests.
# Keep this list small — we're checking reachability + parse, not exhaustive coverage.
_STATES_2024 = ["Maine", "Pennsylvania", "Virginia"]

# States to try for 2025 — may be empty if pages don't exist yet; that's OK.
_STATES_2025 = ["Maine", "Virginia"]

# A state that very likely has no page for the given year (used to exercise
# the "no data" code path without a real 404 year).
_NO_PAGE_STATE = "Wyoming"   # uncommon enough that missing pages are plausible
_NO_PAGE_YEAR  = 2025        # change if Wyoming 2025 ever appears

# Expected column names (derived from dataclass field names)
_EXPECTED_COLUMNS = {f.name for f in fields(StateElectionCandidateRow)}

# Supported year lower bound (must match registry.py / state_elections.py)
_SUPPORTED_FROM = 2024


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scraper() -> StateElectionsScraper:
    return StateElectionsScraper(sleep_s=1.0)


def _check_columns(rows: list, label: str) -> None:
    """Verify every row has exactly the expected fields."""
    from dataclasses import asdict
    for row in rows[:3]:  # spot-check first few rows
        keys = set(asdict(row).keys())
        missing = _EXPECTED_COLUMNS - keys
        extra   = keys - _EXPECTED_COLUMNS
        assert not missing, f"[{label}] Missing columns: {missing}"
        assert not extra,   f"[{label}] Unexpected columns: {extra}"


# ---------------------------------------------------------------------------
# Supported-year tests (2024)
# ---------------------------------------------------------------------------

def smoke_state_2024(state: str) -> List[StateElectionCandidateRow]:
    """Fetch all candidates for a state in 2024 and validate the result."""
    scraper = _scraper()
    rows = scraper.scrape_listings(year=2024, state=state, level="all")

    assert len(rows) > 0, (
        f"Expected rows for {state} 2024 but got none. "
        f"Page may not exist or structure may have changed."
    )

    # At least one known level should be present
    levels = {r.level for r in rows}
    assert levels.issubset({"federal", "state", "local"}), \
        f"Unexpected level values: {levels - {'federal', 'state', 'local'}}"
    assert len(levels) > 0, f"No levels found in {state} 2024 rows"

    # Every row must have a candidate name and a contest URL
    missing_cand = [r for r in rows if not r.candidate]
    assert not missing_cand, \
        f"{len(missing_cand)} rows have no candidate name in {state} 2024"

    missing_url = [r for r in rows if not r.contest_url]
    assert not missing_url, \
        f"{len(missing_url)} rows have no contest_url in {state} 2024"

    _check_columns(rows, f"{state} 2024")

    sample = rows[0]
    print(
        f"  ✓ {len(rows):,} rows | levels={sorted(levels)} | "
        f"sample: [{sample.level}] {sample.office} — {sample.candidate} ({sample.party})"
    )
    return rows


# ---------------------------------------------------------------------------
# Supported-year tests (2025)
# ---------------------------------------------------------------------------

def smoke_state_2025(state: str) -> List[StateElectionCandidateRow]:
    """Fetch candidates for a state in 2025; allow empty if page not yet live."""
    scraper = _scraper()
    rows = scraper.scrape_listings(year=2025, state=state, level="all")

    if not rows:
        print(f"  ⚠  No rows for {state} 2025 (page may not exist yet) — skipped")
        return []

    _check_columns(rows, f"{state} 2025")
    sample = rows[0]
    print(
        f"  ✓ {len(rows):,} rows | "
        f"sample: [{sample.level}] {sample.office} — {sample.candidate}"
    )
    return rows


# ---------------------------------------------------------------------------
# Level-filter test
# ---------------------------------------------------------------------------

def smoke_level_filter(state: str = "Maine", year: int = 2024) -> None:
    """Verify that election_level filtering returns subsets of the full result."""
    scraper = _scraper()
    all_rows     = scraper.scrape_listings(year=year, state=state, level="all")
    federal_rows = scraper.scrape_listings(year=year, state=state, level="federal")
    state_rows   = scraper.scrape_listings(year=year, state=state, level="state")
    local_rows   = scraper.scrape_listings(year=year, state=state, level="local")

    assert len(all_rows) > 0, f"No rows for {state} {year} (all levels)"

    # Filtered sets must be subsets
    assert len(federal_rows) <= len(all_rows)
    assert len(state_rows)   <= len(all_rows)
    assert len(local_rows)   <= len(all_rows)

    # Filtered rows must not contain other levels
    assert all(r.level == "federal" for r in federal_rows), \
        "federal filter returned non-federal rows"
    assert all(r.level == "state" for r in state_rows), \
        "state filter returned non-state rows"
    assert all(r.level == "local" for r in local_rows), \
        "local filter returned non-local rows"

    # Combined filtered count should equal total
    combined = len(federal_rows) + len(state_rows) + len(local_rows)
    assert combined == len(all_rows), (
        f"Level filter counts don't add up: "
        f"{len(federal_rows)} + {len(state_rows)} + {len(local_rows)} "
        f"= {combined} ≠ {len(all_rows)} total"
    )

    print(
        f"  ✓ Level filter: federal={len(federal_rows)}, "
        f"state={len(state_rows)}, local={len(local_rows)}, "
        f"total={len(all_rows)}"
    )


# ---------------------------------------------------------------------------
# Unsupported-year tests
# ---------------------------------------------------------------------------

def smoke_unsupported_year(year: int, state: str = "Maine") -> None:
    """Verify that requesting an unsupported year returns empty + prints a message."""
    import io, contextlib

    scraper = _scraper()
    buf = io.StringIO()

    with contextlib.redirect_stdout(buf):
        rows = scraper.scrape_listings(year=year, state=state, level="all")

    output = buf.getvalue()

    # Should return no rows (either 404 or page with wrong layout)
    assert len(rows) == 0, (
        f"Expected 0 rows for unsupported year {year} but got {len(rows)}"
    )

    # Should have printed something (either the "no data" message or similar)
    assert output.strip(), (
        f"Expected a printed message for unsupported year {year} but got none"
    )

    print(f"  ✓ {year} → 0 rows | message: {output.strip()[:80]!r}")


def smoke_unsupported_year_registry(year: int, state: str = "Maine") -> None:
    """Verify the registry-level early-exit and message for unsupported years."""
    import io, contextlib
    import datetime

    # Import registry directly (avoids needing full R/reticulate stack)
    sys.path.insert(0, ".")
    import registry

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        result = registry._scrape_ballotpedia_elections(
            year=year, state=state, mode="listings", election_level="all"
        )

    output = buf.getvalue()

    # Must be an empty DataFrame (early exit)
    assert hasattr(result, "empty"), "Expected a DataFrame from registry function"
    assert result.empty, f"Expected empty DataFrame for year {year}, got {len(result)} rows"

    # Message must mention available years
    assert str(_SUPPORTED_FROM) in output, (
        f"Expected '{_SUPPORTED_FROM}' in warning message but got: {output!r}"
    )

    print(f"  ✓ Registry early-exit for {year} | message: {output.strip()[:80]!r}")


# ---------------------------------------------------------------------------
# No-page test (state with no Ballotpedia page for the year)
# ---------------------------------------------------------------------------

def smoke_no_page_state(state: str = _NO_PAGE_STATE, year: int = _NO_PAGE_YEAR) -> None:
    """Verify that a state without a page returns 0 rows and a helpful message."""
    import io, contextlib

    scraper = _scraper()
    buf = io.StringIO()

    with contextlib.redirect_stdout(buf):
        rows = scraper.scrape_listings(year=year, state=state, level="all")

    output = buf.getvalue()

    if len(rows) > 0:
        # The page exists after all — that's fine, just note it
        print(
            f"  ⚠  {state} {year} has data ({len(rows)} rows) — "
            f"no-page test not exercised; pick a different state/year"
        )
        return

    # 0 rows — check that a message was printed
    assert output.strip(), (
        f"Expected a 'no data' message for {state} {year} but nothing was printed"
    )
    print(f"  ✓ {state} {year} → 0 rows | message: {output.strip()[:80]!r}")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def _section(title: str) -> None:
    sep = "=" * 60
    print(f"\n{sep}\n{title}\n{sep}")


def _run(label: str, fn, *args) -> Tuple[bool, str]:
    print(f"\n[{label}]")
    try:
        fn(*args)
        return True, label
    except Exception as exc:
        print(f"  ✗ FAILED: {exc}")
        return False, label


def main(run_2024: bool = True, run_2025: bool = True, run_unsupported: bool = True) -> None:
    passed: List[str] = []
    failed: List[str] = []

    def record(ok: bool, label: str) -> None:
        (passed if ok else failed).append(label)

    # ── 2024 supported-year tests ─────────────────────────────────────────────
    if run_2024:
        _section("Supported year: 2024")
        for state in _STATES_2024:
            ok, lbl = _run(f"{state} 2024 listings", smoke_state_2024, state)
            record(ok, lbl)

        ok, lbl = _run("Level filter (Maine 2024)", smoke_level_filter, "Maine", 2024)
        record(ok, lbl)

    # ── 2025 supported-year tests ─────────────────────────────────────────────
    if run_2025:
        _section("Supported year: 2025")
        for state in _STATES_2025:
            ok, lbl = _run(f"{state} 2025 listings", smoke_state_2025, state)
            record(ok, lbl)

    # ── Unsupported-year / no-page tests ─────────────────────────────────────
    if run_unsupported:
        _section("Unsupported years & missing pages")

        for bad_year in [2023, 1980]:
            ok, lbl = _run(
                f"Scraper: unsupported year {bad_year}",
                smoke_unsupported_year, bad_year,
            )
            record(ok, lbl)

            ok, lbl = _run(
                f"Registry: unsupported year {bad_year}",
                smoke_unsupported_year_registry, bad_year,
            )
            record(ok, lbl)

        ok, lbl = _run(
            f"No page: {_NO_PAGE_STATE} {_NO_PAGE_YEAR}",
            smoke_no_page_state,
        )
        record(ok, lbl)

    # ── Summary ───────────────────────────────────────────────────────────────
    sep = "=" * 60
    print(f"\n{sep}")
    print(f"Results: {len(passed)} passed, {len(failed)} failed")
    if failed:
        print(f"Failed:  {', '.join(failed)}")
        print(sep)
        sys.exit(1)
    else:
        print("✓ All tests passed!")
        print(sep)


if __name__ == "__main__":
    year_2024_only     = "--year" in sys.argv and "2024" in sys.argv
    year_2025_only     = "--year" in sys.argv and "2025" in sys.argv
    unsupported_only   = "--unsupported-only" in sys.argv

    if unsupported_only:
        main(run_2024=False, run_2025=False, run_unsupported=True)
    elif year_2024_only:
        main(run_2024=True, run_2025=False, run_unsupported=False)
    elif year_2025_only:
        main(run_2024=False, run_2025=True, run_unsupported=False)
    else:
        main()
