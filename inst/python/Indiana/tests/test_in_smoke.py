"""
Smoke test for the Indiana General Election scraper.

Run from inst/python/:
    pytest Indiana/tests/test_in_smoke.py -v
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import pytest

from Indiana.pipeline import get_in_election_results


def test_scrape_2020_general_state_only():
    """Statewide results for 2020 General should have expected shape."""
    result = get_in_election_results(year_from=2020, year_to=2020, level="state")
    assert isinstance(result, pd.DataFrame), "Expected a DataFrame"
    assert not result.empty, "State DataFrame should not be empty"
    assert "candidate_name" in result.columns
    assert "votes" in result.columns
    assert "office_title" in result.columns
    assert (result["election_year"] == 2020).all()
    assert (result["election_type"] == "General").all()
    # Should contain the presidential race
    assert result["office_title"].str.contains("President", case=False).any()
    print(f"\nState rows: {len(result):,}")
    print(result[["office_title", "candidate_name", "party", "votes"]].head(10).to_string())


def test_scrape_2020_general_county():
    """County-level results for 2020 General should have county_name column."""
    result = get_in_election_results(year_from=2020, year_to=2020, level="county")
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "county_name" in result.columns
    assert "county_fips" in result.columns
    # Indiana has 92 counties
    n_counties = result["county_name"].nunique()
    assert n_counties > 80, f"Expected 92 counties, got {n_counties}"
    print(f"\nCounty rows: {len(result):,}, unique counties: {n_counties}")


def test_scrape_2020_general_all():
    """level='all' should return a dict with 'state' and 'county' keys."""
    result = get_in_election_results(year_from=2020, year_to=2020, level="all")
    assert isinstance(result, dict)
    assert "state" in result
    assert "county" in result
    assert isinstance(result["state"], pd.DataFrame)
    assert isinstance(result["county"], pd.DataFrame)
    assert not result["state"].empty
    assert not result["county"].empty
