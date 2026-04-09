"""
Indiana General Election results pipeline.

Two-stage process:
  1. Discovery — fetch the IN SOS landing page and extract all
     ``enr.indianavoters.in.gov/archive/{YEAR}General/`` URLs for years in
     the requested range (probes directly for years not yet on the page).
  2. Scraping — for each election, fetch all OffCatC JSON files and parse
     statewide + county-level results into DataFrames.

Public entry point
------------------
``get_in_election_results(year_from, year_to, level)``
    Called by registry.py.
"""

from __future__ import annotations

import pandas as pd

from .client import InElectionClient
from .discovery import discover_general_elections
from .models import InElectionInfo
from .parser import (
    parse_office_categories,
    parse_state_results,
    parse_county_results,
)
from df_utils import concat_or_empty


class InElectionPipeline:
    """Pipeline for Indiana General Election results.

    Parameters
    ----------
    level : str
        ``'all'`` (default) — dict with ``'state'`` and ``'county'`` DataFrames;
        ``'state'`` — statewide candidate totals only;
        ``'county'`` — county-level totals only.
    """

    def __init__(self, level: str = "all"):
        if level not in ("all", "state", "county"):
            raise ValueError(f"level must be 'all', 'state', or 'county'; got {level!r}")
        self.level = level

    def _scrape_election(
        self, election: InElectionInfo
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Scrape one General Election; return (state_df, county_df)."""
        client = InElectionClient(election.archive_base_url)

        # Get office category list + precinct totals
        cat_data = client.get_office_categories()
        root = cat_data.get("Root", {})
        jr = root.get("JurisdictionsReporting", {})
        precincts_reporting = int(jr.get("TOTAL_ALLPRECINCTSREPORTING", 0))
        total_precincts     = int(jr.get("TOTAL_ALLPRECINCTS", 0))

        categories = parse_office_categories(cat_data)
        print(
            f"[IN]   {election.year}General: "
            f"{len(categories)} office categories, "
            f"{precincts_reporting}/{total_precincts} precincts reporting"
        )

        state_frames:  list[pd.DataFrame] = []
        county_frames: list[pd.DataFrame] = []
        failed = 0

        for cat in categories:
            cat_id   = cat.get("OFFICECATEGORYID", "")
            cat_name = cat.get("OFFICE_CATEGORY_NAME", cat_id)
            if not cat_id:
                continue
            try:
                offcat_data = client.get_office_category(cat_id)
            except Exception as exc:
                failed += 1
                print(f"[IN]     WARNING: failed to fetch OffCatC_{cat_id}: {exc}")
                continue

            office_level = cat.get("_heading", "Local")

            if self.level in ("all", "state"):
                df = parse_state_results(
                    offcat_data, election, cat_name, office_level,
                    precincts_reporting, total_precincts,
                )
                if not df.empty:
                    state_frames.append(df)

            if self.level in ("all", "county"):
                df = parse_county_results(offcat_data, election, cat_name, office_level)
                if not df.empty:
                    county_frames.append(df)

        if failed:
            print(f"[IN]   NOTE: {failed} office category fetch(es) failed.")

        return concat_or_empty(state_frames), concat_or_empty(county_frames)

    def run(
        self,
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> "pd.DataFrame | dict":
        """Discover and scrape Indiana General Election results.

        Returns
        -------
        If ``level='state'``: pd.DataFrame.
        If ``level='county'``: pd.DataFrame.
        If ``level='all'``: dict with keys ``'state'`` and ``'county'``.
        """
        elections = discover_general_elections(year_from=year_from, year_to=year_to)

        if not elections:
            lo = year_from or "–"
            hi = year_to or "–"
            print(f"[IN] No General Elections found for range {lo}–{hi}.")
            empty = pd.DataFrame()
            return {"state": empty, "county": empty} if self.level == "all" else empty

        print(f"[IN] Scraping {len(elections)} election(s)...")
        all_state:  list[pd.DataFrame] = []
        all_county: list[pd.DataFrame] = []

        for election in elections:
            print(f"[IN] Scraping {election.year}General ({election.election_date})...")
            try:
                state_df, county_df = self._scrape_election(election)
                print(
                    f"[IN]   Done: {len(state_df):,} state rows, "
                    f"{len(county_df):,} county rows."
                )
                if not state_df.empty:
                    all_state.append(state_df)
                if not county_df.empty:
                    all_county.append(county_df)
            except Exception as exc:
                print(f"[IN]   ERROR scraping {election.year}General: {exc}")

        state_df  = concat_or_empty(all_state)
        county_df = concat_or_empty(all_county)

        if self.level == "state":
            return state_df
        if self.level == "county":
            return county_df
        return {"state": state_df, "county": county_df}


# ── Public entry point (called by registry.py) ────────────────────────────────

def get_in_election_results(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
) -> "pd.DataFrame | dict":
    """Return Indiana General Election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive (default: 2020).
    year_to : int | None
        End year, inclusive (default: current calendar year).
    level : str
        ``'all'`` (default) — dict with ``'state'`` and ``'county'`` DataFrames;
        ``'state'`` — statewide candidate totals only;
        ``'county'`` — county-level totals only.
    """
    pipeline = InElectionPipeline(level=level)
    return pipeline.run(year_from=year_from, year_to=year_to)
