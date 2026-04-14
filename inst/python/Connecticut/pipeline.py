"""
Connecticut CTEMS election results pipeline.

Orchestrates two phases per election:

1. **Statewide Summary** — render the Summary page and extract federal race
   totals.  An empty result is normal (Summary shows federal races only; many
   elections have no federal races on the ballot).

2. **Town-level** — enumerate every county + town, render each town page, and
   extract per-town candidate totals with election-level classification.

State-level DataFrame construction
------------------------------------
The final ``state`` DataFrame is assembled from two sources:

- **Federal rows** — taken directly from the statewide Summary page (already
  statewide totals).  If the Summary page is empty, federal races are instead
  aggregated from town data.
- **State + Local rows** — aggregated from town data (votes summed across all
  towns per contest, ``vote_pct`` recomputed from the aggregated totals).

This means the ``state`` DataFrame always contains one row per candidate per
office for the whole state, tagged with ``election_level``.

Public entry points
-------------------
``get_ct_election_results(year_from, year_to)``
    Returns a dict (or individual DataFrames) — called by the registry.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import warnings

import pandas as pd

from .client import CtPlaywrightClient
from .discovery import parse_election_options
from .models import CtElectionInfo
from .parser import (
    parse_statewide_results, parse_town_results,
    _STATE_COLS, _TOWN_COLS,
    _add_winner, _CONTEST_STATE_COLS,
)
from df_utils import concat_or_empty
from date_utils import year_to_date_range

# Grouping columns used when aggregating town data to state level.
_AGG_GROUP_COLS = [
    "election_name",
    "election_year",
    "election_date",
    "office_level",
    "office",
    "candidate",
    "party",
]

# Columns that identify a unique contest (for recomputing vote_pct).
_CONTEST_COLS = ["election_name", "election_year", "election_date", "office"]


def _aggregate_towns_to_state(town_df: pd.DataFrame) -> pd.DataFrame:
    """Sum town-level vote counts up to statewide totals and recompute vote_pct.

    Parameters
    ----------
    town_df : pd.DataFrame
        Town-level results (output of ``parse_town_results``), optionally
        pre-filtered to a subset of election levels.

    Returns
    -------
    pd.DataFrame
        One row per candidate per office for the whole state, with columns
        matching ``_STATE_COLS``.  ``vote_pct`` is recomputed from the
        aggregated vote totals.
    """
    if town_df.empty:
        return pd.DataFrame(columns=_STATE_COLS)

    # Sum votes across all towns for each (election, level, office, candidate, party).
    agg = (
        town_df
        .groupby(_AGG_GROUP_COLS, dropna=False)["votes"]
        .sum()
        .reset_index()
    )

    # Recompute vote_pct: candidate_votes / total_votes_in_contest × 100.
    contest_totals = agg.groupby(_CONTEST_COLS, dropna=False)["votes"].transform("sum")
    agg["vote_pct"] = (agg["votes"] / contest_totals * 100).round(2)

    # winner is added later by _build_state_df after all parts are combined.
    base_cols = [c for c in _STATE_COLS if c != "winner"]
    return agg[base_cols]


class CtElectionPipeline:
    """Two-phase pipeline for Connecticut CTEMS election results.

    Parameters
    ----------
    headless : bool
        Whether to run the browser in headless mode (default True).
    sleep_s : float
        Seconds to pause after each browser interaction (default 3.0).
    level : str
        What to scrape: ``'all'`` (default), ``'state'``, or ``'town'``.
    max_town_workers : int
        Parallel Chromium browsers for town scraping (default 2).
    """

    state = "CT"

    def __init__(
        self,
        headless: bool = True,
        sleep_s: float = 3.0,
        level: str = "all",
        max_town_workers: int = 2,
    ):
        if level not in ("all", "state", "town"):
            raise ValueError(f"level must be 'all', 'state', or 'town'; got {level!r}")
        self.headless = headless
        self.sleep_s = sleep_s
        self.level = level
        self.max_town_workers = max_town_workers

    # ── Phase 1: discovery ─────────────────────────────────────────────────────

    def discover(self) -> list[CtElectionInfo]:
        """Render the CTEMS home page and return all elections found in the dropdown."""
        print("[CT] Discovering elections from home page dropdown...")
        with CtPlaywrightClient(headless=self.headless, sleep_s=self.sleep_s) as client:
            html = client.get_landing_page()
        elections = parse_election_options(html)
        print(f"[CT] Discovered {len(elections)} election(s).")
        return elections

    # ── Filtering ──────────────────────────────────────────────────────────────

    def _filter(
        self,
        elections: list[CtElectionInfo],
        start_date: date | None,
        end_date: date | None,
    ) -> list[CtElectionInfo]:
        result = []
        for e in elections:
            if start_date is not None and e.year < start_date.year:
                continue
            if end_date is not None and e.year > end_date.year:
                continue
            result.append(e)
        return result

    # ── Phase 2a: statewide Summary scraping ───────────────────────────────────

    def _scrape_state_summary(self, election: CtElectionInfo) -> pd.DataFrame:
        """Render the statewide Summary page; return federal race rows (may be empty)."""
        with CtPlaywrightClient(headless=self.headless, sleep_s=self.sleep_s) as client:
            html = client.get_statewide_results(election.option_value)
        return parse_statewide_results(html, election)

    # ── Phase 2b: town discovery + scraping ────────────────────────────────────

    def _get_county_town_tree(
        self, election: CtElectionInfo
    ) -> list[tuple[str, str, list[tuple[str, str]]]]:
        """Return list of (county_name, county_value, [(town_name, town_value), ...])."""
        with CtPlaywrightClient(headless=self.headless, sleep_s=self.sleep_s) as client:
            return client.get_county_town_options(election.option_value)

    def _scrape_county(
        self,
        election: CtElectionInfo,
        county_name: str,
        county_value: str,
        towns: list[tuple[str, str]],
    ) -> list[pd.DataFrame]:
        """Open one browser session and scrape all towns in a county.

        Selects the county once, then iterates through every town by changing
        only the town dropdown — one browser launch per county instead of per town.
        Returns a list of non-empty town DataFrames.
        """
        frames: list[pd.DataFrame] = []
        with CtPlaywrightClient(headless=self.headless, sleep_s=self.sleep_s) as client:
            town_htmls = client.get_all_towns_for_county(
                election_option_value=election.option_value,
                county_name=county_name,
                county_option_value=county_value,
                towns=towns,
            )
        for town_name, html in town_htmls:
            df = parse_town_results(html, town_name, county_name, election)
            if not df.empty:
                frames.append(df)
        return frames

    # ── State-level assembly ───────────────────────────────────────────────────

    def _build_state_df(
        self,
        summary_df: pd.DataFrame,
        town_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Combine Summary-page federal rows + town-aggregated state/local rows.

        Logic:
        - Federal rows: use ``summary_df`` if non-empty; otherwise fall back to
          aggregating federal rows from ``town_df`` (so federal data is not lost
          when the Summary page is empty).
        - State + Local rows: always aggregated from ``town_df``.

        Parameters
        ----------
        summary_df : pd.DataFrame
            Output of ``_scrape_state_summary`` (may be empty).
        town_df : pd.DataFrame
            Combined town-level results across all towns for this election.

        Returns
        -------
        pd.DataFrame
            Statewide result with ``election_level`` column, one row per
            candidate per office.
        """
        parts: list[pd.DataFrame] = []

        if not summary_df.empty:
            # Federal races come directly from the statewide Summary page.
            parts.append(summary_df)

        if not town_df.empty:
            if summary_df.empty:
                # Summary was empty — include all levels from town aggregation.
                parts.append(_aggregate_towns_to_state(town_df))
            else:
                # Summary had federal data — only aggregate non-federal from towns
                # to avoid double-counting.
                non_federal = town_df[town_df["office_level"] != "Federal"]
                if not non_federal.empty:
                    parts.append(_aggregate_towns_to_state(non_federal))

        return _add_winner(concat_or_empty(parts), _CONTEST_STATE_COLS)

    # ── Orchestrator ───────────────────────────────────────────────────────────

    def run(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> "pd.DataFrame | dict":
        """Discover and scrape Connecticut CTEMS election results.

        Returns
        -------
        If ``level='state'``: pd.DataFrame — statewide totals (federal from
                               Summary page + state/local aggregated from towns).
        If ``level='town'``:  pd.DataFrame — town-level results.
        If ``level='all'``:   dict with keys ``'state'`` and ``'town'``.
                               Reticulate converts this to a named R list.
        """
        all_elections = self.discover()

        if not all_elections:
            warnings.warn(
                "[CT] Discovery returned 0 elections from the CTEMS home page. "
                "The site structure may have changed — inspect the rendered HTML "
                "to verify the election dropdown selector assumptions.",
                stacklevel=2,
            )
            empty = pd.DataFrame()
            if self.level == "all":
                return {"state": empty, "town": empty}
            return empty

        elections = self._filter(all_elections, start_date, end_date)

        if not elections:
            lo = start_date.isoformat() if start_date else "–"
            hi = end_date.isoformat()   if end_date   else "–"
            print(f"[CT] No elections found for range {lo} – {hi}.")
            empty = pd.DataFrame()
            if self.level == "all":
                return {"state": empty, "town": empty}
            return empty

        print(f"[CT] Scraping {len(elections)} election(s)...")
        state_frames: list[pd.DataFrame] = []
        town_frames:  list[pd.DataFrame] = []
        failed = 0

        for election in elections:
            print(f"[CT]   {election.year}: {election.name}")
            state_frames_before = len(state_frames)
            town_frames_before  = len(town_frames)

            # --- Statewide Summary (federal races; empty is normal) ---
            summary_df = pd.DataFrame()
            if self.level in ("all", "state"):
                try:
                    summary_df = self._scrape_state_summary(election)
                    n = len(summary_df)
                    if n:
                        print(f"[CT]     Summary page: {n} federal candidate row(s).")
                    else:
                        print("[CT]     Summary page: empty (no federal races — will use town aggregation).")
                except Exception as exc:
                    print(f"[CT] WARNING: Summary scrape failed for '{election.name}': {exc}")

            # --- Town-level (parallelised by county) ---
            election_town_frames: list[pd.DataFrame] = []
            if self.level in ("all", "town", "state"):
                # Town data is needed for town output AND for state aggregation.
                try:
                    county_town_tree = self._get_county_town_tree(election)
                except Exception as exc:
                    print(f"[CT] WARNING: Could not enumerate towns for '{election.name}': {exc}")
                    county_town_tree = []

                if county_town_tree:
                    total_towns = sum(len(towns) for _, _, towns in county_town_tree)
                    n_counties = len(county_town_tree)
                    w = min(self.max_town_workers, n_counties)
                    print(
                        f"[CT]     Scraping {total_towns} town(s) across "
                        f"{n_counties} county/counties ({w} parallel worker(s))..."
                    )
                    county_failed = 0

                    with ThreadPoolExecutor(max_workers=w) as pool:
                        futures = {
                            pool.submit(
                                self._scrape_county,
                                election,
                                county_name, county_value, towns,
                            ): county_name
                            for county_name, county_value, towns in county_town_tree
                        }
                        for future in as_completed(futures):
                            county_name = futures[future]
                            try:
                                county_frames = future.result()
                                election_town_frames.extend(county_frames)
                                print(f"[CT]     {county_name}: {len(county_frames)} town(s) with results.")
                            except Exception as exc:
                                county_failed += 1
                                print(f"[CT]     WARNING: county scrape failed for {county_name}: {exc}")

                    if county_failed:
                        print(f"[CT]     NOTE: {county_failed}/{n_counties} county scrape(s) failed.")

            election_town_df = concat_or_empty(election_town_frames)

            # --- Build state-level output for this election ---
            if self.level in ("all", "state"):
                state_df = self._build_state_df(summary_df, election_town_df)
                if not state_df.empty:
                    state_frames.append(state_df)

            if not election_town_df.empty:
                town_frames.append(election_town_df)

            # Count elections that produced no output at all
            if len(state_frames) == state_frames_before and len(town_frames) == town_frames_before:
                failed += 1

        if not state_frames and not town_frames:
            if failed == len(elections):
                raise RuntimeError(
                    f"[CT] All {len(elections)} election(s) failed to return any results. "
                    f"This usually means the site is unreachable or its structure has changed. "
                    f"See the warning messages printed above for details."
                )

        final_state_df = concat_or_empty(state_frames)
        final_town_df  = concat_or_empty(town_frames)

        for _df in [final_state_df, final_town_df]:
            if not _df.empty:
                _df.insert(0, "state", "CT")

        print(
            f"[CT] Done. {len(final_state_df):,} total state rows, {len(final_town_df):,} total town rows."
        )

        if self.level == "state":
            return final_state_df
        if self.level == "town":
            return final_town_df
        return {"state": final_state_df, "town": final_town_df}


# ── Public entry point (called by registry.py) ────────────────────────────────

def get_ct_election_results(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_town_workers: int = 2,
):
    """Return Connecticut CTEMS election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.  ``None`` applies no lower bound.
    year_to : int | None
        End year, inclusive.  ``None`` applies no upper bound.
    level : str
        What to return:
          - ``'all'``   (default) dict with keys ``'state'`` and ``'town'``;
                        reticulate converts this to a named R list.
          - ``'state'`` statewide totals (federal from Summary + state/local
                        aggregated from towns).  Town pages are still scraped
                        internally to build the aggregation.
          - ``'town'``  town-level DataFrame only (no state aggregation).
    max_town_workers : int
        Parallel Chromium browsers for town scraping (default 2).
    """
    start, end = year_to_date_range(year_from, year_to)
    pipeline = CtElectionPipeline(
        level=level,
        max_town_workers=max_town_workers,
    )
    return pipeline.run(start_date=start, end_date=end)
