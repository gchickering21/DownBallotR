"""
Georgia SOS election results pipeline.

Orchestrates two phases per election:

1. **State-level** — render the election page and extract statewide candidate
   totals.  Also collects the per-county page URLs from the locality dropdown.

2. **County-level** — for each county URL, render the county election page
   and extract per-county candidate totals.

Vote-method breakdown
---------------------
When ``include_vote_methods=True`` the client clicks every contest's "Vote
Method" toggle before capturing the HTML.  Angular replaces the bar-chart view
with a ``<table class="contest-table">`` showing votes split by method
(Advance in Person / Election Day / Absentee by Mail / Provisional).  A
``vote_method_df`` is returned alongside the normal DataFrames.

Public entry points
-------------------
``get_ga_election_results(year_from, year_to)``
    Returns a dict (or individual DataFrames) — called by the registry.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import pandas as pd

from .client import GaPlaywrightClient
from .discovery import parse_election_links
from .models import GaElectionInfo
from .parser import parse_state_results, parse_county_results, county_name_from_url
from df_utils import concat_or_empty
from date_utils import year_to_date_range


class GaElectionPipeline:
    """Two-phase pipeline for Georgia SOS election results.

    Parameters
    ----------
    headless : bool
        Whether to run the browser in headless mode (default True).
    sleep_s : float
        Seconds to pause after each page load (default 3.0).
    level : str
        What to scrape: ``'all'`` (default), ``'state'``, or ``'county'``.
    max_county_workers : int
        Parallel Chromium browsers for county scraping (default 2).
    include_vote_methods : bool
        When True, clicks each contest's "Vote Method" toggle before capturing
        the HTML, returning per-vote-method vote counts alongside the normal
        totals (default False).
    """

    state = "GA"

    def __init__(
        self,
        headless: bool = True,
        sleep_s: float = 3.0,
        level: str = "all",
        max_county_workers: int = 2,
        include_vote_methods: bool = False,
    ):
        if level not in ("all", "state", "county"):
            raise ValueError(f"level must be 'all', 'state', or 'county'; got {level!r}")
        self.headless = headless
        self.sleep_s = sleep_s
        self.level = level
        self.max_county_workers = max_county_workers
        self.include_vote_methods = include_vote_methods

    # ── Phase 1: discovery ─────────────────────────────────────────────────────

    def discover(self) -> list[GaElectionInfo]:
        """Render the landing page and return all elections found."""
        print("[GA] Discovering elections from landing page...")
        with GaPlaywrightClient(headless=self.headless, sleep_s=self.sleep_s) as client:
            html = client.get_landing_page()
        elections = parse_election_links(html)
        print(f"[GA] Discovered {len(elections)} election(s).")
        return elections

    # ── Filtering ──────────────────────────────────────────────────────────────

    def _filter(
        self,
        elections: list[GaElectionInfo],
        start_date: date | None,
        end_date: date | None,
    ) -> list[GaElectionInfo]:
        result = []
        for e in elections:
            if start_date is not None and e.year < start_date.year:
                continue
            if end_date is not None and e.year > end_date.year:
                continue
            result.append(e)
        return result

    # ── Phase 2: scraping ──────────────────────────────────────────────────────

    def _scrape_state(
        self, election: GaElectionInfo
    ) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
        """Render the election page; return (state_df, vote_method_df, county_urls)."""
        with GaPlaywrightClient(headless=self.headless, sleep_s=self.sleep_s) as client:
            if self.include_vote_methods:
                html = client.get_election_page_with_vote_methods(election.url)
            else:
                html = client.get_election_page(election.url)
        return parse_state_results(html, election)

    def _scrape_county(
        self, url: str, election: GaElectionInfo
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Render one county page; return (county_df, vote_method_df)."""
        county = county_name_from_url(url)
        with GaPlaywrightClient(headless=self.headless, sleep_s=self.sleep_s) as client:
            if self.include_vote_methods:
                html = client.get_county_page_with_vote_methods(url)
            else:
                html = client.get_county_page(url)
        return parse_county_results(html, county, election)

    # ── Orchestrator ───────────────────────────────────────────────────────────

    def run(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> "pd.DataFrame | dict":
        """Discover and scrape Georgia election results.

        Returns
        -------
        If ``level='state'``: pd.DataFrame (statewide totals only).
        If ``level='county'``: pd.DataFrame (county-level only).
        If ``level='all'``:   dict with keys ``'state'``, ``'county'``, and
                               (when ``include_vote_methods=True``) ``'vote_method_state'``
                               and ``'vote_method_county'``.
                               Reticulate converts this to a named R list.
        """
        all_elections = self.discover()
        elections = self._filter(all_elections, start_date, end_date)

        if not elections:
            lo = start_date.isoformat() if start_date else "–"
            hi = end_date.isoformat()   if end_date   else "–"
            print(f"[GA] No elections found for range {lo} – {hi}.")
            empty = pd.DataFrame()
            if self.level == "all":
                result = {"state": empty, "county": empty}
                if self.include_vote_methods:
                    result["vote_method_state"] = empty
                    result["vote_method_county"] = empty
                return result
            return empty

        print(f"[GA] Scraping {len(elections)} election(s)...")
        state_frames: list[pd.DataFrame] = []
        county_frames: list[pd.DataFrame] = []
        vm_state_frames: list[pd.DataFrame] = []
        vm_county_frames: list[pd.DataFrame] = []
        failed: list[tuple[GaElectionInfo, Exception]] = []

        for election in elections:
            print(f"[GA]   {election.year}: {election.name} ({election.slug})")

            # --- State-level ---
            try:
                state_df, vm_state_df, county_urls = self._scrape_state(election)
                if not state_df.empty:
                    state_frames.append(state_df)
                if not vm_state_df.empty:
                    vm_state_frames.append(vm_state_df)
            except Exception as exc:
                failed.append((election, exc))
                print(f"[GA] WARNING: state scrape failed for '{election.name}': {exc}")
                continue

            # --- County-level (parallelised) ---
            if self.level in ("all", "county") and county_urls:
                n = len(county_urls)
                w = self.max_county_workers
                print(f"[GA]     Scraping {n} counties ({w} parallel workers)...")
                county_failed = 0

                with ThreadPoolExecutor(max_workers=w) as pool:
                    futures = {
                        pool.submit(self._scrape_county, url, election): url
                        for url in county_urls
                    }
                    for future in as_completed(futures):
                        url = futures[future]
                        county = county_name_from_url(url)
                        try:
                            cdf, vm_cdf = future.result()
                            if not cdf.empty:
                                county_frames.append(cdf)
                            if not vm_cdf.empty:
                                vm_county_frames.append(vm_cdf)
                        except Exception as exc:
                            county_failed += 1
                            print(
                                f"[GA]     WARNING: county scrape failed for {county}: {exc}"
                            )

                if county_failed:
                    print(f"[GA]     NOTE: {county_failed}/{n} county scrape(s) failed.")

        if failed:
            print(
                f"[GA] NOTE: {len(failed)} election(s) failed; "
                f"returning {len(state_frames)} successful result(s)."
            )

        state_df      = concat_or_empty(state_frames)
        county_df     = concat_or_empty(county_frames)
        vm_state_df   = concat_or_empty(vm_state_frames)
        vm_county_df  = concat_or_empty(vm_county_frames)

        if self.level == "state":
            if self.include_vote_methods:
                return {"state": state_df, "vote_method_state": vm_state_df}
            return state_df

        if self.level == "county":
            if self.include_vote_methods:
                return {"county": county_df, "vote_method_county": vm_county_df}
            return county_df

        # "all"
        result = {"state": state_df, "county": county_df}
        if self.include_vote_methods:
            result["vote_method_state"]  = vm_state_df
            result["vote_method_county"] = vm_county_df
        return result


# ── Public entry point (called by registry.py) ────────────────────────────────

def get_ga_election_results(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_county_workers: int = 2,
    include_vote_methods: bool = False,
):
    """Return Georgia election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.  ``None`` applies no lower bound.
    year_to : int | None
        End year, inclusive.  ``None`` applies no upper bound.
    level : str
        What to return:
          - ``'all'``    (default) dict with keys ``'state'`` and ``'county'``
                        (plus ``'vote_method_state'`` / ``'vote_method_county'``
                        when ``include_vote_methods=True``);
                        reticulate converts this to a named R list.
          - ``'state'``  statewide totals only (skips county scraping).
          - ``'county'`` county-level only.
    max_county_workers : int
        Parallel Chromium browsers for county scraping (default 2).
    include_vote_methods : bool
        When True, expands each contest's vote-method breakdown table and
        returns per-method vote counts (Advance in Person, Election Day,
        Absentee by Mail, Provisional) in addition to the normal totals.
        Adds ``vote_method_state`` / ``vote_method_county`` to the result dict.
        Note: vote-method expansion requires clicking a toggle per contest and
        adds extra time to each page load (default False).
    """
    start, end = year_to_date_range(year_from, year_to)
    pipeline = GaElectionPipeline(
        level=level,
        max_county_workers=max_county_workers,
        include_vote_methods=include_vote_methods,
    )
    return pipeline.run(start_date=start, end_date=end)
