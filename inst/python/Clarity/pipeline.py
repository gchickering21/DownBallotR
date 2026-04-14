"""
Generic pipeline for Angular/PrimeNG SOS election results sites.

Orchestrates two phases per election:

1. **State-level** — render the election page and extract statewide candidate
   totals.  Also collects per-county page URLs from the locality dropdown.

2. **County-level** — for each county URL, render the county election page
   and extract per-county candidate totals.

This module is parameterised on the state-specific configuration (base URL,
county suffix, log prefix) and is not intended to be called directly.
Use the state-specific entry points instead:

  - ``Georgia.pipeline.get_ga_election_results``
  - ``Utah.pipeline.get_ut_election_results``

Public entry point
------------------
``get_clarity_election_results(base_url, county_suffix, log_prefix, ...)``
    Returns a dict (or individual DataFrames) — called by state wrappers.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import warnings

import pandas as pd

from .client import ClarityPlaywrightClient
from .discovery import parse_election_links
from .models import ClarityElectionInfo
from .parser import parse_state_results, parse_county_results, county_name_from_url
from df_utils import concat_or_empty
from date_utils import year_to_date_range


class ClarityPipeline:
    """Generic two-phase pipeline for Angular/PrimeNG SOS election results.

    Parameters
    ----------
    base_url : str
        Landing page URL, e.g.
        ``"https://results.sos.ga.gov/results/public/Georgia"``.
    county_suffix : str
        State-specific suffix used in county URL slugs, e.g. ``"-ga"`` or
        ``"-ut"``.  Used to extract a human-readable county name from URLs.
    log_prefix : str
        Short prefix for console messages, e.g. ``"[GA]"`` or ``"[UT]"``.
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
        the HTML, returning per-vote-method vote counts (default False).
    """

    def __init__(
        self,
        base_url: str,
        county_suffix: str,
        state_abbrev: str,
        log_prefix: str = "",
        headless: bool = True,
        sleep_s: float = 3.0,
        level: str = "all",
        max_county_workers: int = 2,
        include_vote_methods: bool = False,
    ):
        if level not in ("all", "state", "county"):
            raise ValueError(f"level must be 'all', 'state', or 'county'; got {level!r}")
        self.base_url = base_url
        self.county_suffix = county_suffix
        self.state_abbrev = state_abbrev
        self.log_prefix = log_prefix
        self.headless = headless
        self.sleep_s = sleep_s
        self.level = level
        self.max_county_workers = max_county_workers
        self.include_vote_methods = include_vote_methods

    def _client(self) -> ClarityPlaywrightClient:
        return ClarityPlaywrightClient(
            base_url=self.base_url,
            log_prefix=self.log_prefix,
            headless=self.headless,
            sleep_s=self.sleep_s,
        )

    def _p(self, msg: str) -> None:
        """Print with the state log prefix."""
        prefix = f"{self.log_prefix} " if self.log_prefix else ""
        print(f"{prefix}{msg}")

    # ── Phase 1: discovery ─────────────────────────────────────────────────────

    def discover(self) -> list[ClarityElectionInfo]:
        """Render the landing page and return all elections found."""
        self._p("Discovering elections from landing page...")
        with self._client() as client:
            html = client.get_landing_page()
        elections = parse_election_links(html, self.base_url)
        self._p(f"Discovered {len(elections)} election(s).")
        return elections

    # ── Filtering ──────────────────────────────────────────────────────────────

    def _filter(
        self,
        elections: list[ClarityElectionInfo],
        start_date: date | None,
        end_date: date | None,
    ) -> list[ClarityElectionInfo]:
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
        self, election: ClarityElectionInfo
    ) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
        """Render the election page; return (state_df, vote_method_df, county_urls)."""
        with self._client() as client:
            if self.include_vote_methods:
                html = client.get_election_page_with_vote_methods(election.url)
            else:
                html = client.get_election_page(election.url)
        return parse_state_results(html, election)

    def _scrape_county(
        self, url: str, election: ClarityElectionInfo
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Render one county page; return (county_df, vote_method_df)."""
        county = county_name_from_url(url, self.county_suffix)
        with self._client() as client:
            if self.include_vote_methods:
                html = client.get_county_page_with_vote_methods(url)
            else:
                html = client.get_county_page(url)
        return parse_county_results(html, county, election, url=url)

    # ── Orchestrator ───────────────────────────────────────────────────────────

    def run(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> "pd.DataFrame | dict":
        """Discover and scrape election results.

        Returns
        -------
        If ``level='state'``: pd.DataFrame (statewide totals only).
        If ``level='county'``: pd.DataFrame (county-level only).
        If ``level='all'``:   dict with keys ``'state'``, ``'county'``, and
                               optionally ``'vote_method_state'`` and
                               ``'vote_method_county'``.
        """
        all_elections = self.discover()

        if not all_elections:
            warnings.warn(
                f"{self.log_prefix} Discovery returned 0 elections from the landing "
                "page. The site structure may have changed — run the inspect script "
                "to save the rendered HTML and verify selector assumptions.",
                stacklevel=2,
            )
            empty = pd.DataFrame()
            if self.level == "all":
                result = {"state": empty, "county": empty}
                if self.include_vote_methods:
                    result["vote_method_state"] = empty
                    result["vote_method_county"] = empty
                return result
            return empty

        elections = self._filter(all_elections, start_date, end_date)

        if not elections:
            lo = start_date.isoformat() if start_date else "–"
            hi = end_date.isoformat()   if end_date   else "–"
            self._p(f"No elections found for range {lo} – {hi}.")
            empty = pd.DataFrame()
            if self.level == "all":
                result = {"state": empty, "county": empty}
                if self.include_vote_methods:
                    result["vote_method_state"] = empty
                    result["vote_method_county"] = empty
                return result
            return empty

        self._p(f"Scraping {len(elections)} election(s)...")
        state_frames: list[pd.DataFrame] = []
        county_frames: list[pd.DataFrame] = []
        vm_state_frames: list[pd.DataFrame] = []
        vm_county_frames: list[pd.DataFrame] = []
        failed: list[tuple[ClarityElectionInfo, Exception]] = []

        for election in elections:
            self._p(f"  {election.year}: {election.name} ({election.slug})")

            # --- State-level ---
            try:
                state_df, vm_state_df, county_urls = self._scrape_state(election)
                if not state_df.empty:
                    state_frames.append(state_df)
                if not vm_state_df.empty:
                    vm_state_frames.append(vm_state_df)
            except Exception as exc:
                failed.append((election, exc))
                self._p(f"WARNING: state scrape failed for '{election.name}': {exc}")
                continue

            # --- County-level (parallelised) ---
            if self.level in ("all", "county") and county_urls:
                n = len(county_urls)
                w = self.max_county_workers
                self._p(f"    Scraping {n} counties ({w} parallel workers)...")
                county_failed = 0
                county_done = 0

                with ThreadPoolExecutor(max_workers=w) as pool:
                    futures = {
                        pool.submit(self._scrape_county, url, election): url
                        for url in county_urls
                    }
                    for future in as_completed(futures):
                        url = futures[future]
                        county = county_name_from_url(url, self.county_suffix)
                        county_done += 1
                        try:
                            cdf, vm_cdf = future.result()
                            if not cdf.empty:
                                county_frames.append(cdf)
                            if not vm_cdf.empty:
                                vm_county_frames.append(vm_cdf)
                            self._p(f"    County {county_done}/{n}: {county} done.")
                        except Exception as exc:
                            county_failed += 1
                            self._p(f"    WARNING: county scrape failed for {county}: {exc}")

                if county_failed:
                    self._p(f"    NOTE: {county_failed}/{n} county scrape(s) failed.")

        if failed:
            if len(failed) == len(elections):
                names = ", ".join(f"'{e.name}'" for e, _ in failed[:3])
                if len(failed) > 3:
                    names += f", ... ({len(failed) - 3} more)"
                raise RuntimeError(
                    f"{self.log_prefix} All {len(failed)} election(s) failed to scrape. "
                    f"This usually means the site is unreachable or its structure has changed. "
                    f"Elections attempted: {names}. "
                    f"See the warning messages printed above for details."
                )
            self._p(
                f"NOTE: {len(failed)} election(s) failed; "
                f"returning {len(state_frames)} successful result(s)."
            )

        state_df     = concat_or_empty(state_frames)
        county_df    = concat_or_empty(county_frames)
        vm_state_df  = concat_or_empty(vm_state_frames)
        vm_county_df = concat_or_empty(vm_county_frames)

        for _df in [state_df, county_df, vm_state_df, vm_county_df]:
            if not _df.empty:
                _df.insert(0, "state", self.state_abbrev)

        self._p(
            f"Done. {len(state_df):,} total state rows, {len(county_df):,} total county rows."
        )

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


# ── Public entry point ────────────────────────────────────────────────────────

def get_clarity_election_results(
    base_url: str,
    county_suffix: str,
    state_abbrev: str,
    log_prefix: str = "",
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_county_workers: int = 2,
    include_vote_methods: bool = False,
):
    """Return election results for a SOS site using the Angular/PrimeNG framework.

    Parameters
    ----------
    base_url : str
        Landing page URL (e.g. ``"https://results.sos.ga.gov/results/public/Georgia"``).
    county_suffix : str
        State-specific county URL suffix (e.g. ``"-ga"`` or ``"-ut"``).
    log_prefix : str
        Console log prefix (e.g. ``"[GA]"`` or ``"[UT]"``).
    year_from : int | None
        Start year, inclusive.  ``None`` applies no lower bound.
    year_to : int | None
        End year, inclusive.  ``None`` applies no upper bound.
    level : str
        ``'all'`` (default) — dict with ``'state'`` and ``'county'`` DataFrames;
        ``'state'`` — statewide totals only (skips county scraping);
        ``'county'`` — county-level only.
    max_county_workers : int
        Parallel Chromium browsers for county scraping (default 2).
    include_vote_methods : bool
        When True, expands each contest's vote-method breakdown table
        (default False).
    """
    start, end = year_to_date_range(year_from, year_to)
    pipeline = ClarityPipeline(
        base_url=base_url,
        county_suffix=county_suffix,
        state_abbrev=state_abbrev,
        log_prefix=log_prefix,
        level=level,
        max_county_workers=max_county_workers,
        include_vote_methods=include_vote_methods,
    )
    return pipeline.run(start_date=start, end_date=end)
