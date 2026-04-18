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
from .parser import (
    parse_state_results, parse_county_results, parse_precinct_results,
    parse_precinct_links, parse_ballot_item_precinct_links,
    county_name_from_url, precinct_name_from_url,
)
from df_utils import concat_or_empty
from date_utils import year_to_date_range
from column_schemas import (
    CLARITY_STATE_COLS, CLARITY_COUNTY_COLS, CLARITY_PRECINCT_COLS,
    CLARITY_VM_STATE_COLS, CLARITY_VM_COUNTY_COLS,
    finalize_df, compute_vote_pct,
)


_CONTEST_ID = [
    "election_name", "election_type", "election_year", "election_date",
    "office", "district",
]


def _supplement_state_from_county(
    state_df: pd.DataFrame,
    county_df: pd.DataFrame,
) -> pd.DataFrame:
    """Append statewide aggregates for contests present in county_df but absent from state_df.

    The state-level page only shows races the SOS chose to display there.
    Local races that appear on county pages but not the state page would
    otherwise be silently missing from state output.
    """
    if county_df.empty:
        return state_df

    present = [c for c in _CONTEST_ID if c in county_df.columns]

    if state_df.empty:
        missing_county = county_df
    else:
        state_keys = set(
            map(tuple, state_df[present].drop_duplicates().values.tolist())
        )
        mask = ~county_df[present].apply(lambda r: tuple(r), axis=1).isin(state_keys)
        missing_county = county_df[mask]

    if missing_county.empty:
        return state_df

    group_cols = [
        c for c in
        ["election_name", "election_type", "election_year", "election_date",
         "office_level", "office", "district", "candidate", "party"]
        if c in missing_county.columns
    ]
    agg = missing_county.groupby(group_cols, as_index=False, dropna=False)["votes"].sum()

    contest_cols = [c for c in _CONTEST_ID if c in agg.columns]
    agg = compute_vote_pct(agg, contest_cols)
    max_votes = agg.groupby(contest_cols, dropna=False)["votes"].transform("max")
    agg["winner"] = agg["votes"] == max_votes

    # Carry the election-level URL from the first county row for each contest.
    if "url" in missing_county.columns:
        url_map = (
            missing_county
            .groupby(contest_cols, dropna=False)["url"]
            .first()
            .reset_index()
        )
        agg = agg.merge(url_map, on=contest_cols, how="left")

    return concat_or_empty([state_df, agg])


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
        if level not in ("all", "state", "county", "precinct"):
            raise ValueError(
                f"level must be 'all', 'state', 'county', or 'precinct'; got {level!r}"
            )
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

    def _empty_result(self) -> "pd.DataFrame | dict":
        """Return a correctly-schemed empty result for this pipeline's level."""
        s   = pd.DataFrame(columns=CLARITY_STATE_COLS)
        c   = pd.DataFrame(columns=CLARITY_COUNTY_COLS)
        p   = pd.DataFrame(columns=CLARITY_PRECINCT_COLS)
        vms = pd.DataFrame(columns=CLARITY_VM_STATE_COLS)
        vmc = pd.DataFrame(columns=CLARITY_VM_COUNTY_COLS)
        if self.level == "state":
            if self.include_vote_methods:
                return {"state": s, "vote_method_state": vms}
            return s
        if self.level == "county":
            if self.include_vote_methods:
                return {"county": c, "vote_method_county": vmc}
            return c
        if self.level == "precinct":
            return p
        result = {"state": s, "county": c, "precinct": p}
        if self.include_vote_methods:
            result["vote_method_state"]  = vms
            result["vote_method_county"] = vmc
        return result

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

    def _fetch_ballot_item_precinct_urls(
        self, bi_url: str, server_base: str
    ) -> list[str]:
        """Fetch one ballot-item page and return its precinct dropdown URLs."""
        with self._client() as client:
            bi_html = client.get_precinct_page(bi_url)
        return parse_ballot_item_precinct_links(bi_html, server_base)

    def _get_precinct_urls_for_county(
        self, county_url: str
    ) -> tuple[str, list[str]]:
        """Return (county_name, precinct_urls) for a county election page.

        Two-phase navigation, both phases parallelised with ``max_county_workers``:

        1. Render the county election page and extract per-contest
           ``/ballot-items/{uuid}`` links (one per contest, labelled
           "View results by precinct").
        2. Fetch all ballot-item pages in parallel and collect the precinct
           dropdown links each one exposes.  URLs are deduplicated so that if
           all contests share the same precinct set, we only keep one copy.
        """
        from urllib.parse import urlparse
        county_name = county_name_from_url(county_url, self.county_suffix)
        parsed = urlparse(county_url)
        server_base = f"{parsed.scheme}://{parsed.netloc}"

        # Phase 1: county page → ballot-item URLs (one per contest)
        with self._client() as client:
            county_html = client.get_county_page_with_precinct_links(county_url)
        ballot_item_urls = parse_precinct_links(county_html, server_base)

        if not ballot_item_urls:
            return county_name, []

        # Phase 2: all ballot-item pages in parallel → precinct URLs
        seen: set[str] = set()
        precinct_urls: list[str] = []

        with ThreadPoolExecutor(max_workers=self.max_county_workers) as pool:
            futures = {
                pool.submit(self._fetch_ballot_item_precinct_urls, bi_url, server_base): bi_url
                for bi_url in ballot_item_urls
            }
            for future in as_completed(futures):
                try:
                    for u in future.result():
                        if u not in seen:
                            seen.add(u)
                            precinct_urls.append(u)
                except Exception as exc:
                    bi_url = futures[future]
                    self._p(f"    WARNING: failed to fetch ballot-item precinct links from {bi_url}: {exc}")

        return county_name, precinct_urls

    def _scrape_precinct(
        self, url: str, county_name: str, election: ClarityElectionInfo
    ) -> pd.DataFrame:
        """Render one precinct page; return precinct_df."""
        precinct = precinct_name_from_url(url, self.county_suffix)
        with self._client() as client:
            html = client.get_precinct_page(url)
        return parse_precinct_results(html, county_name, precinct, election, url=url)

    # ── Orchestrator ───────────────────────────────────────────────────────────

    def run(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> "pd.DataFrame | dict":
        """Discover and scrape election results.

        Returns
        -------
        If ``level='state'``:    pd.DataFrame (statewide totals only).
        If ``level='county'``:   pd.DataFrame (county-level only).
        If ``level='precinct'``: pd.DataFrame (precinct-level only).
        If ``level='all'``:      dict with keys ``'state'``, ``'county'``, and
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
            return self._empty_result()

        elections = self._filter(all_elections, start_date, end_date)

        if not elections:
            lo = start_date.isoformat() if start_date else "–"
            hi = end_date.isoformat()   if end_date   else "–"
            self._p(f"No elections found for range {lo} – {hi}.")
            return self._empty_result()

        self._p(f"Scraping {len(elections)} election row(s)...")
        state_frames: list[pd.DataFrame] = []
        county_frames: list[pd.DataFrame] = []
        precinct_frames: list[pd.DataFrame] = []
        vm_state_frames: list[pd.DataFrame] = []
        vm_county_frames: list[pd.DataFrame] = []
        failed: list[tuple[ClarityElectionInfo, Exception]] = []

        for election in elections:
            self._p(f"  {election.year}: {election.name} ({election.slug})")

            # --- State-level (also yields county URLs for sub-scrapes) ---
            try:
                state_df, vm_state_df, county_urls = self._scrape_state(election)
                if self.level != "precinct" and not state_df.empty:
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

            # --- Precinct-level (parallelised per county then per precinct) ---
            if self.level in ("all", "precinct") and county_urls:
                n_counties = len(county_urls)
                w = self.max_county_workers
                self._p(
                    f"    Getting precinct links for {n_counties} counties "
                    f"({w} parallel workers)..."
                )
                # Phase A: render each county page with precinct button clicked
                # to collect (county_name, precinct_urls) pairs.
                county_precinct_map: list[tuple[str, list[str]]] = []
                county_link_failed = 0
                with ThreadPoolExecutor(max_workers=w) as pool:
                    futures = {
                        pool.submit(self._get_precinct_urls_for_county, url): url
                        for url in county_urls
                    }
                    for future in as_completed(futures):
                        url = futures[future]
                        county = county_name_from_url(url, self.county_suffix)
                        try:
                            county_name_r, precinct_urls = future.result()
                            if precinct_urls:
                                county_precinct_map.append((county_name_r, precinct_urls))
                            else:
                                self._p(
                                    f"    NOTE: no precinct links found for {county}."
                                )
                        except Exception as exc:
                            county_link_failed += 1
                            self._p(
                                f"    WARNING: precinct link fetch failed for {county}: {exc}"
                            )

                if county_link_failed:
                    self._p(
                        f"    NOTE: {county_link_failed}/{n_counties} county "
                        f"precinct-link fetch(es) failed."
                    )

                # Phase B: render each precinct page in parallel.
                all_precinct_urls = [
                    (county_name_r, p_url)
                    for county_name_r, p_urls in county_precinct_map
                    for p_url in p_urls
                ]
                n_precincts = len(all_precinct_urls)
                if n_precincts:
                    self._p(
                        f"    Scraping {n_precincts} precincts across "
                        f"{len(county_precinct_map)} counties ({w} parallel workers)..."
                    )
                    precinct_failed = 0
                    precinct_done = 0
                    with ThreadPoolExecutor(max_workers=w) as pool:
                        futures = {
                            pool.submit(self._scrape_precinct, p_url, cname, election): (cname, p_url)
                            for cname, p_url in all_precinct_urls
                        }
                        for future in as_completed(futures):
                            cname, p_url = futures[future]
                            precinct_done += 1
                            try:
                                pdf = future.result()
                                if not pdf.empty:
                                    precinct_frames.append(pdf)
                                if precinct_done % 10 == 0 or precinct_done == n_precincts:
                                    self._p(
                                        f"    Precinct {precinct_done}/{n_precincts} done."
                                    )
                            except Exception as exc:
                                precinct_failed += 1
                                self._p(
                                    f"    WARNING: precinct scrape failed for "
                                    f"{precinct_name_from_url(p_url)} ({cname}): {exc}"
                                )

                    if precinct_failed:
                        self._p(
                            f"    NOTE: {precinct_failed}/{n_precincts} precinct scrape(s) failed."
                        )

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

        # --- Finalise ---
        precinct_df = finalize_df(
            concat_or_empty(precinct_frames), CLARITY_PRECINCT_COLS, state=self.state_abbrev
        )

        if self.level == "precinct":
            self._p(f"Done. {len(precinct_df):,} total precinct rows.")
            return precinct_df

        _state_raw  = _supplement_state_from_county(
            concat_or_empty(state_frames),
            concat_or_empty(county_frames),
        )
        _n_supplemented = len(_state_raw) - sum(len(f) for f in state_frames)
        if _n_supplemented > 0:
            self._p(f"  Supplemented state_df with {_n_supplemented:,} row(s) aggregated from county data.")

        state_df     = finalize_df(_state_raw,                           CLARITY_STATE_COLS,    state=self.state_abbrev)
        county_df    = finalize_df(concat_or_empty(county_frames),       CLARITY_COUNTY_COLS,   state=self.state_abbrev)
        vm_state_df  = finalize_df(concat_or_empty(vm_state_frames),  CLARITY_VM_STATE_COLS, state=self.state_abbrev)
        vm_county_df = finalize_df(concat_or_empty(vm_county_frames), CLARITY_VM_COUNTY_COLS, state=self.state_abbrev)

        self._p(
            f"Done. {len(state_df):,} total state rows, "
            f"{len(county_df):,} total county rows, "
            f"{len(precinct_df):,} total precinct rows."
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
        result = {"state": state_df, "county": county_df, "precinct": precinct_df}
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
        ``'county'`` — county-level only;
        ``'precinct'`` — precinct-level only (navigates county pages to find
        precinct links, then scrapes each precinct page).
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
