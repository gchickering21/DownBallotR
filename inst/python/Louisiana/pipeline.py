"""
Louisiana SOS Graphical election results pipeline.

Orchestrates two phases per election:

1. **Statewide tabs** — for each non-Parish tab (Congressional, Presidential,
   Statewide, Multiparish, etc.) that exists for the election, render the tab
   and extract candidate results into a statewide DataFrame.

2. **Parish tab** — enumerate all parishes from the Parish sub-dropdown,
   filter to those with > 0 precincts reporting, render each parish page, and
   extract per-parish candidate results into a parish-level DataFrame.

Return value
------------
- ``level='all'``    → dict with keys ``'state'`` and ``'parish'``
- ``level='state'``  → pd.DataFrame (statewide tabs only, no parish scraping)
- ``level='parish'`` → pd.DataFrame (parish tab only)

Public entry point
------------------
``get_la_election_results(year_from, year_to, level, max_parish_workers)``
    Called by registry.py.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import warnings

import pandas as pd

from .client import LaPlaywrightClient, _KNOWN_STATE_TABS, _PARISH_TAB_LABEL
from .discovery import parse_election_options
from .models import LaElectionInfo
from .parser import parse_tab_results, parse_parish_results, _STATE_COLS, _PARISH_COLS
from df_utils import concat_or_empty
from date_utils import year_to_date_range


class LaElectionPipeline:
    """Two-phase pipeline for Louisiana SOS Graphical election results.

    Parameters
    ----------
    headless : bool
        Whether to run the browser in headless mode (default True).
    sleep_s : float
        Seconds to pause after each browser interaction (default 3.0).
    level : str
        What to scrape: ``'all'`` (default), ``'state'``, or ``'parish'``.
    max_parish_workers : int
        Parallel Chromium browsers for parish scraping within each election
        (default 2).  Each worker handles one parish at a time.
    """

    def __init__(
        self,
        headless: bool = True,
        sleep_s: float = 3.0,
        level: str = "all",
        max_parish_workers: int = 2,
    ):
        if level not in ("all", "state", "parish"):
            raise ValueError(f"level must be 'all', 'state', or 'parish'; got {level!r}")
        self.headless = headless
        self.sleep_s = sleep_s
        self.level = level
        self.max_parish_workers = max_parish_workers

    # ── Phase 1: discovery ─────────────────────────────────────────────────────

    def discover(self) -> list[LaElectionInfo]:
        """Render the landing page and return all elections from the dropdown."""
        print("[LA] Discovering elections from landing page dropdown...")
        with LaPlaywrightClient(headless=self.headless, sleep_s=self.sleep_s) as client:
            html = client.get_landing_page()
        elections = parse_election_options(html)
        print(f"[LA] Discovered {len(elections)} election(s).")
        return elections

    # ── Filtering ──────────────────────────────────────────────────────────────

    def _filter(
        self,
        elections: list[LaElectionInfo],
        start_date: date | None,
        end_date: date | None,
    ) -> list[LaElectionInfo]:
        result = []
        for e in elections:
            if start_date is not None and e.year < start_date.year:
                continue
            if end_date is not None and e.year > end_date.year:
                continue
            result.append(e)
        return result

    # ── Phase 2a: statewide tabs ───────────────────────────────────────────────

    def _scrape_state_tabs(self, election: LaElectionInfo) -> "pd.DataFrame | None":
        """Render each non-Parish tab for one election; return combined DataFrame.

        Returns None if the site has no results published for this election
        (results container never loaded). Returns an empty DataFrame if results
        loaded but no parseable rows were found.

        Navigates to the landing page once, selects the election, then clicks
        through each tab in the same browser session — no redundant page loads.
        """
        state_frames: list[pd.DataFrame] = []

        with LaPlaywrightClient(headless=self.headless, sleep_s=self.sleep_s) as client:
            # Discover available tabs (single navigation).
            available_tabs = client.get_available_tabs(election.option_value)
            if available_tabs is None:
                return None  # site has no results for this election
            print(f"[LA]   Available tabs: {available_tabs}")

            # Filter to recognised non-Parish tabs.
            tabs_to_scrape_unsorted = [
                t for t in available_tabs
                if not t.lower().startswith(_PARISH_TAB_LABEL.lower())
                and any(t.lower().startswith(k.lower()) for k in _KNOWN_STATE_TABS)
            ]
            skipped = [
                t for t in available_tabs
                if t not in tabs_to_scrape_unsorted
                and not t.lower().startswith(_PARISH_TAB_LABEL.lower())
            ]
            for t in skipped:
                print(f"[LA]     Skipping unrecognised tab: {t!r}")

            # Sort by priority order in _KNOWN_STATE_TABS so that when we
            # deduplicate cross-tab rows later, higher-priority tabs win.
            def _tab_priority(label: str) -> int:
                for i, known in enumerate(_KNOWN_STATE_TABS):
                    if label.lower().startswith(known.lower()):
                        return i
                return len(_KNOWN_STATE_TABS)

            tabs_to_scrape = sorted(tabs_to_scrape_unsorted, key=_tab_priority)

            if not tabs_to_scrape:
                return pd.DataFrame(columns=_STATE_COLS)

            # Capture all tabs in a single browser session.
            tab_htmls = client.get_all_state_tabs_html(
                election.option_value, tabs_to_scrape
            )

        for tab_label, html in tab_htmls:
            try:
                df = parse_tab_results(html, tab_label, election)
                if not df.empty:
                    state_frames.append(df)
                    print(f"[LA]     {tab_label}: {len(df)} candidate row(s).")
                else:
                    print(f"[LA]     {tab_label}: no results parsed (empty).")
            except Exception as exc:
                print(f"[LA]     WARNING: Failed to parse tab {tab_label!r}: {exc}")

        combined = concat_or_empty(state_frames)
        if combined.empty:
            return combined

        # Drop cross-tab duplicates: same office + candidate + vote total appearing
        # in multiple tabs (Multiparish commonly repeats races found in other tabs).
        # Tabs were sorted by priority, so the first occurrence is from the
        # higher-priority tab (Congressional > Statewide > Legislative > Multiparish).
        before = len(combined)
        combined = combined.drop_duplicates(
            subset=["office", "candidate", "votes"], keep="first"
        ).reset_index(drop=True)
        dupes = before - len(combined)
        if dupes:
            print(f"[LA]     Dropped {dupes} duplicate cross-tab row(s).")

        return combined

    # ── Phase 2b: parish tab ───────────────────────────────────────────────────

    def _scrape_parish_tab(self, election: LaElectionInfo) -> "pd.DataFrame | None":
        """Scrape all parishes from the Parish tab; return combined DataFrame.

        Parishes are enumerated once, filtered to > 0 reporting, then scraped
        in a single browser session (most efficient for sequential operation).
        For parallel scraping, split parishes across multiple workers — each
        worker calls ``get_all_parishes_results`` with its subset.
        """
        # Step 1: get the list of parishes with reporting > 0.
        with LaPlaywrightClient(headless=self.headless, sleep_s=self.sleep_s) as client:
            parishes = client.get_parish_options(election.option_value)

        if parishes is None:
            return None  # site has no results for this election
        if not parishes:
            print(f"[LA]   No parishes with > 0 reporting found.")
            return pd.DataFrame(columns=_PARISH_COLS)

        n = len(parishes)
        w = self.max_parish_workers
        print(f"[LA]   Scraping {n} parish/parishes ({w} parallel worker(s))...")

        parish_frames: list[pd.DataFrame] = []
        parish_failed = 0

        if w <= 1:
            # Sequential: one browser, select each parish in turn.
            with LaPlaywrightClient(headless=self.headless, sleep_s=self.sleep_s) as client:
                parish_htmls = client.get_all_parishes_results(election.option_value, parishes)
            for parish_name, html in parish_htmls:
                try:
                    df = parse_parish_results(html, parish_name, election)
                    if not df.empty:
                        parish_frames.append(df)
                except Exception as exc:
                    parish_failed += 1
                    print(f"[LA]     WARNING: Failed to parse {parish_name}: {exc}")
        else:
            # Parallel: split parishes across workers, each with its own browser.
            chunk_size = max(1, (n + w - 1) // w)
            chunks = [parishes[i : i + chunk_size] for i in range(0, n, chunk_size)]

            def _worker(chunk: list[tuple[str, str]]) -> list[tuple[str, str]]:
                with LaPlaywrightClient(
                    headless=self.headless, sleep_s=self.sleep_s
                ) as client:
                    return client.get_all_parishes_results(election.option_value, chunk)

            with ThreadPoolExecutor(max_workers=w) as pool:
                futures = {pool.submit(_worker, chunk): chunk for chunk in chunks}
                for future in as_completed(futures):
                    try:
                        for parish_name, html in future.result():
                            df = parse_parish_results(html, parish_name, election)
                            if not df.empty:
                                parish_frames.append(df)
                    except Exception as exc:
                        parish_failed += 1
                        print(f"[LA]     WARNING: Parish worker failed: {exc}")

        if parish_failed:
            print(f"[LA]   NOTE: {parish_failed} parish scrape(s) failed.")

        return concat_or_empty(parish_frames)

    # ── Orchestrator ───────────────────────────────────────────────────────────

    def run(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> "pd.DataFrame | dict":
        """Discover elections, filter by date, and scrape results.

        Returns
        -------
        If ``level='state'``:  pd.DataFrame — statewide tab results only.
        If ``level='parish'``: pd.DataFrame — parish-level results only.
        If ``level='all'``:    dict with keys ``'state'`` and ``'parish'``.
                                Reticulate converts this to a named R list.
        """
        all_elections = self.discover()

        if not all_elections:
            warnings.warn(
                "[LA] Discovery returned 0 elections from the Louisiana landing page. "
                "The site structure may have changed — run inspect_landing.py to save "
                "the rendered HTML and verify the election dropdown selector assumptions.",
                stacklevel=2,
            )
            empty = pd.DataFrame()
            if self.level == "all":
                return {"state": empty, "parish": empty}
            return empty

        elections = self._filter(all_elections, start_date, end_date)

        if not elections:
            lo = start_date.isoformat() if start_date else "–"
            hi = end_date.isoformat() if end_date else "–"
            print(f"[LA] No elections found for range {lo} – {hi}.")
            empty = pd.DataFrame()
            if self.level == "all":
                return {"state": empty, "parish": empty}
            return empty

        print(f"[LA] Scraping {len(elections)} election(s)...")
        state_frames:  list[pd.DataFrame] = []
        parish_frames: list[pd.DataFrame] = []
        failed = 0

        for election in elections:
            date_str = election.election_date.isoformat() if election.election_date else str(election.year)
            print(f"[LA]   {date_str}: {election.name}")

            no_results = False
            election_failed = True

            if self.level in ("all", "state"):
                try:
                    state_df = self._scrape_state_tabs(election)
                    if state_df is None:
                        no_results = True
                        print(f"[LA]   No results published on the site for this election — skipping.")
                    elif not state_df.empty:
                        state_frames.append(state_df)
                        election_failed = False
                        print(f"[LA]   State tabs done: {len(state_df):,} row(s).")
                    else:
                        election_failed = False
                except Exception as exc:
                    print(f"[LA]   ERROR scraping state tabs for {election.name!r}: {exc}")

            if no_results:
                failed += 1
                continue

            if self.level in ("all", "parish"):
                try:
                    parish_df = self._scrape_parish_tab(election)
                    if parish_df is None:
                        print(f"[LA]   No results published on the site for this election — skipping.")
                    elif not parish_df.empty:
                        parish_frames.append(parish_df)
                        election_failed = False
                        print(f"[LA]   Parish tab done: {len(parish_df):,} row(s).")
                    else:
                        election_failed = False
                except Exception as exc:
                    print(f"[LA]   ERROR scraping parish tab for {election.name!r}: {exc}")

            if election_failed:
                failed += 1

        if not state_frames and not parish_frames:
            if failed == len(elections):
                raise RuntimeError(
                    f"[LA] All {len(elections)} election(s) failed to scrape. "
                    f"This usually means the site is unreachable or its structure has changed. "
                    f"See the error messages printed above for details."
                )

        state_all  = concat_or_empty(state_frames)
        parish_all = concat_or_empty(parish_frames)

        for _df in [state_all, parish_all]:
            if not _df.empty:
                _df.insert(0, "state", "LA")

        print(
            f"[LA] Done. {len(state_all):,} total state rows, {len(parish_all):,} total parish rows."
        )

        if self.level == "state":
            return state_all
        if self.level == "parish":
            return parish_all
        return {"state": state_all, "parish": parish_all}


# ── Public entry point (called by registry.py) ────────────────────────────────

def get_la_election_results(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_parish_workers: int = 2,
) -> "pd.DataFrame | dict":
    """Return Louisiana SOS Graphical election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.  ``None`` applies no lower bound.
    year_to : int | None
        End year, inclusive.  ``None`` applies no upper bound.
    level : str
        What to return:
          - ``'all'``    (default) dict with keys ``'state'`` and ``'parish'``;
                         reticulate converts this to a named R list.
          - ``'state'``  statewide tab DataFrames only (no parish scraping).
          - ``'parish'`` parish-level DataFrame only.
    max_parish_workers : int
        Parallel Chromium browsers for parish scraping (default 2).
    """
    start, end = year_to_date_range(year_from, year_to)
    pipeline = LaElectionPipeline(
        level=level,
        max_parish_workers=max_parish_workers,
    )
    return pipeline.run(start_date=start, end_date=end)
