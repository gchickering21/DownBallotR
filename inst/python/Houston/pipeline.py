"""Harris County (Houston) election results pipeline.

Orchestrates three phases:

1. Discovery — fetch the harrisvotes.com elections archive (paginated) and
   parse all election items into HoustonElectionInfo objects, each carrying
   URLs to a cumulative PDF and a canvass PDF.

2. Filter — narrow the election list to the requested year range.

3. PDF scraping — for each election, download and parse:
   - Cumulative PDF → county-wide candidate totals (summary level)
   - Canvass PDF → precinct-level candidate totals (precinct level)

Output levels
-------------
``level='all'``      → dict {"summary": summary_df, "precinct": precinct_df}
``level='summary'``  → county-wide totals DataFrame
``level='precinct'`` → precinct-level DataFrame

Public entry point
------------------
``get_houston_election_results(year_from, year_to, level, max_workers)``
    Called by registry._scrapers._scrape_houston().
"""

from __future__ import annotations

import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import pandas as pd

from .client import HoustonHttpClient
from .discovery import discover_all_elections
from .models import HoustonElectionInfo
from .parser import to_summary_df, to_precinct_df
from .pdf_parser import (
    parse_cumulative,
    parse_canvass,
    build_contest_map,
    CumulativeContest,
    CanvassContest,
)

from df_utils import concat_or_empty
from date_utils import year_to_date_range
from column_schemas import (
    HOUSTON_SUMMARY_COLS,
    HOUSTON_PRECINCT_COLS,
    finalize_df,
    compute_vote_pct,
)

_LOG = "[Houston]"
_thread_local = threading.local()

_VALID_LEVELS = ("all", "summary", "precinct")

_SUMMARY_CONTEST_COLS  = ["election_name", "election_year", "election_date", "office", "district"]
_PRECINCT_CONTEST_COLS = ["election_name", "election_year", "election_date", "office", "district", "precinct"]


class HoustonElectionPipeline:
    """Three-phase pipeline for Harris County election results.

    Parameters
    ----------
    level : str
        ``'all'`` (default), ``'summary'``, or ``'precinct'``.
    max_workers : int
        Number of elections to scrape in parallel (default 2).
        Set low by default because canvass PDFs can be 40–50 MB each.
    """

    state = "TX"

    def __init__(self, level: str = "all", max_workers: int = 2):
        if level not in _VALID_LEVELS:
            raise ValueError(f"level must be one of {_VALID_LEVELS}; got {level!r}")
        self.level = level
        self.max_workers = max_workers
        self._client = HoustonHttpClient()

    def _get_client(self) -> HoustonHttpClient:
        """Return a per-thread HTTP client (safe for parallel election scraping)."""
        if not hasattr(_thread_local, "houston_client"):
            _thread_local.houston_client = HoustonHttpClient()
        return _thread_local.houston_client

    # ── Phase 1: discovery ─────────────────────────────────────────────────────

    def discover(self) -> "list[HoustonElectionInfo]":
        print(f"{_LOG} Discovering elections from archive...")
        elections = discover_all_elections(self._client)
        print(f"{_LOG} Discovered {len(elections)} election(s).")
        return elections

    # ── Phase 2: filtering ─────────────────────────────────────────────────────

    def _filter(
        self,
        elections: "list[HoustonElectionInfo]",
        start_date: "date | None",
        end_date:   "date | None",
    ) -> "list[HoustonElectionInfo]":
        result = []
        for e in elections:
            if start_date is not None and e.year < start_date.year:
                continue
            if end_date is not None and e.year > end_date.year:
                continue
            result.append(e)
        return result

    # ── Phase 3: PDF scraping ──────────────────────────────────────────────────

    def _scrape_cumulative(
        self,
        election: HoustonElectionInfo,
    ) -> "tuple[list[CumulativeContest], pd.DataFrame]":
        """Download and parse the cumulative PDF.  Returns (contests, summary_df)."""
        if not election.cumulative_url:
            return [], pd.DataFrame()
        try:
            pdf_bytes = self._get_client().get_pdf(election.cumulative_url)
        except Exception as exc:
            print(f"{_LOG}     WARNING: Failed to fetch cumulative PDF: {exc}")
            return [], pd.DataFrame()

        contests = parse_cumulative(pdf_bytes)
        if not contests:
            print(f"{_LOG}     WARNING: No contests parsed from cumulative PDF.")
            return [], pd.DataFrame()

        print(f"{_LOG}     Cumulative: {len(contests)} contest(s).")
        df = to_summary_df(contests, election)
        return contests, df

    def _scrape_canvass(
        self,
        election: HoustonElectionInfo,
        cumulative_contests: "list[CumulativeContest]",
    ) -> pd.DataFrame:
        """Download and parse the canvass PDF using the cumulative contest map."""
        if not election.canvass_url:
            return pd.DataFrame()
        if not cumulative_contests:
            print(f"{_LOG}     WARNING: No cumulative contests — skipping canvass.")
            return pd.DataFrame()

        try:
            pdf_bytes = self._get_client().get_pdf(election.canvass_url)
        except Exception as exc:
            print(f"{_LOG}     WARNING: Failed to fetch canvass PDF: {exc}")
            return pd.DataFrame()

        contest_map = build_contest_map(cumulative_contests)
        canvass_contests = parse_canvass(pdf_bytes, contest_map, cumulative_contests)
        if not canvass_contests:
            print(f"{_LOG}     WARNING: No contests parsed from canvass PDF.")
            return pd.DataFrame()

        total_precincts = sum(len(c.precinct_rows) for c in canvass_contests)
        print(
            f"{_LOG}     Canvass: {len(canvass_contests)} contest(s), "
            f"{total_precincts:,} precinct-row(s)."
        )
        return to_precinct_df(canvass_contests, election)

    def _scrape_election(
        self,
        election: HoustonElectionInfo,
    ) -> "tuple[pd.DataFrame, pd.DataFrame]":
        """Download and parse both PDFs for one election.

        Returns (summary_df, precinct_df).
        """
        summary_df  = pd.DataFrame()
        precinct_df = pd.DataFrame()

        need_cumulative = self.level in ("all", "summary")
        need_canvass    = self.level in ("all", "precinct")

        cumulative_contests: list[CumulativeContest] = []

        if need_cumulative or need_canvass:
            # Cumulative is always needed — it provides the contest map for canvass.
            cumulative_contests, summary_df = self._scrape_cumulative(election)

        if need_canvass:
            precinct_df = self._scrape_canvass(election, cumulative_contests)

        return summary_df, precinct_df

    # ── Orchestrator ───────────────────────────────────────────────────────────

    def run(
        self,
        start_date: "date | None" = None,
        end_date:   "date | None" = None,
    ) -> "pd.DataFrame | dict":
        """Discover, filter, and scrape Harris County election results.

        Returns
        -------
        If ``level='summary'``:  pd.DataFrame — county-wide candidate totals.
        If ``level='precinct'``: pd.DataFrame — precinct-level candidate totals.
        If ``level='all'``:      dict with keys ``'summary'`` and ``'precinct'``.
        """
        all_elections = self.discover()

        if not all_elections:
            warnings.warn(
                f"{_LOG} Discovery returned 0 elections.  "
                "The site structure may have changed.",
                stacklevel=2,
            )
            return self._empty_result()

        elections = self._filter(all_elections, start_date, end_date)

        if not elections:
            lo = start_date.isoformat() if start_date else "–"
            hi = end_date.isoformat()   if end_date   else "–"
            print(f"{_LOG} No elections in range {lo} – {hi}.")
            return self._empty_result()

        print(f"{_LOG} Scraping {len(elections)} election(s) ({self.max_workers} worker(s))...")
        all_summary_frames:  list[pd.DataFrame] = []
        all_precinct_frames: list[pd.DataFrame] = []
        failed = 0

        def _task(election: HoustonElectionInfo):
            date_str = (
                election.election_date.isoformat()
                if election.election_date
                else str(election.year)
            )
            print(f"{_LOG}   {date_str}: {election.election_type} — {election.name}")
            return self._scrape_election(election)

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            future_map = {pool.submit(_task, e): e for e in elections}
            for future in as_completed(future_map):
                election = future_map[future]
                try:
                    summary_df, precinct_df = future.result()
                except Exception as exc:
                    failed += 1
                    print(f"{_LOG}   WARNING: Exception scraping {election.name!r}: {exc}")
                    continue

                if summary_df.empty and precinct_df.empty:
                    failed += 1
                    print(f"{_LOG}   WARNING: No results parsed for {election.name!r}.")
                else:
                    if not summary_df.empty:
                        all_summary_frames.append(summary_df)
                    if not precinct_df.empty:
                        all_precinct_frames.append(precinct_df)

        if not all_summary_frames and not all_precinct_frames:
            if failed == len(elections):
                raise RuntimeError(
                    f"{_LOG} All {len(elections)} election(s) failed to return results. "
                    "The site may be unreachable or the PDF structure has changed."
                )

        _summary_raw = compute_vote_pct(
            concat_or_empty(all_summary_frames),
            _SUMMARY_CONTEST_COLS,
            fill_missing_only=True,
        )
        _precinct_raw = compute_vote_pct(
            concat_or_empty(all_precinct_frames),
            _PRECINCT_CONTEST_COLS,
            fill_missing_only=True,
        )

        summary_df  = finalize_df(_summary_raw,  HOUSTON_SUMMARY_COLS,  state=self.state)
        precinct_df = finalize_df(_precinct_raw, HOUSTON_PRECINCT_COLS, state=self.state)

        print(
            f"{_LOG} Done. {len(summary_df):,} summary rows, "
            f"{len(precinct_df):,} precinct rows."
        )

        if self.level == "summary":
            return summary_df
        if self.level == "precinct":
            return precinct_df
        return {"summary": summary_df, "precinct": precinct_df}

    def _empty_result(self) -> "pd.DataFrame | dict":
        s = pd.DataFrame(columns=HOUSTON_SUMMARY_COLS)
        p = pd.DataFrame(columns=HOUSTON_PRECINCT_COLS)
        if self.level == "summary":
            return s
        if self.level == "precinct":
            return p
        return {"summary": s, "precinct": p}


# ── Public entry point ─────────────────────────────────────────────────────────

def get_houston_election_results(
    year_from: "int | None" = None,
    year_to:   "int | None" = None,
    level: str = "all",
    max_workers: int = 2,
) -> "pd.DataFrame | dict":
    """Return Harris County (Houston) election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.  None applies no lower bound (data from ~2004).
    year_to : int | None
        End year, inclusive.  None applies no upper bound.
    level : str
        ``'all'``      (default) — dict with keys ``'summary'`` and ``'precinct'``.
        ``'summary'``  — county-wide candidate totals only.
        ``'precinct'`` — precinct-level candidate totals only.
    max_workers : int
        Elections to scrape in parallel (default 2).  Set low because canvass
        PDFs are very large (40–50 MB each) — each worker holds one in memory.
    """
    start, end = year_to_date_range(year_from, year_to)
    pipeline = HoustonElectionPipeline(level=level, max_workers=max_workers)
    return pipeline.run(start_date=start, end_date=end)
