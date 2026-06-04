"""City of Boston election results pipeline.

Orchestrates three phases:

1. **Discovery** — fetch the boston.gov elections landing page (single HTTP
   request) and parse all election drawers into BostonElectionInfo objects,
   each carrying a list of PDF result links.

2. **Filter** — narrow the election list to the requested year range.

3. **PDF scraping** — for each election, download every linked PDF in parallel
   and parse the ward-summary table on page 1 into city-level and ward-level
   DataFrames; also parse pages 2+ into precinct-level DataFrames.

Output levels
-------------
``level='all'``      → dict {"city": city_df, "ward": ward_df, "precinct": precinct_df}
``level='city'``     → citywide totals DataFrame
``level='ward'``     → ward-level DataFrame
``level='precinct'`` → precinct-level DataFrame

Public entry point
------------------
``get_boston_election_results(year_from, year_to, level, max_pdf_workers)``
    Called by registry._scrapers._scrape_boston().
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import warnings

import pandas as pd

from .client import BostonHttpClient
from .discovery import parse_landing_page
from .models import BostonElectionInfo, BostonResultLink
from .parser import to_city_df, to_ward_df, to_precinct_df
from .pdf_parser import parse_pdf, PdfTableData, PdfPrecinctData

from df_utils import concat_or_empty
from date_utils import year_to_date_range
from column_schemas import (
    BOSTON_CITY_COLS,
    BOSTON_WARD_COLS,
    BOSTON_PRECINCT_COLS,
    finalize_df,
    compute_vote_pct,
)

_LOG = "[Boston]"
_thread_local = threading.local()

_CITY_CONTEST_COLS     = ["election_name", "election_year", "election_date", "office", "district"]
_WARD_CONTEST_COLS     = ["election_name", "election_year", "election_date", "office", "district", "ward"]
_PRECINCT_CONTEST_COLS = ["election_name", "election_year", "election_date", "office", "district", "ward", "precinct"]

_VALID_LEVELS = ("all", "city", "ward", "precinct")


class BostonElectionPipeline:
    """Three-phase pipeline for City of Boston election results.

    Parameters
    ----------
    level : str
        ``'all'`` (default), ``'city'``, ``'ward'``, or ``'precinct'``.
    max_pdf_workers : int
        Number of parallel PDF downloads per election (default 4).
    max_election_workers : int
        Number of elections to scrape in parallel (default 3).
    """

    state = "MA"

    def __init__(
        self,
        level: str = "all",
        max_pdf_workers: int = 4,
        max_election_workers: int = 3,
    ):
        if level not in _VALID_LEVELS:
            raise ValueError(f"level must be one of {_VALID_LEVELS}; got {level!r}")
        self.level = level
        self.max_pdf_workers = max_pdf_workers
        self.max_election_workers = max_election_workers
        self._client = BostonHttpClient()

    def _get_client(self) -> BostonHttpClient:
        """Return a per-thread HTTP client (safe for parallel election scraping)."""
        if not hasattr(_thread_local, "boston_client"):
            _thread_local.boston_client = BostonHttpClient()
        return _thread_local.boston_client

    # ── Phase 1: discovery ─────────────────────────────────────────────────────

    def discover(self) -> list[BostonElectionInfo]:
        """Fetch the landing page and return all elections discovered."""
        print(f"{_LOG} Discovering elections from landing page...")
        html = self._client.get_landing_page()
        elections = parse_landing_page(html)
        print(f"{_LOG} Discovered {len(elections)} election(s).")
        return elections

    # ── Phase 2: filtering ─────────────────────────────────────────────────────

    def _filter(
        self,
        elections: list[BostonElectionInfo],
        start_date: "date | None",
        end_date: "date | None",
    ) -> list[BostonElectionInfo]:
        result = []
        for e in elections:
            if start_date is not None and e.year < start_date.year:
                continue
            if end_date is not None and e.year > end_date.year:
                continue
            result.append(e)
        return result

    # ── Phase 3: PDF scraping ──────────────────────────────────────────────────

    def _scrape_link(
        self, link: BostonResultLink
    ) -> "tuple[BostonResultLink, PdfTableData | None, list[PdfPrecinctData]]":
        """Download and parse one PDF result file."""
        try:
            pdf_bytes = self._get_client().get_pdf(link.pdf_url)
            need_precincts = self.level in ("all", "precinct")
            ward_data, precinct_list = parse_pdf(pdf_bytes, parse_precinct_pages=need_precincts)
            return link, ward_data, precinct_list
        except Exception as exc:
            print(f"{_LOG}     WARNING: Failed to fetch/parse {link.pdf_url!r}: {exc}")
            return link, None, []

    def _scrape_election(
        self,
        election: BostonElectionInfo,
    ) -> "tuple[list[pd.DataFrame], list[pd.DataFrame], list[pd.DataFrame]]":
        """Download and parse all PDFs for one election.

        Returns (city_frames, ward_frames, precinct_frames).
        """
        links = election.result_links
        n = len(links)
        w = min(self.max_pdf_workers, n)
        print(f"{_LOG}   Scraping {n} PDF(s) ({w} worker(s))...")

        city_frames: list[pd.DataFrame] = []
        ward_frames: list[pd.DataFrame] = []
        precinct_frames: list[pd.DataFrame] = []

        with ThreadPoolExecutor(max_workers=w) as pool:
            futures = {pool.submit(self._scrape_link, link): link for link in links}
            for future in as_completed(futures):
                link, pdf_data, precinct_data_list = future.result()
                if pdf_data is None or pdf_data.is_empty:
                    print(f"{_LOG}     SKIP (empty): {link.pdf_url}")
                    continue
                print(f"{_LOG}     OK: {link.office}"
                      + (f" {link.district}" if link.district else "")
                      + (f" [{link.party}]" if link.party else ""))
                if self.level in ("all", "city"):
                    df = to_city_df(pdf_data, election, link)
                    if not df.empty:
                        city_frames.append(df)
                if self.level in ("all", "ward"):
                    df = to_ward_df(pdf_data, election, link)
                    if not df.empty:
                        ward_frames.append(df)
                if self.level in ("all", "precinct") and precinct_data_list:
                    df = to_precinct_df(precinct_data_list, election, link)
                    if not df.empty:
                        precinct_frames.append(df)

        return city_frames, ward_frames, precinct_frames

    # ── Orchestrator ───────────────────────────────────────────────────────────

    def run(
        self,
        start_date: "date | None" = None,
        end_date: "date | None" = None,
    ) -> "pd.DataFrame | dict":
        """Discover, filter, and scrape Boston election results.

        Returns
        -------
        If ``level='city'``:     pd.DataFrame — citywide candidate totals.
        If ``level='ward'``:     pd.DataFrame — ward-level candidate totals.
        If ``level='precinct'``: pd.DataFrame — precinct-level candidate totals.
        If ``level='all'``:      dict with keys ``'city'``, ``'ward'``, and ``'precinct'``.
        """
        all_elections = self.discover()

        if not all_elections:
            warnings.warn(
                f"{_LOG} Discovery returned 0 elections. "
                "The site structure may have changed.",
                stacklevel=2,
            )
            return self._empty_result()

        elections = self._filter(all_elections, start_date, end_date)

        if not elections:
            lo = start_date.isoformat() if start_date else "–"
            hi = end_date.isoformat() if end_date else "–"
            print(f"{_LOG} No elections in range {lo} – {hi}.")
            return self._empty_result()

        w = min(self.max_election_workers, len(elections))
        print(f"{_LOG} Scraping {len(elections)} election(s) ({w} worker(s))...")
        all_city_frames: list[pd.DataFrame] = []
        all_ward_frames: list[pd.DataFrame] = []
        all_precinct_frames: list[pd.DataFrame] = []
        failed = 0

        def _task(election: BostonElectionInfo):
            date_str = (
                election.election_date.isoformat()
                if election.election_date else str(election.year)
            )
            print(f"{_LOG}   {date_str}: {election.election_type} "
                  f"({len(election.result_links)} PDF(s))")
            return self._scrape_election(election)

        with ThreadPoolExecutor(max_workers=w) as pool:
            future_map = {pool.submit(_task, e): e for e in elections}
            for future in as_completed(future_map):
                election = future_map[future]
                try:
                    city_frames, ward_frames, precinct_frames = future.result()
                except Exception as exc:
                    failed += 1
                    print(f"{_LOG}   WARNING: Exception scraping {election.name!r}: {exc}")
                    continue

                if not city_frames and not ward_frames and not precinct_frames:
                    failed += 1
                    print(f"{_LOG}   WARNING: No results parsed for {election.name!r}.")
                else:
                    all_city_frames.extend(city_frames)
                    all_ward_frames.extend(ward_frames)
                    all_precinct_frames.extend(precinct_frames)

        if not all_city_frames and not all_ward_frames and not all_precinct_frames:
            if failed == len(elections):
                raise RuntimeError(
                    f"{_LOG} All {len(elections)} election(s) failed to return results. "
                    "The site may be unreachable or the PDF structure has changed."
                )

        _city_raw = compute_vote_pct(
            concat_or_empty(all_city_frames),
            _CITY_CONTEST_COLS,
            fill_missing_only=True,
        )
        _ward_raw = compute_vote_pct(
            concat_or_empty(all_ward_frames),
            _WARD_CONTEST_COLS,
            fill_missing_only=True,
        )
        _precinct_raw = compute_vote_pct(
            concat_or_empty(all_precinct_frames),
            _PRECINCT_CONTEST_COLS,
            fill_missing_only=True,
        )

        city_df     = finalize_df(_city_raw,     BOSTON_CITY_COLS,     state=self.state)
        ward_df     = finalize_df(_ward_raw,     BOSTON_WARD_COLS,     state=self.state)
        precinct_df = finalize_df(_precinct_raw, BOSTON_PRECINCT_COLS, state=self.state)

        print(
            f"{_LOG} Done. {len(city_df):,} city rows, "
            f"{len(ward_df):,} ward rows, {len(precinct_df):,} precinct rows."
        )

        if self.level == "city":
            return city_df
        if self.level == "ward":
            return ward_df
        if self.level == "precinct":
            return precinct_df
        return {"city": city_df, "ward": ward_df, "precinct": precinct_df}

    def _empty_result(self) -> "pd.DataFrame | dict":
        city_empty     = pd.DataFrame(columns=BOSTON_CITY_COLS)
        ward_empty     = pd.DataFrame(columns=BOSTON_WARD_COLS)
        precinct_empty = pd.DataFrame(columns=BOSTON_PRECINCT_COLS)
        if self.level == "city":
            return city_empty
        if self.level == "ward":
            return ward_empty
        if self.level == "precinct":
            return precinct_empty
        return {"city": city_empty, "ward": ward_empty, "precinct": precinct_empty}


# ── Public entry point (called by registry._scrapers) ─────────────────────────

def get_boston_election_results(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    level: str = "all",
    max_pdf_workers: int = 4,
    max_election_workers: int = 3,
) -> "pd.DataFrame | dict":
    """Return City of Boston election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.  None applies no lower bound (data from 2005).
    year_to : int | None
        End year, inclusive.  None applies no upper bound.
    level : str
        ``'all'``      (default) — dict with keys ``'city'``, ``'ward'``, and ``'precinct'``.
        ``'city'``     — citywide candidate totals only.
        ``'ward'``     — ward-level candidate totals only.
        ``'precinct'`` — precinct-level candidate totals only.
    max_pdf_workers : int
        Parallel PDF download threads per election (default 4).
    max_election_workers : int
        Elections to scrape in parallel (default 3).
    """
    start, end = year_to_date_range(year_from, year_to)
    pipeline = BostonElectionPipeline(
        level=level,
        max_pdf_workers=max_pdf_workers,
        max_election_workers=max_election_workers,
    )
    return pipeline.run(start_date=start, end_date=end)
