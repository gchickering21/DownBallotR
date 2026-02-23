from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from typing import Iterable

import pandas as pd

from .discovery import discover_nc_results_zips
from .selection import select_elections
from .io_utils import download_zip_bytes, read_results_pct_from_zip
from .normalize import normalize_nc_results_cols, get_config
from .aggregate import aggregate_to_county_level, aggregate_county_to_state


def _get_attr(obj, name: str):
    # supports either dataclass/obj or dict
    if isinstance(obj, dict):
        return obj[name]
    return getattr(obj, name)


class NcElectionPipeline:
    state = "NC"

    def discover(self):
        # returns list[NcElectionZip] (or similar)
        return discover_nc_results_zips()

    def _filter_elections(self, elections, start_date: date | None, end_date: date | None):
        # your selection.py should handle None bounds
        return select_elections(elections, start_date=start_date, end_date=end_date)

    def run(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        min_supported_date: date | None = None,
        max_supported_date: date | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        cfg = get_config()

        elections = self._filter_elections(
            self.discover(),
            start_date=start_date,
            end_date=end_date,
        )

        supported: list[object] = []
        skipped: list[object] = []

        for e in elections:
            ed = _get_attr(e, "election_date")
            if min_supported_date is not None and ed < min_supported_date:
                skipped.append(e)
            elif max_supported_date is not None and ed > max_supported_date:
                skipped.append(e)
            else:
                supported.append(e)

        if skipped:
            lo = min_supported_date.isoformat() if min_supported_date else "–"
            hi = max_supported_date.isoformat() if max_supported_date else "–"
            print(
                f"[NC] NOTE: skipping {len(skipped)} election(s) outside "
                f"supported range {lo} – {hi}."
            )

        precinct_frames: list[pd.DataFrame] = []
        county_frames: list[pd.DataFrame] = []
        state_frames : list[pd.DataFrame] = []
        failed: list[tuple[object, Exception]] = []

        for e in supported:
            try:
                precinct_df, county_df, state_df = self._scrape_one(e)
                precinct_frames.append(precinct_df)
                county_frames.append(county_df)
                state_frames.append(state_df)
            except Exception as ex:
                failed.append((e, ex))
                print(
                    "[NC] WARNING: failed to scrape "
                    f"{_get_attr(e,'election_date')} ({_get_attr(e,'zip_url')}): {ex}"
                )

        if failed:
            print(
                f"[NC] NOTE: {len(failed)} election(s) failed; "
                f"returning {len(precinct_frames)} successful result(s)."
            )

        # If nothing succeeded, return empty DFs with expected schemas
        if not precinct_frames:
            precinct_empty = pd.DataFrame(columns=cfg.schema.join_cols + ["election_year"])
            county_empty = pd.DataFrame(columns=cfg.schema.county_cols + ["election_year"])
            state_empty = pd.DataFrame(columns=cfg.schema.state_cols + ["election_year"])

            return precinct_empty, county_empty, state_empty

        precinct_final = pd.concat(precinct_frames, ignore_index=True)
        county_final = pd.concat(county_frames, ignore_index=True) if county_frames else pd.DataFrame()
        state_final = pd.concat(state_frames, ignore_index=True) if state_frames else pd.DataFrame()

        return precinct_final, county_final, state_final

    def _scrape_one(self, election) -> pd.DataFrame:
        zip_url = _get_attr(election, "zip_url")
        election_date = _get_attr(election, "election_date")

        zip_bytes = download_zip_bytes(zip_url)
        _member, raw = read_results_pct_from_zip(zip_bytes)

        norm = normalize_nc_results_cols(raw, fallback_election_date=election_date)

        county_df = aggregate_to_county_level(norm)
        state_df = aggregate_county_to_state(county_df)

        print(f"[NC SCRAPE] Finished scraping election results for {election_date}")
        return norm, county_df, state_df


def get_nc_election_results(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    min_supported_date: "date | None" = None,
    max_supported_date: "date | None" = None,
) -> pd.DataFrame:
    """Return precinct-level NC election results.

    Parameters
    ----------
    year_from : int | None
        Start year, inclusive.  Elections on or after Jan 1 of this year.
        ``None`` applies no lower bound.
    year_to : int | None
        End year, inclusive.  Elections on or before Dec 31 of this year.
        ``None`` applies no upper bound.
    min_supported_date : date | None
        Pipeline lower-bound guard.  Elections before this date are skipped.
        ``None`` (default) attempts all elections in the requested range.
    max_supported_date : date | None
        Pipeline upper-bound guard.  Elections after this date are skipped.
        ``None`` (default) attempts all elections in the requested range.
    """
    start = date(int(year_from), 1, 1) if year_from is not None else None
    end   = date(int(year_to),   12, 31) if year_to   is not None else None

    pipeline = NcElectionPipeline()
    precinct_df, _county_df, _state_df = pipeline.run(
        start_date=start,
        end_date=end,
        min_supported_date=min_supported_date,
        max_supported_date=max_supported_date,
    )
    return precinct_df
