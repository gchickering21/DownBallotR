from __future__ import annotations

from dataclasses import asdict
from datetime import date
from typing import Iterable
import warnings

import pandas as pd

from .discovery import discover_northcarolina_results_zips
from .selection import select_elections
from .io_utils import download_zip_bytes, read_results_pct_from_zip
from .normalize import normalize_northcarolina_results_cols, get_config, extract_office_short
from .aggregate import aggregate_to_county_level, aggregate_county_to_state
from df_utils import concat_or_empty
from date_utils import year_to_date_range


def _get_attr(obj, name: str):
    # supports either dataclass/obj or dict
    if isinstance(obj, dict):
        return obj[name]
    return getattr(obj, name)


class NcElectionPipeline:
    state = "NC"

    def discover(self):
        # returns list[NcElectionZip] (or similar)
        return discover_northcarolina_results_zips()

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

        all_elections = self.discover()

        if not all_elections:
            warnings.warn(
                "[NC] Discovery returned 0 election zip files from the NCSBE page. "
                "The site structure may have changed — verify the zip URL pattern "
                "and XPath selector in discovery.py.",
                stacklevel=2,
            )
            precinct_empty = pd.DataFrame(columns=cfg.schema.join_cols + ["election_year"])
            county_empty   = pd.DataFrame(columns=cfg.schema.county_cols)
            state_empty    = pd.DataFrame(columns=cfg.schema.state_cols)
            return precinct_empty, county_empty, state_empty

        elections = self._filter_elections(
            all_elections,
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

        print(f"[NC] Scraping {len(supported)} election(s)...")
        for e in supported:
            election_date = _get_attr(e, "election_date")
            print(f"[NC]   {election_date}: downloading ZIP...", flush=True)
            try:
                precinct_df, county_df, state_df = self._scrape_one(e)
                precinct_frames.append(precinct_df)
                county_frames.append(county_df)
                state_frames.append(state_df)
            except Exception as ex:
                failed.append((e, ex))
                print(
                    "[NC] WARNING: failed to scrape "
                    f"{election_date} ({_get_attr(e,'zip_url')}): {ex}"
                )

        if failed:
            print(
                f"[NC] NOTE: {len(failed)} election(s) failed; "
                f"returning {len(precinct_frames)} successful result(s)."
            )

        # If nothing succeeded, return empty DFs with expected schemas
        if not precinct_frames:
            if failed:
                raise RuntimeError(
                    f"[NC] All {len(failed)} election(s) failed to scrape. "
                    f"This usually means the site is unreachable or its structure has changed. "
                    f"See the warning messages printed above for details."
                )
            precinct_empty = pd.DataFrame(columns=cfg.schema.join_cols + ["election_year"])
            county_empty = pd.DataFrame(columns=cfg.schema.county_cols)
            state_empty = pd.DataFrame(columns=cfg.schema.state_cols)
            return precinct_empty, county_empty, state_empty

        precinct_final = pd.concat(precinct_frames, ignore_index=True)
        county_final = concat_or_empty(county_frames)
        state_final  = concat_or_empty(state_frames)

        print(
            f"[NC] Done. {len(precinct_final):,} total precinct rows, "
            f"{len(county_final):,} county rows, {len(state_final):,} state rows."
        )
        return precinct_final, county_final, state_final

    def _scrape_one(self, election) -> pd.DataFrame:
        cfg = get_config()
        zip_url = _get_attr(election, "zip_url")
        election_date = _get_attr(election, "election_date")

        zip_bytes = download_zip_bytes(zip_url)
        _member, raw = read_results_pct_from_zip(zip_bytes)

        norm = normalize_northcarolina_results_cols(raw, fallback_election_date=election_date)

        # Aggregation functions expect choice/choice_party/total_votes internally
        county_df = aggregate_to_county_level(norm)
        state_df = aggregate_county_to_state(county_df)

        # Add election_year to county/state outputs derived from election_date
        for df in (county_df, state_df):
            df["election_year"] = (
                pd.to_datetime(df["election_date"], errors="coerce")
                .dt.year
                .astype("Int64")
            )

        # Rename contest_name → full_office_name; derive short office label for county/state
        county_df = county_df.rename(columns={"contest_name": "full_office_name"})
        state_df  = state_df.rename(columns={"contest_name": "full_office_name"})
        county_df["office"] = county_df["full_office_name"].apply(extract_office_short)
        state_df["office"]  = state_df["full_office_name"].apply(extract_office_short)

        # Enforce canonical column order from schema
        county_df = county_df.reindex(columns=cfg.schema.county_cols)
        state_df = state_df.reindex(columns=cfg.schema.state_cols)

        # Drop columns that are not wanted in the precinct output.
        norm = norm.drop(columns=["provisional", "precinct_abbrv", "real_precinct"], errors="ignore")

        # Rename precinct columns to match county/state schema so all three
        # levels share candidate/party/votes as join keys; also derive office columns
        norm = norm.rename(columns={
            "choice":        "candidate",
            "choice_party":  "party",
            "total_votes":   "votes",
            "contest_name":  "full_office_name",
        })
        norm["office"] = norm["full_office_name"].apply(extract_office_short)

        # Compute precinct-level vote_pct and precinct_winner
        contest_cols = ["state", "election_date", "county", "precinct", "full_office_name"]
        contest_total = norm.groupby(contest_cols, dropna=False)["votes"].transform("sum")
        norm["vote_pct"] = ((norm["votes"] / contest_total) * 100).round(2)
        max_votes = norm.groupby(contest_cols, dropna=False)["votes"].transform("max")
        norm["precinct_winner"] = norm["votes"].eq(max_votes)

        print(f"[NC]   Done: {len(norm):,} precinct rows, {len(county_df):,} county rows.")
        return norm, county_df, state_df


def get_nc_election_results(
    year_from: "int | None" = None,
    year_to: "int | None" = None,
    min_supported_date: "date | None" = None,
    max_supported_date: "date | None" = None,
) -> dict:
    """Return NC election results at precinct, county, and state levels.

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

    Returns
    -------
    dict with keys ``'precinct'``, ``'county'``, and ``'state'``.
    Reticulate converts this to a named R list.
    """
    start, end = year_to_date_range(year_from, year_to)

    pipeline = NcElectionPipeline()
    precinct_df, county_df, state_df = pipeline.run(
        start_date=start,
        end_date=end,
        min_supported_date=min_supported_date,
        max_supported_date=max_supported_date,
    )
    return {"precinct": precinct_df, "county": county_df, "state": state_df}
