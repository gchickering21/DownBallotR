from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from typing import Iterable

import pandas as pd

from .constants import NC_MIN_SUPPORTED_ELECTION_DATE
from .discovery import discover_nc_results_zips
from .selection import select_elections
from .io_utils import download_zip_bytes, read_results_pct_from_zip
from .normalize import normalize_nc_results_cols
from .aggregate import aggregate_to_contest_level
from .canonicalize import classify_nc_office, extract_district, extract_jurisdiction


_CANONICAL_COLUMNS = [
    "state",
    "year",
    "election_date",
    "election_type",
    "office",
    "office_raw",
    "jurisdiction",
    "jurisdiction_type",
    "district",
    "candidate",
    "party",
    "votes",
    "vote_share",
    "won",
    "incumbent",
    "source_url",
    "retrieved_at",
]


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

    def run(self, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        elections = self.discover()
        elections = self._filter_elections(elections, start_date=start_date, end_date=end_date)

        supported = []
        skipped = []
        for e in elections:
            ed = _get_attr(e, "election_date")
            if ed < NC_MIN_SUPPORTED_ELECTION_DATE:
                skipped.append(e)
            else:
                supported.append(e)

        if skipped:
            print(
                f"[NC] NOTE: skipping {len(skipped)} election(s) before "
                f"{NC_MIN_SUPPORTED_ELECTION_DATE.isoformat()} "
                f"(legacy layouts not yet supported)."
            )

        frames: list[pd.DataFrame] = []
        failed: list[tuple[object, Exception]] = []

        for e in supported:
            try:
                frames.append(self._scrape_one(e))
            except Exception as ex:
                failed.append((e, ex))
                print(f"[NC] WARNING: failed to scrape {_get_attr(e,'election_date')} ({_get_attr(e,'zip_url')}): {ex}")

        if failed:
            print(f"[NC] NOTE: {len(failed)} election(s) failed; returning results for {len(frames)} election(s).")

        if not frames:
            return pd.DataFrame(columns=_CANONICAL_COLUMNS)

        out = pd.concat(frames, ignore_index=True)

        # Ensure canonical columns exist + order is stable
        for c in _CANONICAL_COLUMNS:
            if c not in out.columns:
                out[c] = pd.NA
        out = out[_CANONICAL_COLUMNS]

        return out

    def _scrape_one(self, election) -> pd.DataFrame:
        zip_url = _get_attr(election, "zip_url")
        election_date = _get_attr(election, "election_date")

        zip_bytes = download_zip_bytes(zip_url)
        _member, raw = read_results_pct_from_zip(zip_bytes)

        norm = normalize_nc_results_cols(raw, fallback_election_date=election_date)
        contest = aggregate_to_contest_level(norm)

        # aggregate_to_contest_level outputs: election_date, contest_name, candidate, party, votes, vote_share, won, ...
        # Preserve the raw contest name for future mapping work.
        contest = contest.rename(columns={"contest_name": "contest_name_raw"}).copy()

        # ---- Raw identifiers (always preserved) ----
        contest["office_raw"] = contest["contest_name_raw"].astype(str)

        # ---- Two-step office strategy (Option 2) ----
        # 1) office_guess: what our current heuristic thinks
        contest["office_guess"] = contest["contest_name_raw"].apply(classify_nc_office)

        # 2) office_mapped: placeholder for future mapping table / overrides
        #    For now, initialize as NA (or leave if already present).
        if "office_mapped" not in contest.columns:
            contest["office_mapped"] = pd.NA

        # 3) office: the effective value used downstream (mapped overrides guess)
        #    If neither exists, keep as "unclassified" (but DO NOT drop rows).
        contest["office"] = contest["office_mapped"].combine_first(contest["office_guess"])
        unclassified_mask = contest["office"].isna()
        if unclassified_mask.any():
            example = contest.loc[unclassified_mask, "office_raw"].iloc[0]
            print(
                f"[NC] NOTE: keeping {int(unclassified_mask.sum())} unclassified contests "
                f"(e.g., '{example}')."
            )
            contest.loc[unclassified_mask, "office"] = "unclassified"

        # ---- Other parsing based on effective office ----
        contest["district"] = contest["contest_name_raw"].apply(extract_district)

        j = contest.apply(
            lambda r: extract_jurisdiction(r["contest_name_raw"], r["office"]),
            axis=1,
        )
        contest["jurisdiction"] = j.apply(lambda x: x[0])
        contest["jurisdiction_type"] = j.apply(lambda x: x[1])

        # ---- Package-wide fields ----
        contest.insert(0, "state", self.state)
        contest.insert(
            1,
            "year",
            contest["election_date"].apply(lambda d: d.year if isinstance(d, date) else pd.NA),
        )
        contest["election_type"] = "general"
        contest["incumbent"] = pd.NA
        contest["source_url"] = zip_url
        contest["retrieved_at"] = datetime.now()

        # ---- Canonical columns ----
        # If you want to keep the mapping helpers for debugging / later joins,
        # add them to _CANONICAL_COLUMNS (recommended).
        # For now, we ensure all canonical cols exist, then select.
        for c in _CANONICAL_COLUMNS:
            if c not in contest.columns:
                contest[c] = pd.NA
        contest = contest[_CANONICAL_COLUMNS]

        return contest
