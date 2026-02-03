from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

import pandas as pd


def _norm_col(c: object) -> str:
    return re.sub(r"\s+", " ", str(c).strip().lower())


# -------------------------------------------------------------------
# Canonical intermediate columns we want downstream:
# county, election_date, precinct, contest_group_id, contest_type,
# contest_name, choice, choice_party, vote_for,
# election_day, early_voting, absentee_by_mail, provisional, total_votes,
# plus optional older-era fields:
# district, runoff_status, recount_status, winner_status, absentee_or_early, ftp_date, precinct_abbrv
# -------------------------------------------------------------------
_COL_MAP = {
    # ----------------------------
    # After Jan 1, 2014 (tab delimited) and after July 1, 2018
    # ----------------------------
    "county": "county",
    "election date": "election_date",
    "precinct": "precinct",
    "contest group id": "contest_group_id",
    "contest type": "contest_type",
    "contest name": "contest_name",
    "choice": "choice",
    "choice party": "choice_party",
    "vote for": "vote_for",
    "election day": "election_day",
    "early voting": "early_voting",
    "absentee by mail": "absentee_by_mail",
    "provisional": "provisional",
    "total votes": "total_votes",
    "real precinct": "real_precinct",

    # common variants on older files
    "absentee / one stop": "absentee_or_early",
    "absentee/one stop": "absentee_or_early",
    "one stop": "absentee_or_early",

    # ----------------------------
    # 2010–2014 (CSV)
    # ----------------------------
    "county_name": "county",
    "contest": "contest_name",
    "party": "choice_party",
    "contest_type": "contest_type",
    "runoff_status": "runoff_status",
    "recount_status": "recount_status",
    "winner_status": "winner_status",
    "district": "district",
    "total votes": "total_votes",

    # ----------------------------
    # 2008–2010 (CSV)
    # ----------------------------
    "absentee/early voting": "absentee_or_early",

    # ----------------------------
    # 2007–2008 (CSV)
    # ----------------------------
    "election_dt": "election_date",
    "name_on_ballot": "choice",
    "party_cd": "choice_party",
    "election_day_count": "election_day",
    "absentee_count": "absentee_or_early",
    "provisional_count": "provisional",
    "total_vote_count": "total_votes",

    # ----------------------------
    # Before 2007 (CSV)
    # ----------------------------
    "precinct_abbrv": "precinct_abbrv",
    "contest_name": "contest_name",
    "ballot_count": "total_votes",  # total votes all total
    "ftp_date": "ftp_date",
}


def _parse_nc_date(x: object) -> object:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return pd.NA
    s = str(x).strip()
    if not s:
        return pd.NA

    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return pd.NA


# -------------------------------------------------------------------
# Headerless / positional layouts
# When io_utils detects "no real header", it reads with header=None,
# giving integer columns 0..N-1. We map by column count to the known
# NC layouts from layout_results_pct.txt.
# -------------------------------------------------------------------
def _is_headerless(df: pd.DataFrame) -> bool:
    # header=None -> RangeIndex-like integer columns
    return all(isinstance(c, int) for c in df.columns)


def _assign_positional_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign column names for headerless files using known NC layouts.

    We use column-count-based heuristics that match the NC documentation:
      - Before 2007: 9 cols  (county, election_dt, precinct_abbrv, precinct, contest_name, name_on_ballot, party_cd, ballot_count, ftp_date)
      - 2007–2008: 11 cols  (county, election_dt, precinct, contest, name_on_ballot, party_cd, election_day_count, absentee_count, provisional_count, total_vote_count)  <-- some zips omit fields; see note
      - 2008–2010: 13 cols  (county_name, precinct, contest_type, runoff_status, recount_status, contest, choice, winner_status, party, election day, absentee/early voting, provisional, total votes, district)
      - 2010–2014: 15 cols  (county_name, precinct, contest_type, runoff_status, recount_status, contest, choice, winner_status, party, election day, early voting, absentee by mail, provisional, total votes, district)

    Reality: some files omit "district" or include ftp_date; so we handle a few near-matches.
    """
    n = df.shape[1]

    # --- Before 2007 (9 columns) ---
    if n == 9:
        cols = [
            "county",
            "election_dt",
            "precinct_abbrv",
            "precinct",
            "contest_name",
            "name_on_ballot",
            "party_cd",
            "ballot_count",
            "ftp_date",
        ]
        return df.set_axis(cols, axis=1, copy=False)

    # --- 2007–2008-ish (10 columns) ---
    # Some files appear as 10 cols without an explicit total_vote_count vs ballot_count split.
    # In practice, if it has election_day_count + absentee_count + provisional_count + total_vote_count, it’s 10.
    if n == 10:
        cols = [
            "county",
            "election_dt",
            "precinct",
            "contest",
            "name_on_ballot",
            "party_cd",
            "election_day_count",
            "absentee_count",
            "provisional_count",
            "total_vote_count",
        ]
        return df.set_axis(cols, axis=1, copy=False)

    # --- 2008–2010 (14 columns) ---
    # Docs show 14 including district; sometimes district missing -> 13.
    if n == 14:
        cols = [
            "county_name",
            "precinct",
            "contest_type",
            "runoff_status",
            "recount_status",
            "contest",
            "choice",
            "winner_status",
            "party",
            "election day",
            "absentee/early voting",
            "provisional",
            "total votes",
            "district",
        ]
        return df.set_axis(cols, axis=1, copy=False)

    if n == 13:
        # same as above but no district
        cols = [
            "county_name",
            "precinct",
            "contest_type",
            "runoff_status",
            "recount_status",
            "contest",
            "choice",
            "winner_status",
            "party",
            "election day",
            "absentee/early voting",
            "provisional",
            "total votes",
        ]
        return df.set_axis(cols, axis=1, copy=False)

    # --- 2010–2014 (15 columns) ---
    if n == 15:
        cols = [
            "county_name",
            "precinct",
            "contest_type",
            "runoff_status",
            "recount_status",
            "contest",
            "choice",
            "winner_status",
            "party",
            "election day",
            "early voting",
            "absentee by mail",
            "provisional",
            "total votes",
            "district",
        ]
        return df.set_axis(cols, axis=1, copy=False)

    # If we can't confidently map, leave as-is (upstream will warn/fail gracefully).
    return df


def normalize_nc_results_cols(
    df: pd.DataFrame,
    fallback_election_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    Normalize NC results file columns across eras.
    If election_date is missing in the file, optionally fill it from fallback_election_date.
    """
    work = df.copy()

    if _is_headerless(work):
        work = _assign_positional_columns(work)

    rename = {}
    for c in work.columns:
        key = _norm_col(c)
        if key in _COL_MAP:
            rename[c] = _COL_MAP[key]

    out = work.rename(columns=rename).copy()

    # Parse election_date if present
    if "election_date" in out.columns:
        out["election_date"] = out["election_date"].apply(_parse_nc_date)

    # If election_date missing OR all NA, fill from fallback
    if fallback_election_date is not None:
        if "election_date" not in out.columns:
            out["election_date"] = fallback_election_date
        else:
            if out["election_date"].isna().all():
                out["election_date"] = fallback_election_date

    # numeric coercions
    numeric_cols = [
        "vote_for",
        "election_day",
        "early_voting",
        "absentee_by_mail",
        "provisional",
        "total_votes",
        "absentee_or_early",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "choice_party" in out.columns:
        out["choice_party"] = (
            out["choice_party"]
            .astype(str)
            .str.strip()
            .replace({"nan": pd.NA, "none": pd.NA, "": pd.NA})
        )

    for col in ["county", "precinct", "contest_name", "choice"]:
        if col in out.columns:
            out[col] = out[col].astype(str).replace({"nan": pd.NA, "None": pd.NA}).str.strip()

    return out
