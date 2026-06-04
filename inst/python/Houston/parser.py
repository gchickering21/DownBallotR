"""Convert raw Houston PDF data into standardised DataFrames.

Two output levels
-----------------
summary  — one row per candidate per contest, county-wide totals (from cumulative PDF).
precinct — one row per candidate per precinct per contest (from canvass PDF).

Public functions
----------------
to_summary_df(cumulative_contests, election, drop_writeins) → pd.DataFrame
to_precinct_df(canvass_contests, election, drop_writeins)   → pd.DataFrame
"""

from __future__ import annotations

import pandas as pd

from office_level_utils import classify_office_level

from .models import HoustonElectionInfo
from .pdf_parser import CumulativeContest, CanvassContest

_SUMMARY_CONTEST_COLS  = ["election_name", "election_year", "election_date", "office", "district"]
_PRECINCT_CONTEST_COLS = ["election_name", "election_year", "election_date", "office", "district", "precinct"]


def _add_winner(
    df: pd.DataFrame,
    contest_cols: list[str],
    winner_col: str = "winner",
) -> pd.DataFrame:
    """Mark the candidate(s) with the highest votes in each contest as winner=True."""
    if df.empty or "votes" not in df.columns:
        return df
    votes = pd.to_numeric(df["votes"], errors="coerce")
    present = [c for c in contest_cols if c in df.columns]
    if present:
        max_votes = votes.groupby(df.groupby(present, dropna=False).ngroup()).transform("max")
    else:
        max_votes = votes.max()
    df[winner_col] = (votes == max_votes) & votes.notna()
    return df


def to_summary_df(
    cumulative_contests: "list[CumulativeContest]",
    election: HoustonElectionInfo,
) -> pd.DataFrame:
    """Build a county-wide summary DataFrame from the cumulative PDF contests.

    One row per candidate per contest.  Columns include the six vote-method
    breakdowns (votes_bm, votes_ev, votes_ed, votes_evp, votes_edp) plus the
    TOTAL column (votes).

    Returns
    -------
    pd.DataFrame
        Columns: election_name, election_year, election_date, election_type,
        office_level, office, district, candidate, party,
        votes_bm, votes_ev, votes_ed, votes_evp, votes_edp,
        votes, vote_pct, winner.
    """
    col_names:     list = []
    col_years:     list = []
    col_dates:     list = []
    col_types:     list = []
    col_levels:    list = []
    col_offices:   list = []
    col_districts: list = []
    col_cands:     list = []
    col_parties:   list = []
    col_bm:        list = []
    col_ev:        list = []
    col_ed:        list = []
    col_evp:       list = []
    col_edp:       list = []
    col_votes:     list = []

    e_name  = election.name
    e_year  = election.year
    e_date  = election.election_date
    e_type  = election.election_type

    for contest in cumulative_contests:
        o_level  = classify_office_level(contest.office)
        office   = contest.office
        district = contest.district
        for cand in contest.candidates:
            col_names.append(e_name)
            col_years.append(e_year)
            col_dates.append(e_date)
            col_types.append(e_type)
            col_levels.append(o_level)
            col_offices.append(office)
            col_districts.append(district)
            col_cands.append(cand.candidate)
            col_parties.append(cand.party)
            col_bm.append(cand.votes_bm)
            col_ev.append(cand.votes_ev)
            col_ed.append(cand.votes_ed)
            col_evp.append(cand.votes_evp)
            col_edp.append(cand.votes_edp)
            col_votes.append(cand.votes)

    if not col_names:
        return pd.DataFrame()

    df = pd.DataFrame({
        "election_name":  col_names,
        "election_year":  col_years,
        "election_date":  col_dates,
        "election_type":  col_types,
        "office_level":   col_levels,
        "office":         col_offices,
        "district":       col_districts,
        "candidate":      col_cands,
        "party":          col_parties,
        "votes_bm":       col_bm,
        "votes_ev":       col_ev,
        "votes_ed":       col_ed,
        "votes_evp":      col_evp,
        "votes_edp":      col_edp,
        "votes":          col_votes,
    })
    return _add_winner(df, _SUMMARY_CONTEST_COLS, winner_col="winner")


def to_precinct_df(
    canvass_contests: "list[CanvassContest]",
    election: HoustonElectionInfo,
) -> pd.DataFrame:
    """Build a precinct-level DataFrame from the canvass PDF contests.

    One row per (candidate, precinct) pair per contest.

    Returns
    -------
    pd.DataFrame
        Columns: election_name, election_year, election_date, election_type,
        office_level, office, district, precinct, candidate, party,
        votes, vote_pct, precinct_winner.
    """
    col_names:     list = []
    col_years:     list = []
    col_dates:     list = []
    col_types:     list = []
    col_levels:    list = []
    col_offices:   list = []
    col_districts: list = []
    col_precincts: list = []
    col_cands:     list = []
    col_parties:   list = []
    col_votes:     list = []

    e_name = election.name
    e_year = election.year
    e_date = election.election_date
    e_type = election.election_type

    for contest in canvass_contests:
        o_level    = classify_office_level(contest.office)
        office     = contest.office
        district   = contest.district
        candidates = contest.candidates
        parties    = contest.parties
        n_cands    = len(candidates)
        n_parties  = len(parties)
        # Precompute per-contest party list (same for every precinct row)
        parties_padded = [parties[i] if i < n_parties else "" for i in range(n_cands)]

        for prow in contest.precinct_rows:
            precinct   = prow.precinct
            cand_votes = prow.candidate_votes
            n_votes    = len(cand_votes)
            col_names.extend([e_name]    * n_cands)
            col_years.extend([e_year]    * n_cands)
            col_dates.extend([e_date]    * n_cands)
            col_types.extend([e_type]    * n_cands)
            col_levels.extend([o_level]  * n_cands)
            col_offices.extend([office]  * n_cands)
            col_districts.extend([district] * n_cands)
            col_precincts.extend([precinct] * n_cands)
            col_cands.extend(candidates)
            col_parties.extend(parties_padded)
            col_votes.extend(
                [cand_votes[i] if i < n_votes else None for i in range(n_cands)]
            )

    if not col_names:
        return pd.DataFrame()

    df = pd.DataFrame({
        "election_name":  col_names,
        "election_year":  col_years,
        "election_date":  col_dates,
        "election_type":  col_types,
        "office_level":   col_levels,
        "office":         col_offices,
        "district":       col_districts,
        "precinct":       col_precincts,
        "candidate":      col_cands,
        "party":          col_parties,
        "votes":          col_votes,
    })
    return _add_winner(df, _PRECINCT_CONTEST_COLS, winner_col="precinct_winner")
