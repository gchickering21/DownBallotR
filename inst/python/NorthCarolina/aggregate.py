from __future__ import annotations

import pandas as pd

import pandas as pd

def aggregate_county_to_state(df_county: pd.DataFrame) -> pd.DataFrame:
    # df_county is already county-level (with renamed candidate/party/votes columns);
    # we roll it up across counties to produce statewide totals.
    group_cols = [
        "state",
        "election_date",
        "contest_name",
        "candidate",
        "party",
        "jurisdiction",
        "office_level",
        "district"
    ]

    vote_cols = [
        "election_day",
        "early_voting",
        "absentee_by_mail",
        "absentee_or_early",
        "provisional",
        "votes",
    ]

    required = group_cols + vote_cols
    missing = [c for c in required if c not in df_county.columns]
    if missing:
        raise ValueError(
            f"Missing required columns for aggregation: {missing}. "
            f"Columns={list(df_county.columns)}"
        )

    work = df_county.copy()

    # Ensure vote columns are numeric integers
    work[vote_cols] = (
        work[vote_cols]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype("Int64")
    )

    # 1) Roll up county -> state totals
    out = (
        work.groupby(group_cols, dropna=False, as_index=False)[vote_cols]
        .sum()
    )

    # Contest grain: contest_name uniquely identifies a race; office_level/jurisdiction/district
    # are per-contest attributes carried through for groupby correctness.
    contest_cols = ["state", "election_date", "contest_name", "jurisdiction", "office_level", "district"]

    # 2) Vote share within contest
    contest_total = out.groupby(contest_cols, dropna=False)["votes"].transform("sum")
    out["vote_pct"] = ((out["votes"] / contest_total) * 100).round(2)

    # 3) Boolean winner: True for highest-voted candidate(s) per contest (ties share True)
    max_votes = out.groupby(contest_cols, dropna=False)["votes"].transform("max")
    out["winner"] = out["votes"].eq(max_votes)

    return out


def aggregate_to_county_level(df: pd.DataFrame) -> pd.DataFrame:
    # Input df uses the raw precinct column names (choice, choice_party, total_votes).
    # Output renames these to the standard schema (candidate, party, votes).
    # office_level is already computed in normalize.py and present in df.
    group_cols = [
        "state",
        "county",
        "election_date",
        "contest_name",
        "choice",
        "choice_party",
        "jurisdiction",
        "office_level",
        "district"
    ]

    vote_cols = [
        "election_day",
        "early_voting",
        "absentee_by_mail",
        "absentee_or_early",
        "provisional",
        "total_votes",
    ]

    required = group_cols + vote_cols
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns for aggregation: {missing}. "
            f"Columns={list(df.columns)}"
        )

    work = df.copy()

    # Ensure vote columns are numeric integers
    work[vote_cols] = (
        work[vote_cols]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype("Int64")
    )

    out = (
        work
        .groupby(group_cols, dropna=False, as_index=False)[vote_cols]
        .sum()
    )

    # Rename raw precinct column names to standard schema
    out = out.rename(columns={
        "choice":       "candidate",
        "choice_party": "party",
        "total_votes":  "votes",
    })

    contest_cols = ["state", "election_date", "county", "contest_name", "jurisdiction", "office_level", "district"]
    contest_total = out.groupby(contest_cols, dropna=False)["votes"].transform("sum")
    out["vote_pct"] = ((out["votes"] / contest_total) * 100).round(2)
    max_votes = out.groupby(contest_cols, dropna=False)["votes"].transform("max")
    out["county_winner"] = out["votes"].eq(max_votes)

    return out

