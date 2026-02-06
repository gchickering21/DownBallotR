from __future__ import annotations

import pandas as pd

import pandas as pd

def aggregate_county_to_state(df_county: pd.DataFrame) -> pd.DataFrame:
    # df_county is already county-level; we roll it up across counties
    group_cols = [
        "state",
        "election_date",
        "contest_name",
        "choice",
        "choice_party",
        "jurisdiction",
        "office",
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

    # Contest grain (no county here)
    contest_cols = ["state", "election_date", "contest_name", "jurisdiction", "office", "district"]

    # 2) Vote share within contest
    contest_total = out.groupby(contest_cols, dropna=False)["total_votes"].transform("sum")
    out["vote_share"] = ((out["total_votes"] / contest_total) * 100).round(2)


    # 3) Winner/Loser with tie handling
    max_votes = out.groupby(contest_cols, dropna=False)["total_votes"].transform("max")
    is_top = out["total_votes"].eq(max_votes)

    # count top-vote entries per contest to detect ties
    n_top = (
        is_top.groupby([out[c] for c in contest_cols], dropna=False)
        .transform("sum")
    )

    out["contest_outcome"] = "Loser"
    out.loc[is_top & (n_top == 1), "contest_outcome"] = "Winner"
    out.loc[is_top & (n_top > 1), "contest_outcome"] = "Tied Winner"

    return out


def aggregate_to_county_level(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = [
        "state",
        "county",
        "election_date",
        "contest_name",
        "choice",
        "choice_party",
        "jurisdiction",
        "office",
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

    return out

