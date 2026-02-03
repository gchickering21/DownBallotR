from __future__ import annotations

import pandas as pd


def aggregate_to_contest_level(df: pd.DataFrame) -> pd.DataFrame:
    # Must have these after normalization
    required = ["election_date", "contest_name", "choice", "total_votes"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns for aggregation: {missing}. "
            f"Columns={list(df.columns)}"
        )

    work = df.copy()
    work["candidate"] = work["choice"].astype(str).str.strip()
    work["party"] = work["choice_party"] if "choice_party" in work.columns else pd.NA

    # Contest identity keys (contest_group_id not present in older eras)
    group_keys = ["election_date", "contest_name"]
    if "contest_group_id" in work.columns:
        group_keys.insert(1, "contest_group_id")  # keep stable-ish order
    if "district" in work.columns:
        group_keys.append("district")

    group_choice = group_keys + ["candidate", "party"]

    agg = (
        work.groupby(group_choice, dropna=False, as_index=False)
        .agg(
            votes=("total_votes", "sum"),
            vote_for=("vote_for", "max") if "vote_for" in work.columns else ("total_votes", "size"),
        )
    )

    # contest totals + share
    totals = agg.groupby(group_keys, dropna=False, as_index=False).agg(contest_total_votes=("votes", "sum"))
    out = agg.merge(totals, on=group_keys, how="left")
    out["vote_share"] = out["votes"] / out["contest_total_votes"]
    out.loc[out["contest_total_votes"].isna() | (out["contest_total_votes"] == 0), "vote_share"] = pd.NA

    # winner logic
    out["won"] = pd.NA

    # Prefer explicit winner_status when present (2010–2014 etc.)
    if "winner_status" in work.columns:
        ws = (
            work[group_choice + ["winner_status"]]
            .dropna(subset=["winner_status"])
            .drop_duplicates(subset=group_choice)
        )
        out = out.merge(ws, on=group_choice, how="left")
        # Interpret common patterns: W / WINNER / Y etc. (anything non-empty and not "N"/"0")
        def _to_won(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return pd.NA
            s = str(v).strip().lower()
            if s in {"", "0", "n", "no", "false"}:
                return pd.NA
            return True

        out["won"] = out["winner_status"].apply(_to_won)
        out = out.drop(columns=["winner_status"], errors="ignore")

    # Otherwise infer only for single-winner contests when vote_for exists
    elif "vote_for" in out.columns:
        vf = pd.to_numeric(out["vote_for"], errors="coerce")
        is_single = out.groupby(group_keys, dropna=False)["vote_for"].transform(
            lambda s: (pd.to_numeric(s, errors="coerce") == 1).any()
        )
        max_votes = out.groupby(group_keys, dropna=False)["votes"].transform("max")
        out.loc[is_single & (out["votes"] == max_votes), "won"] = True
        out.loc[is_single & (out["votes"] != max_votes), "won"] = False

    return out
