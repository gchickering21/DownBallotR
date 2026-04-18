"""
Centralized column schema definitions for all state election results pipelines.

Defines the complete output column lists for each state/level combination.
Each parser's internal _*_COLS list remains the partial (no-state) working schema;
these lists are the authoritative final output schemas that include "state".

Usage in pipelines
------------------
    from column_schemas import CT_STATE_COLS, CT_TOWN_COLS, finalize_df

    final_state_df = finalize_df(final_state_df, CT_STATE_COLS, state="CT")
    final_town_df  = finalize_df(final_town_df,  CT_TOWN_COLS,  state="CT")

Adding a new state
------------------
1. Define <STATE>_STATE_COLS (and _COUNTY_COLS / _PRECINCT_COLS as needed) below.
2. In the new state's pipeline, import and call finalize_df() at the end of run().
3. Return empty DataFrames as pd.DataFrame(columns=<STATE>_STATE_COLS) so callers
   always get a predictable schema even when no data is found.
"""

from __future__ import annotations

import re
import pandas as pd
import numpy as np

# Matches candidate names that represent non-candidate entries (write-ins, blanks, etc.)
_WRITEIN_RE = re.compile(
    r"^\s*(?!adjudicated\s+write[\s\-]*ins?\b)(?:"
    r"write[\s\-]*in(?:s|votes?)?(?::.*)?"   # allow suffix like ": Invalid"
    r"|all\s+write[\s\-]*ins"
    r"|scattering"
    r"|blanks?"
    r"|void"
    r"|over\s*votes?"
    r"|under\s*votes?"
    r")\s*$",
    re.IGNORECASE,
)


# ── Universal base sets ────────────────────────────────────────────────────────
# Every state produces at least these columns at the given level.

BASE_STATE_COLS: list[str] = [
    "state", "election_year",
    "office_level", "office", "district",
    "candidate", "party",
    "votes", "vote_pct", "winner",
]

BASE_COUNTY_COLS: list[str] = [
    "state", "election_year",
    "office_level", "office", "district",
    "candidate", "party",
    "votes", "vote_pct", "county_winner",
]

BASE_PRECINCT_COLS: list[str] = [
    "state", "election_year",
    "office_level", "office", "district",
    "candidate", "party",
    "votes", "vote_pct", "precinct_winner",
]


# ── Connecticut ────────────────────────────────────────────────────────────────

CT_STATE_COLS: list[str] = [
    "state", "election_name", "election_year", "election_date", "election_type",
    "office_level", "office", "district", "town",
    "candidate", "party",
    "votes", "vote_pct", "winner",
]

CT_TOWN_COLS: list[str] = [
    "state", "election_name", "election_year", "election_date", "election_type",
    "office_level", "office", "district", "town",
    "candidate", "party",
    "votes", "vote_pct", "town_winner",
]


# ── Clarity (Georgia + Utah) ───────────────────────────────────────────────────

CLARITY_STATE_COLS: list[str] = [
    "state", "election_name", "election_type", "election_year", "election_date",
    "office_level", "office", "district",
    "candidate", "party",
    "votes", "vote_pct", "winner",
    "url",
]

CLARITY_COUNTY_COLS: list[str] = [
    "state", "election_name", "election_type", "election_year", "election_date",
    "office_level", "office", "district", "county",
    "candidate", "party",
    "votes", "vote_pct", "county_winner",
    "url",
]

CLARITY_PRECINCT_COLS: list[str] = [
    "state", "election_name", "election_type", "election_year", "election_date",
    "office_level", "office", "district", "county", "precinct",
    "candidate", "party",
    "votes", "vote_pct", "precinct_winner",
    "url",
]

# Vote-method variants (optional; returned when include_vote_methods=True)
CLARITY_VM_STATE_COLS: list[str] = [
    "state", "election_name", "election_type", "election_year", "election_date",
    "office_level", "office", "district",
    "candidate", "party",
    "votes_advance_in_person", "votes_election_day",
    "votes_absentee", "votes_provisional", "votes_total",
    "url",
]

CLARITY_VM_COUNTY_COLS: list[str] = [
    "state", "election_name", "election_type", "election_year", "election_date",
    "office_level", "office", "district", "county",
    "candidate", "party",
    "votes_advance_in_person", "votes_election_day",
    "votes_absentee", "votes_provisional", "votes_total",
    "url",
]


# ── Indiana ────────────────────────────────────────────────────────────────────

IN_STATE_COLS: list[str] = [
    "state", "election_year", "election_date", "election_type",
    "office_level", "office", "district",
    "candidate", "party",
    "votes", "vote_pct", "winner",
    "num_seats",
]

IN_COUNTY_COLS: list[str] = [
    "state", "election_year", "election_date", "election_type",
    "office_level", "office", "district", "county_name",
    "candidate", "party",
    "votes", "vote_pct", "county_winner",
    "num_seats",
]


# ── Louisiana ─────────────────────────────────────────────────────────────────

LA_STATE_COLS: list[str] = [
    "state", "election_name", "election_year", "election_date",
    "office_level", "office", "district",
    "candidate", "party",
    "votes", "vote_pct", "winner",
    "voter_turnout_pct",
]

LA_PARISH_COLS: list[str] = [
    "state", "election_name", "election_year", "election_date",
    "office_level", "office", "district", "parish",
    "candidate", "party",
    "votes", "vote_pct", "parish_winner",
    "parish_voter_turnout_pct",
]


# ── ElectionStats (CO, ID, MA, NH, NM, NY, SC, VT, VA) ───────────────────────

ES_STATE_COLS: list[str] = [
    "state", "election_year", "election_id", "election_type",
    "office_level", "office", "district",
    "candidate_id", "candidate", "party",
    "votes", "vote_pct", "winner",
    "url",
]

ES_COUNTY_COLS: list[str] = [
    "state", "election_year", "election_id", "election_type",
    "office_level", "office", "district", "county_or_city",
    "candidate_id", "candidate", "party",
    "votes", "vote_pct", "county_winner",
    "url",
]

ES_PRECINCT_COLS: list[str] = [
    "state", "election_year", "election_id", "election_type",
    "office_level", "office", "district", "county", "precinct",
    "candidate_id", "candidate", "party",
    "votes", "vote_pct", "precinct_winner",
    "url",
]

# ── Helpers ────────────────────────────────────────────────────────────────────
def drop_writeins(df: pd.DataFrame, candidate_col: str = "candidate") -> pd.DataFrame:
    """Drop rows where the candidate column is a write-in, blank, scattering, void, etc."""
    if df.empty or candidate_col not in df.columns:
        return df

    mask = df[candidate_col].astype(str).str.match(_WRITEIN_RE, na=False)
    out = df[~mask].reset_index(drop=True)
    return out


def compute_vote_pct(
    df: pd.DataFrame,
    group_cols: list[str],
    *,
    total_col: str | None = None,
    fill_missing_only: bool = False,
) -> pd.DataFrame:
    """Compute vote_pct as votes / contest_total * 100, rounded to 2 decimal places.

    Parameters
    ----------
    group_cols : list[str]
        Columns that together identify a unique contest at the desired level.
        Include geographic columns (e.g. county_name, precinct) for sub-state
        levels. Columns absent from df are silently ignored.
    total_col : str | None
        If provided and present in df, use this column as the denominator
        instead of summing votes within group. Useful when the source reports
        a total that includes blanks/write-ins/scattering.
    fill_missing_only : bool
        When True, only fills rows where vote_pct is currently null/NaN.
        Useful for parsers that extract vote_pct from HTML but miss some rows.
    """
    if df.empty or "votes" not in df.columns:
        return df

    df = df.copy()
    votes = pd.to_numeric(df["votes"], errors="coerce").astype("float64")

    if total_col and total_col in df.columns:
        denom = pd.to_numeric(df[total_col], errors="coerce").astype("float64")
        denom = denom.replace(0, np.nan)
    else:
        present = [c for c in group_cols if c in df.columns]
        if present:
            denom = (
                votes.groupby(df.groupby(present, dropna=False).ngroup())
                .transform("sum")
                .astype("float64")
                .replace(0, np.nan)
            )
        else:
            total = votes.sum()
            denom = float(total) if pd.notna(total) and total != 0 else np.nan

    computed = ((votes / denom) * 100).astype("float64").round(2)
    computed = computed.where(votes != 0, 0)

    if fill_missing_only and "vote_pct" in df.columns:
        existing = pd.to_numeric(df["vote_pct"], errors="coerce")
        missing = existing.isna()
        df.loc[missing, "vote_pct"] = computed[missing]
    else:
        df["vote_pct"] = computed

    return df


def finalize_df(
    df: pd.DataFrame,
    cols: list[str],
    *,
    state: str | None = None,
    rename_map: dict[str, str] | None = None,
) -> pd.DataFrame:

    if rename_map:
        df = df.rename(columns=rename_map)

    if state is not None:
        df = df.copy()
        df["state"] = state

    df = drop_writeins(df)

    out = df.reindex(columns=cols)

    if "district" in out.columns:
        out["district"] = out["district"].fillna("").astype(str).replace("None", "")

    n_before = len(out)
    out = out.drop_duplicates().reset_index(drop=True)
    n_dropped = n_before - len(out)
    if n_dropped:
        print(f"[finalize_df] dropped {n_dropped:,} exact duplicate row(s).")

    return out
