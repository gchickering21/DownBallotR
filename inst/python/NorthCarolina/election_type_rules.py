from dataclasses import dataclass
from typing import Iterable
from datetime import date, datetime
import pandas as pd


# ----------------------------
# Election type classification
# ----------------------------

@dataclass(frozen=True)
class ElectionTypeRules:
    """
    Rules for classifying election_type from election_date.

    - general_dates: dates that should be labeled "General"
    - special_dates: dates that should be labeled "Special"
    - default: label used when date is not in either set (typically "Primary")
    """
    general_dates: set[date]
    special_dates: set[date]
    default: str = "Primary"


def add_election_type(df: pd.DataFrame, rules: ElectionTypeRules) -> pd.DataFrame:
    """
    Add/overwrite df['election_type'] using df['election_date'].

    Assumes df['election_date'] contains python `date` objects or pd.NA.
    """
    if "election_date" not in df.columns:
        df["election_type"] = pd.NA
        return df

    def classify(d: object) -> object:
        if pd.isna(d):
            return pd.NA
        # d should be a python date; be defensive anyway
        if d in rules.general_dates:
            return "General"
        if d in rules.special_dates:
            return "Special"
        return rules.default

    df["election_type"] = df["election_date"].apply(classify).astype("string")
    return df
