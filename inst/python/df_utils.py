"""
DataFrame helpers shared across DownBallotR scrapers.
"""

from __future__ import annotations

import pandas as pd


def concat_or_empty(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate a list of DataFrames; return an empty DataFrame if the list is empty."""
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def rows_to_dataframe(rows: list) -> pd.DataFrame:
    """Convert a list of dataclass instances to a DataFrame via ``asdict``.

    Returns an empty DataFrame (no columns) when *rows* is empty.
    """
    from dataclasses import asdict
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame.from_records([asdict(r) for r in rows])
