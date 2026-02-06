from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pandas as pd

from .election_type_rules import ElectionTypeRules, add_election_type
from .canonicalize import extract_jurisdiction_office_and_district

# =========================================================
# Config model + loader
# =========================================================

@dataclass(frozen=True)
class Layout:
    key: str
    start_inclusive: date
    end_exclusive: date
    columns: list[str]


@dataclass(frozen=True)
class CanonicalSchema:
    join_cols: list[str]
    numeric_cols: list[str]
    string_cols: list[str]
    county_cols: list[str]
    state_cols : list[str]


@dataclass(frozen=True)
class NcResultsConfig:
    schema: CanonicalSchema
    col_map: dict[str, str] 
    layouts: list[Layout]
    election_type_rules: ElectionTypeRules


def _parse_iso_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def load_nc_results_config(config_path: str | Path) -> NcResultsConfig:
    """
    Load NC results_pct normalization config from JSON.
    Keeps the main code small and makes updates easy/safe.
    """
    p = Path(config_path)
    data = json.loads(p.read_text(encoding="utf-8"))

    schema = CanonicalSchema(
        join_cols=list(data["canonical"]["join_cols"]),
        numeric_cols=list(data["canonical"]["numeric_cols"]),
        string_cols=list(data["canonical"]["string_cols"]),
        county_cols=list(data["canonical"]["county_cols"]),
        state_cols=list(data["canonical"]["state_cols"]),
    )

    col_map = {str(k): str(v) for k, v in data["col_map"].items()}

    layouts: list[Layout] = []
    for item in data["layouts"]:
        layouts.append(
            Layout(
                key=str(item["key"]),
                start_inclusive=_parse_iso_date(item["start_inclusive"]),
                end_exclusive=_parse_iso_date(item["end_exclusive"]),
                columns=list(item["columns"]),
            )
        )

    etr = data.get("election_type_rules", {})
    election_type_rules = ElectionTypeRules(
        general_dates={_parse_iso_date(s) for s in etr.get("general_dates", [])},
        special_dates={_parse_iso_date(s) for s in etr.get("special_dates", [])},
        default=str(etr.get("default", "Primary")),
    )

    return NcResultsConfig(
        schema=schema,
        col_map=col_map,
        layouts=layouts,
        election_type_rules=election_type_rules,
    )




# =========================================================
# Normalization helpers
# =========================================================

_WHITESPACE_RE = re.compile(r"\s+")


def _norm_col(c: object) -> str:
    """Normalize raw column labels for robust matching."""
    return _WHITESPACE_RE.sub(" ", str(c).strip().lower())


def _as_date(d: object) -> Optional[date]:
    """Accept date or datetime; return date; otherwise None."""
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return None


def _parse_nc_date(x: object) -> object:
    """Parse NC election dates into datetime.date, else NA."""
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


def _layout_for_fallback(layouts: list[Layout], fallback_election_date: Optional[object]) -> Optional[Layout]:
    d = _as_date(fallback_election_date)
    if d is None:
        return None
    for L in layouts:
        if L.start_inclusive <= d < L.end_exclusive:
            return L
    return None


# =========================================================
# Header repair logic
# =========================================================

def _is_int_columns(df: pd.DataFrame) -> bool:
    """True when df was read with header=None (integer columns)."""
    return all(isinstance(c, int) for c in df.columns)


def _normalized_tokens(objs: Iterable[object]) -> list[str]:
    return [_norm_col(x) for x in objs]


def _header_overlap_ratio(tokens: Sequence[str], expected: Sequence[str]) -> float:
    """
    Overlap ratio between token sets, using expected as denominator.
    1.0 => all expected tokens appear in tokens (order not required).
    """
    exp = set(_normalized_tokens(expected))
    got = set(tokens)
    if not exp:
        return 0.0
    return len(exp & got) / len(exp)


def _df_columns_look_like_expected_header(df: pd.DataFrame, expected_cols: list[str], threshold: float = 0.6) -> bool:
    return _header_overlap_ratio(_normalized_tokens(df.columns), expected_cols) >= threshold


def _first_row_looks_like_expected_header(df: pd.DataFrame, expected_cols: list[str], threshold: float = 0.6) -> bool:
    if df.empty:
        return False
    return _header_overlap_ratio(_normalized_tokens(df.iloc[0].tolist()), expected_cols) >= threshold


def _shift_columns_into_first_row(df: pd.DataFrame, expected_cols: list[str]) -> pd.DataFrame:
    """
    Fix: headerless file read with header=0 → first data row became df.columns.

    We move df.columns into row 0, then apply expected_cols as headers.
    """
    first_data_row = pd.DataFrame([list(df.columns)], columns=range(len(df.columns)))

    body = df.copy()
    body.columns = range(len(body.columns))

    rebuilt = pd.concat([first_data_row, body], ignore_index=True)
    rebuilt.columns = expected_cols[: rebuilt.shape[1]]
    return rebuilt


def _ensure_expected_raw_layout(df: pd.DataFrame, layout: Layout) -> pd.DataFrame:
    """
    Ensure df has the raw layout header for the chosen layout.
    Handles:
      A) header=None integer columns
      B) df.columns already correct
      C) first row is the header
      D) df.columns are actually first data row
      E) best-effort positional assignment
    """
    expected = layout.columns

    if _is_int_columns(df):  # A
        out = df.copy()
        out.columns = expected[: out.shape[1]]
        return out

    if _df_columns_look_like_expected_header(df, expected):  # B
        out = df.copy()
        if out.shape[1] == len(expected):
            out.columns = expected
        return out

    if _first_row_looks_like_expected_header(df, expected):  # C
        out = df.copy()
        out.columns = expected[: out.shape[1]]
        return out.iloc[1:].reset_index(drop=True)

    if len(df.columns) == len(expected):  # D
        return _shift_columns_into_first_row(df, expected)

    # E
    out = df.copy()
    out.columns = expected[: out.shape[1]]
    return out


# =========================================================
# Finalization for concat across years
# =========================================================

def _finalize_for_cross_year_concat(out: pd.DataFrame, schema: CanonicalSchema) -> pd.DataFrame:
    """
    Make output safe to concatenate across years:
      - force a stable set/order of columns
      - coerce numeric vote fields
      - coerce strings
      - add election_year
    """
    df = out.copy()

    df = df.reindex(columns=schema.join_cols)

    for col in schema.numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")


    for col in schema.string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip().replace({"": pd.NA})

    if "election_date" in df.columns:
        df["election_year"] = (
            df["election_date"]
            .apply(lambda d: d.year if pd.notna(d) else pd.NA)
            .astype("Int64")
        )
    else:
        df["election_year"] = pd.NA

    if "contest_type" in df.columns:
        mapping = {"S": "State", "C": "County"}
        df["contest_type"] = df["contest_type"].map(mapping).fillna(df["contest_type"])

    if "state" not in df.columns:
        df.insert(0, "state", "NC")

    df[["jurisdiction", "office", "district"]] = (
        df["contest_name"]
        .apply(extract_jurisdiction_office_and_district)
        .apply(pd.Series)
    )

    cols = df.columns.tolist()

    i = cols.index("contest_name") + 1
    new_order = (
        cols[:i]
        + ["jurisdiction", "office"]
        + [c for c in cols[i:] if c not in {"jurisdiction", "office"}]
    )

    df = df[new_order]


    df["choice_party"] = df["choice_party"].fillna(
        df["contest_name"].str.extract(r"\(([^)]+)\)$", expand=False)
    )

    return df


# =========================================================
# Public API
# =========================================================
def get_config() -> NcResultsConfig:
    _DEFAULT_CONFIG_PATH = Path(__file__).with_name("nc_results_pct_config.json")
    _CONFIG: NcResultsConfig = load_nc_results_config(_DEFAULT_CONFIG_PATH)

    return _CONFIG


def normalize_nc_results_cols(
    df: pd.DataFrame,
    fallback_election_date: Optional[date] = None,
) -> pd.DataFrame:

    """
    Normalize NC results file columns across eras, then finalize into a stable schema
    suitable for concatenating across years.

    If `config` is None, you should pass one in (loaded from JSON) in the pipeline.
    """
    cfg = get_config()

   # Work on a shallow copy to avoid mutating caller state
    work = df.copy()

    # Select expected raw layout from fallback election date (if available)
    layout = _layout_for_fallback(cfg.layouts, fallback_election_date)
    if layout:
        work = _ensure_expected_raw_layout(work, layout)

    # Build rename mapping from normalized raw column names → canonical names
    rename = {
        col: cfg.col_map[norm]
        for col in work.columns
        if (norm := _norm_col(col)) in cfg.col_map
    }

    # Apply canonical renaming
    out = work.rename(columns=rename)


    # election_date parsing + fallback fill (fill ANY NA, not just all-NA)
    if "election_date" in out.columns:
        out["election_date"] = out["election_date"].apply(_parse_nc_date)

    fb = _as_date(fallback_election_date)
    if fb is not None:
        if "election_date" not in out.columns:
            out["election_date"] = fb
        else:
            out["election_date"] = out["election_date"].fillna(fb)

    # Add election_type BEFORE finalizing (finalizer reindexes columns and can drop it)
    out = add_election_type(out, cfg.election_type_rules)

    # force stable schema for cross-year concatenation
    out = _finalize_for_cross_year_concat(out, cfg.schema)

    return out

