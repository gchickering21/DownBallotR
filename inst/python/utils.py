"""
Backwards-compatibility shim — import from the focused sub-modules instead.

    text_utils  — clean_text, clean_node, parse_int, parse_percentage,
                  strip_trailing_parens, extract_party_from_parens,
                  is_incumbent, ensure_percent_suffix
    df_utils    — concat_or_empty, rows_to_dataframe
    date_utils  — year_to_date_range, validate_year_range
    http_utils  — fetch_with_retry
"""

from text_utils import (
    clean_text,
    clean_node,
    parse_int,
    parse_percentage,
    strip_trailing_parens,
    extract_party_from_parens,
    is_incumbent,
    ensure_percent_suffix,
)
from df_utils import concat_or_empty, rows_to_dataframe
from date_utils import year_to_date_range, validate_year_range
from http_utils import fetch_with_retry

__all__ = [
    "clean_text",
    "clean_node",
    "parse_int",
    "parse_percentage",
    "strip_trailing_parens",
    "extract_party_from_parens",
    "is_incumbent",
    "ensure_percent_suffix",
    "concat_or_empty",
    "rows_to_dataframe",
    "year_to_date_range",
    "validate_year_range",
    "fetch_with_retry",
]
