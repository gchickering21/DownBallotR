"""pdfplumber-based extraction of vote tables from Boston City elections PDFs.

PDF structure (confirmed from live PDFs)
-----------------------------------------
Page 1 — Ward summary:
  Title block (centred, bold):
    CITY OF BOSTON - OFFICIAL
    {ELECTION TYPE} - {DATE}
    {OFFICE LINE}                    ← e.g. "CITY COUNCILLOR - DISTRICT 9"

  Section: VOTES CAST BY WARD
    Table columns: CANDIDATES | <ward1> | <ward2> | … | TOTAL
    Candidate rows (names in ALL CAPS)
    Summary rows: VOTES CAST / BLANKS / BALLOTS CAST  ← excluded from output

  Section: PERCENTAGE OF VOTES CAST BY PRECINCT
    Same column layout, values are percentages  ← ignored (we recompute)

Pages 2+ — Per-ward precinct breakdowns:
  Header text containing "PRECINCT <WARD_NUM>" somewhere on the page.
  Table columns: [blank or CANDIDATES] | <precinct1> | <precinct2> | … | TOTAL
  One page per ward that appears in the contest district.
  Precincts not in the district have None (empty cell) values — skipped in output.

This module provides one public function:

    parse_pdf(pdf_bytes) → tuple[PdfTableData | None, list[PdfPrecinctData]]

PdfTableData holds the ward column headers and the candidate vote rows from page 1.
PdfPrecinctData holds the ward number, precinct column headers, and candidate rows
for a single ward's precinct breakdown (one per page 2+).
The caller (parser.py) is responsible for mapping these to DataFrames.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field

import pdfplumber

# Rows in the table that are summary totals, not candidate names.
_SUMMARY_ROWS: frozenset[str] = frozenset({
    "VOTES CAST", "BLANKS", "BALLOTS CAST",
})

# Percentage marker — used to distinguish the vote table from the pct table.
_PCT_RE = re.compile(r"\d+\.\d+%")

# Matches "VOTES CAST BY PRECINCT 14", "PRECINCT 14", etc. → group(1) = "14"
# Intentionally broad: the exact phrasing varies across PDF versions.
_PRECINCT_WARD_RE = re.compile(r'PRECINCT[^\d\n]*(\d+)', re.IGNORECASE)


@dataclass
class PdfTableData:
    """Raw table data extracted from the first page of a Boston elections PDF.

    Attributes
    ----------
    ward_headers : list[str]
        Ward numbers found in the column header, e.g. ["21", "22"].
    candidate_rows : list[dict]
        One dict per candidate with keys:
          - "candidate": str
          - "ward_votes": dict[str, int | None]
          - "total": int | None
    """

    ward_headers: list[str] = field(default_factory=list)
    candidate_rows: list[dict] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.candidate_rows


@dataclass
class PdfPrecinctData:
    """Raw table data from a single precinct-breakdown page (pages 2+ of a Boston PDF).

    Attributes
    ----------
    ward_num : str
        Ward number this page covers, e.g. "14".
    precinct_headers : list[str]
        Precinct numbers found in the column header, e.g. ["1", "2", "3"].
    candidate_rows : list[dict]
        One dict per candidate with keys:
          - "candidate": str
          - "precinct_votes": dict[str, int | None]
          - "total": int | None
    """

    ward_num: str
    precinct_headers: list[str] = field(default_factory=list)
    candidate_rows: list[dict] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.candidate_rows


def _parse_int(val: "str | None") -> "int | None":
    if val is None:
        return None
    cleaned = str(val).replace(",", "").strip()
    try:
        return int(float(cleaned))
    except (ValueError, TypeError):
        return None


def _is_percentage_table(table: list[list]) -> bool:
    """Return True if any data cell (first two rows) contains a percentage string."""
    for row in table[1:3]:
        for cell in (row or [])[1:]:
            if cell and _PCT_RE.search(str(cell)):
                return True
    return False


def _find_votes_table(tables: list[list[list]]) -> "list[list] | None":
    """Return the first integer-valued table whose header contains CANDIDATES.

    Used for page 1 (ward summary), which always has 'CANDIDATES' in column 0.
    """
    for table in tables:
        if not table or len(table) < 2:
            continue
        header = table[0] or []
        if not any("CANDIDATES" in str(c).upper() for c in header if c):
            continue
        if _is_percentage_table(table):
            continue
        return table
    return None


def _find_numeric_col_table(tables: list[list[list]]) -> "list[list] | None":
    """Return the first non-percentage table with at least one numeric column header.

    Used as a fallback for precinct pages, where the first column header may be
    blank or differ from 'CANDIDATES'.
    """
    for table in tables:
        if not table or len(table) < 2:
            continue
        header = table[0] or []
        if not any(str(c or "").strip().isdigit() for c in header):
            continue
        if _is_percentage_table(table):
            continue
        return table
    return None


def _parse_int_col_table(table: list[list]) -> "tuple[list[str], list[dict]]":
    """Parse a table with numeric column headers into (col_headers, candidate_rows).

    Works for both ward-summary (page 1) and precinct-breakdown (pages 2+) tables.
    Returns candidate_rows with a generic "col_votes" key; callers rename it.
    """
    header = [str(c).strip() if c else "" for c in (table[0] or [])]

    col_indices: list[tuple[int, str]] = [
        (i, h) for i, h in enumerate(header) if h.isdigit()
    ]
    total_col_idx = next(
        (i for i, h in enumerate(header) if "TOTAL" in h.upper()), -1
    )

    col_headers = [col for _, col in col_indices]
    candidate_rows: list[dict] = []

    for row in table[1:]:
        if not row:
            continue
        candidate = str(row[0]).strip() if row[0] else ""
        if not candidate or candidate.upper() in _SUMMARY_ROWS:
            continue

        col_votes = {
            col: (_parse_int(row[idx]) if idx < len(row) else None)
            for idx, col in col_indices
        }
        total = _parse_int(row[total_col_idx]) if 0 <= total_col_idx < len(row) else None

        candidate_rows.append({
            "candidate": candidate,
            "col_votes": col_votes,
            "total": total,
        })

    return col_headers, candidate_rows


def _parse_ward_table(table: list[list]) -> "tuple[list[str], list[dict]]":
    headers, rows = _parse_int_col_table(table)
    for row in rows:
        row["ward_votes"] = row.pop("col_votes")
    return headers, rows


def _parse_precinct_table(table: list[list]) -> "tuple[list[str], list[dict]]":
    headers, rows = _parse_int_col_table(table)
    for row in rows:
        row["precinct_votes"] = row.pop("col_votes")
    return headers, rows


def _parse_precinct_page(page, page_num: int) -> "PdfPrecinctData | None":
    """Extract ward number and precinct vote table from one precinct-breakdown page.

    Parameters
    ----------
    page : pdfplumber.Page
    page_num : int
        1-based page number (for diagnostic messages).
    """
    text = page.extract_text() or ""

    # Silently skip pages that are plainly percentage/registration summaries with
    # no ward number (e.g. the "PERCENTAGE OF VOTES CAST" overview page that
    # precedes individual ward pct tables in multi-page PDFs).
    m = _PRECINCT_WARD_RE.search(text)
    if not m:
        return None

    ward_num = m.group(1)
    tables = page.extract_tables()
    if not tables:
        return None

    # If every table on this page is a percentage table, it's the pct-companion
    # page that appears after each ward's vote table in citywide-race PDFs.
    # Skip silently — these are expected and not a parsing failure.
    all_pct = all(_is_percentage_table(t) for t in tables if t and len(t) >= 2)
    if all_pct:
        return None

    # Strict search first (needs "CANDIDATES" header); fall back to any table
    # with numeric columns (precinct pages often omit the "CANDIDATES" label).
    votes_table = _find_votes_table(tables) or _find_numeric_col_table(tables)
    if votes_table is None:
        print(f"[Boston pdf_parser] p{page_num}: ward {ward_num} — no vote table identified.")
        return None

    precinct_headers, candidate_rows = _parse_precinct_table(votes_table)
    if not precinct_headers:
        print(f"[Boston pdf_parser] p{page_num}: ward {ward_num} — table has no numeric columns.")
        return None

    return PdfPrecinctData(
        ward_num=ward_num,
        precinct_headers=precinct_headers,
        candidate_rows=candidate_rows,
    )


def parse_pdf(
    pdf_bytes: bytes,
    parse_precinct_pages: bool = True,
) -> "tuple[PdfTableData | None, list[PdfPrecinctData]]":
    """Extract ward-level and precinct-level vote data from a Boston elections PDF.

    Parameters
    ----------
    parse_precinct_pages : bool
        When False, skip pages 2+ entirely.  Use when only city- or ward-level
        data is needed — saves ~90% of per-PDF parse time for large PDFs.

    Returns
    -------
    (ward_data, precinct_data_list)
        ward_data — PdfTableData from page 1, or None if not parseable.
        precinct_data_list — list of PdfPrecinctData from pages 2+, one per ward.
                             Always empty when parse_precinct_pages=False.
    """
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return None, []

            # ── Page 1: ward summary ────────────────────────────────────────────
            tables1 = pdf.pages[0].extract_tables()
            ward_data: "PdfTableData | None" = None

            if not tables1:
                print("[Boston pdf_parser] WARNING: No tables found on page 1.")
            else:
                votes_table = _find_votes_table(tables1)
                if votes_table is None:
                    print("[Boston pdf_parser] WARNING: Could not identify the votes table on page 1.")
                else:
                    ward_headers, candidate_rows = _parse_ward_table(votes_table)
                    if not candidate_rows:
                        print("[Boston pdf_parser] WARNING: Votes table found but no candidate rows parsed.")
                    ward_data = PdfTableData(ward_headers=ward_headers, candidate_rows=candidate_rows)

            # ── Pages 2+: per-ward precinct breakdowns ──────────────────────────
            precinct_data_list: list[PdfPrecinctData] = []
            if parse_precinct_pages:
                for i, page in enumerate(pdf.pages[1:]):
                    pd_data = _parse_precinct_page(page, page_num=i + 2)
                    if pd_data is not None and not pd_data.is_empty:
                        precinct_data_list.append(pd_data)

            return ward_data, precinct_data_list

    except Exception as exc:
        print(f"[Boston pdf_parser] WARNING: Could not open PDF: {exc}")
        return None, []
