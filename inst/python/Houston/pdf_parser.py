"""pdfplumber-based extraction of vote data from Harris County election PDFs.

Two PDF types
--------------
Cumulative PDF (~90 pages)
    County-wide vote totals per candidate per contest.  Text extraction is used
    because the tables contain merged cells and inconsistent column alignment.

    Actual page structure (confirmed from live PDFs):
      - Election header: "Cumulative Results Report Harris County, Texas Official Results"
      - Per contest:
          {Contest Name}                         ← Title Case, e.g. "President / Vice President"
          Choice Party Ballot by Mail Early Voting Election Day EV Provisional ED Provisional Total
          {Candidate} {PARTY} BM BM% EV EV% ED ED% EVP EVP% EDP EDP% TOTAL TOTAL%
          Cast Votes: ...    ← skipped
          Undervotes: ...    ← skipped
          Overvotes:  ...    ← skipped
      - The "Choice Party..." column header is the reliable contest separator.
      - Party codes follow the candidate name inline (REP, DEM, LIB, GRE, etc.)
        or are absent (no party); write-ins are indicated by "(W)".

Canvass PDF (~6,000 pages, ~46 MB)
    Precinct-level vote totals.  Candidate column headers are rotated/reversed
    in the PDF rendering (pdfplumber reads them bottom-to-top).  We skip header
    parsing and instead use the cumulative PDF's candidate list to map columns.

    Each contest spans multiple pages; continuation pages have no contest header.
    Data rows: PRECINCT#  cand1  cand2 … candN  CAST  UNDER  OVER  WRITEIN
               BM  EV  ED  EVP  EDP  TOTAL  REG  PCT

Public functions
----------------
    parse_cumulative(pdf_bytes) → list[CumulativeContest]
    parse_canvass(pdf_bytes, contest_map) → list[CanvassContest]
    build_contest_map(cumulative_contests) → dict[str, CumulativeContest]
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from difflib import get_close_matches
from typing import Optional

import pdfplumber


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class CumulativeCandidate:
    """One candidate row from the cumulative PDF."""
    candidate: str
    party:     str
    votes_bm:  Optional[int]
    votes_ev:  Optional[int]
    votes_ed:  Optional[int]
    votes_evp: Optional[int]
    votes_edp: Optional[int]
    votes:     Optional[int]   # TOTAL column


@dataclass
class CumulativeContest:
    """One contest from the cumulative PDF (county-wide totals)."""
    office:     str
    district:   str
    candidates: list[CumulativeCandidate] = field(default_factory=list)

    @property
    def key(self) -> str:
        return _norm_key(self.office + " " + self.district)

    @property
    def n_candidates(self) -> int:
        return len(self.candidates)


@dataclass
class CanvassPrecinctRow:
    """One precinct data row from the canvass PDF."""
    precinct:        str
    candidate_votes: list[Optional[int]]  # ordered same as CumulativeContest.candidates


@dataclass
class CanvassContest:
    """One contest from the canvass PDF (precinct-level votes)."""
    office:        str
    district:      str
    candidates:    list[str]   # names in column order (from cumulative)
    parties:       list[str]   # parties in column order (from cumulative)
    precinct_rows: list[CanvassPrecinctRow] = field(default_factory=list)

    @property
    def key(self) -> str:
        return _norm_key(self.office + " " + self.district)


# ── Shared utilities ───────────────────────────────────────────────────────────

def _norm_key(text: str) -> str:
    """Strip everything except lowercase alphanumerics for fuzzy matching."""
    return re.sub(r'[^a-z0-9]', '', text.lower())


def _parse_int(val: "str | None") -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val.replace(",", ""))
    except (ValueError, TypeError):
        return None


# ── Cumulative PDF parsing ─────────────────────────────────────────────────────

# The column header line that appears after every contest name.
# "Choice Party Ballot by Mail Early Voting Election Day EV Provisional..."
_COL_HDR_RE = re.compile(
    r'^\s*Choice\s+Party\s+Ballot\s+by\s+Mail',
    re.IGNORECASE,
)

# Summary rows to skip (appear after candidate rows for each contest).
_SUMMARY_ROW_RE = re.compile(
    r'^\s*(?:Cast\s+Votes?|Under\s*[Vv]otes?|Over\s*[Vv]otes?|Times\s+Cast|'
    r'Total\s+Votes?|Registered\s+Voters?|Precincts?\s+Reporting|'
    r'Run\s+(?:Time|Date)|Page\s+\d|'
    r'Cumulative\s+Results|Harris\s+County|Official\s+Results)',
    re.IGNORECASE,
)

# Election-level header lines that should not be treated as contest names.
_ELECTION_HDR_RE = re.compile(
    r'^\s*(?:Cumulative\s+Results|Canvass\s+Results|Harris\s+County|Official\s+Results|'
    r'Registered\s+Voters|Precincts?\s+Reporting|'
    r'Run\s+(?:Time|Date)|Page\s+\d|\d{1,2}/\d{1,2}/\d{2,4})',
    re.IGNORECASE,
)

# Trailing voting-instruction suffixes to strip from contest names.
_CONTEST_SUFFIX_RE = re.compile(
    r'\s*-\s*(?:Vote\s+for\s+\w+(?:\s+or\s+\w+)?'
    r'|Unexpired\s+Term.*'
    r'|Full\s+Term.*'
    r'|Short\s+Term.*'
    r'|Remainder\s+of\s+Term.*)\s*$',
    re.IGNORECASE,
)

# Trailing party-name suffix in primary PDFs: "- Democratic Party", "- Republican Party", etc.
_PARTY_NAME_SUFFIX_RE = re.compile(
    r'\s*-\s*(?:Democratic|Republican|Libertarian|Green|Independent|'
    r'Non-Partisan|Nonpartisan|Working\s+Families)\s+Party\s*$',
    re.IGNORECASE,
)

# Leading party-label prefix in primary PDFs: "Dem - ", "Rep - ", etc.
_PARTY_LABEL_PREFIX_RE = re.compile(
    r'^\s*(?:Dem|Rep|Lib|Gre|Ind|Lbt)\s*-\s*',
    re.IGNORECASE,
)

# District/place suffix on office names.
_DISTRICT_SUFFIX_RE = re.compile(
    r',?\s*(District|Precinct|Place|Seat|Position|Ward|Zone)\s+(\S+)\s*$',
    re.IGNORECASE,
)

# Candidate row: {NAME} [{PARTY}] BM BM% EV EV% ED ED% EVP EVP% EDP EDP% TOTAL TOTAL%
# The lazy (.+?) captures everything up to the first number group (which is BM votes).
_CAND_6_RE = re.compile(
    r'^(.+?)\s+'
    r'([\d,]+)\s+\d+\.\d+%\s+'    # BM votes  BM%
    r'([\d,]+)\s+\d+\.\d+%\s+'    # EV
    r'([\d,]+)\s+\d+\.\d+%\s+'    # ED
    r'([\d,]+)\s+\d+\.\d+%\s+'    # EVP
    r'([\d,]+)\s+\d+\.\d+%\s+'    # EDP
    r'([\d,]+)\s+\d+\.\d+%\s*$'   # TOTAL
)

# Older elections: 5 pairs (no EVP/EDP distinction)
_CAND_5_RE = re.compile(
    r'^(.+?)\s+'
    r'([\d,]+)\s+\d+\.\d+%\s+'
    r'([\d,]+)\s+\d+\.\d+%\s+'
    r'([\d,]+)\s+\d+\.\d+%\s+'
    r'([\d,]+)\s+\d+\.\d+%\s+'
    r'([\d,]+)\s+\d+\.\d+%\s*$'
)

# Known Harris County / Texas party abbreviations — only these are stripped as party codes.
_KNOWN_PARTY_CODES: frozenset[str] = frozenset({
    'REP', 'DEM', 'LIB', 'GRE', 'IND', 'LBT', 'WFP', 'NOP', 'CON', 'SOC',
    'AIP', 'NPA', 'GRT', 'NOR', 'CIT', 'TEA',
})

# Lines that look like they carry vote data (contain at least one "N.NN%")
_HAS_PCT_RE = re.compile(r'\d+\.\d+%')


def _parse_office_district(line: str) -> "tuple[str, str]":
    """Clean and split a contest name into (office, district).

    Strips:
    - Trailing voting instructions ("- Vote for none or one", "- Unexpired Term")
    - Trailing party name suffixes ("- Democratic Party")
    - Leading party label prefixes ("Dem - ", "Rep - ")
    Then extracts a district suffix ("District 18", "Place 1", etc.).
    """
    line = _CONTEST_SUFFIX_RE.sub('', line).strip()
    line = _PARTY_NAME_SUFFIX_RE.sub('', line).strip()
    line = _PARTY_LABEL_PREFIX_RE.sub('', line).strip()
    dm = _DISTRICT_SUFFIX_RE.search(line)
    if dm:
        district = f"{dm.group(1).title()} {dm.group(2)}"
        office   = line[:dm.start()].strip().rstrip(",").strip()
        return office, district
    return line.strip(), ""


def _split_name_party(raw: str) -> "tuple[str, str]":
    """Split 'DONALD J. TRUMP / JD VANCE REP' → ('DONALD J. TRUMP / JD VANCE', 'REP').

    Uses a whitelist of known party codes to avoid misidentifying candidate last
    names (e.g., CRUZ, VANCE, ELLIS) as party abbreviations.
    Returns (name, "") when no known party code is found.
    """
    raw = raw.strip()
    if raw.endswith("(W)"):
        return raw[:-3].strip(), "W"
    parts = raw.rsplit(None, 1)
    if len(parts) == 2 and parts[1].upper() in _KNOWN_PARTY_CODES:
        return parts[0].strip(), parts[1].upper()
    return raw, ""


def _is_contest_name_candidate(line: str) -> bool:
    """Return True if a line contains vote data (should NOT be used as contest name)."""
    return bool(_HAS_PCT_RE.search(line))


def parse_cumulative(pdf_bytes: bytes) -> "list[CumulativeContest]":
    """Parse a cumulative results PDF and return all contests with candidate totals.

    Strategy: the "Choice Party Ballot by Mail..." column header line reliably
    signals the start of a new contest.  The last non-blank, non-header line
    before it is the contest name.
    """
    contests: list[CumulativeContest] = []
    current:  "CumulativeContest | None" = None
    prev_line = ""   # last non-blank, non-summary, non-header line

    def _start_new_contest(name_line: str) -> "CumulativeContest | None":
        if not name_line or _is_contest_name_candidate(name_line):
            return None
        office, district = _parse_office_district(name_line)
        if not office:
            return None
        return CumulativeContest(office=office, district=district)

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for raw_line in text.splitlines():
                    line = raw_line.strip()
                    if not line:
                        continue

                    # ── Column header → start new contest ────────────────────
                    if _COL_HDR_RE.match(line):
                        if current is not None and current.candidates:
                            contests.append(current)
                        current = _start_new_contest(prev_line)
                        prev_line = ""
                        continue

                    # ── Skip summary and election-level header lines ──────────
                    if _SUMMARY_ROW_RE.match(line) or _ELECTION_HDR_RE.match(line):
                        continue

                    # ── Try 6-pair candidate row ──────────────────────────────
                    m6 = _CAND_6_RE.match(line)
                    if m6 and current is not None:
                        name_raw   = m6.group(1)
                        candidate, party = _split_name_party(name_raw)
                        current.candidates.append(CumulativeCandidate(
                            candidate=candidate, party=party,
                            votes_bm=_parse_int(m6.group(2)),
                            votes_ev=_parse_int(m6.group(3)),
                            votes_ed=_parse_int(m6.group(4)),
                            votes_evp=_parse_int(m6.group(5)),
                            votes_edp=_parse_int(m6.group(6)),
                            votes=_parse_int(m6.group(7)),
                        ))
                        prev_line = ""
                        continue

                    # ── Try 5-pair candidate row ──────────────────────────────
                    m5 = _CAND_5_RE.match(line)
                    if m5 and current is not None:
                        name_raw   = m5.group(1)
                        candidate, party = _split_name_party(name_raw)
                        current.candidates.append(CumulativeCandidate(
                            candidate=candidate, party=party,
                            votes_bm=_parse_int(m5.group(2)),
                            votes_ev=_parse_int(m5.group(3)),
                            votes_ed=_parse_int(m5.group(4)),
                            votes_evp=None,
                            votes_edp=_parse_int(m5.group(5)),
                            votes=_parse_int(m5.group(6)),
                        ))
                        prev_line = ""
                        continue

                    # ── Otherwise: potential contest name line ────────────────
                    prev_line = line

    except Exception as exc:
        print(f"[Houston pdf_parser] WARNING: cumulative PDF error: {exc}")

    if current is not None and current.candidates:
        contests.append(current)

    return contests


# ── Canvass PDF parsing ────────────────────────────────────────────────────────

# Precinct data row: starts with 3–5 digits followed by whitespace
_PRECINCT_ROW_RE = re.compile(r'^(\d{3,5})\s+(.+)$')

# Rows to skip regardless
_CANVASS_SKIP_RE = re.compile(
    r'^\s*(?:total|totals?|harris\s+county|official|report|all\s+precincts?)\b',
    re.IGNORECASE,
)

# The canvass column header is the same pattern as the cumulative.
_CANVASS_COL_HDR_RE = _COL_HDR_RE

# Lines that look like office/contest names in the canvass PDF.
# We accept any non-blank line that:
# - is not a data row (doesn't start with digits)
# - is not the column header
# - is not a known skip pattern
# Contest names in the canvass are Title Case (same as cumulative).
def _is_canvass_contest_line(line: str) -> bool:
    if not line or _CANVASS_SKIP_RE.match(line):
        return False
    if _CANVASS_COL_HDR_RE.match(line):
        return False
    if _PRECINCT_ROW_RE.match(line):
        return False
    if _HAS_PCT_RE.search(line):
        return False
    if _SUMMARY_ROW_RE.match(line) or _ELECTION_HDR_RE.match(line):
        return False
    # Must start with a letter (not a digit or punctuation)
    return line[0].isalpha()


def build_contest_map(
    cumulative_contests: "list[CumulativeContest]",
) -> "dict[str, CumulativeContest]":
    """Build a normalized-key → CumulativeContest lookup from the cumulative list."""
    return {c.key: c for c in cumulative_contests}


def _match_to_cumulative(
    office: str,
    district: str,
    contest_map: "dict[str, CumulativeContest]",
    cumulative_list: "list[CumulativeContest]",
    seen_indices: set,
) -> "tuple[CumulativeContest | None, int | None]":
    """Find the best matching cumulative contest for the given office/district.

    Strategy (in order): exact → fuzzy → positional.
    """
    key = _norm_key(office + " " + district)

    # 1. Exact
    if key in contest_map:
        c = contest_map[key]
        try:
            idx = cumulative_list.index(c)
        except ValueError:
            idx = None
        return c, idx

    # 2. Fuzzy
    matches = get_close_matches(key, contest_map.keys(), n=1, cutoff=0.70)
    if matches:
        c = contest_map[matches[0]]
        try:
            idx = cumulative_list.index(c)
        except ValueError:
            idx = None
        print(f"[Houston pdf_parser] fuzzy match: {office!r} → {c.office!r}")
        return c, idx

    # 3. Positional fallback: next unseen contest
    for idx, c in enumerate(cumulative_list):
        if idx not in seen_indices:
            print(
                f"[Houston pdf_parser] positional fallback: "
                f"{office!r} → contest #{idx} ({c.office!r})"
            )
            return c, idx

    return None, None


def parse_canvass(
    pdf_bytes: bytes,
    contest_map: "dict[str, CumulativeContest]",
    cumulative_list: "list[CumulativeContest] | None" = None,
) -> "list[CanvassContest]":
    """Parse a canvass PDF and return precinct-level vote data.

    Canvass PDF page structure (confirmed from live PDFs):
      - Page headers: Canvass Results Report, Harris County, Registered Voters, etc.
      - Contest name: e.g. "City of Baytown, Mayor - Vote for none or one"
      - "Precinct"  ← standalone keyword that triggers contest setup
      - Reversed/rotated column headers (candidate names, vote-method labels)
      - Data rows: NNNN v1 v2 ... CastVotes Undervotes Overvotes BM EV ED EVP EDP Total Reg Pct%
      - Continuation pages repeat the contest name + "Precinct" + headers before more data rows.

    Parameters
    ----------
    pdf_bytes : bytes
        Raw bytes of the canvass PDF.
    contest_map : dict[str, CumulativeContest]
        Built by build_contest_map() from the cumulative PDF's contests.
    cumulative_list : list[CumulativeContest] | None
        Ordered list for positional fallback matching.
    """
    if cumulative_list is None:
        cumulative_list = list(contest_map.values())

    results:           list[CanvassContest] = []
    current_canvass:   "CanvassContest | None" = None
    current_cumul:     "CumulativeContest | None" = None
    seen_indices:      set[int] = set()
    pending_name_line: str = ""   # most recent potential contest name
    in_header_block:   bool = False  # True after "Precinct" label, until first data row

    def _try_set_contest(name_line: str) -> None:
        nonlocal current_canvass, current_cumul
        if not name_line:
            return
        office, district = _parse_office_district(name_line)
        new_key = _norm_key(office + " " + district)
        # Continuation page for the same contest — don't restart it
        if current_canvass is not None and new_key == current_canvass.key:
            return
        cumul, idx = _match_to_cumulative(
            office, district, contest_map, cumulative_list, seen_indices
        )
        if cumul is None:
            return
        if current_canvass is not None and current_canvass.precinct_rows:
            results.append(current_canvass)
        if idx is not None:
            seen_indices.add(idx)
        current_cumul   = cumul
        current_canvass = CanvassContest(
            office=office,
            district=district,
            candidates=[c.candidate for c in cumul.candidates],
            parties=[c.party      for c in cumul.candidates],
        )

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for raw_line in text.splitlines():
                    line = raw_line.strip()
                    if not line:
                        continue

                    if _CANVASS_SKIP_RE.match(line):
                        continue

                    if _SUMMARY_ROW_RE.match(line) or _ELECTION_HDR_RE.match(line):
                        continue

                    # "Precinct" label: everything before this was the contest name;
                    # everything after (until first data row) is reversed header text.
                    if line == "Precinct":
                        _try_set_contest(pending_name_line)
                        pending_name_line = ""
                        in_header_block = True
                        continue

                    # ── Precinct data row ─────────────────────────────────────
                    pm = _PRECINCT_ROW_RE.match(line)
                    if pm:
                        in_header_block = False
                        if current_cumul is None:
                            continue
                        precinct_id = pm.group(1)
                        tokens = pm.group(2).split()
                        # First token must be a digit (not "of", "=", etc. from header rows)
                        first = tokens[0].replace(",", "") if tokens else ""
                        if not first.isdigit():
                            continue
                        n = current_cumul.n_candidates
                        if n == 0 or len(tokens) < n:
                            continue
                        votes = [_parse_int(t) for t in tokens[:n]]
                        current_canvass.precinct_rows.append(
                            CanvassPrecinctRow(
                                precinct=precinct_id,
                                candidate_votes=votes,
                            )
                        )
                        continue

                    # Skip reversed column header block (candidate names, vote-method labels)
                    if in_header_block:
                        continue

                    # ── Potential contest name line ────────────────────────────
                    if _is_canvass_contest_line(line):
                        pending_name_line = line

    except Exception as exc:
        print(f"[Houston pdf_parser] WARNING: canvass PDF error: {exc}")

    if current_canvass is not None and current_canvass.precinct_rows:
        results.append(current_canvass)

    return results
