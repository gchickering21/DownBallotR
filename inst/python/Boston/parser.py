"""Convert raw Boston PDF table data into standardised DataFrames.

Two output levels
-----------------
city  — one row per candidate per contest, citywide totals (TOTAL column).
ward  — one row per candidate per ward per contest.

Both levels share the same election metadata columns (name, year, date, type)
and candidate columns (office, district, candidate, party).  The ward level
adds a ``ward`` column and uses ``ward_winner`` instead of ``winner``.

Public functions
----------------
to_city_df(pdf_data, election, link) → pd.DataFrame
to_ward_df(pdf_data, election, link) → pd.DataFrame
"""

from __future__ import annotations

import pandas as pd

from office_level_utils import classify_office_level

from .models import BostonElectionInfo, BostonResultLink
from .pdf_parser import PdfTableData, PdfPrecinctData

# Contest identity columns for winner computation at each level.
_CITY_CONTEST_COLS = [
    "election_name", "election_year", "election_date", "office", "district",
]
_WARD_CONTEST_COLS = [
    "election_name", "election_year", "election_date", "office", "district", "ward",
]
_PRECINCT_CONTEST_COLS = [
    "election_name", "election_year", "election_date", "office", "district", "ward", "precinct",
]


def _add_winner(
    df: pd.DataFrame,
    contest_cols: list[str],
    winner_col: str = "winner",
) -> pd.DataFrame:
    """Mark the candidate with the highest votes in each contest as winner.

    Ties result in multiple winners (all tied candidates get True).
    Rows with null votes are never marked as winner.
    """
    if df.empty or "votes" not in df.columns:
        return df
    votes = pd.to_numeric(df["votes"], errors="coerce")
    present = [c for c in contest_cols if c in df.columns]
    if present:
        max_votes = votes.groupby(df.groupby(present, dropna=False).ngroup()).transform("max")
    else:
        max_votes = votes.max()
    df[winner_col] = (votes == max_votes) & votes.notna()
    return df


def _base_fields(election: BostonElectionInfo, link: BostonResultLink) -> tuple:
    """Return shared metadata as a tuple for fast per-row reuse."""
    return (
        election.election_type,  # election_name
        election.year,
        election.election_date,
        election.election_type,
        classify_office_level(link.office),
        link.office,
        link.district,
        link.party,
    )


def to_city_df(
    pdf_data: PdfTableData,
    election: BostonElectionInfo,
    link: BostonResultLink,
) -> pd.DataFrame:
    """Build a city-level (citywide totals) DataFrame from one PDF's table data.

    Uses the TOTAL column from the ward-summary table on page 1.

    Parameters
    ----------
    pdf_data : PdfTableData
        Extracted table data from pdf_parser.parse_pdf().
    election : BostonElectionInfo
        Election metadata (name, year, date, type).
    link : BostonResultLink
        Result link metadata (office, district, party).

    Returns
    -------
    pd.DataFrame
        Columns: election_name, election_year, election_date, election_type,
        office_level, office, district, candidate, party, votes.
        vote_pct and winner are added by the pipeline after aggregation.
    """
    if pdf_data.is_empty:
        return pd.DataFrame()

    e_name, e_year, e_date, e_type, o_level, office, district, party = _base_fields(election, link)
    n = len(pdf_data.candidate_rows)

    df = pd.DataFrame({
        "election_name": [e_name]   * n,
        "election_year": [e_year]   * n,
        "election_date": [e_date]   * n,
        "election_type": [e_type]   * n,
        "office_level":  [o_level]  * n,
        "office":        [office]   * n,
        "district":      [district] * n,
        "party":         [party]    * n,
        "candidate":     [cr["candidate"] for cr in pdf_data.candidate_rows],
        "votes":         [cr["total"]     for cr in pdf_data.candidate_rows],
    })
    return _add_winner(df, _CITY_CONTEST_COLS, winner_col="winner")


def to_ward_df(
    pdf_data: PdfTableData,
    election: BostonElectionInfo,
    link: BostonResultLink,
) -> pd.DataFrame:
    """Build a ward-level DataFrame from one PDF's table data.

    Produces one row per (candidate, ward) pair using the individual ward
    columns from the ward-summary table on page 1.

    Parameters
    ----------
    pdf_data : PdfTableData
        Extracted table data from pdf_parser.parse_pdf().
    election : BostonElectionInfo
        Election metadata (name, year, date, type).
    link : BostonResultLink
        Result link metadata (office, district, party).

    Returns
    -------
    pd.DataFrame
        Columns: election_name, election_year, election_date, election_type,
        office_level, office, district, ward, candidate, party, votes.
        vote_pct and ward_winner are added by the pipeline after aggregation.
    """
    if pdf_data.is_empty or not pdf_data.ward_headers:
        return pd.DataFrame()

    e_name, e_year, e_date, e_type, o_level, office, district, party = _base_fields(election, link)

    col_names:     list = []
    col_years:     list = []
    col_dates:     list = []
    col_types:     list = []
    col_levels:    list = []
    col_offices:   list = []
    col_districts: list = []
    col_parties:   list = []
    col_wards:     list = []
    col_cands:     list = []
    col_votes:     list = []

    n_cands = len(pdf_data.candidate_rows)
    cand_names = [cr["candidate"] for cr in pdf_data.candidate_rows]

    for ward in pdf_data.ward_headers:
        col_names.extend([e_name]    * n_cands)
        col_years.extend([e_year]    * n_cands)
        col_dates.extend([e_date]    * n_cands)
        col_types.extend([e_type]    * n_cands)
        col_levels.extend([o_level]  * n_cands)
        col_offices.extend([office]  * n_cands)
        col_districts.extend([district] * n_cands)
        col_parties.extend([party]   * n_cands)
        col_wards.extend([ward]      * n_cands)
        col_cands.extend(cand_names)
        col_votes.extend([cr["ward_votes"].get(ward) for cr in pdf_data.candidate_rows])

    if not col_names:
        return pd.DataFrame()

    df = pd.DataFrame({
        "election_name": col_names,
        "election_year": col_years,
        "election_date": col_dates,
        "election_type": col_types,
        "office_level":  col_levels,
        "office":        col_offices,
        "district":      col_districts,
        "ward":          col_wards,
        "candidate":     col_cands,
        "party":         col_parties,
        "votes":         col_votes,
    })
    return _add_winner(df, _WARD_CONTEST_COLS, winner_col="ward_winner")


def to_precinct_df(
    precinct_data_list: "list[PdfPrecinctData]",
    election: BostonElectionInfo,
    link: BostonResultLink,
) -> pd.DataFrame:
    """Build a precinct-level DataFrame from one PDF's precinct-breakdown pages.

    Each page 2+ of a Boston PDF covers one ward, with precincts as columns.
    Cells where pdfplumber returns None indicate the precinct is not part of
    this contest's district — those rows are dropped.

    Parameters
    ----------
    precinct_data_list : list[PdfPrecinctData]
        Parsed precinct-breakdown data, one item per ward page (pages 2+).
    election : BostonElectionInfo
        Election metadata (name, year, date, type).
    link : BostonResultLink
        Result link metadata (office, district, party).

    Returns
    -------
    pd.DataFrame
        Columns: election_name, election_year, election_date, election_type,
        office_level, office, district, ward, precinct, candidate, party, votes.
        vote_pct and precinct_winner are added by the pipeline after aggregation.
    """
    if not precinct_data_list:
        return pd.DataFrame()

    e_name, e_year, e_date, e_type, o_level, office, district, party = _base_fields(election, link)

    col_names:     list = []
    col_years:     list = []
    col_dates:     list = []
    col_types:     list = []
    col_levels:    list = []
    col_offices:   list = []
    col_districts: list = []
    col_parties:   list = []
    col_wards:     list = []
    col_precincts: list = []
    col_cands:     list = []
    col_votes:     list = []

    for pd_data in precinct_data_list:
        ward      = pd_data.ward_num
        cand_rows = pd_data.candidate_rows

        for precinct in pd_data.precinct_headers:
            for cr in cand_rows:
                votes = cr["precinct_votes"].get(precinct)
                if votes is None:
                    continue  # precinct not part of this contest's district
                col_names.append(e_name)
                col_years.append(e_year)
                col_dates.append(e_date)
                col_types.append(e_type)
                col_levels.append(o_level)
                col_offices.append(office)
                col_districts.append(district)
                col_parties.append(party)
                col_wards.append(ward)
                col_precincts.append(precinct)
                col_cands.append(cr["candidate"])
                col_votes.append(votes)

    if not col_names:
        return pd.DataFrame()

    df = pd.DataFrame({
        "election_name": col_names,
        "election_year": col_years,
        "election_date": col_dates,
        "election_type": col_types,
        "office_level":  col_levels,
        "office":        col_offices,
        "district":      col_districts,
        "ward":          col_wards,
        "precinct":      col_precincts,
        "candidate":     col_cands,
        "party":         col_parties,
        "votes":         col_votes,
    })
    return _add_winner(df, _PRECINCT_CONTEST_COLS, winner_col="precinct_winner")
