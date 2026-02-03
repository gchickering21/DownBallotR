from __future__ import annotations
from dataclasses import dataclass
from datetime import date
import pandas as pd


@dataclass(frozen=True)
class NcElectionZip:
    election_date: date
    zip_url: str
    label: str


@dataclass(frozen=True)
class NcScrapeResult:
    state: str
    election_date: date
    source_url: str
    member_name: str
    df: pd.DataFrame
