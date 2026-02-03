from __future__ import annotations
from datetime import date, datetime
from typing import Iterable, Optional

from .models import NcElectionZip


def _parse_date(d: str | date) -> date:
    if isinstance(d, date):
        return d
    return datetime.strptime(d, "%Y-%m-%d").date()


def select_elections(
    elections: Iterable[NcElectionZip],
    date_: Optional[str | date] = None,
    start_date: Optional[str | date] = None,
    end_date: Optional[str | date] = None,
) -> list[NcElectionZip]:
    elections = list(elections)
    if not elections:
        return []

    if date_ is not None:
        target = _parse_date(date_)
        return [e for e in elections if e.election_date == target]

    if start_date is not None:
        start = _parse_date(start_date)
    else:
        start = min(e.election_date for e in elections)

    if end_date is not None:
        end = _parse_date(end_date)
    else:
        end = max(e.election_date for e in elections)

    if start > end:
        raise ValueError("start_date cannot be after end_date")

    return [
        e for e in elections
        if start <= e.election_date <= end
    ]
