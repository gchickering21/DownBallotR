from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class VaElectionSearchRow:
    election_id: int
    year: int
    office: str
    district: str
    stage: str
    candidates_summary: Optional[str] = None
    candidates_url: Optional[str] = None

    @property
    def detail_url(self) -> str:
        return f"https://historical.elections.virginia.gov/elections/view/{self.election_id}/"

