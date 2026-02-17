from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class ElectionSearchRow:
    state: str
    election_id: int
    year: int
    office: str
    district: str
    stage: str
    candidate_id: int
    candidate: str
    party: str
    total_vote_count: int
    vote_percentage: str
    contest_outcome: str

    def detail_url(self, base_url: str, path_pattern: str) -> str:
        return f"{base_url}{path_pattern.format(election_id=self.election_id)}"


@dataclass(frozen=True)
class CountyVotes:
    state: str
    election_id: int
    county_or_city: str
    candidate_id: int
    candidate_name: str
    votes: int
