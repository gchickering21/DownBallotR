"""
Data models for the Indiana election results scraper.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class InElectionInfo:
    """Metadata for one Indiana General Election archive."""

    year: int
    archive_slug: str        # e.g. "2020General"
    archive_base_url: str    # e.g. "https://enr.indianavoters.in.gov/archive/2020General"
    election_date: str       # "MM/DD/YYYY" from settings.json
    certified: bool          # True when Certified == "T"
