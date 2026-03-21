"""Data models for the Georgia SOS election results scraper."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class GaElectionInfo:
    """Metadata for a single Georgia election discovered from the landing page.

    Attributes
    ----------
    name : str
        Human-readable election name (e.g. "November 2024 General Election").
    year : int
        Calendar year of the election.
    slug : str
        URL slug identifying the election (e.g. "November2024General").
    url : str
        Full absolute URL to the election results page.
    """

    name: str
    year: int
    slug: str
    url: str
