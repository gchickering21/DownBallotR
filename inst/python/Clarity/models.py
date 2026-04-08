"""Shared data model for Angular/PrimeNG SOS election results scrapers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ClarityElectionInfo:
    """Metadata for a single election discovered from a SOS landing page.

    Used by all state scrapers built on the shared Clarity backend
    (currently Georgia and Utah).

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
