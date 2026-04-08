"""Utah election results discovery — thin wrapper around Clarity.discovery."""

from Clarity.discovery import parse_election_links as _parse_election_links
from Clarity.Utah.client import UT_BASE_URL
from Clarity.models import ClarityElectionInfo as UtElectionInfo

__all__ = ["parse_election_links", "UtElectionInfo"]


def parse_election_links(html_str: str):
    """Parse the rendered Utah elections landing page and return all elections.

    Thin wrapper around :func:`Clarity.discovery.parse_election_links`
    with the Utah base URL pre-filled.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the Utah elections landing page.

    Returns
    -------
    list[UtElectionInfo]
        Elections sorted ascending by year, then by name.
    """
    return _parse_election_links(html_str, UT_BASE_URL)
