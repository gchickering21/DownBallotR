"""Georgia SOS discovery — thin wrapper around Clarity.discovery."""

from Clarity.discovery import parse_election_links as _parse_election_links
from Clarity.Georgia.client import GA_BASE_URL

# Re-export ClarityElectionInfo as GaElectionInfo for backward compatibility.
from Clarity.models import ClarityElectionInfo as GaElectionInfo

__all__ = ["parse_election_links", "GaElectionInfo"]


def parse_election_links(html_str: str):
    """Parse the rendered Georgia SOS landing page and return all elections.

    Thin wrapper around :func:`Clarity.discovery.parse_election_links`
    with the Georgia base URL pre-filled.

    Parameters
    ----------
    html_str : str
        Fully rendered HTML of the Georgia elections landing page.

    Returns
    -------
    list[GaElectionInfo]
        Elections sorted ascending by year, then by name.
    """
    return _parse_election_links(html_str, GA_BASE_URL)
