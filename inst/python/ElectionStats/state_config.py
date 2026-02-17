"""
State configuration registry for ElectionStats scrapers.

Defines scraper types, scraping methods, and URL configurations for all supported states.
"""

from typing import Literal, TypedDict

ScraperType = Literal["classic", "v2"]
ScrapingMethod = Literal["requests", "playwright"]


class StateConfig(TypedDict):
    """Configuration for a state's ElectionStats scraper."""

    base_url: str
    search_path: str
    scraper_type: ScraperType
    scraping_method: ScrapingMethod


STATE_CONFIGS: dict[str, StateConfig] = {
    "virginia": {
        "base_url": "https://historical.elections.virginia.gov/elections",
        "search_path": "/search",
        "scraper_type": "classic",
        "scraping_method": "requests",
    },
    "massachusetts": {
        "base_url": "https://electionstats.state.ma.us/elections",
        "search_path": "/search",
        "scraper_type": "classic",
        "scraping_method": "requests",
    },
    "colorado": {
        "base_url": "https://co.elstats2.civera.com/eng/contests",
        "search_path": "",
        "scraper_type": "classic",
        "scraping_method": "requests",
    },
    "south_carolina": {
        "base_url": "https://electionhistory.scvotes.gov",
        "search_path": "/search",
        "scraper_type": "v2",
        "scraping_method": "playwright",
    },
    "new_mexico": {
        "base_url": "https://electionstats.sos.nm.gov",
        "search_path": "/search",
        "scraper_type": "v2",
        "scraping_method": "playwright",
    },
}


def get_scraper_type(state_key: str) -> ScraperType:
    """Get the scraper type ('classic' or 'v2') for a given state.

    Parameters
    ----------
    state_key : str
        State identifier (e.g., 'south_carolina', 'virginia')

    Returns
    -------
    ScraperType
        Either 'classic' or 'v2'

    Raises
    ------
    ValueError
        If state_key is not recognized
    """
    state_key = state_key.strip().lower().replace(" ", "_")
    config = STATE_CONFIGS.get(state_key)
    if not config:
        raise ValueError(f"Unknown state: {state_key}")
    return config["scraper_type"]


def get_state_config(state_key: str) -> StateConfig:
    """Get full configuration for a state.

    Parameters
    ----------
    state_key : str
        State identifier (e.g., 'south_carolina', 'virginia')

    Returns
    -------
    StateConfig
        Complete state configuration including URLs and scraping method

    Raises
    ------
    ValueError
        If state_key is not recognized
    """
    state_key = state_key.strip().lower().replace(" ", "_")
    if state_key not in STATE_CONFIGS:
        raise ValueError(
            f"Unknown state: {state_key}. Available: {sorted(STATE_CONFIGS.keys())}"
        )
    return STATE_CONFIGS[state_key]


def requires_playwright(state_key: str) -> bool:
    """Check if a state requires Playwright for scraping.

    Parameters
    ----------
    state_key : str
        State identifier (e.g., 'south_carolina', 'virginia')

    Returns
    -------
    bool
        True if Playwright is required, False if simple requests suffice
    """
    config = get_state_config(state_key)
    return config["scraping_method"] == "playwright"
