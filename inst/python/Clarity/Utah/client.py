"""Utah election results browser client — thin wrapper around ClarityPlaywrightClient."""

from Clarity.client import ClarityPlaywrightClient

UT_BASE_URL = "https://electionresults.utah.gov/results/public/Utah"


class UtPlaywrightClient(ClarityPlaywrightClient):
    """Browser client for the Utah election results site.

    Thin wrapper around :class:`Clarity.client.ClarityPlaywrightClient` with
    Utah-specific defaults (base URL and log prefix).

    Parameters
    ----------
    headless : bool
        Run browser in headless mode (default True).
    sleep_s : float
        Seconds to wait after a page loads to allow JS rendering to complete.
    """

    def __init__(self, headless: bool = True, sleep_s: float = 3.0):
        super().__init__(
            base_url=UT_BASE_URL,
            log_prefix="[UT]",
            headless=headless,
            sleep_s=sleep_s,
        )
