"""Georgia SOS browser client — thin wrapper around ClarityPlaywrightClient."""

from Clarity.client import ClarityPlaywrightClient

GA_BASE_URL = "https://results.sos.ga.gov/results/public/Georgia"


class GaPlaywrightClient(ClarityPlaywrightClient):
    """Browser client for the Georgia SOS election results site.

    Thin wrapper around :class:`Clarity.client.ClarityPlaywrightClient` with
    Georgia-specific defaults (base URL and log prefix).

    Parameters
    ----------
    headless : bool
        Run browser in headless mode (default True).
    sleep_s : float
        Seconds to wait after a page loads to allow JS rendering to complete.
    """

    def __init__(self, headless: bool = True, sleep_s: float = 3.0):
        super().__init__(
            base_url=GA_BASE_URL,
            log_prefix="[GA]",
            headless=headless,
            sleep_s=sleep_s,
        )
