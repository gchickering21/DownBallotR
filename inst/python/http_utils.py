"""
HTTP retry helpers shared across DownBallotR scrapers.
"""

from __future__ import annotations

import time


_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


def fetch_with_retry(
    fetch_fn,
    url: str,
    retries: int = 3,
    backoff_s: float = 5.0,
    retry_statuses: frozenset = _RETRY_STATUSES,
) -> str:
    """Call ``fetch_fn(url)`` with exponential-backoff retries on transient errors.

    Parameters
    ----------
    fetch_fn : callable
        A function accepting a URL string and returning the response text.
        Should raise ``requests.exceptions.Timeout`` or
        ``requests.exceptions.HTTPError`` on failure.
    url : str
        URL to fetch.
    retries : int
        Maximum number of attempts (default 3).
    backoff_s : float
        Initial delay between retries in seconds; doubles each attempt (default 5.0).
    retry_statuses : frozenset[int]
        HTTP status codes that trigger a retry (default: 429, 500, 502, 503, 504).

    Raises
    ------
    Exception
        Re-raises the last exception if all retries are exhausted.
    """
    import requests

    delay = backoff_s
    last_exc: Exception = RuntimeError("no attempts made")

    for attempt in range(1, retries + 1):
        try:
            return fetch_fn(url)
        except requests.exceptions.Timeout as e:
            last_exc = e
        except requests.exceptions.HTTPError as e:
            resp = getattr(e, "response", None)
            if resp is not None and resp.status_code not in retry_statuses:
                raise
            last_exc = e

        if attempt < retries:
            print(
                f"  [WARN] Transient error on attempt {attempt}/{retries} "
                f"— retrying in {delay:.0f}s ({url})"
            )
            time.sleep(delay)
            delay *= 2

    raise last_exc
