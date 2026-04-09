"""
HTTP retry helpers shared across DownBallotR scrapers.
"""

from __future__ import annotations

import datetime
import time
import requests

_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

# Canonical User-Agent for all requests-based scrapers in DownBallotR.
# Identifies the tool and links to the repo so site admins can contact us.
DOWNBALLOT_UA = "DownBallotR/1.0 (+https://github.com/gchickering21/DownBallotR)"


def _parse_retry_after(response) -> "float | None":
    """Return the number of seconds to wait from a Retry-After header, or None.

    Handles both the integer-seconds form (``Retry-After: 30``) and the
    HTTP-date form (``Retry-After: Wed, 21 Oct 2025 07:28:00 GMT``).
    """
    val = response.headers.get("Retry-After")
    if val is None:
        return None
    # Integer seconds form
    try:
        return max(float(val), 0.0)
    except ValueError:
        pass
    # HTTP-date form
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(val)
        wait = (dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
        return max(wait, 0.0)
    except Exception:
        return None


def fetch_with_retry(
    fetch_fn,
    url: str,
    retries: int = 3,
    backoff_s: float = 5.0,
    retry_statuses: frozenset = _RETRY_STATUSES,
) -> str:
    """Call ``fetch_fn(url)`` with exponential-backoff retries on transient errors.

    Honors the ``Retry-After`` response header on 429 replies — if the server
    specifies a wait time, that takes precedence over the local backoff value.

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
            # Respect Retry-After on 429 — the server is explicitly telling us
            # how long to back off; ignoring it risks getting blocked entirely.
            if resp is not None and resp.status_code == 429:
                server_wait = _parse_retry_after(resp)
                if server_wait is not None:
                    delay = max(delay, server_wait)
                    print(
                        f"  [WARN] Rate-limited (429) — server requests "
                        f"{server_wait:.0f}s wait. Honoring Retry-After."
                    )
            last_exc = e

        if attempt < retries:
            print(
                f"  [WARN] Transient error on attempt {attempt}/{retries} "
                f"— retrying in {delay:.0f}s ({url})"
            )
            time.sleep(delay)
            delay *= 2

    raise last_exc
