# registry/_validators.py
#
# Input validation helpers shared across all _scrape_* functions.
# Covers year coercion, level checking, and worker-count enforcement.

from __future__ import annotations

_VALID_LEVELS    = ("all", "state", "county")
_VALID_LEVELS_CT = ("all", "state", "town")
_VALID_LEVELS_LA = ("all", "state", "parish")

# Max parallel workers allowed via the R / registry.scrape() path.
# download_all_data.py calls pipeline functions directly and is not subject
# to this cap — it is only enforced here (and in R's .validate_max_workers()).
_R_WORKERS_CAP = 4


def _to_year(v) -> "int | None":
    """Coerce *v* to an integer year, accepting int/float/str/None."""
    if v is None:
        return None
    try:
        year = int(float(v))
    except (TypeError, ValueError):
        raise ValueError(f"Cannot convert {v!r} to a year integer.")
    if not (1900 <= year <= 2100):
        raise ValueError(f"Year must be between 1900 and 2100; got {year}.")
    return year


def _validate_level(level: str) -> None:
    if level not in _VALID_LEVELS:
        raise ValueError(f"level must be one of {_VALID_LEVELS}; got {level!r}.")


def _validate_level_ct(level: str) -> None:
    if level not in _VALID_LEVELS_CT:
        raise ValueError(
            f"level must be one of {_VALID_LEVELS_CT} for Connecticut; got {level!r}."
        )


def _validate_workers(value: int, name: str = "max_workers") -> int:
    """Ensure a parallelism argument is a positive integer, capped at _R_WORKERS_CAP.

    This cap applies only to calls routed through registry.scrape() (i.e. from R).
    The download_all_data.py script calls pipeline functions directly and is not
    subject to this limit.
    """
    try:
        val = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a positive integer; got {value!r}.")
    if val < 1:
        raise ValueError(f"{name} must be >= 1; got {val}.")
    if val > _R_WORKERS_CAP:
        print(
            f"  [NOTE] {name}={val} reduced to {_R_WORKERS_CAP}. DownBallotR caps "
            f"parallel workers at {_R_WORKERS_CAP} when called from R to avoid "
            f"overwhelming publicly funded government election sites that are not "
            f"designed for high-volume automated access."
        )
        val = _R_WORKERS_CAP
    return val
