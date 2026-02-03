# inst/python/registry.py

from inst.python.NorthCarolina.pipeline import get_nc_election_results
# from Virginia.va_pipeline import get_va_election_results  # later

_STATE_HANDLERS = {
    "NC": get_nc_election_results,
    # "VA": get_va_election_results,
}


def get_local_elections(state: str, date: str | None = None):
    state = state.upper()

    if state not in _STATE_HANDLERS:
        raise ValueError(
            f"State '{state}' not supported. "
            f"Available states: {sorted(_STATE_HANDLERS.keys())}"
        )

    return _STATE_HANDLERS[state](date=date)


def available_states():
    return sorted(_STATE_HANDLERS.keys())
