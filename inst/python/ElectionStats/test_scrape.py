from __future__ import annotations

from ElectionStats.electionStats_client import HttpConfig, StateHttpClient
from ElectionStats.electionStats_models import ElectionSearchRow
from ElectionStats.electionStats_search import fetch_search_results, rows_to_dataframe
from ElectionStats.electionStats_county_search  import build_county_dataframe_parallel, build_county_dataframe



# --- Simple State Registry ---
STATE_REGISTRY = {
    "virginia": "https://historical.elections.virginia.gov",
    "massachusetts": "https://electionstats.state.ma.us",
}


def main() -> None:
    # -------------------------
    # ✏️ EDIT THESE ONLY
    # -------------------------
    state = "virginia"       # or "massachusetts"
    year_from = 2024
    year_to = 2025
    # -------------------------

    state_key = state.lower().replace(" ", "_")

    if state_key not in STATE_REGISTRY:
        raise ValueError(
            f"Unknown state {state!r}. "
            f"Available: {list(STATE_REGISTRY.keys())}"
        )

    base_url = STATE_REGISTRY[state_key]

    config = HttpConfig(timeout_s=60, sleep_s=0.25)

    client = StateHttpClient(
        state=state_key,
        base_url=base_url,
        config=config,
    )

    print(f"\nScraping {state_key} elections {year_from}-{year_to}...\n")

    # --- Step 1: Search Results ---
    rows = fetch_search_results(
        client,
        year_from=year_from,
        year_to=year_to,
        page=1,
    )

    print(f"Parsed rows: {len(rows)}")
    if not rows:
        print("No rows found.")
        return

    state_df = rows_to_dataframe(rows, client=client)

    print("\nSTATE DF PREVIEW:")
    print(state_df.head())

    # --- Step 2: County Results ---
    def client_factory():
        return StateHttpClient(
            state=state_key,
            base_url=base_url,
            config=HttpConfig(timeout_s=60, sleep_s=0.1),
        )

    county_df = build_county_dataframe_parallel(
        state_df=state_df,
        client_factory=client_factory,
        max_workers=6,
    )

    # county_df = build_county_dataframe(
    #     state_df=state_df,
    #     client=client,
    # )

    print("\nCOUNTY DF PREVIEW:")
    print(county_df.head())

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
