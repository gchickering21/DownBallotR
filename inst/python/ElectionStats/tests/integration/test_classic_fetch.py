"""Test fetching from classic states (VA/MA/CO) using requests."""

from ElectionStats.electionStats_client import StateHttpClient, HttpConfig
from ElectionStats.electionStats_search import fetch_search_results
from ElectionStats.state_config import get_state_config


def test_virginia():
    """Test fetching Virginia 2024 elections."""
    print("=" * 60)
    print("Testing Virginia (classic state with requests)")
    print("=" * 60)

    config = get_state_config("virginia")

    client = StateHttpClient(
        state="virginia",
        base_url=config["base_url"],
        config=HttpConfig(timeout_s=30, sleep_s=0.2),
        search_path=config["search_path"],
    )

    # Fetch one page of 2024 results
    rows = fetch_search_results(
        client,
        year_from=2024,
        year_to=2024,
        page=1,
        state_name="virginia"
    )

    print(f"\n✓ Fetched {len(rows)} election results from Virginia\n")

    if rows:
        print("Sample result:")
        r = rows[0]
        print(f"  Year: {r.year}")
        print(f"  Office: {r.office}")
        print(f"  District: {r.district}")
        print(f"  Stage: {r.stage}")
        print(f"  Candidate: {r.candidate}")
        print(f"  Vote %: {r.vote_percentage}")
        print(f"  Outcome: {r.contest_outcome}")


def test_massachusetts():
    """Test fetching Massachusetts 2024 elections."""
    print("\n" + "=" * 60)
    print("Testing Massachusetts (classic state with requests)")
    print("=" * 60)

    config = get_state_config("massachusetts")

    client = StateHttpClient(
        state="massachusetts",
        base_url=config["base_url"],
        config=HttpConfig(timeout_s=30, sleep_s=0.2),
        search_path=config["search_path"],
    )

    rows = fetch_search_results(
        client,
        year_from=2024,
        year_to=2024,
        page=1,
        state_name="massachusetts"
    )

    print(f"\n✓ Fetched {len(rows)} election results from Massachusetts\n")

    if rows:
        print("First result:")
        r = rows[0]
        print(f"  {r.candidate} - {r.office} ({r.contest_outcome})")


def test_colorado():
    """Test fetching Colorado 2024 elections."""
    print("\n" + "=" * 60)
    print("Testing Colorado (classic state with requests)")
    print("=" * 60)

    config = get_state_config("colorado")

    client = StateHttpClient(
        state="colorado",
        base_url=config["base_url"],
        config=HttpConfig(timeout_s=30, sleep_s=0.2),
        search_path=config["search_path"],
    )

    rows = fetch_search_results(
        client,
        year_from=2024,
        year_to=2024,
        page=1,
        state_name="colorado"
    )

    print(f"\n✓ Fetched {len(rows)} election results from Colorado\n")

    if rows:
        print("First result:")
        r = rows[0]
        print(f"  {r.candidate} - {r.office} ({r.contest_outcome})")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Classic States Fetch Test")
    print("=" * 60 + "\n")

    try:
        test_virginia()
        test_massachusetts()
        test_colorado()

        print("\n" + "=" * 60)
        print("✓ All classic state tests passed!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
