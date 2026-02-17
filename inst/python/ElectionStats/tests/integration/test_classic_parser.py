"""Test classic state parsers (VA/MA/CO) with live data."""

from ElectionStats.electionStats_search import _choose_row_parser, parse_search_results
from ElectionStats.electionStats_client import StateHttpClient, HttpConfig
from ElectionStats.state_config import get_state_config


def test_parser_selection():
    """Test that correct parsers are selected for each classic state."""
    print("=" * 60)
    print("Testing Parser Selection")
    print("=" * 60)

    # Test Virginia gets vama parser
    parser_va = _choose_row_parser("virginia")
    print(f"Virginia parser: {parser_va.__name__}")

    # Test Massachusetts gets vama parser
    parser_ma = _choose_row_parser("massachusetts")
    print(f"Massachusetts parser: {parser_ma.__name__}")

    # Test Colorado gets colorado parser
    parser_co = _choose_row_parser("colorado")
    print(f"Colorado parser: {parser_co.__name__}")

    assert parser_va.__name__ == "_parse_search_row_vama"
    assert parser_ma.__name__ == "_parse_search_row_vama"
    assert parser_co.__name__ == "_parse_search_row_colorado"

    print("\n✓ All parsers selected correctly\n")


def test_virginia_parser():
    """Test parsing Virginia HTML."""
    print("=" * 60)
    print("Testing Virginia Parser (VAMA)")
    print("=" * 60)

    config = get_state_config("virginia")

    client = StateHttpClient(
        state="virginia",
        base_url=config["base_url"],
        config=HttpConfig(timeout_s=30, sleep_s=0.1),
        search_path=config["search_path"],
    )

    # Fetch and parse one page
    url = client.build_search_url(year_from=2024, year_to=2024, page=1)
    html = client.get_html(url)
    rows = parse_search_results(html, client, "virginia", url)

    print(f"\n✓ Parsed {len(rows)} results from Virginia\n")

    if rows:
        print("Sample parsed data:")
        r = rows[0]
        print(f"  Election ID: {r.election_id}")
        print(f"  Year: {r.year}")
        print(f"  Office: {r.office}")
        print(f"  Candidate: {r.candidate}")
        print(f"  Party: {r.party}")


def test_colorado_parser():
    """Test parsing Colorado HTML."""
    print("\n" + "=" * 60)
    print("Testing Colorado Parser (Colorado-specific)")
    print("=" * 60)

    config = get_state_config("colorado")

    client = StateHttpClient(
        state="colorado",
        base_url=config["base_url"],
        config=HttpConfig(timeout_s=30, sleep_s=0.1),
        search_path=config["search_path"],
    )

    # Fetch and parse one page
    url = client.build_search_url(year_from=2024, year_to=2024, page=1)
    html = client.get_html(url)
    rows = parse_search_results(html, client, "colorado", url)

    print(f"\n✓ Parsed {len(rows)} results from Colorado\n")

    if rows:
        print("Sample parsed data:")
        r = rows[0]
        print(f"  Election ID: {r.election_id}")
        print(f"  Year: {r.year}")
        print(f"  Office: {r.office}")
        print(f"  Candidate: {r.candidate}")
        print(f"  Party: {r.party}")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Classic State Parser Tests")
    print("=" * 60 + "\n")

    try:
        test_parser_selection()
        test_virginia_parser()
        test_colorado_parser()

        print("\n" + "=" * 60)
        print("✓ All parser tests passed!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
