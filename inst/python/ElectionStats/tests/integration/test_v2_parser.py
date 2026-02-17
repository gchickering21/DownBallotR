"""Test v2 parser with rendered SC HTML."""

from pathlib import Path
from ElectionStats.electionStats_search import parse_search_results, fetch_all_search_results_v2
from ElectionStats.playwright_client import PlaywrightClient
from ElectionStats.state_config import get_state_config

# Mock client for parse_search_results
class MockClient:
    def __init__(self, state, base_url):
        self.state = state
        self.base_url = base_url

# Path to saved HTML file
OUTPUT_DIR = Path(__file__).parent.parent / "output"
html_file = OUTPUT_DIR / "sc_rendered_2024.html"

# Test with saved HTML (if available)
if html_file.exists():
    print("Testing v2 parser with saved SC HTML...")
    with open(html_file, "r") as f:
        html_content = f.read()

    mock_client = MockClient("south_carolina", "https://electionhistory.scvotes.gov")
    rows = parse_search_results(html_content, mock_client, "south_carolina", "")

    print(f"\n✓ Parsed {len(rows)} election results\n")

    if rows:
        print("Sample result:")
        r = rows[0]
        print(f"  Year: {r.year}")
        print(f"  Office: {r.office}")
        print(f"  District: {r.district}")
        print(f"  Stage: {r.stage}")
        print(f"  Election ID: {r.election_id}")
        print(f"  Candidate: {r.candidate}")
        print(f"  Party: {r.party}")
        print(f"  Vote %: {r.vote_percentage}")
        print(f"  Outcome: {r.contest_outcome}")
else:
    print(f"⚠️  Saved HTML not found at {html_file}")
    print("   Run test_playwright.py first to generate it.")

print("\n" + "=" * 60)
print("Testing live fetch with Playwright...")
print("=" * 60)

try:
    with PlaywrightClient("south_carolina", "https://electionhistory.scvotes.gov") as client:
        live_rows = fetch_all_search_results_v2(client, 2024, 2024, "south_carolina")
        print(f"\n✓ Fetched {len(live_rows)} results live\n")

        if live_rows:
            print("First live result:")
            r = live_rows[0]
            print(f"  {r.candidate} - {r.office} ({r.contest_outcome})")

except Exception as e:
    print(f"\n✗ Live fetch failed: {e}")
    import traceback
    traceback.print_exc()
