"""
Test script to render SC/NM ElectionStats pages and save HTML for inspection.

Run this to understand the rendered DOM structure before implementing v2 parsers.
"""

from pathlib import Path
from ElectionStats.playwright_client import PlaywrightClient

# Output directory for saved HTML files
OUTPUT_DIR = Path(__file__).parent.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def test_south_carolina():
    """Render SC search page and save HTML."""
    print("Fetching South Carolina 2024 elections...")
    with PlaywrightClient("south_carolina", "https://electionhistory.scvotes.gov") as client:
        html = client.get_search_page(2024, 2024)

        output_file = OUTPUT_DIR / "sc_rendered_2024.html"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"✓ Saved to {output_file}")
        print(f"  HTML length: {len(html):,} chars")


def test_new_mexico():
    """Render NM search page and save HTML."""
    print("\nFetching New Mexico 2024 elections...")
    with PlaywrightClient("new_mexico", "https://electionstats.sos.nm.gov") as client:
        html = client.get_search_page(2024, 2024)

        output_file = OUTPUT_DIR / "nm_rendered_2024.html"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"✓ Saved to {output_file}")
        print(f"  HTML length: {len(html):,} chars")


if __name__ == "__main__":
    print("=" * 60)
    print("Playwright HTML Rendering Test")
    print("=" * 60)

    try:
        test_south_carolina()
        test_new_mexico()

        print("\n" + "=" * 60)
        print("✓ Success! Inspect the saved HTML files to understand structure.")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
