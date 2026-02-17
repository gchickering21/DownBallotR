"""Test fetching SC detail/county page."""

from pathlib import Path
from ElectionStats.playwright_client import PlaywrightClient

# Output directory for saved HTML files
OUTPUT_DIR = Path(__file__).parent.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

print("Fetching SC contest detail page (election_id=8119)...")
with PlaywrightClient("south_carolina", "https://electionhistory.scvotes.gov") as client:
    # Navigate to detail page (URL pattern is /contest/ID not /view/ID)
    url = f"{client.base_url}/contest/8119"
    client.page.goto(url, wait_until="networkidle")

    # Wait a bit for any JS to load
    import time
    time.sleep(2)

    html = client.page.content()

    output_file = OUTPUT_DIR / "sc_detail_8119.html"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✓ Saved to {output_file}")
    print(f"  HTML length: {len(html):,} chars")
