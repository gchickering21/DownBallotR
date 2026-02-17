"""Quick script to inspect rendered HTML structure."""

from pathlib import Path
from lxml import html

# Read from tests/output directory
output_dir = Path(__file__).parent.parent / "output"
html_file = output_dir / "sc_rendered_2024.html"

if not html_file.exists():
    print(f"⚠️  File not found: {html_file}")
    print("   Run test_playwright.py first to generate it.")
    exit(1)

with open(html_file, "r") as f:
    content = f.read()

doc = html.fromstring(content)

# Try to find tables
tables = doc.xpath("//table")
print(f"Found {len(tables)} tables")

for i, table in enumerate(tables[:5]):  # First 5 tables
    print(f"\nTable {i+1}:")
    print(f"  ID: {table.get('id')}")
    print(f"  Class: {table.get('class')}")

    # Find rows
    rows = table.xpath(".//tr")
    print(f"  Rows: {len(rows)}")

    if len(rows) > 0:
        # Look at first data row
        first_row = rows[0]
        print(f"  First row ID: {first_row.get('id')}")
        print(f"  First row class: {first_row.get('class')}")

        cells = first_row.xpath("./th|./td")
        print(f"  First row cells: {len(cells)}")

        if cells:
            print(f"  First cell text: {' '.join(cells[0].xpath('.//text()'))[:100]}")

# Look for specific patterns
print("\n\n=== Looking for election/contest IDs ===")
elements_with_id = doc.xpath("//*[starts-with(@id, 'election-') or starts-with(@id, 'contest-')]")
print(f"Found {len(elements_with_id)} elements with election-/contest- IDs")
for elem in elements_with_id[:5]:
    print(f"  {elem.tag}: id={elem.get('id')}, class={elem.get('class')}")
