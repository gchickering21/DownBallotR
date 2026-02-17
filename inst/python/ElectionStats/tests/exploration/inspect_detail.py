"""Inspect detail page structure."""

from pathlib import Path
from lxml import html

# Read from tests/output directory
output_dir = Path(__file__).parent.parent / "output"
html_file = output_dir / "sc_detail_8119.html"

if not html_file.exists():
    print(f"⚠️  File not found: {html_file}")
    print("   Run test_detail_page.py first to generate it.")
    exit(1)

with open(html_file, "r") as f:
    content = f.read()

doc = html.fromstring(content)

# Find all tables
tables = doc.xpath("//table")
print(f"Found {len(tables)} tables\n")

for i, table in enumerate(tables):
    print(f"Table {i+1}:")
    print(f"  ID: {table.get('id')}")
    print(f"  Class: {table.get('class')}")

    rows = table.xpath(".//tr")
    print(f"  Rows: {len(rows)}")

    if rows:
        # Look at first few rows
        for j, row in enumerate(rows[:3]):
            cells = row.xpath("./th|./td")
            if cells:
                cell_text = [' '.join(cell.xpath('.//text()')).strip()[:50] for cell in cells]
                print(f"    Row {j+1} cells: {cell_text}")

    print()
