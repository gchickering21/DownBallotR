"""Inspect the actual data rows in detail."""

from lxml import html

with open("../sc_rendered_2024.html", "r") as f:
    content = f.read()

doc = html.fromstring(content)

# Get the contest table
table = doc.xpath("//table[@id='contestCollectionTable']")[0]
rows = table.xpath(".//tbody/tr")

print(f"Data rows: {len(rows)}\n")

# Inspect first 2 data rows in detail
for i, row in enumerate(rows[:2]):
    print(f"=== Row {i+1} ===")
    print(f"ID: {row.get('id')}")
    print(f"Class: {row.get('class')}")

    # Get all cells
    cells = row.xpath("./td")
    print(f"Cells: {len(cells)}\n")

    for j, cell in enumerate(cells):
        text = ' '.join(cell.xpath('.//text()')).strip()
        # Truncate if too long
        if len(text) > 150:
            text = text[:150] + "..."
        print(f"Cell {j+1}: {text}")

        # Check for links
        links = cell.xpath(".//a/@href")
        if links:
            print(f"  -> Link: {links[0]}")

    print("\n")
