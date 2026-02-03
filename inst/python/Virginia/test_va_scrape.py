# test_va_scrape.py
from __future__ import annotations

import json
from typing import Any
import html

from va_client import VaHttpClient, HttpConfig
from va_search import fetch_search_results, fetch_search_results_dicts, rows_to_dataframe


def main() -> None:
    # Polite config: add a small delay if you're going to loop pages later
    config = HttpConfig(timeout_s=60, sleep_s=0.25)
    client = VaHttpClient(config=config)

    # Keep this narrow for a quick test (adjust as you like)
    year_from = 2024
    year_to = 2025
    page = 1

    print(f"Fetching VA elections search: {year_from}-{year_to}, page {page} ...")

    # 1) Fetch+parse dataclasses
    rows = fetch_search_results(client, year_from=year_from, year_to=year_to, page=page)

    print(f"Parsed rows: {len(rows)}")
    if not rows:
        print(
            "No rows parsed. Possible causes:\n"
            "  - Search returned zero results for that year range\n"
            "  - The table id/xpath changed on the site\n"
            "  - You got blocked or received unexpected HTML\n"
        )
        # Also fetch dicts and show raw payload (same underlying parse)
        return

    # Print a small sample
    print("\nSample rows (first 5):")
    for r in rows[:5]:
        print(
            f"- id={r.election_id} year={r.year} office={r.office!r} "
            f"district={r.district!r} stage={r.stage!r} url={r.detail_url}"
        )

    # 2) Fetch dicts for easy JSON output
    dict_rows: list[dict[str, Any]] = fetch_search_results_dicts(
        client, year_from=year_from, year_to=year_to, page=page
    )

    # Quick consistency check: same count
    assert len(dict_rows) == len(rows), "Mismatch between dataclass rows and dict rows?"

    # Print JSON sample
    print("\nJSON sample (first 2):")
    print(json.dumps(dict_rows[:2], indent=2))

    # Simple sanity checks on parsed structure
    assert all(isinstance(r.election_id, int) and r.election_id > 0 for r in rows)
    assert all(year_from <= r.year <= year_to for r in rows), "Unexpected year parsed?"
    assert all(r.office for r in rows), "Empty office field encountered?"

    df = rows_to_dataframe(rows)
    print(df.head())

    print("\n✅ Basic scrape+parse checks passed.")


if __name__ == "__main__":
    main()
