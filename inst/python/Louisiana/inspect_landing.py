"""
One-shot inspection script for the Louisiana SOS Graphical election results page.

Run from inst/python/ with:

    # Step 1 — discover all elections (visible browser):
    python -m Louisiana.inspect_landing

    # Step 2 — inspect tabs for a specific election:
    python -m Louisiana.inspect_landing --election 'object:153'

    # Step 3 — save the rendered HTML for a specific tab (for parser development):
    python -m Louisiana.inspect_landing --election 'object:153' --tab 'Statewide' --save-tab /tmp/la_statewide.html
    python -m Louisiana.inspect_landing --election 'object:153' --tab 'Parish'    --save-tab /tmp/la_parish.html

    # Headless mode:
    python -m Louisiana.inspect_landing --headless --election 'object:153' --tab 'Statewide'

What it does
------------
1. Renders https://voterportal.sos.la.gov/Graphical in a browser.
2. Saves the raw landing-page HTML to /tmp/la_landing.html.
3. Lists every discovered election.
4. If --election is given, lists available tabs.
5. If --tab is also given, clicks that tab and saves its rendered HTML to
   --save-tab path so you can inspect the results structure for parser development.

How to use tab HTML to update the parser
-----------------------------------------
After saving a tab's HTML:
  - Open /tmp/la_statewide.html and look for race/candidate structure.
  - Find the CSS selector for race headers (office names).
  - Find the CSS selector for candidate rows (name, party, votes, pct).
  - Update _parse_results_table() in parser.py accordingly.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from .client import LaPlaywrightClient, _TAB_SEL, _PARISH_TAB_LABEL
from .discovery import parse_election_options


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect the Louisiana SOS Graphical elections page."
    )
    parser.add_argument("--year", type=int, default=None,
                        help="Filter election list to a single year.")
    parser.add_argument("--year-from", type=int, default=None, dest="year_from",
                        help="Filter election list: start year (inclusive).")
    parser.add_argument("--year-to", type=int, default=None, dest="year_to",
                        help="Filter election list: end year (inclusive).")
    parser.add_argument("--headless", action="store_true", default=False,
                        help="Run browser headlessly (default: visible window).")
    parser.add_argument("--save", default="/tmp/la_landing.html",
                        help="Path to save landing-page HTML (default: /tmp/la_landing.html).")
    parser.add_argument("--election", default=None, dest="election_value",
                        help="Option value of a specific election (e.g. 'object:153').")
    parser.add_argument("--tab", default=None, dest="tab_label",
                        help="Tab label to click and capture HTML for (e.g. 'Statewide').")
    parser.add_argument("--save-tab", default=None, dest="save_tab",
                        help="Path to save the tab HTML (default: /tmp/la_<tab>.html).")
    args = parser.parse_args()

    print(f"[LA inspect] Fetching landing page (headless={args.headless})...")

    with LaPlaywrightClient(headless=args.headless, sleep_s=3.0) as client:
        html = client.get_landing_page()

        inspect_tabs = None
        tab_html = None

        if args.election_value:
            print(f"\n[LA inspect] Selecting election {args.election_value!r}...")
            client._select_election(args.election_value)
            time.sleep(3.0)  # let results render
            assert client.page is not None
            inspect_tabs = client._read_tab_labels()
            print(f"[LA inspect] Available tabs: {inspect_tabs}")

            if args.tab_label:
                print(f"[LA inspect] Clicking tab {args.tab_label!r}...")
                client._click_tab(args.tab_label)

                # For Parish tab: also print parish options
                if args.tab_label.lower().startswith(_PARISH_TAB_LABEL.lower()):
                    print("[LA inspect] Parish tab — waiting for parish dropdown...")
                    client._wait_for_parish_dropdown()
                    parish_options = client._read_parish_options()
                    reporting_parishes = client._filter_reporting_parishes(parish_options)
                    print(f"\n[LA inspect] All parish options ({len(parish_options)}):")
                    for name, value in parish_options[:10]:
                        print(f"  [{value}]  {name}")
                    if len(parish_options) > 10:
                        print(f"  ... and {len(parish_options) - 10} more")
                    print(
                        f"\n[LA inspect] Parishes with > 0 reporting: "
                        f"{len(reporting_parishes)}"
                    )

                tab_html = client.page.content()

    # Save landing page HTML.
    out_path = Path(args.save)
    out_path.write_text(html, encoding="utf-8")
    print(f"\n[LA inspect] Saved landing-page HTML → {out_path}  ({len(html):,} bytes)")

    # Save tab HTML if captured.
    if tab_html is not None:
        if args.save_tab:
            tab_path = Path(args.save_tab)
        else:
            safe_label = (args.tab_label or "tab").lower().replace(" ", "_")
            tab_path = Path(f"/tmp/la_{safe_label}.html")
        tab_path.write_text(tab_html, encoding="utf-8")
        print(f"[LA inspect] Saved tab HTML      → {tab_path}  ({len(tab_html):,} bytes)")
        print(
            "\n[LA inspect] Grep for vote counts to locate the results structure:\n"
            f"    grep -n '[0-9]\\{{4,\\}}' {tab_path}  | head -20"
        )

    # Parse and print election list.
    elections = parse_election_options(html)

    if not elections:
        print(
            "\n[LA inspect] WARNING: parse_election_options() returned 0 elections.\n"
            "  Open the saved HTML and look for the election dropdown structure.\n"
            "  Then update _ELECTION_SELECT_INDEX in discovery.py accordingly."
        )
        sys.exit(1)

    if not args.election_value:
        # Full list only when not already printed above.
        print(f"\n[LA inspect] Found {len(elections)} election(s) total:")
        years_seen = sorted({e.year for e in elections}, reverse=True)
        for year in years_seen[:5]:  # show 5 most recent years
            year_elections = [e for e in elections if e.year == year]
            print(f"  {year}  ({len(year_elections)} election(s))")
            for e in year_elections:
                date_str = e.election_date.isoformat() if e.election_date else "date unknown"
                print(f"    [{e.option_value}]  {date_str}  {e.name}")
        print(f"  ... ({len(elections)} total going back to {min(e.year for e in elections)})")

        most_recent = elections[0]
        print(
            f"\n[LA inspect] TIP: Inspect tabs for the most recent election:\n"
            f"  python -m Louisiana.inspect_landing --election {most_recent.option_value!r}\n"
            f"\n  Then save a tab's HTML for parser development:\n"
            f"  python -m Louisiana.inspect_landing "
            f"--election {most_recent.option_value!r} --tab 'Statewide'"
        )

    # Apply year filter if requested.
    year_from = args.year or args.year_from
    year_to   = args.year or args.year_to

    if year_from or year_to:
        filtered = [
            e for e in elections
            if (year_from is None or e.year >= year_from)
            and (year_to   is None or e.year <= year_to)
        ]
        lo = year_from or "–"
        hi = year_to   or "–"
        print(f"\n[LA inspect] Filtered to {lo}–{hi}: {len(filtered)} election(s)")
        for e in filtered:
            date_str = e.election_date.isoformat() if e.election_date else "date unknown"
            print(f"  [{e.option_value}]  {date_str}  {e.name}")


if __name__ == "__main__":
    main()
