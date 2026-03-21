"""
Inspect the vote-method expanded HTML on a Georgia SOS election page.

Loads an election page, clicks all per-panel "Vote Method" toggle buttons,
waits for Angular to re-render, then saves the resulting HTML to a file.

Run from inst/python/ with::

    python -m Georgia.inspect_vote_method
    python -m Georgia.inspect_vote_method --slug 2024NovGen --out /tmp/ga_vm.html
"""

from __future__ import annotations

import argparse
import time

from .client import GaPlaywrightClient

GA_ELECTION_URL = "https://results.sos.ga.gov/results/public/Georgia/elections/{slug}"


def inspect(slug: str, out: str, sleep_s: float = 4.0) -> None:
    url = GA_ELECTION_URL.format(slug=slug)
    print(f"[inspect_vote_method] Loading: {url}")

    with GaPlaywrightClient(headless=True, sleep_s=sleep_s) as client:
        # 1. Load the election page
        html_before = client.get_election_page(url)
        with open(out.replace(".html", "_before.html"), "w") as f:
            f.write(html_before)
        print(f"  Saved pre-click HTML → {out.replace('.html', '_before.html')}")

        assert client.page is not None

        # 2. Count vote-method buttons
        btns = client.page.query_selector_all(
            "button[role='checkbox'][aria-checked='false'] span.pi-stop, "
            "button[role='checkbox'] span[class*='pi-stop']"
        )
        # Fallback: find all per-panel header checkboxes
        if not btns:
            btns = client.page.query_selector_all(
                "button.p-panel-header-icon[role='checkbox']"
            )
        print(f"  Found {len(btns)} vote-method button(s)")

        if not btns:
            print("  WARNING: No vote-method buttons found. Trying JS click on all aria-checked=false buttons...")
            client.page.evaluate("""
                document.querySelectorAll('button[role="checkbox"][aria-checked="false"]')
                    .forEach(btn => btn.click());
            """)
        else:
            # Click only the first panel's button for a minimal test
            print("  Clicking first panel's vote-method button...")
            btns[0].click()

        # Wait for Angular re-render
        print(f"  Waiting {sleep_s}s for re-render...")
        time.sleep(sleep_s)

        html_after = client.page.content()
        with open(out, "w") as f:
            f.write(html_after)
        print(f"  Saved post-click HTML → {out}")

    # Quick diff: what new classes appeared?
    from lxml import html as lhtml
    doc_before = lhtml.fromstring(html_before)
    doc_after  = lhtml.fromstring(html_after)

    cls_before = {c for el in doc_before.xpath("//*[@class]") for c in el.get("class","").split()}
    cls_after  = {c for el in doc_after.xpath("//*[@class]") for c in el.get("class","").split()}
    new_classes = cls_after - cls_before
    print(f"\n  New classes after click: {sorted(new_classes)}")

    # Show first few ballot-option-like elements that changed
    import re
    new_terms = ["vote-method", "method", "election-day", "advance", "absentee", "breakdown"]
    for term in new_terms:
        hits = [m.start() for m in re.finditer(term, html_after, re.I)]
        if hits:
            ctx = html_after[max(0, hits[0]-100):hits[0]+400]
            print(f"\n  [{term}] first hit context:")
            print(repr(ctx))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect GA SOS vote-method expanded HTML."
    )
    parser.add_argument(
        "--slug", default="2024NovGen",
        help="Election slug (default: 2024NovGen).",
    )
    parser.add_argument(
        "--out", default="/tmp/ga_vote_method.html",
        help="Output file for post-click HTML (default: /tmp/ga_vote_method.html).",
    )
    parser.add_argument(
        "--sleep", type=float, default=4.0,
        help="Seconds to wait after click for Angular re-render (default: 4.0).",
    )
    args = parser.parse_args()
    inspect(args.slug, args.out, args.sleep)


if __name__ == "__main__":
    main()
