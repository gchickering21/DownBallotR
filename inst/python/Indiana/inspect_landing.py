"""
One-shot script to inspect the Indiana FirstTuesday election results site.

Run from inst/python/ with:
    python -m Indiana.inspect_landing
    python -m Indiana.inspect_landing --archive 2020General
    python -m Indiana.inspect_landing --headless

What it does:
  1. Navigates to https://enr.indianavoters.in.gov/site/index.html
     (or an archive election if --archive is given).
  2. Intercepts all XHR/fetch network requests and logs their URLs and responses.
  3. Saves the rendered HTML to /tmp/in_landing.html for manual inspection.
  4. Prints a summary of all data-bearing network requests found.

This output is used to discover the API endpoints before writing the scraper.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from playwright.sync_api import sync_playwright, Request, Response

IN_SITE_URL    = "https://enr.indianavoters.in.gov/site/index.html"
IN_ARCHIVE_URL = "https://enr.indianavoters.in.gov/archive/{slug}/index.html"

# Extensions and mime types that are definitely not data (skip logging them).
_SKIP_EXTENSIONS = {".js", ".css", ".png", ".jpg", ".ico", ".woff", ".woff2", ".svg", ".gif"}
_SKIP_PREFIXES   = ["/cdn-cgi/", "google", "gtag", "analytics"]


def _should_log(url: str) -> bool:
    """Return True if this URL looks like a data/API request worth logging."""
    lower = url.lower()
    for prefix in _SKIP_PREFIXES:
        if prefix in lower:
            return False
    for ext in _SKIP_EXTENSIONS:
        path = url.split("?")[0]
        if path.endswith(ext):
            return False
    return True


def inspect(url: str, save_path: str, sleep_s: float, headless: bool) -> None:
    captured: list[dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = context.new_page()

        # Intercept responses to capture API calls.
        def on_response(response: Response) -> None:
            req_url = response.url
            if not _should_log(req_url):
                return
            try:
                body = response.text()
            except Exception:
                body = "<binary or unreadable>"
            captured.append({
                "url":    req_url,
                "status": response.status,
                "method": response.request.method,
                "body_preview": body[:500] if body else "",
                "body_len": len(body) if body else 0,
            })

        page.on("response", on_response)

        print(f"[IN inspect] Navigating to: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)

        # Wait for Cloudflare challenge if present.
        try:
            title = page.title()
            if "just a moment" in title.lower():
                print("[IN inspect] Cloudflare challenge detected — waiting up to 15s...")
                page.wait_for_function(
                    "() => document.title.toLowerCase().indexOf('just a moment') === -1",
                    timeout=15_000,
                )
                print("[IN inspect] Challenge resolved.")
        except Exception:
            pass

        print(f"[IN inspect] Sleeping {sleep_s}s for JS to render...")
        time.sleep(sleep_s)

        # Try to find and log the page title and any dropdown options.
        try:
            title = page.title()
            print(f"[IN inspect] Page title: {title!r}")
        except Exception:
            pass

        # Log all <select> elements and their options.
        try:
            selects = page.evaluate("""
                () => Array.from(document.querySelectorAll('select')).map((s, i) => ({
                    index: i,
                    id: s.id,
                    name: s.name,
                    options: Array.from(s.options).map(o => ({text: o.text.trim(), value: o.value}))
                }))
            """)
            if selects:
                print(f"\n[IN inspect] Found {len(selects)} <select> element(s):")
                for sel in selects:
                    print(f"  [{sel['index']}] id={sel['id']!r} name={sel['name']!r}"
                          f"  ({len(sel['options'])} options)")
                    for opt in sel["options"][:10]:
                        print(f"      {opt['value']!r:40}  {opt['text']!r}")
                    if len(sel["options"]) > 10:
                        print(f"      ... ({len(sel['options']) - 10} more)")
            else:
                print("\n[IN inspect] No <select> elements found on page.")
        except Exception as exc:
            print(f"[IN inspect] Could not read <select> elements: {exc}")

        # Log all visible links.
        try:
            links = page.evaluate("""
                () => Array.from(document.querySelectorAll('a[href]'))
                    .map(a => ({text: a.innerText.trim().slice(0, 80), href: a.href}))
                    .filter(a => a.href && !a.href.startsWith('javascript'))
                    .slice(0, 30)
            """)
            if links:
                print(f"\n[IN inspect] Sample links ({len(links)} shown):")
                for lnk in links:
                    print(f"  {lnk['href']:<70}  {lnk['text']!r}")
        except Exception as exc:
            print(f"[IN inspect] Could not read links: {exc}")

        # Save rendered HTML.
        html = page.content()
        out_path = Path(save_path)
        out_path.write_text(html, encoding="utf-8")
        print(f"\n[IN inspect] Saved rendered HTML → {out_path}  ({len(html):,} bytes)")

        browser.close()

    # Print captured network requests.
    data_requests = [r for r in captured if r["body_len"] > 0]
    print(f"\n[IN inspect] Captured {len(captured)} network response(s), "
          f"{len(data_requests)} with non-empty body:\n")
    for req in captured:
        marker = "*** " if req["body_len"] > 100 else "    "
        print(f"  {marker}[{req['status']}] {req['method']} {req['url']}")
        if req["body_len"] > 100:
            preview = req["body_preview"].replace("\n", " ")[:200]
            print(f"       body ({req['body_len']} bytes): {preview}")

    # Also save the captured network log.
    log_path = Path(save_path).parent / "in_network_log.json"
    log_path.write_text(json.dumps(captured, indent=2), encoding="utf-8")
    print(f"\n[IN inspect] Network log saved → {log_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect the Indiana FirstTuesday election results site."
    )
    parser.add_argument(
        "--archive", default=None, metavar="SLUG",
        help="Inspect an archived election (e.g. '2020General'). "
             "Default: inspect the live /site/ page."
    )
    parser.add_argument(
        "--headless", action="store_true", default=False,
        help="Run browser headlessly (default: visible window for debugging)."
    )
    parser.add_argument(
        "--sleep", type=float, default=5.0,
        help="Seconds to wait after page load for JS to settle (default: 5.0)."
    )
    parser.add_argument(
        "--save", default="/tmp/in_landing.html",
        help="Path to save rendered HTML (default: /tmp/in_landing.html)."
    )
    args = parser.parse_args()

    url = (
        IN_ARCHIVE_URL.format(slug=args.archive)
        if args.archive
        else IN_SITE_URL
    )

    inspect(url=url, save_path=args.save, sleep_s=args.sleep, headless=args.headless)


if __name__ == "__main__":
    main()
