# ElectionStats Tests

This directory contains development and testing scripts for ElectionStats scrapers.

## Directory Structure

```
tests/
├── exploration/          # HTML inspection scripts (used during development)
│   ├── inspect_detail.py     - Inspect county/detail page structure
│   ├── inspect_html.py       - Inspect search results table structure
│   └── inspect_rows.py       - Inspect individual row contents
└── integration/          # Component and integration tests
    ├── test_classic_fetch.py   - Test fetching from classic states (VA/MA/CO)
    ├── test_classic_parser.py  - Test classic state parsers
    ├── test_v2_parser.py       - Test v2 parser with SC/NM
    ├── test_playwright.py      - Test Playwright client renders pages
    └── test_detail_page.py     - Test fetching detail pages
```

## Running Tests

### Integration Tests (Live Scraping)

**Test classic states (VA/MA/CO):**
```bash
cd inst/python
python -m ElectionStats.tests.integration.test_classic_fetch
python -m ElectionStats.tests.integration.test_classic_parser
```

**Test v2 states (SC/NM):**
```bash
python -m ElectionStats.tests.integration.test_playwright
python -m ElectionStats.tests.integration.test_v2_parser
```

**Test detail page fetching:**
```bash
python -m ElectionStats.tests.integration.test_detail_page
```

### Exploration Scripts (HTML Inspection)

These scripts help understand HTML structure when adding new states or debugging:

**Inspect rendered v2 HTML:**
```bash
# First, render a page and save HTML
python -m ElectionStats.tests.integration.test_playwright

# Then inspect the structure
cd ElectionStats/tests/exploration
python inspect_html.py    # Shows tables and rows
python inspect_rows.py    # Shows cell contents
```

**Inspect detail pages:**
```bash
# First, fetch a detail page
python -m ElectionStats.tests.integration.test_detail_page

# Then inspect
cd ElectionStats/tests/exploration
python inspect_detail.py
```

## When to Use These Tests

### Adding a New Classic State
1. Run `test_classic_fetch.py` with your state to verify basic connectivity
2. Use `exploration/` scripts to inspect HTML structure
3. Modify parsers if needed
4. Run `test_classic_parser.py` to verify parsing

### Adding a New V2 State
1. Add state to `state_config.py`
2. Run `test_playwright.py` to render and save HTML
3. Use `exploration/` scripts to inspect structure
4. Verify v2 parsers work with `test_v2_parser.py`

### Debugging Scraping Issues
1. Use `test_*_fetch.py` to verify connectivity
2. Use `test_*_parser.py` to verify parsing
3. Use `exploration/` scripts to inspect HTML when parsers fail

## Notes

- **Exploration scripts** save HTML files to `inst/python/` for inspection
- **Integration tests** require internet connection and hit live sites
- Tests use small samples (1 page, 1 year) to minimize load on election sites
- Always use reasonable delays (`sleep_s`) when testing to be respectful
