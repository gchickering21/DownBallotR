# DownBallotR — Developer Guide

This document covers repo structure, architecture, how to add new states, and how
to run the test suite. See `README.md` for user-facing installation and usage.

---

## Repository layout

```
DownBallotR/
├── R/                              # R package source
│   ├── scraper_registry.R          # scrape_elections() + db_list_* exports
│   ├── python_bind.R               # db_bind_python() — virtualenv + sys.path setup
│   ├── downballot_use_python.R     # downballot_use_python() helper
│   ├── downballot_install_python.R # downballot_install_python() — one-time setup
│   ├── downballot_python_status.R  # db_python_status() diagnostic
│   └── zzz.R                       # .onLoad hook
│
├── inst/python/                    # Python scraper modules (added to sys.path at runtime)
│   ├── registry.py                 # Central Python entry point — scrape(), list_sources(), etc.
│   │
│   ├── ElectionStats/              # Multi-state ElectionStats scraper
│   │   ├── state_config.py         # STATE_CONFIGS dict — one entry per supported state
│   │   ├── electionStats_client.py # StateHttpClient (requests-based, classic states)
│   │   ├── playwright_client.py    # PlaywrightClient (browser automation, v2 states)
│   │   ├── electionStats_search.py # fetch_search_results / fetch_all_search_results*
│   │   ├── electionStats_county_search.py  # county detail scraping
│   │   ├── electionStats_models.py # ElectionSearchRow dataclass
│   │   ├── run_scrape_yearly.py    # scrape_one_year() — top-level orchestrator
│   │   └── tests/
│   │       ├── integration/
│   │       │   ├── test_all_states_smoke.py  # end-to-end smoke test for all states
│   │       │   ├── test_classic_fetch.py
│   │       │   ├── test_classic_parser.py
│   │       │   ├── test_detail_page.py
│   │       │   ├── test_playwright.py
│   │       │   └── test_v2_parser.py
│   │       ├── exploration/        # Ad-hoc inspection scripts (not CI)
│   │       └── run_all_tests.py
│   │
│   ├── NorthCarolina/              # NC State Board of Elections scraper
│   │   ├── pipeline.py             # NcElectionPipeline + get_nc_election_results()
│   │   ├── discovery.py            # discover_nc_results_zips() — NCSBE index
│   │   ├── aggregate.py            # precinct → county/state rollups
│   │   ├── normalize.py            # column name normalization
│   │   ├── models.py               # NC data models
│   │   ├── selection.py            # election filtering logic
│   │   ├── canonicalize.py         # race/office canonicalization
│   │   ├── election_type_rules.py
│   │   ├── constants.py
│   │   └── io_utils.py             # ZIP download helpers
│   │
│   └── Ballotpedia/                # School board elections scraper
│       ├── school_board_elections.py  # SchoolBoardScraper
│       ├── ballotpedia_client.py
│       └── scrape_school_boards.py
│
├── tests/testthat/                 # R-level tests (testthat)
│   └── test-python-smoke.R
│
├── DESCRIPTION
├── README.md                       # User-facing docs
└── DEVELOPER.md                    # This file
```

---

## Architecture

### R → Python bridge

`scrape_elections()` (R) calls Python via `reticulate`. On first call, `db_bind_python()`:
1. Activates the package's virtualenv (created by `downballot_install_python()`)
2. Adds `inst/python/` to `sys.path`
3. Imports `registry.py` as a module

All routing logic lives in `R/scraper_registry.R`; all scraping logic lives in Python.
`reticulate::py_to_r()` converts the returned pandas DataFrames to R data frames.

### Three scraper backends

| Backend | States | Transport | Entry point |
|---|---|---|---|
| **Classic** (ElectionStats v1) | VA, MA, NH, CO | `requests` | `StateHttpClient` |
| **V2** (ElectionStats v2 / React) | SC, NM, NY | Playwright (headless Chromium) | `PlaywrightClient` |
| **NC** (NCSBE ZIP pipeline) | NC | HTTP ZIP download | `NcElectionPipeline` |
| **Ballotpedia** | All US states | `requests` | `SchoolBoardScraper` |

### ElectionStats URL formats

Classic states (v1) use Rails-style path segments:
```
https://historical.elections.virginia.gov/elections/search/year_from:2024/year_to:2024
```

Colorado (Civera backend) uses query parameters:
```
https://co.elstats2.civera.com/eng/contests?year_from=2024&year_to=2024&page=1
```

V2 states (SC/NM/NY) use a React SPA with the same query-param URL shape as CO but
require Playwright for rendering; the `url_style` field in `STATE_CONFIGS` controls
which format `StateHttpClient.build_search_url()` generates.

### Cloudflare (NY)

`results.elections.ny.gov` sits behind Cloudflare bot protection. `PlaywrightClient`
uses a stealth browser context (realistic Chrome user-agent, viewport, locale,
`navigator.webdriver` patch) that allows it to pass the Cloudflare challenge.

---

## Python environment setup

```r
# One-time — installs a virtualenv at ~/.virtualenvs/DownBallotR (or configured path)
downballot_install_python()

# Check status
db_python_status()
```

Key Python dependencies: `requests`, `lxml`, `pandas`, `playwright`, `pyreadr`.
After installing the virtualenv, install Playwright browsers once:
```bash
python -m playwright install chromium
```

---

## Running tests

### Smoke test — all states (recommended first check)

Run from `inst/python/` with the DownBallotR virtualenv active:

```bash
cd inst/python

# All 8 sources (VA, MA, NH, CO, SC, NM, NY, NC) — takes ~2-3 min
python -m ElectionStats.tests.integration.test_all_states_smoke

# Only requests-based states (fast, ~20 s)
python -m ElectionStats.tests.integration.test_all_states_smoke --classic-only

# Only Playwright-based states (SC, NM, NY)
python -m ElectionStats.tests.integration.test_all_states_smoke --v2-only

# Only North Carolina
python -m ElectionStats.tests.integration.test_all_states_smoke --nc-only
```

Exit code is 0 on success, 1 if any state fails.

### Other integration tests

```bash
cd inst/python

# Classic state fetch and parser
python -m ElectionStats.tests.integration.test_classic_fetch
python -m ElectionStats.tests.integration.test_classic_parser

# V2 (Playwright) parser
python -m ElectionStats.tests.integration.test_v2_parser

# Detail page scraping
python -m ElectionStats.tests.integration.test_detail_page

# Playwright connectivity
python -m ElectionStats.tests.integration.test_playwright
```

### R-level tests

```r
devtools::test()
# or
Rscript -e "testthat::test_dir('tests/testthat')"
```

### Quick manual test (single state)

```r
df <- scrape_elections(state = "virginia", year_from = 2024, year_to = 2024, level = "state")
nrow(df)  # should be > 0
```

---

## Adding a new ElectionStats state

1. **Identify the URL and scraper type** — visit the state's ElectionStats site and check:
   - Does it use server-rendered HTML (classic) or a React SPA (v2/Playwright)?
   - Does the search URL use path segments (`/year_from:YYYY/year_to:YYYY`) or query params?

2. **Add an entry to `STATE_CONFIGS`** in [inst/python/ElectionStats/state_config.py](inst/python/ElectionStats/state_config.py):

   ```python
   "new_state": {
       "base_url": "https://example.electionstats.gov/elections",
       "search_path": "/search",
       "scraper_type": "classic",          # or "v2"
       "scraping_method": "requests",      # or "playwright"
       "url_style": "path_params",         # or "query_params"
   },
   ```

3. **Add a year range** in `_YEAR_RANGES["election_stats"]` in [inst/python/registry.py](inst/python/registry.py):

   ```python
   "new_state": (1990, None),  # None = open-ended (current year)
   ```

4. **Smoke test** — run the smoke test for the new state:

   ```bash
   # Add it to STATE_CONFIGS, then confirm it appears in classic or v2:
   python -m ElectionStats.tests.integration.test_all_states_smoke --classic-only
   ```

5. **No other changes needed** — `scrape_elections(state = "new_state", ...)` will
   route automatically once the state is in `STATE_CONFIGS`.

---

## Key data flow (ElectionStats classic)

```
scrape_elections("virginia", year_from=2024, year_to=2024)   [R]
  └─ .scrape_election_stats(state="virginia", ...)           [R]
       └─ registry.scrape("election_stats", ...)             [Python]
            └─ _scrape_election_stats()                      [Python]
                 └─ scrape_one_year(year=2024)               [Python]
                      ├─ StateHttpClient.build_search_url()
                      │    → /elections/search/year_from:2024/year_to:2024
                      ├─ fetch_all_search_results()
                      │    → list[ElectionSearchRow]
                      ├─ rows_to_dataframe()
                      │    → state_df (one row per candidate)
                      └─ build_county_dataframe_parallel()
                           → county_df (one row per candidate×county)
```

For v2 states (SC/NM/NY), `StateHttpClient` is replaced by `PlaywrightClient` and
`fetch_all_search_results` by `fetch_all_search_results_v2`.

---

## Data output schema

### `level = "state"` / `state_df`
One row per candidate per election. Key columns: `state`, `year`, `election_id`,
`candidate_id`, `office`, `district`, `candidate`, `party`, `votes`, `total_votes`,
`detail_url`.

### `level = "county"` / `county_df`
One row per candidate per county per election. Key columns: `state`, `year`,
`election_id`, `candidate_id`, `county_or_city`, `candidate_name`, `votes`.

### `level = "joined"`
County rows left-joined with state-level candidate metadata on
`(state, election_id, candidate_id)`.

### `level = "all"` (default)
Returns a named R list with `$state` and `$county` data frames.

---

## Useful one-liners

```r
# See all supported sources
db_list_sources()

# See all ElectionStats states
db_list_states("election_stats")

# Check data availability per state
db_available_years()
db_available_years(state = "south_carolina")
```

```python
# From inst/python — check which states exist and their scraping method
from ElectionStats.state_config import STATE_CONFIGS
for k, v in STATE_CONFIGS.items():
    print(k, v["scraping_method"], v["url_style"])
```
