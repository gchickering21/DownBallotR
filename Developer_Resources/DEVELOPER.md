# DownBallotR — Developer Guide

This document covers repo structure, architecture, how to add new states, and how
to run the test suite. See `README.md` for user-facing installation and usage.

---

## Repository layout

```
DownBallotR/
├── R/                              # R package source
│   ├── scrape_elections.R          # scrape_elections() — routing + dispatch
│   ├── scraper_helpers.R           # Internal .scrape_*() functions (one per source)
│   ├── db_query.R                  # db_list_sources(), db_list_states(), db_available_years()
│   ├── state_utils.R               # .normalize_state(), .STATE_ABBREV, .to_year(), etc.
│   ├── python_bind.R               # db_bind_python() — virtualenv + sys.path setup
│   ├── downballot_use_python.R     # downballot_use_python() helper
│   ├── downballot_install_python.R # downballot_install_python() — one-time setup
│   ├── downballot_python_status.R  # downballot_python_status() diagnostic
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
│   │   ├── discovery.py            # discover_northcarolina_results_zips() — NCSBE index
│   │   ├── aggregate.py            # precinct → county/state rollups
│   │   ├── normalize.py            # column name normalization
│   │   ├── models.py               # NC data models
│   │   ├── selection.py            # election filtering logic
│   │   ├── canonicalize.py         # race/office canonicalization
│   │   ├── election_type_rules.py
│   │   ├── constants.py
│   │   └── io_utils.py             # ZIP download helpers
│   │
│   ├── Ballotpedia/                # School board elections scraper
│   │   ├── school_board_elections.py  # SchoolBoardScraper
│   │   ├── ballotpedia_client.py
│   │   └── scrape_school_boards.py
│   │
│   ├── Georgia/                    # GA Secretary of State scraper
│   │   ├── pipeline.py             # GaElectionPipeline + get_ga_election_results()
│   │   ├── client.py               # GaElectionClient — Playwright browser automation
│   │   ├── parser.py               # parse_state_results() / parse_county_results()
│   │   ├── models.py               # GA data models / column lists
│   │   ├── inspect_vote_method.py  # Dev tool: inspect vote-method HTML after click
│   │   └── tests/
│   │       └── test_ga_smoke.py    # Smoke test (single year, state level)
│   │
│   └── Connecticut/                # CT CTEMS scraper
│       ├── pipeline.py             # CtElectionPipeline + get_ct_election_results()
│       ├── client.py               # CtPlaywrightClient — AngularJS SPA automation
│       ├── discovery.py            # parse_election_options() — election dropdown
│       ├── parser.py               # parse_statewide_results() / parse_town_results()
│       ├── models.py               # CtElectionInfo dataclass + date parsing
│       └── tests/
│           └── test_ct_smoke.py    # Smoke test (discovery + statewide + town)
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

Routing logic lives in `R/scrape_elections.R`; per-source argument shaping in `R/scraper_helpers.R`; state normalization in `R/state_utils.R`; all scraping logic lives in Python.
`reticulate::py_to_r()` converts the returned pandas DataFrames to R data frames.

### Five scraper backends

| Backend | States | Transport | Entry point |
|---|---|---|---|
| **Classic** (ElectionStats v1) | MA, NH, CO, ID, VT | `requests` | `StateHttpClient` |
| **V2** (ElectionStats v2 / React) | SC, NM, NY, VA | Playwright (headless Chromium) | `PlaywrightClient` |
| **NC** (NCSBE ZIP pipeline) | NC | HTTP ZIP download | `NcElectionPipeline` |
| **Ballotpedia** | All US states | `requests` | `SchoolBoardScraper` |
| **Georgia SOS** | GA | Playwright (headless Chromium) | `GaElectionPipeline` |
| **Connecticut CTEMS** | CT | Playwright (headless Chromium) | `CtElectionPipeline` |

### ElectionStats URL formats

Classic states (v1) use Rails-style path segments:
```
https://co.elstats2.civera.com/elections/search/year_from:2024/year_to:2024
```

V2 states (SC/NM/NY/VA) use the Civera React SPA with query-parameter URLs:
```
https://electionstats.sos.nm.gov/eng/contests?year_from=2024&year_to=2024&page=1
```

These require Playwright for rendering; the `url_style` field in `STATE_CONFIGS` controls
which format `StateHttpClient.build_search_url()` generates.

V2 states also expose a public CSV download API for county/precinct data that does
**not** require a browser:
```
{base_url}/api/download_contest/{election_id}_table.csv?split_party=false
```
The CSV contains all geographic levels (County, Precinct) tagged in the first column.
SC, NM, and VA have precinct data; NY does not.

### Cloudflare (NY)

`results.elections.ny.gov` sits behind Cloudflare bot protection. `PlaywrightClient`
uses a stealth browser context (realistic Chrome user-agent, viewport, locale,
`navigator.webdriver` patch) that allows it to pass the Cloudflare challenge.

### Georgia SOS — Angular virtual scroll

`results.sos.ga.gov` is a JavaScript-rendered Angular SPA. Two non-obvious
implementation details:

1. **Virtual scrolling**: The page renders only the panels visible in the viewport.
   `GaElectionClient._scroll_to_load_all()` scrolls to the bottom in a loop until
   the panel count stabilises (up to 30 rounds, 1.5 s settle between each). Without
   this, a 125-contest page silently returns only ~50 contests.

2. **Vote-method toggle**: Per-contest vote totals split by Advanced Voting /
   Election Day / Absentee by Mail / Provisional are only present in the HTML
   *after* clicking each panel's `button[role="checkbox"]` toggle. The client's
   `_click_all_vote_method_buttons()` clicks every toggle and waits for re-render;
   then `parser.py` parses the resulting `<table class="contest-table">`.
   Pass `include_vote_methods=True` to opt in (significantly slower).

---

## Python environment setup

```r
# One-time — installs a reticulate-managed Python and creates the downballotR virtualenv
reticulate::install_python()
downballot_install_python(python = reticulate::virtualenv_starter())

# Check status
downballot_python_status()
```

Key Python dependencies: `requests`, `lxml`, `pandas`, `playwright`.
Playwright Chromium is installed automatically by `downballot_install_python()`.
To install it manually if needed:
```bash
~/.virtualenvs/downballotR/bin/python -m playwright install chromium
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

### Georgia smoke test

```bash
cd inst/python

# Single year, state level (fast — skips county scraping)
python -m Georgia.tests.test_ga_smoke

# With vote-method breakdown
python -m Georgia.tests.test_ga_smoke --vote-methods

# Specific year
python -m Georgia.tests.test_ga_smoke --year 2022
```

### Connecticut smoke test

```bash
cd inst/python

# Discovery only — verify election dropdown loads (fast, ~10 s)
python -m Connecticut.tests.test_ct_smoke --discovery-only

# Single year — statewide + first county's towns
python -m Connecticut.tests.test_ct_smoke --year 2024

# Single year — statewide only, skip town scraping
python -m Connecticut.tests.test_ct_smoke --year 2024 --state-only

# Save rendered HTML to /tmp/ for selector debugging
python -m Connecticut.tests.test_ct_smoke --year 2024 --save-html
```

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

## Key data flow (Georgia SOS)

```
scrape_elections(state = "georgia", year_from = 2024, year_to = 2024)   [R]
  └─ .scrape_ga(year_from=2024, year_to=2024, level="all",
                include_vote_methods=FALSE)                               [R]
       └─ registry.scrape("georgia_results", ...)                        [Python]
            └─ _scrape_ga()                                              [Python]
                 └─ get_ga_election_results(year_from, year_to, ...)
                      └─ GaElectionPipeline.run()
                           ├─ _discover_elections(year)
                           │    → list of election slugs/URLs
                           ├─ _scrape_state(url)
                           │    ├─ GaElectionClient.get_election_page[_with_vote_methods]()
                           │    │    ├─ Playwright: navigate + _scroll_to_load_all()
                           │    │    └─ [optional] _click_all_vote_method_buttons()
                           │    └─ parser.parse_state_results(html)
                           │         → (state_df, vote_method_df, county_urls)
                           └─ _scrape_county(url)  [parallel, --county-workers]
                                ├─ GaElectionClient.get_county_page[_with_vote_methods]()
                                └─ parser.parse_county_results(html)
                                     → (county_df, vote_method_df)
```

Result dict keys: `"state"`, `"county"`, `"vote_method_state"`, `"vote_method_county"`
(last two only when `include_vote_methods=True`).

---

## Key data flow (Connecticut CTEMS)

```
scrape_elections(state = "CT", year_from = 2024, year_to = 2024)   [R]
  └─ .scrape_ct(year_from=2024, year_to=2024, level="all",
                max_workers=2)                                  [R]
       └─ registry.scrape("connecticut_results", ...)               [Python]
            └─ _scrape_ct()                                         [Python]
                 └─ get_ct_election_results(year_from, year_to, ...)
                      └─ CtElectionPipeline.run()
                           ├─ discover()
                           │    → CtPlaywrightClient.get_landing_page()
                           │    → parse_election_options(html)
                           │    → list[CtElectionInfo]
                           ├─ _scrape_state_summary(election)
                           │    → CtPlaywrightClient.get_statewide_results()
                           │    → parse_statewide_results(html)
                           │    → federal_df  (empty for non-federal elections)
                           ├─ _get_county_town_tree(election)
                           │    → CtPlaywrightClient.get_county_town_options()
                           │    → [(county, county_val, [(town, town_val), ...]), ...]
                           ├─ _scrape_county()  [parallel, --ct-town-workers]
                           │    → CtPlaywrightClient.get_all_towns_for_county()
                           │    → parse_town_results(html) × N towns
                           │    → list[town_df]
                           └─ _build_state_df(summary_df, town_df)
                                → federal rows from summary (or aggregated from towns)
                                → state/local rows aggregated from towns
                                → vote_pct recomputed, contest_outcome added
```

Result dict keys: `"state"`, `"town"`

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

### `level = "all"` (default)
Returns a named R list with `$state`, `$county`, and (for CO, MA, ID, SC, NM, VA)
`$precinct` data frames. NH, VT, and NY only return `$state` and `$county`; `$precinct`
is `NULL` for those states.

---

## Georgia output schema

### `state_df` / `county_df`

One row per candidate per election (state) or per candidate per county per election (county).

| Column | Description |
|---|---|
| `election_date` | Date of the election (from page header) |
| `election_name` | Human-readable election name (e.g. "2024 Nov General") |
| `election_slug` | URL slug (e.g. `2024NovGen`) |
| `result_status` | Reporting status (e.g. "100% Reporting") |
| `office` | Office name |
| `district` | District / jurisdiction |
| `vote_for` | Number of seats contested ("Vote for N") |
| `localities_reporting` | "X/Y" localities reporting string |
| `candidate` | Candidate name (cleaned — `(I)` and party suffix stripped) |
| `party` | Party abbreviation |
| `is_incumbent` | `True` if `(I)` appears in raw name; `False` otherwise |
| `is_winner` | Always `None` — winner marker absent from GA SOS HTML |
| `votes` | Vote total for the candidate |
| `pct` | Percentage of votes |
| `county` | County name (`county_df` only) |

### `vote_method_state_df` / `vote_method_county_df`

Same rows as above, with `votes` split by method:

| Column | Description |
|---|---|
| `votes_advanced` | Advanced Voting ballots |
| `votes_election_day` | Election Day ballots |
| `votes_absentee` | Absentee by Mail ballots |
| `votes_provisional` | Provisional ballots |
| `votes_total` | Sum of all methods |

---

## Connecticut output schema

### `state_df`

One row per candidate per office for the whole state. Federal races come
directly from the CTEMS statewide Summary page; State and Local races are
aggregated by summing town-level totals.

| Column | Description |
|---|---|
| `election_name` | Human-readable election name (e.g. "11/05/2024 -- November General Election") |
| `election_year` | Calendar year |
| `election_date` | ISO date string when parseable; `None` otherwise |
| `election_level` | `"Federal"`, `"State"`, or `"Local"` |
| `office` | Office name (includes district suffix when present) |
| `candidate` | Candidate name |
| `party` | Party name (e.g. `"Democratic"`, `"Republican"`) |
| `votes` | Total votes (integer) |
| `vote_pct` | Percentage of votes in contest (recomputed from town totals) |
| `contest_outcome` | `"Won"` or `"Lost"` (ties both receive `"Won"`); `None` if votes missing |

### `town_df`

One row per candidate per office per town.

| Column | Description |
|---|---|
| `election_name` | Human-readable election name |
| `election_year` | Calendar year |
| `election_date` | ISO date string when parseable; `None` otherwise |
| `county` | County name |
| `town` | Town name |
| `election_level` | `"Federal"`, `"State"`, or `"Local"` |
| `office` | Office name |
| `candidate` | Candidate name |
| `party` | Party name |
| `votes` | Votes in this town (integer) |
| `vote_pct` | Percentage as reported by CTEMS for this town |

---

## Useful one-liners

```r
# See all supported sources
db_list_sources()

# See all ElectionStats states
db_list_states("election_stats")

# Check data availability per state
db_available_years()
db_available_years(state = "South Carolina")
```

```python
# From inst/python — check which states exist and their scraping method
from ElectionStats.state_config import STATE_CONFIGS
for k, v in STATE_CONFIGS.items():
    print(k, v["scraping_method"], v["url_style"])
```
