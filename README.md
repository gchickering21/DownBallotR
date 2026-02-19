# DownBallotR

<!-- badges: start -->
[![R CMD check](https://github.com/gchickering21/DownBallotR/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/gchickering21/DownBallotR/actions/workflows/R-CMD-check.yaml)
[![pkgdown](https://github.com/gchickering21/DownBallotR/actions/workflows/pkgdown.yaml/badge.svg)](https://github.com/gchickering21/DownBallotR/actions/workflows/pkgdown.yaml)
[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html)

<!-- badges: end -->

`DownBallotR` is an R package for downloading and standardizing local and
state election data. It wraps state-specific Python web scrapers via
**reticulate** and exposes a single, consistent R interface.

---

## Installation

```r
# From GitHub (development version)
install.packages("pak")
pak::pak("gchickering21/Downballot")
```

---

## Quick start

### 1. One-time Python setup

```r
library(DownBallotR)
downballot_install_python()   # downloads ~100–200 MB the first time
```

### 2. Activate Python each session

```r
library(DownBallotR)
downballot_use_python()
```

### 3. Scrape data

All data retrieval uses a single function:

```r
scrape_elections(source, ...)
```

```r
# School board district metadata — Ballotpedia, 2024, Alabama
scrape_elections("ballotpedia", year = 2024, state = "Alabama")

# Candidate-level results — ElectionStats, Virginia 2023
scrape_elections("election_stats",
                 state = "virginia", year_from = 2023, year_to = 2023,
                 level = "state")

# Both state and county levels at once (returns a named list)
res <- scrape_elections("election_stats",
                        state = "virginia", year_from = 2023, year_to = 2023)
res$state    # candidate-level data frame
res$county   # county vote breakdown data frame

# North Carolina local results
scrape_elections("nc_results", date = "2024-11-05")
```

---

## Data sources

| Source | `source =` | States / coverage | Notes |
|---|---|---|---|
| [ElectionStats](https://historical.elections.virginia.gov) | `"election_stats"` | VA, MA, CO, NH, SC, NM, NY | Classic states use HTTP requests; SC/NM/NY use Playwright |
| [Ballotpedia](https://ballotpedia.org) | `"ballotpedia"` | All US states, 2013–present | School board elections only |
| NC State Board of Elections | `"nc_results"` | North Carolina | Local election results |

Discover sources and supported states at runtime:

```r
db_list_sources()
db_list_states("election_stats")
```

---

## Documentation

| Resource | Link |
|---|---|
| Python setup & troubleshooting | [vignettes/python-setup.Rmd](vignettes/python-setup.Rmd) · `vignette("python-setup", package = "DownBallotR")` |
| Scraping data (all sources, examples) | [vignettes/scraping-data.Rmd](vignettes/scraping-data.Rmd) · `vignette("scraping-data", package = "DownBallotR")` |
| pkgdown site | <https://gchickering21.github.io/DownBallotR/> |

---

## Design notes

- `DownBallotR` intentionally does **not** auto-install Python dependencies on
  `library(DownBallotR)` — this avoids unexpected downloads and ensures
  predictable behavior
- Python is only initialized when explicitly requested by the user
- All scrapers include built-in polite delays between requests
