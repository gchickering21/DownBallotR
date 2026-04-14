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

## Overview

`DownBallotR` requires a one-time Python setup and exposes all data retrieval
through a single function, `scrape_elections()`. The appropriate backend scraper
is selected **automatically** based on the `state` and `office` arguments — no
need to specify a source by name.

| `office` | `state` | Scraper | Coverage |
|---|---|---|---|
| `"general"` (default) | VA, MA, CO, NH, ID, SC, NM, NY, VT | ElectionStats | Candidate + county results (+ precinct for CO, MA, ID, SC, NM, VA); years vary by state |
| `"general"` (default) | `"NC"` / `"north_carolina"` | NC State Board of Elections | Precinct-level local election results, 2000–present |
| `"general"` (default) | `"CT"` / `"connecticut"` | Connecticut CTEMS | Statewide + town results, 2016–present |
| `"general"` (default) | `"GA"` / `"georgia"` | Georgia Secretary of State | Statewide + county results, 2000–present |
| `"general"` (default) | `"UT"` / `"utah"` | Utah elections site | Statewide + county results, 2023–present |
| `"general"` (default) | `"IN"` / `"indiana"` | Indiana voters portal | Statewide + county General Election results, 2019–present |
| `"general"` (default) | `"LA"` / `"louisiana"` | Louisiana Secretary of State | Statewide + parish results, 1982–present |
| `"school_district"` | any state or `NULL` | Ballotpedia | School board elections, all US states, 2013–present |
| `"state_elections"` | any state | Ballotpedia | Federal, state, and local candidates, all US states, 2024–present |
| `"municipal_elections"` | any state or `NULL` | Ballotpedia | City, county, and mayoral elections, all US states, 2014–present |

```r
library(DownBallotR)
library(dplyr)

# General election results — routes automatically by state
scrape_elections(state = "virginia", year_from = 2023, year_to = 2023)

# Precinct-level results (CO, MA, ID; also SC, NM, VA via CSV API)
scrape_elections(state = "colorado", year_from = 2022, year_to = 2022, level = "precinct")

# North Carolina precinct results
scrape_elections(state = "NC", year_from = 2025, year_to = 2025)

# Indiana General Election results (statewide + county)
scrape_elections(state = "IN", year_from = 2024, year_to = 2024)

# Louisiana statewide + parish results
scrape_elections(state = "LA", year_from = 2024, year_to = 2024)

# School board elections — all US states (Ballotpedia)
scrape_elections(office = "school_district", year = 2024)

# State + federal + local candidate listings (Ballotpedia)
scrape_elections(state = "Maine", office = "state_elections", year = 2024)

# Municipal election results with tidyverse filtering
scrape_elections(
  office    = "municipal_elections",
  year      = 2022,
  state     = "Texas",
  mode      = "results"
) %>%
  filter(is_winner, office == "Mayor") %>%
  select(location, state, candidate, party, pct, votes)
```

---

## Python requirement

`DownBallotR` uses Python internally via **reticulate**. **You do not need to
install Python yourself.** The two-line setup below works on Windows, macOS,
and Linux and handles everything automatically:

```r
reticulate::install_python()
downballot_install_python(python = reticulate::virtualenv_starter())
```

`reticulate::install_python()` downloads a standalone Python (any version 3.10+
works; pin a specific one with e.g. `version = "3.12"` if needed).
`virtualenv_starter()` then selects the best available Python for creating the
virtual environment — no hardcoded version required.

This creates an isolated virtual environment, installs all required packages,
and downloads Playwright Chromium (~100–200MB, first time only).

> If you already have Python 3.10+ installed and working, you can try
> `downballot_install_python()` without arguments. Fall back to the
> `reticulate::install_python()` approach above if virtualenv creation fails.

---

## About the data

`DownBallotR` retrieves election results live from official and semi-official
sources at the time of your request. No data is bundled with the package or
hosted by the maintainers.

**Coverage:** 15+ US states across 9 distinct sources (ElectionStats, state
election portals for NC, CT, GA, UT, IN, LA, and Ballotpedia for school board,
state, and municipal elections). Historical depth varies from 1789 (Vermont,
Virginia via ElectionStats) to 2019–present (Indiana).

**What the data is:** Vote totals by candidate and contest at the statewide,
county/parish/town, or precinct level, depending on source and the `level`
argument. Results are returned as-collected; party labels, contest names, and
geographic identifiers are not normalized to a common schema.

**What the data is not:** Certified results. Data should be verified against
the original source before publication or high-stakes use. Cite the source
website — not this package — when reporting specific election results.

Full documentation of data composition, collection process, limitations, and
responsible use is in the
[Datasheet](https://gchickering21.github.io/DownBallotR/articles/datasheet.html).

---

## Vignettes

**Python setup** — environment installation, session activation, and troubleshooting:

- Source: [vignettes/python-setup.Rmd](vignettes/python-setup.Rmd)
- In R (after installing): `vignette("python-setup", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/python-setup.html>

**Scraping data** — entry point overview, routing rules, data availability, and quick-reference table:

- Source: [vignettes/scraping-data.Rmd](vignettes/scraping-data.Rmd)
- In R (after installing): `vignette("scraping-data", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/scraping-data.html>

**ElectionStats states** — VA, MA, CO, NH, ID, SC, NM, NY, VT; candidate + county results:

- Source: [vignettes/election-stats.Rmd](vignettes/election-stats.Rmd)
- In R (after installing): `vignette("election-stats", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/election-stats.html>

**North Carolina** — NC State Board of Elections; precinct-level results:

- Source: [vignettes/north-carolina.Rmd](vignettes/north-carolina.Rmd)
- In R (after installing): `vignette("north-carolina", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/north-carolina.html>

**Connecticut** — CT CTEMS; statewide + town results, 2016–present:

- Source: [vignettes/connecticut.Rmd](vignettes/connecticut.Rmd)
- In R (after installing): `vignette("connecticut", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/connecticut.html>

**Georgia** — GA Secretary of State; statewide + county results, 2000–present:

- Source: [vignettes/georgia.Rmd](vignettes/georgia.Rmd)
- In R (after installing): `vignette("georgia", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/georgia.html>

**Utah** — Utah elections site; statewide + county results, 2023–present:

- Source: [vignettes/utah.Rmd](vignettes/utah.Rmd)
- In R (after installing): `vignette("utah", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/utah.html>

**Indiana** — Indiana voters portal; statewide + county General Election results, 2019–present:

- Source: [vignettes/indiana.Rmd](vignettes/indiana.Rmd)
- In R (after installing): `vignette("indiana", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/indiana.html>

**Louisiana** — Louisiana Secretary of State; statewide + parish results, 1982–present:

- Source: [vignettes/louisiana.Rmd](vignettes/louisiana.Rmd)
- In R (after installing): `vignette("louisiana", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/louisiana.html>

**School district elections** — Ballotpedia school board data, all US states, 2013–present:

- Source: [vignettes/school-district-elections.Rmd](vignettes/school-district-elections.Rmd)
- In R (after installing): `vignette("school-district-elections", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/school-district-elections.html>

**State elections** — Ballotpedia federal/state/local candidates, all US states, 2024–present:

- Source: [vignettes/state-elections.Rmd](vignettes/state-elections.Rmd)
- In R (after installing): `vignette("state-elections", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/state-elections.html>

**Municipal elections** — Ballotpedia city, county, and mayoral races, all US states, 2014–present:

- Source: [vignettes/municipal-elections.Rmd](vignettes/municipal-elections.Rmd)
- In R (after installing): `vignette("municipal-elections", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/municipal-elections.html>
