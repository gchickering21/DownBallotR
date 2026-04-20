# DownBallotR

<!-- badges: start -->
[![R-CMD-check](https://github.com/gchickering21/DownBallotR/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/gchickering21/DownBallotR/actions/workflows/R-CMD-check.yaml)
[![pkgdown](https://github.com/gchickering21/DownBallotR/actions/workflows/pkgdown.yaml/badge.svg)](https://github.com/gchickering21/DownBallotR/actions/workflows/pkgdown.yaml)
[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html)
<!-- badges: end -->
---

`DownBallotR` is an R package for downloading and standardizing federal, state, and local election results data across U.S. jurisdictions. It provides a consistent interface for accessing election results from official state and local sources, which are often published in different formats and structures. The package harmonizes these data into a common, structured format, allowing users to more easily analyze and compare election results across states, offices, and levels of government. It is designed to reduce the time and effort required to collect and clean election data, enabling researchers, analysts, and practitioners to focus on analysis rather than data acquisition.

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
is selected **automatically** based on the `state` and `level` arguments — no
need to specify a source by name.

| `state` | Scraper | Coverage |
|---|---|---|
| VA, MA, CO, NH, ID, SC, NM, NY, VT | ElectionStats | Candidate + county results (+ precinct for CO, MA, ID, SC, NM, VA); years vary by state |
| `"NC"` / `"north_carolina"` | NC State Board of Elections | Precinct-level local election results, 2000–present |
| `"CT"` / `"connecticut"` | Connecticut CTEMS | Statewide + town results, 2016–present |
| `"GA"` / `"georgia"` | Georgia Secretary of State | Statewide + county + precinct results, 2000–present |
| `"UT"` / `"utah"` | Utah elections site | Statewide + county + precinct results, 2023–present |
| `"IN"` / `"indiana"` | Indiana voters portal | Statewide + county General Election results, 2019–present |
| `"LA"` / `"louisiana"` | Louisiana Secretary of State | Statewide + parish results, 1982–present |

```r
library(DownBallotR)
library(dplyr)

# General election results — routes automatically by state
scrape_elections(state = "virginia", year_from = 2023, year_to = 2023)

# Precinct-level results (CO, MA, ID; also SC, NM, VA via CSV API)
scrape_elections(state = "colorado", year_from = 2022, year_to = 2022, level = "precinct")

# North Carolina precinct results
scrape_elections(state = "NC", year_from = 2025, year_to = 2025)

# Georgia statewide + county + precinct results
scrape_elections(state = "GA", year_from = 2024, year_to = 2024)

# Utah statewide + county + precinct results
scrape_elections(state = "UT", year_from = 2024, year_to = 2024)

# Indiana General Election results (statewide + county)
scrape_elections(state = "IN", year_from = 2024, year_to = 2024)

# Louisiana statewide + parish results
scrape_elections(state = "LA", year_from = 2024, year_to = 2024)

# Summarize a results data frame — auto-detects state, counts elections,
# candidates, and offices broken down by Federal / State / Local
ma_results <- scrape_elections(state = " MA", year_from = 2024, year_to = 2024, level = "state")
summarize_results(ma_results)
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

`DownBallotR` retrieves election results live from official at the time of your request. 
No data is bundled with the package or hosted by the maintainers. **Note:** We are currently working on creating a repository of validated datasets to accompany this package so users in the future do not need to run requests to download and validate the data themselves. 

**Current Coverage:** 15 US states. Historical depth varies from 1789
(Vermont, Virginia) to 2019(Indiana) to the present.

**What the data is:** Vote totals and counts by candidate and contest at the statewide,
county/parish/town, or precinct level, depending on source and the `level`
argument. Some states (such as Georgia, North Carolina) contain voting methods counts as well. 

**What the data is not:** Certified results. Data should be verified against
the original source before publication or high-stakes use.

Full documentation of data composition, collection process, limitations, and
responsible use is in the
[Datasheet](https://gchickering21.github.io/DownBallotR/articles/datasheet.html).

---

## Vignettes

These four vignettes are bundled with the package and accessible offline after installing:

**Datasheet** — overview of the package’s purpose, data sources, structure, and ethical considerations:

- In R: `vignette("datasheet", package = "DownBallotR")`
- Online: <https://gchickering21.github.io/DownBallotR/articles/datasheet.html>

**Python setup** — environment installation, session activation, and troubleshooting:

- In R: `vignette("python-setup", package = "DownBallotR")`
- Online: <https://gchickering21.github.io/DownBallotR/articles/python-setup.html>

**Data dictionary** — all columns returned across all states and sources:

- In R: `vignette("data-dictionary", package = "DownBallotR")`
- Online: <https://gchickering21.github.io/DownBallotR/articles/data-dictionary.html>

**Scraping data** — entry point overview, routing rules, and data availability:

- In R: `vignette("scraping-data", package = "DownBallotR")`
- Online: <https://gchickering21.github.io/DownBallotR/articles/scraping-data.html>

## Scraper articles

Detailed per-state documentation is available on the package website:

- [ElectionStats states](https://gchickering21.github.io/DownBallotR/articles/election-stats.html) — VA, MA, CO, NH, ID, SC, NM, NY, VT
- [North Carolina](https://gchickering21.github.io/DownBallotR/articles/north-carolina.html) — precinct-level results, 2000–present
- [Connecticut](https://gchickering21.github.io/DownBallotR/articles/connecticut.html) — statewide + town results, 2016–present
- [Georgia](https://gchickering21.github.io/DownBallotR/articles/georgia.html) — statewide + county + precinct results, 2000–present
- [Utah](https://gchickering21.github.io/DownBallotR/articles/utah.html) — statewide + county + precinct results, 2023–present
- [Indiana](https://gchickering21.github.io/DownBallotR/articles/indiana.html) — statewide + county General Election results, 2019–present
- [Louisiana](https://gchickering21.github.io/DownBallotR/articles/louisiana.html) — statewide + parish results, 1982–present

