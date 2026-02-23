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
| `"general"` (default) | any ElectionStats state | ElectionStats | VA, MA, CO, NH, SC, NM, NY — candidate + county results |
| `"general"` (default) | `"NC"` / `"north_carolina"` | NC State Board of Elections | North Carolina local election results |
| `"school_district"` | any state or `NULL` | Ballotpedia | All US states, 2013–present — school board elections |

```r
# General election results — routes automatically by state
scrape_elections(state = "virginia", year_from = 2023, year_to = 2023)

# North Carolina results by year range
scrape_elections(state = "NC", year_from = 2022, year_to = 2024)

# School board elections — Ballotpedia
scrape_elections(state = "Alabama", office = "school_district", year = 2024)
```

---

## Vignettes

📘 **Python setup** — environment installation, session activation, and troubleshooting:

- 📄 Source: [vignettes/python-setup.Rmd](vignettes/python-setup.Rmd)
- 🧭 In R (after installing): `vignette("python-setup", package = "DownBallotR")`
- 🌐 Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/python-setup.html>

📊 **Scraping data** — all sources, arguments, and worked examples:

- 📄 Source: [vignettes/scraping-data.Rmd](vignettes/scraping-data.Rmd)
- 🧭 In R (after installing): `vignette("scraping-data", package = "DownBallotR")`
- 🌐 Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/scraping-data.html>
