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
| `"general"` (default) | VA, MA, CO, NH, SC, NM, NY, VT | ElectionStats | Candidate + county results; years vary by state |
| `"general"` (default) | `"NC"` / `"north_carolina"` | NC State Board of Elections | Precinct-level local election results, 2025–present |
| `"school_district"` | any state or `NULL` | Ballotpedia | School board elections, all US states, 2013–present |
| `"state_elections"` | any state | Ballotpedia | Federal, state, and local candidates, all US states, 2024–present |
| `"municipal_elections"` | any state or `NULL` | Ballotpedia | City, county, and mayoral elections, all US states, 2014–present |

```r
library(DownBallotR)
library(dplyr)

# General election results — routes automatically by state
scrape_elections(state = "virginia", year_from = 2023, year_to = 2023)

# North Carolina precinct results
scrape_elections(state = "NC", year_from = 2025, year_to = 2025)

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

`DownBallotR` uses Python internally via **reticulate**. **Python 3.10 or later
must be installed on your machine** before running `downballot_install_python()`.

If you do not have Python installed, download it from the official site:
<https://www.python.org/downloads/>

Once Python is available, the one-time setup command handles everything else
(creating an isolated virtual environment and installing all required packages):

```r
downballot_install_python()
```

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

**ElectionStats states** — VA, MA, CO, NH, SC, NM, NY, VT; candidate + county results:

- Source: [vignettes/election-stats.Rmd](vignettes/election-stats.Rmd)
- In R (after installing): `vignette("election-stats", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/election-stats.html>

**North Carolina** — NC State Board of Elections; precinct-level results:

- Source: [vignettes/north-carolina.Rmd](vignettes/north-carolina.Rmd)
- In R (after installing): `vignette("north-carolina", package = "DownBallotR")`
- Rendered HTML (pkgdown): <https://gchickering21.github.io/DownBallotR/articles/north-carolina.html>

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
