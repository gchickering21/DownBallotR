# DownBallotR 0.1.0

* Initial CRAN submission.

## New functions

* `scrape_elections()` — single entry point for downloading election results.
  Routes automatically to the appropriate scraper based on state. Supports
  state, county, precinct, town, and parish level results depending on the
  jurisdiction. Covers dedicated scrapers for Connecticut (2016–present),
  Georgia (2000–present), Indiana (2019–present), Louisiana (1982–present),
  North Carolina (2000–present), and Utah (2023–present), plus a multi-state
  ElectionStats scraper for the remaining states.

* `db_available_years()` — returns a data frame of earliest and latest
  available years for each state and scraper source.

* `db_list_states()` — lists states supported by DownBallotR scrapers,
  optionally filtered by source.

* `db_list_sources()` — lists all registered Python scraper sources.

* `downballot_install_python()` — creates a `reticulate` virtual environment
  and installs required Python dependencies (`pandas`, `requests`, `lxml`,
  `bs4`, `playwright`) and Playwright Chromium.

* `downballot_use_python()` — pins `reticulate` to the DownBallotR virtual
  environment for the current R session.

* `downballot_python_status()` — reports the status of the Python environment,
  installed packages, and Playwright Chromium availability.
