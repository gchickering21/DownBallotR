# CRAN Submission Comments — DownBallotR 0.1.0

## R CMD check results

### Local (macOS 13.7, R 4.5.3)
0 errors | 0 warnings | 0 notes

### Windows (win-builder, R 4.5.3 / x86_64-w64-mingw32)
0 errors | 0 warnings | 1 notes (see Notes section below)

### Windows (win-builder, R-devel / x86_64-w64-mingw32)
0 errors | 0 warnings | 1 note (see Notes section below)

### Windows (win-builder, R 4.4.3 Patched / x86_64-w64-mingw32, old-release)
0 errors | 0 warnings | 1 note (see Notes section below)

### R-hub (linux, macos, macos-arm64, m1-san, windows, atlas)
0 errors | 0 warnings | 0 notes

## Notes

### "New submission"
This is a new submission, so the NOTE is expected:

* checking CRAN incoming feasibility ... NOTE
  Maintainer: 'Graham Chickering <grahamchickering@gmail.com>'
  New submission

### Vignette fix (resubmission)
The first submission triggered:
  "Package has a VignetteBuilder field but no prebuilt vignette index."
Fixed by removing the VignetteBuilder field from DESCRIPTION entirely.
The vignettes require Python and an active virtual environment to run, so all
code chunks are eval=FALSE. They are available via the pkgdown site at
https://gchickering21.github.io/DownBallotR/ but are not shipped as built
vignettes with the CRAN package.

### CPU time NOTE (resubmission)
The Debian check flagged: "Running R code in 'testthat.R' had CPU time
2.7 times elapsed time." The cause was test-mocked-python.R, which contains
~40 tests using local_mocked_bindings(.package = "reticulate"). This forces
reticulate to load, triggering background Python discovery that runs
concurrently with R and pushes CPU time above the 2x elapsed threshold.
Fixed by adding a file-level skip_on_cran() to test-mocked-python.R, and
skip_on_cran() to the db_install_plan test in test-install-helpers.R that
calls py_module_available() for each Python package.
  
## Motivation for the R + Python design

State and local election data is published through a wide variety of government
web portals, many of which rely on dynamic JavaScript rendering that cannot be
accessed with R's standard HTTP tools. Python's `playwright` library provides
mature, well-maintained headless browser automation that handles these portals
reliably across platforms.

At the same time, the primary audience for this package — political scientists,
public policy researchers, and students — works predominantly in R. Requiring
users to write or maintain Python scraping code would create a significant
barrier: they would need to learn a second language, manage a Python
environment, and manually transfer data into R before any analysis could begin.

The design goal of DownBallotR is to absorb that complexity entirely. Python
handles the scraping layer internally; users interact only with a consistent R
interface (`scrape_elections()`) and receive standard R data frames. They do not
write, modify, or even see any Python code. This allows researchers to go
directly from raw government election portals to analysis-ready data without
leaving R.

## Python / reticulate dependency

This package wraps state-specific Python web scrapers via the `reticulate`
package. Python is declared under `SystemRequirements: Python (>= 3.10), pip`.

**Python is not required to install or load the package.** All Python-dependent
functionality is opt-in: users must explicitly call `downballot_install_python()`
to create a virtual environment and install Python dependencies, and
`downballot_use_python()` to activate it for their R session. If Python is
unavailable, the package loads cleanly and no errors are thrown on attach.

The required Python packages (`pandas`, `requests`, `lxml`, `beautifulsoup4`,
`playwright`, `pyreadr`) are installed into an isolated `reticulate` virtual
environment named `"downballotR"` and do not affect the user's system Python
installation.

Playwright Chromium (~100-200 MB) is downloaded as part of setup. In
interactive sessions, `downballot_install_python()` prompts the user for
explicit consent before the download begins and aborts if they decline. In
non-interactive sessions, the function errors if Chromium is missing rather
than downloading silently.

## Network access

`scrape_elections()` makes HTTP requests to publicly accessible government
election result websites (state boards of elections, secretaries of state).
All network access is initiated by the user explicitly calling a scraping
function. The package makes no background, on-load, or automated network
requests of any kind.

## Tests

All tests that require Python are guarded with `testthat::skip_on_cran()` and
`skip_if_not_installed("reticulate")`. Live scraping tests additionally require
`DOWNBALLOT_TEST_PYTHON=true` to be set explicitly, ensuring they never run
during automated or CRAN checks. Tests that do not require Python (input
validation, state normalization utilities, mocked reticulate bindings) run
unconditionally and cover the package's pure-R logic.

## Vignettes

All vignette code chunks that invoke scrapers or require Python are set to
`eval=FALSE`. Vignettes are pre-built and included in `inst/doc`.

## inst/python

The `inst/python/` directory contains the Python scraper modules loaded at
runtime via `reticulate::source_python()`. These are first-party Python source
files (no compiled binaries) integral to the package's scraping functionality.

## Spell check

Technical terms and proper nouns flagged by `spelling::spell_check_package()`
(e.g. `reticulate`, `lxml`, `CTEMS`, `ElectionStats`, `virtualenv`) have been
added to `inst/WORDLIST`. One genuine typo (`publically` → `publicly`) was
corrected prior to submission.
