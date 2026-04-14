# tests/testthat/test-input-validation.R
#
# Tests for scrape_elections() input validation. All checks here fire BEFORE
# Python is touched (they raise errors in the R validation block), so no
# virtualenv is required and no real scraping occurs.

# ── parallel / include_vote_methods must be logical ───────────────────────────

test_that("scrape_elections: non-logical parallel errors", {
  expect_error(scrape_elections(state = "VA", parallel = "yes"), "'parallel' must be TRUE or FALSE")
  expect_error(scrape_elections(state = "VA", parallel = 1L),    "'parallel' must be TRUE or FALSE")
  expect_error(scrape_elections(state = "VA", parallel = NA),    "'parallel' must be TRUE or FALSE")
})

test_that("scrape_elections: non-logical include_vote_methods errors", {
  expect_error(
    scrape_elections(state = "GA", include_vote_methods = "yes"),
    "'include_vote_methods' must be TRUE or FALSE"
  )
  expect_error(
    scrape_elections(state = "GA", include_vote_methods = NA),
    "'include_vote_methods' must be TRUE or FALSE"
  )
})

# ── max_workers validation ────────────────────────────────────────────────────

test_that("scrape_elections: max_workers < 1 errors", {
  expect_error(scrape_elections(state = "GA", max_workers = 0L), "positive integer")
})

test_that("scrape_elections: max_workers = -1 errors", {
  expect_error(scrape_elections(state = "GA", max_workers = -1L), "positive integer")
})

test_that("scrape_elections: max_workers non-numeric errors", {
  expect_error(scrape_elections(state = "GA", max_workers = "a"), "positive integer")
})

# ── Year coercion and cross-checks ────────────────────────────────────────────

test_that("scrape_elections: non-coercible year_from errors", {
  expect_error(scrape_elections(state = "VA", year_from = "abc"), "Cannot convert")
})

test_that("scrape_elections: non-coercible year_to errors", {
  expect_error(scrape_elections(state = "VA", year_to = "xyz"), "Cannot convert")
})

test_that("scrape_elections: year_from > year_to errors", {
  expect_error(
    scrape_elections(state = "VA", year_from = 2024, year_to = 2020),
    "'year_from' \\(2024\\) cannot be greater than 'year_to' \\(2020\\)"
  )
})

test_that("scrape_elections: year vector errors (not scalar)", {
  expect_error(
    scrape_elections(state = "VA", year_from = c(2020, 2024)),
    "single value"
  )
})

# ── State validation ──────────────────────────────────────────────────────────

test_that("scrape_elections: unrecognized state errors", {
  expect_error(scrape_elections(state = "Narnia"), "Unrecognized state")
})

test_that("scrape_elections: typo in state name gives unrecognized error", {
  expect_error(scrape_elections(state = "Virgina"), "Unrecognized state")
})

test_that("scrape_elections: state vector errors (not scalar)", {
  expect_error(scrape_elections(state = c("VA", "NC")), "single value")
})

# ── year_from > year_to triggers before any Python call ──────────────────────
# These use recognized states + impossible year ranges to confirm the R-level
# cross-check fires for every routing path (NC, CT, GA, etc.)

test_that("scrape_elections: year cross-check fires for NC routing", {
  expect_error(
    scrape_elections(state = "NC", year_from = 2025, year_to = 2020),
    "year_from.*cannot be greater"
  )
})

test_that("scrape_elections: year cross-check fires for CT routing", {
  expect_error(
    scrape_elections(state = "CT", year_from = 2025, year_to = 2016),
    "year_from.*cannot be greater"
  )
})

test_that("scrape_elections: year cross-check fires for GA routing", {
  expect_error(
    scrape_elections(state = "GA", year_from = 2025, year_to = 2000),
    "year_from.*cannot be greater"
  )
})

test_that("scrape_elections: year cross-check fires for UT routing", {
  expect_error(
    scrape_elections(state = "UT", year_from = 2025, year_to = 2023),
    "year_from.*cannot be greater"
  )
})

test_that("scrape_elections: year cross-check fires for IN routing", {
  expect_error(
    scrape_elections(state = "IN", year_from = 2024, year_to = 2019),
    "year_from.*cannot be greater"
  )
})

test_that("scrape_elections: year cross-check fires for LA routing", {
  expect_error(
    scrape_elections(state = "LA", year_from = 2024, year_to = 2020),
    "year_from.*cannot be greater"
  )
})

test_that("scrape_elections: year cross-check fires for ElectionStats routing", {
  expect_error(
    scrape_elections(state = "VA", year_from = 2024, year_to = 2020),
    "year_from.*cannot be greater"
  )
})

# ── Invalid level for state-specific scrapers ─────────────────────────────────
# Each scraper only supports a subset of level values; passing the wrong one
# should produce an informative error BEFORE Python is called.

test_that("scrape_elections: level=precinct invalid for GA", {
  expect_error(
    scrape_elections(state = "GA", level = "precinct"),
    "not valid for"
  )
})

test_that("scrape_elections: level=precinct invalid for UT", {
  expect_error(
    scrape_elections(state = "UT", level = "precinct"),
    "not valid for"
  )
})

test_that("scrape_elections: level=precinct invalid for IN", {
  expect_error(
    scrape_elections(state = "IN", level = "precinct"),
    "not valid for"
  )
})

test_that("scrape_elections: level=precinct invalid for LA", {
  expect_error(
    scrape_elections(state = "LA", level = "precinct"),
    "not valid for"
  )
})

test_that("scrape_elections: level=town invalid for GA", {
  expect_error(
    scrape_elections(state = "GA", level = "town"),
    "not valid for"
  )
})

test_that("scrape_elections: level=town invalid for ElectionStats (VA)", {
  expect_error(
    scrape_elections(state = "VA", level = "town"),
    "not valid for"
  )
})

test_that("scrape_elections: level=parish invalid for GA", {
  expect_error(
    scrape_elections(state = "GA", level = "parish"),
    "not valid for"
  )
})

test_that("scrape_elections: level=parish invalid for NC", {
  expect_error(
    scrape_elections(state = "NC", level = "parish"),
    "not valid for"
  )
})

test_that("scrape_elections: level=county invalid for CT (only all/state/town)", {
  expect_error(
    scrape_elections(state = "CT", level = "county"),
    "not valid for"
  )
})

test_that("scrape_elections: level=precinct invalid for CT", {
  expect_error(
    scrape_elections(state = "CT", level = "precinct"),
    "not valid for"
  )
})

test_that("scrape_elections: level=parish invalid for CT", {
  expect_error(
    scrape_elections(state = "CT", level = "parish"),
    "not valid for"
  )
})

test_that("scrape_elections: level=town invalid for IN", {
  expect_error(
    scrape_elections(state = "IN", level = "town"),
    "not valid for"
  )
})

test_that("scrape_elections: level=parish invalid for IN", {
  expect_error(
    scrape_elections(state = "IN", level = "parish"),
    "not valid for"
  )
})

# ── Cross-parameter errors ────────────────────────────────────────────────────

test_that("scrape_elections: include_vote_methods=TRUE errors for non-GA/UT states", {
  expect_error(
    scrape_elections(state = "NC", include_vote_methods = TRUE),
    "only supported for Georgia and Utah"
  )
  expect_error(
    scrape_elections(state = "VA", include_vote_methods = TRUE),
    "only supported for Georgia and Utah"
  )
  expect_error(
    scrape_elections(state = "LA", include_vote_methods = TRUE),
    "only supported for Georgia and Utah"
  )
})

# ── parallel / max_workers inapplicable source errors ─────────────────────────

test_that("scrape_elections: parallel errors for non-ElectionStats sources", {
  expect_error(
    scrape_elections(state = "NC", parallel = FALSE),
    "'parallel' is only applicable for ElectionStats"
  )
  expect_error(
    scrape_elections(state = "GA", parallel = FALSE),
    "'parallel' is only applicable for ElectionStats"
  )
  expect_error(
    scrape_elections(state = "LA", parallel = FALSE),
    "'parallel' is only applicable for ElectionStats"
  )
})

test_that("scrape_elections: max_workers errors for sources without sub-unit parallelism", {
  expect_error(
    scrape_elections(state = "VA", max_workers = 2L),
    "'max_workers' is not applicable"
  )
  expect_error(
    scrape_elections(state = "NC", max_workers = 2L),
    "'max_workers' is not applicable"
  )
  expect_error(
    scrape_elections(state = "IN", max_workers = 2L),
    "'max_workers' is not applicable"
  )
})
