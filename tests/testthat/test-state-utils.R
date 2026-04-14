# tests/testthat/test-state-utils.R
#
# Pure-R tests for state_utils.R helpers. No Python required.

# ── .normalize_state ──────────────────────────────────────────────────────────

test_that(".normalize_state: NULL returns NULL", {
  expect_null(DownBallotR:::.normalize_state(NULL))
})

test_that(".normalize_state: 2-letter abbreviations (any case)", {
  expect_equal(DownBallotR:::.normalize_state("VA"), "Virginia")
  expect_equal(DownBallotR:::.normalize_state("va"), "Virginia")
  expect_equal(DownBallotR:::.normalize_state("NC"), "North Carolina")
  expect_equal(DownBallotR:::.normalize_state("ct"), "Connecticut")
  expect_equal(DownBallotR:::.normalize_state("DC"), "District of Columbia")
})

test_that(".normalize_state: full names in any case", {
  expect_equal(DownBallotR:::.normalize_state("virginia"), "Virginia")
  expect_equal(DownBallotR:::.normalize_state("VIRGINIA"), "Virginia")
  expect_equal(DownBallotR:::.normalize_state("Virginia"), "Virginia")
})

test_that(".normalize_state: underscores and hyphens become spaces", {
  expect_equal(DownBallotR:::.normalize_state("north_carolina"), "North Carolina")
  expect_equal(DownBallotR:::.normalize_state("north-carolina"), "North Carolina")
  expect_equal(DownBallotR:::.normalize_state("new_mexico"), "New Mexico")
})

test_that(".normalize_state: leading/trailing whitespace stripped", {
  expect_equal(DownBallotR:::.normalize_state("  VA  "), "Virginia")
  expect_equal(DownBallotR:::.normalize_state(" virginia "), "Virginia")
})

# ── .state_to_es_key ──────────────────────────────────────────────────────────

test_that(".state_to_es_key: NULL returns NULL", {
  expect_null(DownBallotR:::.state_to_es_key(NULL))
})

test_that(".state_to_es_key: spaces become underscores, lowercase", {
  expect_equal(DownBallotR:::.state_to_es_key("Virginia"), "virginia")
  expect_equal(DownBallotR:::.state_to_es_key("North Carolina"), "north_carolina")
  expect_equal(DownBallotR:::.state_to_es_key("New Mexico"), "new_mexico")
})

# ── .to_year ──────────────────────────────────────────────────────────────────

test_that(".to_year: NULL returns NULL", {
  expect_null(DownBallotR:::.to_year(NULL))
})

test_that(".to_year: integer passthrough", {
  expect_equal(DownBallotR:::.to_year(2024L), 2024L)
})

test_that(".to_year: numeric coercion", {
  expect_equal(DownBallotR:::.to_year(2024), 2024L)
  expect_equal(DownBallotR:::.to_year(2024.0), 2024L)
})

test_that(".to_year: string coercion", {
  expect_equal(DownBallotR:::.to_year("2024"), 2024L)
})

test_that(".to_year: errors on non-numeric string", {
  expect_error(DownBallotR:::.to_year("abc"), "Cannot convert")
})

test_that(".to_year: errors on vector length > 1", {
  expect_error(DownBallotR:::.to_year(c(2020, 2024)), "single value")
})

# ── .stop_if_not_scalar ───────────────────────────────────────────────────────

test_that(".stop_if_not_scalar: scalars pass through invisibly", {
  expect_invisible(DownBallotR:::.stop_if_not_scalar("VA", "state"))
  expect_invisible(DownBallotR:::.stop_if_not_scalar(4L, "max_workers"))
})

test_that(".stop_if_not_scalar: NULL is allowed", {
  expect_invisible(DownBallotR:::.stop_if_not_scalar(NULL, "state"))
})

test_that(".stop_if_not_scalar: length > 1 errors", {
  expect_error(DownBallotR:::.stop_if_not_scalar(c("VA", "NC"), "state"), "single value")
  expect_error(DownBallotR:::.stop_if_not_scalar(1:3, "x"), "single value")
})

# ── .validate_max_workers ─────────────────────────────────────────────────────

test_that(".validate_max_workers: valid values pass through", {
  expect_equal(DownBallotR:::.validate_max_workers(1L), 1L)
  expect_equal(DownBallotR:::.validate_max_workers(4L), 4L)
  expect_equal(DownBallotR:::.validate_max_workers(2),  2L)
})

test_that(".validate_max_workers: caps at 4 with a message", {
  expect_message(
    result <- DownBallotR:::.validate_max_workers(8L),
    regexp = "capped at 4"
  )
  expect_equal(result, 4L)
})

test_that(".validate_max_workers: errors on zero or negative", {
  expect_error(DownBallotR:::.validate_max_workers(0L),  "positive integer")
  expect_error(DownBallotR:::.validate_max_workers(-1L), "positive integer")
})

test_that(".validate_max_workers: errors on non-numeric", {
  expect_error(DownBallotR:::.validate_max_workers("a"), "positive integer")
})

test_that(".validate_max_workers: errors on vector", {
  expect_error(DownBallotR:::.validate_max_workers(c(2L, 4L)), "single value")
})

# ── .check_state_recognized ───────────────────────────────────────────────────

test_that(".check_state_recognized: NULL passes silently", {
  expect_invisible(DownBallotR:::.check_state_recognized(NULL))
})

test_that(".check_state_recognized: all 50 states + DC are recognized", {
  known <- c(
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming", "District of Columbia"
  )
  for (s in known) {
    expect_invisible(DownBallotR:::.check_state_recognized(s))
  }
})

test_that(".check_state_recognized: unrecognized state errors with hint", {
  expect_error(
    DownBallotR:::.check_state_recognized("Virgina"),  # typo
    regexp = "Unrecognized state"
  )
})

test_that(".check_state_recognized: completely unknown state errors", {
  expect_error(
    DownBallotR:::.check_state_recognized("Narnia"),
    regexp = "Unrecognized state"
  )
})
