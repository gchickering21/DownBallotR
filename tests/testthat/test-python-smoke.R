testthat::test_that("python status returns expected structure and class", {
  st <- downballot_python_status(quiet = TRUE)

  testthat::expect_true(is.list(st))
  testthat::expect_s3_class(st, "downballot_python_status")

  # Required fields (keep this list short + stable)
  required <- c(
    "envname",
    "virtualenv_exists",
    "virtualenv_python",
    "reticulate_initialized",
    "active_python",
    "missing_packages",
    "playwright_chromium_installed",
    "advice"
  )
  testthat::expect_true(all(required %in% names(st)))

  # Types / sanity
  testthat::expect_true(is.character(st$envname) && length(st$envname) == 1)
  testthat::expect_true(is.logical(st$virtualenv_exists) && length(st$virtualenv_exists) == 1)
  testthat::expect_true(is.logical(st$reticulate_initialized) && length(st$reticulate_initialized) == 1)
  testthat::expect_true(is.character(st$missing_packages))
})

testthat::test_that("print method runs without error", {
  st <- downballot_python_status(quiet = TRUE)
  testthat::expect_output(print(st), regexp = "downballot", fixed = FALSE)
})
