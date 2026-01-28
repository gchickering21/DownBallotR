testthat::test_that("optional: env can be activated and modules visible", {
  # Only run when user opts in (local dev), never by default.
  testthat::skip_if_not(isTRUE(Sys.getenv("DOWNBALLOT_TEST_PYTHON", "false") == "true"))
  
  # If the env doesn't exist, skip (don't try to install in tests)
  if (!isTRUE(reticulate::virtualenv_exists("downballotR"))) {
    testthat::skip("downballotR virtualenv not found; run downballot_install_python() locally first")
  }
  
  # Use the env (should pin + initialize)
  downballot_use_python("downballotR")
  
  st <- downballot_python_status(envname = "downballotR", quiet = TRUE)
  
  testthat::expect_true(isTRUE(st$reticulate_initialized))
  testthat::expect_true(isTRUE(st$virtualenv_exists))
  
  # Only assert missing_packages == 0 if your status checks modules when initialized
  testthat::expect_length(st$missing_packages, 0)
  
  # Chromium check can be slow; assert only that it returns a scalar logical
  testthat::expect_true(is.logical(st$playwright_chromium_installed) &&
                          length(st$playwright_chromium_installed) == 1)
})
