# tests/testthat/test-install-helpers.R
#
# Tests for pure-R helpers in downballot_install_python.R. No Python required.

test_that("db_required_python_packages returns expected pip-installable packages", {
  pkgs <- DownBallotR:::db_required_python_packages()
  expect_type(pkgs, "character")
  expect_true(length(pkgs) > 0)

  # These must be present
  expect_true("pandas"     %in% pkgs)
  expect_true("requests"   %in% pkgs)
  expect_true("lxml"       %in% pkgs)
  expect_true("playwright" %in% pkgs)

  # stdlib modules must NOT be in the pip list
  expect_false("datetime"   %in% pkgs)
  expect_false("re"         %in% pkgs)
  expect_false("dataclasses" %in% pkgs)
})

test_that("db_missing_python_packages with reinstall=TRUE returns all packages", {
  pkgs <- c("pandas", "requests", "lxml")
  result <- DownBallotR:::db_missing_python_packages(pkgs, reinstall = TRUE)
  expect_equal(result, pkgs)
})

test_that("db_missing_python_packages with empty input returns character(0)", {
  result <- DownBallotR:::db_missing_python_packages(character(0))
  expect_equal(result, character(0))
})

test_that("db_install_plan with reinstall=TRUE marks everything as missing", {
  pkgs <- DownBallotR:::db_required_python_packages()
  plan <- DownBallotR:::db_install_plan(pkgs, reinstall = TRUE, install_chromium = TRUE)
  expect_equal(plan$missing_pkgs, pkgs)
  expect_true(plan$chromium_missing)
})

test_that("db_install_plan with reinstall=FALSE and no Python: all packages missing", {
  # Without Python initialized, py_module_available returns FALSE for all
  skip_if(
    isTRUE(tryCatch(reticulate::py_available(initialize = FALSE), error = function(e) FALSE)),
    "Python already initialized; skipping cold-start check"
  )
  pkgs <- DownBallotR:::db_required_python_packages()
  plan <- DownBallotR:::db_install_plan(pkgs, reinstall = FALSE, install_chromium = FALSE)
  expect_type(plan$missing_pkgs, "character")
})
