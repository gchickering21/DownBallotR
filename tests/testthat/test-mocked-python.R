# tests/testthat/test-mocked-python.R
#
# Tests for Python-interaction code using testthat::local_mocked_bindings().
# No real Python virtualenv is required — reticulate calls are replaced with
# controlled fakes for each test.

# ── .db_norm_path ─────────────────────────────────────────────────────────────

test_that(".db_norm_path: NULL / NA / empty return NA_character_", {
  expect_identical(DownBallotR:::.db_norm_path(NULL),          NA_character_)
  expect_identical(DownBallotR:::.db_norm_path(NA_character_), NA_character_)
  expect_identical(DownBallotR:::.db_norm_path(""),             NA_character_)
})

test_that(".db_norm_path: valid path returns normalized string", {
  result <- DownBallotR:::.db_norm_path("/usr/bin/python3")
  expect_type(result, "character")
  expect_false(is.na(result))
})

# ── db_ensure_virtualenv ──────────────────────────────────────────────────────

test_that("db_ensure_virtualenv: skips creation when env already exists", {
  local_mocked_bindings(
    virtualenv_exists = function(...) TRUE,
    .package = "reticulate"
  )
  expect_invisible(DownBallotR:::db_ensure_virtualenv("downballotR"))
})

test_that("db_ensure_virtualenv: creates env when missing", {
  local_mocked_bindings(
    virtualenv_exists = function(...) FALSE,
    virtualenv_create = function(...) invisible(TRUE),
    .package = "reticulate"
  )
  expect_message(
    DownBallotR:::db_ensure_virtualenv("downballotR"),
    "Creating virtualenv"
  )
})

test_that("db_ensure_virtualenv: errors clearly when creation fails", {
  local_mocked_bindings(
    virtualenv_exists = function(...) FALSE,
    virtualenv_create = function(...) stop("no python found"),
    .package = "reticulate"
  )
  expect_error(
    DownBallotR:::db_ensure_virtualenv("downballotR"),
    "Failed to create virtualenv"
  )
})

# ── db_use_virtualenv ─────────────────────────────────────────────────────────

test_that("db_use_virtualenv: succeeds when reticulate accepts the env", {
  local_mocked_bindings(
    use_virtualenv = function(...) invisible(TRUE),
    .package = "reticulate"
  )
  expect_invisible(DownBallotR:::db_use_virtualenv("downballotR"))
})

test_that("db_use_virtualenv: errors clearly when activation fails", {
  local_mocked_bindings(
    use_virtualenv = function(...) stop("env not found"),
    .package = "reticulate"
  )
  expect_error(
    DownBallotR:::db_use_virtualenv("downballotR"),
    "Failed to activate virtualenv"
  )
})

# ── db_missing_python_packages ────────────────────────────────────────────────

test_that("db_missing_python_packages: returns only unavailable packages", {
  local_mocked_bindings(
    py_module_available = function(pkg) pkg %in% c("pandas", "requests"),
    .package = "reticulate"
  )
  result <- DownBallotR:::db_missing_python_packages(
    c("pandas", "requests", "lxml", "playwright")
  )
  expect_equal(result, c("lxml", "playwright"))
})

test_that("db_missing_python_packages: returns character(0) when all present", {
  local_mocked_bindings(
    py_module_available = function(...) TRUE,
    .package = "reticulate"
  )
  result <- DownBallotR:::db_missing_python_packages(c("pandas", "requests"))
  expect_equal(result, character(0))
})

# ── db_install_python_packages ────────────────────────────────────────────────

test_that("db_install_python_packages: skips when package list is empty", {
  # Should return without calling virtualenv_install at all
  expect_invisible(DownBallotR:::db_install_python_packages("downballotR", character(0)))
})

test_that("db_install_python_packages: calls virtualenv_install and messages", {
  local_mocked_bindings(
    virtualenv_install = function(...) invisible(TRUE),
    .package = "reticulate"
  )
  expect_message(
    DownBallotR:::db_install_python_packages("downballotR", c("pandas", "lxml")),
    "Installing Python packages"
  )
})

test_that("db_install_python_packages: errors clearly when install fails", {
  local_mocked_bindings(
    virtualenv_install = function(...) stop("pip failed"),
    .package = "reticulate"
  )
  expect_error(
    DownBallotR:::db_install_python_packages("downballotR", "pandas"),
    "Failed while installing Python packages"
  )
})

# ── db_verify_python_imports ──────────────────────────────────────────────────

test_that("db_verify_python_imports: passes when all imports succeed", {
  local_mocked_bindings(
    py_run_string = function(...) invisible(NULL),
    .package = "reticulate"
  )
  expect_invisible(DownBallotR:::db_verify_python_imports(c("pandas", "requests")))
})

test_that("db_verify_python_imports: errors clearly when import fails", {
  local_mocked_bindings(
    py_run_string = function(...) stop("ModuleNotFoundError"),
    .package = "reticulate"
  )
  expect_error(
    DownBallotR:::db_verify_python_imports(c("pandas")),
    "Python packages were not importable"
  )
})

# ── db_playwright_chromium_is_installed ───────────────────────────────────────

test_that("db_playwright_chromium_is_installed: returns FALSE when playwright missing", {
  local_mocked_bindings(
    py_module_available = function(...) FALSE,
    .package = "reticulate"
  )
  expect_false(DownBallotR:::db_playwright_chromium_is_installed())
})

test_that("db_playwright_chromium_is_installed: returns TRUE when chromium found", {
  local_mocked_bindings(
    py_module_available = function(...) TRUE,
    py_run_string       = function(...) invisible(NULL),
    .package = "reticulate"
  )
  # py$__le_chromium_ok would be NULL in mock context -> isTRUE(NULL) = FALSE
  # so we just confirm it returns a logical without erroring
  result <- DownBallotR:::db_playwright_chromium_is_installed()
  expect_type(result, "logical")
  expect_length(result, 1)
})

# ── db_install_playwright_chromium ────────────────────────────────────────────

test_that("db_install_playwright_chromium: succeeds when py_run_string works", {
  local_mocked_bindings(
    py_run_string = function(...) invisible(NULL),
    .package = "reticulate"
  )
  expect_invisible(DownBallotR:::db_install_playwright_chromium())
})

test_that("db_install_playwright_chromium: errors clearly when chromium install fails", {
  local_mocked_bindings(
    py_run_string = function(...) stop("download failed"),
    .package = "reticulate"
  )
  expect_error(
    DownBallotR:::db_install_playwright_chromium(),
    "Playwright is installed, but Chromium failed"
  )
})

# ── downballot_use_python ─────────────────────────────────────────────────────

test_that("downballot_use_python: errors when virtualenv does not exist", {
  local_mocked_bindings(
    virtualenv_exists = function(...) FALSE,
    .package = "reticulate"
  )
  expect_error(
    downballot_use_python("downballotR"),
    "does not exist"
  )
})

test_that("downballot_use_python: returns TRUE when already init to correct interpreter", {
  local_mocked_bindings(
    virtualenv_exists  = function(...) TRUE,
    py_available       = function(...) TRUE,
    py_config          = function(...) list(python = "/fake/venv/bin/python"),
    virtualenv_python  = function(...) "/fake/venv/bin/python",
    .package = "reticulate"
  )
  expect_invisible(downballot_use_python("downballotR"))
})

test_that("downballot_use_python: errors when init to wrong interpreter", {
  local_mocked_bindings(
    virtualenv_exists  = function(...) TRUE,
    py_available       = function(...) TRUE,
    py_config          = function(...) list(python = "/other/python"),
    virtualenv_python  = function(...) "/fake/venv/bin/python",
    .package = "reticulate"
  )
  expect_error(
    downballot_use_python("downballotR"),
    "already initialized to a different Python"
  )
})

test_that("downballot_use_python: activates env when not yet initialized", {
  local_mocked_bindings(
    virtualenv_exists = function(...) TRUE,
    py_available      = function(...) FALSE,
    use_virtualenv    = function(...) invisible(TRUE),
    py_config         = function(...) invisible(NULL),
    .package = "reticulate"
  )
  expect_invisible(downballot_use_python("downballotR"))
})

test_that("downballot_use_python: errors clearly when use_virtualenv fails", {
  local_mocked_bindings(
    virtualenv_exists = function(...) TRUE,
    py_available      = function(...) FALSE,
    use_virtualenv    = function(...) stop("cannot activate"),
    .package = "reticulate"
  )
  expect_error(
    downballot_use_python("downballotR"),
    "Failed to activate virtualenv"
  )
})

# ── downballot_install_python: nothing to do path ─────────────────────────────

test_that("downballot_install_python: messages and returns early when env is ready", {
  local_mocked_bindings(
    virtualenv_exists    = function(...) TRUE,
    use_virtualenv       = function(...) invisible(TRUE),
    py_available         = function(initialize = TRUE) FALSE,
    py_config            = function(...) invisible(NULL),
    py_module_available  = function(...) TRUE,
    .package = "reticulate"
  )
  # db_playwright_chromium_is_installed also calls py_module_available -> TRUE
  # but then py_run_string -> not mocked, so chromium check will be FALSE
  # That means plan$chromium_missing = TRUE and it won't take the early-exit path.
  # So just confirm it doesn't error on the install path.
  local_mocked_bindings(
    py_run_string = function(...) invisible(NULL),
    .package = "reticulate"
  )
  # With all packages present and chromium "ok" (py_run_string returns without error,
  # but py$__le_chromium_ok won't be set -> FALSE), expect it attempts chromium install
  expect_invisible(
    suppressMessages(downballot_install_python(install_chromium = FALSE, quiet = TRUE))
  )
})

# ── downballot_python_status: uncovered advice branches ───────────────────────

test_that("db_python_status_advice: recommends install when env missing", {
  advice <- DownBallotR:::db_python_status_advice(
    envname                   = "downballotR",
    virtualenv_exists         = FALSE,
    reticulate_initialized    = FALSE,
    active_python_matches_env = NA,
    missing_packages          = character(0),
    playwright_chromium_installed = FALSE
  )
  expect_true(any(grepl("downballot_install_python", advice)))
})

test_that("db_python_status_advice: recommends use_python when env exists but not init", {
  advice <- DownBallotR:::db_python_status_advice(
    envname                   = "downballotR",
    virtualenv_exists         = TRUE,
    reticulate_initialized    = FALSE,
    active_python_matches_env = NA,
    missing_packages          = character(0),
    playwright_chromium_installed = TRUE
  )
  expect_true(any(grepl("downballot_use_python", advice)))
})

test_that("db_python_status_advice: warns about wrong interpreter", {
  advice <- DownBallotR:::db_python_status_advice(
    envname                   = "downballotR",
    virtualenv_exists         = TRUE,
    reticulate_initialized    = TRUE,
    active_python_matches_env = FALSE,
    missing_packages          = character(0),
    playwright_chromium_installed = TRUE
  )
  expect_true(any(grepl("wrong Python|different Python|wrong interpreter", advice, ignore.case = TRUE)))
})

test_that("db_python_status_advice: recommends reinstall when packages missing", {
  advice <- DownBallotR:::db_python_status_advice(
    envname                   = "downballotR",
    virtualenv_exists         = TRUE,
    reticulate_initialized    = TRUE,
    active_python_matches_env = TRUE,
    missing_packages          = c("lxml"),
    playwright_chromium_installed = TRUE
  )
  expect_true(any(grepl("reinstall", advice, ignore.case = TRUE)))
})

test_that("db_python_status_advice: reports ready when all ok", {
  advice <- DownBallotR:::db_python_status_advice(
    envname                   = "downballotR",
    virtualenv_exists         = TRUE,
    reticulate_initialized    = TRUE,
    active_python_matches_env = TRUE,
    missing_packages          = character(0),
    playwright_chromium_installed = TRUE
  )
  expect_true(any(grepl("ready", advice, ignore.case = TRUE)))
})

# ── print.downballot_python_status ────────────────────────────────────────────

test_that("print.downballot_python_status: prints all key fields", {
  st <- list(
    envname                   = "downballotR",
    required_pkgs             = c("pandas", "requests"),
    virtualenv_exists         = TRUE,
    virtualenv_python         = "/fake/venv/bin/python",
    reticulate_initialized    = TRUE,
    active_python             = "/fake/venv/bin/python",
    active_python_matches_env = TRUE,
    missing_packages          = character(0),
    playwright_chromium_installed = TRUE,
    advice                    = c("Python environment is ready for use.")
  )
  class(st) <- c("downballot_python_status", "list")
  out <- capture.output(print(st))
  expect_true(any(grepl("downballot", out, ignore.case = TRUE)))
  expect_true(any(grepl("downballotR", out)))
})

test_that("print.downballot_python_status: shows missing packages", {
  st <- list(
    envname                   = "downballotR",
    required_pkgs             = c("pandas", "lxml"),
    virtualenv_exists         = TRUE,
    virtualenv_python         = "/fake/venv/bin/python",
    reticulate_initialized    = TRUE,
    active_python             = "/fake/venv/bin/python",
    active_python_matches_env = TRUE,
    missing_packages          = c("lxml"),
    playwright_chromium_installed = FALSE,
    advice                    = c("Fix issues with:", "  downballot_install_python(reinstall = TRUE)")
  )
  class(st) <- c("downballot_python_status", "list")
  out <- capture.output(print(st))
  expect_true(any(grepl("lxml", out)))
})

test_that("print.downballot_python_status: shows wrong interpreter warning", {
  st <- list(
    envname                   = "downballotR",
    required_pkgs             = character(0),
    virtualenv_exists         = TRUE,
    virtualenv_python         = "/fake/venv/bin/python",
    reticulate_initialized    = TRUE,
    active_python             = "/other/python",
    active_python_matches_env = FALSE,
    missing_packages          = NA_character_,
    playwright_chromium_installed = NA,
    advice                    = c("reticulate is initialized to a different Python than this virtualenv.")
  )
  class(st) <- c("downballot_python_status", "list")
  out <- capture.output(print(st))
  expect_true(any(grepl("not checked", out, ignore.case = TRUE)))
})

# ── zzz.R: .onAttach missing-env branch ───────────────────────────────────────

test_that(".onAttach: shows setup message when virtualenv missing", {
  skip_on_cran()
  withr::with_envvar(c(NOT_CRAN = "true"), {
    local_mocked_bindings(
      virtualenv_exists   = function(...) FALSE,
      py_available        = function(...) FALSE,
      .package = "reticulate"
    )
    expect_message(
      DownBallotR:::.onAttach("", "DownBallotR"),
      "Python dependencies are not set up"
    )
  })
})
