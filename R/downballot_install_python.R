# ---- Internal helpers --------------------------------------------------------

#' @keywords internal
db_required_python_packages <- function() {
  c("pandas", "requests", "lxml", "playwright")
}

#' @keywords internal
db_get_venv_python <- function(envname) {
  reticulate::virtualenv_python(envname)
}

#' @keywords internal
#' @keywords internal
db_stop_if_python_initialized_to_other <- function(envname) {
  # IMPORTANT: Don't call py_config() unless Python is already initialized.
  # py_config() can initialize reticulate and cause it to auto-select (e.g., uv).
  already_init <- isTRUE(tryCatch(reticulate::py_available(initialize = FALSE), error = function(e) FALSE))
  if (!already_init) {
    return(invisible(TRUE))
  }

  cfg <- tryCatch(reticulate::py_config(), error = function(e) NULL)

  # If config is unavailable, treat as not initialized
  if (is.null(cfg) || is.null(cfg$python) || !nzchar(cfg$python)) {
    return(invisible(TRUE))
  }

  wanted_raw <- tryCatch(db_get_venv_python(envname), error = function(e) NA_character_)
  wanted <- tryCatch(normalizePath(wanted_raw, winslash = "/"), error = function(e) NA_character_)
  current <- tryCatch(normalizePath(cfg$python, winslash = "/"), error = function(e) cfg$python)

  if (!is.na(wanted) && !identical(current, wanted)) {
    stop(
      "reticulate is already initialized to a different Python interpreter.\n",
      "  current: ", cfg$python, "\n",
      "  wanted:  ", wanted_raw, "\n\n",
      "Fix: Restart your R session, then run:\n",
      "  downballot_use_python('", envname, "')\n",
      "  downballot_install_python('", envname, "')\n",
      call. = FALSE
    )
  }

  invisible(TRUE)
}


#' @keywords internal
db_ensure_virtualenv <- function(envname, python = NULL) {
  if (isTRUE(tryCatch(reticulate::virtualenv_exists(envname), error = function(e) FALSE))) {
    return(invisible(TRUE))
  }
  
  message("Creating virtualenv '", envname, "' ...")
  tryCatch(
    reticulate::virtualenv_create(envname = envname, python = python),
    error = function(e) {
      stop(
        "Failed to create virtualenv '", envname, "'.\n",
        "Original error: ", conditionMessage(e),
        call. = FALSE
      )
    }
  )
  
  invisible(TRUE)
}

#' @keywords internal
db_use_virtualenv <- function(envname) {
  tryCatch(
    reticulate::use_virtualenv(envname, required = TRUE),
    error = function(e) {
      stop(
        "Failed to activate virtualenv '", envname, "'.\n",
        "Original error: ", conditionMessage(e),
        call. = FALSE
      )
    }
  )
  
  invisible(TRUE)
}

#' @keywords internal
db_force_reticulate_init <- function() {
  # Version-safe: py_config() exists across reticulate versions and will
  # initialize Python to the selected interpreter if not already initialized.
  tryCatch(reticulate::py_config(), error = function(e) NULL)
  invisible(TRUE)
}

#' @keywords internal
db_missing_python_packages <- function(pkgs, reinstall = FALSE) {
  if (length(pkgs) == 0) {
    return(character(0))
  }
  if (isTRUE(reinstall)) {
    return(pkgs)
  }
  
  ok <- vapply(
    pkgs,
    FUN = function(pkg) {
      tryCatch(reticulate::py_module_available(pkg), error = function(e) FALSE)
    },
    FUN.VALUE = logical(1)
  )
  
  pkgs[!ok]
}

#' @keywords internal
db_install_python_packages <- function(envname, packages, reinstall = FALSE) {
  if (length(packages) == 0) return(invisible(TRUE))
  
  message("Installing Python packages into '", envname, "': ", paste(packages, collapse = ", "))
  tryCatch(
    reticulate::virtualenv_install(
      envname,
      packages = packages,
      ignore_installed = isTRUE(reinstall)
    ),
    error = function(e) {
      stop(
        "Failed while installing Python packages into '", envname, "'.\n",
        "Attempted packages: ", paste(packages, collapse = ", "), "\n",
        "Active python: ", tryCatch(reticulate::py_config()$python, error = function(e2) "(unknown)"), "\n",
        "Original error: ", conditionMessage(e),
        call. = FALSE
      )
    }
  )
  
  invisible(TRUE)
}

#' @keywords internal
db_verify_python_imports <- function(pkgs = db_required_python_packages()) {
  import_stmt <- paste0("import ", paste(pkgs, collapse = ", "))
  tryCatch(
    reticulate::py_run_string(import_stmt),
    error = function(e) {
      stop(
        "Python packages were not importable after install.\n",
        "Tried: ", paste(pkgs, collapse = ", "), "\n",
        "Active python: ", tryCatch(reticulate::py_config()$python, error = function(e2) "(unknown)"), "\n",
        "Original error: ", conditionMessage(e),
        call. = FALSE
      )
    }
  )
  invisible(TRUE)
}

#' @keywords internal
db_playwright_chromium_is_installed <- function() {
  if (!isTRUE(tryCatch(reticulate::py_module_available("playwright"), error = function(e) FALSE))) {
    return(FALSE)
  }
  
  ok <- tryCatch({
    reticulate::py_run_string(
      "
from playwright.sync_api import sync_playwright
__le_chromium_ok = False
try:
    with sync_playwright() as p:
        __le_chromium_ok = bool(p.chromium.executable_path)
except Exception:
    __le_chromium_ok = False
"
    )
    isTRUE(reticulate::py$`__le_chromium_ok`)
  }, error = function(e) FALSE)
  
  ok
}

#' @keywords internal
db_install_playwright_chromium <- function() {
  message("Ensuring Playwright Chromium is installed (may download ~100-200MB)...")
  tryCatch({
    reticulate::py_run_string(
      "
import sys
from playwright.__main__ import main
sys.argv = ['playwright', 'install', 'chromium']
try:
    main()
except SystemExit:
    pass
"
    )
  }, error = function(e) {
    stop(
      "Playwright is installed, but Chromium failed to install.\n",
      "You can retry manually with:\n",
      "  reticulate::py_run_string(\"from playwright.__main__ import main; import sys; sys.argv=['playwright','install','chromium']; main()\")\n\n",
      "Active python: ", tryCatch(reticulate::py_config()$python, error = function(e2) "(unknown)"), "\n",
      "Original error: ", conditionMessage(e),
      call. = FALSE
    )
  })
  
  invisible(TRUE)
}

#' @keywords internal
db_install_plan <- function(pkgs, reinstall = FALSE, install_chromium = TRUE) {
  missing_pkgs <- db_missing_python_packages(pkgs, reinstall = reinstall)
  
  chromium_missing <- FALSE
  if (isTRUE(install_chromium) && !isTRUE(reinstall)) {
    chromium_missing <- !db_playwright_chromium_is_installed()
  }
  if (isTRUE(install_chromium) && isTRUE(reinstall)) {
    chromium_missing <- TRUE
  }
  
  list(
    missing_pkgs = missing_pkgs,
    chromium_missing = chromium_missing
  )
}

# ---- Public API --------------------------------------------------------------

#' Install Python dependencies for downballotR
#'
#' Creates/uses a named virtual environment and installs Python requirements
#' (pandas, requests, lxml, playwright), then installs Playwright Chromium.
#'
#' If the environment already exists and all required packages are present,
#' the function prints a message and returns without doing work (unless Chromium
#' is missing and `install_chromium = TRUE`). In all cases, it attempts to
#' initialize reticulate to the selected interpreter for this session.
#'
#' @param envname Name of the virtualenv to create/use.
#' @param python Path to a python executable to use when creating the env (optional).
#' @param reinstall If TRUE, reinstall packages even if already installed.
#' @param install_chromium If TRUE, install Playwright Chromium browser.
#' @param quiet If TRUE, suppress progress messages.
#' @export
downballot_install_python <- function(
    envname = "downballotR",
    python = NULL,
    reinstall = FALSE,
    install_chromium = TRUE,
    quiet = FALSE
) {
  pkgs <- db_required_python_packages()
  
  .msg <- function(...) if (!isTRUE(quiet)) message(...)
  
  # If Python already initialized in this session, ensure it's the right interpreter
  db_stop_if_python_initialized_to_other(envname)
  
  # Create env if needed
  db_ensure_virtualenv(envname, python = python)
  
  # Prefer this env in this session
  db_use_virtualenv(envname)
  
  # (2) Force reticulate to initialize to the selected interpreter now,
  # so status checks and py_module_available reflect the correct Python.
  db_force_reticulate_init()
  
  # Determine what to install (packages + chromium)
  plan <- db_install_plan(pkgs, reinstall = reinstall, install_chromium = install_chromium)
  
  # If nothing to do, stop early with a friendly message
  if (length(plan$missing_pkgs) == 0 && !isTRUE(plan$chromium_missing)) {
    .msg(
      "Python environment '", envname, "' already exists and is ready.\n",
      "Packages present: ", paste(pkgs, collapse = ", "), "\n",
      "Playwright Chromium: Correctly installed\n",
      "Nothing to do."
    )
    
    # Still ensure Python is usable in-session (already forced above, but keep safe)
    db_force_reticulate_init()
    return(invisible(TRUE))
  }
  
  # Install missing packages
  if (length(plan$missing_pkgs) > 0) {
    db_install_python_packages(envname, plan$missing_pkgs, reinstall = reinstall)
    
    # Verify imports (also keeps reticulate initialized to this interpreter)
    db_verify_python_imports(pkgs = pkgs)
  }
  
  # Install Chromium if requested and missing
  if (isTRUE(install_chromium) && isTRUE(plan$chromium_missing)) {
    db_install_playwright_chromium()
    
    if (!isTRUE(db_playwright_chromium_is_installed())) {
      stop(
        "Playwright Chromium install step ran, but Chromium still appears unavailable.\n",
        "Active python: ", tryCatch(reticulate::py_config()$python, error = function(e) "(unknown)"),
        call. = FALSE
      )
    }
  }
  
  .msg("Python setup complete for env '", envname, "'.")
  invisible(TRUE)
}
