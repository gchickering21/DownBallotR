#' Check Python environment status for downballotR
#'
#' Reports whether the Python virtual environment exists, whether reticulate
#' is initialized (and which Python is active), which required packages are
#' missing, and whether Playwright Chromium is available.
#'
#' This function does not modify the environment.
#'
#' @param envname Name of the virtualenv to check.
#' @param required_pkgs Character vector of required Python packages. Defaults
#'   to `db_required_python_packages()`.
#' @param quiet If `TRUE`, do not print. (You can still `print()` the returned
#'   object explicitly.)
#' @return An object of class `downballot_python_status`. Invisibly when
#'   `quiet = FALSE`.
#' @export
downballot_python_status <- function(
    envname = "downballotR",
    required_pkgs = db_required_python_packages(),
    quiet = FALSE
) {
  status <- db_python_status_collect(envname = envname, required_pkgs = required_pkgs)
  
  if (!quiet) {
    print(status)
    return(invisible(status))
  }
  
  status
}

# ---- internal helpers ----

db_python_status_collect <- function(envname, required_pkgs) {
  ve <- db_status_virtualenv(envname)
  rt <- db_status_reticulate()
  
  # Detect whether active python matches the virtualenv python (when both exist)
  active_matches_env <- NA
  if (isTRUE(rt$reticulate_initialized) && isTRUE(ve$virtualenv_exists) &&
      !is.null(rt$active_python) && !is.null(ve$virtualenv_python) &&
      nzchar(rt$active_python) && nzchar(ve$virtualenv_python)) {
    
    wanted <- tryCatch(normalizePath(ve$virtualenv_python, winslash = "/"), error = function(e) ve$virtualenv_python)
    current <- tryCatch(normalizePath(rt$active_python, winslash = "/"), error = function(e) rt$active_python)
    active_matches_env <- identical(current, wanted)
  }
  
  # Package/chromium checks only make sense if:
  # - reticulate is initialized AND
  # - it is initialized to the correct env python
  missing <- character(0)
  chromium_ok <- FALSE
  
  if (!isTRUE(rt$reticulate_initialized)) {
    missing <- required_pkgs
    chromium_ok <- FALSE
  } else if (identical(active_matches_env, FALSE)) {
    # reticulate is bound to the WRONG python; don't report missing packages
    # because they'd be missing in the wrong interpreter even if env is fine.
    missing <- NA_character_
    chromium_ok <- NA
  } else {
    missing <- db_status_missing_packages(required_pkgs)
    chromium_ok <- db_playwright_chromium_is_installed()
  }
  
  status <- list(
    envname = envname,
    required_pkgs = required_pkgs,
    
    virtualenv_exists = ve$virtualenv_exists,
    virtualenv_python = ve$virtualenv_python,
    
    reticulate_initialized = rt$reticulate_initialized,
    active_python = rt$active_python,
    
    active_python_matches_env = active_matches_env,
    
    missing_packages = missing,
    playwright_chromium_installed = chromium_ok,
    
    advice = db_python_status_advice(
      envname = envname,
      virtualenv_exists = ve$virtualenv_exists,
      reticulate_initialized = rt$reticulate_initialized,
      active_python_matches_env = active_matches_env,
      missing_packages = missing,
      playwright_chromium_installed = chromium_ok
    )
  )
  
  class(status) <- c("downballot_python_status", class(status))
  status
}

db_status_virtualenv <- function(envname) {
  out <- list(
    virtualenv_exists = FALSE,
    virtualenv_python = NULL
  )
  
  exists <- tryCatch(
    reticulate::virtualenv_exists(envname),
    error = function(e) FALSE
  )
  
  out$virtualenv_exists <- isTRUE(exists)
  
  if (out$virtualenv_exists) {
    out$virtualenv_python <- tryCatch(
      reticulate::virtualenv_python(envname),
      error = function(e) NA_character_
    )
  }
  
  out
}

db_status_reticulate <- function() {
  out <- list(
    reticulate_initialized = FALSE,
    active_python = NULL
  )

  # Important: do NOT initialize Python just to check status
  is_init <- tryCatch(
    reticulate::py_available(initialize = FALSE),
    error = function(e) FALSE
  )

  if (!isTRUE(is_init)) {
    return(out)
  }

  # Safe now: already initialized, so py_config() won't trigger interpreter selection
  cfg <- tryCatch(reticulate::py_config(), error = function(e) NULL)

  if (!is.null(cfg) && !is.null(cfg$python) && nzchar(cfg$python)) {
    out$reticulate_initialized <- TRUE
    out$active_python <- cfg$python
  }

  out
}


db_status_missing_packages <- function(required_pkgs) {
  if (length(required_pkgs) == 0) {
    return(character(0))
  }
  
  ok <- vapply(
    required_pkgs,
    FUN = function(pkg) {
      tryCatch(reticulate::py_module_available(pkg), error = function(e) FALSE)
    },
    FUN.VALUE = logical(1)
  )
  
  required_pkgs[!ok]
}

db_python_status_advice <- function(
    envname,
    virtualenv_exists,
    reticulate_initialized,
    active_python_matches_env,
    missing_packages,
    playwright_chromium_installed
) {
  if (!isTRUE(virtualenv_exists)) {
    return(c("Next step:", "  downballot_install_python()"))
  }
  
  if (!isTRUE(reticulate_initialized)) {
    return(c("Next step:", sprintf("  downballot_use_python('%s')", envname)))
  }
  
  if (identical(active_python_matches_env, FALSE)) {
    return(c(
      "reticulate is initialized to a different Python than this virtualenv.",
      "Fix: Restart your R session, then run:",
      sprintf("  downballot_use_python('%s')", envname)
    ))
  }
  
  if (length(missing_packages) > 0 || !isTRUE(playwright_chromium_installed)) {
    return(c("Fix issues with:", "  downballot_install_python(reinstall = TRUE)"))
  }
  
  c("Python environment is ready for use.")
}

# ---- printing (S3) ----

#' @export
print.downballot_python_status <- function(x, ...) {
  cat("\n downballotR Python status\n")
  cat("--------------------------------------\n")
  cat("Virtualenv name:         ", x$envname, "\n", sep = "")
  cat("Virtualenv exists:       ", x$virtualenv_exists, "\n", sep = "")
  
  if (isTRUE(x$virtualenv_exists)) {
    cat("Virtualenv python:       ", x$virtualenv_python, "\n", sep = "")
  }
  
  cat("reticulate initialized:  ", x$reticulate_initialized, "\n", sep = "")
  
  if (isTRUE(x$reticulate_initialized)) {
    cat("Active python:           ", x$active_python, "\n", sep = "")
    
    if (identical(x$active_python_matches_env, FALSE)) {
      cat(" Active python does NOT match the virtualenv python for '", x$envname, "'.\n", sep = "")
    }
  }
  
  if (identical(x$active_python_matches_env, FALSE)) {
    cat("Python packages:         (not checked - wrong interpreter)\n")
    cat("Playwright Chromium:     (not checked - wrong interpreter)\n")
  } else if (length(x$missing_packages) == 0) {
    cat("Python packages:         all required packages correctly installed\n")
    cat(
      "Playwright Chromium:     ",
      if (isTRUE(x$playwright_chromium_installed)) "correctly installed" else " missing",
      "\n",
      sep = ""
    )
  } else {
    cat("Missing packages:        ", paste(x$missing_packages, collapse = ", "), "\n", sep = "")
    cat(
      "Playwright Chromium:     ",
      if (isTRUE(x$playwright_chromium_installed)) " correctly nstalled" else " missing",
      "\n",
      sep = ""
    )
  }
  
  if (!is.null(x$advice) && length(x$advice) > 0) {
    cat("\n")
    cat(paste0("- ", x$advice[[1]], "\n"), sep = "")
    if (length(x$advice) > 1) {
      cat(paste0(x$advice[-1], collapse = "\n"), "\n", sep = "")
    }
  }
  
  invisible(x)
}
