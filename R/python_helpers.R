#' Ensure downballot Python environment is ready
#'
#' @keywords internal
downballot_ensure_python <- function(envname = "downballot") {
  # If python already initialized incorrectly, give a clear restart instruction
  downballot_use_python(envname)

  st <- downballot_python_status(envname = envname, quiet = TRUE)
  if (!isTRUE(st$virtualenv_exists) || !isTRUE(st$reticulate_initialized)) {
    stop("Python not ready. Run downballot_install_python() first.", call. = FALSE)
  }
  if (isFALSE(st$active_python_matches_env)) {
    stop("Reticulate is bound to the wrong Python. Restart R and run downballot_use_python().", call. = FALSE)
  }
  if (length(st$missing_packages) > 0 || !isTRUE(st$playwright_chromium_installed)) {
    stop("Python deps missing. Run downballot_install_python(reinstall = TRUE).", call. = FALSE)
  }

  invisible(TRUE)
}


#' Normalize a path safely (no warnings on NA)
#' @keywords internal
.db_norm_path <- function(x) {
  if (is.null(x) || length(x) == 0 || isTRUE(is.na(x)) || !nzchar(x)) return(NA_character_)
  suppressWarnings(
    tryCatch(normalizePath(x, winslash = "/", mustWork = FALSE), error = function(e) x)
  )
}


