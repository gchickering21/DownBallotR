#' Use the DownBallotR Python virtualenv in this R session
#'
#' Pins reticulate to the package's virtualenv for the current R session.
#' If reticulate is already initialized to a different interpreter, this
#' errors with a clear message (reticulate cannot switch interpreters mid-session).
#'
#' @param envname Name of the virtualenv to use.
#' @return Invisibly TRUE on success.
#' @export
downballot_use_python <- function(envname = "downballotR") {
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("The 'reticulate' package is required.", call. = FALSE)
  }
  
  # ---- 1) Ensure the virtualenv exists ----
  if (!isTRUE(tryCatch(reticulate::virtualenv_exists(envname), error = function(e) FALSE))) {
    stop(
      "Python environment '", envname, "' does not exist.\n",
      "Run downballots_install_python() first.",
      call. = FALSE
    )
  }
  
  # IMPORTANT: don't call py_config() unless Python is already initialized
  already_init <- isTRUE(
    tryCatch(reticulate::py_available(initialize = FALSE), error = function(e) FALSE)
  )
  
  # ---- 2) If Python is already initialized, verify it's the right interpreter ----
  if (already_init) {
    cfg <- tryCatch(reticulate::py_config(), error = function(e) NULL)
    
    current_raw <- if (!is.null(cfg) && nzchar(cfg$python %||% "")) cfg$python else NA_character_
    wanted_raw  <- tryCatch(reticulate::virtualenv_python(envname), error = function(e) NA_character_)
    
    current <- .db_norm_path(current_raw)
    wanted  <- .db_norm_path(wanted_raw)
    
    if (!is.na(current) && !is.na(wanted) && !identical(current, wanted)) {
      stop(
        "reticulate is already initialized to a different Python interpreter.\n",
        "  current: ", current_raw, "\n",
        "  wanted:  ", wanted_raw, "\n\n",
        "Fix: Restart your R session, then run:\n",
        "  downballot_use_python('", envname, "')\n",
        call. = FALSE
      )
    }
    
    return(invisible(TRUE))
  }
  
  # ---- 3) Not initialized yet: select env, then initialize ----
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
  
  # Now initialize Python (should pick the venv)
  tryCatch(reticulate::py_config(), error = function(e) NULL)
  
  invisible(TRUE)
}
