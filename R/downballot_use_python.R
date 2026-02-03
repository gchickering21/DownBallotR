#' Use the downballotR Python virtualenv in this R session
#'
#' Pins reticulate to the package's virtualenv for the current R session.
#' If reticulate is already initialized to a different interpreter, this will
#' error with a clear message (because reticulate cannot switch interpreters
#' mid-session).
#'
#' @param envname Name of the virtualenv to use.
#' @return Invisibly TRUE on success.
#' @export
downballot_use_python <- function(envname = "downballotR") {
  if (!isTRUE(tryCatch(reticulate::virtualenv_exists(envname), error = function(e) FALSE))) {
    stop(
      "Python environment '", envname, "' does not exist.\n",
      "Run downballots_install_python() first.",
      call. = FALSE
    )
  }

  # IMPORTANT: Do NOT call py_config() unless Python is already initialized,
  # because py_config() can trigger reticulate to auto-select an interpreter.
  already_init <- isTRUE(tryCatch(reticulate::py_available(initialize = FALSE), error = function(e) FALSE))

  if (already_init) {
    cfg <- tryCatch(reticulate::py_config(), error = function(e) NULL)

    if (!is.null(cfg) && !is.null(cfg$python) && nzchar(cfg$python)) {
      wanted_raw <- tryCatch(reticulate::virtualenv_python(envname), error = function(e) NA_character_)
      wanted <- tryCatch(normalizePath(wanted_raw, winslash = "/"), error = function(e) NA_character_)
      current <- tryCatch(normalizePath(cfg$python, winslash = "/"), error = function(e) cfg$python)

      if (!is.na(wanted) && !identical(current, wanted)) {
        stop(
          "reticulate is already initialized to a different Python interpreter.\n",
          "  current: ", cfg$python, "\n",
          "  wanted:  ", wanted_raw, "\n\n",
          "Fix: Restart your R session, then run:\n",
          "  downballot_use_python('", envname, "')\n",
          call. = FALSE
        )
      }

      return(invisible(TRUE))
    }
  }

  # Not initialized yet: set the preferred env first...
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

  # ...then initialize (now it should initialize to the venv)
  tryCatch(reticulate::py_config(), error = function(e) NULL)

  invisible(TRUE)
}
