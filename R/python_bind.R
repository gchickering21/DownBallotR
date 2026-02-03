#' Ensure reticulate is bound to DownBallotR's Python environment
#'
#' This function is idempotent: safe to call multiple times in a session.
#' It binds reticulate to the package's configured virtualenv and adds
#' inst/python to sys.path so Python can import our modules.
#'
#' @keywords internal
db_bind_python <- function() {
  
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("The 'reticulate' package is required.", call. = FALSE)
  }
  
  # If python already initialized, ensure it's not the "wrong" python.
  # You already have a helper for this in your package exports list.
  # If you prefer to keep this internal, you can keep using it as internal too.
  if (reticulate::py_available(initialize = FALSE)) {
    # This should error if initialized to something else than your venv.
    db_stop_if_python_initialized_to_other()
  } else {
    downballot_use_python()
  }
  
  # Ensure our package python modules are importable
  python_path <- system.file("python", package = "DownBallotR")
  if (!nzchar(python_path)) {
    stop("Could not locate inst/python in the installed package.", call. = FALSE)
  }
  
  # Add to sys.path once
  # Use a sentinel to avoid repeated inserts
  reticulate::py_run_string(sprintf(paste0(
    "import sys\n",
    "p = r'''%s'''\n",
    "if p not in sys.path:\n",
    "    sys.path.insert(0, p)\n"
  ), python_path))
  
  
  invisible(TRUE)
}
