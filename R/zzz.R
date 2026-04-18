.onAttach <- function(libname, pkgname) {
  # Skip Python status check on CRAN — reticulate’s Python detection
  # causes background CPU usage that exceeds CRAN’s CPU/elapsed threshold.
  if (!identical(Sys.getenv("NOT_CRAN"), "true")) return(invisible(NULL))

  ok <- FALSE
  try(
    {
      st <- downballot_python_status(quiet = TRUE)
      ok <- isTRUE(st$virtualenv_exists)
    },
    silent = TRUE
  )

  if (!ok) {
    packageStartupMessage(
      "downballot: Python dependencies are not set up yet.\n",
      "Run: downballot_install_python()\n",
      "Then: downballot_use_python()"
    )
  }
}
