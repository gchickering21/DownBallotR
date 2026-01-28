.onAttach <- function(libname, pkgname) {
  # Don’t initialize Python here.
  # Just provide helpful guidance if things aren’t set up.
  ok <- FALSE
  try({
    st <- downballot_python_status(quiet = TRUE)  # your status fn renamed for downballot
    ok <- isTRUE(st$virtualenv_exists)
  }, silent = TRUE)
  
  if (!ok) {
    packageStartupMessage(
      "downballot: Python dependencies are not set up yet.\n",
      "Run: downballot_install_python()\n",
      "Then: downballot_use_python()"
    )
  }
}
