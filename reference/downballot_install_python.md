# Install Python dependencies for downballotR

Creates/uses a named virtual environment and installs Python
requirements (pandas, requests, lxml, playwright), then installs
Playwright Chromium.

## Usage

``` r
downballot_install_python(
  envname = "downballotR",
  python = NULL,
  reinstall = FALSE,
  install_chromium = TRUE,
  quiet = FALSE
)
```

## Arguments

- envname:

  Name of the virtualenv to create/use.

- python:

  Path to a python executable to use when creating the env (optional).

- reinstall:

  If TRUE, reinstall packages even if already installed.

- install_chromium:

  If TRUE, install Playwright Chromium browser.

- quiet:

  If TRUE, suppress progress messages.

## Details

If the environment already exists and all required packages are present,
the function prints a message and returns without doing work (unless
Chromium is missing and `install_chromium = TRUE`). In all cases, it
attempts to initialize reticulate to the selected interpreter for this
session.
