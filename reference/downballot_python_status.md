# Check Python environment status for downballotR

Reports whether the Python virtual environment exists, whether
reticulate is initialized (and which Python is active), which required
packages are missing, and whether Playwright Chromium is available.

## Usage

``` r
downballot_python_status(
  envname = "downballotR",
  required_pkgs = db_required_python_packages(),
  quiet = FALSE
)
```

## Arguments

- envname:

  Name of the virtualenv to check.

- required_pkgs:

  Character vector of required Python packages. Defaults to
  `db_required_python_packages()`.

- quiet:

  If `TRUE`, do not print. (You can still
  [`print()`](https://rdrr.io/r/base/print.html) the returned object
  explicitly.)

## Value

An object of class `downballot_python_status`. Invisibly when
`quiet = FALSE`.

## Details

This function does not modify the environment.
