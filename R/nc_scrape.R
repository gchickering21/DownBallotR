# R/nc_scrape.R

# NOTE:
# - This file is written to work both in development *and* after installation.
# - We bind reticulate to the package's configured virtualenv via db_bind_python().
# - We add the *installed* inst/python directory (system.file("python", ...)) to sys.path.
# - We keep Python -> R conversion robust by using a CSV boundary (avoid pandas conversion).

#' Ensure NC Python modules are importable and return module handles
#'
#' - Binds reticulate to DownBallotR's Python environment (idempotent).
#' - Adds the installed package's inst/python directory to sys.path once.
#' - Imports modules with convert = FALSE to avoid pandas->R conversion issues.
#'
#' @keywords internal
.nc_python_import <- function() {
  db_bind_python()
  
  py_dir <- system.file("python", package = "DownBallotR")
  if (!nzchar(py_dir) || !dir.exists(py_dir)) {
    stop("Could not locate installed inst/python directory.", call. = FALSE)
  }
  
  # Add to sys.path once (idempotent)
  reticulate::py_run_string(sprintf(
    paste0(
      "import sys\n",
      "p = r'''%s'''\n",
      "if p not in sys.path:\n",
      "    sys.path.insert(0, p)\n"
    ),
    py_dir
  ))
  
  list(
    pipeline  = reticulate::import("NorthCarolina.pipeline",  convert = FALSE),
    discovery = reticulate::import("NorthCarolina.discovery", convert = FALSE),
    constants = reticulate::import("NorthCarolina.constants", convert = FALSE)
  )
}

#' Get the minimum supported NC election date from Python (as Date)
#'
#' Python owns the default (because it reflects parser support),
#' but R can use it to clamp user-requested date ranges pre-scrape.
#'
#' @keywords internal
.nc_min_supported_date <- function() {
  mods <- .nc_python_import()
  
  # Python `date` object -> ISO string -> R Date
  iso <- reticulate::py_to_r(mods$constants$NC_MIN_SUPPORTED_ELECTION_DATE$isoformat())
  as.Date(iso)
}

#' Clamp a requested range to the minimum supported NC election date
#'
#' If end_date is entirely before the supported range, marks the range empty.
#'
#' @keywords internal
.nc_clamp_range <- function(start_date, end_date) {
  min_supported <- .nc_min_supported_date()
  
  start_date <- .parse_date_or_null(start_date)
  end_date   <- .parse_date_or_null(end_date)
  
  if (!is.null(start_date) && start_date < min_supported) {
    start_date <- min_supported
  }
  
  if (!is.null(end_date) && end_date < min_supported) {
    return(list(start_date = start_date, end_date = end_date, empty = TRUE))
  }
  
  list(start_date = start_date, end_date = end_date, empty = FALSE)
}

#' Discover available NC election result zips (returns data.frame)
#'
#' We intentionally convert through a CSV boundary to avoid reticulate pandas
#' conversion edge cases. The Python function may return dicts or dataclasses.
#'
#' @keywords internal
.nc_discover_election_table <- function() {
  .nc_python_import()
  
  tmp <- tempfile(fileext = ".csv")
  
  reticulate::py_run_string(sprintf(
    paste0(
      "import pandas as pd\n",
      "from NorthCarolina.discovery import discover_nc_results_zips\n",
      "\n",
      "rows = discover_nc_results_zips()\n",
      "out = []\n",
      "for r in rows:\n",
      "    d = getattr(r, '__dict__', None)\n",
      "    if d is None:\n",
      "        try:\n",
      "            d = dict(r)\n",
      "        except Exception:\n",
      "            d = {\n",
      "                'zip_url': getattr(r, 'zip_url', None),\n",
      "                'election_date': getattr(r, 'election_date', None)\n",
      "            }\n",
      "    out.append(d)\n",
      "\n",
      "df = pd.DataFrame(out)\n",
      "df.to_csv(r'%s', index=False)\n"
    ),
    tmp
  ))
  
  df <- utils::read.csv(tmp, stringsAsFactors = FALSE)
  
  if (!"election_date" %in% names(df)) {
    stop("Discovery output is missing required column `election_date`.", call. = FALSE)
  }
  
  df$election_date <- as.Date(df$election_date)
  df
}

#' Identify discovered NC election dates missing from the shipped snapshot
#'
#' - covered: dates present in the snapshot (via manifest)
#' - universe: discovered dates within requested range
#' - missing: universe - covered
#'
#' If start/end not provided, defaults to the min/max discovered.
#' Dates are clamped to the minimum supported date.
#'
#' @keywords internal
.nc_missing_election_dates <- function(start_date = NULL, end_date = NULL) {
  covered    <- sort(unique(nc_snapshot_dates()))
  discovered <- .nc_discover_election_table()
  
  # Default caller range = discovered min/max
  start_date <- .parse_date_or_null(start_date) %||% min(discovered$election_date, na.rm = TRUE)
  end_date   <- .parse_date_or_null(end_date)   %||% max(discovered$election_date, na.rm = TRUE)
  
  # Clamp to minimum supported date
  rng <- .nc_clamp_range(start_date, end_date)
  if (isTRUE(rng$empty)) {
    return(list(
      covered  = covered,
      universe = as.Date(character()),
      missing  = as.Date(character())
    ))
  }
  start_date <- rng$start_date
  end_date   <- rng$end_date
  
  universe <- discovered$election_date[
    discovered$election_date >= start_date & discovered$election_date <= end_date
  ]
  universe <- sort(unique(universe))
  
  missing <- as.Date(setdiff(as.character(universe), as.character(covered)))

  list(
    covered  = covered,
    universe = universe,
    missing  = missing
  )
}

#' Scrape NC elections for a date range (returns data.frame)
#'
#' - Range is clamped to the Python minimum supported election date.
#' - Uses a CSV boundary to avoid pandas conversion issues.
#' - Returns an empty data.frame if the requested range is entirely unsupported.
#'
#' @keywords internal
.nc_scrape_range <- function(start_date = NULL, end_date = NULL) {
  .nc_python_import()
  
  rng <- .nc_clamp_range(start_date, end_date)
  if (isTRUE(rng$empty)) {
    # Range entirely before supported layouts; nothing to scrape
    return(data.frame())
  }
  start_date <- rng$start_date
  end_date   <- rng$end_date
  
  tmp <- tempfile(fileext = ".csv")
  
  # Pass dates as Python None or ISO string
  sd <- if (is.null(start_date)) "None" else sprintf("'%s'", as.character(start_date))
  ed <- if (is.null(end_date))   "None" else sprintf("'%s'", as.character(end_date))
  
  reticulate::py_run_string(sprintf(
    paste0(
      "import pandas as pd\n",
      "from NorthCarolina.pipeline import NcElectionPipeline\n",
      "\n",
      "pipe = NcElectionPipeline()\n",
      "df = pipe.run(start_date=%s, end_date=%s)\n",
      "\n",
      "# scrub embedded NUL bytes in object columns (defensive)\n",
      "for c in df.columns:\n",
      "    if df[c].dtype == 'object':\n",
      "        df[c] = df[c].astype('string').str.replace('\\x00', '', regex=False)\n",
      "\n",
      "df.to_csv(r'%s', index=False)\n"
    ),
    sd, ed, tmp
  ))
  
  df <- utils::read.csv(tmp, stringsAsFactors = FALSE)
  
  if ("election_date" %in% names(df)) {
    df$election_date <- as.Date(df$election_date)
  }
  
  df
}

#' Scrape only missing NC election dates for a requested range
#'
#' Returns NULL if no missing dates are detected (signals "no scrape needed").
#' Currently uses a simple strategy: scrape the entire requested range.
#' (Later optimization: scrape per missing date.)
#'
#' @keywords internal
.nc_scrape_missing_dates <- function(start_date = NULL, end_date = NULL) {
  miss <- .nc_missing_election_dates(start_date, end_date)
  
  if (length(miss$missing) == 0) {
    return(NULL)
  }
  
  .nc_scrape_range(start_date, end_date)
}
