# R/nc_scrape.R

#' @keywords internal
.ensure_reticulate <- function() {
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("Scraping requires the 'reticulate' package. Install it or use source='snapshot'.", call. = FALSE)
  }
}

#' @keywords internal
.nc_python_import <- function() {
  .ensure_reticulate()
  
  pkg_root <- normalizePath(".")
  reticulate::py_run_string(sprintf(
    "import sys; sys.path.insert(0, r'%s')",
    file.path(pkg_root, "inst", "python")
  ))
  
  # IMPORTANT: convert=FALSE to avoid pandas->R conversion issues
  list(
    pipeline = reticulate::import("NorthCarolina.pipeline", convert = FALSE),
    discovery = reticulate::import("NorthCarolina.discovery", convert = FALSE)
  )
}

#' @keywords internal
.nc_discover_election_table <- function() {
  mods <- .nc_python_import()
  
  # discover_nc_results_zips() should return something list-like; if it returns a list of objects,
  # we’ll convert via Python to a clean pandas DataFrame then to CSV then into R.
  # We'll do conversion through CSV to avoid pandas conversion edge cases.
  tmp <- tempfile(fileext = ".csv")
  reticulate::py_run_string(sprintf("
import pandas as pd
from NorthCarolina.discovery import discover_nc_results_zips

rows = discover_nc_results_zips()
# rows might be list[dataclass]; normalize:
out = []
for r in rows:
    d = getattr(r, '__dict__', None)
    if d is None:
        # maybe it's already a dict
        try:
            d = dict(r)
        except Exception:
            d = {'zip_url': getattr(r, 'zip_url', None), 'election_date': getattr(r, 'election_date', None)}
    out.append(d)

df = pd.DataFrame(out)
df.to_csv(r'%s', index=False)
", tmp))
  
  read.csv(tmp, stringsAsFactors = FALSE) |>
    transform(election_date = as.Date(election_date))
}

#' @keywords internal
.nc_missing_election_dates <- function(start_date = NULL, end_date = NULL) {
  covered <- sort(unique(nc_snapshot_dates()))
  discovered <- .nc_discover_election_table()
  
  start_date <- .parse_date_or_null(start_date) %||% min(discovered$election_date, na.rm = TRUE)
  end_date   <- .parse_date_or_null(end_date)   %||% max(discovered$election_date, na.rm = TRUE)
  
  universe <- sort(unique(discovered$election_date[discovered$election_date >= start_date &
                                                     discovered$election_date <= end_date]))
  missing <- setdiff(universe, covered)
  
  list(
    covered = covered,
    universe = universe,
    missing = missing
  )
}

#' @keywords internal
.nc_scrape_range <- function(start_date = NULL, end_date = NULL) {
  mods <- .nc_python_import()
  pipe <- mods$pipeline$NcElectionPipeline()
  
  # returns pandas DataFrame; write to CSV in python; read in R
  tmp <- tempfile(fileext = ".csv")
  
  # pass dates as ISO strings
  sd <- if (is.null(start_date)) "None" else sprintf("'%s'", as.character(as.Date(start_date)))
  ed <- if (is.null(end_date))   "None" else sprintf("'%s'", as.character(as.Date(end_date)))
  
  reticulate::py_run_string(sprintf("
import pandas as pd
from datetime import date
from NorthCarolina.pipeline import NcElectionPipeline

pipe = NcElectionPipeline()
df = pipe.run(start_date=%s, end_date=%s)

# sanitize embedded NUL bytes just in case
for c in df.columns:
    if df[c].dtype == 'object':
        df[c] = df[c].astype('string').str.replace('\\x00', '', regex=False)

df.to_csv(r'%s', index=False)
", sd, ed, tmp))
  
  df <- read.csv(tmp, stringsAsFactors = FALSE)
  df$election_date <- as.Date(df$election_date)
  df
}

#' Scrape only missing NC election dates for a requested range
#' @keywords internal
.nc_scrape_missing_dates <- function(start_date = NULL, end_date = NULL) {
  miss <- .nc_missing_election_dates(start_date, end_date)
  if (length(miss$missing) == 0) {
    return(NULL)
  }
  
  # scrape the whole requested range (simpler) or do per-date (later optimization)
  .nc_scrape_range(start_date, end_date)
}
