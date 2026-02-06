# data-raw/build_nc_snapshot.R
#
# Generates (FULL data as package datasets):
#   data/nc_precinct_results.rda
#   data/nc_county_results.rda
#   data/nc_state_results.rda
#
# Generates (SMALL samples for inspection/examples):
#   inst/extdata/nc_precinct_sample.csv   (100 rows)
#   inst/extdata/nc_county_sample.csv     (100 rows)
#   inst/extdata/nc_state_sample.csv      (100 rows)

library(reticulate)
library(dplyr)

# ---- helpers ----
stop_if_missing_dir <- function(path) {
  if (!dir.exists(path)) stop("Missing directory: ", path)
}

scrub_nul_bytes <- function(path) {
  bytes <- readBin(path, what = "raw", n = file.info(path)$size)
  if (!any(bytes == as.raw(0x00))) return(path)
  
  message("Found embedded NUL bytes in CSV; scrubbing them...")
  bytes <- bytes[bytes != as.raw(0x00)]
  clean_path <- tempfile(fileext = ".csv")
  writeBin(bytes, clean_path)
  message("Wrote scrubbed CSV: ", clean_path)
  clean_path
}

# Extract a pandas DF from a python object that might be:
# - dict-like (out['precinct_final'])
# - object with attributes (out.precinct_final)
# - tuple/list (out[0])
get_df_from_out <- function(out, key, idx = NULL) {
  # dict-like
  try({
    df <- out$`__getitem__`(key)
    if (!is.null(df)) return(df)
  }, silent = TRUE)
  
  # attribute-like
  try({
    df <- out[[key]]
    if (!is.null(df)) return(df)
  }, silent = TRUE)
  
  # positional
  if (!is.null(idx)) {
    try({
      df <- out$`__getitem__`(as.integer(idx))
      if (!is.null(df)) return(df)
    }, silent = TRUE)
  }
  
  NULL
}

normalize_types <- function(df) {
  if ("retrieved_at" %in% names(df)) {
    df$retrieved_at <- as.POSIXct(
      df$retrieved_at,
      format = "%Y-%m-%d %H:%M:%S",
      tz = "UTC"
    )
  }
  if ("election_date" %in% names(df)) {
    df$election_date <- as.Date(df$election_date, format = "%Y-%m-%d")
  }
  df
}

write_sample_csv <- function(df, out_path, n = 100, seed = 1) {
  if (nrow(df) <= 0) stop("Cannot sample from empty data frame: ", out_path)
  set.seed(seed)
  df_s <- df %>% dplyr::slice_sample(n = min(n, nrow(df)))
  write.csv(df_s, out_path, row.names = FALSE)
}

# ---- 1) Confirm python ----
py_cfg <- reticulate::py_config()
message("Using python: ", py_cfg$python)

# ---- 2) Ensure inst/python is on sys.path ----
pkg_root <- normalizePath(".", winslash = "/", mustWork = TRUE)
inst_python <- normalizePath(
  file.path(pkg_root, "inst", "python"),
  winslash = "/",
  mustWork = FALSE
)
stop_if_missing_dir(inst_python)

reticulate::py_run_string(
  sprintf("import sys; sys.path.insert(0, r'%s')", inst_python),
  local = FALSE
)

# ---- 3) Import module WITHOUT conversion ----
nc <- reticulate::import("NorthCarolina.pipeline", convert = FALSE)

# ---- 4) Run pipeline (returns 3 pandas.DataFrame objects) ----
out <- nc$NcElectionPipeline()$run()
if (is.null(out)) stop("Pipeline returned NULL (expected 3 pandas DataFrames).")

precinct_py <- get_df_from_out(out, "precinct_final", idx = 0L)
county_py   <- get_df_from_out(out, "county_final",   idx = 1L)
state_py    <- get_df_from_out(out, "state_final",    idx = 2L)

if (is.null(precinct_py) || is.null(county_py) || is.null(state_py)) {
  stop(
    "Could not extract all three DataFrames from pipeline output. ",
    "Expected keys/attrs: precinct_final, county_final, state_final."
  )
}

n_precinct <- as.integer(reticulate::py_to_r(precinct_py$shape[[0]]))
n_county   <- as.integer(reticulate::py_to_r(county_py$shape[[0]]))
n_state    <- as.integer(reticulate::py_to_r(state_py$shape[[0]]))

message("Python rows - precinct: ", n_precinct, " | county: ", n_county, " | state: ", n_state)

if (is.na(n_state) || n_state <= 0) stop("state_final returned 0 rows (unexpected).")

# ---- 5) Python helper to sanitize + write CSV (for safe transfer to R) ----
reticulate::py_run_string("
import pandas as pd

def _write_clean_csv(df, path):
    df = df.copy()

    # Strip NUL bytes from string-like columns
    for c in df.columns:
        if str(df[c].dtype) == 'object':
            df[c] = df[c].astype('string').str.replace('\\x00', '', regex=False)

    # Write CSV (pandas versions differ on errors=)
    try:
        df.to_csv(path, index=False, encoding='utf-8', errors='replace')
    except TypeError:
        df.to_csv(path, index=False, encoding='utf-8')
", local = FALSE)

read_python_df_via_clean_csv <- function(df_py, label) {
  tmp_csv <- tempfile(pattern = paste0("nc_", label, "_"), fileext = ".csv")
  reticulate::py$`_write_clean_csv`(df_py, tmp_csv)
  message("Wrote temp CSV (", label, "): ", tmp_csv)
  
  tmp_csv <- scrub_nul_bytes(tmp_csv)
  
  df <- read.csv(tmp_csv, stringsAsFactors = FALSE, fileEncoding = "UTF-8")
  if (nrow(df) <= 0) stop("Read 0 rows from CSV (", label, "): ", tmp_csv)
  df
}

precinct_df <- read_python_df_via_clean_csv(precinct_py, "precinct")
county_df   <- read_python_df_via_clean_csv(county_py, "county")
state_df    <- read_python_df_via_clean_csv(state_py, "state")

# ---- 6) Normalize types in R ----
nc_precinct_results <- normalize_types(precinct_df)
nc_county_results   <- normalize_types(county_df)
nc_state_results    <- normalize_types(state_df)

# ---- 7) Save FULL datasets as package data (.rda) ----
usethis::use_data(nc_precinct_results, overwrite = TRUE)
usethis::use_data(nc_county_results, overwrite = TRUE)
usethis::use_data(nc_state_results, overwrite = TRUE)

# ---- 8) Write SMALL sampled CSVs (100 rows each) to inst/extdata ----
dir.create(file.path("inst", "extdata"), recursive = TRUE, showWarnings = FALSE)

write_sample_csv(
  nc_precinct_results,
  file.path("inst", "extdata", "nc_precinct_sample.csv"),
  n = 100,
  seed = 1
)
write_sample_csv(
  nc_county_results,
  file.path("inst", "extdata", "nc_county_sample.csv"),
  n = 100,
  seed = 1
)
write_sample_csv(
  nc_state_results,
  file.path("inst", "extdata", "nc_state_sample.csv"),
  n = 100,
  seed = 1
)

message("✅ Wrote 3 full .rda datasets and 3 sampled CSVs (100 rows each) to inst/extdata/")
