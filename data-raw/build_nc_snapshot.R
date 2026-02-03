# data-raw/build_nc_snapshot.R
# Generates:
#   data/nc_results.rda
#   inst/extdata/nc_manifest.csv

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

# ---- 1) Confirm python ----
py_cfg <- reticulate::py_config()
message("Using python: ", py_cfg$python)

# ---- 2) Ensure inst/python is on sys.path ----
pkg_root <- normalizePath(".", winslash = "/", mustWork = TRUE)
inst_python <- normalizePath(file.path(pkg_root, "inst", "python"),
                             winslash = "/", mustWork = FALSE)
stop_if_missing_dir(inst_python)

reticulate::py_run_string(
  sprintf("import sys; sys.path.insert(0, r'%s')", inst_python),
  local = FALSE
)

# ---- 3) Import module WITHOUT conversion ----
nc <- reticulate::import("NorthCarolina.pipeline", convert = FALSE)

# ---- 4) Run pipeline (returns pandas.DataFrame) ----
df_py <- nc$NcElectionPipeline()$run()
if (is.null(df_py)) stop("Pipeline returned NULL (expected pandas DataFrame).")

nrows <- as.integer(reticulate::py_to_r(df_py$shape[[0]]))
if (is.na(nrows) || nrows <= 0) stop("Pipeline returned 0 rows.")
message("Python rows: ", nrows)

# ---- 5) Python helper to sanitize + write CSV ----
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

tmp_csv <- tempfile(fileext = ".csv")
reticulate::py$`_write_clean_csv`(df_py, tmp_csv)
message("Wrote temp CSV: ", tmp_csv)

# ---- 6) Scrub any remaining embedded NULs at the file level, then read in R ----
tmp_csv <- scrub_nul_bytes(tmp_csv)

df <- read.csv(tmp_csv, stringsAsFactors = FALSE, fileEncoding = "UTF-8")
if (nrow(df) <= 0) stop("Read 0 rows from CSV: ", tmp_csv)

# ---- 7) Normalize types in R ----
if ("retrieved_at" %in% names(df)) {
  df$retrieved_at <- as.POSIXct(df$retrieved_at,
                                format = "%Y-%m-%d %H:%M:%S",
                                tz = "UTC")
}
if ("election_date" %in% names(df)) {
  df$election_date <- as.Date(df$election_date, format = "%Y-%m-%d")
}

# ---- 8) Save snapshot as .rda ----
nc_results <- df
usethis::use_data(nc_results, overwrite = TRUE)

# ---- 9) Manifest for coverage checks ----
required_cols <- c("state", "election_date", "source_url")
missing_cols <- setdiff(required_cols, names(df))
if (length(missing_cols) > 0) {
  stop("Missing required columns for manifest: ", paste(missing_cols, collapse = ", "))
}

manifest <- df %>%
  distinct(state, election_date, source_url) %>%
  arrange(election_date)

dir.create(file.path("inst", "extdata"), recursive = TRUE, showWarnings = FALSE)
write.csv(manifest, file.path("inst", "extdata", "nc_manifest.csv"), row.names = FALSE)

message("✅ Wrote data/nc_results.rda and inst/extdata/nc_manifest.csv")
