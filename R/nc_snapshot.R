# internal cache so we don't re-load every call
.nc_cache <- new.env(parent = emptyenv())

#' @keywords internal
.pkg_name <- function() utils::packageName()

#' Load shipped NC election results snapshot
#'
#' @return a data.frame
#' @export
get_nc_snapshot <- function() {
  data("nc_results", package = .pkg_name(), envir = environment())
  get("nc_results", envir = environment())
}

#' List election dates covered by the shipped snapshot (fast)
#'
#' @return a vector of Date
#' @export
nc_snapshot_dates <- function() {
  path <- system.file("extdata", "nc_manifest.csv", package = "DownballotR")
  if (nzchar(path) && file.exists(path)) {
    m <- utils::read.csv(path, stringsAsFactors = FALSE)
    return(sort(unique(as.Date(m$election_date))))
  }
  
  df <- get_nc_snapshot()
  sort(unique(as.Date(df$election_date)))
}


#' Check which NC election dates are missing from the snapshot
#'
#' @param dates vector of Date (election days)
#' @return list(covered=..., missing=...)
#' @export
nc_missing_dates <- function(dates) {
  dates <- as.Date(dates)
  covered <- sort(unique(nc_snapshot_dates()))
  missing <- setdiff(sort(unique(dates)), covered)
  list(covered = covered, missing = missing)
}

#' Get NC election results (snapshot-first)
#'
#' @param election_dates vector of election-day Dates (e.g., as.Date("2024-11-05"))
#' @param source "snapshot" (default) or "snapshot_or_scrape"
#' @param scrape_if_missing logical; only used when source="snapshot_or_scrape"
#' @return data.frame
#' @export
get_nc_results <- function(election_dates,
                           source = c("snapshot", "snapshot_or_scrape"),
                           scrape_if_missing = TRUE) {
  source <- match.arg(source)
  election_dates <- as.Date(election_dates)
  
  snap <- get_nc_snapshot()
  snap$election_date <- as.Date(snap$election_date)
  
  have <- snap[snap$election_date %in% election_dates, , drop = FALSE]
  
  # snapshot-only mode
  if (source == "snapshot") {
    missing <- setdiff(unique(election_dates), unique(have$election_date))
    if (length(missing)) {
      stop(
        "Snapshot is missing election_dates: ",
        paste(format(missing), collapse = ", "),
        "\nUse source='snapshot_or_scrape' to scrape missing dates."
      )
    }
    return(have)
  }
  
  # snapshot_or_scrape mode (Step 3 will implement scraping; for now, just error clearly)
  missing <- setdiff(unique(election_dates), unique(have$election_date))
  if (length(missing) && scrape_if_missing) {
    stop(
      "Scraping from R is not wired yet. Missing election_dates: ",
      paste(format(missing), collapse = ", "),
      "\nNext step is Step 3: call Python pipeline for missing dates."
    )
  }
  
  have
}

