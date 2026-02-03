# R/coverage_helpers.R

#' @keywords internal
.parse_date_or_null <- function(x) {
  if (is.null(x)) return(NULL)
  as.Date(x)
}

#' @keywords internal
.requested_election_dates <- function(covered_dates, start_date, end_date) {
  # interpret "range request" as: election dates that exist in snapshot universe
  # within [start_date, end_date]. (For now.)
  # Later: use Python discovery to get the true universe.
  start_date <- start_date %||% min(covered_dates, na.rm = TRUE)
  end_date   <- end_date   %||% max(covered_dates, na.rm = TRUE)
  
  covered_dates[covered_dates >= start_date & covered_dates <= end_date]
}

`%||%` <- function(a, b) if (is.null(a)) b else a

#' @keywords internal
.compute_missing_dates <- function(covered_dates, start_date = NULL, end_date = NULL) {
  covered_dates <- sort(unique(as.Date(covered_dates)))
  
  start_date <- .parse_date_or_null(start_date)
  end_date   <- .parse_date_or_null(end_date)
  
  requested <- .requested_election_dates(covered_dates, start_date, end_date)
  missing <- setdiff(requested, covered_dates) # usually empty with current definition
  # NOTE: right now this will always be empty because requested derives from covered.
  # Next step: swap requested source to Python discovery (true universe).
  list(covered = covered_dates, requested = requested, missing = missing)
}
