#' Get local election results (snapshot-first, multi-state)
#'
#' @param state Two-letter abbreviation or full name.
#' @param start_date,end_date Optional Date range.
#' @param source "auto" (default), "snapshot", or "scrape".
#' @export
get_local_elections <- function(
    state,
    start_date = NULL,
    end_date = NULL,
    source = c("auto", "snapshot", "scrape")
) {
  source <- match.arg(source)
  
  st   <- .normalize_state(state)
  impl <- .get_state_impl(st)
  
  start_date <- .parse_date_or_null(start_date)
  end_date   <- .parse_date_or_null(end_date)
  
  # ---- load snapshot (baseline) ----
  df_snap <- impl$snapshot()
  
  if (!("election_date" %in% names(df_snap))) {
    stop("Snapshot data is missing required column `election_date`.", call. = FALSE)
  }
  df_snap$election_date <- as.Date(df_snap$election_date)
  
  # ---- filtering helper ----
  .filter_range <- function(df) {
    if (!is.null(start_date)) df <- df[df$election_date >= start_date, , drop = FALSE]
    if (!is.null(end_date))   df <- df[df$election_date <= end_date, , drop = FALSE]
    df
  }
  
  # ---- snapshot-only ----
  if (identical(source, "snapshot")) {
    return(.filter_range(df_snap))
  }
  
  # ---- helper: merge + de-dupe ----
  .merge_dedup <- function(a, b) {
    out <- rbind(a, b)
    out[!duplicated(out), , drop = FALSE]
  }
  
  # ---- scrape-only ----
  if (identical(source, "scrape")) {
    if (is.null(impl$scrape_missing)) {
      stop("Scraping not supported yet for state = '", st, "'.", call. = FALSE)
    }
    
    df_new <- impl$scrape_missing(start_date, end_date)
    if (is.null(df_new) || nrow(df_new) == 0) {
      return(.filter_range(df_snap))
    }
    
    df_new$election_date <- as.Date(df_new$election_date)
    return(.filter_range(.merge_dedup(df_snap, df_new)))
  }
  
  # ---- auto ----
  # Goal: avoid importing Python / doing discovery if snapshot already answers the question.
  
  if (!is.null(impl$scrape_missing)) {
    
    # Fast-path: single-day query that's already present in the snapshot
    if (!is.null(start_date) &&
        !is.null(end_date) &&
        identical(as.Date(start_date), as.Date(end_date)) &&
        as.Date(start_date) %in% df_snap$election_date) {
      return(.filter_range(df_snap))
    }
    
    # Fast-path: requested range is fully within the snapshot's min/max coverage
    # (assumes snapshot is reasonably dense within its time span; still safe because we filter)
    snap_min <- suppressWarnings(min(df_snap$election_date, na.rm = TRUE))
    snap_max <- suppressWarnings(max(df_snap$election_date, na.rm = TRUE))
    
    if (is.finite(snap_min) && is.finite(snap_max)) {
      sd <- start_date %||% snap_min
      ed <- end_date   %||% snap_max
      
      if (!is.null(sd) && !is.null(ed) && sd >= snap_min && ed <= snap_max) {
        return(.filter_range(df_snap))
      }
    }
    
    # Only now do we do state-specific discovery of missing dates (can touch Python)
    if (identical(st, "NC")) {
      miss <- .nc_missing_election_dates(start_date, end_date)
      
      if (length(miss$missing) == 0) {
        return(.filter_range(df_snap))
      }
    }
    
    # Scrape + merge (range-based scrape for now)
    df_new <- impl$scrape_missing(start_date, end_date)
    
    if (!is.null(df_new) && nrow(df_new) > 0) {
      df_new$election_date <- as.Date(df_new$election_date)
      return(.filter_range(.merge_dedup(df_snap, df_new)))
    }
  }
  
  # Fallback: snapshot only
  .filter_range(df_snap)
}
