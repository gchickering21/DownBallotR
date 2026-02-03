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
  
  st <- .normalize_state(state)
  impl <- .get_state_impl(st)
  
  start_date <- .parse_date_or_null(start_date)
  end_date   <- .parse_date_or_null(end_date)
  
  # always load snapshot if available
  df_snap <- impl$snapshot()
  if (!("election_date" %in% names(df_snap))) {
    stop("Snapshot data is missing required column `election_date`.", call. = FALSE)
  }
  df_snap$election_date <- as.Date(df_snap$election_date)
  
  # filter helper
  .filter_range <- function(df) {
    if (!is.null(start_date)) df <- df[df$election_date >= start_date, , drop = FALSE]
    if (!is.null(end_date))   df <- df[df$election_date <= end_date, , drop = FALSE]
    df
  }
  
  if (source == "snapshot") {
    return(.filter_range(df_snap))
  }
  
  if (source == "scrape") {
    if (is.null(impl$scrape_missing)) {
      stop("Scraping not supported yet for state = '", st, "'.", call. = FALSE)
    }
    df_new <- impl$scrape_missing(start_date, end_date)
    if (is.null(df_new)) return(.filter_range(df_snap))
    df_new$election_date <- as.Date(df_new$election_date)
    out <- rbind(df_snap, df_new)
    out <- out[!duplicated(out), , drop = FALSE]
    return(.filter_range(out))
  }
  
  # ---- auto ----
  # 1) Check if snapshot covers the *discovered* election dates for that range
  if (!is.null(impl$scrape_missing)) {
    miss <- .nc_missing_election_dates(start_date, end_date)  # currently NC-specific
    if (length(miss$missing) == 0) {
      return(.filter_range(df_snap))
    }
    
    # 2) scrape and merge
    df_new <- impl$scrape_missing(start_date, end_date)
    if (!is.null(df_new)) {
      df_new$election_date <- as.Date(df_new$election_date)
      out <- rbind(df_snap, df_new)
      out <- out[!duplicated(out), , drop = FALSE]
      return(.filter_range(out))
    }
  }
  
  # If we can't scrape for that state, fall back to snapshot
  .filter_range(df_snap)
}
