# R/db_query.R
#
# Exported utility functions for querying available sources, states, and year
# ranges without running a full scrape.


#' List all registered Python scraper sources
#'
#' @return Character vector of source names.
#' @export
db_list_sources <- function() {
  unlist(.db_registry()$list_sources())
}


#' List states supported by DownBallotR scrapers
#'
#' @param source One of the sources returned by \code{db_list_sources()}, or
#'   \code{NULL} (default) to return all states with dedicated scrapers across
#'   all sources (Ballotpedia sources, which accept any US state via a
#'   \code{state=} filter, are excluded from this aggregation).
#' @return Named character vector of canonical state names. When
#'   \code{source = NULL} each element is named by its source; when a single
#'   source is given the names are omitted.
#' @export
db_list_states <- function(source = NULL) {
  reg <- .db_registry()
  if (!is.null(source)) {
    states <- unlist(reg$list_states(source))
    return(vapply(states, .normalize_state, character(1L), USE.NAMES = FALSE))
  }

  # Aggregate across all sources, skipping sources with no state list
  # (Ballotpedia sources accept any US state via a state= parameter and do not
  # enumerate individual states).
  all_sources <- unlist(reg$list_sources())
  result <- unlist(lapply(all_sources, function(src) {
    states <- unlist(reg$list_states(src))
    if (length(states) == 0L) return(NULL)
    normalized <- vapply(states, .normalize_state, character(1L), USE.NAMES = FALSE)
    stats::setNames(normalized, rep(src, length(normalized)))
  }))
  result[!duplicated(result)]
}


#' Show data availability for election scrapers
#'
#' Returns a data frame listing the earliest available year for each state and
#' scraper source tracked by DownBallotR. All sources include data through the
#' current calendar year.
#'
#' @param state Optional state name to filter results (e.g. \code{"Virginia"}).
#'   Pass \code{NULL} (default) to return all states for the chosen source(s).
#' @param office Type of election, matching \code{scrape_elections()}.
#'   \code{"general"} (default) returns availability for ElectionStats states,
#'   North Carolina, Connecticut, and Georgia; \code{"school_district"} returns
#'   Ballotpedia school board availability; \code{"state_elections"} returns
#'   Ballotpedia state election availability; \code{"municipal_elections"}
#'   returns Ballotpedia municipal and mayoral election availability.
#'
#' @return A \code{data.frame} with columns \code{source}, \code{state},
#'   \code{start_year}, and \code{end_year}.
#'
#' @examples
#' \dontrun{
#' # General election sources
#' db_available_years()
#'
#' # School district (Ballotpedia)
#' db_available_years(office = "school_district")
#'
#' # State elections (Ballotpedia)
#' db_available_years(office = "state_elections")
#'
#' # Municipal and mayoral elections (Ballotpedia)
#' db_available_years(office = "municipal_elections")
#'
#' # Filter to one state
#' db_available_years(state = "Virginia")
#' }
#'
#' @export
db_available_years <- function(state = NULL,
                               office = c("general", "school_district",
                                          "state_elections", "municipal_elections")) {
  office <- match.arg(office)
  reg    <- .db_registry()

  if (office == "school_district") {
    avail <- reg$get_available_years("ballotpedia")
    return(data.frame(
      source     = "ballotpedia",
      state      = "All US states",
      start_year = avail$start_year,
      end_year   = avail$end_year,
      stringsAsFactors = FALSE
    ))
  }

  if (office == "state_elections") {
    avail <- reg$get_available_years("ballotpedia_elections")
    return(data.frame(
      source     = "ballotpedia_elections",
      state      = "All US states (where page exists)",
      start_year = avail$start_year,
      end_year   = avail$end_year,
      stringsAsFactors = FALSE
    ))
  }

  if (office == "municipal_elections") {
    avail <- reg$get_available_years("ballotpedia_municipal")
    return(data.frame(
      source     = "ballotpedia_municipal",
      state      = "All US states (race_type='all': 2014+; 'mayoral': 2020+)",
      start_year = avail$start_year,
      end_year   = avail$end_year,
      stringsAsFactors = FALSE
    ))
  }

  # General elections: ElectionStats + NC + CT + GA
  es_states <- db_list_states("election_stats")
  es_rows <- lapply(es_states, function(s) {
    avail <- reg$get_available_years("election_stats", state = s)
    data.frame(
      source     = "election_stats",
      state      = .normalize_state(s),
      start_year = avail$start_year,
      end_year   = avail$end_year,
      stringsAsFactors = FALSE
    )
  })
  es_df <- do.call(rbind, es_rows)

  nc_avail <- reg$get_available_years("northcarolina_results")
  nc_row <- data.frame(
    source     = "northcarolina_results",
    state      = "North Carolina",
    start_year = nc_avail$start_year,
    end_year   = nc_avail$end_year,
    stringsAsFactors = FALSE
  )

  ct_avail <- reg$get_available_years("connecticut_results")
  ct_row <- data.frame(
    source     = "connecticut_results",
    state      = "Connecticut",
    start_year = ct_avail$start_year,
    end_year   = ct_avail$end_year,
    stringsAsFactors = FALSE
  )

  ga_avail <- reg$get_available_years("georgia_results")
  ga_row <- data.frame(
    source     = "georgia_results",
    state      = "Georgia",
    start_year = ga_avail$start_year,
    end_year   = ga_avail$end_year,
    stringsAsFactors = FALSE
  )

  ut_avail <- reg$get_available_years("utah_results")
  ut_row <- data.frame(
    source     = "utah_results",
    state      = "Utah",
    start_year = ut_avail$start_year,
    end_year   = ut_avail$end_year,
    stringsAsFactors = FALSE
  )

  la_avail <- reg$get_available_years("louisiana_results")
  la_row <- data.frame(
    source     = "louisiana_results",
    state      = "Louisiana",
    start_year = la_avail$start_year,
    end_year   = la_avail$end_year,
    stringsAsFactors = FALSE
  )

  in_avail <- reg$get_available_years("indiana_results")
  in_row <- data.frame(
    source     = "indiana_results",
    state      = "Indiana",
    start_year = in_avail$start_year,
    end_year   = in_avail$end_year,
    stringsAsFactors = FALSE
  )

  result <- rbind(es_df, nc_row, ct_row, ga_row, ut_row, la_row, in_row)

  if (!is.null(state)) {
    .stop_if_not_scalar(state, "state")
    state_norm <- .normalize_state(state)
    .check_state_recognized(state_norm)
    state_key  <- .state_to_es_key(state_norm)
    result_key <- vapply(result$state, function(s) .state_to_es_key(.normalize_state(s)),
                         character(1L))
    result <- result[result_key == state_key, , drop = FALSE]
  }

  rownames(result) <- NULL
  result
}
