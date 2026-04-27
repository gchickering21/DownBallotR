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
#'   all sources.
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
#'   Pass \code{NULL} (default) to return all states.
#'
#' @return A \code{data.frame} with columns \code{source}, \code{state},
#'   \code{start_year}, and \code{end_year}.
#'
#' @examples
#' \donttest{
#' # All sources
#' db_available_years()
#'
#' # Filter to one state
#' db_available_years(state = "Virginia")
#' }
#'
#' @export
db_available_years <- function(state = NULL) {
  reg <- .db_registry()

  # ElectionStats states
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

  dedicated_sources <- c(
    northcarolina_results = "North Carolina",
    connecticut_results   = "Connecticut",
    georgia_results       = "Georgia",
    utah_results          = "Utah",
    louisiana_results     = "Louisiana",
    indiana_results       = "Indiana"
  )
  dedicated_df <- do.call(rbind, lapply(names(dedicated_sources), function(src) {
    avail <- reg$get_available_years(src)
    data.frame(
      source     = src,
      state      = dedicated_sources[[src]],
      start_year = avail$start_year,
      end_year   = avail$end_year,
      stringsAsFactors = FALSE
    )
  }))

  result <- rbind(es_df, dedicated_df)
  result <- result[order(result$state), ]

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
