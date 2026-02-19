# R/scraper_registry.R
#
# R connector to the Python scraper registry (inst/python/registry.py).
#
# One primary exported function: scrape_elections()
# Internal helpers (.db_registry, .scrape_nc, .scrape_election_stats,
# .scrape_ballotpedia) handle source-specific argument shaping.
# Utility exports db_list_sources() / db_list_states() aid discoverability.


# ── Internal helpers ──────────────────────────────────────────────────────────

#' Import the Python registry module (after ensuring Python is ready)
#' @keywords internal
.db_registry <- function() {
  db_bind_python()
  reticulate::import("registry", delay_load = FALSE)
}


#' Call the NC results scraper
#' @keywords internal
.scrape_nc <- function(date = NULL) {
  .db_registry()$scrape("nc_results", date = date)
}


#' Call the ElectionStats scraper
#' @keywords internal
.scrape_election_stats <- function(
    state,
    year_from = 1789L,
    year_to   = NULL,
    level     = "all",
    parallel  = FALSE) {

  level <- match.arg(level, c("all", "state", "county", "joined"))
  .db_registry()$scrape(
    "election_stats",
    state     = state,
    year_from = as.integer(year_from),
    year_to   = year_to,        # NULL becomes Python None via reticulate
    level     = level,
    parallel  = parallel
  )
}


#' Call the Ballotpedia school board scraper
#' @keywords internal
.scrape_ballotpedia <- function(
    year       = NULL,
    state      = NULL,
    mode       = "districts",
    start_year = 2013L,
    end_year   = NULL) {

  mode <- match.arg(mode, c("districts", "results", "joined"))
  .db_registry()$scrape(
    "ballotpedia",
    year       = year,
    state      = state,
    mode       = mode,
    start_year = as.integer(start_year),
    end_year   = end_year       # NULL becomes Python None via reticulate
  )
}


# ── Primary exported function ─────────────────────────────────────────────────

#' Scrape election data from a registered source
#'
#' A single entry point that routes to the appropriate Python scraper based on
#' \code{source}. Use \code{db_list_sources()} to see available sources and
#' \code{db_list_states(source)} to see supported states.
#'
#' @param source Which data source to use. One of:
#'   \describe{
#'     \item{"election_stats"}{Multi-state ElectionStats scraper (VA, MA, CO, NH, SC, NM, NY).}
#'     \item{"ballotpedia"}{Ballotpedia school board elections (all US states, 2013–present).}
#'     \item{"nc_results"}{North Carolina local election results.}
#'   }
#'
#' @section ElectionStats arguments (\code{source = "election_stats"}):
#' \describe{
#'   \item{\code{state}}{State name, e.g. \code{"virginia"}. See \code{db_list_states("election_stats")}.}
#'   \item{\code{year_from}}{Start year (default \code{1789}).}
#'   \item{\code{year_to}}{End year, inclusive (default: current calendar year).}
#'   \item{\code{level}}{What to return:
#'     \code{"all"} (default, named list with \code{$state} and \code{$county} data frames),
#'     \code{"state"}, \code{"county"}, or \code{"joined"}.}
#'   \item{\code{parallel}}{Use parallel county scraping for classic (requests-based) states.}
#' }
#'
#' @section Ballotpedia arguments (\code{source = "ballotpedia"}):
#' \describe{
#'   \item{\code{year}}{Election year (e.g. \code{2024}).
#'     Required for \code{mode = "results"} or \code{mode = "joined"}.}
#'   \item{\code{state}}{Filter to one state name (e.g. \code{"Alabama"}), or \code{NULL} for all.}
#'   \item{\code{mode}}{\code{"districts"} (default, fast district metadata),
#'     \code{"results"} (follows each district for candidate data),
#'     or \code{"joined"} (districts + candidates merged).}
#'   \item{\code{start_year}}{Earliest year for multi-year district scrape (default \code{2013}).}
#'   \item{\code{end_year}}{Latest year for multi-year district scrape (default: current year).}
#' }
#'
#' @section NC Results arguments (\code{source = "nc_results"}):
#' \describe{
#'   \item{\code{date}}{Election date string (e.g. \code{"2024-11-05"}), or \code{NULL} for all.}
#' }
#'
#' @return A \code{data.frame}, or a named list with elements \code{$state} and
#'   \code{$county} when \code{level = "all"} for \code{source = "election_stats"}.
#'
#' @examples
#' \dontrun{
#' # List available sources and states
#' db_list_sources()
#' db_list_states("election_stats")
#'
#' # Ballotpedia — district metadata for one year and state (fast)
#' df <- scrape_elections("ballotpedia", year = 2024, state = "Alabama")
#'
#' # ElectionStats — state-level results
#' df <- scrape_elections("election_stats",
#'                        state = "virginia", year_from = 2023, year_to = 2023,
#'                        level = "state")
#'
#' # ElectionStats — both state and county levels
#' res <- scrape_elections("election_stats",
#'                         state = "virginia", year_from = 2023, year_to = 2023)
#' res$state   # candidate-level data frame
#' res$county  # county vote breakdown data frame
#'
#' # North Carolina results
#' df <- scrape_elections("nc_results", date = "2024-11-05")
#' }
#'
#' @export
scrape_elections <- function(
    source     = c("election_stats", "ballotpedia", "nc_results"),
    # ElectionStats args
    state      = NULL,
    year_from  = 1789L,
    year_to    = NULL,
    level      = c("all", "state", "county", "joined"),
    parallel   = FALSE,
    # Ballotpedia args
    year       = NULL,
    mode       = c("districts", "results", "joined"),
    start_year = 2013L,
    end_year   = NULL,
    # NC args
    date       = NULL) {

  source <- match.arg(source)
  level  <- match.arg(level)
  mode   <- match.arg(mode)

  result <- switch(
    source,
    "election_stats" = .scrape_election_stats(
      state     = state,
      year_from = year_from,
      year_to   = year_to,
      level     = level,
      parallel  = parallel
    ),
    "ballotpedia" = .scrape_ballotpedia(
      year       = year,
      state      = state,
      mode       = mode,
      start_year = start_year,
      end_year   = end_year
    ),
    "nc_results" = .scrape_nc(date = date)
  )

  reticulate::py_to_r(result)
}


# ── Utility exports ───────────────────────────────────────────────────────────

#' List all registered Python scraper sources
#'
#' @return Character vector of source names.
#' @export
db_list_sources <- function() {
  unlist(.db_registry()$list_sources())
}


#' List states supported by a scraper source
#'
#' @param source One of the sources returned by \code{db_list_sources()}.
#' @return Character vector of state names or codes.
#' @export
db_list_states <- function(source) {
  unlist(.db_registry()$list_states(source))
}
