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
#' @param source Which data source to use. One of \code{"election_stats"},
#'   \code{"ballotpedia"}, or \code{"nc_results"}.
#'   Call \code{db_list_sources()} to see all options.
#' @param state (\code{election_stats} / \code{ballotpedia})
#'   State name. For \code{election_stats}: snake_case key such as
#'   \code{"virginia"} — see \code{db_list_states("election_stats")}. For
#'   \code{ballotpedia}: title-case name such as \code{"Alabama"}, or
#'   \code{NULL} for all states.
#' @param year_from (\code{election_stats}) Start year, inclusive (default \code{1789}).
#' @param year_to (\code{election_stats}) End year, inclusive (default: current
#'   calendar year).
#' @param level (\code{election_stats}) What to return. \code{"all"} (default)
#'   returns a named list with \code{$state} and \code{$county} data frames;
#'   \code{"state"} returns candidate-level results; \code{"county"} returns
#'   county vote breakdowns; \code{"joined"} returns county rows merged with
#'   candidate metadata.
#' @param parallel (\code{election_stats}) Use parallel county scraping for
#'   classic (requests-based) states (default \code{FALSE}).
#' @param year (\code{ballotpedia}) Election year (e.g. \code{2024}). Required
#'   when \code{mode = "results"} or \code{mode = "joined"}. If \code{NULL}
#'   with \code{mode = "districts"}, use \code{start_year} / \code{end_year}
#'   for a multi-year scrape.
#' @param mode (\code{ballotpedia}) What to return. \code{"districts"} (default)
#'   returns fast district metadata (one request per year-page);
#'   \code{"results"} follows each district URL for candidate/vote data;
#'   \code{"joined"} returns districts and candidates merged into one data frame.
#' @param start_year (\code{ballotpedia}) Earliest year for a multi-year
#'   district scrape when \code{year} is \code{NULL} (default \code{2013}).
#' @param end_year (\code{ballotpedia}) Latest year for a multi-year district
#'   scrape when \code{year} is \code{NULL} (default: current calendar year).
#' @param date (\code{nc_results}) Election date string (e.g.
#'   \code{"2024-11-05"}), or \code{NULL} to return all available results.
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
