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
.scrape_nc <- function(year_from = NULL, year_to = NULL) {
  .db_registry()$scrape(
    "nc_results",
    year_from = year_from,
    year_to   = year_to
  )
}


#' Call the ElectionStats scraper
#' @keywords internal
.scrape_election_stats <- function(
    state,
    year_from = 1789L,
    year_to   = NULL,
    level     = "all",
    parallel  = TRUE) {

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


# NC state identifiers (case-insensitive) recognised for auto-routing
.nc_state_keys <- c("nc", "north carolina", "north_carolina")


#' Coerce a year value to integer, accepting numeric, string, or NULL
#' @keywords internal
.to_year <- function(x) {
  if (is.null(x)) return(NULL)
  val <- suppressWarnings(as.integer(as.numeric(as.character(x))))
  if (is.na(val)) stop("Cannot convert year value ", deparse(x), " to an integer.")
  val
}


# ── Primary exported function ─────────────────────────────────────────────────

#' Scrape election data
#'
#' A single entry point that automatically routes to the appropriate scraper
#' based on \code{state} and \code{office}. Use \code{db_list_states("election_stats")}
#' to see states supported by the general-election scraper.
#'
#' Routing rules (applied in order):
#' \enumerate{
#'   \item \code{office = "school_district"} → Ballotpedia school board scraper
#'         (all US states; use \code{state} to filter to one state).
#'   \item \code{state} matches North Carolina (e.g. \code{"NC"},
#'         \code{"north_carolina"}) → NC State Board of Elections scraper.
#'   \item All other states → ElectionStats multi-state scraper.
#' }
#'
#' @param state State name used both for routing and filtering. For the
#'   general-election scraper use snake_case (e.g. \code{"virginia"});
#'   for the school-district scraper use title-case (e.g. \code{"Alabama"}),
#'   or \code{NULL} to return all states. North Carolina can be passed as
#'   \code{"NC"}, \code{"north_carolina"}, or \code{"north carolina"}.
#' @param office Type of election to retrieve. \code{"general"} (default)
#'   fetches general election results via ElectionStats or the NC scraper;
#'   \code{"school_district"} fetches school board elections via Ballotpedia.
#' @param year_from (\code{general} / NC) Start year, inclusive (default
#'   \code{NULL}). When \code{NULL}, ElectionStats starts at \code{1789};
#'   the NC scraper applies no lower bound.
#' @param year_to (\code{general} / NC) End year, inclusive (default
#'   \code{NULL}). When \code{NULL}, ElectionStats uses the current calendar
#'   year; the NC scraper applies no upper bound.
#' @param level (\code{general} / ElectionStats) What to return.
#'   \code{"all"} (default) returns a named list with \code{$state} and
#'   \code{$county} data frames; \code{"state"} returns candidate-level
#'   results; \code{"county"} returns county vote breakdowns; \code{"joined"}
#'   returns county rows merged with candidate metadata.
#' @param parallel (\code{general} / ElectionStats) Use parallel county
#'   scraping for classic (requests-based) states (default \code{TRUE}).
#'   Ignored automatically for Playwright-based states (SC, NM, NY).
#' @param year (\code{school_district}) Election year (e.g. \code{2024}).
#'   Required when \code{mode = "results"} or \code{mode = "joined"}. If
#'   \code{NULL} with \code{mode = "districts"}, use \code{start_year} /
#'   \code{end_year} for a multi-year scrape.
#' @param mode (\code{school_district}) What to return. \code{"districts"}
#'   (default) returns fast district metadata (one request per year-page);
#'   \code{"results"} follows each district URL for candidate/vote data;
#'   \code{"joined"} returns districts and candidates merged into one data
#'   frame.
#' @param start_year (\code{school_district}) Earliest year for a multi-year
#'   district scrape when \code{year} is \code{NULL} (default \code{2013}).
#' @param end_year (\code{school_district}) Latest year for a multi-year
#'   district scrape when \code{year} is \code{NULL} (default: current
#'   calendar year).
#'
#' @return A \code{data.frame}, or a named list with elements \code{$state}
#'   and \code{$county} when \code{level = "all"} and \code{office =
#'   "general"} for a non-NC state.
#'
#' @examples
#' \dontrun{
#' # General election results — Virginia
#' df <- scrape_elections(state = "virginia", year_from = 2023, year_to = 2023,
#'                        level = "state")
#'
#' # General election results — Virginia, both state and county levels
#' res <- scrape_elections(state = "virginia", year_from = 2023, year_to = 2023)
#' res$state   # candidate-level data frame
#' res$county  # county vote breakdown data frame
#'
#' # North Carolina — single year
#' df <- scrape_elections(state = "NC", year_from = 2024, year_to = 2024)
#'
#' # North Carolina — multi-year range
#' df <- scrape_elections(state = "NC", year_from = 2022, year_to = 2024)
#'
#' # School district elections — one state, one year (fast district metadata)
#' df <- scrape_elections(state = "Alabama", office = "school_district",
#'                        year = 2024)
#'
#' # School district elections — all states, multi-year
#' df <- scrape_elections(office = "school_district",
#'                        start_year = 2020, end_year = 2024)
#' }
#'
#' @export
scrape_elections <- function(
    state      = NULL,
    office     = c("general", "school_district"),
    # General-election (ElectionStats / NC) args
    year_from  = NULL,
    year_to    = NULL,
    level      = c("all", "state", "county", "joined"),
    parallel   = TRUE,
    # School-district (Ballotpedia) args
    year       = NULL,
    mode       = c("districts", "results", "joined"),
    start_year = 2013L,
    end_year   = NULL) {

  office <- match.arg(office)
  level  <- match.arg(level)
  mode   <- match.arg(mode)

  # Guard against old source= positional usage (e.g. scrape_elections("ballotpedia", ...))
  if (!is.null(state) &&
      tolower(trimws(state)) %in% c("ballotpedia", "election_stats", "nc_results")) {
    stop(
      "The 'source' argument has been removed. Route by state instead:\n",
      "  - For school board elections:  office = \"school_district\"\n",
      "  - For NC results:              state = \"NC\"\n",
      "  - For other states:            state = \"virginia\"  (or any ElectionStats state)",
      call. = FALSE
    )
  }

  year_from <- .to_year(year_from)
  year_to   <- .to_year(year_to)

  # ── Auto-route ──────────────────────────────────────────────────────────────
  source <- if (office == "school_district") {
    "ballotpedia"
  } else if (!is.null(state) &&
             tolower(trimws(state)) %in% .nc_state_keys) {
    "nc_results"
  } else {
    "election_stats"
  }

  # ── Print available year range for chosen source/state ────────────────────
  tryCatch({
    avail <- .db_registry()$get_available_years(
      source = source,
      state  = if (source == "election_stats") state else NULL
    )
    label <- switch(
      source,
      "ballotpedia"    = "school district elections (Ballotpedia)",
      "nc_results"     = "North Carolina (NC State Board of Elections)",
      "election_stats" = paste0(state, " (ElectionStats)")
    )
    message("Available years for ", label, ": ",
            avail$start_year, "\u2013", avail$end_year)
  }, error = function(e) NULL)  # silently skip if year registry lookup fails

  result <- switch(
    source,
    "election_stats" = .scrape_election_stats(
      state     = state,
      year_from = if (is.null(year_from)) 1789L else year_from,
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
    "nc_results" = .scrape_nc(
      year_from = year_from,
      year_to   = year_to
    )
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


#' Show data availability for election scrapers
#'
#' Returns a data frame listing the earliest available year for each state and
#' scraper source tracked by DownBallotR. All sources include data through the
#' current calendar year.
#'
#' @param state Optional state name to filter results (e.g. \code{"virginia"}).
#'   Pass \code{NULL} (default) to return all states for the chosen source(s).
#' @param office Type of election, matching \code{scrape_elections()}.
#'   \code{"general"} (default) returns availability for ElectionStats states
#'   and North Carolina; \code{"school_district"} returns Ballotpedia
#'   availability.
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
#' # Filter to one state
#' db_available_years(state = "virginia")
#' }
#'
#' @export
db_available_years <- function(state = NULL, office = c("general", "school_district")) {
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

  # General elections: ElectionStats states + NC
  es_states <- db_list_states("election_stats")
  es_rows <- lapply(es_states, function(s) {
    avail <- reg$get_available_years("election_stats", state = s)
    data.frame(
      source     = "election_stats",
      state      = s,
      start_year = avail$start_year,
      end_year   = avail$end_year,
      stringsAsFactors = FALSE
    )
  })
  es_df <- do.call(rbind, es_rows)

  nc_avail <- reg$get_available_years("nc_results")
  nc_row <- data.frame(
    source     = "nc_results",
    state      = "NC",
    start_year = nc_avail$start_year,
    end_year   = nc_avail$end_year,
    stringsAsFactors = FALSE
  )

  result <- rbind(es_df, nc_row)

  if (!is.null(state)) {
    state_key <- tolower(trimws(state))
    result <- result[
      tolower(result$state) == state_key | result$state == state, ,
      drop = FALSE
    ]
  }

  rownames(result) <- NULL
  result
}
