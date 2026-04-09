# R/scraper_helpers.R
#
# Internal per-source scraper functions. Each function shapes arguments for
# one Python backend and calls registry.scrape() via reticulate.
#
# To add a new source: add a .scrape_<source>() function here, then add
# routing + dispatch in scrape_elections.R.


# ── Python registry binding ───────────────────────────────────────────────────

#' Import the Python registry module (after ensuring Python is ready)
#' @keywords internal
.db_registry <- function() {
  db_bind_python()
  reticulate::import("registry", delay_load = FALSE)
}


# ── Per-source scraper helpers ────────────────────────────────────────────────

#' Call the NC results scraper
#' @keywords internal
.scrape_nc <- function(year_from = NULL, year_to = NULL) {
  .db_registry()$scrape(
    "northcarolina_results",
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


#' Call the Ballotpedia municipal/mayoral elections scraper
#' @keywords internal
.scrape_ballotpedia_municipal <- function(
    year       = NULL,
    state      = NULL,
    race_type  = "all",
    mode       = "links",
    start_year = 2014L,
    end_year   = NULL) {

  race_type <- match.arg(race_type, c("all", "mayoral"))
  mode      <- match.arg(mode, c("links", "results"))
  .db_registry()$scrape(
    "ballotpedia_municipal",
    year       = year,
    state      = state,
    race_type  = race_type,
    mode       = mode,
    start_year = as.integer(start_year),
    end_year   = end_year        # NULL becomes Python None via reticulate
  )
}


#' Call the Ballotpedia state elections scraper
#' @keywords internal
.scrape_ballotpedia_elections <- function(
    year            = NULL,
    state           = NULL,
    mode            = "listings",
    election_level  = "all",
    start_year      = 2024L,
    end_year        = NULL) {

  mode            <- match.arg(mode, c("listings", "results"))
  election_level  <- match.arg(election_level, c("all", "federal", "state", "local"))
  .db_registry()$scrape(
    "ballotpedia_elections",
    year            = year,
    state           = state,
    mode            = mode,
    election_level  = election_level,
    start_year      = as.integer(start_year),
    end_year        = end_year        # NULL becomes Python None via reticulate
  )
}


#' Call the Georgia SOS election results scraper
#' @keywords internal
.scrape_ga <- function(
    year_from            = NULL,
    year_to              = NULL,
    level                = "all",
    max_county_workers   = 4L,
    include_vote_methods = FALSE) {
  level <- match.arg(level, c("all", "state", "county"))
  .db_registry()$scrape(
    "georgia_results",
    year_from            = year_from,
    year_to              = year_to,
    level                = level,
    max_county_workers   = as.integer(max_county_workers),
    include_vote_methods = isTRUE(include_vote_methods)
  )
}


#' Call the Utah election results scraper
#' @keywords internal
.scrape_ut <- function(
    year_from            = NULL,
    year_to              = NULL,
    level                = "all",
    max_county_workers   = 4L,
    include_vote_methods = FALSE) {
  level <- match.arg(level, c("all", "state", "county"))
  .db_registry()$scrape(
    "utah_results",
    year_from            = year_from,
    year_to              = year_to,
    level                = level,
    max_county_workers   = as.integer(max_county_workers),
    include_vote_methods = isTRUE(include_vote_methods)
  )
}


#' Call the Indiana General Election results scraper
#' @keywords internal
.scrape_in <- function(
    year_from = NULL,
    year_to   = NULL,
    level     = "all") {
  level <- match.arg(level, c("all", "state", "county"))
  .db_registry()$scrape(
    "indiana_results",
    year_from = year_from,
    year_to   = year_to,
    level     = level
  )
}


#' Call the Connecticut CTEMS election results scraper
#' @keywords internal
.scrape_ct <- function(
    year_from        = NULL,
    year_to          = NULL,
    level            = "all",
    max_town_workers = 2L) {
  level <- match.arg(level, c("all", "state", "town"))
  .db_registry()$scrape(
    "connecticut_results",
    year_from        = year_from,
    year_to          = year_to,
    level            = level,
    max_town_workers = as.integer(max_town_workers)
  )
}
