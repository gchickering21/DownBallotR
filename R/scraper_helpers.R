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


# ── Source metadata ───────────────────────────────────────────────────────────

# Valid 'level' values per source; NULL = source ignores 'level' entirely
.SOURCE_LEVELS <- list(
  election_stats        = c("all", "state", "county", "precinct"),
  northcarolina_results = c("all", "precinct", "county", "state"),
  connecticut_results   = c("all", "state", "town"),
  georgia_results       = c("all", "state", "county", "precinct"),
  utah_results          = c("all", "state", "county", "precinct"),
  indiana_results       = c("all", "state", "county"),
  louisiana_results     = c("all", "state", "parish")
)

# Sources that use max_workers for sub-unit parallelism (county/town/parish)
.USES_MAX_WORKERS <- c("georgia_results", "utah_results",
                        "connecticut_results", "louisiana_results")

# Maps canonical state names to their dedicated source (checked before ElectionStats)
.STATE_ROUTES <- c(
  "North Carolina" = "northcarolina_results",
  "Connecticut"    = "connecticut_results",
  "Georgia"        = "georgia_results",
  "Utah"           = "utah_results",
  "Indiana"        = "indiana_results",
  "Louisiana"      = "louisiana_results"
)


#' Human-readable label for a source, used in messages and errors
#' @keywords internal
.source_label <- function(source, state = NULL) {
  switch(source,
    election_stats        = paste0(state, " (ElectionStats)"),
    northcarolina_results = "North Carolina (NC State Board of Elections)",
    connecticut_results   = "Connecticut (CTEMS)",
    georgia_results       = "Georgia (GA Secretary of State)",
    utah_results          = "Utah (electionresults.utah.gov)",
    indiana_results       = "Indiana (enr.indianavoters.in.gov)",
    louisiana_results     = "Louisiana (voterportal.sos.la.gov)",
    source
  )
}


#' Route office + normalised state to a registry source name
#' @keywords internal
.route_to_source <- function(state) {
  if (!is.null(state) && state %in% names(.STATE_ROUTES))
    return(unname(.STATE_ROUTES[state]))
  "election_stats"
}


#' Emit source availability and unconfirmed-year notice
#' @keywords internal
.emit_availability <- function(source, state, year_to) {
  label <- .source_label(source, state)
  avail <- tryCatch(
    .db_registry()$get_available_years(
      source = source,
      state  = if (source == "election_stats") .state_to_es_key(state) else NULL
    ),
    error = function(e) NULL
  )
  if (is.null(avail)) return(invisible(NULL))

  message("Available years for ", label, ": ",
          avail$start_year, "\u2013", avail$end_year)

  if (!is.null(year_to) && year_to > avail$end_year) {
    message(
      "\nNote: ", year_to, " is beyond the last confirmed year (",
      avail$end_year, ") for ", label, ".\n",
      "  This year has not been verified. DownBallotR will attempt the scrape,\n",
      "  but results are not guaranteed.\n",
      "  If you encounter problems, please file a report at:\n",
      "  https://github.com/gchickering21/DownBallotR/issues"
    )
  }
  invisible(avail)
}


#' Assign each element of a list result into the caller's environment
#'
#' When level = "all" returns a named list of data frames, this assigns each
#' frame into the caller's environment with a state-prefixed name
#' (e.g. \code{ga_state}, \code{ga_county}).
#' @keywords internal
.assign_list_result <- function(result, state, caller_env) {
  prefix    <- .state_to_abbrev(state)
  var_names <- paste0(prefix, "_", names(result))
  for (i in seq_along(result)) {
    assign(var_names[[i]], result[[i]], envir = caller_env)
  }
  message("Created: ", paste(var_names, collapse = ", "))
  invisible(result)
}


# ── Per-source scraper helpers ────────────────────────────────────────────────

#' Call the NC results scraper
#' @keywords internal
.scrape_nc <- function(year_from = NULL, year_to = NULL, level = "all") {
  level <- match.arg(level, c("all", "precinct", "county", "state"))
  .db_registry()$scrape(
    "northcarolina_results",
    year_from = year_from,
    year_to   = year_to,
    level     = level
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

  level <- match.arg(level, c("all", "state", "county", "precinct"))
  .db_registry()$scrape(
    "election_stats",
    state     = state,
    year_from = as.integer(year_from),
    year_to   = year_to,        # NULL becomes Python None via reticulate
    level     = level,
    parallel  = parallel
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
  level <- match.arg(level, c("all", "state", "county", "precinct"))
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
    year_from          = NULL,
    year_to            = NULL,
    level              = "all",
    max_county_workers = 4L) {
  level <- match.arg(level, c("all", "state", "county", "precinct"))
  .db_registry()$scrape(
    "utah_results",
    year_from          = year_from,
    year_to            = year_to,
    level              = level,
    max_county_workers = as.integer(max_county_workers)
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


#' Call the Louisiana Secretary of State election results scraper
#' @keywords internal
.scrape_la <- function(
    year_from          = NULL,
    year_to            = NULL,
    level              = "all",
    max_parish_workers = 2L) {
  level <- match.arg(level, c("all", "state", "parish"))
  .db_registry()$scrape(
    "louisiana_results",
    year_from          = year_from,
    year_to            = year_to,
    level              = level,
    max_parish_workers = as.integer(max_parish_workers)
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
