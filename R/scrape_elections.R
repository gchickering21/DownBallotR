# R/scrape_elections.R
#
# The primary exported function: scrape_elections().
# Routing logic lives here; per-source argument shaping is in scraper_helpers.R;
# state normalization utilities are in state_utils.R.


#' Scrape election data
#'
#' A single entry point that automatically routes to the appropriate scraper
#' based on \code{state}. Use \code{db_list_states("election_stats")}
#' to see states supported by the general-election scraper.
#'
#' Routing rules (applied in order):
#' \enumerate{
#'   \item \code{state} matches North Carolina (e.g. \code{"NC"},
#'         \code{"north_carolina"}) → NC State Board of Elections scraper (2000–present).
#'   \item \code{state} matches Connecticut (e.g. \code{"CT"},
#'         \code{"connecticut"}) → Connecticut CTEMS scraper (2016–present).
#'   \item \code{state} matches Georgia (e.g. \code{"GA"},
#'         \code{"georgia"}) → Georgia Secretary of State scraper (2000–present).
#'   \item \code{state} matches Utah (e.g. \code{"UT"}, \code{"utah"}) →
#'         Utah election results scraper (2023–present).
#'   \item \code{state} matches Indiana (e.g. \code{"IN"}, \code{"indiana"}) →
#'         Indiana General Election results scraper (2019–present).
#'   \item \code{state} matches Louisiana (e.g. \code{"LA"}, \code{"louisiana"}) →
#'         Louisiana Secretary of State scraper (1982–present).
#'   \item All other states → ElectionStats multi-state scraper.
#' }
#'
#' @param state State name or 2-letter abbreviation, accepted in any case or
#'   spacing style (e.g. \code{"VA"}, \code{"virginia"}, \code{"Virginia"},
#'   \code{"south_carolina"}, \code{"SC"}).  The value is normalised
#'   automatically before being passed to the underlying scraper, so callers
#'   do not need to worry about the exact format.
#' @param year_from Start year, inclusive (default \code{NULL}). When
#'   \code{NULL}, ElectionStats starts at \code{1789}; all state-portal scrapers
#'   apply no lower bound (data is clamped to each scraper's earliest confirmed
#'   year).
#' @param year_to End year, inclusive (default \code{NULL}). When \code{NULL},
#'   the current calendar year is used as the upper bound.
#' @param level What to return. \code{"all"} (default) returns a named list with
#'   \code{$state}, \code{$county}, and (when available) \code{$precinct} data
#'   frames (ElectionStats); \code{$state} and \code{$county} data frames
#'   (Georgia / Utah / Indiana); \code{$state} and \code{$town} data frames
#'   (Connecticut); \code{$state} and \code{$parish} data frames (Louisiana);
#'   or \code{$precinct}, \code{$county}, and \code{$state} data frames (NC).
#'   \code{"state"} returns statewide candidate-level results only;
#'   \code{"county"} returns county vote breakdowns (ElectionStats / Georgia /
#'   Utah / Indiana);
#'   \code{"precinct"} returns precinct-level vote breakdowns — columns:
#'   \code{state}, \code{election_id}, \code{candidate_id}, \code{county},
#'   \code{precinct}, \code{candidate}, \code{votes}
#'   (ElectionStats classic states: CO, MA, ID; v2 states: SC, NM, VA;
#'   NC via \code{state="NC"});
#'   \code{"town"} returns town-level results only (Connecticut);
#'   \code{"parish"} returns parish-level results only (Louisiana).
#' @param parallel (\code{ElectionStats}) Use parallel county scraping for
#'   classic (requests-based) states (default \code{TRUE}). Ignored
#'   automatically for Playwright-based states (SC, NM, NY, VA).
#' @param max_workers (Georgia / Utah / Connecticut / Louisiana) Maximum number
#'   of parallel Chromium browsers (default \code{4L}). For Georgia and Utah,
#'   controls county-level parallelism; for Connecticut, controls town-level
#'   parallelism; for Louisiana, controls parish-level parallelism (default is
#'   capped at 2 for LA). Ignored for all other states.
#' @param include_vote_methods (Georgia only) If \code{TRUE}, also return a
#'   vote-method breakdown table (Advance in Person, Election Day, Absentee by
#'   Mail, Provisional) for Georgia results (default \code{FALSE}).
#'   Ignored for all other states.
#'
#' @return A \code{data.frame}, or a named list when \code{level = "all"}:
#'   \code{$state} + \code{$county} (+ \code{$precinct} when available) for ElectionStats;
#'   \code{$state} + \code{$county} for Georgia / Utah / Indiana;
#'   \code{$state} + \code{$town} for Connecticut;
#'   \code{$state} + \code{$parish} for Louisiana;
#'   \code{$precinct} + \code{$county} + \code{$state} for North Carolina.
#'   Each component is also assigned directly into the calling environment
#'   (e.g. \code{ga_state}, \code{ga_county}) when \code{level = "all"}.
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
#' # Connecticut — statewide + town results for 2024
#' res <- scrape_elections(state = "CT", year_from = 2024, year_to = 2024)
#' res$state  # statewide totals
#' res$town   # town-level results
#'
#' # Connecticut — statewide only (faster; no town scraping)
#' df <- scrape_elections(state = "CT", year_from = 2024, year_to = 2024,
#'                        level = "state")
#'
#' # Connecticut — with more parallel workers
#' res <- scrape_elections(state = "CT", year_from = 2022, year_to = 2022,
#'                         max_workers = 4L)
#'
#' # Georgia — statewide + county results
#' res <- scrape_elections(state = "GA", year_from = 2024, year_to = 2024)
#'
#' # Georgia — statewide only (faster)
#' df <- scrape_elections(state = "GA", year_from = 2024, year_to = 2024,
#'                        level = "state")
#'
#' # Georgia — with vote-method breakdown
#' res <- scrape_elections(state = "GA", year_from = 2024, year_to = 2024,
#'                         include_vote_methods = TRUE)
#'
#' # Utah — statewide + county results
#' res <- scrape_elections(state = "UT", year_from = 2024, year_to = 2024)
#'
#' # Indiana — General Election results (statewide + county)
#' res <- scrape_elections(state = "IN", year_from = 2024, year_to = 2024)
#' res$state   # statewide candidate totals
#' res$county  # county-level breakdown
#'
#' # Indiana — statewide only (faster)
#' df <- scrape_elections(state = "IN", year_from = 2022, year_to = 2022,
#'                        level = "state")
#'
#' # Louisiana — statewide + parish results
#' res <- scrape_elections(state = "LA", year_from = 2024, year_to = 2024)
#' res$state   # statewide candidate totals
#' res$parish  # parish-level breakdown
#'
#' # Louisiana — statewide only (faster; skips parish scraping)
#' df <- scrape_elections(state = "LA", year_from = 2023, year_to = 2023,
#'                        level = "state")
#' }
#'
#' @export
scrape_elections <- function(
    state                = NULL,
    year_from            = NULL,
    year_to              = NULL,
    level                = c("all", "state", "county", "precinct", "town", "parish"),
    parallel             = TRUE,
    max_workers          = 4L,
    include_vote_methods = FALSE) {

  caller_env <- parent.frame()

  # Capture before match.arg() assigns to locals — missing() is call-time state
  .parallel_supplied           <- !missing(parallel)
  .max_workers_supplied        <- !missing(max_workers)
  .include_vote_methods_supplied <- !missing(include_vote_methods)

  level <- match.arg(level)

  # ── Input validation ──────────────────────────────────────────────────────
  .stop_if_not_scalar(state, "state")

  if (!is.logical(parallel) || length(parallel) != 1L || is.na(parallel))
    stop("'parallel' must be TRUE or FALSE.", call. = FALSE)
  if (!is.logical(include_vote_methods) || length(include_vote_methods) != 1L || is.na(include_vote_methods))
    stop("'include_vote_methods' must be TRUE or FALSE.", call. = FALSE)

  max_workers <- .validate_max_workers(max_workers)

  year_from <- .to_year(year_from, "year_from")
  year_to   <- .to_year(year_to,   "year_to")

  if (!is.null(year_from) && !is.null(year_to) && year_from > year_to)
    stop(sprintf("'year_from' (%d) cannot be greater than 'year_to' (%d).", year_from, year_to),
         call. = FALSE)

  state <- .normalize_state(state)
  .check_state_recognized(state)

  # ── Route ─────────────────────────────────────────────────────────────────
  source <- .route_to_source(state)
  label  <- .source_label(source, state)

  # ── Argument compatibility ────────────────────────────────────────────────
  valid_levels <- .SOURCE_LEVELS[[source]]
  if (!level %in% valid_levels)
    stop(sprintf(
      paste0(
        "'level = \"%s\"' is not valid for %s.\n",
        "  Valid options: %s\n",
        "  Tip: county/town/parish sub-levels depend on the scraper;\n",
        "       use level = \"all\" to return everything."
      ),
      level, label,
      paste0('"', valid_levels, '"', collapse = ", ")
    ), call. = FALSE)

  if (.include_vote_methods_supplied && source == "utah_results")
    stop(
      "'include_vote_methods' is not supported for Utah.\n",
      "  Remove this argument.",
      call. = FALSE
    )

  if (isTRUE(include_vote_methods) && source != "georgia_results")
    stop(
      "'include_vote_methods = TRUE' is only supported for Georgia.\n",
      "  Remove this argument or set include_vote_methods = FALSE.",
      call. = FALSE
    )

  if (.parallel_supplied && source != "election_stats")
    stop("'parallel' is only applicable for ElectionStats; it is not used by ", label, ".",
         call. = FALSE)

  if (.max_workers_supplied && !source %in% .USES_MAX_WORKERS)
    stop("'max_workers' is not applicable for ", label, ".", call. = FALSE)

  # ── Availability info + unconfirmed-year notice ───────────────────────────
  .emit_availability(source, state, year_to)

  # ── Dispatch ──────────────────────────────────────────────────────────────
  result <- tryCatch(switch(source,
    "election_stats" = .scrape_election_stats(
      state     = .state_to_es_key(state),
      year_from = if (is.null(year_from)) 1789L else year_from,
      year_to   = year_to,
      level     = level,
      parallel  = parallel
    ),
    "northcarolina_results" = .scrape_nc(
      year_from = year_from,
      year_to   = year_to,
      level     = level
    ),
    "connecticut_results" = .scrape_ct(
      year_from        = year_from,
      year_to          = year_to,
      level            = level,
      max_town_workers = as.integer(max_workers)
    ),
    "georgia_results" = .scrape_ga(
      year_from            = year_from,
      year_to              = year_to,
      level                = level,
      max_county_workers   = as.integer(max_workers),
      include_vote_methods = isTRUE(include_vote_methods)
    ),
    "utah_results" = .scrape_ut(
      year_from          = year_from,
      year_to            = year_to,
      level              = level,
      max_county_workers = as.integer(max_workers)
    ),
    "indiana_results" = .scrape_in(
      year_from = year_from,
      year_to   = year_to,
      level     = level
    ),
    "louisiana_results" = .scrape_la(
      year_from          = year_from,
      year_to            = year_to,
      level              = level,
      max_parish_workers = as.integer(max_workers)
    )
  ),
  error = function(e) {
    stop(
      "Scraping failed for ", label, ".\n\n",
      "  Error: ", conditionMessage(e), "\n\n",
      "  If this looks like a bug or the data source may have changed,\n",
      "  please file a report (including the error above) at:\n",
      "  https://github.com/gchickering21/DownBallotR/issues",
      call. = FALSE
    )
  })

  result <- reticulate::py_to_r(result)

  if (is.list(result) && !is.data.frame(result))
    return(.assign_list_result(result, state, caller_env))

  result
}
