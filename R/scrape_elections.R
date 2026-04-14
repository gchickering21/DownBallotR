# R/scrape_elections.R
#
# The primary exported function: scrape_elections().
# Routing logic lives here; per-source argument shaping is in scraper_helpers.R;
# state normalization utilities are in state_utils.R.


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
#'   \item \code{office = "state_elections"} → Ballotpedia state elections
#'         scraper (federal, state, and local candidates; 2024–present).
#'   \item \code{office = "municipal_elections"} → Ballotpedia municipal and
#'         mayoral elections scraper (all US states, 2014–present).
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
#'   do not need to worry about the exact format. Pass \code{NULL} to return
#'   all states (Ballotpedia scrapers only).
#' @param office Type of election to retrieve. \code{"general"} (default)
#'   fetches general election results via ElectionStats or the NC scraper;
#'   \code{"school_district"} fetches school board elections via Ballotpedia;
#'   \code{"state_elections"} fetches federal, state, and local candidate
#'   listings from Ballotpedia state election pages (2024–present);
#'   \code{"municipal_elections"} fetches city, county, and mayoral election
#'   data from Ballotpedia municipal election index pages (2014–present).
#' @param year_from (\code{general} / NC / CT / GA / UT / IN / LA) Start year,
#'   inclusive (default \code{NULL}). When \code{NULL}, ElectionStats starts at
#'   \code{1789}; all state-portal scrapers apply no lower bound (data is
#'   clamped to each scraper's earliest confirmed year).
#' @param year_to (\code{general} / NC / CT / GA / UT / IN / LA) End year,
#'   inclusive (default \code{NULL}). When \code{NULL}, the current calendar
#'   year is used as the upper bound.
#' @param level (\code{general} / ElectionStats / NC / CT / GA / UT / IN / LA)
#'   What to return. \code{"all"} (default) returns a named list with
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
#' @param parallel (\code{general} / ElectionStats) Use parallel county
#'   scraping for classic (requests-based) states (default \code{TRUE}).
#'   Ignored automatically for Playwright-based states (SC, NM, NY, VA).
#' @param year (\code{school_district}) Election year (e.g. \code{2024}).
#'   Required when \code{mode = "results"} or \code{mode = "joined"}. If
#'   \code{NULL} with \code{mode = "districts"}, use \code{start_year} /
#'   \code{end_year} for a multi-year scrape.
#' @param mode (\code{school_district}) What to return. \code{"districts"}
#'   (default) returns fast district metadata (one request per year-page);
#'   \code{"results"} follows each district URL for candidate/vote data;
#'   \code{"joined"} returns districts and candidates merged into one data
#'   frame. For \code{state_elections}: \code{"listings"} (default) returns
#'   one row per candidate from the state+year page (fast); \code{"results"}
#'   additionally follows each contest URL for vote counts (slower).
#'   For \code{municipal_elections}: \code{"links"} (default) returns index
#'   discovery only; \code{"results"} follows each sub-URL for full candidate
#'   and vote data (slower).
#' @param start_year (\code{school_district}) Earliest year for a multi-year
#'   district scrape when \code{year} is \code{NULL} (default \code{2013}).
#'   For \code{state_elections}, earliest year when \code{year} is \code{NULL}
#'   (default \code{2024}). For \code{municipal_elections}, earliest year when
#'   \code{year} is \code{NULL} (default \code{2014}).
#' @param end_year (\code{school_district} / \code{state_elections} /
#'   \code{municipal_elections}) Latest year for a multi-year scrape when
#'   \code{year} is \code{NULL} (default: current calendar year).
#' @param election_level (\code{state_elections}) Which candidate tier to
#'   return. \code{"all"} (default) returns all tiers; \code{"federal"}
#'   returns U.S. House / Senate / Presidential Electors only; \code{"state"}
#'   returns state-level races only; \code{"local"} returns local races only.
#' @param race_type (\code{municipal_elections}) Which index page to use.
#'   \code{"all"} (default) uses the broader United_States_municipal_elections
#'   page (2014–present). \code{"mayoral"} uses the mayoral-only page
#'   (2020–present).
#' @param max_workers (\code{general} / Georgia / Utah / Connecticut /
#'   Louisiana) Maximum number of parallel Chromium browsers (default
#'   \code{4L}). For Georgia and Utah, controls county-level parallelism; for
#'   Connecticut, controls town-level parallelism; for Louisiana, controls
#'   parish-level parallelism (default is capped at 2 for LA).
#'   Ignored for all other states.
#' @param include_vote_methods (\code{general} / Georgia / Utah) If
#'   \code{TRUE}, also return a vote-method breakdown table (Advance in Person,
#'   Election Day, Absentee by Mail, Provisional) for Georgia and Utah results
#'   (default \code{FALSE}). Ignored for all other states.
#'
#' @return A \code{data.frame}, or a named list when \code{level = "all"}:
#'   \code{$state} + \code{$county} (+ \code{$precinct} when available) for ElectionStats;
#'   \code{$state} + \code{$county} for Georgia / Utah / Indiana;
#'   \code{$state} + \code{$town} for Connecticut;
#'   \code{$state} + \code{$parish} for Louisiana;
#'   \code{$precinct} + \code{$county} + \code{$state} for North Carolina.
#'   Ballotpedia scrapers always return a single \code{data.frame}.
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
#'
#' # School district elections — one state, one year
#' df <- scrape_elections(state = "Alabama", office = "school_district", year = 2024)
#'
#' # State elections — candidate listings
#' df <- scrape_elections(state = "Maine", office = "state_elections", year = 2024)
#'
#' # Municipal elections — mayoral only, full results
#' df <- scrape_elections(office = "municipal_elections", year = 2022,
#'                        race_type = "mayoral", mode = "results")
#' }
#'
#' @export
scrape_elections <- function(
    state               = NULL,
    office              = c("general", "school_district", "state_elections",
                            "municipal_elections"),
    # General-election (ElectionStats / NC / CT / GA) args
    year_from           = NULL,
    year_to             = NULL,
    level               = c("all", "state", "county", "precinct", "town", "parish"),
    parallel            = TRUE,
    # School-district / state-elections / municipal-elections (Ballotpedia) args
    year                = NULL,
    mode                = c("districts", "results", "joined", "listings", "links"),
    start_year          = NULL,
    end_year            = NULL,
    election_level      = c("all", "federal", "state", "local"),
    race_type           = c("all", "mayoral"),
    # Georgia / Connecticut args
    max_workers          = 4L,
    include_vote_methods = FALSE) {

  caller_env <- parent.frame()

  # Capture before match.arg() assigns to locals — missing() is call-time state
  .parallel_supplied    <- !missing(parallel)
  .max_workers_supplied <- !missing(max_workers)

  office         <- match.arg(office)
  level          <- match.arg(level)
  mode           <- match.arg(mode)
  election_level <- match.arg(election_level)
  race_type      <- match.arg(race_type)

  # ── Input validation ──────────────────────────────────────────────────────
  .stop_if_not_scalar(state, "state")

  # Guard against old source= positional usage (e.g. scrape_elections("ballotpedia", ...))
  if (!is.null(state) &&
      tolower(trimws(state)) %in% c("ballotpedia", "election_stats", "northcarolina_results")) {
    stop(
      "The 'source' argument has been removed. Route by state instead:\n",
      "  - For school board elections:  office = \"school_district\"\n",
      "  - For NC results:              state = \"NC\"\n",
      "  - For other states:            state = \"virginia\"  (or any ElectionStats state)",
      call. = FALSE
    )
  }

  if (!is.logical(parallel) || length(parallel) != 1L || is.na(parallel))
    stop("'parallel' must be TRUE or FALSE.", call. = FALSE)
  if (!is.logical(include_vote_methods) || length(include_vote_methods) != 1L || is.na(include_vote_methods))
    stop("'include_vote_methods' must be TRUE or FALSE.", call. = FALSE)

  max_workers <- .validate_max_workers(max_workers)

  year_from  <- .to_year(year_from,  "year_from")
  year_to    <- .to_year(year_to,    "year_to")
  year       <- .to_year(year,       "year")
  start_year <- .to_year(start_year, "start_year")
  end_year   <- .to_year(end_year,   "end_year")

  if (!is.null(year_from) && !is.null(year_to) && year_from > year_to)
    stop(sprintf("'year_from' (%d) cannot be greater than 'year_to' (%d).", year_from, year_to),
         call. = FALSE)
  if (!is.null(start_year) && !is.null(end_year) && start_year > end_year)
    stop(sprintf("'start_year' (%d) cannot be greater than 'end_year' (%d).", start_year, end_year),
         call. = FALSE)

  state <- .normalize_state(state)
  .check_state_recognized(state)

  if (office == "state_elections" && is.null(state))
    stop("'state' is required when office = \"state_elections\".", call. = FALSE)
  if (office %in% c("school_district", "state_elections") &&
      mode %in% c("results", "joined") && is.null(year))
    stop(sprintf("'year' is required when office = \"%s\" and mode = \"%s\".", office, mode),
         call. = FALSE)

  if (is.null(start_year)) {
    start_year <- switch(office,
      "school_district"     = 2013L,
      "state_elections"     = 2024L,
      "municipal_elections" = 2014L,
      NULL
    )
  }

  # ── Route ─────────────────────────────────────────────────────────────────
  source <- .route_to_source(office, state)
  label  <- .source_label(source, state, race_type)

  # ── Argument compatibility ────────────────────────────────────────────────
  valid_levels <- .SOURCE_LEVELS[[source]]
  if (is.null(valid_levels)) {
    if (!identical(level, "all")) {
      stop(sprintf(
        "'level = \"%s\"' is not applicable for %s.\n  This scraper always returns a single data frame; remove the level= argument.",
        level, label
      ), call. = FALSE)
    }
  } else if (!level %in% valid_levels) {
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
  }

  if (isTRUE(include_vote_methods) && !source %in% c("georgia_results", "utah_results"))
    stop(
      "'include_vote_methods = TRUE' is only supported for Georgia and Utah.\n",
      "  Remove this argument or set include_vote_methods = FALSE.",
      call. = FALSE
    )
  if (office == "general" && mode != "districts")
    stop(sprintf(
      "'mode = \"%s\"' is not valid for office = \"general\".\n  The mode= argument only applies to Ballotpedia scrapers.",
      mode
    ), call. = FALSE)
  if (office != "state_elections" && election_level != "all")
    stop(sprintf(
      "'election_level = \"%s\"' is only valid when office = \"state_elections\".",
      election_level
    ), call. = FALSE)
  if (office != "municipal_elections" && race_type != "all")
    stop(sprintf(
      "'race_type = \"%s\"' is only valid when office = \"municipal_elections\".",
      race_type
    ), call. = FALSE)

  # ── Parameters inapplicable for this source ───────────────────────────────
  is_ballotpedia <- startsWith(source, "ballotpedia")

  if (is_ballotpedia) {
    if (!is.null(year_from))
      stop("'year_from' is not used for ", label, ".\n",
           "  Use 'year' or 'start_year' to set the lower bound.", call. = FALSE)
    if (!is.null(year_to))
      stop("'year_to' is not used for ", label, ".\n",
           "  Use 'year' or 'end_year' to set the upper bound.", call. = FALSE)
  } else {
    if (!is.null(year))
      stop("'year' is not used for ", label, ".\n",
           "  Use 'year_from' and 'year_to' instead.", call. = FALSE)
    if (!is.null(start_year))
      stop("'start_year' is not used for ", label, ".\n",
           "  Use 'year_from' instead.", call. = FALSE)
    if (!is.null(end_year))
      stop("'end_year' is not used for ", label, ".\n",
           "  Use 'year_to' instead.", call. = FALSE)
  }

  if (.parallel_supplied && source != "election_stats")
    stop("'parallel' is only applicable for ElectionStats; it is not used by ", label, ".",
         call. = FALSE)

  if (.max_workers_supplied && !source %in% .USES_MAX_WORKERS)
    stop("'max_workers' is not applicable for ", label, ".", call. = FALSE)

  # ── Availability info + unconfirmed-year notice ───────────────────────────
  .emit_availability(source, state, race_type, year_to, year, end_year)

  # ── Dispatch ──────────────────────────────────────────────────────────────
  result <- tryCatch(switch(source,
    "election_stats" = .scrape_election_stats(
      state     = .state_to_es_key(state),
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
    "ballotpedia_elections" = .scrape_ballotpedia_elections(
      year           = year,
      state          = state,
      mode           = if (mode == "listings") "listings" else "results",
      election_level = election_level,
      start_year     = start_year,
      end_year       = end_year
    ),
    "ballotpedia_municipal" = .scrape_ballotpedia_municipal(
      year       = year,
      state      = state,
      race_type  = race_type,
      mode       = if (mode == "links") "links" else "results",
      start_year = start_year,
      end_year   = end_year
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
      year_from            = year_from,
      year_to              = year_to,
      level                = level,
      max_county_workers   = as.integer(max_workers),
      include_vote_methods = isTRUE(include_vote_methods)
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
