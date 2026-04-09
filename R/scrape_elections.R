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
#'         \code{"north_carolina"}) → NC State Board of Elections scraper.
#'   \item \code{state} matches Connecticut (e.g. \code{"CT"},
#'         \code{"connecticut"}) → Connecticut CTEMS scraper (2016–present).
#'   \item \code{state} matches Georgia (e.g. \code{"GA"},
#'         \code{"georgia"}) → Georgia Secretary of State scraper (2000–present).
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
#' @param year_from (\code{general} / NC / CT / GA) Start year, inclusive
#'   (default \code{NULL}). When \code{NULL}, ElectionStats starts at
#'   \code{1789}; NC/CT/GA apply no lower bound.
#' @param year_to (\code{general} / NC / CT / GA) End year, inclusive (default
#'   \code{NULL}). When \code{NULL}, ElectionStats uses the current calendar
#'   year; NC/CT/GA apply no upper bound.
#' @param level (\code{general} / ElectionStats / Connecticut / Georgia) What
#'   to return. \code{"all"} (default) returns a named list with \code{$state}
#'   and \code{$county} data frames (ElectionStats / Georgia), or
#'   \code{$state} and \code{$town} data frames (Connecticut);
#'   \code{"state"} returns statewide candidate-level results only;
#'   \code{"county"} returns county vote breakdowns (ElectionStats / Georgia);
#'   \code{"town"} returns town-level results only (Connecticut);
#'   \code{"joined"} returns county rows merged with candidate metadata
#'   (ElectionStats only).
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
#' @param max_workers (\code{general} / Georgia / Connecticut) Maximum number
#'   of parallel Chromium browsers (default \code{4L}). For Georgia, controls
#'   county-level parallelism; for Connecticut, controls town-level parallelism.
#'   Ignored for all other states.
#' @param include_vote_methods (\code{general} / Georgia) If \code{TRUE},
#'   also return a vote-method breakdown table (Advance in Person, Election
#'   Day, Absentee by Mail, Provisional) for Georgia results (default
#'   \code{FALSE}). Ignored for all other states.
#'
#' @return A \code{data.frame}, or a named list when \code{level = "all"}:
#'   \code{$state} + \code{$county} for ElectionStats / Georgia;
#'   \code{$state} + \code{$town} for Connecticut.
#'   NC and state-elections always return a single \code{data.frame}.
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
    level               = c("all", "state", "county", "joined", "town"),
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

  office         <- match.arg(office)
  level          <- match.arg(level)
  mode           <- match.arg(mode)
  election_level <- match.arg(election_level)
  race_type      <- match.arg(race_type)

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

  # ── Input validation ─────────────────────────────────────────────────────────

  # state must be a single string (not a vector)
  .stop_if_not_scalar(state, "state")

  # logical scalars
  if (!is.logical(parallel) || length(parallel) != 1L || is.na(parallel))
    stop("'parallel' must be TRUE or FALSE.", call. = FALSE)
  if (!is.logical(include_vote_methods) || length(include_vote_methods) != 1L || is.na(include_vote_methods))
    stop("'include_vote_methods' must be TRUE or FALSE.", call. = FALSE)

  # max_workers must be a positive integer
  max_workers <- .validate_max_workers(max_workers)

  # year arguments: scalar + integer coercion
  year_from  <- .to_year(year_from,  "year_from")
  year_to    <- .to_year(year_to,    "year_to")
  year       <- .to_year(year,       "year")
  start_year <- .to_year(start_year, "start_year")
  end_year   <- .to_year(end_year,   "end_year")

  # year range cross-check (R-level, before hitting Python)
  if (!is.null(year_from) && !is.null(year_to) && year_from > year_to)
    stop(sprintf("'year_from' (%d) cannot be greater than 'year_to' (%d).", year_from, year_to),
         call. = FALSE)
  if (!is.null(start_year) && !is.null(end_year) && start_year > end_year)
    stop(sprintf("'start_year' (%d) cannot be greater than 'end_year' (%d).", start_year, end_year),
         call. = FALSE)

  # Normalize and validate state
  state <- .normalize_state(state)
  .check_state_recognized(state)

  # office-specific required-argument checks (catch early before Python does)
  if (office == "state_elections" && is.null(state))
    stop("'state' is required when office = \"state_elections\".", call. = FALSE)

  if (office %in% c("school_district", "state_elections") &&
      mode %in% c("results", "joined") && is.null(year))
    stop(sprintf("'year' is required when office = \"%s\" and mode = \"%s\".", office, mode),
         call. = FALSE)

  # Set default start_year per office type if not supplied
  if (is.null(start_year)) {
    start_year <- switch(office,
      "school_district"     = 2013L,
      "state_elections"     = 2024L,
      "municipal_elections" = 2014L,
      NULL
    )
  }

  # ── Auto-route ───────────────────────────────────────────────────────────────
  # state has already been normalised to title-case by .normalize_state() above,
  # so direct equality against canonical names is sufficient.
  source <- if (office == "school_district") {
    "ballotpedia"
  } else if (office == "state_elections") {
    "ballotpedia_elections"
  } else if (office == "municipal_elections") {
    "ballotpedia_municipal"
  } else if (!is.null(state) && state == "North Carolina") {
    "northcarolina_results"
  } else if (!is.null(state) && state == "Connecticut") {
    "connecticut_results"
  } else if (!is.null(state) && state == "Georgia") {
    "georgia_results"
  } else if (!is.null(state) && state == "Utah") {
    "utah_results"
  } else if (!is.null(state) && state == "Indiana") {
    "indiana_results"
  } else if (!is.null(state) && state == "Louisiana") {
    "louisiana_results"
  } else {
    "election_stats"
  }

  # ── Post-routing: argument-compatibility checks ───────────────────────────────

  # Which level values are meaningful per source (NULL = level is ignored)
  .SOURCE_LEVELS <- list(
    election_stats        = c("all", "state", "county", "joined"),
    northcarolina_results = NULL,
    connecticut_results   = c("all", "state", "town"),
    georgia_results       = c("all", "state", "county"),
    utah_results          = c("all", "state", "county"),
    indiana_results       = c("all", "state", "county"),
    louisiana_results     = c("all", "state", "parish"),
    ballotpedia           = NULL,
    ballotpedia_elections = NULL,
    ballotpedia_municipal = NULL
  )

  .source_name <- switch(
    source,
    election_stats        = paste0(state, " (ElectionStats)"),
    northcarolina_results = "North Carolina (NC State Board of Elections)",
    connecticut_results   = "Connecticut (CTEMS)",
    georgia_results       = "Georgia (GA Secretary of State)",
    utah_results          = "Utah",
    indiana_results       = "Indiana",
    louisiana_results     = "Louisiana (Secretary of State)",
    ballotpedia           = "school district elections (Ballotpedia)",
    ballotpedia_elections = paste0(state, " state elections (Ballotpedia)"),
    ballotpedia_municipal = "municipal elections (Ballotpedia)",
    source
  )

  valid_levels <- .SOURCE_LEVELS[[source]]
  if (is.null(valid_levels)) {
    # Scraper returns a single flat data frame; 'level' has no effect
    if (level != "all")
      warning(sprintf(
        "'level = \"%s\"' is not applicable for %s and will be ignored.\n  This scraper always returns a single data frame.",
        level, .source_name
      ), call. = FALSE)
  } else if (!level %in% valid_levels) {
    stop(sprintf(
      paste0(
        "'level = \"%s\"' is not valid for %s.\n",
        "  Valid options: %s\n",
        "  Tip: \"joined\" and the county/town/parish sub-levels depend on the scraper;\n",
        "       use level = \"all\" to return everything."
      ),
      level, .source_name,
      paste0('"', valid_levels, '"', collapse = ", ")
    ), call. = FALSE)
  }

  # include_vote_methods is only meaningful for GA and UT
  if (isTRUE(include_vote_methods) && !source %in% c("georgia_results", "utah_results"))
    warning(
      "'include_vote_methods = TRUE' is only supported for Georgia and Utah; it will be ignored.",
      call. = FALSE
    )

  # Warn when office-specific parameters are set but won't be used
  if (office == "general" && !mode %in% c("districts", "results", "joined"))
    warning(sprintf(
      "'mode = \"%s\"' is not used for office = \"general\";\n  routing is determined by state. Did you mean a different 'office'?",
      mode
    ), call. = FALSE)

  if (office != "state_elections" && election_level != "all")
    warning(sprintf(
      "'election_level = \"%s\"' is only used with office = \"state_elections\"; it will be ignored.",
      election_level
    ), call. = FALSE)

  if (office != "municipal_elections" && race_type != "all")
    warning(sprintf(
      "'race_type = \"%s\"' is only used with office = \"municipal_elections\"; it will be ignored.",
      race_type
    ), call. = FALSE)

  # ── Availability info + unconfirmed-year notice ───────────────────────────
  .avail_label <- switch(
    source,
    "ballotpedia"           = "school district elections (Ballotpedia)",
    "ballotpedia_elections" = paste0(state, " state elections (Ballotpedia)"),
    "ballotpedia_municipal" = paste0(
      "municipal/mayoral elections (Ballotpedia, race_type='", race_type, "')"
    ),
    "northcarolina_results" = "North Carolina (NC State Board of Elections)",
    "connecticut_results"   = "Connecticut (CTEMS)",
    "georgia_results"       = "Georgia (GA Secretary of State)",
    "utah_results"          = "Utah (electionresults.utah.gov)",
    "indiana_results"       = "Indiana (enr.indianavoters.in.gov, General elections)",
    "louisiana_results"     = "Louisiana (voterportal.sos.la.gov)",
    "election_stats"        = paste0(state, " (ElectionStats)")
  )

  .avail <- tryCatch(
    .db_registry()$get_available_years(
      source = source,
      state  = if (source == "election_stats") .state_to_es_key(state) else NULL
    ),
    error = function(e) NULL
  )

  if (!is.null(.avail)) {
    message("Available years for ", .avail_label, ": ",
            .avail$start_year, "\u2013", .avail$end_year)

    # Determine the furthest year the user is requesting
    .requested_to <- year_to
    if (is.null(.requested_to)) .requested_to <- year
    if (is.null(.requested_to)) .requested_to <- end_year

    if (!is.null(.requested_to) && .requested_to > .avail$end_year) {
      message(
        "\nNote: ", .requested_to, " is beyond the last confirmed year (",
        .avail$end_year, ") for ", .avail_label, ".\n",
        "  This year has not been verified. DownBallotR will attempt the scrape,\n",
        "  but results are not guaranteed.\n",
        "  If you encounter problems, please file a report at:\n",
        "  https://github.com/gchickering21/DownBallotR/issues"
      )
    }
  }

  # ── Dispatch ─────────────────────────────────────────────────────────────────
  result <- tryCatch(switch(
    source,
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
      year_to   = year_to
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
      "Scraping failed for ", .avail_label, ".\n\n",
      "  Error: ", conditionMessage(e), "\n\n",
      "  If this looks like a bug or the data source may have changed,\n",
      "  please file a report (including the error above) at:\n",
      "  https://github.com/gchickering21/DownBallotR/issues",
      call. = FALSE
    )
  })

  reticulate::py_to_r(result)
}
