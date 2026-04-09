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

  state     <- .normalize_state(state)
  year_from <- .to_year(year_from)
  year_to   <- .to_year(year_to)

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
  } else {
    "election_stats"
  }

  # ── Print available year range for chosen source/state ────────────────────
  tryCatch({
    avail <- .db_registry()$get_available_years(
      source = source,
      state  = if (source == "election_stats") .state_to_es_key(state) else NULL
    )
    label <- switch(
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
      "election_stats"        = paste0(state, " (ElectionStats)")
    )
    message("Available years for ", label, ": ",
            avail$start_year, "\u2013", avail$end_year)
  }, error = function(e) NULL)

  # ── Dispatch ─────────────────────────────────────────────────────────────────
  result <- switch(
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
      level            = match.arg(level, c("all", "state", "town")),
      max_town_workers = as.integer(max_workers)
    ),
    "georgia_results" = .scrape_ga(
      year_from            = year_from,
      year_to              = year_to,
      level                = match.arg(level, c("all", "state", "county")),
      max_county_workers   = as.integer(max_workers),
      include_vote_methods = isTRUE(include_vote_methods)
    ),
    "utah_results" = .scrape_ut(
      year_from            = year_from,
      year_to              = year_to,
      level                = match.arg(level, c("all", "state", "county")),
      max_county_workers   = as.integer(max_workers),
      include_vote_methods = isTRUE(include_vote_methods)
    ),
    "indiana_results" = .scrape_in(
      year_from = year_from,
      year_to   = year_to,
      level     = match.arg(level, c("all", "state", "county"))
    )
  )

  reticulate::py_to_r(result)
}
