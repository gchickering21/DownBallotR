# R/state_utils.R
#
# State normalization utilities: abbreviation lookup, canonical name conversion,
# and year coercion helpers used across scraper_helpers.R and scrape_elections.R.


# ── Abbreviation → full name lookup ──────────────────────────────────────────

# All 50 states + DC: 2-letter abbreviation → canonical title-case full name
.STATE_ABBREV <- c(
  AL = "Alabama",        AK = "Alaska",         AZ = "Arizona",
  AR = "Arkansas",       CA = "California",      CO = "Colorado",
  CT = "Connecticut",    DE = "Delaware",        FL = "Florida",
  GA = "Georgia",        HI = "Hawaii",          ID = "Idaho",
  IL = "Illinois",       IN = "Indiana",         IA = "Iowa",
  KS = "Kansas",         KY = "Kentucky",        LA = "Louisiana",
  ME = "Maine",          MD = "Maryland",        MA = "Massachusetts",
  MI = "Michigan",       MN = "Minnesota",       MS = "Mississippi",
  MO = "Missouri",       MT = "Montana",         NE = "Nebraska",
  NV = "Nevada",         NH = "New Hampshire",   NJ = "New Jersey",
  NM = "New Mexico",     NY = "New York",        NC = "North Carolina",
  ND = "North Dakota",   OH = "Ohio",            OK = "Oklahoma",
  OR = "Oregon",         PA = "Pennsylvania",    RI = "Rhode Island",
  SC = "South Carolina", SD = "South Dakota",    TN = "Tennessee",
  TX = "Texas",          UT = "Utah",            VT = "Vermont",
  VA = "Virginia",       WA = "Washington",      WV = "West Virginia",
  WI = "Wisconsin",      WY = "Wyoming",         DC = "District of Columbia"
)


# ── Helper functions ──────────────────────────────────────────────────────────

#' Normalise a state to canonical title-case full name
#'
#' Accepts 2-letter abbreviations (any case) or full names in any
#' case/spacing/underscore style. Returns \code{NULL} invisibly when the input
#' is \code{NULL}.
#'
#' @keywords internal
.normalize_state <- function(state) {
  if (is.null(state)) return(NULL)
  s <- trimws(state)

  # 2-letter abbreviation (case-insensitive)
  s_upper <- toupper(s)
  if (nchar(s_upper) == 2L && s_upper %in% names(.STATE_ABBREV)) {
    return(.STATE_ABBREV[[s_upper]])
  }

  # Full name: replace underscores/hyphens with spaces, then title-case
  tools::toTitleCase(tolower(gsub("[_\\-]", " ", s)))
}


#' Convert canonical title-case state to ElectionStats key (lowercase, underscores)
#' @keywords internal
.state_to_es_key <- function(state) {
  if (is.null(state)) return(NULL)
  tolower(gsub(" ", "_", state))
}


#' Coerce a year value to integer, accepting numeric, string, or NULL
#' @keywords internal
.to_year <- function(x, arg = deparse(substitute(x))) {
  if (is.null(x)) return(NULL)
  if (length(x) != 1L)
    stop(sprintf("'%s' must be a single value, not a vector of length %d.", arg, length(x)),
         call. = FALSE)
  val <- suppressWarnings(as.integer(as.numeric(as.character(x))))
  if (is.na(val))
    stop(sprintf("Cannot convert '%s' value %s to an integer year.", arg, deparse(x)),
         call. = FALSE)
  val
}


#' Stop if a scalar argument has length != 1
#' @keywords internal
.stop_if_not_scalar <- function(x, arg) {
  if (!is.null(x) && length(x) != 1L)
    stop(sprintf("'%s' must be a single value, not a vector of length %d.", arg, length(x)),
         call. = FALSE)
  invisible(x)
}


#' Validate and coerce max_workers to a positive integer, capped at 4
#'
#' R users are capped at 4 parallel workers to avoid overwhelming public
#' election data sites. Values above 4 are silently reduced with a message.
#' @keywords internal
.validate_max_workers <- function(x, arg = "max_workers") {
  .stop_if_not_scalar(x, arg)
  val <- suppressWarnings(as.integer(x))
  if (is.na(val) || val < 1L)
    stop(sprintf("'%s' must be a positive integer (e.g. 4L); got %s.", arg, deparse(x)),
         call. = FALSE)
  if (val > 4L) {
    message(sprintf(
      paste0(
        "Note: '%s' has been capped at 4 (requested %d).\n",
        "  DownBallotR scrapes government election sites that are not\n",
        "  designed for high-volume automated access. Running too many parallel browsers\n",
        "  at once can slow or crash these sites for other users, including election\n",
        "  officials. A limit of 4 workers balances scraping speed with being a\n",
        "  responsible user of shared public infrastructure."
      ),
      arg, val
    ))
    val <- 4L
  }
  val
}


#' Check that a normalized state name is a recognized US state
#'
#' Raises a user-friendly error with fuzzy-match suggestions when the state
#' cannot be identified. Pass \code{required = FALSE} to only warn (used when
#' \code{NULL} is a valid "all states" sentinel).
#' @keywords internal
.check_state_recognized <- function(state) {
  if (is.null(state)) return(invisible(NULL))
  known <- unname(.STATE_ABBREV)
  if (state %in% known) return(invisible(state))

  # Fuzzy-match suggestions for a helpful hint
  matches <- agrep(state, known, ignore.case = TRUE, value = TRUE, max.distance = 0.2)
  hint <- if (length(matches) > 0L)
    paste0('\n  Did you mean: "', paste(utils::head(matches, 3L), collapse = '", "'), '"?')
  else
    ""

  stop(sprintf(
    'Unrecognized state: "%s".%s\n  Pass a 2-letter abbreviation (e.g. "VA") or full name (e.g. "Virginia").',
    state, hint
  ), call. = FALSE)
}
