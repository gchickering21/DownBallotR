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
.to_year <- function(x) {
  if (is.null(x)) return(NULL)
  val <- suppressWarnings(as.integer(as.numeric(as.character(x))))
  if (is.na(val)) stop("Cannot convert year value ", deparse(x), " to an integer.")
  val
}
