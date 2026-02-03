# R/state_registry.R

# ---- Internal state normalization ----

#' @keywords internal
.state_abbrev_map <- function() {
  # Keep this as the single source of truth for name -> abbrev.
  # (You can expand synonyms later.)
  c(
    "ALABAMA" = "AL",
    "ALASKA" = "AK",
    "ARIZONA" = "AZ",
    "ARKANSAS" = "AR",
    "CALIFORNIA" = "CA",
    "COLORADO" = "CO",
    "CONNECTICUT" = "CT",
    "DELAWARE" = "DE",
    "FLORIDA" = "FL",
    "GEORGIA" = "GA",
    "HAWAII" = "HI",
    "IDAHO" = "ID",
    "ILLINOIS" = "IL",
    "INDIANA" = "IN",
    "IOWA" = "IA",
    "KANSAS" = "KS",
    "KENTUCKY" = "KY",
    "LOUISIANA" = "LA",
    "MAINE" = "ME",
    "MARYLAND" = "MD",
    "MASSACHUSETTS" = "MA",
    "MICHIGAN" = "MI",
    "MINNESOTA" = "MN",
    "MISSISSIPPI" = "MS",
    "MISSOURI" = "MO",
    "MONTANA" = "MT",
    "NEBRASKA" = "NE",
    "NEVADA" = "NV",
    "NEW HAMPSHIRE" = "NH",
    "NEW JERSEY" = "NJ",
    "NEW MEXICO" = "NM",
    "NEW YORK" = "NY",
    "NORTH CAROLINA" = "NC",
    "NORTH DAKOTA" = "ND",
    "OHIO" = "OH",
    "OKLAHOMA" = "OK",
    "OREGON" = "OR",
    "PENNSYLVANIA" = "PA",
    "RHODE ISLAND" = "RI",
    "SOUTH CAROLINA" = "SC",
    "SOUTH DAKOTA" = "SD",
    "TENNESSEE" = "TN",
    "TEXAS" = "TX",
    "UTAH" = "UT",
    "VERMONT" = "VT",
    "VIRGINIA" = "VA",
    "WASHINGTON" = "WA",
    "WEST VIRGINIA" = "WV",
    "WISCONSIN" = "WI",
    "WYOMING" = "WY",
    "DISTRICT OF COLUMBIA" = "DC"
  )
}

#' Normalize a state input to a 2-letter USPS abbreviation
#'
#' Accepts "NC" or "North Carolina" (case-insensitive). Trims whitespace.
#' Also accepts a few common punctuation variants (e.g., "Washington, DC").
#'
#' @keywords internal
.normalize_state <- function(state) {
  if (is.null(state) || !nzchar(trimws(state))) {
    stop("`state` must be provided (e.g., 'NC' or 'North Carolina').", call. = FALSE)
  }
  
  s <- toupper(trimws(state))
  
  # Quick path: already a 2-letter code
  if (nchar(s) == 2 && grepl("^[A-Z]{2}$", s)) {
    return(s)
  }
  
  # Normalize punctuation for names
  s <- gsub("[\\.]", "", s)      # remove periods: D.C. -> DC-ish
  s <- gsub("[,]", " ", s)       # commas to spaces
  s <- gsub("\\s+", " ", s)      # collapse whitespace
  s <- trimws(s)
  
  # Special-case common DC variant
  if (s %in% c("WASHINGTON DC", "WASHINGTON D C", "DC")) {
    return("DC")
  }
  
  m <- .state_abbrev_map()
  if (s %in% names(m)) {
    return(unname(m[[s]]))
  }
  
  stop(
    "Unrecognized `state`: '", state, "'.\n",
    "Use a 2-letter abbreviation (e.g., 'NC') or full name (e.g., 'North Carolina').",
    call. = FALSE
  )
}

# ---- Internal registry ----

#' Registry of per-state implementations
#'
#' Each state maps to a list of functions. For now we only require snapshot().
#' Later you can add discover() / scrape() etc.
#'
#' @keywords internal
#' @keywords internal
.state_registry <- function() {
  list(
    NC = list(
      snapshot = get_nc_snapshot,
      manifest_dates = nc_snapshot_dates,
      scrape_missing = .nc_scrape_missing_dates
    )
  )
}


#' @keywords internal
.get_state_impl <- function(state_abbrev) {
  reg <- .state_registry()
  impl <- reg[[state_abbrev]]
  if (is.null(impl)) {
    stop(
      "No implementation registered for state = '", state_abbrev, "'.\n",
      "Currently supported: ", paste(names(reg), collapse = ", "),
      call. = FALSE
    )
  }
  impl
}
