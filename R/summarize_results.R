# R/summarize_results.R


#' Summarize an election results data frame
#'
#' Computes aggregate statistics for a data frame of election results. The
#' state is detected automatically from the \code{state} column when present,
#' or from the variable name (e.g. \code{ga_results} -> "Georgia").
#'
#' @param df A data frame returned by \code{\link{scrape_elections}}.
#' @param state Optional two-letter state abbreviation or full state name.
#'   Overrides auto-detection when supplied.
#'
#' @return A named list (printed on call) with:
#' \describe{
#'   \item{\code{state}}{Detected or supplied state name.}
#'   \item{\code{years}}{Integer vector of election years present.}
#'   \item{\code{n_years}}{Number of distinct election years.}
#'   \item{\code{n_elections}}{Number of distinct elections.}
#'   \item{\code{n_candidates}}{Number of distinct candidate names.}
#'   \item{\code{office_level_breakdown}}{Named integer vector: distinct
#'     elections by level (Federal / State / Local).}
#'   \item{\code{offices_by_level}}{Named list: distinct office names per level.}
#' }
#'
#' @examples
#' \donttest{
#' ga_results <- scrape_elections("GA", 2020, 2024)
#' summarize_results(ga_results)
#' }
#'
#' @importFrom rlang .data
#' @export
summarize_results <- function(df, state = NULL) {
  if (!is.data.frame(df)) stop("`df` must be a data frame.", call. = FALSE)

  # Auto-detect state from the data or the variable name if not supplied
  detected_state <- .detect_state(df, state, deparse(substitute(df)))

  years        <- sort(unique(df$election_year))
  n_elections  <- .count_distinct_elections(df)
  n_candidates <- dplyr::n_distinct(df$candidate, na.rm = TRUE)

  # Office-level breakdown — only computed when the required columns exist
  level_breakdown  <- NULL
  offices_by_level <- NULL

  if ("office_level" %in% names(df) && "office" %in% names(df)) {
    known_levels <- c("Federal", "State", "Local")

    # One pass over each level: count elections and collect office names
    level_stats <- purrr::map(
      purrr::set_names(known_levels),
      function(lvl) {
        sub <- dplyr::filter(df, .data$office_level == lvl)
        list(
          n_elections = .count_distinct_elections(sub),
          offices     = sort(unique(sub$office))
        )
      }
    )

    # Pull the two pieces out of the per-level results
    level_breakdown  <- purrr::map_int(level_stats, "n_elections")
    offices_by_level <- purrr::map(level_stats, "offices")
  }

  result <- list(
    state                  = detected_state,
    years                  = years,
    n_years                = length(years),
    n_elections            = n_elections,
    n_candidates           = n_candidates,
    office_level_breakdown = level_breakdown,
    offices_by_level       = offices_by_level
  )

  .print_summary(result)
  invisible(result)
}


# ── Internal helpers ──────────────────────────────────────────────────────────

# Not all states have the same id columns, so we fall back through options:
#   election_name + year        (Clarity states: GA, UT, CT, LA)
#   election_id                 (ElectionStats states: CO, ID, MA, etc.)
#   election_year + type + office  (Indiana — no name or id, but the combination
#                                   of year, type, and office uniquely identifies elections)
.count_distinct_elections <- function(df) {
  has <- function(...) all(c(...) %in% names(df))

  if (has("election_name", "election_date", "office"))
    return(dplyr::n_distinct(df$election_name, df$election_date, df$office, na.rm = TRUE))
  if (has("election_id"))
    return(dplyr::n_distinct(df$election_id, na.rm = TRUE))
  if (has("election_year", "election_type", "office"))
    return(dplyr::n_distinct(df$election_year, df$election_type, df$office, na.rm = TRUE))
  if (has("election_date", "office"))
    return(dplyr::n_distinct(df$election_date, df$office, na.rm = TRUE))
  NA_integer_
}


# Convert a state abbreviation (e.g. "GA") to its full name (e.g. "Georgia").
# Returns NULL if the token is not a recognized abbreviation.
.abbrev_to_fullname <- function(x) {
  abbrev <- toupper(trimws(x))
  if (abbrev %in% names(.STATE_ABBREV)) return(.STATE_ABBREV[[abbrev]])
  NULL
}

# Detect the state name using three fallbacks in priority order:
#   1. Explicit `state` argument supplied by the user
#   2. The `state` column in the data frame
#   3. Tokens in the R variable name (e.g. "ga_results" -> "Georgia")
.detect_state <- function(df, state_arg, varname) {
  # 1. User supplied a state argument — normalize and return it
  if (!is.null(state_arg) && nzchar(state_arg)) {
    full <- .abbrev_to_fullname(state_arg)
    if (!is.null(full)) return(full)
    # Try title-casing in case they passed "georgia" instead of "GA"
    canonical <- tools::toTitleCase(tolower(trimws(state_arg)))
    if (canonical %in% .STATE_ABBREV) return(canonical)
    return(state_arg)
  }

  # 2. `state` column in the data (all rows should share one value)
  if ("state" %in% names(df)) {
    vals <- unique(stats::na.omit(df$state))
    if (length(vals) == 1L) {
      full <- .abbrev_to_fullname(vals)
      return(if (!is.null(full)) full else vals)
    }
    if (length(vals) > 1L) return(paste(vals, collapse = ", "))
  }

  # 3. Parse the variable name — split on non-letters and check each token
  #    e.g. "ga_2024_results" -> c("ga", "results") -> "Georgia"
  tokens <- strsplit(varname, "[^a-zA-Z]+")[[1L]]
  for (tok in tokens) {
    full <- .abbrev_to_fullname(tok)
    if (!is.null(full)) return(full)
    canonical <- tools::toTitleCase(tolower(tok))
    if (canonical %in% .STATE_ABBREV) return(canonical)
  }

  "Unknown"
}


.print_summary <- function(s) {
  # Helper so we don't repeat the singular/plural logic inline
  plur <- function(n, word) paste0(if (is.na(n)) "NA" else n, " ", word, if (!is.na(n) && n == 1L) "" else "s")

  year_range <- if (length(s$years)) paste(range(s$years), collapse = "\u2013") else "N/A"

  cat("Election Results Summary\n")
  cat("========================\n")
  cat(sprintf("State            : %s\n", s$state))
  cat(sprintf("Years covered    : %s  (%s)\n", year_range, plur(s$n_years, "year")))
  cat(sprintf("Elections        : %s\n", format(s$n_elections, big.mark = ",")))
  cat(sprintf("Unique candidates: %s\n", format(s$n_candidates, big.mark = ",")))

  if (!is.null(s$office_level_breakdown)) {
    cat("\nElections by office level:\n")
    for (lvl in names(s$office_level_breakdown)) {
      offices <- s$offices_by_level[[lvl]]
      cat(sprintf("  %-8s : %s, %s\n",
                  lvl,
                  plur(s$office_level_breakdown[[lvl]], "election"),
                  plur(length(offices), "office")))
      for (o in offices) cat(sprintf("             - %s\n", o))
    }
  }
  cat("\n")
}
