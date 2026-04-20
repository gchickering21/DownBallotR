library(tidyverse)
library(DownBallotR)

# ---- user input ----
root_folder <- "data"
file_pattern <- "\\.csv$"

# ---- find all csv files under data/ recursively ----
file_paths <- list.files(
  path = root_folder,
  pattern = file_pattern,
  full.names = TRUE,
  recursive = TRUE
)

if (length(file_paths) == 0) {
  stop("No CSV files found under the specified root folder.")
}

# ---- classify each file ----
files_tbl <- tibble(path = file_paths) %>%
  mutate(
    file_name = basename(path),
    state_folder = basename(dirname(path)),
    geography = case_when(
      str_detect(file_name, regex("precinct|pct", ignore_case = TRUE)) ~ "precinct",
      str_detect(file_name, regex("county", ignore_case = TRUE)) ~ "county",
      str_detect(file_name, regex("state", ignore_case = TRUE)) ~ "state",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(geography))

# ---- helper to read + bind ----
read_and_bind <- function(files) {
  if (length(files) == 0) return(NULL)
  
  map_dfr(
    files,
    ~ readr::read_csv(.x, show_col_types = FALSE) %>%
      mutate(source_file = basename(.x))
  )
}

# ---- helper to safely run summarize_results ----
run_summary <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(NULL)
  DownBallotR::summarize_results(df)
}

# ---- helper to flatten summarize_results output into one row ----
flatten_summary <- function(summary_obj, state_folder, geography) {
  if (is.null(summary_obj)) return(NULL)
  
  tibble(
    state_folder = state_folder,
    geography = geography,
    detected_state = summary_obj$state,
    n_years = summary_obj$n_years,
    n_elections = summary_obj$n_elections,
    n_candidates = summary_obj$n_candidates,
    years_min = if (length(summary_obj$years) > 0) min(summary_obj$years, na.rm = TRUE) else NA_integer_,
    years_max = if (length(summary_obj$years) > 0) max(summary_obj$years, na.rm = TRUE) else NA_integer_,
    federal_elections = if (!is.null(summary_obj$office_level_breakdown) && "Federal" %in% names(summary_obj$office_level_breakdown)) summary_obj$office_level_breakdown[["Federal"]] else 0L,
    state_elections   = if (!is.null(summary_obj$office_level_breakdown) && "State"   %in% names(summary_obj$office_level_breakdown)) summary_obj$office_level_breakdown[["State"]] else 0L,
    local_elections   = if (!is.null(summary_obj$office_level_breakdown) && "Local"   %in% names(summary_obj$office_level_breakdown)) summary_obj$office_level_breakdown[["Local"]] else 0L,
    n_federal_offices = if (!is.null(summary_obj$offices_by_level) && "Federal" %in% names(summary_obj$offices_by_level)) length(summary_obj$offices_by_level[["Federal"]]) else 0L,
    n_state_offices   = if (!is.null(summary_obj$offices_by_level) && "State"   %in% names(summary_obj$offices_by_level)) length(summary_obj$offices_by_level[["State"]]) else 0L,
    n_local_offices   = if (!is.null(summary_obj$offices_by_level) && "Local"   %in% names(summary_obj$offices_by_level)) length(summary_obj$offices_by_level[["Local"]]) else 0L
  )
}

# ---- combine files by state folder + geography ----
grouped_files <- files_tbl %>%
  group_by(state_folder, geography) %>%
  summarise(files = list(path), .groups = "drop")

# ---- read, summarize, flatten ----
summary_by_group <- grouped_files %>%
  mutate(
    data = map(files, read_and_bind),
    summary_obj = map(data, run_summary),
    summary_row = pmap(
      list(summary_obj, state_folder, geography),
      flatten_summary
    )
  ) %>%
  select(summary_row) %>%
  unnest(summary_row)

# ---- total across all your data ----
grand_totals <- summary_by_group %>%
  summarise(
    groups_processed = n(),
    distinct_state_folders = n_distinct(state_folder),
    total_n_years = sum(n_years, na.rm = TRUE),
    total_n_elections = sum(n_elections, na.rm = TRUE),
    total_n_candidates = sum(n_candidates, na.rm = TRUE),
    total_federal_elections = sum(federal_elections, na.rm = TRUE),
    total_state_elections = sum(state_elections, na.rm = TRUE),
    total_local_elections = sum(local_elections, na.rm = TRUE),
    total_federal_offices = sum(n_federal_offices, na.rm = TRUE),
    total_state_offices = sum(n_state_offices, na.rm = TRUE),
    total_local_offices = sum(n_local_offices, na.rm = TRUE)
  )

# ---- optional: totals by geography ----
totals_by_geography <- summary_by_group %>%
  group_by(geography) %>%
  summarise(
    groups_processed = n(),
    total_n_years = sum(n_years, na.rm = TRUE),
    total_n_elections = sum(n_elections, na.rm = TRUE),
    total_n_candidates = sum(n_candidates, na.rm = TRUE),
    total_federal_elections = sum(federal_elections, na.rm = TRUE),
    total_state_elections = sum(state_elections, na.rm = TRUE),
    total_local_elections = sum(local_elections, na.rm = TRUE),
    .groups = "drop"
  )

# ---- inspect ----
print(summary_by_group)
print(grand_totals)
print(totals_by_geography)