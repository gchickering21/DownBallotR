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
      str_detect(file_name, regex("vote_method", ignore_case = TRUE)) ~ NA_character_,
      str_detect(file_name, regex("precinct|pct", ignore_case = TRUE)) ~ "precinct",
      str_detect(file_name, regex("county|parish|town", ignore_case = TRUE)) ~ "county",
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
      mutate(
        source_file   = basename(.x),
        election_date = as.character(election_date)
      )
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

# ---- elections by state x geography ----
elections_by_level <- summary_by_group %>%
  filter(geography %in% c("state", "county", "precinct")) %>%
  select(state_folder, geography, n_elections) %>%
  pivot_wider(
    names_from  = geography,
    values_from = n_elections,
    names_prefix = "n_elections_"
  ) %>%
  arrange(state_folder)

# ---- inspect ----
print(summary_by_group)
print(grand_totals)
print(totals_by_geography)
print(elections_by_level)

# ============================================================
# LOCAL OFFICES OF INTEREST
# ============================================================

# Regex patterns for each target office type (case-insensitive)
local_office_patterns <- list(
  Mayor = paste(
    "mayor",
    "mayoralty",
    sep = "|"
  ),
  City_Council = paste(
    "city council",
    "town council",
    "village council",
    "municipal council",
    "city commissioner",
    "alderman",
    "alderwoman",
    "alderperson",
    "alder(man|woman|person|)",
    "selectman",
    "selectmen",
    "selectwoman",
    "selectperson",
    "councilman",
    "councilwoman",
    "councilperson",
    "council member",
    "council at.large",
    "city board",
    sep = "|"
  ),
  County_Legislature = paste(
    "county comm(ission|issioner)",
    "board of county comm(ission|issioner)",
    "county council",
    "county supervisor",
    "county freeholder",
    "county legislature",
    "county board",
    "county committee",
    "board of supervisors",
    "board of freeholders",
    sep = "|"
  ),
  County_Executive = paste(
    "county executive",
    "county manager",
    "county administrator",
    "county president",
    "county mayor",
    sep = "|"
  ),
  Sheriff            = "sheriff",
  School_Board       = paste(
    "school board",
    "board of education",
    "board of ed\\b",
    "school committee",
    "school director",
    "school district board",
    "education board",
    sep = "|"
  )
)

classify_local_office <- function(office) {
  office <- tolower(trimws(office))
  for (type in names(local_office_patterns)) {
    if (grepl(local_office_patterns[[type]], office, ignore.case = TRUE)) {
      return(type)
    }
  }
  NA_character_
}

# ---- bind all state-level CSV data together ----
all_data <- grouped_files %>%
  mutate(data = map(files, read_and_bind)) %>%
  pull(data) %>%
  bind_rows()

# ---- classify offices and filter to target types ----
local_offices_data <- all_data %>%
  filter(office_level == "Local", !is.na(office)) %>%
  mutate(office_type = map_chr(office, classify_local_office)) %>%
  filter(!is.na(office_type))

# ---- elections per state x office type ----
local_office_by_state <- local_offices_data %>%
  group_by(state, office_type) %>%
  summarise(
    n_elections  = n_distinct(election_year, office, district, na.rm = TRUE),
    years_min    = min(election_year, na.rm = TRUE),
    years_max    = max(election_year, na.rm = TRUE),
    n_candidates = n_distinct(candidate, na.rm = TRUE),
    raw_offices  = paste(sort(unique(office)), collapse = " | "),
    .groups = "drop"
  ) %>%
  arrange(office_type, state)

# ---- coverage matrix: which states have which office types ----
office_coverage <- local_office_by_state %>%
  select(state, office_type, n_elections) %>%
  pivot_wider(
    names_from  = office_type,
    values_from = n_elections,
    values_fill = 0L
  ) %>%
  arrange(state)

# ---- totals across all states per office type ----
office_type_totals <- local_office_by_state %>%
  group_by(office_type) %>%
  summarise(
    n_states     = n_distinct(state),
    n_elections  = sum(n_elections, na.rm = TRUE),
    n_candidates = sum(n_candidates, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(n_elections))

# ---- inspect ----
cat("\n=== Local offices of interest — by state x office type ===\n")
print(local_office_by_state)

cat("\n=== Coverage matrix (n elections per state x office type) ===\n")
print(office_coverage)

cat("\n=== Totals per office type across all states ===\n")
print(office_type_totals)

# ---- browse raw office name variants per type (useful for QA) ----
cat("\n=== Raw office name variants captured per type ===\n")
local_offices_data %>%
  distinct(office_type, office) %>%
  arrange(office_type, office) %>%
  print(n = Inf)

# ---- local offices NOT captured by any target pattern ----
uncaptured_local <- all_data %>%
  filter(office_level == "Local", !is.na(office)) %>%
  mutate(office_type = map_chr(office, classify_local_office)) %>%
  filter(is.na(office_type)) %>%
  group_by(state, office) %>%
  summarise(
    n_elections  = n_distinct(election_year, office, district, na.rm = TRUE),
    years_min    = min(election_year, na.rm = TRUE),
    years_max    = max(election_year, na.rm = TRUE),
    n_candidates = n_distinct(candidate, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(state, desc(n_elections))

cat("\n=== Local offices NOT captured by any target pattern ===\n")
print(uncaptured_local, n = Inf)
