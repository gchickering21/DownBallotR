library(tidyverse)

# ---- user input ----
target_state <- "louisiana"   # state folder name, e.g. "colorado", "georgia", "northcarolina"

# ---- find files for this state ----
root_folder <- "data"

files_tbl <- tibble(path = list.files(root_folder, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)) %>%
  mutate(
    file_name    = basename(path),
    state_folder = basename(dirname(path)),
    geography = case_when(
      str_detect(file_name, regex("vote_method", ignore_case = TRUE)) ~ NA_character_,
      str_detect(file_name, regex("precinct|pct", ignore_case = TRUE)) ~ "precinct",
      str_detect(file_name, regex("county|parish|town", ignore_case = TRUE)) ~ "county",
      str_detect(file_name, regex("state",  ignore_case = TRUE)) ~ "state",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(geography), state_folder == target_state)

if (nrow(files_tbl) == 0) stop(sprintf("No files found for state folder '%s'.", target_state))

# ---- read all files for each level ----
levels_data <- files_tbl %>%
  group_by(geography) %>%
  summarise(files = list(path), .groups = "drop") %>%
  mutate(data = map(files, ~ map_dfr(.x, readr::read_csv, show_col_types = FALSE)))

get_level <- function(geo) {
  row <- filter(levels_data, geography == geo)
  if (nrow(row) == 0) return(NULL)
  row$data[[1]]
}

state_df    <- get_level("state")
county_df   <- get_level("county")
precinct_df <- get_level("precinct")

# ---- detect the election key columns for a dataframe ----
get_election_key <- function(df) {
  has <- function(...) all(c(...) %in% names(df))
  if (has("election_name", "election_date", "office")) return(c("election_name", "election_date", "office"))
  if (has("election_id"))                               return("election_id")
  if (has("election_year", "election_type", "office"))  return(c("election_year", "election_type", "office"))
  if (has("election_date", "office"))                   return(c("election_date", "office"))
  stop("Cannot determine election key columns for this dataframe.")
}

# ---- get distinct elections using only the key columns ----
distinct_elections <- function(df) {
  if (is.null(df)) return(NULL)
  key <- get_election_key(df)
  df %>% select(all_of(key)) %>% distinct()
}

state_el    <- distinct_elections(state_df)
county_el   <- distinct_elections(county_df)
precinct_el <- distinct_elections(precinct_df)

# ---- compare two levels, returning rows unique to each ----
compare_levels <- function(df_a, df_b, name_a, name_b) {
  if (is.null(df_a) || is.null(df_b)) {
    message(sprintf("  Skipping %s vs %s — one or both levels not present.", name_a, name_b))
    return(NULL)
  }

  key_cols <- intersect(names(df_a), names(df_b))

  # Only flag elections present in the lower-geography level (df_b) but missing
  # from the higher-geography level (df_a). The reverse is expected and okay.
  anti_join(df_b, df_a, by = key_cols) %>%
    mutate(.present_in = name_b, .absent_from = name_a) %>%
    arrange(across(any_of(c("election_year", "election_date", "office"))))
}

state_vs_county    <- compare_levels(state_el, county_el,   "state",  "county")
state_vs_precinct  <- compare_levels(state_el, precinct_el, "state",  "precinct")
county_vs_precinct <- compare_levels(county_el, precinct_el, "county", "precinct")

# ---- combine all differences into one dataframe ----
level_differences <- bind_rows(
  if (!is.null(state_vs_county))    mutate(state_vs_county,    .comparison = "state vs county"),
  if (!is.null(state_vs_precinct))  mutate(state_vs_precinct,  .comparison = "state vs precinct"),
  if (!is.null(county_vs_precinct)) mutate(county_vs_precinct, .comparison = "county vs precinct")
) %>%
  relocate(.comparison, .present_in, .absent_from)
