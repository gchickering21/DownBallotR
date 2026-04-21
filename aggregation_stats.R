library(tidyverse)
library(DownBallotR)

# ---- user inputs ----
#election_stats/colorado/
folder_path <- "data/election_stats/idaho"
file_pattern <- "\\.csv$"

# ---- get all files ----
file_paths <- list.files(
  path = folder_path,
  pattern = file_pattern,
  full.names = TRUE
)

if (length(file_paths) == 0) {
  stop("No files found in the specified folder.")
}

# ---- split files by name ----
state_files <- file_paths[
  str_detect(basename(file_paths), regex("state", ignore_case = TRUE))
]

county_files <- file_paths[
  str_detect(basename(file_paths), regex("county", ignore_case = TRUE))
]

precinct_files <- file_paths[
  str_detect(basename(file_paths), regex("precinct", ignore_case = TRUE))
]

# ---- helper to read + bind ----
read_and_bind <- function(files) {
  if (length(files) == 0) return(NULL)

  files %>%
    set_names() %>%
    map_dfr(~ readr::read_csv(.x, show_col_types = FALSE) %>%
              mutate(source_file = basename(.x)))
}

# ---- create combined datasets ----
state_data <- read_and_bind(state_files)
county_data <- read_and_bind(county_files)
precinct_data <- read_and_bind(precinct_files)

# ---- helper to safely run summary ----
run_summary <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(NULL)
  DownBallotR::summarize_results(df)
}

# ---- run your package function ----
state_result <- run_summary(state_data)
county_result <- run_summary(county_data)
precinct_result <- run_summary(precinct_data)

