# update_all_data.R -- run from pipeline repo root
#
# This script builds each data frame for the EndZone NFL data pipeline,
# incrementally updates the current season, and uploads current-season
# data as .parquet, .rds, and .csv to GitHub Releases in nflendzoneData.
# A local .rds archive (all seasons) is maintained for each data frame.
# If no archive is found, the script will attempt to download one from
# the release asset. If that also fails, a full rebuild is triggered.

# ============================================================================ #
# LIBRARIES ----
# ============================================================================ #

library(arrow)
library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(lubridate)
library(piggyback)
# library(nflverse) # Uncomment if needed for nflverse_releases()
library(nflreadr)
# ...add any more you need

# ============================================================================ #
# LOAD PIPELINE FUNCTIONS ----
# ============================================================================ #
if (requireNamespace("devtools", quietly = TRUE)) {
  devtools::load_all()
}

# ============================================================================ #
# GLOBAL VARIABLES ----
# ============================================================================ #

# The earliest season to compute data for (change if your archive grows)
start_season <- 2006

# The most recent season (pulled from helper function)
current_season <- get_current_season()

# A vector of all seasons to consider
all_seasons <- start_season:current_season

github_data_repo <- "TylerPollard410/nflendzoneData"
full_build <- TRUE # Set FALSE for incremental update

seasons_to_process <- if (full_build) all_seasons else current_season

# ============================================================================ #
# ENSURE ALL RELEASE TAGS EXIST IN GITHUB DATA REPO ----
# ============================================================================ #

needed_tags <- c(
  "season_standings", "weekly_standings", "elo", "srs", "epa", "scores",
  "series", "turnover", "redzone", "model_data_long", "model_data"
  # Add/remove as needed
)
purrr::walk(needed_tags, ~ piggyback::pb_new_release(repo = github_data_repo, tag = .x))


# ============================================================================ #
# NFLVERSE RELEASES ----
# ============================================================================ #
# (for future incremental logic)
# Uncomment the next line if you want to pre-cache these releases now
# nflverse_data_releases <- nflverse_releases()



# ============================================================================ #
# game_data ----
# ============================================================================ #

cat("%%%% Generating game_data %%%%\n")

game_data <- compute_game_data(seasons = all_seasons)


# ============================================================================ #
# game_data_long ----
# ============================================================================ #

cat("%%%% Generating game_data_long %%%%\n")

game_data_long <- compute_game_data_long(game_df = game_data)


# ============================================================================ #
# pbp_data ----
# ============================================================================ #

cat("%%%% Generating pbp_data %%%%\n")

pbp_data <- compute_pbp_data(seasons = all_seasons)


# ============================================================================ #
# player_offense_data ----
# ============================================================================ #

cat("%%%% Generating player_offense_data %%%%\n")

player_offense_data <- compute_player_data(
  seasons = all_seasons,
  game_long_df = game_data_long,
  stat = "offense"
)


# ============================================================================ #
# season_standings ----
# ============================================================================ #

cat("%%%% Generating season_standings %%%%\n")

tag <- "season_standings"
archive_dir <- file.path("artifacts/data-archive", tag)
dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))

# Load prior archive if exists
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  full_data <- compute_season_standings_data(
    game_df = game_data,
    tol = 1e-3, max_iter = 200, print_message = TRUE
  )
} else {
  game_data_current <- game_data |> filter(season == current_season)
  new_data <- compute_season_standings_data(
    game_df = game_data_current,
    tol = 1e-3, max_iter = 200, print_message = TRUE
  )
  prior_data_no_current <- prior_data |> filter(season < current_season)
  full_data <- bind_rows(prior_data_no_current, new_data)
}

# Save full archive locally
saveRDS(full_data, full_rds_path, compress = "xz")
write_csv(full_data, full_csv_path)

# Save/upload per-season files
season_files <- purrr::map(seasons_to_process, \(season) {
  season_df <- full_data |> filter(season == !!season)
  pq_path  <- file.path(archive_dir, paste0(tag, "_", season, ".parquet"))
  rds_path <- file.path(archive_dir, paste0(tag, "_", season, ".rds"))
  csv_path <- file.path(archive_dir, paste0(tag, "_", season, ".csv"))
  arrow::write_parquet(season_df, pq_path)
  saveRDS(season_df, rds_path, compress = "xz")
  write_csv(season_df, csv_path)
  list(parquet = pq_path, rds = rds_path, csv = csv_path)
})

purrr::walk(season_files, \(filelist) {
  #piggyback::pb_new_release(repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
})



# ============================================================================ #
# weekly_standings ----
# ============================================================================ #

cat("%%%% Generating weekly_standings %%%%\n")

tag <- "weekly_standings"
archive_dir <- file.path("artifacts/data-archive", tag)
dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))

# Load prior archive if exists
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  full_data <- compute_weekly_standings_data(
    game_df = game_data,
    tol = 1e-3,
    max_iter = 200,
    reset = TRUE,
    recompute_all = FALSE
  )
} else {
  game_data_current <- game_data |> filter(season == current_season)
  new_data <- compute_weekly_standings_data(
    game_df = game_data_current,
    tol = 1e-3,
    max_iter = 200,
    reset = TRUE,
    recompute_all = FALSE
  )
  prior_data_no_current <- prior_data |> filter(season < current_season)
  full_data <- bind_rows(prior_data_no_current, new_data)
}

# Save full archive locally
saveRDS(full_data, full_rds_path, compress = "xz")
write_csv(full_data, full_csv_path)

# Write & upload each season (files are just temporary outputs)
season_files <- purrr::map(seasons_to_process, \(season) {
  season_df <- full_data |> filter(season == !!season)
  pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
  rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
  csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
  arrow::write_parquet(season_df, pq_path)
  saveRDS(season_df, rds_path, compress = "xz")
  write_csv(season_df, csv_path)
  list(parquet = pq_path, rds = rds_path, csv = csv_path)
})

purrr::walk(season_files, \(filelist) {
  piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
})



# ============================================================================ #
# elo ----
# ============================================================================ #

cat("%%%% Generating elo %%%%\n")

tag <- "elo"
archive_dir <- file.path("artifacts/data-archive", tag)
dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))

# Load prior archive if exists
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  full_data <- compute_elo_data(
    game_df = game_data,
    initial_elo = 1500,
    K = 20,
    home_advantage = 0,
    d = 400,
    apply_margin_multiplier = TRUE,
    recompute_all = FALSE,
    season_factor = 0.6
  )
} else {
  game_data_current <- game_data |> filter(season == current_season)
  # If your function supports incremental builds using prior_data, pass it here
  new_data <- compute_elo_data(
    game_df = game_data_current,
    initial_elo = 1500,
    K = 20,
    home_advantage = 0,
    d = 400,
    apply_margin_multiplier = TRUE,
    recompute_all = FALSE,
    season_factor = 0.6,
    cache_file = full_rds_path #prior_data |> filter(season < current_season)
  )
  prior_data_no_current <- prior_data |> filter(season < current_season)
  full_data <- bind_rows(prior_data_no_current, new_data)
}

# Save full archive locally (one file only)
saveRDS(full_data, full_rds_path, compress = "xz")
write_csv(full_data, full_csv_path)

# Per-season files: only for uploading to GitHub release
season_files <- purrr::map(seasons_to_process, \(season) {
  season_df <- full_data |> filter(season == !!season)
  pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
  rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
  csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
  arrow::write_parquet(season_df, pq_path)
  saveRDS(season_df, rds_path, compress = "xz")
  write_csv(season_df, csv_path)
  list(parquet = pq_path, rds = rds_path, csv = csv_path)
})

purrr::walk(season_files, \(filelist) {
  piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
  # Optionally clean up temp files:
  #unlink(unlist(filelist), force = TRUE)
})


# ============================================================================ #
# srs ----
# ============================================================================ #

cat("%%%% Generating srs %%%%\n")

tag <- "srs"
archive_dir <- file.path("artifacts/data-archive", tag)
dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))

# Load prior archive if exists
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  full_data <- compute_srs_data(
    game_df = game_data,
    resets = c(TRUE, as.list(5:20)),
    tol = 1e-3,
    max_iter = 200,
    recompute_all = FALSE
    # add other arguments if needed
  )
} else {
  game_data_current <- game_data |> filter(season == current_season)
  new_data <- compute_srs_data(
    game_df = game_data_current,
    resets = c(TRUE, as.list(5:20)),
    tol = 1e-3,
    max_iter = 200,
    recompute_all = FALSE
    # add other arguments if needed
  )
  prior_data_no_current <- prior_data |> filter(season < current_season)
  full_data <- bind_rows(prior_data_no_current, new_data)
}

# Save full archive locally (single .rds, single .csv)
saveRDS(full_data, full_rds_path, compress = "xz")
write_csv(full_data, full_csv_path)

# Per-season files: only for uploading to GitHub release
season_files <- purrr::map(seasons_to_process, \(season) {
  season_df <- full_data |> filter(season == !!season)
  pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
  rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
  csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
  arrow::write_parquet(season_df, pq_path)
  saveRDS(season_df, rds_path, compress = "xz")
  write_csv(season_df, csv_path)
  list(parquet = pq_path, rds = rds_path, csv = csv_path)
})

purrr::walk(season_files, \(filelist) {
  piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
  # Optionally, clean up temp files:
  # unlink(unlist(filelist), force = TRUE)
})



# ============================================================================ #
# epa ----
# ============================================================================ #

cat("%%%% Generating epa %%%%\n")

tag <- "epa"
archive_dir <- file.path("artifacts/data-archive", tag)
dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))

# Load prior archive if exists
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  full_data <- compute_epa_data(
    pbp_df = pbp_data,
    scaled_wp = FALSE
  )
} else {
  pbp_data_current <- pbp_data |> filter(season == current_season)
  new_data <- compute_epa_data(
    pbp_df = pbp_data_current,
    scaled_wp = FALSE
  )
  prior_data_no_current <- prior_data |> filter(season < current_season)
  full_data <- bind_rows(prior_data_no_current, new_data)
}

# Save full archive locally (single .rds, single .csv)
saveRDS(full_data, full_rds_path, compress = "xz")
write_csv(full_data, full_csv_path)

# Per-season files: only for uploading to GitHub release
season_files <- purrr::map(seasons_to_process, \(season) {
  season_df <- full_data |> filter(season == !!season)
  pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
  rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
  csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
  arrow::write_parquet(season_df, pq_path)
  saveRDS(season_df, rds_path, compress = "xz")
  write_csv(season_df, csv_path)
  list(parquet = pq_path, rds = rds_path, csv = csv_path)
})

purrr::walk(season_files, \(filelist) {
  piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
  # Optionally: unlink(unlist(filelist), force = TRUE)
})


# ============================================================================ #
# scores ----
# ============================================================================ #

cat("%%%% Generating scores %%%%\n")

tag <- "scores"
archive_dir <- file.path("artifacts/data-archive", tag)
dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))

# Load prior archive if exists
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  full_data <- compute_scores_data(
    game_long_df = game_data_long,
    pbp_df = pbp_data,
    seasons = all_seasons,
    sum_level = "week",
    stat_level = "team",
    season_level = "REG+POST",
    # stats_loc = "artifacts/data-archive/nflStatsWeek.rda", # Uncomment if needed
    recompute_all = FALSE
  )
} else {
  game_data_long_current <- game_data_long |> filter(season == current_season)
  pbp_data_current <- pbp_data |> filter(season == current_season)
  new_data <- compute_scores_data(
    game_long_df = game_data_long_current,
    pbp_df = pbp_data_current,
    seasons = current_season,
    sum_level = "week",
    stat_level = "team",
    season_level = "REG+POST",
    # stats_loc = "artifacts/data-archive/nflStatsWeek.rda", # Uncomment if needed
    recompute_all = FALSE
  )
  prior_data_no_current <- prior_data |> filter(season < current_season)
  full_data <- bind_rows(prior_data_no_current, new_data)
}

# Save full archive locally (single .rds, single .csv)
saveRDS(full_data, full_rds_path, compress = "xz")
write_csv(full_data, full_csv_path)

# Per-season files: only for uploading to GitHub release
season_files <- purrr::map(seasons_to_process, \(season) {
  season_df <- full_data |> filter(season == !!season)
  pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
  rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
  csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
  arrow::write_parquet(season_df, pq_path)
  saveRDS(season_df, rds_path, compress = "xz")
  write_csv(season_df, csv_path)
  list(parquet = pq_path, rds = rds_path, csv = csv_path)
})

purrr::walk(season_files, \(filelist) {
  piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
  piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
  # Optionally: unlink(unlist(filelist), force = TRUE)
})







