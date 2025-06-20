# Ideas for update_all_data.R

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
library(nflreadr)
# ...add any more as needed (qs, tibble, etc.)

# ============================================================================ #
# LOAD PIPELINE FUNCTIONS ----
# ============================================================================ #
if (requireNamespace("devtools", quietly = TRUE)) {
  devtools::load_all()
}

# ============================================================================ #
# GLOBAL VARIABLES ----
# ============================================================================ #

start_season <- 2006
current_season <- get_current_season()
all_seasons <- start_season:current_season
github_data_repo <- "TylerPollard410/nflendzoneData"
full_build <- TRUE # Set FALSE for incremental update

seasons_to_process <- if (full_build) all_seasons else current_season

# List all tags you need releases for
needed_tags <- c(
  "season_standings", "weekly_standings", "elo", "srs", "epa", "scores",
  "series", "turnover", "redzone", "model_data_long", "model_data"
  # Add/remove as needed
)

# ============================================================================ #
# ENSURE ALL RELEASE TAGS EXIST IN GITHUB DATA REPO ----
# ============================================================================ #
purrr::walk(needed_tags, ~ piggyback::pb_new_release(repo = github_data_repo, tag = .x))

# ============================================================================ #
# CORE DATA PRODUCTS ----
# ============================================================================ #

cat("%%%% Generating game_data %%%%\n")
game_data <- compute_game_data(seasons = all_seasons)

cat("%%%% Generating game_data_long %%%%\n")
game_data_long <- compute_game_data_long(game_df = game_data)

cat("%%%% Generating pbp %%%%\n")
pbp <- compute_pbp_data(seasons = all_seasons)

cat("%%%% Generating player_offense %%%%\n")
player_offense <- compute_player_data(
  seasons = all_seasons,
  game_long_df = game_data_long,
  stat = "offense"
)

# =================== Template block for all incrementally-updated frames ==================== #

# Helper to streamline logic for each incremental product:
save_and_upload <- function(tag, compute_fn, ...) {
  cat(glue::glue("%%%% Generating {tag} %%%%\n"))

  archive_dir <- file.path("artifacts/data-archive", tag)
  dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
  full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
  full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))

  prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

  if (full_build || is.null(prior_data)) {
    # Compute full data for all seasons
    full_data <- compute_fn(...)
  } else {
    # Compute only for current season and merge with archive
    # Assumes all compute_* functions take a 'season' or subsettable input
    # (Adjust args as needed per function)
    new_data <- compute_fn(..., season = current_season)
    prior_data_no_current <- prior_data |> filter(season < current_season)
    full_data <- bind_rows(prior_data_no_current, new_data)
  }

  # Save full archive
  saveRDS(full_data, full_rds_path, compress = "xz")
  write_csv(full_data, full_csv_path)

  # Save and upload per-season files
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
    piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
    piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
    piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
  })
}

# =================== Apply to each data frame =================== #

# Example for season_standings:
save_and_upload(
  tag = "season_standings",
  compute_fn = function(game_df, ...) compute_season_standings_data(
    game_df = game_df, tol = 1e-3, max_iter = 200, print_message = TRUE, ...
  ),
  game_df = game_data
)

save_and_upload(
  tag = "weekly_standings",
  compute_fn = function(game_df, ...) compute_weekly_standings_data(
    game_df = game_df, tol = 1e-3, max_iter = 200, reset = TRUE, recompute_all = FALSE, ...
  ),
  game_df = game_data
)

save_and_upload(
  tag = "elo",
  compute_fn = function(game_df, ...) compute_elo_data(
    game_df = game_df, initial_elo = 1500, K = 20, home_advantage = 0, d = 400,
    apply_margin_multiplier = TRUE, recompute_all = FALSE, season_factor = 0.6, ...
  ),
  game_df = game_data
)

save_and_upload(
  tag = "srs",
  compute_fn = function(game_df, ...) compute_srs_data(
    game_df = game_df, resets = c(TRUE, as.list(5:20)), tol = 1e-3, max_iter = 200,
    recompute_all = FALSE, ...
  ),
  game_df = game_data
)

save_and_upload(
  tag = "epa",
  compute_fn = function(pbp_df, ...) compute_epa_data(
    pbp_df = pbp_df, scaled_wp = FALSE, ...
  ),
  pbp_df = pbp
)

save_and_upload(
  tag = "scores",
  compute_fn = function(game_long_df, pbp_df, ...) compute_scores_data(
    game_long_df = game_long_df, pbp_df = pbp_df, seasons = seasons_to_process,
    sum_level = "week", stat_level = "team", season_level = "REG+POST",
    recompute_all = FALSE, ...
  ),
  game_long_df = game_data_long,
  pbp_df = pbp
)

save_and_upload(
  tag = "series",
  compute_fn = function(game_long_df, pbp_df, ...) compute_series_data(
    game_long_df = game_long_df, pbp_df = pbp_df, recompute_all = FALSE, ...
  ),
  game_long_df = game_data_long,
  pbp_df = pbp
)

save_and_upload(
  tag = "turnover",
  compute_fn = function(game_long_df, pbp_df, ...) compute_turnover_data(
    game_long_df = game_long_df, pbp_df = pbp_df, ...
  ),
  game_long_df = game_data_long,
  pbp_df = pbp
)

save_and_upload(
  tag = "redzone",
  compute_fn = function(game_long_df, pbp_df, ...) compute_redzone_data(
    game_long_df = game_long_df, pbp_df = pbp_df, ...
  ),
  game_long_df = game_data_long,
  pbp_df = pbp
)

save_and_upload(
  tag = "model_data_long",
  compute_fn = function(archive_loc, ...) compute_model_data_long(
    archive_loc = archive_loc, window = 5, span = 5, ...
  ),
  archive_loc = "artifacts/data-archive/"
)

save_and_upload(
  tag = "model_data",
  compute_fn = function(model_data_long, game_data, ...) compute_model_data(
    model_data_long = model_data_long, game_data = game_data, ...
  ),
  model_data_long = readRDS(file.path("artifacts/data-archive/model_data_long/model_data_long.rds")),
  game_data = game_data
)

cat("\n%%%% DATA UPDATE COMPLETE %%%%\n")





# ELO SPECIFIC ----
if (full_build || is.null(prior_data)) {
  # full build as before
  full_data <- compute_elo_data(
    game_df = game_data,  # all seasons
    ... # params
  )
} else {
  # Subset prior seasons for game_df
  game_data_current <- game_data |> filter(season == current_season)
  prior_elo <- prior_data |> filter(season < current_season)
  # Pass prior_elo to your compute_elo_data if needed (as argument)
  new_elo <- compute_elo_data(
    game_df = game_data_current,
    prior_elo = prior_elo,
    ... # other params
  )
  # bind together for full archive
  full_data <- bind_rows(prior_elo, new_elo)
}


# Suppose you want per-week releases for weekly_standings

save_and_upload_by_group <- function(tag, compute_fn, group_vars, ...) {
  # ...initial logic as before...

  # After `full_data` is created:
  grouped <- full_data |> group_split(!!!syms(group_vars))

  purrr::walk(grouped, \(group_df) {
    group_vals <- group_df |> dplyr::slice(1) |> dplyr::select(!!!syms(group_vars)) |> as.list()
    file_stub <- paste0(tag, "_", paste(unlist(group_vals), collapse = "_"))
    pq_path  <- file.path(archive_dir, paste0(file_stub, ".parquet"))
    rds_path <- file.path(archive_dir, paste0(file_stub, ".rds"))
    csv_path <- file.path(archive_dir, paste0(file_stub, ".csv"))
    arrow::write_parquet(group_df, pq_path)
    saveRDS(group_df, rds_path, compress = "xz")
    readr::write_csv(group_df, csv_path)
    piggyback::pb_upload(pq_path, repo = github_data_repo, tag = tag)
    piggyback::pb_upload(rds_path, repo = github_data_repo, tag = tag)
    piggyback::pb_upload(csv_path, repo = github_data_repo, tag = tag)
  })
}

# Usage example for weekly_standings:
save_and_upload_by_group(
  tag = "weekly_standings",
  compute_fn = function(game_df, ...) compute_weekly_standings_data(...),
  group_vars = c("season", "week"),
  game_df = game_data
)



cat("%%%% Generating elo %%%%\n")
tag <- "elo"
archive_dir <- file.path("artifacts/data-archive", tag)
dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))

prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  full_data <- compute_elo_data(
    game_df = game_data,
    initial_elo = 1500, K = 20, home_advantage = 0, d = 400,
    apply_margin_multiplier = TRUE, recompute_all = FALSE, season_factor = 0.6
  )
} else {
  game_data_current <- game_data |> filter(season == current_season)
  prior_elo <- prior_data |> filter(season < current_season)
  new_elo <- compute_elo_data(
    game_df = game_data_current,
    prior_elo = prior_elo,    # You need to accept this in your function!
    initial_elo = 1500, K = 20, home_advantage = 0, d = 400,
    apply_margin_multiplier = TRUE, recompute_all = FALSE, season_factor = 0.6
  )
  full_data <- bind_rows(prior_elo, new_elo)
}
# Save/upload logic as above
