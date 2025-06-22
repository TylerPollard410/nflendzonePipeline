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
#detach("package:nflendzonePipeline",unload = TRUE, force = TRUE)
#install.packages(".", repos = NULL, type = "source")
library(arrow)
library(glue)
library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(lubridate)
library(piggyback)
library(tidyr)
library(nflreadr)
library(nflfastR)
library(nflseedR)
library(nflendzonePipeline)

# ============================================================================ #
# LOAD PIPELINE FUNCTIONS ----
# ============================================================================ #
# if (requireNamespace("devtools", quietly = TRUE)) {
#   devtools::load_all()
# }

# ============================================================================ #
# GLOBAL VARIABLES ----
# ============================================================================ #
start_season <- 2006
current_season <- get_current_season()
all_seasons <- start_season:current_season
github_data_repo <- "TylerPollard410/nflendzoneData"

# Get full_build flag from command line argument or default to FALSE
args <- commandArgs(trailingOnly = TRUE)
full_build <- if (length(args) > 0) as.logical(args[1]) else FALSE # Set FALSE for incremental update
#full_build <- TRUE
seasons_to_process <- if (full_build) all_seasons else current_season

needed_tags <- c(
  "season_standings",
  "weekly_standings",
  "elo",
  "srs",
  "epa",
  "scores",
  "series",
  "turnover",
  "redzone",
  "feature_data_long",
  #"feature_data",
  #"model_data_long",
  "model_data"
  # Add/remove as needed
)

# ============================================================================ #
# ENSURE ALL RELEASE TAGS EXIST IN GITHUB DATA REPO ----
# ============================================================================ #
suppressWarnings({
  purrr::walk(needed_tags, ~ piggyback::pb_new_release(repo = github_data_repo, tag = .x))
})


# ============================================================================ #
# NFLVERSE RELEASES ----
# ============================================================================ #
# (for future incremental logic)
# Uncomment the next line if you want to pre-cache these releases now
# nflverse_data_releases <- nflverse_releases()



# ============================================================================ #
# DATA GENERATION BLOCKS ----
# ============================================================================ #

# ============================================================================ #
## nflverse Source Data ====
# (always rebuild, no incremental logic needed)

# ---------------------------------------------------------------------------- #
### game_data ----
cat("%%%% Generating game_data %%%%\n")
game_data <- compute_game_data(seasons = all_seasons)

# ---------------------------------------------------------------------------- #
### game_data_long ----
cat("%%%% Generating game_data_long %%%%\n")
game_data_long <- compute_game_data_long(game_df = game_data)

# ---------------------------------------------------------------------------- #
### pbp_data ----
cat("%%%% Generating pbp_data %%%%\n")
pbp_data <- compute_pbp_data(seasons = all_seasons)



# ============================================================================ #
## nflverse Function Data ====

# ---------------------------------------------------------------------------- #
### player_offense_data ----
cat("%%%% Generating player_offense_data %%%%\n")
player_offense_data <- compute_player_data(
  seasons = all_seasons,
  game_long_df = game_data_long,
  stat = "offense"
)



# ============================================================================ #
## Calculated/Derived Data ====
# (incremental logic, archive)

# ---------------------------------------------------------------------------- #
### season_standings ----
cat("%%%% Generating season_standings %%%%\n")
tag <- "season_standings"
archive_dir <- file.path("artifacts/data-archive", tag)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || should_rebuild(game_data, prior_data, id_cols = "season")) {
  cat("[season_standings] Recomputing standings...\n")
  full_data <- compute_season_standings_data(
    game_df = game_data,
    tol = 1e-3,
    max_iter = 200,
    print_message = TRUE
  )
} else {
  cat("[season_standings] No new seasons, using prior archive.\n")
  full_data <- prior_data
}

save_and_upload(
  tag         = tag,
  full_data   = full_data,
  seasons     = seasons_to_process,
  repo        = github_data_repo,
  archive_dir = archive_dir
)



# ---------------------------------------------------------------------------- #
### weekly_standings ----
cat("%%%% Generating weekly_standings %%%%\n")
tag <- "weekly_standings"
archive_dir <- file.path("artifacts/data-archive", tag)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  # FULL REBUILD
  cat("[weekly_standings] Recomputing ALL weekly standings...\n")
  full_data <- compute_weekly_standings_data(
    game_df = game_data,
    season_type = "ALL",
    tol = 1e-3,
    max_iter = 200,
    reset = TRUE,
    print_message = TRUE
  )
} else if (should_rebuild(game_data, prior_data, id_cols = "season")) {
  # INCREMENTAL: Only update current season
  cat("[weekly_standings] Incrementally updating current season...\n")
  # All previous seasons from archive
  prev_data <- prior_data |> filter(season < current_season)
  # Current season from latest games
  curr_season_data <- game_data |> filter(season == current_season)
  # Optionally: skip if no new weeks/games
  if (nrow(curr_season_data) == 0) {
    cat("[weekly_standings] No current season games. Using prior archive.\n")
    full_data <- prior_data
  } else {
    current_standings <- compute_weekly_standings_data(
      game_df = curr_season_data,
      season_type = "ALL",
      tol = 1e-3,
      max_iter = 200,
      reset = TRUE,
      print_message = TRUE
    )
    full_data <- bind_rows(prev_data, current_standings)
  }
} else {
  # NO UPDATE NEEDED
  cat("[weekly_standings] No new data. Using prior archive.\n")
  full_data <- prior_data
}

save_and_upload(
  tag         = tag,
  full_data   = full_data,
  seasons     = seasons_to_process,
  repo        = github_data_repo,
  archive_dir = archive_dir
)

# ---------------------------------------------------------------------------- #
### elo ----
cat("%%%% Generating elo %%%%\n")
tag <- "elo"
archive_dir <- file.path("artifacts/data-archive", tag)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  cat("[elo] Full rebuild of ELO data...\n")
  full_data <- compute_elo_data(
    game_df = game_data |> dplyr::filter(!is.na(result)),
    initial_elo = 1500,
    K = 20,
    home_advantage = 0,
    d = 400,
    apply_margin_multiplier = TRUE,
    season_factor = 0.6,
    prior_data = NULL,
    verbose = TRUE
  )
} else if (should_rebuild(game_data, prior_data, id_cols = c("season", "week"))) {
  cat("[elo] Incrementally updating ELO data...\n")
  full_data <- compute_elo_data(
    game_df = game_data |> dplyr::filter(!is.na(result)),
    initial_elo = 1500,
    K = 20,
    home_advantage = 0,
    d = 400,
    apply_margin_multiplier = TRUE,
    season_factor = 0.6,
    prior_data = prior_data,
    verbose = TRUE
  )
} else {
  cat("[elo] No update needed. Using prior archive.\n")
  full_data <- prior_data
}

save_and_upload(
  tag         = tag,
  full_data   = full_data,
  seasons     = seasons_to_process,
  repo        = github_data_repo,
  archive_dir = archive_dir
)




# ---------------------------------------------------------------------------- #
### srs ----
cat("%%%% Generating srs %%%%\n")
tag <- "srs"
archive_dir <- file.path("artifacts/data-archive", tag)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

reset_windows <- c(TRUE, as.list(5:20))
max_window <- max(unlist(Filter(is.numeric, reset_windows)), na.rm = TRUE)

if (full_build || is.null(prior_data)) {
  cat("[srs] Full rebuild of SRS data...\n")
  full_data <- compute_srs_data(
    game_df = game_data |> dplyr::filter(season %in% seasons_to_process),
    resets = reset_windows,
    tol = 1e-3,
    max_iter = 200
  )
} else {
  current_weeks <- game_data |>
    dplyr::filter(season %in% seasons_to_process, !is.na(result)) |>
    dplyr::distinct(season, week)
  prior_weeks <- prior_data |> dplyr::distinct(season, week)
  new_weeks <- dplyr::anti_join(current_weeks, prior_weeks, by = c("season", "week"))

  if (nrow(new_weeks) == 0) {
    cat("[srs] No new weeks in game_data, using prior archive.\n")
    full_data <- prior_data
  } else {
    cat("[srs] Incrementally updating SRS for new weeks...\n")
    weekGrid <- game_data |>
      dplyr::filter(season %in% seasons_to_process, !is.na(result)) |>
      dplyr::distinct(season, week) |>
      dplyr::arrange(season, week) |>
      dplyr::mutate(idx = dplyr::row_number())
    new_idx   <- weekGrid |>
      dplyr::semi_join(new_weeks, by = c("season", "week")) |>
      dplyr::pull(idx)
    start_idx <- max(1, min(new_idx) - max_window)
    context_weeks <- weekGrid |>
      dplyr::slice(start_idx:max(new_idx)) |>
      dplyr::select(season, week)
    df_subset <- game_data |>
      dplyr::semi_join(context_weeks, by = c("season", "week")) |>
      dplyr::filter(!is.na(result))
    full_new <- compute_srs_data(
      game_df = df_subset,
      resets = reset_windows,
      tol = 1e-3,
      max_iter = 200
    )
    new_ratings <- full_new |>
      dplyr::semi_join(new_weeks, by = c("season", "week"))
    full_data <- dplyr::bind_rows(prior_data, new_ratings)
  }
}

save_and_upload(
  tag         = tag,
  full_data   = full_data,
  seasons     = seasons_to_process,
  repo        = github_data_repo,
  archive_dir = archive_dir
)




# ---------------------------------------------------------------------------- #
### epa ----
cat("%%%% Generating epa %%%%\n")
tag <- "epa"
archive_dir <- file.path("artifacts/data-archive", tag)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  cat("[epa] Full rebuild of EPA data...\n")
  full_data <- compute_epa_data(
    pbp_df = pbp_data |> dplyr::filter(season %in% seasons_to_process),
    scaled_wp = FALSE
  )
} else if (should_rebuild(
  pbp_data |> dplyr::filter(season %in% seasons_to_process),
  prior_data,
  id_cols = "season"
)) {
  cat("[epa] Incrementally updating EPA data for seasons: ",
      paste(seasons_to_process, collapse = ", "), "...\n")
  # Drop prior data for seasons being updated
  prior_data_no_update <- prior_data |> dplyr::filter(!(season %in% seasons_to_process))
  # Recompute only for those seasons
  new_epa_data <- compute_epa_data(
    pbp_df = pbp_data |> dplyr::filter(season %in% seasons_to_process),
    scaled_wp = FALSE
  )
  full_data <- dplyr::bind_rows(prior_data_no_update, new_epa_data)
} else {
  cat("[epa] No new seasons in pbp_data, using prior archive.\n")
  full_data <- prior_data
}

save_and_upload(
  tag         = tag,
  full_data   = full_data,
  seasons     = seasons_to_process,
  repo        = github_data_repo,
  archive_dir = archive_dir
)





# ---------------------------------------------------------------------------- #
### scores ----
cat("%%%% Generating scores %%%%\n")
tag <- "scores"
archive_dir <- file.path("artifacts/data-archive", tag)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  cat("[scores] Full rebuild of scores data...\n")
  full_data <- compute_scores_data(
    game_long_df = game_data_long |> dplyr::filter(!is.na(result)),
    pbp_df       = pbp_data,
    season_proc = all_seasons,
    sum_level = "week",
    stat_level = "team",
    season_level = "ALL"
  )
  cat("[scores] COMPLETE Full rebuild of scores data...\n")
} else if (should_rebuild(game_data_long |> dplyr::filter(!is.na(result)),
                          prior_data,
                          id_cols = c("season", "week", "game_id"))) {
  cat("[scores] Incrementally updating scores data...\n")
  full_data <- compute_scores_data(
    game_long_df = game_data_long |> dplyr::filter(!is.na(result)),
    pbp_df       = pbp_data,
    season_proc = seasons_to_process,
    sum_level = "week",
    stat_level = "team",
    season_level = "ALL"
  )
} else {
  cat("[scores] No update needed. Using prior archive.\n")
  full_data <- prior_data
}

save_and_upload(
  tag         = tag,
  full_data   = full_data,
  seasons     = seasons_to_process,
  repo        = github_data_repo,
  archive_dir = archive_dir
)


# ---------------------------------------------------------------------------- #
### series ----
cat("%%%% Generating series %%%%\n")
tag <- "series"
archive_dir <- file.path("artifacts/data-archive", tag)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || is.null(prior_data)) {
  cat("[series] Full rebuild of series data...\n")
  full_data <- compute_series_data(
    pbp_df = pbp_data |> dplyr::filter(season %in% seasons_to_process),
    weekly = TRUE
  )
} else if (should_rebuild(pbp_data |> dplyr::filter(season %in% seasons_to_process),
                          prior_data,
                          id_cols = c("season", "week", "game_id"))) {
  cat("[series] Incrementally updating series data for seasons: ",
      paste(seasons_to_process, collapse = ", "), "...\n")
  # Drop prior data for seasons being updated
  prior_data_no_update <- prior_data |> dplyr::filter(!(season %in% seasons_to_process))
  # Recompute only for those seasons
  new_series_data <- compute_series_data(
    pbp_df = pbp_data |> dplyr::filter(season %in% seasons_to_process),
    weekly = TRUE
  )
  full_data <- dplyr::bind_rows(prior_data_no_update, new_series_data)
} else {
  cat("[series] No new games in pbp_data, using prior archive.\n")
  full_data <- prior_data
}

save_and_upload(
  tag         = tag,
  full_data   = full_data,
  seasons     = seasons_to_process,
  repo        = github_data_repo,
  archive_dir = archive_dir
)

# ---------------------------------------------------------------------------- #
### turnover ----
cat("%%%% Generating turnover %%%%\n")
tag <- "turnover"
archive_dir <- file.path("artifacts/data-archive", tag)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || should_rebuild(game_data_long |> dplyr::filter(!is.na(result)),
                                 prior_data,
                                 id_cols = c("season", "week", "game_id"))) {
  cat("[turnover] Recomputing turnover data...\n")
  full_data <- compute_turnover_data(
    game_long_df = game_data_long |> dplyr::filter(!is.na(result)),
    pbp_df = pbp_data
  )
} else {
  cat("[turnover] No new seasons in game_data_long, using prior archive.\n")
  full_data <- prior_data
}

save_and_upload(
  tag         = tag,
  full_data   = full_data,
  seasons     = seasons_to_process,
  repo        = github_data_repo,
  archive_dir = archive_dir
)

# ---------------------------------------------------------------------------- #
### redzone ----
cat("%%%% Generating redzone %%%%\n")
tag <- "redzone"
archive_dir <- file.path("artifacts/data-archive", tag)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (full_build || should_rebuild(game_data_long |> dplyr::filter(!is.na(result)),
                                 prior_data,
                                 c("season", "week", "game_id"))) {
  cat("[redzone] Recomputing redzone data...\n")
  full_data <- compute_redzone_data(
    game_long_df = game_data_long |> dplyr::filter(!is.na(result)),
    pbp_df = pbp_data
  )
} else {
  cat("[redzone] No new seasons in game_data_long, using prior archive.\n")
  full_data <- prior_data
}

save_and_upload(
  tag         = tag,
  full_data   = full_data,
  seasons     = seasons_to_process,
  repo        = github_data_repo,
  archive_dir = archive_dir
)


# ============================================================================ #
## Aggregated Data ====
# (built completely from prior data)


# ---------------------------------------------------------------------------- #
### feature_data_long ----
tag <- "feature_long"
archive_dir <- file.path("artifacts/data-archive", tag)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

full_data <- compute_feature_long(
  features = c("elo", "srs", "epa", "scores", "series", "redzone", "turnover")
)

save_and_upload(
  tag         = tag,
  full_data   = full_data,
  seasons     = seasons_to_process,
  repo        = github_data_repo,
  archive_dir = archive_dir
)

# ---------------------------------------------------------------------------- #
### feature_data ----


# ---------------------------------------------------------------------------- #
### model_data_long ----


# ---------------------------------------------------------------------------- #
### model_data ----





cat("\n%%%% DATA UPDATE COMPLETE %%%%\n")


