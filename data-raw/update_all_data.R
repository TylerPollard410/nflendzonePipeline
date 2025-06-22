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
seasons_to_process <- if (full_build) all_seasons else current_season

needed_tags <- c(
  "season_standings", "weekly_standings", "elo", "srs", "epa", "scores",
  "series", "turnover", "redzone", "model_data_long", "model_data"
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
# (always rebuild, no incremental logic needed)

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
    game_long_df = game_data_long,
    pbp_df       = pbp_data,
    season_proc = seasons_to_process,
    sum_level = "week",
    stat_level = "team",
    season_level = "ALL"
  )
} else if (should_rebuild(game_data_long,
                          prior_data,
                          id_cols = c("season", "week", "game_id"))) {
  cat("[scores] Incrementally updating scores data...\n")
  full_data <- compute_scores_data(
    game_long_df = game_data_long,
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

if (full_build || should_rebuild(game_data_long, prior_data, id_cols = c("season", "week", "game_id"))) {
  cat("[turnover] Recomputing turnover data...\n")
  full_data <- compute_turnover_data(
    game_long_df = game_data_long,
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

if (full_build || should_rebuild(game_data_long, prior_data, c("season", "week", "game_id"))) {
  cat("[redzone] Recomputing redzone data...\n")
  full_data <- compute_redzone_data(
    game_long_df = game_data_long,
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



cat("\n%%%% DATA UPDATE COMPLETE %%%%\n")




# ---------------------------------------------------------------------------- #
# ### nflverse_stats ---- #
# cat("%%%% Generating/Loading nflverse week_team_all stats %%%%\n")
#
# nfl_stats_spec <- list(
#   sum_level    = "week",
#   stat_level   = "team",
#   season_level = "ALL"
# )
# stats_path <- make_stats_filename(nfl_stats_spec)
# prior_stats <- if (file.exists(stats_path)) readRDS(stats_path) else NULL
#
# if (full_build) {
#   cat("[nflverse_stats] Full rebuild: recalculating all weekly team stats...\n")
#   nflStatsWeek <- compute_nflverse_stats(
#     seasons = all_seasons,
#     spec = nfl_stats_spec,
#     prior_stats = NULL    # Ignore prior_stats for full rebuild
#   )
# } else {
#   cat("[nflverse_stats] Incremental update: checking for new seasons...\n")
#   nflStatsWeek <- compute_nflverse_stats(
#     seasons = all_seasons,
#     spec = nfl_stats_spec,
#     prior_stats = prior_stats
#   )
# }
#
# dir.create(dirname(stats_path), recursive = TRUE, showWarnings = FALSE)
# saveRDS(nflStatsWeek, stats_path)

# # ---------------------------------------------------------------------------- #
# ### scores ---- #
# cat("%%%% Generating scores %%%%\n")
# tag <- "scores"
# archive_dir <- file.path("artifacts/data-archive", tag)
# full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
# prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL
#
# # Always use the nflStatsWeek loaded earlier in the script (from nflverse_stats block)
# if (full_build || should_rebuild(game_data_long, prior_data, id_col = "season")) {
#   cat("[scores] Recomputing scores...\n")
#   full_data <- compute_scores_data(
#     game_long_df = game_data_long,
#     pbp_df       = pbp_data,
#     nflStatsWeek = nflStatsWeek
#   )
# } else {
#   cat("[scores] No new seasons in game_data_long, using prior archive.\n")
#   full_data <- prior_data
# }
#
# save_and_upload(
#   tag         = tag,
#   full_data   = full_data,
#   seasons     = seasons_to_process,
#   repo        = github_data_repo,
#   archive_dir = archive_dir
# )

# ---------------------------------------------------------------------------- #
# ### series_week ---- #
# cat("%%%% Generating series conversion rates weekly %%%%\n")
# tag <- "nflverse_series"
# archive_dir <- file.path("artifacts/data-archive", tag)
# series_weekly <- TRUE  # set FALSE if you want season summary
# full_rds_path <- if(series_weekly) {
#   file.path(archive_dir, paste0(tag, "_week.rds"))
# } else {
#   file.path(archive_dir, paste0(tag,  "_season.rds"))
# }
# prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL
#
# if (full_build || is.null(prior_data)) {
#   cat("[nflverse_series] Full rebuild of series conversion rates... \n")
#   full_data <- progressr::with_progress({
#     calculate_series_conversion_rates(
#       pbp = pbp_data,
#       weekly = series_weekly
#     )
#   }) |> arrange(season, week, team)
# } else {
#   cat("[nflverse_series] Partial rebuild of series conversion rates... \n")
#   partial_data <- progressr::with_progress({
#     calculate_series_conversion_rates(
#       pbp = pbp_data |> filter(season %in% seasons_to_process),
#       weekly = series_weekly
#     )
#   })
#   full_data <- bind_rows(
#     prior_data |> filter(!(season %in% seasons_to_process)),
#     partial_data
#   ) |> arrange(season, week, team)
# }
#
# dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
# saveRDS(full_data, full_rds_path)
#
# ### nflverse_series ---- #
# cat("%%%% Generating nflverse series conversion rates %%%%\n")
# tag <- "nflverse_series"
# archive_dir <- file.path("artifacts/data-archive", tag)
# series_weekly <- TRUE  # set FALSE if you want season summary
# full_rds_path <- if(series_weekly) {
#   file.path(archive_dir, paste0(tag, "_week.rds"))
# } else {
#   file.path(archive_dir, paste0(tag,  "_season.rds"))
# }
# prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL
#
# if (full_build || is.null(prior_data)) {
#   cat("[nflverse_series] Full rebuild of series conversion rates... \n")
#   full_data <- progressr::with_progress({
#     calculate_series_conversion_rates(
#       pbp = pbp_data,
#       weekly = series_weekly
#     )
#   }) |> arrange(season, week, team)
# } else {
#   cat("[nflverse_series] Partial rebuild of series conversion rates... \n")
#   partial_data <- progressr::with_progress({
#     calculate_series_conversion_rates(
#       pbp = pbp_data |> filter(season %in% seasons_to_process),
#       weekly = series_weekly
#     )
#   })
#   full_data <- bind_rows(
#     prior_data |> filter(!(season %in% seasons_to_process)),
#     partial_data
#   ) |> arrange(season, week, team)
# }
#
# dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
# saveRDS(full_data, full_rds_path)

# # ============================================================================ #
# # season_standings ---- #
# # ============================================================================ #
#
# cat("%%%% Generating season_standings %%%%\n")
#
# tag <- "season_standings"
# archive_dir <- file.path("artifacts/data-archive", tag)
# dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
# full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
# full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))
#
# # Load prior archive if exists
# prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL
#
# if (full_build || is.null(prior_data)) {
#   full_data <- compute_season_standings_data(
#     game_df = game_data,
#     tol = 1e-3, max_iter = 200, print_message = TRUE
#   )
# } else {
#   game_data_current <- game_data |> filter(season == current_season)
#   new_data <- compute_season_standings_data(
#     game_df = game_data_current,
#     tol = 1e-3, max_iter = 200, print_message = TRUE
#   )
#   prior_data_no_current <- prior_data |> filter(season < current_season)
#   full_data <- bind_rows(prior_data_no_current, new_data)
# }
#
# # Save full archive locally
# saveRDS(full_data, full_rds_path, compress = "xz")
# write_csv(full_data, full_csv_path)
#
# # Save/upload per-season files
# season_files <- purrr::map(seasons_to_process, \(season) {
#   season_df <- full_data |> filter(season == !!season)
#   pq_path  <- file.path(archive_dir, paste0(tag, "_", season, ".parquet"))
#   rds_path <- file.path(archive_dir, paste0(tag, "_", season, ".rds"))
#   csv_path <- file.path(archive_dir, paste0(tag, "_", season, ".csv"))
#   arrow::write_parquet(season_df, pq_path)
#   saveRDS(season_df, rds_path, compress = "xz")
#   write_csv(season_df, csv_path)
#   list(parquet = pq_path, rds = rds_path, csv = csv_path)
# })
#
# purrr::walk(season_files, \(filelist) {
#   #piggyback::pb_new_release(repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
# })
#
#
#
# # ============================================================================ #
# # weekly_standings ---- #
# # ============================================================================ #
#
# cat("%%%% Generating weekly_standings %%%%\n")
#
# tag <- "weekly_standings"
# archive_dir <- file.path("artifacts/data-archive", tag)
# dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
# full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
# full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))
#
# # Load prior archive if exists
# prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL
#
# if (full_build || is.null(prior_data)) {
#   full_data <- compute_weekly_standings_data(
#     game_df = game_data,
#     tol = 1e-3,
#     max_iter = 200,
#     reset = TRUE,
#     recompute_all = FALSE
#   )
# } else {
#   game_data_current <- game_data |> filter(season == current_season)
#   new_data <- compute_weekly_standings_data(
#     game_df = game_data_current,
#     tol = 1e-3,
#     max_iter = 200,
#     reset = TRUE,
#     recompute_all = FALSE
#   )
#   prior_data_no_current <- prior_data |> filter(season < current_season)
#   full_data <- bind_rows(prior_data_no_current, new_data)
# }
#
# # Save full archive locally
# saveRDS(full_data, full_rds_path, compress = "xz")
# write_csv(full_data, full_csv_path)
#
# # Write & upload each season (files are just temporary outputs)
# season_files <- purrr::map(seasons_to_process, \(season) {
#   season_df <- full_data |> filter(season == !!season)
#   pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
#   rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
#   csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
#   arrow::write_parquet(season_df, pq_path)
#   saveRDS(season_df, rds_path, compress = "xz")
#   write_csv(season_df, csv_path)
#   list(parquet = pq_path, rds = rds_path, csv = csv_path)
# })
#
# purrr::walk(season_files, \(filelist) {
#   piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
# })
#
#
#
# # ============================================================================ #
# # elo ---- #
# # ============================================================================ #
#
# cat("%%%% Generating elo %%%%\n")
#
# tag <- "elo"
# archive_dir <- file.path("artifacts/data-archive", tag)
# dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
# full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
# full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))
#
# # Load prior archive if exists
# prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL
#
# if (full_build || is.null(prior_data)) {
#   full_data <- compute_elo_data(
#     game_df = game_data,
#     initial_elo = 1500,
#     K = 20,
#     home_advantage = 0,
#     d = 400,
#     apply_margin_multiplier = TRUE,
#     recompute_all = FALSE,
#     season_factor = 0.6
#   )
# } else {
#   game_data_current <- game_data |> filter(season == current_season)
#   # If your function supports incremental builds using prior_data, pass it here
#   new_data <- compute_elo_data(
#     game_df = game_data_current,
#     initial_elo = 1500,
#     K = 20,
#     home_advantage = 0,
#     d = 400,
#     apply_margin_multiplier = TRUE,
#     recompute_all = FALSE,
#     season_factor = 0.6,
#     cache_file = full_rds_path #prior_data |> filter(season < current_season)
#   )
#   prior_data_no_current <- prior_data |> filter(season < current_season)
#   full_data <- bind_rows(prior_data_no_current, new_data)
# }
#
# # Save full archive locally (one file only)
# saveRDS(full_data, full_rds_path, compress = "xz")
# write_csv(full_data, full_csv_path)
#
# # Per-season files: only for uploading to GitHub release
# season_files <- purrr::map(seasons_to_process, \(season) {
#   season_df <- full_data |> filter(season == !!season)
#   pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
#   rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
#   csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
#   arrow::write_parquet(season_df, pq_path)
#   saveRDS(season_df, rds_path, compress = "xz")
#   write_csv(season_df, csv_path)
#   list(parquet = pq_path, rds = rds_path, csv = csv_path)
# })
#
# purrr::walk(season_files, \(filelist) {
#   piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
#   # Optionally clean up temp files:
#   #unlink(unlist(filelist), force = TRUE)
# })
#
#
#
# # ============================================================================ #
# # srs ---- #
# # ============================================================================ #
#
# cat("%%%% Generating srs %%%%\n")
#
# tag <- "srs"
# archive_dir <- file.path("artifacts/data-archive", tag)
# dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
# full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
# full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))
#
# # Load prior archive if exists
# prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL
#
# if (full_build || is.null(prior_data)) {
#   full_data <- compute_srs_data(
#     game_df = game_data,
#     resets = c(TRUE, as.list(5:20)),
#     tol = 1e-3,
#     max_iter = 200,
#     recompute_all = FALSE
#     # add other arguments if needed
#   )
# } else {
#   game_data_current <- game_data |> filter(season == current_season)
#   new_data <- compute_srs_data(
#     game_df = game_data_current,
#     resets = c(TRUE, as.list(5:20)),
#     tol = 1e-3,
#     max_iter = 200,
#     recompute_all = FALSE
#     # add other arguments if needed
#   )
#   prior_data_no_current <- prior_data |> filter(season < current_season)
#   full_data <- bind_rows(prior_data_no_current, new_data)
# }
#
# # Save full archive locally (single .rds, single .csv)
# saveRDS(full_data, full_rds_path, compress = "xz")
# write_csv(full_data, full_csv_path)
#
# # Per-season files: only for uploading to GitHub release
# season_files <- purrr::map(seasons_to_process, \(season) {
#   season_df <- full_data |> filter(season == !!season)
#   pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
#   rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
#   csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
#   arrow::write_parquet(season_df, pq_path)
#   saveRDS(season_df, rds_path, compress = "xz")
#   write_csv(season_df, csv_path)
#   list(parquet = pq_path, rds = rds_path, csv = csv_path)
# })
#
# purrr::walk(season_files, \(filelist) {
#   piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
#   # Optionally, clean up temp files:
#   # unlink(unlist(filelist), force = TRUE)
# })
#
#
#
# # ============================================================================ #
# # epa ---- #
# # ============================================================================ #
#
# cat("%%%% Generating epa %%%%\n")
#
# tag <- "epa"
# archive_dir <- file.path("artifacts/data-archive", tag)
# dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
# full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
# full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))
#
# # Load prior archive if exists
# prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL
#
# if (full_build || is.null(prior_data)) {
#   full_data <- compute_epa_data(
#     pbp_df = pbp_data,
#     scaled_wp = FALSE
#   )
# } else {
#   pbp_data_current <- pbp_data |> filter(season == current_season)
#   new_data <- compute_epa_data(
#     pbp_df = pbp_data_current,
#     scaled_wp = FALSE
#   )
#   prior_data_no_current <- prior_data |> filter(season < current_season)
#   full_data <- bind_rows(prior_data_no_current, new_data)
# }
#
# # Save full archive locally (single .rds, single .csv)
# saveRDS(full_data, full_rds_path, compress = "xz")
# write_csv(full_data, full_csv_path)
#
# # Per-season files: only for uploading to GitHub release
# season_files <- purrr::map(seasons_to_process, \(season) {
#   season_df <- full_data |> filter(season == !!season)
#   pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
#   rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
#   csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
#   arrow::write_parquet(season_df, pq_path)
#   saveRDS(season_df, rds_path, compress = "xz")
#   write_csv(season_df, csv_path)
#   list(parquet = pq_path, rds = rds_path, csv = csv_path)
# })
#
# purrr::walk(season_files, \(filelist) {
#   piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
#   # Optionally: unlink(unlist(filelist), force = TRUE)
# })
#
#
# # ============================================================================ #
# # scores ---- #
# # ============================================================================ #
#
# cat("%%%% Generating scores %%%%\n")
#
# tag <- "scores"
# archive_dir <- file.path("artifacts/data-archive", tag)
# dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
# full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
# full_csv_path <- file.path(archive_dir, paste0(tag, ".csv"))
#
# # Load prior archive if exists
# prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL
#
# if (full_build || is.null(prior_data)) {
#   full_data <- compute_scores_data(
#     game_long_df = game_data_long,
#     pbp_df = pbp_data,
#     seasons = all_seasons,
#     sum_level = "week",
#     stat_level = "team",
#     season_level = "REG+POST",
#     # stats_loc = "artifacts/data-archive/nflStatsWeek.rda", # Uncomment if needed
#     recompute_all = FALSE
#   )
# } else {
#   game_data_long_current <- game_data_long |> filter(season == current_season)
#   pbp_data_current <- pbp_data |> filter(season == current_season)
#   new_data <- compute_scores_data(
#     game_long_df = game_data_long_current,
#     pbp_df = pbp_data_current,
#     seasons = current_season,
#     sum_level = "week",
#     stat_level = "team",
#     season_level = "REG+POST",
#     # stats_loc = "artifacts/data-archive/nflStatsWeek.rda", # Uncomment if needed
#     recompute_all = FALSE
#   )
#   prior_data_no_current <- prior_data |> filter(season < current_season)
#   full_data <- bind_rows(prior_data_no_current, new_data)
# }
#
# # Save full archive locally (single .rds, single .csv)
# saveRDS(full_data, full_rds_path, compress = "xz")
# write_csv(full_data, full_csv_path)
#
# # Per-season files: only for uploading to GitHub release
# season_files <- purrr::map(seasons_to_process, \(season) {
#   season_df <- full_data |> filter(season == !!season)
#   pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
#   rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
#   csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
#   arrow::write_parquet(season_df, pq_path)
#   saveRDS(season_df, rds_path, compress = "xz")
#   write_csv(season_df, csv_path)
#   list(parquet = pq_path, rds = rds_path, csv = csv_path)
# })
#
# purrr::walk(season_files, \(filelist) {
#   piggyback::pb_upload(filelist$parquet, repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$rds,     repo = github_data_repo, tag = tag)
#   piggyback::pb_upload(filelist$csv,     repo = github_data_repo, tag = tag)
#   # Optionally: unlink(unlist(filelist), force = TRUE)
# })






