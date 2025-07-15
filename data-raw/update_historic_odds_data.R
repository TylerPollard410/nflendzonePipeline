# ============================================================================ #
# update_historic_odds_data.R
# Incremental odds data extraction for NFL game and player markets
# Part of the nflendzonePipeline (used after update_all_data.R)
# ============================================================================ #


# ============================================================================ #
# 1. LIBRARIES ----
# ============================================================================ #

golem::detach_all_attached()

#detach("package:nflendzonePipeline",unload = TRUE, force = TRUE)
#install.packages(".", repos = NULL, type = "source")

# Libraries already loaded from update_all_data.R
library(arrow)
library(tibble)
library(glue)
library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(lubridate)
library(slider)
library(piggyback)
library(tidyr)
# library(nflreadr)
# library(nflfastR)
# library(nflseedR)
# library(nflendzonePipeline)

# For odds API requests and JSON parsing (from utils_odds_api.R)
#library(httr2)      # Required by odds_api_get()
#library(jsonlite)   # Required by odds_api_get()

# Always load nfl packages last
library(nflreadr)
library(nflfastR)
library(nflseedR)
library(nflendzonePipeline)

# ============================================================================ #
# 2. PARAMETERS & SETUP ----
# ============================================================================ #

# Define which seasons to process
start_season <- 2007
current_season <- nflreadr::get_current_season()
current_week <- nflreadr::get_current_week()
all_seasons <- seq(start_season, current_season)

# Use incremental update logic
full_build <- FALSE
seasons_to_process <- current_season

# Data repo for piggyback releases
github_data_repo <- "TylerPollard410/nflendzoneData"
github_releases_base_url <- paste0("https://github.com/",
                                   github_data_repo,
                                   "/releases/download/")

# # Odds tags (used for releases)
# odds_tags <- c("game_spreads", "player_rush_props")
#
# # Ensure releases exist for new tags
# purrr::walk(odds_tags, ~ piggyback::pb_new_release(repo = github_data_repo, tag = .x))

# ============================================================================ #
# 3. LOAD MODEL DATASETS ----
# ============================================================================ #

## features ----
game_features <- rds_from_url(paste0(github_releases_base_url, "game_features/game_features", ".rds"))
team_features <- rds_from_url(paste0(github_releases_base_url, "team_features/team_features", ".rds"))

## model ----
game_model <- rds_from_url(paste0(github_releases_base_url, "game_model/game_model", ".rds"))
team_model <- rds_from_url(paste0(github_releases_base_url, "team_model/team_model", ".rds"))

## completed ----
# Filter for completed games
completed_games <- game_model |>
  filter(!is.na(result)) |>
  select(game_id, season, week, week_seq, season_type,
         gameday, weekday, gametime, time_of_day,
         home_team, away_team, location) |>
  mutate(
    odds_gametime = paste(gameday, gametime),
    odds_gametime = ymd_hm(odds_gametime, tz = "America/New_York"),
    odds_gametime = format_ISO8601(odds_gametime, usetz = "Z", precision = "ymdhms")
  ) |>
  mutate(
    odds_week_start = min(odds_gametime),
    odds_week_end = max(odds_gametime),
    .by = c("season", "week")
  )


# Get data frame for distinct season week combos
completed_season_weeks <- completed_games |>
  distinct(season, week, week_seq, odds_week_start, odds_week_end)

# ============================================================================ #
# 4. GAME-LEVEL SPREAD ODDS ----
# ============================================================================ #





cat("========== [Game-Level] Updating Historical Spread Odds ==========\n")

tag <- "game_spreads"
archive_dir <- file.path("artifacts/data-archive", tag)
dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_spread_odds <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (!is.null(prior_spread_odds) && !full_build) {
  games_to_get <- anti_join(completed_games, prior_spread_odds, by = "game_id")
  if (nrow(games_to_get) == 0) {
    cat("[game_spreads] No new completed games. Skipping odds fetch.\n")
    spread_odds_final <- prior_spread_odds
  } else {
    cat(glue("[game_spreads] Fetching odds for {nrow(games_to_get)} new games...\n"))
  }
} else {
  cat("[game_spreads] No prior archive or full build requested. Processing ALL games.\n")
  games_to_get <- completed_games
}

# Only fetch odds if new games exist
if (exists("games_to_get") && nrow(games_to_get) > 0) {

  # Helper to find the event and fetch all odds snapshots for a given game
  fetch_game_spread_history <- function(game_row) {
    # Lookup event in Odds API (by date and teams)
    events <- get_odds_api_events(
      sport = "americanfootball_nfl",
      regions = "us,us2",
      markets = "spreads",
      dateFormat = "iso",
      commenceTimeFrom = as.character(as.Date(game_row$gametime) - 1),
      commenceTimeTo = as.character(as.Date(game_row$gametime) + 1)
    )
    # Fuzzy match home/away
    event <- events |>
      filter(
        str_to_upper(home_team) == str_to_upper(game_row$home_team),
        str_to_upper(away_team) == str_to_upper(game_row$away_team),
        as.Date(commence_time) == as.Date(game_row$gametime)
      )
    if (nrow(event) == 0) return(NULL)
    event_id <- event$id[1]
    event_time <- event$commence_time[1]

    # Traverse all historical snapshots for this event
    snapshot_list <- list()
    snap <- get_odds_api_historical_event_odds(
      sport = "americanfootball_nfl",
      eventId = event_id,
      date = event_time,
      markets = "spreads"
    )
    if (nrow(snap) == 0) return(NULL)
    snapshot_list[[1]] <- snap

    while (!is.na(snap$previous_timestamp[1])) {
      snap <- get_odds_api_historical_event_odds(
        sport = "americanfootball_nfl",
        eventId = event_id,
        date = snap$previous_timestamp[1],
        markets = "spreads"
      )
      if (nrow(snap) == 0) break
      snapshot_list[[length(snapshot_list) + 1]] <- snap
    }
    odds_all <- bind_rows(snapshot_list)
    # Home team spreads only
    home_spreads <- odds_all |>
      filter(market_key == "spreads", outcomes_name == game_row$home_team) |>
      mutate(spread = outcomes_point)
    spread_vals <- home_spreads$spread
    if (length(spread_vals) == 0) return(NULL)
    tibble(
      game_id      = game_row$game_id,
      season       = game_row$season,
      week         = game_row$week,
      home_team    = game_row$home_team,
      away_team    = game_row$away_team,
      open_spread  = spread_vals[length(spread_vals)],
      close_spread = spread_vals[1],
      min_spread   = min(spread_vals, na.rm = TRUE),
      max_spread   = max(spread_vals, na.rm = TRUE)
    )
  }

  spread_odds_new <- purrr::map_dfr(seq_len(nrow(games_to_get)), function(i) {
    tryCatch(fetch_game_spread_history(games_to_get[i,]), error = function(e) NULL)
  })

  spread_odds_final <- if (!is.null(prior_spread_odds) && !full_build) {
    bind_rows(prior_spread_odds, spread_odds_new)
  } else {
    spread_odds_new
  }

  save_and_upload(
    tag = tag,
    full_data = spread_odds_final,
    repo = github_data_repo,
    archive_dir = archive_dir,
    upload = TRUE,
    archive_formats = c("rds", "parquet", "csv")
  )
}

# ============================================================================ #
# 5. PLAYER-LEVEL RUSHING YARDS PROPS ----
# ============================================================================ #

cat("========== [Player-Level] Updating Historical Rushing Yards Props ==========\n")

tag <- "player_rush_props"
archive_dir <- file.path("artifacts/data-archive", tag)
dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_rush_props <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

if (!is.null(prior_rush_props) && !full_build) {
  games_to_get <- anti_join(completed_games, prior_rush_props, by = "game_id")
  if (nrow(games_to_get) == 0) {
    cat("[player_rush_props] No new completed games. Skipping props fetch.\n")
    rush_props_final <- prior_rush_props
  } else {
    cat(glue("[player_rush_props] Fetching props for {nrow(games_to_get)} new games...\n"))
  }
} else {
  cat("[player_rush_props] No prior archive or full build requested. Processing ALL games.\n")
  games_to_get <- completed_games
}

if (exists("games_to_get") && nrow(games_to_get) > 0) {

  fetch_player_rush_props <- function(game_row) {
    events <- get_odds_api_events(
      sport = "americanfootball_nfl",
      regions = "us,us2",
      markets = "player_rushing_yards",
      dateFormat = "iso",
      commenceTimeFrom = as.character(as.Date(game_row$gametime) - 1),
      commenceTimeTo = as.character(as.Date(game_row$gametime) + 1)
    )
    event <- events |>
      filter(
        str_to_upper(home_team) == str_to_upper(game_row$home_team),
        str_to_upper(away_team) == str_to_upper(game_row$away_team),
        as.Date(commence_time) == as.Date(game_row$gametime)
      )
    if (nrow(event) == 0) return(NULL)
    event_id <- event$id[1]
    event_time <- event$commence_time[1]

    snapshot_list <- list()
    snap <- get_odds_api_historical_event_odds(
      sport = "americanfootball_nfl",
      eventId = event_id,
      date = event_time,
      markets = "player_rushing_yards"
    )
    if (nrow(snap) == 0) return(NULL)
    snapshot_list[[1]] <- snap

    while (!is.na(snap$previous_timestamp[1])) {
      snap <- get_odds_api_historical_event_odds(
        sport = "americanfootball_nfl",
        eventId = event_id,
        date = snap$previous_timestamp[1],
        markets = "player_rushing_yards"
      )
      if (nrow(snap) == 0) break
      snapshot_list[[length(snapshot_list) + 1]] <- snap
    }
    odds_all <- bind_rows(snapshot_list)
    # Each unique player (description) - summarize lines
    player_props <- odds_all |>
      filter(market_key == "player_rushing_yards") |>
      select(outcomes_description, outcomes_point, timestamp) |>
      group_by(outcomes_description) |>
      summarise(
        game_id    = game_row$game_id,
        season     = game_row$season,
        week       = game_row$week,
        player     = outcomes_description[1],
        open_line  = outcomes_point[which.max(timestamp)],
        close_line = outcomes_point[which.min(timestamp)],
        min_line   = min(outcomes_point, na.rm = TRUE),
        max_line   = max(outcomes_point, na.rm = TRUE),
        .groups = "drop"
      )
    player_props
  }

  rush_props_new <- purrr::map_dfr(seq_len(nrow(games_to_get)), function(i) {
    tryCatch(fetch_player_rush_props(games_to_get[i,]), error = function(e) NULL)
  })

  rush_props_final <- if (!is.null(prior_rush_props) && !full_build) {
    bind_rows(prior_rush_props, rush_props_new)
  } else {
    rush_props_new
  }

  save_and_upload(
    tag = tag,
    full_data = rush_props_final,
    repo = github_data_repo,
    archive_dir = archive_dir,
    upload = TRUE,
    archive_formats = c("rds", "parquet", "csv")
  )
}

cat("\n========== update_historic_odds_data.R COMPLETE ==========\n")
