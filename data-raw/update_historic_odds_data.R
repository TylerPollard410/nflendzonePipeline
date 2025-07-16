# ============================================================================ #
# update_historic_odds_data.R
# Incremental odds data extraction for NFL game and player markets
# Part of the nflendzonePipeline (used after update_all_data.R)
# ============================================================================ #


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# 1. LIBRARIES ----
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #

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

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# 2. PARAMETERS & SETUP ----
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #

## NFL Variables ----
# Define which seasons to process
start_season <- 2007
current_season <- nflreadr::get_current_season()
current_week <- nflreadr::get_current_week()
all_seasons <- seq(start_season, current_season)

## Odds Variables ----
# sport
sport_var <- "americanfootball_nfl"

# historic odds min date
historic_odds_start <- "2020-06-06T00:00:00Z"

# historic event odds min date
historic_event_start <- "2023-05-03T05:30:00Z"

# US Bookmakers
us_bookmakers <- c(
  #"betonlineag",
  "betmgm",
  #"betrivers",
  #"betus",
  #"bovada",
  #"williamhill_us",
  "draftkings",
  "fanatics",
  "fanduel"
  #"lowvig",
  #"mybookieag"
)

us2_bookmakers <- c(
  #"ballybet",
  #"betanysports",
  #"betparx",
  "espnbet",
  #"fliff",
  "hardrockbet"
  #"rebet",
  #"windcreek"
)

us_dfs <- c(
  "pick6",
  "prizepicks",
  "underdog"
)

us_exchanges <- c(
  "betopenly"
  #"novig",
  #"prophetx"
)


## Release Variables ----
# Use incremental update logic
full_build <- FALSE
seasons_to_process <- 2020:current_season

# Data repo for piggyback releases
github_data_repo <- "TylerPollard410/nflendzoneData"
github_releases_base_url <- paste0("https://github.com/",
                                   github_data_repo,
                                   "/releases/download/")

needed_tags <- c(
  # historic odds
  "historic_events"
  #"historic_odds",
  #"historic_event_odds",
  # live odds
  # "live_events",
  # "live_odds",
  # "live_event_odds"
)

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# 3. RELEASE TAGS EXIST IN GITHUB DATA REPO ----
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #

suppressWarnings({
  purrr::walk(needed_tags,
              ~ piggyback::pb_new_release(repo = github_data_repo,
                                          tag = .x)
  )
})

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# 4. LOAD MODEL DATASETS ----
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #

## teams_data ----
teams_data <- load_teams(current = TRUE)

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


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# 5. HISTORIC ODDS ----
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #

historic_games <- completed_games |>
  filter(odds_gametime >= historic_odds_start)

historic_games_unique <- completed_season_weeks |>
  filter(odds_week_start >= historic_odds_start)

get_odds_api_usage()

# ============================================================================ #
## Events ----
cat("%%%% Generating historic_events %%%%\n")
tag <- "historic_events"
archive_dir <- file.path("artifacts/data-archive", tag)
full_rds_path <- file.path(archive_dir, paste0(tag, ".rds"))
prior_data <- if (file.exists(full_rds_path)) readRDS(full_rds_path) else NULL

historic_events <- historic_games_unique |>
  rowwise() |>
  mutate(
    event_data = list({
      events <- get_odds_api_historical_events(
        sport = sport_var,
        date = format_ISO8601(as_datetime(odds_week_start), usetz = "Z", precision = "ymdhms"),
        dateFormat = "iso",
        oddsFormat = "decimal",
        eventIds = NULL,
        commenceTimeFrom = format_ISO8601(as_datetime(odds_week_start) - hours(1), usetz = "Z", precision = "ymdhms"),
        commenceTimeTo = format_ISO8601(as_datetime(odds_week_end) + hours(1), usetz = "Z", precision = "ymdhms")
      )
      if (nrow(events) > 0) events else tibble(
        timestamp = NA_character_,
        previous_timestamp = NA_character_,
        next_timestamp = NA_character_,
        id = NA_character_,
        sport_key = NA_character_,
        sport_title = NA_character_,
        commence_time = NA_character_,
        home_team = NA_character_,
        away_team = NA_character_
      )
    })
  ) |>
  ungroup() |>
  unnest(event_data)

get_odds_api_usage()

historic_events <-  historic_events |>
  mutate(
    home_team = ifelse(home_team == "Washington Football Team", "Washington Commanders", home_team),
    away_team = ifelse(away_team == "Washington Football Team", "Washington Commanders", away_team)
  ) |>
  left_join(
    teams_data |> select(home_team_abbr = team_abbr, team_name),
    by = c("home_team" = "team_name"),
    relationship = "many-to-many"
  ) |>
  relocate(home_team_abbr, .after = home_team) |>
  left_join(
    teams_data |> select(away_team_abbr = team_abbr, team_name),
    by = c("away_team" = "team_name"),
    relationship = "many-to-many"
  ) |>
  relocate(away_team_abbr, .after = away_team) |>
  #select(-home_team, -away_team) |>
  rename(home_team_name = home_team,
         away_team_name = away_team,
         home_team = home_team_abbr,
         away_team = away_team_abbr)

historic_events <- historic_games |>
  left_join(
    historic_events,
    by = join_by(season, week, week_seq, home_team, away_team, odds_week_start, odds_week_end)
  )
#glimpse(historic_events_games)
get_odds_api_usage()

full_data <- historic_events

save_and_upload(
  tag         = tag,
  full_data   = full_data,
  #seasons     = all_seasons,
  seasons     = seasons_to_process,
  repo        = github_data_repo,
  archive_dir = archive_dir,
  upload = TRUE,
  archive_formats = c("rds", "parquet")
)

# ---------------------------------------------------------------------------- #
## Odds ----
### Spreads, Totals, H2H ----
market_string <- paste("spreads", "totals", "h2h", sep = ",")
bookmaker_string <- paste("hardrockbet", "draftkings", sep = ",")

# get_odds_api_usage()
#
# historic_odds <- historic_events_games |>
#   filter(!is.na(id), !is.na(commence_time)) |>
#   rowwise() |>
#   mutate(
#     odds_data = list(
#       get_odds_api_historical_odds(
#         sport = sport_var,
#         date = commence_time,
#         regions = NULL,
#         markets = market_string,
#         dateFormat = "iso",
#         oddsFormat = "decimal",
#         eventIds = id,
#         bookmakers = bookmaker_string,
#         includeLinks = NULL,
#         includeSids = NULL,
#         includeBetLimits = NULL
#       )
#     )
#   ) |>
#   ungroup() |>
#   unnest(odds_data)

get_odds_api_usage()

historic_odds <- get_odds_api_historical_odds(
  sport = sport_var,
  date = historic_games$odds_gametime[1],
  regions = NULL,
  markets = paste("h2h", "spreads", "totals", sep = ","),
  dateFormat = "iso",
  oddsFormat = "decimal",
  eventIds = NULL,
  bookmakers = paste("hardrockbet", "draftkings", sep = ","),
  includeLinks = NULL,
  includeSids = NULL,
  includeBetLimits = NULL
)

get_odds_api_usage()

historical_game_odds <- historic_odds |>
  left_join(
    teams_data |> select(home_team_abbr = team_abbr, team_name),
    by = c("home_team" = "team_name"),
    relationship = "many-to-many"
  ) |>
  relocate(home_team_abbr, .after = home_team) |>
  left_join(
    teams_data |> select(away_team_abbr = team_abbr, team_name),
    by = c("away_team" = "team_name"),
    relationship = "many-to-many"
  ) |>
  relocate(away_team_abbr, .after = away_team) |>
  #select(-home_team, -away_team) |>
  rename(home_team_name = home_team,
         away_team_name = away_team,
         home_team = home_team_abbr,
         away_team = away_team_abbr)

# historical_game_odds2 <- historic_games |>
#   right_join(
#     historical_game_odds,
#     by = join_by(home_team, away_team, between(y$commence_time, x$odds_week_start, x$odds_week_end))
#   ) |>
#   mutate(
#     market_name = case_when(
#       home_team_name == outcomes_name ~ paste0("home_", market_key),
#       away_team_name == outcomes_name ~ paste0("away_", market_key),
#       outcomes_name == "Over" ~ "overs",
#       outcomes_name == "Under" ~ "unders",
#       TRUE ~ NA_character_
#     )
#   ) |>
#   select(-outcomes_name) |>
#   mutate(outcomes_point = ifelse(market_name == "home_spreads", -outcomes_point, outcomes_point)) |>
#   pivot_wider(
#     names_from = c("market_name"),
#     names_glue = c("{market_name}_odds"),
#     values_from = c("outcomes_price")
#   ) |>
#   pivot_wider(
#     names_from = "market_key",
#     names_glue = c("{market_key}_line"),
#     values_from = c("outcomes_point")
#   )


# 2. Attach game_id/game meta by fuzzy week matching
historical_game_odds <- historic_games |>
  right_join(
    historical_game_odds,
    by = join_by(home_team, away_team, between(y$commence_time, x$odds_week_start, x$odds_week_end))
  ) |>
  mutate(
    market_side = case_when(
      market_key == "spreads" & outcomes_name == home_team_name ~ "home_spreads_odds",
      market_key == "spreads" & outcomes_name == away_team_name ~ "away_spreads_odds",
      market_key == "totals"  & outcomes_name == "Over"        ~ "overs_odds",
      market_key == "totals"  & outcomes_name == "Under"       ~ "unders_odds",
      market_key == "h2h"     & outcomes_name == home_team_name ~ "home_h2h_odds",
      market_key == "h2h"     & outcomes_name == away_team_name ~ "away_h2h_odds",
      TRUE ~ NA_character_
    ),
    # Spread line: always from home team perspective (flip if away)
    spreads_line = case_when(
      market_key == "spreads" & outcomes_name == home_team_name ~ -outcomes_point,
      market_key == "spreads" & outcomes_name == away_team_name ~  outcomes_point,
      TRUE ~ NA_real_
    ),
    totals_line = if_else(market_key == "totals", outcomes_point, NA_real_)
  ) |>
  mutate(
    spreads_line      = first(na.omit(spreads_line)),
    totals_line       = first(na.omit(totals_line)),
    home_spreads_odds = first(outcomes_price[market_side == "home_spreads_odds" & !is.na(market_side)]),
    away_spreads_odds = first(outcomes_price[market_side == "away_spreads_odds" & !is.na(market_side)]),
    overs_odds        = first(outcomes_price[market_side == "overs_odds" & !is.na(market_side)]),
    unders_odds       = first(outcomes_price[market_side == "unders_odds" & !is.na(market_side)]),
    home_h2h_odds     = first(outcomes_price[market_side == "home_h2h_odds" & !is.na(market_side)]),
    away_h2h_odds     = first(outcomes_price[market_side == "away_h2h_odds" & !is.na(market_side)]),
    .by = c("game_id", "bookmaker_key"),
    .keep = "all"
  )

# 4. Collapse to one row per game/bookmaker
historical_game_odds4 <- historical_game_odds3 |>
  # group_by(
  #   game_id, season, week, home_team, away_team, gameday, gametime, bookmaker_key, bookmaker
  # ) |>
  mutate(
    spreads_line      = first(na.omit(spreads_line)),
    totals_line       = first(na.omit(totals_line)),
    home_spreads_odds = first(outcomes_price[market_side == "home_spreads_odds" & !is.na(market_side)]),
    away_spreads_odds = first(outcomes_price[market_side == "away_spreads_odds" & !is.na(market_side)]),
    overs_odds        = first(outcomes_price[market_side == "overs_odds" & !is.na(market_side)]),
    unders_odds       = first(outcomes_price[market_side == "unders_odds" & !is.na(market_side)]),
    home_h2h_odds     = first(outcomes_price[market_side == "home_h2h_odds" & !is.na(market_side)]),
    away_h2h_odds     = first(outcomes_price[market_side == "away_h2h_odds" & !is.na(market_side)]),
    .by = c("game_id", "bookmaker_key"),
    .keep = "all"
  )
# ungroup() |>
# select(
#   game_id, season, week, home_team, away_team, gameday, gametime,
#   bookmaker_key, bookmaker,
#   spreads_line, totals_line,
#   home_spreads_odds, away_spreads_odds,
#   overs_odds, unders_odds,
#   home_h2h_odds, away_h2h_odds
# ) |>
# arrange(season, week, game_id, bookmaker_key)

## Event Odds ----
historic_events <- completed_games |>
  filter(odds_gametime >= historic_event_start)


# ============================================================================ #
# 4. LIVE ODDS ----
# ============================================================================ #

## Odds ----
historic_games <- completed_games |>
  filter(odds_gametime >= historic_odds_start)

## Events ----

## Event Odds ----
historic_events <- completed_games |>
  filter(odds_gametime >= historic_event_start)

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
