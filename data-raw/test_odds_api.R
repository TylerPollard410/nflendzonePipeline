# ============================================================================ #
# SPORTS ----
# ============================================================================ #

# Use environment variable (recommended)
sports <- get_odds_api_sports(all = TRUE)

# Check usage after test
get_odds_api_usage()

# ============================================================================ #
# CURRENT EVENTS ----
# ============================================================================ #

# ---------------------------------------------------------------------------- #
## Odds ----
# Test getting NFL odds (adjust the sport key if needed)
nfl_odds <- get_odds_api_odds(
  sport = "americanfootball_nfl",
  regions = "us2",
  markets = "spreads",
  dateFormat = "iso",
  oddsFormat = "decimal",
  eventIds = NULL,
  bookmakers = "hardrockbet",
  commenceTimeFrom = NULL,
  commenceTimeTo = NULL,
  includeLinks = NULL,
  includeSids = NULL,
  includeBetLimits = NULL)
print("NFL Moneyline and Spread Odds:")
print(nfl_odds)

# Example: Filter for games in a date range (if you know some upcoming dates)
# nfl_odds_time <- get_odds_api_odds(
#   sport = "americanfootball_nfl",
#   commenceTimeFrom = "2024-09-05T00:00:00Z",
#   commenceTimeTo = "2024-09-10T23:59:59Z"
# )
# print(nfl_odds_time)

# Check your API usage after tests
get_odds_api_usage()

# ---------------------------------------------------------------------------- #
## Scores ----
nfl_scores <- get_odds_api_scores(
  sport = "americanfootball_nfl",
  api_key = NULL,
  daysFrom = NULL,
  dateFormat = "iso",
  eventIds = NULL)

nfl_scores1 <- get_odds_api_scores(
  sport = "americanfootball_nfl",
  api_key = NULL,
  daysFrom = 1,
  dateFormat = "iso",
  eventIds = NULL)

nfl_scores2 <- get_odds_api_scores(
  sport = "americanfootball_nfl",
  api_key = NULL,
  daysFrom = 2,
  dateFormat = "iso",
  eventIds = NULL)

nfl_scores3 <- get_odds_api_scores(
  sport = "americanfootball_nfl",
  api_key = NULL,
  daysFrom = 3,
  dateFormat = "iso",
  eventIds = NULL)

# Check your API usage after tests
get_odds_api_usage()


# ---------------------------------------------------------------------------- #
## Events ----
nfl_events <- get_odds_api_events(
  sport = "americanfootball_nfl",
  api_key = NULL,
  dateFormat = "iso",
  eventIds = NULL,
  commenceTimeFrom = NULL,
  commenceTimeTo = NULL
)

# Check your API usage after tests
get_odds_api_usage()

nfl_events_old <- get_odds_api_events(
  sport = "americanfootball_nfl",
  api_key = NULL,
  dateFormat = "iso",
  eventIds = NULL,
  commenceTimeFrom = "2024-09-06T00:20:00Z",
  commenceTimeTo = "2024-09-10T00:15:00Z"
)

# Check your API usage after tests
get_odds_api_usage()

# ---------------------------------------------------------------------------- #
## Event Odds ----
nfl_event_odds <- get_odds_api_event_odds(
  sport = "americanfootball_nfl",
  eventId = "dee0a41ed5e8201a96d457899adbe918",
  api_key = NULL,
  regions = "us2",
  markets = "player_pass_yds",
  dateFormat = "iso",
  oddsFormat = "decimal",
  bookmakers = "hardrockbet",
  includeLinks = NULL,
  includeSids = NULL,
  includeBetLimits = NULL
)

# Check your API usage after tests
get_odds_api_usage()

# ============================================================================ #
# DEFAULT ----
# ============================================================================ #

# ---------------------------------------------------------------------------- #
## Participants ----
nfl_participants <- get_odds_api_participants(
  sport = "americanfootball_nfl",
  api_key = NULL
)

# Check your API usage after tests
get_odds_api_usage()


# ============================================================================ #
# HISTORICAL EVENTS ----
# ============================================================================ #

# ---------------------------------------------------------------------------- #
## Odds ----
# NFL historical odds: snapshot at season start 2024
nfl_hist_odds_start <- get_odds_api_historical_odds(
  sport = "americanfootball_nfl",
  date = "2024-09-05T00:00:00Z",  # NFL season kickoff night
  regions = "us2",
  markets = "spreads",
  dateFormat = "iso",
  oddsFormat = "decimal",
  eventIds = NULL,
  bookmakers = "hardrockbet",
  includeLinks = NULL,
  includeSids = NULL,
  includeBetLimits = NULL
)
print("NFL Historical Odds Snapshot (2024 Week 1):")
print(nfl_hist_odds_start)

# Check your API usage after tests
get_odds_api_usage()

nfl_hist_odds_start3 <- get_odds_api_historical_odds(
  sport = "americanfootball_nfl",
  date = "2020-09-11T00:20:00Z",  # NFL season kickoff night
  regions = "us2",
  markets = "spreads",
  dateFormat = "iso",
  oddsFormat = "decimal",
  eventIds = NULL,
  #bookmakers = "hardrockbet",
  includeLinks = NULL,
  includeSids = NULL,
  includeBetLimits = NULL
)

library(dplyr)
library(purrr)
library(glue)

schedules_2024 <- nflreadr::load_schedules(2024)

first_game_per_week <- schedules_2024 |>
  #filter(season_type == "REG") |>
  group_by(week) |>
  arrange(gameday, gametime, game_id) |>
  slice(1) |>
  ungroup() |>
  mutate(
    kickoff_ts = paste0(gameday, "T", substr(gametime, 1, 5), ":00Z")
  )

# This is now preferred:
nfl_historical_odds_2024 <- map2(
  first_game_per_week$kickoff_ts,
  first_game_per_week$week,
  \(kickoff_ts, week) {
    message(glue("Fetching Week {week} odds for kickoff time {kickoff_ts}"))
    Sys.sleep(1)
    get_odds_api_historical_odds(
      sport = "americanfootball_nfl",
      date = kickoff_ts,
      regions = "us2",
      markets = "spreads",
      dateFormat = "iso",
      oddsFormat = "decimal"
    ) |> mutate(week = week)
  }
) |> bind_rows()

saveRDS(nfl_historical_odds_2024, file = "~/Desktop/nfl_historical_odds_2024.rds")
nfl_historical_odds_2024_hardrock <- nfl_historical_odds_2024 |>
  filter(bookmaker_key == "hardrockbet")

teams_2024 <- nflreadr::load_teams(current = TRUE)

nfl_historical_odds_2024_hardrock2 <- nfl_historical_odds_2024_hardrock |>
  left_join(teams_2024 |> select(team_abbr, team_name),
            by = c("outcomes_name" = "team_name")) |>
  rename(team = team_abbr)

schedules_2024 <- schedules_2024 |>
  left_join(nfl_historical_odds_2024_hardrock2 |>
              select(week, team,
                     home_spread_price = outcomes_price),
                     #outcomes_point),
            by = c("week", "home_team" = "team")) |>
  left_join(nfl_historical_odds_2024_hardrock2 |>
              select(week, team,
                     away_spread_price = outcomes_price,
                     outcomes_point),
            by = c("week", "away_team" = "team")) |>
  relocate(outcomes_point,  .after = spread_line)

# Check your API usage after tests
get_odds_api_usage()

# ---------------------------------------------------------------------------- #
## Events ----

# Batch get NFL historical events for every regular season week in 2024 ---- #
# NFL historical odds: snapshot at season start 2024
nfl_hist_events_start <- get_odds_api_historical_events(
  sport = "americanfootball_nfl",
  date = "2024-09-05T00:00:00Z",  # NFL season kickoff night
  dateFormat = "iso",
  eventIds = NULL,
  commenceTimeFrom = NULL,
  commenceTimeTo = NULL
)
print("NFL Historical Events Snapshot (2024 Week 1):")
print(nfl_hist_odds_start)

# Check your API usage after tests
get_odds_api_usage()

nfl_hist_events_start2 <- get_odds_api_historical_events(
  sport = "americanfootball_nfl",
  date = "2020-09-11T00:20:00Z",  # NFL season kickoff night
  dateFormat = "iso",
  eventIds = NULL,
  commenceTimeFrom = NULL,
  commenceTimeTo = NULL
)

library(dplyr)
library(purrr)
library(glue)

schedules_2024 <- nflreadr::load_schedules(2024)

first_game_per_week_playoffs <- schedules_2024 |>
  filter(game_type != "REG") |>
  group_by(week) |>
  arrange(gameday, gametime, game_id) |>
  slice(1) |>
  ungroup() |>
  mutate(
    kickoff_ts = paste0(gameday, "T", substr(gametime, 1, 5), ":00Z")
  )

nfl_hist_events_2024_playoffs <- map2(
  first_game_per_week_playoffs$kickoff_ts,
  first_game_per_week_playoffs$week,
  \(kickoff_ts, week) {
    message(glue("Fetching Week {week} historical event snapshot for kickoff time {kickoff_ts}"))
    Sys.sleep(1)
    get_odds_api_historical_events(
      sport = "americanfootball_nfl",
      date = kickoff_ts,
      dateFormat = "iso",
      eventIds = NULL,
      commenceTimeFrom = NULL,
      commenceTimeTo = NULL
    ) |> mutate(week = week)
  }
) |> bind_rows()


print("NFL 2024 Historical Events Snapshots:")
print(nfl_hist_events_2024)

#saveRDS(nfl_hist_events_2024, file = "~/Desktop/nfl_hist_events_2024.rds")

# Check your API usage after tests
get_odds_api_usage()


# ---------------------------------------------------------------------------- #
## Event Odds ----

# Example: Get historical odds for a single event at a snapshot

# 1. Find an event ID from the Week 1 snapshot
# (Replace with your actual event/game id from nfl_hist_events_start, for example the first game)
event_id_example <- nfl_hist_events_start$id[1]
print(glue::glue("Testing with event ID: {event_id_example}"))

# 2. Pick a snapshot time -- here we use the season kickoff again,
# but you could use any valid ISO8601 timestamp during the event's window
snapshot_time <- "2024-09-05T00:00:00Z"  # Same as above; adjust as needed

# 3. Call your function to get historical odds for that event and time
nfl_hist_event_odds_1 <- get_odds_api_historical_event_odds(
  sport = "americanfootball_nfl",
  eventId = event_id_example,
  date = snapshot_time,
  regions = "us2",
  markets = "player_pass_yds",
  dateFormat = "iso",
  oddsFormat = "decimal",
  bookmakers = "hardrockbet",
  includeLinks = NULL,
  includeSids = NULL,
  includeBetLimits = NULL
)

print("NFL Historical Odds for One Event at Week 1:")
print(nfl_hist_event_odds_1)

# Check your API usage after tests
get_odds_api_usage()

# BATCH EXAMPLE: Get historical event odds for ALL events in Week 1 ---- #
library(dplyr)
library(purrr)
library(glue)

# (Assuming nfl_hist_events_start is your Week 1 snapshot tibble)
nfl_hist_events_week1 <- nfl_hist_events_start |> slice(1)
snapshot_time <- "2024-09-05T00:00:00Z"

nfl_hist_event_odds_week1 <- map(
  nfl_hist_events_week1$id,
  \(eid) {
    Sys.sleep(1)
    get_odds_api_historical_event_odds(
      sport = "americanfootball_nfl",
      eventId = eid,
      date = snapshot_time,
      regions = "us2",
      markets = "player_pass_yds",
      dateFormat = "iso",
      oddsFormat = "decimal",
      bookmakers = "hardrockbet",
      includeLinks = NULL,
      includeSids = NULL,
      includeBetLimits = NULL
    )
  }
) |> bind_rows()

print("All NFL Week 1 Historical Event Odds:")
print(nfl_hist_event_odds_week1)


# Check your API usage after batch call
get_odds_api_usage()


