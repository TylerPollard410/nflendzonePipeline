# Helper script to compute and process game schedule data for the app

# Dependencies: nflverse, tidyverse (loaded externally in UpdateData.R)

#' Load and process game schedule data with market probabilities and related features
#'
#' @param seasons Integer vector of seasons to include (default: 2006 through most_recent_season()).
#' @return A tibble of game schedules for the specified seasons, with: clean team abbreviations, betting probabilities,
#'         cover flags for spread and total, game winner, and time-of-day classification.
#' @export
#' @noRd
load_game_data <- function(seasons = 1999:nflreadr::most_recent_season()) {
  seasons <- unique(as.integer(seasons))
  games <- nflreadr::load_schedules(seasons = seasons)
  drop_cols <- c(
    "old_game_id",
    "gsis",
    "nfl_detail_id",
    "pfr",
    "pff",
    "espn",
    "ftn",
    "away_qb_id",
    "home_qb_id",
    "stadium_id"
  )

  games |>
    dplyr::filter(season %in% seasons) |>
    dplyr::mutate(
      home_team = clean_team_abbrs(home_team),
      away_team = clean_team_abbrs(away_team),
      # season type: REG vs POST
      season_type = ifelse(game_type == "REG", "REG", "POST"),
      # betting probabilities from American odds
      home_spread_prob = american_odds_to_prob(home_spread_odds),
      away_spread_prob = american_odds_to_prob(away_spread_odds),
      under_prob = american_odds_to_prob(under_odds),
      over_prob = american_odds_to_prob(over_odds),
      home_moneyline_prob = american_odds_to_prob(home_moneyline),
      away_moneyline_prob = american_odds_to_prob(away_moneyline),
      # cover flags and winner
      spreadCover = dplyr::case_when(
        result > spread_line ~ TRUE,
        result < spread_line ~ FALSE,
        TRUE ~ NA
      ),
      totalCover = dplyr::case_when(
        total > total_line ~ TRUE,
        total < total_line ~ FALSE,
        TRUE ~ NA
      ),
      winner = dplyr::case_when(
        result > 0 ~ home_team,
        result < 0 ~ away_team,
        TRUE ~ NA_character_
      ),
      # time of day based on gametime ("HH:MM:SS" or similar)
      gamehour = as.numeric(stringr::str_extract(gametime, "[:digit:]+(?=:)")),
      time_of_day = dplyr::case_when(
        gamehour < 15 ~ "Day",
        dplyr::between(gamehour, 15, 18) ~ "Evening",
        gamehour > 18 ~ "Night",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(-gamehour) |>
    dplyr::relocate(season_type, .after = game_type) |>
    dplyr::relocate(home_spread_prob, .after = home_spread_odds) |>
    dplyr::relocate(away_spread_prob, .after = away_spread_odds) |>
    dplyr::relocate(under_prob, .after = under_odds) |>
    dplyr::relocate(over_prob, .after = over_odds) |>
    dplyr::relocate(home_moneyline_prob, .after = home_moneyline) |>
    dplyr::relocate(away_moneyline_prob, .after = away_moneyline) |>
    dplyr::relocate(spreadCover, .after = spread_line) |>
    dplyr::relocate(totalCover, .after = total_line) |>
    dplyr::relocate(winner, .after = result) |>
    dplyr::relocate(time_of_day, .after = gametime) |>
    add_week_seq() |>
    dplyr::select(-dplyr::any_of(drop_cols))
}
