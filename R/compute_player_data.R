# playerOffenseData.R
# Function to load and process player offense stats for modeling

# Dependencies: nflverse, tidyverse

#' Load player offense statistics and merge with game-level identifiers
#'
#' @param seasons Integer vector of seasons to load (default: 2006:most_recent_season())
#' @param game_long_df Data frame with game-level identifiers (default: game_data_long)
#' @return A tibble of player offense stats with game_id, season, week, and opponent fields
#' @export
#' @noRd
compute_player_data <- function(seasons = 2006:most_recent_season(),
                                game_long_df = game_data_long,
                                stat = "offense") {
  # Load raw player offense stats
  player_stats <- nflreadr::load_player_stats(
    seasons = seasons,
    stat_type = stat
  )

  # Join with game-level info to get game_id, week, and opponent
  playerOffenseData <- player_stats |>
    dplyr::left_join(
      game_long_df |> dplyr::select(game_id, season, week, opponent),
      by = dplyr::join_by(season, week, opponent_team == opponent)
    ) |>
    dplyr::relocate(game_id, .before = 1)

  return(playerOffenseData)
}

# Auto-run when sourced in UpdateData.R
# if (exists("allSeasons") && exists("gameDataLong")) {
#   playerOffenseData <- compute_player_offense_data(allSeasons, gameDataLong)
# }
