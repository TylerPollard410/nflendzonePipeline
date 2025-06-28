# turnoverData.R
# Function to compute and combine turnover statistics per game-team

#' Compute turnover feature dataset for modeling
#'
#' @param prev_team_week_stats_df  Tibble of long-format game-team rows with:
#'                                  all stats from nflfastR::calculate_stats
#' @return Tibble with one row per game-team containing turnover features:
#'   turnover_diff, turnover_won, turnover_lost,
#'   interception_won, interception_lost, fumble_won, fumble_lost
#' @export
#' @noRd
compute_turnover_data <- function(prev_stats_df) {
  # uses dplyr

  # STEP 1: Summarise raw turnover metrics by team-game
  turnoverData <- prev_stats_df |>
    select(
      game_id, season, week, team, opponent, location,
      passing_interceptions,
      sack_fumbles_lost, rushing_fumbles_lost, receiving_fumbles_lost, def_fumbles_forced,
      def_interceptions, def_fumbles,
      fumble_recovery_opp
    ) |>
    mutate(
      interceptions_won = def_interceptions,
      interceptions_lost = passing_interceptions,
      fumbles_lost = sack_fumbles_lost + rushing_fumbles_lost + receiving_fumbles_lost
    ) |>
    mutate(
      fumbles_won = rev(fumbles_lost),
      .by = game_id
    ) |>
    select(
      game_id, season, week, team, opponent, location,
      interceptions_won, fumbles_won,
      interceptions_lost, fumbles_lost
    ) |>
    mutate(
      turnovers_won = interceptions_won + fumbles_won,
      turnovers_lost = interceptions_lost + fumbles_lost,
      .after = location
    ) |>
    mutate(
      turnover_diff = turnovers_won - turnovers_lost,
      .after = location
    )

  return(turnoverData)
}

# Example usage:
# turnover_data <- compute_turnover_data(gameDataLong, pbpData)
