#' Compute Long-Format Team-Game Data from Game Schedule
#'
#' Takes a game-level data frame and generates a long-format team-game data set
#' with per-team rolling records and game stats.
#'
#' @param game_df Data frame or tibble of game schedule data. Must contain columns:
#'   `season`, `game_id`, `team`, `opponent`, `result`, `spread_line`,
#'   `team_score`, `opponent_score`, `winner`, and `location`.
#'
#' @return A tibble in long format with one row per team-game.
#' @export
#' @noRd
load_game_data_long <- function(game_df) {
  game_df |>
    nflreadr::clean_homeaway(invert = c("result", "spread_line")) |>
    dplyr::group_by(season, team) |>
    dplyr::mutate(
      team_GP = dplyr::row_number(),
      winner = ifelse(
        team == winner,
        TRUE,
        ifelse(opponent == winner, FALSE, NA)
      ),
      team_W = cumsum(dplyr::coalesce(result > 0, FALSE)),
      team_L = cumsum(dplyr::coalesce(result < 0, FALSE)),
      team_T = team_GP - team_W - team_L,
      team_PF = cumsum(dplyr::coalesce(team_score, 0)),
      team_PFG = team_PF / team_GP,
      team_PA = cumsum(dplyr::coalesce(opponent_score, 0)),
      team_PAG = team_PA / team_GP
    ) |>
    dplyr::mutate(
      team_W = dplyr::lag(team_W, default = 0),
      team_L = dplyr::lag(team_L, default = 0),
      team_T = dplyr::lag(team_T, default = 0)
    ) |>
    dplyr::ungroup() |>
    dplyr::group_by(game_id) |>
    dplyr::mutate(
      locationID = dplyr::row_number(),
      .after = location
    ) |>
    dplyr::ungroup()
}
