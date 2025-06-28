# compute_game_features_data.R

#' Compute Game-Level Feature Data for a Team Feature Data
#'
#' This function computes game-level features by joining game metadata to pre-computed team features
#' for both home and away teams, for all games played.
#'
#' @param game_data Data frame of game-level information. Must include at least game_id, season, week, home_team, away_team, location.
#' @param team_features_data Data frame of precomputed team features, including columns: game_id, season, week, team, gameday, and features.
#' @return A tibble (data.frame) of game features, including identifiers and joined home/away team features.
#'
#' @details
#' The function extracts the core game identifiers and joins home and away team features
#' using appropriate key columns. All columns from \code{team_features_data} (except location) will be joined in,
#' with "home_" and "away_" prefixes for the respective teams.
#'
#' @examples
#' \dontrun{
#' features <- compute_game_features_data(
#'   game_data = games_df,
#'   team_features_data = features_df
#' )
#' }
#'
#' @export
#' @noRd
compute_game_features_data <- function(game_data,
                                       team_features_data) {

  game_id_keys <- game_data |> dplyr::select(
    game_id, season, game_type, season_type, week, home_team, away_team, location
  )

  game_features_data <- game_id_keys |>
    dplyr::left_join(
      team_features_data |>
        dplyr::select(-location) |>
        dplyr::rename_with(~paste0("home_", .x),
                           .cols = -c(game_id, season, week, team, gameday)),
      by = dplyr::join_by(game_id, season, week, home_team == team)
    ) |>
    dplyr::left_join(
      team_features_data |>
        dplyr::select(-location) |>
        dplyr::rename_with(~paste0("away_", .x),
                           .cols = -c(game_id, season, week, team, gameday)),
      by = dplyr::join_by(game_id, season, week, away_team == team, gameday)
    )

  return(game_features_data)
}
