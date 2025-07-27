# compute_game_model_data.R

#' Compute Game-Level Model Data with Net Features
#'
#' Builds a modeling data frame for each game by joining in team-level features for both teams,
#' then computing flexible "net" features via user-configurable rules. Merges additional game data,
#' imputes missing weather, and adds a sequential week column.
#'
#' @param game_id_keys Data frame with core game columns (game_id, season, week, home_team, away_team, etc.).
#' @param game_data Data frame. Original game-level data (for scores, lines, weather, etc).
#' @param team_model_data Data frame. Long-format team-game-level features (one row per team per game).
#' @param net_configs List of lists. Each sublist must specify \code{var1_prefix}, \code{var2_prefix},
#'   \code{pattern1}, \code{pattern2}, and a binary function \code{fun}. See details.
#'
#' @return A tibble (data.frame) with game-level modeling features, including computed net features, weather, and week sequence.
#'
#' @details
#' 1. Joins in score/line columns from \code{game_data} and team features for both home/away teams from \code{team_model_data}.
#' 2. Adds "net" features for each config using \code{\link{add_flexible_net_features}}.
#' 3. Merges any remaining columns from \code{game_data}, imputes missing weather, removes old week_seq, and adds new week_seq.
#'
#' @examples
#' \dontrun{
#' game_model_data <- compute_game_model_data(game_id_keys, game_data, team_model_data)
#' }
#'
#' @importFrom dplyr left_join select rename_with mutate filter everything
#' @importFrom dplyr join_by
#' @importFrom purrr reduce
#' @export
#' @noRd
compute_game_model_data <- function(
    game_data,
    team_model_data,
    net_configs = list(
      list(
        var1_prefix = "home_", var2_prefix = "away_",
        pattern1 = "elo|MOV|SOS|SRS", pattern2 = "elo|MOV|SOS|SRS",
        fun = `-`,
        order_by_team = TRUE
      ),
      list(
        var1_prefix = "home_", var2_prefix = "away_",
        pattern1 = "pfg|OSRS", pattern2 = "pag|DSRS",
        fun = `-`,
        order_by_team = TRUE
      ),
      list(
        var1_prefix = "home_", var2_prefix = "away_",
        pattern1 = "pag|DSRS", pattern2 = "pfg|OSRS",
        fun = `-`,
        order_by_team = TRUE
      ),
      list(
        var1_prefix = "home_", var2_prefix = "away_",
        pattern1 = "(?=.*off)(?=.*epa).*", pattern2 = "(?=.*def)(?=.*epa).*",
        fun = `+`,
        order_by_team = TRUE
      ),
      list(
        var1_prefix = "home_", var2_prefix = "away_",
        pattern1 = "(?=.*off)(?=.*redzone).*", pattern2 = "(?=.*def)(?=.*redzone).*",
        fun = `+`,
        order_by_team = TRUE
      )
    )
) {

  # 1. Build base table by joining in game and team features for both teams
  game_model_data <- game_data |>
    dplyr::select(
      game_id, season, game_type, season_type, week, home_team, away_team, location,
      home_score, away_score, result, spread_line, total, total_line
    ) |>
    dplyr::left_join(
      team_model_data |>
        dplyr::select(-c(opponent, location, game_type, season_type, gameday)) |>
        dplyr::rename_with(~paste0("home_", .x),
                           .cols = -c(game_id, season, week, team)),
      by = dplyr::join_by(game_id, season, week, home_team == team)
    ) |>
    dplyr::left_join(
      team_model_data |>
        dplyr::select(-c(opponent, location, game_type, season_type, gameday)) |>
        dplyr::rename_with(~paste0("away_", .x),
                           .cols = -c(game_id, season, week, team)),
      by = dplyr::join_by(game_id, season, week, away_team == team)
    )

  # 2. Compute all net features
  game_model_data <- purrr::reduce(
    net_configs,
    \(df, cfg) add_flexible_net_features(
      df,
      var1_prefix = cfg$var1_prefix,
      var2_prefix = cfg$var2_prefix,
      pattern1    = cfg$pattern1,
      pattern2    = cfg$pattern2,
      fun         = cfg$fun
    ),
    .init = game_model_data
  )

  # 3. Merge remaining game data (weather, metadata), impute weather, filter, add week_seq
  game_model_data <- game_model_data |>
    dplyr::left_join(game_data) |>
    dplyr::mutate(
      temp = ifelse(is.na(temp), 68, temp),
      wind = ifelse(is.na(wind), 0, wind)
    ) |>
    dplyr::filter(season >= 2006) |>
    dplyr::select(-week_seq) |>
    add_week_seq()

  game_model_data
}
