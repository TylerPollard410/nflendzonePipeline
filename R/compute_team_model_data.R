# compute_team_model_data.R

#' Compute Team-Level Modeling Data (Long Format)
#'
#' This function computes engineered features for team-level model data in long format,
#' including ELO smoothing, rolling means, cumulative averages, and lagged features for each team.
#'
#' @param game_data_long Data frame (long format) of team-game rows. Must include columns:
#'   game_id, season, game_type, season_type, week, team, opponent, location.
#' @param team_features_data Data frame of team-level features to join, including ELO and performance metrics.
#' @param elo_update_roll_window Integer. Window for rolling mean of `elo_update` (default: 5).
#' @param feats_roll_window Integer. Window for rolling/cumulative means of stat features (default: 5).
#'
#' @return A tibble (data.frame) with one row per team-game, including all engineered features.
#'
#' @details
#' The returned data frame contains team-game-level features with various engineered columns,
#' such as lagged ELO, rolling updates, per-game rates, and cumulative means. Columns that match
#' scoring and performance stats (pf, pfg, pa, pag, win_pct, MOV, SOS, SRS, OSRS, DSRS) are lagged
#' and have rolling/cumulative features computed. ELO initialization is handled with weighted averages
#' for first games in a season.
#'
#' @examples
#' \dontrun{
#' model_data <- compute_team_model_data(game_data_long, team_features_data, 3, 7)
#' }
#'
#' @importFrom dplyr select filter left_join mutate across case_when lag cummean all_of
#' @importFrom slider slide_dbl
#' @importFrom purrr reduce
#' @importFrom glue glue
#' @export
#' @noRd
compute_team_model_data <- function(game_data_long,
                                    team_features_data,
                                    elo_update_roll_window = 5,
                                    feats_roll_window = 5) {

  game_long_id_keys <- game_data_long |>
    dplyr::select(
      game_id, season, game_type, season_type, week, team, opponent, location
    )

  team_model_data <- game_long_id_keys |>
    dplyr::left_join(team_features_data) |>
    dplyr::mutate(
      elo_pre = dplyr::case_when(
        week == 1 ~ dplyr::lag(elo_post, n = 1)*0.6 + 1500*0.4,
        week != 1 ~ elo_pre,
        TRUE ~ NA_real_
      ),
      .by = team
    ) |>
    dplyr::mutate(
      "{glue::glue('elo_update_roll_{elo_update_roll_window}')}" :=
        slider::slide_dbl(elo_update, mean, .before = elo_update_roll_window - 1, .complete = FALSE),
      .after = elo_update,
      .by = team
    ) |>
    dplyr::mutate(
      "{glue::glue('elo_update_roll_{elo_update_roll_window}')}" :=
        dplyr::lag(.data[[glue::glue('elo_update_roll_{elo_update_roll_window}')]], n = 1),
      .by = team
    ) |>
    dplyr::mutate(
      pfg = pf/games,
      pag = pa/games,
      .after = pa
    ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(matches("^(pf|pfg|pa|pag|win_pct|MOV|SOS|SRS|OSRS|DSRS)($|_|[0-9])")),
        ~dplyr::lag(.x, n = 1, default = NA)),
      .by = team
    ) |>
    (\(.) {
      after_cols <- names(.)[(which(names(.) == "DSRS_20") + 1):ncol(.)]
      purrr::reduce(
        after_cols,
        \(df, col) {
          roll_name <- glue::glue("{col}_roll_{feats_roll_window}")
          cummean_name <- glue::glue("{col}_cummean")
          df |>
            dplyr::mutate(
              "{cummean_name}" := cummean(.data[[col]]),
              "{roll_name}"    := slider::slide_dbl(.data[[col]], mean, .before = feats_roll_window - 1, .complete = FALSE),
              "{cummean_name}" := dplyr::lag(.data[[cummean_name]], n = 1),
              "{roll_name}"    := dplyr::lag(.data[[roll_name]], n = 1),
              .after = col,
              .by = team
            ) |>
            dplyr::select(-dplyr::all_of(col))
        },
        .init = .
      )
    })()
}
