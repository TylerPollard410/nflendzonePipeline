#' Compute SRS data for multiple reset window options, no caching
#'
#' @param game_df      Data frame of games with columns: season, week, team, opponent, result, team_score, opponent_score
#' @param resets       List of logical or numeric values specifying reset parameter options (default: list(TRUE, 10, 20))
#' @param tol          Numeric tolerance for SRS convergence (default: 1e-3)
#' @param max_iter     Maximum iterations for convergence (default: 100)
#' @param ...          Additional arguments passed to calc_weekly_standings
#' @return A tibble with season, week, team, summary columns and rating stats (MOV, SOS, SRS, OSRS, DSRS) for each reset window
#' @export
#' @noRd
compute_srs_data <- function(game_df,
                             resets       = list(TRUE, 10, 20),
                             tol          = 1e-3,
                             max_iter     = 100,
                             ...) {
  compute_for_df <- \(df_subset) {
    rating_cols <- c("MOV", "SOS", "SRS", "OSRS", "DSRS")
    suffixes    <- purrr::map_chr(resets, \(x) if (identical(x, TRUE)) "" else paste0("_", x))
    srs_list    <- purrr::map2(resets, suffixes, \(r, suffix) {
      message("---- Computing ratings for reset mode:   ", r)
      weekly <- calc_weekly_standings(
        df_subset,
        tol      = tol,
        max_iter = max_iter,
        reset    = r,
        ...
      )
      if (suffix == "") {
        weekly |>
          dplyr::select(season, week, team,
                        games, wins, losses, ties, pf, pa, pd, win_pct,
                        dplyr::all_of(rating_cols))
      } else {
        weekly |>
          dplyr::select(season, week, team, dplyr::all_of(rating_cols)) |>
          dplyr::rename_with(\(.x) paste0(.x, suffix), dplyr::all_of(rating_cols))
      }
    })
    purrr::reduce(srs_list, dplyr::left_join, by = c("season", "week", "team"))
  }

  compute_for_df(game_df) |>
    dplyr::arrange(season, week, team)
}
