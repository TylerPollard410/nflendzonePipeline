#' Compute weekly SRS standings across all seasons and weeks (wide input)
#'
#' @param game_df      Wide-format games data: season, week, season_type, home_team, away_team, home_score, away_score, result, etc.
#' @param season_type  Filter for season_type ("REG", "POST", or "ALL") [default "ALL"]
#' @param tol          Numeric tolerance for SRS convergence (default 1e-3)
#' @param max_iter     Maximum iterations for convergence (default 100)
#' @param reset        TRUE (reset per season), FALSE (cumulative), or integer (rolling window)
#' @param print_message Logical, print progress per week (default TRUE)
#' @return Tibble with summary stats plus MOV, SOS, SRS, OSRS, DSRS, and week
#' @export
#' @noRd
compute_weekly_standings_data <- function(
    game_df,
    season_type = "ALL",  # Accepts "REG", "POST", or "ALL"
    tol = 1e-3,
    max_iter = 100,
    reset = TRUE,
    print_message = TRUE,
    ...
) {
  req_cols <- c("season", "week", "season_type", "home_team", "away_team", "home_score", "away_score", "result")
  missing_cols <- setdiff(req_cols, names(game_df))
  if (length(missing_cols) > 0) {
    stop("Missing columns in game_df: ", paste(missing_cols, collapse = ", "))
  }

  stype <- toupper(season_type)
  allowed_types <- c("REG", "POST", "ALL")
  if (!all(stype %in% allowed_types)) {
    stop("season_type must be one of: REG, POST, ALL (case-insensitive)")
  }

  # Filter by season_type if not ALL
  if (!identical(stype, "ALL")) {
    valid_games <- game_df |> dplyr::filter(season_type %in% stype, !is.na(result))
  } else {
    valid_games <- game_df |> dplyr::filter(!is.na(result))
  }

  # Wide format in, passed straight to calc_weekly_standings
  seasonWeekStandings <- calc_weekly_standings(
    valid_games,
    tol = tol,
    max_iter = max_iter,
    reset = reset,
    print_message = print_message,
    ...
  )

  seasonWeekStandings |>
    dplyr::select(season, week, team, dplyr::everything()) |>
    dplyr::arrange(season, week, team) |>
    tibble::as_tibble()
}
