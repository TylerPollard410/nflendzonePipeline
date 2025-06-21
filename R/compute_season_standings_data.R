#' Compute full season standings with ratings
#'
#' @param game_df Data frame of all games. Must include columns: season, team, opponent, result, team_score, opponent_score, season_type
#' @param ...     Passed to calc_srs_ratings (e.g., tol, max_iter, print_message)
#' @return A tibble with combined standings and rating stats
#' @export
#' @noRd
compute_season_standings_data <- function(game_df, ...) {
  # Check required columns up front
  req_cols <- c("season", "season_type", "home_team", "away_team", "home_score", "away_score")
  missing_cols <- setdiff(req_cols, names(game_df))
  if (length(missing_cols) > 0) {
    stop("Missing columns in game_df: ", paste(missing_cols, collapse = ", "))
  }

  # Filter to regular season games with valid result
  reg_data <- game_df |>
    filter(season_type == "REG", !is.na(result))
  if (nrow(reg_data) == 0) stop("No regular season games found after filtering.")

  # Compute base standings using nflseedR (wraps nflverse style)
  base_tbl <- nflseedR::nfl_standings(games = reg_data)

  # Prepare long-format game data for ratings
  long <- reg_data |>
    clean_homeaway(invert = c("result", "spread_line"))

  # Compute ratings (SRS, MOV, etc) per season
  rating_tbl <- long |>
    group_by(season) |>
    group_modify(function(.x, .y) calc_srs_ratings(.x, season_year = .y$season, ...)) |>
    ungroup()

  # Merge base standings and ratings
  out_tbl <- base_tbl |>
    left_join(rating_tbl, by = c("season", "team")) |>
    arrange(season, team)

  # Relocate MOV, SOS, SRS, OSRS, DSRS before SOV if present
  if ("sov" %in% names(out_tbl)) {
    out_tbl <- out_tbl |>
      relocate(MOV, SOS, SRS, OSRS, DSRS, .before = sov)
  }

  tibble::as_tibble(out_tbl)
}
