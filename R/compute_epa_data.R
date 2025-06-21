#' Combine offense and defense EPA metrics per team-game
#'
#' Computes team-game level EPA summaries for both offense and defense,
#' then joins them by team, game, and season.
#'
#' @param pbp_df   Play-by-play data frame (must include game_id, season, week, posteam, defteam, epa, etc.)
#' @param scaled_wp  Logical; if TRUE, scale EPA by Vegas win probability (default FALSE)
#' @return Tibble: One row per team per game, with offense and defense EPA columns
#' @export
#' @noRd
compute_epa_data <- function(pbp_df, scaled_wp = FALSE) {
  epa_off <- calc_epa_ratings(pbp_df, side = "offense", scaled_wp = scaled_wp)
  epa_def <- calc_epa_ratings(pbp_df, side = "defense", scaled_wp = scaled_wp)
  epaData <- epa_off |>
    dplyr::left_join(
      epa_def,
      by = c("game_id", "season", "week", "team", "home_team", "away_team")
    )
  # Optional: warn if join dropped or duplicated rows
  expected_n <- nrow(epa_off)
  actual_n <- nrow(epaData)
  if (actual_n != expected_n) {
    warning(sprintf("EPA join: expected %d rows, got %d. Offense and defense join mismatch!", expected_n, actual_n))
  }
  return(epaData)
}
