#' Compute rating statistics (SOS, SRS, OSRS, DSRS) for a single season or slice
#'
#' Computes Simple Rating System (SRS), Strength of Schedule (SOS),
#' Offensive SRS (OSRS), and Defensive SRS (DSRS) for all teams in a given set of games.
#'
#' @param game_long_df Long-format game data for one slice (season or week),
#'   must have columns: team, opponent, result, team_score, opponent_score
#' @param season_year  Integer. Season year for messages (default: current).
#' @param season_week  Integer. Week for messages (optional; default: NULL).
#' @param tol          Numeric. Convergence tolerance for SRS (default: 1e-3).
#' @param max_iter     Integer. Maximum iterations for SRS (default: 100).
#' @param print_message Logical. If TRUE, print progress messages (default: TRUE).
#' @return Tibble with columns team, MOV, SOS, SRS, OSRS, DSRS
#' @export
#' @noRd
calc_srs_ratings <- function(
    game_long_df,
    season_year = get_current_season(),
    season_week = NULL,
    tol = 1e-3,
    max_iter = 100,
    print_message = TRUE
) {
  # Check required columns up front
  req_cols <- c("team", "opponent", "result", "team_score", "opponent_score")
  missing_cols <- setdiff(req_cols, names(game_long_df))
  if (length(missing_cols) > 0) {
    stop("Missing columns in game_long_df: ", paste(missing_cols, collapse = ", "))
  }

  # Compose status message for logging
  msg <- NULL
  if (print_message) {
    msg <- if (is.null(season_week)) {
      paste("Computing Season", season_year, "...")
    } else {
      paste(sprintf("Computing Season %s Week %s...", season_year, season_week))
    }
  }

  # Initial summary for each team
  base <- game_long_df |>
    dplyr::group_by(team) |>
    dplyr::summarise(
      games_played   = dplyr::n(),
      team_score     = sum(team_score, na.rm = TRUE),
      opponent_score = sum(opponent_score, na.rm = TRUE),
      result         = sum(result, na.rm = TRUE),
      .groups        = "drop"
    ) |>
    dplyr::mutate(
      team_PPG = team_score / games_played,
      opp_PPG  = opponent_score / games_played,
      MOV      = result / games_played
    )

  leaguePPG <- sum(base$team_score, na.rm = TRUE) / sum(base$games_played, na.rm = TRUE)

  # Initialize ratings
  ratings <- base |>
    dplyr::transmute(
      team,
      MOV,
      SOS  = 0,
      SRS  = MOV,
      OSRS = team_PPG - leaguePPG,
      DSRS = SRS - OSRS
    )

  iter <- 0
  repeat {
    iter <- iter + 1
    prev <- ratings

    updates <- game_long_df |>
      dplyr::select(team, opponent, result, team_score) |>
      dplyr::left_join(
        ratings |> dplyr::select(team, SRS, DSRS),
        by = c("opponent" = "team")
      ) |>
      dplyr::mutate(
        opp_SRS = SRS,
        newSRS  = result + opp_SRS,
        newOSRS = team_score + DSRS - mean(team_score, na.rm = TRUE)
      ) |>
      dplyr::group_by(team) |>
      dplyr::summarise(
        SOS  = mean(opp_SRS, na.rm = TRUE),
        SRS  = mean(newSRS, na.rm = TRUE),
        OSRS = mean(newOSRS, na.rm = TRUE),
        .groups = "drop"
      ) |>
      dplyr::mutate(DSRS = SRS - OSRS)

    ratings <- ratings |>
      dplyr::select(team, MOV) |>
      dplyr::left_join(updates, by = "team")

    delta <- max(
      abs(ratings$SRS - prev$SRS),
      abs(ratings$OSRS - prev$OSRS),
      abs(ratings$DSRS - prev$DSRS),
      na.rm = TRUE
    )
    if (delta < tol || iter >= max_iter) break
  }

  # Only print message if requested and msg exists
  if (isTRUE(print_message) && !is.null(msg)) {
    if (delta < tol) {
      cat(msg, sprintf("  [calc_srs_ratings] Converged after %d iterations (delta=%.6f).\n", iter, delta))
    }
    if (iter >= max_iter) {
      cat(msg, sprintf("  [calc_srs_ratings] Reached maximum iterations (%d) without full convergence (delta=%.6f)\n", max_iter, delta))
    }
  }

  ratings
}
