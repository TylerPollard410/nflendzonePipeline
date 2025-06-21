#' Compute weekly SRS standings across all seasons and weeks
#'
#' @param game_df   Data frame of games with columns season, week, team, opponent, result, team_score, opponent_score
#' @param tol        Numeric tolerance for SRS convergence (default 1e-3)
#' @param max_iter   Maximum iterations for convergence (default 100)
#' @param reset      TRUE (by season), FALSE (last 20 weeks rolling), or integer N (rolling window)
#' @param print_message Logical, print progress per week (default TRUE)
#' @return Tibble with all summary fields plus MOV, SOS, SRS, OSRS, DSRS, and week
#' @export
#' @noRd
calc_weekly_standings <- function(
    game_df,
    tol = 1e-3,
    max_iter = 100,
    reset = TRUE,
    print_message = TRUE,
    ...
) {
  valid_games <- game_df |> dplyr::filter(!is.na(result))
  weekGrid <- valid_games |> dplyr::distinct(season, week) |> dplyr::arrange(season, week)
  results <- vector("list", nrow(weekGrid))

  # Decide reset mode and window
  if (identical(reset, TRUE)) {
    reset_mode <- "season"; window <- NA_integer_
  } else if (identical(reset, FALSE)) {
    reset_mode <- "rolling"; window <- 20L
  } else if (is.numeric(reset) && length(reset) == 1 && reset > 0) {
    reset_mode <- "rolling"; window <- as.integer(reset)
  } else {
    stop("reset must be TRUE, FALSE, or a single positive integer")
  }

  for (i in seq_len(nrow(weekGrid))) {
    s <- weekGrid$season[i]
    w <- weekGrid$week[i]

    if (reset_mode == "season") {
      slice_df <- valid_games |> dplyr::filter(season == s, week <= w)
    } else if (reset_mode == "rolling") {
      week_id <- which(weekGrid$season == s & weekGrid$week == w)
      if (week_id <= window) {
        slice_weeks <- weekGrid[seq_len(week_id), ]
      } else {
        slice_weeks <- weekGrid[(week_id - window + 1):week_id, ]
      }
      slice_df <- valid_games |> dplyr::semi_join(slice_weeks, by = c("season", "week"))
    }

    long_df <- clean_homeaway(slice_df, invert = c("result", "spread_line"))
    # Team-level weekly summary
    base_w <- long_df |>
      dplyr::group_by(team) |>
      dplyr::summarise(
        games   = dplyr::n(),
        wins    = sum(result > 0),
        true_wins = sum(result > 0),
        losses  = sum(result < 0),
        ties    = sum(result == 0),
        pf      = sum(team_score, na.rm = TRUE),
        pa      = sum(opponent_score, na.rm = TRUE),
        pd      = pf - pa,
        win_pct = ifelse(games > 0, wins / games, NA_real_),
        .groups = "drop"
      ) |>
      dplyr::mutate(season = s) |>
      dplyr::relocate(season)

    ratings <- calc_srs_ratings(
      game_long_df  = long_df,
      season_year   = s,
      season_week   = w,
      tol           = tol,
      max_iter      = max_iter,
      print_message = print_message,
      ...
    )

    results[[i]] <- base_w |> dplyr::left_join(ratings, by = "team") |> dplyr::mutate(week = w)
  }
  dplyr::bind_rows(results)
}
