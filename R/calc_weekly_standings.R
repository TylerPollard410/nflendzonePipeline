# calc_weekly_standings.R
# Helper script to compute cumulative weekly SRS standings for each season-week

# Dependencies: dplyr, nflverse (loaded externally in UpdateData.R)

#' Compute weekly SRS standings across all seasons and weeks
#'
#' @param game_df   Data frame of games with columns season, week, team, opponent, result, team_score, opponent_score
#' @param tol        Numeric tolerance for SRS convergence (default 1e-3)
#' @param max_iter   Maximum iterations for convergence (default 100)
#' @param reset      TRUE (by season), FALSE (last 20 weeks rolling), or integer N (rolling window)
#' @return Tibble with all summary fields plus MOV, SOS, SRS, OSRS, DSRS, and week
#' @export
#' @noRd
calc_weekly_standings <- function(game_df = game_data,
                                     tol = 1e-3,
                                     max_iter = 100,
                                     reset = TRUE,
                                     ...) {
  valid_games <- game_df |> filter(!is.na(result))
  weekGrid <- valid_games |> distinct(season, week) |> arrange(season, week)
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
    s <- weekGrid$season[i]; w <- weekGrid$week[i]
    #cat(sprintf("Computing Season %s Week %s...", s, w))

    if (reset_mode == "season") {
      slice_df <- valid_games |> filter(season == s, week <= w)
    } else if (reset_mode == "rolling") {
      week_id <- which(weekGrid$season == s & weekGrid$week == w)
      if (week_id <= window) {
        slice_weeks <- weekGrid[seq_len(week_id), ]
      } else {
        slice_weeks <- weekGrid[(week_id - window + 1):week_id, ]
      }
      slice_df <- valid_games |> semi_join(slice_weeks, by = c("season", "week"))
    }

    long_df <- clean_homeaway(slice_df, invert = c("result", "spread_line"))
    # Always group by team only so no duplicates when rolling window crosses seasons
    base_w <- long_df |>
      group_by(team) |>
      summarise(
        games   = n(),
        wins    = sum(result > 0),
        true_wins = sum(result > 0),
        losses  = sum(result < 0),
        ties    = sum(result == 0),
        pf      = sum(team_score, na.rm = TRUE),
        pa      = sum(opponent_score, na.rm = TRUE),
        pd      = pf - pa,
        win_pct = ifelse(games > 0, wins / games, NA_real_),
        .groups = "drop"
      ) |> mutate(season = s) |> relocate(season)

    ratings <- calc_srs_ratings(
      game_long_df = long_df,
      season_year  = s,
      season_week  = w,
      tol          = tol,
      max_iter     = max_iter,
      print_message = TRUE,
      ...
    )

    results[[i]] <- base_w |> left_join(ratings, by = "team") |> mutate(week = w)
  }
  bind_rows(results)
}


# Auto-run when sourced in UpdateData.R
# if (exists("game_df")) {
#   seasonWeekStandings <- update_weekly_standings(
#     game_df,
#     reset = TRUE,
#     recompute_all = FALSE,
#     cache_file = "./scripts/UpdateData/PriorData/seasonWeekStandings.rda"
#   )
# }

