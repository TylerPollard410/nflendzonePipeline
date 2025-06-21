#' Compute and update full ELO history for all NFL games
#'
#' Handles both full rebuild (no prior_data) and incremental updates (if prior_data is supplied).
#'
#' @param game_df Data frame of all games (wide format): should include season, week, gameday, game_id, home_team, away_team, home_score, away_score, location
#' @param initial_elo Numeric (default 1500) or named vector (team names) of initial ratings
#' @param K Numeric ELO learning rate (default 20)
#' @param home_advantage Numeric points for home field (default 65)
#' @param d Scaling factor for ELO probability (default 400)
#' @param apply_margin_multiplier Logical; weight update by margin of victory (default TRUE)
#' @param season_factor Rollover factor for team ratings each season (0 = reset, 1 = full carryover)
#' @param prior_data Optionally, a prior elo_history data.frame for incremental update (default NULL)
#' @param verbose Print console feedback (default TRUE)
#' @return Data frame of game-by-game ELO ratings, including pre- and post-game ratings for each team
#' @export
#' @noRd
compute_elo_data <- function(
    game_df,
    initial_elo            = 1500,
    K                      = 20,
    home_advantage         = 65,
    d                      = 400,
    apply_margin_multiplier = TRUE,
    season_factor          = 0.6,
    prior_data             = NULL,
    verbose                = TRUE
) {
  # Sort for reproducibility
  games <- game_df |>
    dplyr::arrange(season, week, as.Date(gameday), game_id)

  # FULL REBUILD: no prior data provided
  if (is.null(prior_data)) {
    if (verbose) message("[ELO] Full recompute from scratch...")

    elo_hist  <- tibble::tibble()
    teams_all <- unique(c(games$home_team, games$away_team))
    ratings   <- setNames(rep(initial_elo, length(teams_all)), teams_all)

    for (ss in sort(unique(games$season))) {
      if (verbose) message(sprintf("[ELO] Computing season %s...", ss))
      games_ss <- games |> dplyr::filter(season == ss)
      if (ss != min(games$season)) {
        m       <- mean(ratings, na.rm = TRUE)
        ratings <- ratings * season_factor + m * (1 - season_factor)
        if (verbose) message(sprintf("[ELO] Season rollover for %s; mean ELO: %.1f", ss, m))
      }
      res <- calc_elo_ratings(
        games                  = games_ss,
        initial_elo            = ratings,
        K                      = K,
        home_advantage         = home_advantage,
        d                      = d,
        apply_margin_multiplier = apply_margin_multiplier
      )
      ratings  <- res$final_ratings
      elo_hist <- dplyr::bind_rows(elo_hist, res$elo_history)
    }
    return(elo_hist)
  }

  # INCREMENTAL: prior data provided (assume prior is already elo_history, as in archive or in-memory)
  if (verbose) {
    last_season <- max(prior_data$season)
    last_week   <- max(prior_data$week[prior_data$season == last_season])
    message(sprintf("[ELO] Incremental update: prior data covers season %s week %s", last_season, last_week))
  }

  # Find new games (only those after last recorded week in prior_data)
  last_season <- max(prior_data$season)
  last_week   <- max(prior_data$week[prior_data$season == last_season])
  games_remain <- games |>
    dplyr::filter(season > last_season | (season == last_season & week > last_week))

  if (nrow(games_remain) == 0) {
    if (verbose) message("[ELO] No new games found. Returning prior ELO data.")
    return(prior_data)
  }

  # Get team ELOs at the end of last archive
  finals <- prior_data |>
    dplyr::filter(season < last_season | (season == last_season & week <= last_week)) |>
    dplyr::transmute(season, week, team = home_team, elo = home_elo_post) |>
    dplyr::bind_rows(
      prior_data |>
        dplyr::filter(season < last_season | (season == last_season & week <= last_week)) |>
        dplyr::transmute(season, week, team = away_team, elo = away_elo_post)
    ) |>
    dplyr::arrange(season, week) |>
    dplyr::group_by(team) |>
    dplyr::slice_tail(n = 1) |>
    dplyr::ungroup()

  all_teams  <- unique(c(games$home_team, games$away_team))
  mean_final <- mean(finals$elo, na.rm = TRUE)
  ratings    <- setNames(
    sapply(all_teams, function(tm) {
      x <- finals$elo[finals$team == tm]
      if (length(x) == 0) mean_final else x
    }),
    all_teams
  )

  # Loop through remaining games by season
  elo_out        <- prior_data
  current_season <- last_season
  for (ss in sort(unique(games_remain$season))) {
    if (verbose) message(sprintf("[ELO] Processing season %s (new weeks/games)...", ss))
    if (ss != current_season) {
      m       <- mean(ratings, na.rm = TRUE)
      ratings <- ratings * season_factor + m * (1 - season_factor)
      current_season <- ss
      if (verbose) message(sprintf("[ELO] Season rollover for %s; mean ELO: %.1f", ss, m))
    }
    games_ss <- games_remain |> dplyr::filter(season == ss)
    res <- calc_elo_ratings(
      games                  = games_ss,
      initial_elo            = ratings,
      K                      = K,
      home_advantage         = home_advantage,
      d                      = d,
      apply_margin_multiplier = apply_margin_multiplier
    )
    ratings[names(res$final_ratings)] <- res$final_ratings
    elo_out <- dplyr::bind_rows(elo_out, res$elo_history)
  }

  elo_out
}
