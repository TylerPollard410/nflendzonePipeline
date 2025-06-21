#' Compute game-by-game and end-of-week ELO ratings for a series of games
#'
#' @param games Data frame of games: game_id, season, week, gameday, home_team, away_team, home_score, away_score, location
#' @param initial_elo Numeric or named vector of starting ratings (default 1500)
#' @param K Numeric learning rate (default 20)
#' @param home_advantage Numeric points for home team (default 65)
#' @param d Numeric scaling factor for ELO formula (default 400)
#' @param apply_margin_multiplier Logical; whether to weight updates by margin of victory (default TRUE)
#' @return A list with elements: elo_history (data frame of pre- and post-game ELOs), final_ratings (named vector after all games)
#' @export
#' @noRd
calc_elo_ratings <- function(
    games,
    initial_elo = 1500,
    K = 20,
    home_advantage = 65,
    d = 400,
    apply_margin_multiplier = TRUE
) {
  games <- games |>
    dplyr::arrange(season, week, as.Date(gameday), game_id) |>
    dplyr::filter(!is.na(home_score), !is.na(away_score))

  teams <- unique(c(games$home_team, games$away_team))
  if (length(initial_elo) == 1) {
    ratings <- setNames(rep(initial_elo, length(teams)), teams)
  } else {
    if (is.null(names(initial_elo)))
      stop("initial_elo must be a named vector")
    ratings <- initial_elo[teams]
  }

  hist <- games |>
    dplyr::select(game_id, season, week, gameday,
                  home_team, away_team, home_score, away_score, location) |>
    dplyr::mutate(home_elo_pre  = NA_real_,
                  away_elo_pre  = NA_real_,
                  home_elo_post = NA_real_,
                  away_elo_post = NA_real_)

  for (i in seq_len(nrow(games))) {
    g <- games[i,]
    missing <- setdiff(c(g$home_team, g$away_team), names(ratings))
    if (length(missing) > 0) {
      stop("No seed for teams: ", paste(missing, collapse = ", "))
    }
    is_home <- ifelse(g$location == "Home", 1, 0)
    hfa_adj <- home_advantage * is_home

    he <- ratings[g$home_team]
    ae <- ratings[g$away_team]
    hist$home_elo_pre[i] <- he
    hist$away_elo_pre[i] <- ae

    exp_h <- 1 / (1 + 10 ^ ((ae - (he + hfa_adj)) / d))
    if      (g$home_score > g$away_score) { sh <- 1; sa <- 0 }
    else if (g$home_score < g$away_score) { sh <- 0; sa <- 1 }
    else                                  { sh <- 0.5; sa <- 0.5 }

    mult <- 1
    if (apply_margin_multiplier && g$home_score != g$away_score) {
      m    <- abs(g$home_score - g$away_score)
      diff <- abs(he - ae + hfa_adj)
      mult <- log(m + 1) * (2.2 / ((diff * 0.001) + 2.2))
    }

    nh <- he + K * mult * (sh - exp_h)
    na <- ae + K * mult * (sa - (1 - exp_h))
    ratings[g$home_team] <- nh
    ratings[g$away_team] <- na

    hist$home_elo_post[i] <- nh
    hist$away_elo_post[i] <- na

    # Print progress for end of week
    next_wk <- if (i < nrow(games)) games$week[i + 1] else NA_integer_
    if (is.na(next_wk) || games$week[i] != next_wk) {
      cat(sprintf("      [ELO] Finished Season %s Week %s\n", g$season, g$week))
    }
  }

  list(
    elo_history   = hist,
    final_ratings = ratings
  )
}
