#' Compute comprehensive score features for modeling (uses precomputed nflStatsWeek)
#'
#' @param game_long_df Tibble of long-format game-team rows (game_id, season, week, team, etc)
#' @param pbp_df       Play-by-play tibble containing two-point conversion info
#' @param nflStatsWeek Precomputed weekly stats data frame (from compute_nflverse_stats), not a file path!
#' @return Tibble with one row per game-team containing efficiency and score features
#' @export
#' @noRd
compute_scores_data <- function(
    game_long_df,
    pbp_df,
    nflStatsWeek
) {
  # identifier columns for gameDataLong
  id_cols <- c("game_id", "season", "week", "team")

  # STEP 1: Select raw scoring stats
  scoresData <- nflStatsWeek |>
    dplyr::select(
      season, week, team,
      passing_tds,
      rushing_tds,
      td_special   = special_teams_tds,
      def_tds,
      fumble_recovery_tds,
      passing_2pt_conversions,
      rushing_2pt_conversions,
      pat_att,
      pat_made,
      fg_att,
      fg_made,
      safeties_def = def_safeties
    )

  # STEP 2: Fix known data errors
  det_2011_13 <- scoresData$season == 2011 & scoresData$week == 13 & scoresData$team == "DET"
  scoresData$rushing_tds[det_2011_13] <- 1
  scoresData$pat_att[det_2011_13]     <- 2
  scoresData$pat_made[det_2011_13]    <- 2

  no_2011_13 <- scoresData$season == 2011 & scoresData$week == 13 & scoresData$team == "NO"
  scoresData$rushing_tds[no_2011_13]  <- 1
  scoresData$passing_tds[no_2011_13]  <- 3
  scoresData$pat_att[no_2011_13]      <- 4
  scoresData$pat_made[no_2011_13]     <- 4

  ari_2019_15 <- scoresData$season == 2019 & scoresData$week == 15 & scoresData$team == "ARI"
  scoresData$rushing_tds[ari_2019_15] <- 4
  scoresData$td_special[ari_2019_15]  <- 0

  cle_2019_15 <- scoresData$season == 2019 & scoresData$week == 15 & scoresData$team == "CLE"
  scoresData$passing_tds[cle_2019_15] <- 2
  scoresData$pat_att[cle_2019_15]     <- 3
  scoresData$pat_made[cle_2019_15]    <- 3

  # STEP 3: Aggregate scores
  scoresDataAgg <- scoresData |>
    dplyr::mutate(across(-c(season, week, team), ~ tidyr::replace_na(.x, 0))) |>
    dplyr::mutate(
      td_off   = passing_tds + rushing_tds,
      td_def   = def_tds + fumble_recovery_tds,
      td_total = td_off + td_special + td_def
    )

  # STEP 4: Two-point conversion data
  scores_two_pt_df <- pbp_df |>
    dplyr::select(
      season, week, posteam,
      defensive_two_point_conv,
      two_point_attempt, two_point_conv_result
    ) |>
    dplyr::filter(!is.na(posteam)) |>
    dplyr::mutate(
      two_pt_att  = two_point_attempt,
      two_pt_made = as.integer(two_point_conv_result == "success"),
      two_pt_def  = defensive_two_point_conv
    ) |>
    dplyr::group_by(season, week, posteam) |>
    dplyr::summarise(
      two_pt_att  = sum(two_pt_att, na.rm = TRUE),
      two_pt_made = sum(two_pt_made, na.rm = TRUE),
      two_pt_def  = sum(two_pt_def, na.rm = TRUE),
      .groups     = "drop"
    )

  # Join two-point stats
  scoresDataAgg <- scoresDataAgg |>
    dplyr::left_join(
      scores_two_pt_df,
      by = dplyr::join_by(season, week, team == posteam)
    )

  # STEP 5: Combine with gameDataLong and compute point breakdowns
  scoresFeatures <- game_long_df |>
    dplyr::filter(!is.na(result)) |>
    dplyr::select(all_of(id_cols)) |>
    dplyr::left_join(scoresDataAgg, by = dplyr::join_by(season, week, team)) |>
    dplyr::mutate(
      two_pt_def = rev(two_pt_def),
      .by        = c(game_id)
    ) |>
    dplyr::mutate(across(-all_of(id_cols), ~ tidyr::replace_na(.x, 0))) |>
    dplyr::mutate(
      points8      = two_pt_made,
      points7      = pat_made,
      points6      = td_total - two_pt_made - pat_made,
      points3      = fg_made,
      points2      = safeties_def + two_pt_def,
      points_total = points8*8 + points7*7 + points6*6 + points3*3 + points2*2
    )

  return(scoresFeatures)
}
