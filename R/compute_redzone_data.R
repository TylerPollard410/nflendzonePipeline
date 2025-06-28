# redzoneData.R
# Function to compute and combine red-zone efficiency stats per game-team

# Dependencies: dplyr (loaded externally in UpdateData.R)

#' Compute red-zone feature dataset for modeling
#'
#' @param game_long_df Tibble of long-format game-team rows with columns: game_id, season, week, team, opponent
#' @param pbp_df      Play-by-play tibble including: game_id, season, week,
#'                      posteam, home_team, away_team, fixed_drive,
#'                      fixed_drive_result, drive_inside20, drive_ended_with_score
#' @return Tibble with one row per game-team containing:
#'   off_redzone_app_perc, off_redzone_eff,
#'   def_redzone_app_perc, def_redzone_eff
#' @export
#' @noRd
compute_redzone_data <- function(game_long_df = game_data_long,
                                 pbp_df = pbp_data) {
  # uses dplyr

  # STEP 1: Summarise raw red-zone metrics per team-game
  redzoneData <- pbp_df |>
    filter(!is.na(posteam)) |>
    select(
      game_id, season, week,
      posteam, home_team, away_team,
      fixed_drive, fixed_drive_result, drive_inside20
    ) |>
    distinct() |>
    group_by(game_id, season, week, posteam, home_team, away_team) |>
    reframe(
      drives_num        = n(),
      redzone_app      = sum(drive_inside20, na.rm = TRUE),
      redzone_td       = sum(fixed_drive_result == "Touchdown" & drive_inside20, na.rm = TRUE),
      redzone_app_perc = ifelse(drives_num > 0, redzone_app / drives_num, 0),
      redzone_eff      = ifelse(redzone_app > 0, redzone_td / redzone_app, 0)
    )

  # STEP 2: Build offense and defense features
  redzone_features <- redzoneData |>
    arrange(game_id, posteam) |>
    group_by(game_id) |>
    mutate(
      opponent = rev(posteam)
    ) |>
    ungroup() |>
    rename(team = posteam) |>
    transmute(
      game_id,
      season,
      week,
      team,
      opponent,
      off_redzone_app_perc = redzone_app_perc,
      off_redzone_eff      = redzone_eff,
      def_redzone_app_perc = redzone_app_perc[match(team, opponent)],
      def_redzone_eff      = redzone_eff[match(team, opponent)]
    )

  # STEP 3: Merge into gameDataLong ordering
  id_cols <- c("game_id", "season", "week", "team", "opponent")
  redzone_data <- game_long_df |>
    filter(!is.na(result)) |>
    select(all_of(id_cols)) |>
    left_join(redzone_features, by = id_cols) |>
    mutate(across(starts_with("off_"),  ~ replace_na(.x, 0)),
           across(starts_with("def_"),  ~ replace_na(.x, 0))) |>
    add_nflverse_ids()

  return(redzone_data)
}

# Example usage:
# redzone_data <- compute_redzone_data(gameDataLong, pbpData)
# redzone_cols <- colnames(select(redzone_data, starts_with("off_"), starts_with("def_")))
