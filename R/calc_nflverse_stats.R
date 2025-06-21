#' Compute or update nflverse summary stats for teams or players (smart incremental rebuild)
#'
#' Computes summary-level stats for teams or players using nflfastR.
#' - Always loads (or creates) an .rds file at a canonical path (see make_stats_filename).
#' - Uses should_rebuild to decide whether to recalculate given source games.
#' - If new games are present in any processed season, those seasons are recalculated and their rows replaced.
#'
#' @param game_long_df  Data frame with at least season/week/game_id for all games to process (e.g., game_data_long or game_data)
#' @param sum_level     Character. "week" or "season"
#' @param stat_level    Character. "team" or "player"
#' @param season_level  Character. "ALL", "REG", or "POST"
#' @param seasons_to_process Integer vector of seasons to process (usually all_seasons or just get_current_season())
#' @return              Data frame of summary stats for all seasons (read from archive or newly computed)
#' @export
#' @noRd
calc_nflverse_stats <- function(
    game_long_df,
    sum_level,
    stat_level,
    season_level,
    season_proc
) {
  spec <- list(
    sum_level    = sum_level,
    stat_level   = stat_level,
    season_level = season_level
  )
  stats_path <- make_stats_filename(spec)
  prior_stats <- if (file.exists(stats_path)) readRDS(stats_path) else NULL

  rebuild_needed <- is.null(prior_stats) ||
    should_rebuild(
      source_df = game_long_df,
      prior_df  = prior_stats,
      id_cols   = c("season", "week", "team")
    )

  if (rebuild_needed) {
    season_type <- season_type_for_nflfastR(season_level)
    new_stats <- progressr::with_progress({
      nflfastR::calculate_stats(
        seasons       = season_proc,
        summary_level = sum_level,
        stat_type     = stat_level,
        season_type   = season_type
      )
    })

    if (!is.null(prior_stats)) {
      prior_stats <- prior_stats |> filter(!season %in% season_proc)
      all_stats <- dplyr::bind_rows(prior_stats, new_stats)
    } else {
      all_stats <- new_stats
    }
    all_stats <- all_stats |> add_nflverse_ids()
    dir.create(dirname(stats_path), recursive = TRUE, showWarnings = FALSE)
    saveRDS(all_stats, stats_path)
    return(all_stats)
  } else {
    return(prior_stats)
  }
}
