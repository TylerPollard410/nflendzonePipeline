#' Compute or update nflverse summary stats for teams or players
#'
#' This function computes summary-level stats for teams or players using nflfastR.
#' If prior_stats is provided, it will only compute stats for missing seasons.
#' The stats are always returned in-memory and *not* saved by this function.
#'
#' @param seasons      Integer vector of seasons to process.
#' @param spec         Named list: sum_level, stat_level, season_level.
#' @param prior_stats  Optional data frame of previously cached stats for incremental updates. Default NULL.
#' @return Tibble of summary stats for all requested seasons.
#' @export
#' @noRd
compute_nflverse_stats <- function(
    seasons,
    spec,
    prior_stats = NULL
) {
  season_type <- season_type_for_nflfastR(spec$season_level)
  # Determine which seasons to calculate
  seasons_done <- if (!is.null(prior_stats)) unique(prior_stats$season) else integer(0)
  seasons_missing <- setdiff(seasons, seasons_done)
  # If all done, return prior
  if (length(seasons_missing) == 0) return(prior_stats)
  # Calculate only missing
  new_stats <- progressr::with_progress({
    nflfastR::calculate_stats(
      seasons       = seasons_missing,
      summary_level = spec$sum_level,
      stat_type     = spec$stat_level,
      season_type   = season_type
    )
  })
  if (is.null(prior_stats)) {
    return(new_stats)
  } else {
    dplyr::bind_rows(prior_stats, new_stats)
  }
}
