#' Compute team-level series conversion rates (weekly, long format)
#'
#' Calls nflfastR::calculate_series_conversion_rates to return game-by-game weekly series data.
#'
#' @param pbp_df  Play-by-play data for the target games/seasons (should be pre-filtered).
#' @param weekly  TRUE. Should data be summarized per team per week or per season
#' @return        Data frame of weekly series conversion rates (long format).
#' @export
#' @noRd
compute_series_data <- function(
    pbp_df,
    weekly = TRUE
) {
  progressr::with_progress({
    nflfastR::calculate_series_conversion_rates(
      pbp    = pbp_df,
      weekly = TRUE
    )
  }) |> add_nflverse_ids()
}
