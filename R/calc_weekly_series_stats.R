# seriesData.R
# Functions to compute and combine weekly series conversion rates per game-team

# Dependencies: dplyr, nflverse (loaded externally in UpdateData.R)

#' Update or load weekly series conversion rates
#'
#' @param pbp_df      Play-by-play tibble including season, week, and series-specific fields
#' @param series_loc   File path to save/load nflSeriesWeek RDA (e.g. ".../nflSeriesWeek.rda")
#' @param recompute_all Logical, if TRUE forces full recalculation even if saved file exists (default FALSE)
#' @return Tibble of weekly series conversion rates (nflSeriesWeek)
#' @export
#' @noRd
calc_weekly_series_stats <- function(pbp_df = pbp_data,
                                     series_loc,
                                     recompute_all = FALSE) {
  # uses dplyr, nflverse

  if (!recompute_all && file.exists(series_loc)) {
    cat("Updating Weekly Series Stats\n",
        "Calculating", get_current_season(), "season Data")
    load(series_loc)
    # drop current season to refresh
    nflSeriesWeek <- nflSeriesWeek |> filter(season != get_current_season())
    # compute only current season
    pbpData_series <- pbp_df |> filter(season == get_current_season())
    nflSeriesWeekTemp <- progressr::with_progress({
      nflfastR::calculate_series_conversion_rates(pbpData_series, weekly = TRUE)
    })
    nflSeriesWeek <- bind_rows(nflSeriesWeek, nflSeriesWeekTemp)
  } else {
    cat("Recomputing All Weekly Series Stats\n",
        "Calculating", min(pbp_df$season), "-", get_current_season(), "season Data")
    # full recalculation
    nflSeriesWeek <- progressr::with_progress({
      nflfastR::calculate_series_conversion_rates(pbp_df, weekly = TRUE)
    })
  }

  save(nflSeriesWeek, file = series_loc)
  return(nflSeriesWeek)
}

# Example usage:
# series_loc      <- "~/Desktop/NFLAnalysisTest/scripts/UpdateData/PriorData/nflSeriesWeek.rda"
# series_features <- compute_series_data(gameDataLong, pbpData, series_loc, recompute_all = TRUE)
# series_cols     <- colnames(select(series_features, contains("off"), contains("def")))
