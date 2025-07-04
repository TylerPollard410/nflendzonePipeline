#' Get Historical Odds Snapshots for a Sport/Event from The Odds API
#'
#' Returns a snapshot of games with bookmaker odds for a given sport, region, and market at a specified historical timestamp.
#'
#' @param sport The sport key as obtained from [get_odds_api_sports()].
#' @param date Snapshot timestamp in ISO8601 format (e.g., "2023-10-10T12:10:39Z").
#' @param api_key Optional. Odds API key. If NULL, will use Sys.getenv("ODDS_API_KEY").
#' @param regions Bookmaker regions to return. Valid values: "us", "us2", "uk", "au", "eu". Default is "us".
#' @param markets Odds markets to return. Default is "h2h" (moneyline). Also: "spreads", "totals", "outrights" (comma-separated allowed).
#' @param dateFormat Format for timestamps: "iso" (default) or "unix".
#' @param oddsFormat Format for odds: "decimal" (default) or "american".
#' @param eventIds Optional. Comma-separated game ids to filter.
#' @param bookmakers Optional. Comma-separated list of bookmakers to return.
#' @param includeLinks Optional. If "true", response includes bookmaker/event links.
#' @param includeSids Optional. If "true", includes source IDs.
#' @param includeBetLimits Optional. If "true", includes bet limits (mainly exchanges).
#' @param ... Reserved for future API parameters not yet documented.
#' @return A tibble: one row per bookmaker/market/outcome for each event at the snapshot, plus snapshot metadata columns (`timestamp`, `previous_timestamp`, `next_timestamp`).
#' @examples
#' \dontrun{
#'   get_odds_api_historical_odds(
#'     sport = "americanfootball_nfl",
#'     date = "2023-10-10T12:10:39Z",
#'     markets = "h2h"
#'   )
#' }
#' @export
#' @noRd
get_odds_api_historical_odds <- function(
    sport,
    date,
    api_key = NULL,
    regions = "us",
    markets = "h2h",
    dateFormat = "iso",
    oddsFormat = "decimal",
    eventIds = NULL,
    bookmakers = NULL,
    includeLinks = NULL,
    includeSids = NULL,
    includeBetLimits = NULL,
    ...
) {
  if (missing(sport) || !is.character(sport) || length(sport) != 1)
    stop("You must supply a single sport key as a character string.")
  if (missing(date) || !is.character(date) || length(date) != 1)
    stop("You must supply a single date/time string in ISO8601 format.")

  api_key <- get_odds_api_key(api_key)
  url <- glue::glue("https://api.the-odds-api.com/v4/historical/sports/{sport}/odds")

  query <- list(
    apiKey = api_key,
    date = date,
    regions = regions,
    markets = markets,
    dateFormat = dateFormat,
    oddsFormat = oddsFormat,
    eventIds = eventIds,
    bookmakers = bookmakers,
    includeLinks = includeLinks,
    includeSids = includeSids,
    includeBetLimits = includeBetLimits,
    ...
  )
  query <- query[!vapply(query, is.null, logical(1))]

  snapshot <- odds_api_get(
    url = url,
    query = query
  )

  # Defensive: If no data, return default-structure empty tibble
  if (!is.list(snapshot) || !"data" %in% names(snapshot) || length(snapshot$data) == 0) {
    message("No historical odds found for this sport/date/market combination.")
    return(tibble::tibble(
      timestamp = character(),
      previous_timestamp = character(),
      next_timestamp = character(),
      id = character(),
      sport_key = character(),
      sport_title = character(),
      commence_time = character(),
      home_team = character(),
      away_team = character(),
      bookmaker_key = character(),
      bookmaker = character(),
      bookmaker_last_update = character(),
      market_key = character(),
      market_last_update = character(),
      outcomes_name = character(),
      outcomes_price = numeric(),
      outcomes_point = numeric()
    ))
  }

  # Add snapshot metadata columns to each event row, then flatten as in odds function
  out <- tibble::as_tibble(snapshot$data)
  out$timestamp <- snapshot$timestamp
  out$previous_timestamp <- snapshot$previous_timestamp
  out$next_timestamp <- snapshot$next_timestamp

  out |>
    tidyr::unnest(bookmakers) |>
    dplyr::rename(
      bookmaker_key = key,
      bookmaker = title,
      bookmaker_last_update = last_update
    ) |>
    tidyr::unnest(markets) |>
    dplyr::rename(
      market_key = key,
      market_last_update = last_update
    ) |>
    tidyr::unnest(outcomes) |>
    dplyr::rename(
      outcomes_name = name,
      outcomes_price = price,
      outcomes_point = point
    ) |>
    dplyr::relocate(timestamp, previous_timestamp, next_timestamp)
}
