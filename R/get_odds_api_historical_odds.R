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

  # Helper for default-structure empty tibble (with snapshot columns)
  empty_odds_snapshot_tibble <- function() tibble::tibble(
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
  )

  # Defensive: No data in snapshot
  if (!is.list(snapshot) || !"data" %in% names(snapshot) || length(snapshot$data) == 0) {
    message("No historical odds found for this sport/date/market combination (empty data).")
    return(empty_odds_snapshot_tibble())
  }

  result <- tibble::as_tibble(snapshot$data)
  # Add snapshot-level columns
  result$timestamp <- snapshot$timestamp
  result$previous_timestamp <- snapshot$previous_timestamp
  result$next_timestamp <- snapshot$next_timestamp

  # If ALL bookmaker lists are empty for every event:
  if (all(purrr::map_lgl(result$bookmakers, \(x) length(x) == 0))) {
    message("No bookmakers/odds available for this snapshot, but returning event metadata.")
    return(
      result |>
        dplyr::transmute(
          timestamp, previous_timestamp, next_timestamp,
          id, sport_key, sport_title, commence_time, home_team, away_team,
          bookmaker_key = NA_character_,
          bookmaker = NA_character_,
          bookmaker_last_update = NA_character_,
          market_key = NA_character_,
          market_last_update = NA_character_,
          outcomes_name = NA_character_,
          outcomes_price = NA_real_,
          outcomes_point = NA_real_
        )
    )
  }

  # Otherwise, proceed to unnest as usual
  result <- tidyr::unnest(result, bookmakers)
  if (nrow(result) == 0 || !"key" %in% names(result)) {
    message("No bookmakers found after unnest (likely all empty) -- this should not happen due to previous check.")
    return(empty_odds_snapshot_tibble())
  }
  result <- dplyr::rename(
    result,
    bookmaker_key = key,
    bookmaker = title,
    bookmaker_last_update = last_update
  )

  result <- tidyr::unnest(result, markets)
  if (nrow(result) == 0 || !"key" %in% names(result)) {
    message("No markets found after bookmaker filter (empty markets).")
    return(empty_odds_snapshot_tibble())
  }
  result <- dplyr::rename(
    result,
    market_key = key,
    market_last_update = last_update
  )

  result <- tidyr::unnest(result, outcomes)
  if (nrow(result) == 0 || !"name" %in% names(result)) {
    message("No outcomes found after market filter (empty outcomes).")
    return(empty_odds_snapshot_tibble())
  }
  result <- dplyr::rename(
    result,
    outcomes_name = name,
    outcomes_price = price,
    outcomes_point = point
  )

  result |>
    dplyr::relocate(timestamp, previous_timestamp, next_timestamp)
}
