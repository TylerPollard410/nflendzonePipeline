#' Get Historical Odds for a Single Event from The Odds API
#'
#' Returns historical odds for a single event as they appeared at a specified timestamp.
#'
#' @param sport The sport key as obtained from \code{\link{get_odds_api_sports}}.
#' @param eventId The event ID (from \code{\link{get_odds_api_historical_events}}).
#' @param date Snapshot timestamp in ISO8601 format (e.g., "2023-11-29T22:42:00Z").
#' @param api_key Optional. Odds API key. If NULL, will use Sys.getenv("ODDS_API_KEY").
#' @param regions Bookmaker regions to return. Valid values: "us", "us2", "uk", "au", "eu". Default is "us".
#' @param markets Odds markets to return. Any valid market key (comma-separated allowed).
#' @param dateFormat Format for timestamps: "iso" (default) or "unix".
#' @param oddsFormat Format for odds: "decimal" (default) or "american".
#' @param bookmakers Optional. Comma-separated list of bookmakers to return.
#' @param includeLinks Optional. If "true", response includes bookmaker/event links.
#' @param includeSids Optional. If "true", includes source IDs.
#' @param includeBetLimits Optional. If "true", includes bet limits (mainly exchanges).
#' @param ... Reserved for future API parameters not yet documented.
#' @return A tibble: one row per bookmaker/market/outcome for the event at the snapshot, with snapshot metadata columns.
#' @examples
#' \dontrun{
#'   # Get all market odds for one event at a historical snapshot:
#'   get_odds_api_historical_event_odds(
#'     sport = "basketball_nba",
#'     eventId = "da359da99aa27e97d38f2df709343998",
#'     date = "2023-11-29T22:42:00Z",
#'     markets = "player_points,h2h"
#'   )
#' }
#' @export
#' @noRd
get_odds_api_historical_event_odds <- function(
    sport,
    eventId,
    date,
    api_key = NULL,
    regions = "us",
    markets = "h2h",
    dateFormat = "iso",
    oddsFormat = "decimal",
    bookmakers = NULL,
    includeLinks = NULL,
    includeSids = NULL,
    includeBetLimits = NULL,
    ...
) {
  if (missing(sport) || !is.character(sport) || length(sport) != 1)
    stop("You must supply a single sport key as a character string.")
  if (missing(eventId) || !is.character(eventId) || length(eventId) != 1)
    stop("You must supply a single eventId as a character string.")
  if (missing(date) || !is.character(date) || length(date) != 1)
    stop("You must supply a single date/time string in ISO8601 format.")

  api_key <- get_odds_api_key(api_key)
  url <- glue::glue("https://api.the-odds-api.com/v4/historical/sports/{sport}/events/{eventId}/odds")

  query <- list(
    apiKey = api_key,
    date = date,
    regions = regions,
    markets = markets,
    dateFormat = dateFormat,
    oddsFormat = oddsFormat,
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

  # Defensive: If no data or no odds, return default-structure empty tibble
  if (!is.list(snapshot) || !"data" %in% names(snapshot) || length(snapshot$data) == 0) {
    message("No historical odds found for this event/snapshot/market combination.")
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
      outcomes_description = character(),
      outcomes_price = numeric(),
      outcomes_point = numeric()
    ))
  }

  # Unnest and tidy (may have extra description for props/alternates)
  out <- tibble::as_tibble(snapshot$data)
  out$timestamp <- snapshot$timestamp
  out$previous_timestamp <- snapshot$previous_timestamp
  out$next_timestamp <- snapshot$next_timestamp

  # If bookmakers field exists and has rows, unnest as in main odds functions
  if ("bookmakers" %in% names(out) && length(out$bookmakers[[1]]) > 0) {
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
        outcomes_description = description,
        outcomes_price = price,
        outcomes_point = point
      ) |>
      dplyr::relocate(timestamp, previous_timestamp, next_timestamp)
  } else {
    # No odds available, return just metadata row with NAs for odds columns
    out$bookmaker_key <- NA_character_
    out$bookmaker <- NA_character_
    out$bookmaker_last_update <- NA_character_
    out$market_key <- NA_character_
    out$market_last_update <- NA_character_
    out$outcomes_name <- NA_character_
    out$outcomes_description <- NA_character_
    out$outcomes_price <- NA_real_
    out$outcomes_point <- NA_real_
    out |> dplyr::relocate(timestamp, previous_timestamp, next_timestamp)
  }
}
