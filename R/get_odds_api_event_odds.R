#' Get Odds for a Single Event from The Odds API
#'
#' Returns odds for a single event (game) for any supported market and bookmaker.
#'
#' @param sport The sport key as obtained from \code{\link{get_odds_api_sports}}.
#' @param eventId The id of the event (from the "id" field of \code{\link{get_odds_api_events}}).
#' @param api_key Optional. Odds API key. If NULL, will use Sys.getenv("ODDS_API_KEY").
#' @param regions Bookmaker regions to return. Valid values: "us", "us2", "uk", "au", "eu". Default is "us".
#' @param markets Odds markets to return. Can be **any supported market key** (see [all markets](https://the-odds-api.com/sports-odds-data/betting-markets.html)), not just featured. Use comma-separated market keys or "all" for all available. Default is "h2h".
#' @param dateFormat Format for timestamps: "iso" (default) or "unix".
#' @param oddsFormat Format for odds: "decimal" (default) or "american".
#' @param bookmakers Optional. Comma-separated list of bookmakers to return.
#' @param includeLinks Optional. If "true", response includes bookmaker/event links.
#' @param includeSids Optional. If "true", includes source IDs.
#' @param includeBetLimits Optional. If "true", includes bet limits (mainly exchanges).
#' @param ... Reserved for future API parameters not yet documented.
#' @return A tibble: one row per bookmaker/market/outcome for the specified event.
#' @details
#' This endpoint supports **any available betting market** for the event. For popular markets (e.g., moneyline, spreads, totals), the main /odds endpoint is simpler and more cost-effective.
#' @examples
#' \dontrun{
#'   # All market odds for one event:
#'   evs <- get_odds_api_events("americanfootball_nfl")
#'   eid <- evs$id[1]
#'   get_odds_api_event_odds(
#'     sport = "americanfootball_nfl",
#'     eventId = eid,
#'     markets = "all"
#'   )
#'
#'   # Or just one exotic market:
#'   get_odds_api_event_odds(
#'     sport = "americanfootball_nfl",
#'     eventId = eid,
#'     markets = "first_team_to_score"
#'   )
#' }
#' @export
#' @noRd
get_odds_api_event_odds <- function(
    sport,
    eventId,
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

  api_key <- get_odds_api_key(api_key)
  url <- glue::glue("https://api.the-odds-api.com/v4/sports/{sport}/events/{eventId}/odds")

  query <- list(
    apiKey = api_key,
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

  result <- odds_api_get(
    url = url,
    query = query
  ) |> to_tibble()

  # If result is an empty data frame or no rows, return empty tibble
  if (is.data.frame(result) && (nrow(result) == 0 || !"bookmakers" %in% names(result))) {
    message("No event found.")
    return(tibble::tibble())
  }

  # If it's a list with metadata and empty bookmakers, extract metadata and return NA for odds cols
  if (is.list(result) && !is.data.frame(result) && "bookmakers" %in% names(result)) {
    if (length(result$bookmakers) == 0) {
      message("Event metadata found, but no odds available for this event/market/bookmaker combination.")
      out <- tibble::tibble(
        id = result$id,
        sport_key = result$sport_key,
        sport_title = result$sport_title,
        commence_time = result$commence_time,
        home_team = result$home_team,
        away_team = result$away_team,
        bookmaker_key = NA_character_,
        bookmaker = NA_character_,
        bookmaker_last_update = NA_character_,
        market_key = NA_character_,
        market_last_update = NA_character_,
        outcomes_name = NA_character_,
        outcomes_price = NA_real_,
        outcomes_point = NA_real_
      )
      return(out)
    }
    # Otherwise, convert the list to a one-row tibble for unnesting
    result <- tibble::as_tibble(result)
  }

  # Unnest and rename as in your main odds function
  result |>
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
    )
}

