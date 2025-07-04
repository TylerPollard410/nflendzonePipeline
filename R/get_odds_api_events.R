#' Get Events for a Sport or League from The Odds API
#'
#' Returns a list of in-play and pre-match events (with event id, teams, and commence time) for a specified sport or league.
#' Odds are not included. This endpoint does not count against the usage quota.
#'
#' @param sport The sport key as obtained from [get_odds_api_sports()].
#' @param api_key Optional. Odds API key. If NULL, will use Sys.getenv("ODDS_API_KEY").
#' @param dateFormat Optional. Timestamp format: "iso" (default) or "unix".
#' @param eventIds Optional. Comma-separated game ids to filter results.
#' @param commenceTimeFrom Optional. Show games commencing on/after this ISO 8601 timestamp.
#' @param commenceTimeTo Optional. Show games commencing on/before this ISO 8601 timestamp.
#' @param ... Reserved for future API parameters not yet documented.
#' @return A tibble of events for the sport/league.
#' @examples
#' \dontrun{
#'   # Get all NFL events:
#'   get_odds_api_events("americanfootball_nfl")
#'
#'   # Only games on or after a date:
#'   get_odds_api_events(
#'     sport = "americanfootball_nfl",
#'     commenceTimeFrom = "2025-09-09T00:00:00Z"
#'   )
#' }
#' @export
#' @noRd
get_odds_api_events <- function(
    sport,
    api_key = NULL,
    dateFormat = "iso",
    eventIds = NULL,
    commenceTimeFrom = NULL,
    commenceTimeTo = NULL,
    ...
) {
  if (missing(sport) || !is.character(sport) || length(sport) != 1)
    stop("You must supply a single sport key as a character string.")

  api_key <- get_odds_api_key(api_key)
  url <- glue::glue("https://api.the-odds-api.com/v4/sports/{sport}/events")

  query <- list(
    apiKey = api_key,
    dateFormat = dateFormat,
    eventIds = eventIds,
    commenceTimeFrom = commenceTimeFrom,
    commenceTimeTo = commenceTimeTo,
    ...
  )
  query <- query[!vapply(query, is.null, logical(1))]

  result <- odds_api_get(
    url = url,
    query = query
  ) |> to_tibble()

  result
}
