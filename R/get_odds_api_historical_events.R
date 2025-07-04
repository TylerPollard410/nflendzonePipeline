#' Get Historical Event List Snapshot from The Odds API
#'
#' Returns a list of historical events (games) as they appeared in the API at the specified timestamp (no odds).
#'
#' @param sport The sport key as obtained from [get_odds_api_sports()].
#' @param date Snapshot timestamp in ISO8601 format (e.g., "2023-10-10T12:10:39Z").
#' @param api_key Optional. Odds API key. If NULL, will use Sys.getenv("ODDS_API_KEY").
#' @param dateFormat Format for timestamps: "iso" (default) or "unix".
#' @param eventIds Optional. Comma-separated game ids to filter.
#' @param commenceTimeFrom Optional. Only include events on/after this ISO8601 time.
#' @param commenceTimeTo Optional. Only include events on/before this ISO8601 time.
#' @param ... Reserved for future API parameters not yet documented.
#' @return A tibble: one row per event, with snapshot metadata columns (`timestamp`, `previous_timestamp`, `next_timestamp`).
#' @examples
#' \dontrun{
#'   get_odds_api_historical_events(
#'     sport = "americanfootball_nfl",
#'     date = "2024-10-13T13:00:00Z"
#'   )
#' }
#' @export
#' @noRd
get_odds_api_historical_events <- function(
    sport,
    date,
    api_key = NULL,
    dateFormat = "iso",
    eventIds = NULL,
    commenceTimeFrom = NULL,
    commenceTimeTo = NULL,
    ...
) {
  if (missing(sport) || !is.character(sport) || length(sport) != 1)
    stop("You must supply a single sport key as a character string.")
  if (missing(date) || !is.character(date) || length(date) != 1)
    stop("You must supply a single date/time string in ISO8601 format.")

  api_key <- get_odds_api_key(api_key)
  url <- glue::glue("https://api.the-odds-api.com/v4/historical/sports/{sport}/events")

  query <- list(
    apiKey = api_key,
    date = date,
    dateFormat = dateFormat,
    eventIds = eventIds,
    commenceTimeFrom = commenceTimeFrom,
    commenceTimeTo = commenceTimeTo,
    ...
  )
  query <- query[!vapply(query, is.null, logical(1))]

  snapshot <- odds_api_get(
    url = url,
    query = query
  )

  # Defensive: If no data, return default-structure empty tibble
  if (!is.list(snapshot) || !"data" %in% names(snapshot) || length(snapshot$data) == 0) {
    message("No historical events found for this sport/date combination.")
    return(tibble::tibble(
      timestamp = character(),
      previous_timestamp = character(),
      next_timestamp = character(),
      id = character(),
      sport_key = character(),
      sport_title = character(),
      commence_time = character(),
      home_team = character(),
      away_team = character()
    ))
  }

  # Add snapshot metadata columns to each event row
  out <- tibble::as_tibble(snapshot$data)
  out$timestamp <- snapshot$timestamp
  out$previous_timestamp <- snapshot$previous_timestamp
  out$next_timestamp <- snapshot$next_timestamp

  # Relocate snapshot metadata columns up front
  out |>
    dplyr::relocate(timestamp, previous_timestamp, next_timestamp)
}
