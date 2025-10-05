#' Get Scores for Games from The Odds API
#'
#' Returns upcoming, live, and recently completed games (with scores if available) for a given sport.
#'
#' @param sport The sport key as obtained from \code{\link{get_odds_api_sports}}.
#' @param api_key Optional. Odds API key. If NULL, will use Sys.getenv("ODDS_API_KEY").
#' @param daysFrom Optional. Integer 1–3: how many days back to return completed games. If missing, only live/upcoming games are returned.
#' @param dateFormat Optional. Timestamp format: "iso" (default) or "unix".
#' @param eventIds Optional. Comma-separated game ids to filter results.
#' @param ... Reserved for future API parameters not yet documented.
#' @return A tibble of games with metadata and, for live/recent games, score columns.
#' @examples
#' \dontrun{
#'   # All NFL live/upcoming games:
#'   get_odds_api_scores(sport = "americanfootball_nfl")
#'
#'   # All NFL scores from last 2 days:
#'   get_odds_api_scores(sport = "americanfootball_nfl", daysFrom = 2)
#' }
#' @export
#' @noRd
get_odds_api_scores <- function(
    sport,
    api_key = NULL,
    daysFrom = NULL,
    dateFormat = "iso",
    eventIds = NULL,
    ...
) {
  if (missing(sport) || !is.character(sport) || length(sport) != 1)
    stop("You must supply a single sport key as a character string.")

  api_key <- get_odds_api_key(api_key)
  url <- glue::glue("https://api.the-odds-api.com/v4/sports/{sport}/scores")

  # Build query with all parameters provided
  query <- list(
    apiKey = api_key,
    daysFrom = daysFrom,
    dateFormat = dateFormat,
    eventIds = eventIds,
    ...
  )
  query <- query[!vapply(query, is.null, logical(1))]

  result <- odds_api_get(
    url = url,
    query = query
  ) |> to_tibble()

  # If any columns are still lists (e.g., scores for some sports), you can unnest them as needed.
  # For NFL and most sports, the result is already flat, but you can always check:
  # str(result)
  # If "scores" is a list-column (rare), you might use: result <- tidyr::unnest_wider(result, scores)

  result
}
