#' Get List of Participants for a Sport from The Odds API
#'
#' Returns all participants (teams or players) for a given sport. For most US sports, these will be teams; for individual sports, these may be players.
#'
#' @param sport The sport key as obtained from \code{\link{get_odds_api_sports}}.
#' @param api_key Optional. Odds API key. If NULL, will use Sys.getenv("ODDS_API_KEY").
#' @param ... Reserved for future API parameters not yet documented.
#' @return A tibble with columns: id (participant id), full_name (team/player name).
#' @examples
#' \dontrun{
#'   # Get all NBA teams:
#'   get_odds_api_participants("basketball_nba")
#'
#'   # Get all NFL teams:
#'   get_odds_api_participants("americanfootball_nfl")
#' }
#' @export
#' @noRd
get_odds_api_participants <- function(
    sport,
    api_key = NULL,
    ...
) {
  if (missing(sport) || !is.character(sport) || length(sport) != 1)
    stop("You must supply a single sport key as a character string.")

  api_key <- get_odds_api_key(api_key)
  url <- glue::glue("https://api.the-odds-api.com/v4/sports/{sport}/participants")

  query <- list(
    apiKey = api_key,
    ...
  )
  query <- query[!vapply(query, is.null, logical(1))]

  result <- odds_api_get(
    url = url,
    query = query
  ) |> to_tibble()

  # Return empty tibble if no participants found
  if (!is.data.frame(result) || nrow(result) == 0) {
    message("No participants found for this sport.")
    return(tibble::tibble(
      id = character(),
      full_name = character()
    ))
  }

  result
}
