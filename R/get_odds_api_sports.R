#' Get List of Sports from The Odds API
#'
#' Fetches all sports supported by the-odds-api.com, including their unique keys.
#'
#' @param api_key Optional. Odds API key. If NULL, will use Sys.getenv("ODDS_API_KEY").
#' @param all Logical. If TRUE, returns all sports (in and out of season). Default is FALSE (in-season only).
#' @return A tibble of available sports and their metadata.
#' @examples
#' \dontrun{
#'   # Only in-season sports (default)
#'   get_odds_api_sports()
#'
#'   # All sports, including out-of-season
#'   get_odds_api_sports(all = TRUE)
#' }
#' @export
#' @noRd
get_odds_api_sports <- function(api_key = NULL, all = FALSE) {
  if (!is.logical(all) || length(all) != 1) {
    stop("Argument 'all' must be a single logical value (TRUE or FALSE).")
  }
  api_key <- get_odds_api_key(api_key)
  url <- "https://api.the-odds-api.com/v4/sports"
  result <- odds_api_get(
    url = url,
    query = list(
      apiKey = api_key,
      all = if (isTRUE(all)) "true" else NULL
    )
  )
  to_tibble(result)
}
