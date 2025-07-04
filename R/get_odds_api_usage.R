#' Get Odds API Usage and Remaining Requests
#'
#' Checks your [the-odds-api.com](https://the-odds-api.com) usage and remaining request quota for your API key.
#'
#' @param api_key Optional. Odds API key. If NULL, will use Sys.getenv("ODDS_API_KEY").
#' @return A tibble with columns: \code{requests_remaining}, \code{requests_used}.
#' @examples
#' \dontrun{
#'   get_odds_api_usage()
#' }
#' @export
#' @noRd
get_odds_api_usage <- function(api_key = NULL) {
  api_key <- get_odds_api_key(api_key)
  url <- "https://api.the-odds-api.com/v4/sports"
  resp <- httr2::request(url) |>
    httr2::req_url_query(apiKey = api_key) |>
    httr2::req_perform()

  httr2::resp_check_status(resp)
  headers <- httr2::resp_headers(resp)

  tibble::tibble(
    requests_remaining = as.integer(headers[["x-requests-remaining"]]),
    requests_used      = as.integer(headers[["x-requests-used"]])
  )
}
