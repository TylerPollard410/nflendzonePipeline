#' Odds API Helpers
#'
#' Utilities for authentication and requests to the-odds-api.com.
#' Internal functions, not exported to package users.

#' Get The Odds API Key from Environment
#'
#' Retrieves your Odds API key from the argument, or from Sys.getenv("ODDS_API_KEY").
#' @param api_key Optional API key (character). If NULL, will look for Sys.getenv("ODDS_API_KEY").
#' @return A character string with the API key.
#' @keywords internal
#' @noRd
get_odds_api_key <- function(api_key = NULL) {
  if (is.null(api_key)) {
    api_key <- Sys.getenv("ODDS_API_KEY")
    if (identical(api_key, "")) stop("No Odds API key found. Provide api_key or set ODDS_API_KEY in .Renviron.")
  }
  api_key
}

#' Perform a GET Request to The Odds API and Parse the Response
#'
#' Makes a GET request and parses the JSON content.
#' @param url Full endpoint URL (character).
#' @param query Named list of query parameters (including apiKey).
#' @return List or data frame from parsed JSON.
#' @keywords internal
#' @noRd
odds_api_get <- function(url, query = list()) {
  tryCatch({
    resp <- httr2::request(url) |>
      httr2::req_url_query(!!!query) |>
      httr2::req_perform()
    httr2::resp_check_status(resp)
    jsonlite::fromJSON(httr2::resp_body_string(resp), flatten = TRUE)
  }, httr2_error = function(e) {
    stop(glue::glue("HTTP error: {e$message}"))
  }, error = function(e) {
    stop(glue::glue("Failed to get/parse Odds API response: {e$message}"))
  })
}

#' Convert to Tibble if Possible
#'
#' Tries to convert input to a tibble, otherwise returns unchanged.
#' @param x Any object.
#' @return Tibble if possible, otherwise original object.
#' @keywords internal
#' @noRd
to_tibble <- function(x) {
  if (is.data.frame(x)) tibble::as_tibble(x) else x
}
