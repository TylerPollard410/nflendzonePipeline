#' Get Odds for Games or Events from The Odds API
#'
#' Returns upcoming and live games with recent odds for a given sport, region and market.
#'
#' @param sport The sport key as obtained from [get_odds_api_sports()]. Use \code{"upcoming"} for all live and next 8 upcoming games across all sports.
#' @param api_key Optional. Odds API key. If NULL, will use Sys.getenv("ODDS_API_KEY").
#' @param regions Bookmaker regions to return. Valid values: \code{"us"}, \code{"us2"}, \code{"uk"}, \code{"au"}, \code{"eu"}. Default is \code{"us"}.
#' @param markets Odds markets to return. Default is \code{"h2h"} (moneyline). Also: \code{"spreads"}, \code{"totals"}, \code{"outrights"} (comma-separated allowed).
#' @param dateFormat Format for timestamps: \code{"iso"} (default) or \code{"unix"}.
#' @param oddsFormat Format for odds: \code{"decimal"} (default) or \code{"american"}.
#' @param eventIds Optional. Comma-separated game ids; filters the response to only return specified games.
#' @param bookmakers Optional. Comma-separated list of bookmakers to return.
#' @param commenceTimeFrom Optional. Filter to show games that commence on/after this ISO 8601 time.
#' @param commenceTimeTo Optional. Filter to show games that commence on/before this ISO 8601 time.
#' @param includeLinks Optional. If \code{"true"}, response includes bookmaker/event links.
#' @param includeSids Optional. If \code{"true"}, includes source IDs.
#' @param includeBetLimits Optional. If \code{"true"}, includes bet limits (mainly exchanges).
#' @param ... Reserved for future API parameters not yet documented.
#' @return A tibble containing odds and event metadata.
#' @examples
#' \dontrun{
#'   # NFL moneyline and spreads from US bookmakers:
#'   get_odds_api_odds(sport = "americanfootball_nfl", markets = "h2h,spreads")
#'
#'   # Filter for specific games
#'   get_odds_api_odds(
#'     sport = "americanfootball_nfl",
#'     eventIds = "e1,e2"
#'   )
#' }
#' @export
#' @noRd
get_odds_api_odds <- function(
    sport,
    api_key = NULL,
    regions = "us",
    markets = "h2h",
    dateFormat = "iso",
    oddsFormat = "decimal",
    eventIds = NULL,
    bookmakers = NULL,
    commenceTimeFrom = NULL,
    commenceTimeTo = NULL,
    includeLinks = NULL,
    includeSids = NULL,
    includeBetLimits = NULL,
    ...
) {
  if (missing(sport) || !is.character(sport) || length(sport) != 1)
    stop("You must supply a single sport key as a character string.")

  api_key <- get_odds_api_key(api_key)
  url <- glue::glue("https://api.the-odds-api.com/v4/sports/{sport}/odds")

  # Build the query list with all provided parameters
  query <- list(
    apiKey = api_key,
    regions = regions,
    markets = markets,
    dateFormat = dateFormat,
    oddsFormat = oddsFormat,
    eventIds = eventIds,
    bookmakers = bookmakers,
    commenceTimeFrom = commenceTimeFrom,
    commenceTimeTo = commenceTimeTo,
    includeLinks = includeLinks,
    includeSids = includeSids,
    includeBetLimits = includeBetLimits,
    ...
  )

  # Remove any NULLs to avoid sending as ?param=NULL
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

  # Stepwise unnest/rename after each unnest, so columns don't overwrite/collide
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
