# helper_model_data.R

#' Add Flexible Net Features to a Data Frame
#'
#' Computes "net" features by applying a function (e.g., subtraction) to matched pairs of columns with
#' configurable prefixes (e.g., home/away stats) and optional pattern matching. The new columns are named
#' with a user-specified prefix (default: \code{"net_"}).
#'
#' @param df Data frame containing columns to combine (typically game-level or matchup-level features).
#' @param var1_prefix Character prefix for the first group of columns (default: "home_").
#' @param var2_prefix Character prefix for the second group of columns (default: "away_").
#' @param pattern1 Regex pattern (without prefix) to match variable names after \code{var1_prefix} (default: ".*").
#' @param pattern2 Regex pattern (without prefix) to match variable names after \code{var2_prefix} (default: ".*").
#' @param fun Binary function to combine column pairs (default: \code{-}, i.e., subtraction).
#' @param net_prefix Character prefix for new net features (default: "net_").
#'
#' @return A tibble (data.frame) containing the original columns plus new net features for each matched pair.
#'
#' @details
#' Only columns with matching names (after removing the prefix and applying the pattern) will be combined.
#' If columns do not match 1:1, the function will error. If no columns match, the original data is returned unchanged.
#'
#' @examples
#' \dontrun{
#' df_with_nets <- add_flexible_net_features(df)
#' }
#'
#' @importFrom stringr str_which str_replace
#' @importFrom purrr map2_dfc
#' @importFrom tibble tibble
#' @importFrom dplyr bind_cols
#' @export
#' @noRd
add_flexible_net_features <- function(
    df,
    var1_prefix = "home_",
    var2_prefix = "away_",
    pattern1 = ".*",
    pattern2 = ".*",
    fun = `-`,
    net_prefix = "net_",
    order_by_team = TRUE
) {
  regex1 <- paste0("^", var1_prefix, "(", pattern1, ")(?=_|$|\\b)")
  regex2 <- paste0("^", var2_prefix, "(", pattern2, ")(?=_|$|\\b)")
  var1_idx <- stringr::str_which(names(df), regex1)
  var2_idx <- stringr::str_which(names(df), regex2)
  var1_names <- names(df)[var1_idx]
  var2_names <- names(df)[var2_idx]

  # Must be same length and matching order!
  if (length(var1_names) == 0 || length(var2_names) == 0) return(df)
  if (length(var1_names) != length(var2_names)) {
    stop("home/away columns found do not match 1:1. Check your regex patterns.")
  }

  net_names <- stringr::str_replace(var1_names, paste0("^", var1_prefix), net_prefix)

  net_df <- purrr::map2_dfc(var1_names, var2_names, ~tibble::tibble(
    !!dplyr::sym(stringr::str_replace(.x, paste0("^", var1_prefix), net_prefix)) := fun(df[[.x]], df[[.y]])
  ))

  df2 <- dplyr::bind_cols(df, net_df)

  # Optional: relocate net columns after the corresponding away columns
  if(!order_by_team) {
    for (j in seq_along(net_names)) {
      df2 <- df2 |> dplyr::relocate(dplyr::all_of(net_names[j]), .after = dplyr::all_of(var2_names[j]))
    }
  }

  df2
}
