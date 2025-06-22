#' Aggregate Feature Data Frames into a Long Format
#'
#' This function binds together multiple feature-level data frames into a single wide-format table
#' based on a character vector of feature names. Each name corresponds to a saved file (e.g., `.rds`)
#' in a specified archive directory.
#'
#' @param features Character vector of feature dataset names (e.g., "elo", "srs", "epa", etc.).
#' @param archive_loc Directory path where feature files are stored (default = "archive/data-archive").
#' @param file_type File extension (without dot), e.g., "rds" or "qs" (default = "rds").
#' @return A single data frame with all features bound together.
#' @export
#' @noRd
compute_feature_long <- function(
    features,
    archive_loc = "archive/data-archive",
    file_type = "rds"
) {
  stopifnot(is.character(features), is.character(archive_loc), is.character(file_type))

  feature_data <- map(features, \(x) readRDS(paste0("artifacts/data-archive/", x, "/", paste0(x, ".rds"))))
  # purrr::iwalk(feature_data, \(df, name) {
  #   cat(glue::glue("---- Columns in '{name}' ----\n"))
  #   print(colnames(df))
  #   cat("\n")
  # })

  feature_list_with_ids <- purrr::imap(feature_data, \(df, name) {
    message(glue::glue("Adding nflverse IDs to '{name}'..."))
    add_nflverse_ids(df)
  })
  # purrr::iwalk(feature_list_with_ids, \(df, name) {
  #   cat(glue::glue("---- Columns in '{name}' ----\n"))
  #   print(colnames(df))
  #   cat("\n")
  # })

  # Step 1: Apply clean_homeaway() to first element
  feature_list_with_ids[[1]] <- nflreadr::clean_homeaway(feature_list_with_ids[[1]])

  # Step 2: Join the rest to the cleaned first
  joined_df <- purrr::reduce(
    feature_list_with_ids[-1],  # exclude the first element
    .init = feature_list_with_ids[[1]],  # start with cleaned first df
    ~ left_join(.x, .y, by = c("game_id", "season", "week", "team", "location"))
  ) |>
    dplyr::select(-contains("."))

  return(joined_df)
}
