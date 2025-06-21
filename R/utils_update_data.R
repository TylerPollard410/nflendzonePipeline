#' Should Rebuild If New Games Exist
#'
#' @param source_df  The fresh data (e.g., game_data)
#' @param prior_df   The archive (e.g., elo_history)
#' @param id_cols    Vector of key columns for a unique game (e.g., c("season", "week", "game_id"))
#' @return TRUE if any new rows in source_df are missing from prior_df by id_cols.
#' @export
#' @noRd
should_rebuild <- function(source_df, prior_df, id_cols = c("season", "week", "game_id")) {
  if (is.null(prior_df) || nrow(prior_df) == 0) return(TRUE)
  # Keep only id_cols in both
  src_keys   <- source_df |> dplyr::distinct(dplyr::across(dplyr::all_of(id_cols)))
  prior_keys <- prior_df  |> dplyr::distinct(dplyr::across(dplyr::all_of(id_cols)))
  # See if any new rows in source not present in prior
  missing <- dplyr::anti_join(src_keys, prior_keys, by = id_cols)
  nrow(missing) > 0
}



#' Save and upload pipeline data to disk and GitHub
#'
#' @param tag         Data tag (used as file/stem)
#' @param full_data   Full data frame to save
#' @param seasons     Vector of seasons to process for per-season files
#' @param repo        GitHub repo (username/repo)
#' @param archive_dir Local directory for archive
#' @param upload      Logical: upload to GitHub Releases?
#' @return Invisibly TRUE
#' @keywords internal
save_and_upload <- function(
    tag, full_data, seasons, repo, archive_dir, upload = TRUE
) {
  dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
  saveRDS(full_data, file.path(archive_dir, paste0(tag, ".rds")))
  readr::write_csv(full_data, file.path(archive_dir, paste0(tag, ".csv")))
  arrow::write_parquet(full_data, file.path(archive_dir, paste0(tag, ".parquet")))
  ts_txt <- format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
  ts_json <- paste0('{\n  "updated": "', ts_txt, '"\n}\n')
  writeLines(ts_txt, file.path(archive_dir, "timestamp.txt"))
  writeLines(ts_json, file.path(archive_dir, "timestamp.json"))
  if (upload) {
    piggyback::pb_upload(file.path(archive_dir, paste0(tag, ".rds")), repo = repo, tag = tag)
    piggyback::pb_upload(file.path(archive_dir, paste0(tag, ".csv")), repo = repo, tag = tag)
    piggyback::pb_upload(file.path(archive_dir, paste0(tag, ".parquet")), repo = repo, tag = tag)
    piggyback::pb_upload(file.path(archive_dir, "timestamp.txt"), repo = repo, tag = tag)
    piggyback::pb_upload(file.path(archive_dir, "timestamp.json"), repo = repo, tag = tag)
    # Per-season files
    season_files <- purrr::map(seasons, \(season) {
      season_df <- full_data |> dplyr::filter(season == !!season)
      pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
      rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
      csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
      arrow::write_parquet(season_df, pq_path)
      saveRDS(season_df, rds_path)
      readr::write_csv(season_df, csv_path)
      list(parquet = pq_path, rds = rds_path, csv = csv_path)
    })
    purrr::walk(season_files, \(filelist) {
      piggyback::pb_upload(filelist$parquet, repo = repo, tag = tag)
      piggyback::pb_upload(filelist$rds,     repo = repo, tag = tag)
      piggyback::pb_upload(filelist$csv,     repo = repo, tag = tag)
    })
  }
  invisible(TRUE)
}


#' Build a canonical filename for cached stats/data (configurable directory)
#'
#' @param spec        List. A named list containing sum_level, stat_level, and season_level.
#' @param ext         Character: file extension (default: "rds").
#' @param archive_dir Character: top-level directory (default: "artifacts/data-archive/nflverse_stats").
#' @return            File path (character).
#' @export
#' @noRd
make_stats_filename <- function(
    spec,
    ext = "rds",
    archive_dir = "artifacts/data-archive/nflverse_stats"
) {
  sum_level    <- tolower(spec$sum_level)
  stat_level   <- tolower(spec$stat_level)
  season_level <- tolower(spec$season_level)
  ext          <- tolower(ext)
  if (!season_level %in% c("reg", "post", "all")) stop("season_level must be 'REG', 'POST', or 'ALL'")
  fn <- paste(sum_level, stat_level, season_level, "stats", sep = "_")
  file.path(archive_dir, paste0(fn, ".", ext))
}



#' Map season_level to nflfastR's season_type
#'
#' Converts a user-friendly season_level input ("REG", "POST", "ALL") to the format expected by
#' nflfastR::calculate_stats() ("REG", "POST", or "REG+POST").
#'
#' @param season_level Character. One of "REG", "POST", or "ALL" (case-insensitive).
#' @return Character. "REG", "POST", or "REG+POST" as needed for nflfastR.
#' @export
#' @noRd
season_type_for_nflfastR <- function(season_level) {
  season_level <- toupper(season_level)
  if (season_level == "ALL") return("REG+POST")
  if (season_level == "REG") return("REG")
  if (season_level == "POST") return("POST")
  stop("season_level must be 'REG', 'POST', or 'ALL'")
}

