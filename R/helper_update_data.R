#' Check if data should be rebuilt based on new games
#'
#' Returns TRUE if any new rows in `source_df` are missing from `prior_df` by `id_cols`.
#'
#' @param source_df  The fresh data (e.g., game_data)
#' @param prior_df   The archive (e.g., elo_history)
#' @param id_cols    Vector of key columns for a unique game (default: c("season", "week", "game_id"))
#' @return Logical: TRUE if a rebuild is needed, FALSE otherwise
#' @export
#' @noRd
should_rebuild <- function(source_df, prior_df, id_cols = c("season", "week", "game_id")) {
  if (is.null(prior_df) || nrow(prior_df) == 0) return(TRUE)
  src_keys   <- source_df |> dplyr::distinct(dplyr::across(dplyr::all_of(id_cols)))
  prior_keys <- prior_df  |> dplyr::distinct(dplyr::across(dplyr::all_of(id_cols)))
  missing <- dplyr::anti_join(src_keys, prior_keys, by = id_cols)
  nrow(missing) > 0
}

#' Save and upload data artifacts (all formats, per-season, with timestamps)
#'
#' @param tag         Data tag (used as file/stem and release)
#' @param full_data   Full data frame to save (all seasons)
#' @param seasons     Vector of seasons to process for per-season files
#' @param repo        GitHub repo (username/repo, e.g., "user/repo")
#' @param archive_dir Local directory for archive
#' @param upload      Logical: upload to GitHub Releases? (default: TRUE)
#' @return Invisibly TRUE
#' @keywords internal
#' @noRd
save_and_upload <- function(
    tag, full_data, seasons, repo, archive_dir, upload = TRUE
) {
  dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
  suppressWarnings({
    saveRDS(full_data, file.path(archive_dir, paste0(tag, ".rds")))
    readr::write_csv(full_data, file.path(archive_dir, paste0(tag, ".csv")))
    arrow::write_parquet(full_data, file.path(archive_dir, paste0(tag, ".parquet")))
    ts_txt <- format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
    ts_json <- paste0('{\n  "updated": "', ts_txt, '"\n}\n')
    writeLines(ts_txt, file.path(archive_dir, "timestamp.txt"))
    writeLines(ts_json, file.path(archive_dir, "timestamp.json"))
  })
  if (upload) {
    suppressWarnings({
      piggyback::pb_upload(file.path(archive_dir, paste0(tag, ".rds")), repo = repo, tag = tag)
      piggyback::pb_upload(file.path(archive_dir, paste0(tag, ".csv")), repo = repo, tag = tag)
      piggyback::pb_upload(file.path(archive_dir, paste0(tag, ".parquet")), repo = repo, tag = tag)
      piggyback::pb_upload(file.path(archive_dir, "timestamp.txt"), repo = repo, tag = tag)
      piggyback::pb_upload(file.path(archive_dir, "timestamp.json"), repo = repo, tag = tag)
    })
    season_files <- purrr::map(seasons, \(season) {
      season_df <- full_data |> dplyr::filter(season == !!season)
      pq_path  <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
      rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
      csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
      suppressWarnings({
        arrow::write_parquet(season_df, pq_path)
        saveRDS(season_df, rds_path)
        readr::write_csv(season_df, csv_path)
      })
      list(parquet = pq_path, rds = rds_path, csv = csv_path)
    })
    purrr::walk(season_files, \(filelist) {
      suppressWarnings({
        piggyback::pb_upload(filelist$parquet, repo = repo, tag = tag)
        piggyback::pb_upload(filelist$rds,     repo = repo, tag = tag)
        piggyback::pb_upload(filelist$csv,     repo = repo, tag = tag)
      })
      unlink(c(filelist$parquet, filelist$rds, filelist$csv))
    })
    gc()
  }
  invisible(TRUE)
}


#' Build a canonical filename for cached stats/data (configurable directory)
#'
#' @param spec        List. Named list with sum_level, stat_level, and season_level.
#' @param ext         Character: file extension (default: "rds").
#' @param archive_dir Character: top-level directory (default: "artifacts/data-archive/nflverse_stats").
#' @return File path (character).
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

#' Map season_level to nflfastR's season_type argument
#'
#' @param season_level Character: one of "REG", "POST", or "ALL" (case-insensitive).
#' @return Character: "REG", "POST", or "REG+POST" for nflfastR
#' @export
#' @noRd
season_type_for_nflfastR <- function(season_level) {
  season_level <- toupper(season_level)
  if (season_level == "ALL") return("REG+POST")
  if (season_level == "REG") return("REG")
  if (season_level == "POST") return("POST")
  stop("season_level must be 'REG', 'POST', or 'ALL'")
}

#' Add canonical NFL id columns to a data frame, auto-matching by available keys
#'
#' Always returns all canonical columns (game_id, season, week, team, opponent, home_away, home_team, away_team, location)
#' with those columns appearing first in the output.
#'
#' @param df Data frame to add/merge id columns to
#' @param preserve_order Logical, if TRUE will keep the original row order (default FALSE)
#' @param schedules Optionally supply pre-loaded schedules for efficiency
#' @return Data frame with all canonical id columns and then original columns
#' @export
#' @noRd
add_nflverse_ids <- function(df, preserve_order = FALSE, schedules = NULL) {
  if (is.null(schedules)) schedules <- nflreadr::load_schedules()
  wide_map <- schedules |>
    dplyr::select(game_id, season, week, home_team, away_team, location) |>
    dplyr::mutate(
      home_team = clean_team_abbrs(home_team),
      away_team = clean_team_abbrs(away_team)
    )
  long_map <- nflreadr::clean_homeaway(wide_map)
  nms <- names(df)
  if (preserve_order) df$.rowid <- seq_len(nrow(df))
  `%has%` <- function(x, y) all(y %in% x)

  # Try matches in decreasing order of specificity
  if (nms %has% c("game_id", "home_team", "away_team")) {
    out <- dplyr::inner_join(wide_map, df, by = c("game_id", "home_team", "away_team"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("game_id", "home_team")) {
    out <- dplyr::inner_join(wide_map, df, by = c("game_id", "home_team"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("game_id", "away_team")) {
    out <- dplyr::inner_join(wide_map, df, by = c("game_id", "away_team"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("game_id", "team", "opponent")) {
    out <- dplyr::inner_join(long_map, df, by = c("game_id", "team", "opponent"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("game_id", "team")) {
    out <- dplyr::inner_join(long_map, df, by = c("game_id", "team"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("game_id", "opponent")) {
    out <- dplyr::inner_join(long_map, df, by = c("game_id", "opponent"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "home_team", "away_team")) {
    out <- dplyr::inner_join(wide_map, df, by = c("season", "week", "home_team", "away_team"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "home_team")) {
    out <- dplyr::inner_join(wide_map, df, by = c("season", "week", "home_team"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "away_team")) {
    out <- dplyr::inner_join(wide_map, df, by = c("season", "week", "away_team"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "team", "opponent")) {
    out <- dplyr::inner_join(long_map, df, by = c("season", "week", "team", "opponent"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "team")) {
    out <- dplyr::inner_join(long_map, df, by = c("season", "week", "team"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "opponent")) {
    out <- dplyr::inner_join(long_map, df, by = c("season", "week", "opponent"), suffix = c("", ".y")) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else {
    stop("Input must have at least (game_id and one of: home_team, away_team, team, opponent) OR (season, week, and one of: home_team, away_team, team, opponent)")
  }

  if (preserve_order && ".rowid" %in% names(out)) {
    out <- dplyr::arrange(out, .rowid) |> dplyr::select(-.rowid)
  }
  out
}
