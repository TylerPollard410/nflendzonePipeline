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
should_rebuild <- function(
  source_df,
  prior_df,
  id_cols = c("season", "week", "game_id")
) {
  if (is.null(prior_df) || nrow(prior_df) == 0) {
    return(TRUE)
  }
  src_keys <- source_df |>
    dplyr::distinct(dplyr::across(dplyr::all_of(id_cols)))
  prior_keys <- prior_df |>
    dplyr::distinct(dplyr::across(dplyr::all_of(id_cols)))
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
#' @param archive_formats Character vector of formats to save in archive_dir (default: c("rds", "parquet", "csv"))
#' @return Invisibly TRUE
#' @export
#' @noRd
save_and_upload <- function(
  tag,
  full_data,
  seasons,
  repo,
  archive_dir,
  upload = TRUE,
  archive_formats = c("rds", "parquet", "csv")
) {
  initial_conn <- as.integer(rownames(showConnections(all = TRUE)))
  on.exit(
    {
      final_conn <- as.integer(rownames(showConnections(all = TRUE)))
      new_conn <- setdiff(final_conn, initial_conn)
      for (i in new_conn) {
        try(close(getConnection(i)), silent = TRUE)
      }
    },
    add = TRUE
  )

  dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)

  suppressWarnings({
    if ("rds" %in% archive_formats) {
      saveRDS(full_data, file.path(archive_dir, paste0(tag, ".rds")))
    }
    if ("csv" %in% archive_formats) {
      readr::write_csv(full_data, file.path(archive_dir, paste0(tag, ".csv")))
    }
    if ("parquet" %in% archive_formats) {
      arrow::write_parquet(
        full_data,
        file.path(archive_dir, paste0(tag, ".parquet"))
      )
    }
    ts_nflverse <- attr(full_data, "nflverse_timestamp")
    ts_txt <- if (is.null(ts_nflverse)) {
      # Assign the current time in New York time zone with label
      format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z", tz = "America/New_York")
    } else if (inherits(ts_nflverse, "POSIXt")) {
      format(ts_nflverse, "%Y-%m-%d %H:%M:%S %Z", tz = "America/New_York")
    } else {
      as.character(ts_nflverse)
    }
    ts_json <- paste0('{\n  "updated": "', ts_txt, '"\n}\n')
    writeLines(ts_txt, file.path(archive_dir, "timestamp.txt"))
    writeLines(ts_json, file.path(archive_dir, "timestamp.json"))
  })

  if (upload) {
    suppressWarnings({
      # Always try to upload all three types if they exist
      pb_files <- c(
        file.path(archive_dir, paste0(tag, ".rds")),
        file.path(archive_dir, paste0(tag, ".csv")),
        file.path(archive_dir, paste0(tag, ".parquet")),
        file.path(archive_dir, "timestamp.txt"),
        file.path(archive_dir, "timestamp.json")
      )
      purrr::walk(pb_files, \(f) {
        if (!file.exists(f)) {
          return(invisible(NULL))
        }
        tryCatch({
          piggyback::pb_upload(f, repo = repo, tag = tag, overwrite = TRUE)
          cli::cli_alert_success("Uploaded {basename(f)} for tag {tag}")
        }, error = function(e) {
          cli::cli_warn("Upload failed for {basename(f)}: {conditionMessage(e)}; continuing")
        })
      })
    })
    # Per-season files (leave as-is unless you want archive_formats control here too)
    season_files <- purrr::map(seasons, \(season) {
      season_df <- full_data |> dplyr::filter(season == !!season)
      pq_path <- file.path(tempdir(), paste0(tag, "_", season, ".parquet"))
      rds_path <- file.path(tempdir(), paste0(tag, "_", season, ".rds"))
      csv_path <- file.path(tempdir(), paste0(tag, "_", season, ".csv"))
      suppressWarnings({
        arrow::write_parquet(season_df, pq_path)
        saveRDS(season_df, rds_path)
        readr::write_csv(season_df, csv_path)
      })
      list(parquet = pq_path, rds = rds_path, csv = csv_path)
    })
    # Robust per-season upload with per-file checks and error handling
    purrr::walk(season_files, \(filelist) {
      purrr::walk(filelist, function(path) {
        if (is.null(path) || !nzchar(path) || !file.exists(path)) {
          cli::cli_warn("Skipping missing file: {path}")
          return(invisible(NULL))
        }

        tryCatch({
          piggyback::pb_upload(path, repo = repo, tag = tag, overwrite = TRUE)
          cli::cli_alert_success("Uploaded {basename(path)} for tag {tag}")
        }, error = function(e) {
          cli::cli_warn("Upload failed for {basename(path)}: {conditionMessage(e)}; continuing")
        })
      })

      # Best-effort cleanup; don't abort on cleanup failure
      tryCatch({
        unlink(c(filelist$parquet, filelist$rds, filelist$csv))
      }, error = function(e) {
        cli::cli_warn("Cleanup failed for season files: {conditionMessage(e)}")
      })
    })
    gc()
  }
  invisible(TRUE)
}


#' Safe wrapper for save_and_upload that logs errors rather than aborting
safe_save_and_upload <- function(...) {
  tryCatch({
    save_and_upload(...)
    TRUE
  }, error = function(e) {
    cli::cli_warn("save_and_upload failed: {conditionMessage(e)}")
    FALSE
  })
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
  sum_level <- tolower(spec$sum_level)
  stat_level <- tolower(spec$stat_level)
  season_level <- tolower(spec$season_level)
  ext <- tolower(ext)
  if (!season_level %in% c("reg", "post", "all")) {
    stop("season_level must be 'REG', 'POST', or 'ALL'")
  }
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
  if (season_level == "ALL") {
    return("REG+POST")
  }
  if (season_level == "REG") {
    return("REG")
  }
  if (season_level == "POST") {
    return("POST")
  }
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
  if (is.null(schedules)) {
    schedules <- nflreadr::load_schedules()
  }
  wide_map <- schedules |>
    dplyr::select(game_id, season, week, home_team, away_team, location) |>
    dplyr::mutate(
      home_team = clean_team_abbrs(home_team),
      away_team = clean_team_abbrs(away_team)
    )
  long_map <- nflreadr::clean_homeaway(wide_map)
  nms <- names(df)
  if (preserve_order) {
    df$.rowid <- seq_len(nrow(df))
  }
  `%has%` <- function(x, y) all(y %in% x)

  # Try matches in decreasing order of specificity
  if (nms %has% c("game_id", "home_team", "away_team")) {
    out <- dplyr::inner_join(
      wide_map,
      df,
      by = c("game_id", "home_team", "away_team"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("game_id", "home_team")) {
    out <- dplyr::inner_join(
      wide_map,
      df,
      by = c("game_id", "home_team"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("game_id", "away_team")) {
    out <- dplyr::inner_join(
      wide_map,
      df,
      by = c("game_id", "away_team"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("game_id", "team", "opponent")) {
    out <- dplyr::inner_join(
      long_map,
      df,
      by = c("game_id", "team", "opponent"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("game_id", "team")) {
    out <- dplyr::inner_join(
      long_map,
      df,
      by = c("game_id", "team"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("game_id", "opponent")) {
    out <- dplyr::inner_join(
      long_map,
      df,
      by = c("game_id", "opponent"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "home_team", "away_team")) {
    out <- dplyr::inner_join(
      wide_map,
      df,
      by = c("season", "week", "home_team", "away_team"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "home_team")) {
    out <- dplyr::inner_join(
      wide_map,
      df,
      by = c("season", "week", "home_team"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "away_team")) {
    out <- dplyr::inner_join(
      wide_map,
      df,
      by = c("season", "week", "away_team"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "team", "opponent")) {
    out <- dplyr::inner_join(
      long_map,
      df,
      by = c("season", "week", "team", "opponent"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "team")) {
    out <- dplyr::inner_join(
      long_map,
      df,
      by = c("season", "week", "team"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else if (nms %has% c("season", "week", "opponent")) {
    out <- dplyr::inner_join(
      long_map,
      df,
      by = c("season", "week", "opponent"),
      suffix = c("", ".y")
    ) |>
      dplyr::select(-dplyr::ends_with(".y"))
  } else {
    stop(
      "Input must have at least (game_id and one of: home_team, away_team, team, opponent) OR (season, week, and one of: home_team, away_team, team, opponent)"
    )
  }

  if (preserve_order && ".rowid" %in% names(out)) {
    out <- dplyr::arrange(out, .rowid) |> dplyr::select(-.rowid)
  }
  out
}

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
  if (length(var1_names) == 0 || length(var2_names) == 0) {
    return(df)
  }
  if (length(var1_names) != length(var2_names)) {
    stop("home/away columns found do not match 1:1. Check your regex patterns.")
  }

  net_names <- stringr::str_replace(
    var1_names,
    paste0("^", var1_prefix),
    net_prefix
  )

  net_df <- purrr::map2_dfc(
    var1_names,
    var2_names,
    ~ tibble::tibble(
      !!dplyr::sym(stringr::str_replace(
        .x,
        paste0("^", var1_prefix),
        net_prefix
      )) := fun(df[[.x]], df[[.y]])
    )
  )

  df2 <- dplyr::bind_cols(df, net_df)

  # Optional: relocate net columns after the corresponding away columns
  if (!order_by_team) {
    for (j in seq_along(net_names)) {
      df2 <- df2 |>
        dplyr::relocate(
          dplyr::all_of(net_names[j]),
          .after = dplyr::all_of(var2_names[j])
        )
    }
  }

  df2
}
