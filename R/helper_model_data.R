# helper_model_data.R

#' Upload model output with timestamp to GitHub Releases
#'
#' Uploads a data object and corresponding timestamp to a GitHub release tag.
#' Creates weekly files with season/week identifiers and timestamp files.
#'
#' @param data Data object to upload (tibble, data.frame, etc.)
#' @param tag Release tag name
#' @param season Season number
#' @param week Week number
#' @param week_idx Week index
#' @param repo GitHub repo (username/repo)
#' @param overwrite Logical: overwrite existing files? (default: TRUE)
#' @return Invisibly TRUE
#' @export
#' @noRd
upload_model_output <- function(
  data,
  tag,
  season,
  week,
  week_idx,
  repo,
  overwrite = TRUE
) {
  # Create timestamp vector
  timestamp <- c(
    season = season,
    week = week,
    week_idx = week_idx
  )

  # Upload data file
  data_filename <- paste0(tag, "_", season, "_", week, ".rds")
  data_path <- file.path(tempdir(), data_filename)
  saveRDS(data, data_path)
  piggyback::pb_upload(data_path, repo = repo, tag = tag, overwrite = overwrite)
  unlink(data_path)

  # Upload timestamp.rds
  timestamp_rds <- paste0(tag, "_timestamp.rds")
  timestamp_rds_path <- file.path(tempdir(), timestamp_rds)
  saveRDS(timestamp, timestamp_rds_path)
  piggyback::pb_upload(
    timestamp_rds_path,
    repo = repo,
    tag = tag,
    overwrite = overwrite
  )
  unlink(timestamp_rds_path)

  # Upload timestamp.json (human-readable)
  timestamp_json <- paste0(tag, "_timestamp.json")
  timestamp_json_path <- file.path(tempdir(), timestamp_json)
  json_content <- sprintf(
    '{\n  "season": %d,\n  "week": %d,\n  "week_idx": %d\n}\n',
    season,
    week,
    week_idx
  )
  writeLines(json_content, timestamp_json_path)
  piggyback::pb_upload(
    timestamp_json_path,
    repo = repo,
    tag = tag,
    overwrite = overwrite
  )
  unlink(timestamp_json_path)

  cat(sprintf(
    "[%s] Uploaded S%d W%d (week_idx=%d)\n",
    tag,
    season,
    week,
    week_idx
  ))

  invisible(TRUE)
}

#' Upload CmdStan model output files to a GitHub release
#'
#' Saves timestamp sidecars (RDS/JSON) and optional metadata alongside
#' CmdStan output CSVs.
#'
#' @param files Character vector of CmdStan output file paths.
#' @param tag Release tag name.
#' @param repo GitHub repo (username/repo).
#' @param timestamp Named numeric/integer vector with season/week/week_idx.
#' @param metadata Optional list of metadata to serialize alongside the files.
#' @param overwrite Logical: overwrite existing files? (default: TRUE)
#' @param cleanup_files Logical: remove local files after upload? (default: TRUE)
#' @return Invisibly the metadata payload uploaded.
#' @export
#' @noRd
upload_cmdstan_outputs <- function(
  files,
  tag,
  repo,
  timestamp,
  metadata = NULL,
  overwrite = TRUE,
  cleanup_files = TRUE
) {
  suppressWarnings(piggyback::pb_new_release(repo = repo, tag = tag))

  files <- unlist(files, use.names = FALSE)

  if (length(files) == 0L) {
    stop("No files supplied for upload.", call. = FALSE)
  }

  if (!all(file.exists(files))) {
    missing_paths <- files[!file.exists(files)]
    stop(
      sprintf(
        "Missing CmdStan output files: %s",
        paste(basename(missing_paths), collapse = ", ")
      ),
      call. = FALSE
    )
  }

  required_timestamp_fields <- c("season", "week", "week_idx")
  if (!all(required_timestamp_fields %in% names(timestamp))) {
    stop(
      sprintf(
        "Timestamp must include named elements: %s",
        paste(required_timestamp_fields, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  purrr::walk(
    files,
    function(path) {
      piggyback::pb_upload(
        file = path,
        name = basename(path),
        repo = repo,
        tag = tag,
        overwrite = overwrite
      )
      if (cleanup_files) {
        unlink(path)
      }
    }
  )

  timestamp_rds <- paste0(tag, "_timestamp.rds")
  timestamp_rds_path <- file.path(tempdir(), timestamp_rds)
  saveRDS(timestamp, timestamp_rds_path)
  piggyback::pb_upload(
    timestamp_rds_path,
    repo = repo,
    tag = tag,
    overwrite = overwrite
  )
  unlink(timestamp_rds_path)

  timestamp_json <- paste0(tag, "_timestamp.json")
  timestamp_json_path <- file.path(tempdir(), timestamp_json)
  json_content <- sprintf(
    '{\n  "season": %d,\n  "week": %d,\n  "week_idx": %d\n}\n',
    timestamp[["season"]],
    timestamp[["week"]],
    timestamp[["week_idx"]]
  )
  writeLines(json_content, timestamp_json_path)
  piggyback::pb_upload(
    timestamp_json_path,
    repo = repo,
    tag = tag,
    overwrite = overwrite
  )
  unlink(timestamp_json_path)

  metadata_payload <- if (is.null(metadata)) list() else metadata
  if (!"timestamp" %in% names(metadata_payload)) {
    metadata_payload$timestamp <- as.list(timestamp)
  }
  if (!"files" %in% names(metadata_payload)) {
    metadata_payload$files <- basename(files)
  }

  metadata_path <- file.path(tempdir(), paste0(tag, "_metadata.rds"))
  saveRDS(metadata_payload, metadata_path)
  piggyback::pb_upload(
    metadata_path,
    repo = repo,
    tag = tag,
    overwrite = overwrite
  )
  unlink(metadata_path)

  cat(sprintf(
    "[%s] Uploaded season %s week %s (week_idx=%s)\n",
    tag,
    timestamp[["season"]],
    timestamp[["week"]],
    timestamp[["week_idx"]]
  ))

  invisible(metadata_payload)
}
