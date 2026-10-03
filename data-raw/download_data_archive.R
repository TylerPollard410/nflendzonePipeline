# download_data_archive.R -- run from pipeline repo root
#
# Refreshes the local artifacts/data-archive/ folder with the latest full
# archive files (.rds, .parquet, .csv, timestamp.txt/json) published to
# GitHub Releases in nflendzoneData by the Update EndZone Data workflow.
#
# artifacts/data-archive/ is not tracked in git, so `git pull` no longer
# updates it. Run this script for local analysis and tests. Only files that
# are newer on GitHub than the local copy are downloaded.
#
# This script is for local use only; the CI workflow restores its own archive.

# ============================================================================ #
# 1. LIBRARIES ----
# ============================================================================ #
library(piggyback)

# ============================================================================ #
# 2. GLOBAL VARIABLES ----
# ============================================================================ #
github_data_repo <- "TylerPollard410/nflendzoneData"
archive_root <- file.path("artifacts", "data-archive")

archive_tags <- c(
  "nfl_stats_week_team_regpost",
  "nfl_stats_week_player_regpost",
  "nfl_stats_season_team_regpost",
  "nfl_stats_season_player_regpost",
  "season_standings",
  "weekly_standings",
  "elo",
  "srs",
  "epa",
  "scores",
  "series",
  "turnover",
  "redzone",
  "team_features",
  "game_features",
  "team_model",
  "game_model",
  "historic_events"
)

# ============================================================================ #
# 3. DOWNLOAD ARCHIVE FILES ----
# ============================================================================ #
release_assets <- pb_list(repo = github_data_repo)

for (tag in archive_tags) {
  # Full archive files only (per-season files like elo_2025.rds are skipped),
  # matching the files save_and_upload() writes to the archive directory
  archive_pattern <- paste0(
    "^",
    tag,
    "\\.(rds|csv|parquet)$|^timestamp\\.(txt|json)$"
  )
  tag_files <- release_assets$file_name[
    release_assets$tag == tag &
      grepl(archive_pattern, release_assets$file_name)
  ]

  if (length(tag_files) == 0) {
    message(glue::glue("[{tag}] No archive files found in release, skipping."))
    next
  }

  dest <- file.path(archive_root, tag)
  dir.create(dest, recursive = TRUE, showWarnings = FALSE)

  message(glue::glue("[{tag}] Checking {length(tag_files)} files..."))
  pb_download(
    file = tag_files,
    dest = dest,
    repo = github_data_repo,
    tag = tag,
    overwrite = TRUE,
    use_timestamps = TRUE,
    show_progress = FALSE
  )
}

# ============================================================================ #
# 4. SUMMARY ----
# ============================================================================ #
timestamps <- vapply(
  archive_tags,
  \(tag) {
    ts_path <- file.path(archive_root, tag, "timestamp.txt")
    if (file.exists(ts_path)) readLines(ts_path, n = 1) else NA_character_
  },
  character(1)
)
print(data.frame(updated = timestamps))
