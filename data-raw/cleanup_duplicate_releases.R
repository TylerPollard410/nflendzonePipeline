# cleanup_duplicate_releases.R
#
# This script deletes ALL duplicate GitHub releases (both copies) so the
# pipeline can be rerun with full_build = TRUE to create clean releases.

# ============================================================================ #
# SETUP ----
# ============================================================================ #
library(piggyback)
library(dplyr)

github_data_repo <- "TylerPollard410/nflendzoneData"

# ============================================================================ #
# IDENTIFY DUPLICATES ----
# ============================================================================ #

# Get all releases
releases <- pb_releases(repo = github_data_repo)

# Find ALL releases that have duplicates (both copies)
all_duplicates <- releases |>
  group_by(tag_name) |>
  filter(n() > 1) |>
  arrange(tag_name, created_at)

cat("\n========================================\n")
cat("DUPLICATE RELEASES FOUND (ALL COPIES):\n")
cat("========================================\n")
print(
  all_duplicates |> select(tag_name, release_id, created_at, n_assets),
  n = nrow(all_duplicates)
)

cat("\n========================================\n")
cat("READY TO DELETE ALL ", nrow(all_duplicates), " DUPLICATE RELEASES\n")
cat("========================================\n")
cat("\nThis will delete ALL copies of duplicate releases.\n")
cat("You will need to rerun the pipeline with full_build = TRUE.\n\n")

response <- readline(prompt = "Type 'YES' to proceed with deletion: ")

if (toupper(response) == "YES") {
  cat("\nDeleting ALL duplicate releases...\n\n")

  for (i in 1:nrow(all_duplicates)) {
    tag <- all_duplicates$tag_name[i]
    release_id <- all_duplicates$release_id[i]

    cat(sprintf("Deleting release: %s (ID: %s)...", tag, release_id))

    tryCatch(
      {
        pb_release_delete(
          repo = github_data_repo,
          tag = tag
        )
        cat(" ✓ SUCCESS\n")
      },
      error = function(e) {
        cat(" ✗ FAILED\n")
        cat(sprintf("  Error: %s\n", conditionMessage(e)))
      }
    )

    # Add a small delay to avoid rate limiting
    Sys.sleep(0.5)
  }

  cat("\n========================================\n")
  cat("CLEANUP COMPLETE!\n")
  cat("========================================\n")

  # Verify cleanup
  cat("\nVerifying cleanup...\n")
  releases_after <- pb_releases(repo = github_data_repo)
  remaining_duplicates <- releases_after |>
    group_by(tag_name) |>
    filter(n() > 1)

  if (nrow(remaining_duplicates) == 0) {
    cat("✓ All duplicates have been removed!\n")
    cat("\nNext steps:\n")
    cat(
      "1. Install dev piggyback: remotes::install_github('ropensci/piggyback')\n"
    )
    cat(
      "2. Reinstall package: install.packages('.', repos = NULL, type = 'source')\n"
    )
    cat("3. Set full_build = TRUE in update_all_data.R (line 53)\n")
    cat("4. Run: source('data-raw/update_all_data.R')\n")
  } else {
    cat("⚠ Some duplicates still remain:\n")
    print(remaining_duplicates |> select(tag_name, release_id, created_at))
  }
} else {
  cat("\nDeletion cancelled. No releases were deleted.\n")
}

cat("\n")
