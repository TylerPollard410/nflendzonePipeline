# R/utils_clean_teamopponent.R

#' Reverse `nflreadr::clean_homeaway`: Pivot Long to Wide by Home/Away
#'
#' Given a data frame in “long” format (one row per team‐game, with a column
#' indicating home vs. away vs. neutral), this function will pivot back to one
#' row per game, prefixing each team‐column and any other team‐specific column
#' with `home_` or `away_`. For neutral‐site games (where both rows have
#' location == "neutral"), the first row in row‐order becomes "home_" and the
#' second becomes "away_".
#'
#' @param df A tibble or data frame in long format. Must contain:
#'   - A column (by default `game_id`) that uniquely identifies each game.
#'   - A column (by default `location`) containing at least two of:
#'     `"home"`, `"away"`, or `"neutral"` (case‐insensitive).
#'     For any `"neutral"` pair, we treat one as `home_…` and the other as `away_…`.
#'   - One column (by default `team`) identifying the team on that row.
#'   - Optionally, an `opponent` column (the opposite team), plus any number of
#'     other “team‐level” columns (scores, metrics, stats, etc.).
#' @param game_id Character name of the column that uniquely identifies a game
#'   (default: `"game_id"`).
#' @param location Character name of the column indicating `"home"`, `"away"`, or
#'   `"neutral"` (default: `"location"`). Values are matched case‐insensitively.
#' @param remove_opponent Logical; if `TRUE` (default), the resulting wide
#'   table will drop the automatic `home_opponent` and `away_opponent` columns
#'   (since they are redundant). If you need them, set this to `FALSE`.
#'
#' @return A data frame with one row per game. All columns except `game_id` and
#'   `location` are pivoted into two columns each: `home_<colname>` and
#'   `away_<colname>`. If `remove_opponent = TRUE`, the `home_opponent` and
#'   `away_opponent` columns are dropped before returning.
#'
#' @examples
#' library(tibble)
#' long_df <- tibble(
#'   game_id     = c(1, 1, 2,   2),
#'   location    = c("Home", "Away", "Neutral", "Neutral"),
#'   team        = c("NE", "MIA", "DAL",    "PHI"),
#'   opponent    = c("MIA", "NE", "PHI",    "DAL"),
#'   team_score  = c(21, 14,     28,       30),
#'   yards_gained = c(350, 275,   400,      320)
#' )
#' clean_teamopponent(long_df)
#'
#' @importFrom dplyr across all_of select
#' @importFrom tidyr pivot_wider
#' @export
#' @noRd
clean_teamopponent <- function(
  df,
  game_id = "game_id",
  location = "location",
  remove_opponent = TRUE
) {
  # 1) Basic sanity checks
  if (!is.data.frame(df)) {
    stop("`df` must be a data.frame or tibble.")
  }
  if (!all(c(game_id, location) %in% names(df))) {
    stop(
      "`df` must contain columns named '",
      game_id,
      "' and '",
      location,
      "'."
    )
  }
  # Preserve original attributes for later
  original_attrs <- attributes(df)

  # 2) Standardize location to lowercase
  df[[location]] <- tolower(as.character(df[[location]]))

  # 3) Ensure that each game_id has exactly two rows
  counts_per_game <- table(df[[game_id]])
  if (any(counts_per_game != 2)) {
    stop(
      "Each `",
      game_id,
      "` must appear in exactly two rows. ",
      "Found counts: ",
      paste(names(counts_per_game), counts_per_game, collapse = ", ")
    )
  }

  # 4) Check that after lowercasing, each location is one of "home", "away", "neutral"
  valid_locs <- c("home", "away", "neutral")
  if (!all(df[[location]] %in% valid_locs)) {
    bad <- unique(df[[location]][!df[[location]] %in% valid_locs])
    stop(
      "Invalid ",
      location,
      " values: ",
      paste(bad, collapse = ", "),
      ". Must be one of: ",
      paste(valid_locs, collapse = ", "),
      "."
    )
  }

  # 5) For each game_id, if both rows are "neutral", arbitrarily assign first → home, second → away
  df <- df |>
    dplyr::group_by(dplyr::across(dplyr::all_of(game_id))) |>
    dplyr::mutate(
      # If both rows have location == "neutral", convert them to home/away in order
      location2 = ifelse(
        all(location == "neutral"),
        c("home", "away"),
        location # otherwise keep original "home" or "away"
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-dplyr::all_of(location)) |>
    dplyr::rename(!!location := location2)

  # 6) Identify the “value” columns to pivot
  pivot_cols <- setdiff(names(df), c(game_id, location))

  # 7) Perform the pivot to wide form
  wide_df <- df |>
    tidyr::pivot_wider(
      id_cols = {{ game_id }},
      names_from = tidyselect::all_of(location),
      names_glue = "{location}_{.value}",
      values_from = tidyselect::all_of(pivot_cols),
      values_fill = list(.default = NA)
    )

  # 8) Optionally drop home_opponent / away_opponent if they exist
  if (remove_opponent) {
    drop_cols <- c("home_opponent", "away_opponent")
    drop_cols <- intersect(drop_cols, names(wide_df))
    if (length(drop_cols) > 0) {
      wide_df <- dplyr::select(wide_df, -dplyr::all_of(drop_cols))
    }
  }

  # 9) Restore original classes/attributes (other than column names, which have changed)
  #    We need to keep attributes like “nflverse_type” if present.
  core_attrs <- attributes(wide_df)[c(
    "names",
    "row.names",
    ".internal.selfref"
  )]
  new_attrs <- original_attrs[setdiff(
    names(original_attrs),
    c("names", "row.names", ".internal.selfref")
  )]
  attributes(wide_df) <- c(core_attrs, new_attrs)

  # 10) If there was an nflverse_type attribute, append "by game" to it
  if ("nflverse_type" %in% names(attributes(wide_df))) {
    attr(wide_df, "nflverse_type") <- paste(
      attr(wide_df, "nflverse_type"),
      "by game"
    )
  }

  wide_df
}


#' Clean NFL team abbreviations in a vector or data frame (exact matches only)
#'
#' A thin wrapper around [nflreadr::clean_team_abbrs()] that:
#' - If given a **vector** (character or factor), returns `clean_team_abbrs(x)`,
#'   preserving factor class.
#' - If given a **data frame** (or a plain named list of equal-length vectors),
#'   identifies columns that contain **exact team abbreviations** (any value
#'   equals a known team code, i.e., `%in%`), applies `clean_team_abbrs()` to
#'   those columns, and returns the same object type. Free-text columns where a
#'   team code appears only as a **substring** are **ignored**.
#'
#' An attribute `"cleaned_cols"` (character vector) is attached for data-frame
#' or list inputs, listing which columns were cleaned.
#'
#' @param x A character/factor vector, a data frame/tibble, or a named list of
#'   equal-length vectors.
#' @param verbose Logical; if `TRUE`, message which columns were cleaned for
#'   data-frame/list inputs. Default `FALSE`.
#'
#' @return
#' - Vector input: a cleaned vector (character or factor, matching input).
#' - Data frame/list input: the same class of object with cleaned columns; has
#'   attribute `"cleaned_cols"`.
#'
#' @examples
#' \dontrun{
#'   # Vector
#'   clean_team_abbrs_auto(c("STL", "OAK", "BAL"))
#'
#'   # Data frame
#'   pbp2 <- clean_team_abbrs_auto(pbp, verbose = TRUE)
#'   attr(pbp2, "cleaned_cols")
#' }
#' @importFrom nflreadr clean_team_abbrs
#' @export
#' @noRd
clean_team_abbrs_auto <- function(x, verbose = FALSE) {
  if (!requireNamespace("nflreadr", quietly = TRUE)) {
    stop("Package 'nflreadr' is required.")
  }

  #--- helper: clean a single vector; preserve factor class -------------------
  clean_vec <- function(v) {
    was_factor <- is.factor(v)
    v_chr <- as.character(v)
    v_out <- suppressWarnings(nflreadr::clean_team_abbrs(v_chr))
    if (was_factor) factor(v_out) else v_out
  }

  #--- build allowed set of raw team codes for `%in%` detection ---------------
  allowed <- try(
    {
      teams_df <- nflreadr::load_teams(current = TRUE)
      # primary: nflreadr mapping column (simple & robust)
      if (!"team_abbr" %in% names(teams_df)) {
        stop("no_col")
      }
      unique(stats::na.omit(as.character(teams_df$team_abbr)))
    },
    silent = TRUE
  )

  #--- vector path ------------------------------------------------------------
  if (is.character(x) || is.factor(x)) {
    return(clean_vec(x))
  }

  #--- data frame / list path -------------------------------------------------
  is_df <- is.data.frame(x)
  if (!(is_df || is.list(x))) {
    stop(
      "`x` must be a character/factor vector, a data.frame/tibble, or a named list."
    )
  }

  df <- if (is_df) {
    x
  } else {
    as.data.frame(x, stringsAsFactors = FALSE, optional = TRUE)
  }

  # exact-match detector: does the column contain ANY known team code?
  is_team_col <- vapply(
    df,
    function(col) {
      if (!(is.character(col) || is.factor(col))) {
        return(FALSE)
      }
      any(as.character(col) %in% allowed, na.rm = TRUE)
    },
    logical(1)
  )

  to_clean <- names(df)[is_team_col]

  if (length(to_clean)) {
    df[to_clean] <- lapply(df[to_clean], clean_vec)
    if (verbose) {
      message(
        "Cleaned team abbreviations in ",
        length(to_clean),
        " column(s): ",
        paste(to_clean, collapse = ", ")
      )
    }
  } else if (verbose) {
    message("No columns detected for cleaning.")
  }

  attr(df, "cleaned_cols") <- to_clean

  # return in original container type
  if (is_df) {
    df
  } else {
    # convert back to a named list with original names/order
    out <- as.list(df)
    names(out) <- names(df)
    out
  }
}


#' Add a Consecutive Week Sequence Across Seasons
#'
#' Given a data frame that contains `season` and `week` columns (e.g., `game_data` or
#' `game_data_long`), this function computes a sequential integer index (`week_seq`)
#' that increases by one for each distinct (season, week) pair in ascending order.
#' All rows sharing the same season/week receive the same `week_seq`.
#'
#' @param df A data frame or tibble containing at least two columns:
#'   - `season`: numeric or integer season identifier
#'   - `week`: numeric or integer week identifier within that season
#' @return The input `df`, with a new column `week_seq` appended.  `week_seq` is an
#'   integer that starts at 1 for the earliest season/week and increments by 1 for each
#'   subsequent distinct (season, week) pair sorted by `season` then `week`.
#'
#' @examples
#' library(dplyr)
#' # Example with game_data (one row per game)
#' game_data <- tibble(
#'   season = c(2020, 2020, 2020, 2021, 2021),
#'   week   = c(1,    2,    2,    1,    2),
#'   home_team = c("NE","BUF","MIA","DAL","PHI"),
#'   away_team = c("MIA","NE","BUF","PHI","DAL")
#' )
#' game_data_with_seq <- add_week_seq(game_data)
#' # Result: distinct (2020,1)->week_seq=1; (2020,2)->week_seq=2; (2021,1)->3; (2021,2)->4
#'
#' # Example with game_data_long (one row per team-game)
#' game_data_long <- tibble(
#'   season = c(2020,2020,2020,2020,2020,2021,2021,2021,2021),
#'   week   = c(1,    1,    2,    2,    2,    1,    1,    2,    2),
#'   team   = c("NE","MIA","BUF","MIA","NE","DAL","PHI","DAL","PHI")
#' )
#' game_data_long_with_seq <- add_week_seq(game_data_long)
#'
#' @importFrom dplyr distinct arrange mutate left_join row_number
#' @export
#' @noRd
add_week_seq <- function(df) {
  # 1) Extract distinct season/week combinations, sorted
  weeks_tbl <- df |>
    dplyr::distinct(season, week) |>
    dplyr::arrange(season, week) |>
    dplyr::mutate(week_seq = dplyr::row_number())

  # 2) Join back onto the original data frame
  df |>
    dplyr::left_join(weeks_tbl, by = c("season", "week")) |>
    dplyr::relocate(week_seq, .after = week)
}

#' Check whether two values represent the same number.
#'
#' Handles `NULL`, `NA`, and numeric-ish inputs (characters are coerced).
#' @export
#' @noRd
same_number <- function(x, y) {
  if (is.null(x) || is.null(y)) {
    return(FALSE)
  }
  if (length(x) == 0L || length(y) == 0L) {
    return(FALSE)
  }

  x <- x[[1]]
  y <- y[[1]]

  if (is.na(x) || is.na(y)) {
    return(is.na(x) && is.na(y))
  }

  isTRUE(all.equal(as.numeric(x), as.numeric(y)))
}

#' Convert probability to American odds (implied / fair odds).
#'
#' @param p Numeric vector of probabilities in 0, 1 (inclusive).
#' @param digits Integer; rounding for returned odds.
#' @return Integer vector (American odds); `NA` for invalid inputs.
#' @export
#' @noRd
prob_to_american_odds <- function(p, digits = 0L) {
  p <- as.numeric(p)

  out <- rep(NA_real_, length(p))
  ok <- is.finite(p) & !is.na(p) & p >= 0 & p <= 1

  out[ok & p == 1] <- -Inf
  out[ok & p == 0] <- Inf

  fav <- ok & p > 0 & p < 1 & p > 0.5
  dog <- ok & p > 0 & p < 1 & p < 0.5
  even <- ok & p == 0.5

  out[fav] <- -100 * (p[fav] / (1 - p[fav]))
  out[dog] <- 100 * ((1 - p[dog]) / p[dog])
  out[even] <- 100

  as.integer(round(out, digits = digits))
}
