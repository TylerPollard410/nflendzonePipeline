# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# NEGATIVE BINOMIAL TEAM STRENGTH SUMMARY
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
#
# This script fits the bivariate negative-binomial team strength model and
# publishes the compact summary used by nflendzoneApp score simulations.

library(arrow)
library(dplyr)
library(nflfastR)
library(nflreadr)
library(nflseedR)
library(nflendzoneModel)
library(nflendzonePipeline)
library(piggyback)
library(purrr)
library(stringr)

set.seed(52)

# ============================================================================ #
# 0. Load Data ----
# ============================================================================ #

github_data_repo <- "TylerPollard410/nflendzoneData"
summary_tag <- "team_strength_negbinom_summary"

teams <- nflreadr::load_teams(current = TRUE)$team_abbr
all_seasons <- 2002:nflreadr::get_current_season()
current_season <- nflreadr::get_current_season()
current_week <- nflreadr::get_current_week()

game_data_full <- nflendzonePipeline::load_game_data(seasons = all_seasons)

# ============================================================================ #
# 1. Prepare Model Data ----
# ============================================================================ #

schedule_idx <- prepare_schedule_indices(
  game_data_full,
  teams
)

training_data <- game_data_full |>
  filter(season < current_season, !is.na(result))

fit_stan_data <- prepare_stan_data(
  game_data = training_data,
  teams = teams,
  verbose = TRUE
)

fit_stan_data <- roll_forward_fit_stan_data(
  fit_stan_data,
  schedule_idx,
  weeks_ahead = current_week - 1
)

gq_targets <- next_week_targets(fit_stan_data, horizon = 1L)
predict_week_idx <- gq_targets[[1]]
filter_week_idx <- fit_stan_data$N_weeks

filter_season <- schedule_idx |>
  filter(week_idx == filter_week_idx) |>
  pull(season) |>
  unique()
filter_week <- schedule_idx |>
  filter(week_idx == filter_week_idx) |>
  pull(week) |>
  unique()
predict_season <- schedule_idx |>
  filter(week_idx == predict_week_idx) |>
  pull(season) |>
  unique()
predict_week <- schedule_idx |>
  filter(week_idx == predict_week_idx) |>
  pull(week) |>
  unique()

if (
  length(filter_season) != 1L ||
    length(filter_week) != 1L ||
    length(predict_season) != 1L ||
    length(predict_week) != 1L
) {
  stop("Expected exactly one filtered and predicted season/week.", call. = FALSE)
}

# ============================================================================ #
# 2. Fit Negative Binomial Model ----
# ============================================================================ #

fit_seed <- 52
fit_sig_figs <- 10
fit_chains <- 4
fit_parallel <- parallel::detectCores()
fit_warm <- 1000
fit_samps <- 1000
fit_thin <- 1
fit_adapt_delta <- 0.95
fit_max_treedepth <- 10

cat("\n=== Fitting Negative Binomial Team Strength Model ===\n")

fit_bivar_negbinom <- fit_team_strength_model(
  stan_data = fit_stan_data,
  model = "team_strength_bivar_negbinom",
  seed = fit_seed,
  sig_figs = fit_sig_figs,
  chains = fit_chains,
  parallel_chains = fit_parallel,
  iter_warmup = fit_warm,
  iter_sampling = fit_samps,
  thin = fit_thin,
  adapt_delta = fit_adapt_delta,
  max_treedepth = fit_max_treedepth
)

# ============================================================================ #
# 3. Build App Summary ----
# ============================================================================ #

week_vars <- c(
  "phi_home",
  "phi_away",
  "filtered_alpha_log",
  "filtered_team_off_strength[team]",
  "filtered_team_def_strength[team]",
  "filtered_team_hfa[team]",
  "filtered_league_hfa",
  "predicted_alpha_log",
  "predicted_team_off_strength[team]",
  "predicted_team_def_strength[team]",
  "predicted_team_hfa[team]",
  "predicted_league_hfa"
)

nb_sum <- fit_bivar_negbinom$summary(
  variables = stringr::str_extract(week_vars, "^\\w+")
) |>
  mutate(
    team = stringr::str_extract(variable, "[:digit:]+"),
    .before = 1
  ) |>
  mutate(
    team = teams[as.numeric(team)],
    filtered_season = filter_season,
    filtered_week = filter_week,
    predicted_season = predict_season,
    predicted_week = predict_week,
    .before = 1
  )

# ============================================================================ #
# 4. Upload Release Assets ----
# ============================================================================ #

cat("\n=== Upload Negative Binomial Summary ===\n")

suppressWarnings(
  piggyback::pb_new_release(
    repo = github_data_repo,
    tag = summary_tag
  )
)

summary_files <- c(
  rds = file.path(tempdir(), paste0(summary_tag, "_", filter_season, ".rds")),
  parquet = file.path(tempdir(), paste0(summary_tag, "_", filter_season, ".parquet")),
  arrow = file.path(tempdir(), paste0(summary_tag, "_", filter_season, ".arrow"))
)

saveRDS(nb_sum, summary_files[["rds"]])
arrow::write_parquet(nb_sum, summary_files[["parquet"]])
arrow::write_feather(nb_sum, summary_files[["arrow"]])

purrr::walk(
  summary_files,
  function(path) {
    piggyback::pb_upload(
      file = path,
      name = basename(path),
      repo = github_data_repo,
      tag = summary_tag,
      overwrite = TRUE
    )
    unlink(path)
  }
)

cat(sprintf(
  "\nUploaded %s for filtered S%s W%s, predicted S%s W%s.\n",
  summary_tag,
  filter_season,
  filter_week,
  predict_season,
  predict_week
))

cat("\n=== Complete ===\n")
