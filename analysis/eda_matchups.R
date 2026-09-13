## Exploratory code to look for edges
# Tyler Pollard
# 9 Apr 2026

# libraries ----
library(lubridate)
library(stringr)
library(dplyr)
library(purrr)
library(tidyr)
library(ggplot2)
#library(tidyverse)

library(glue)
library(tictoc)

library(nflplotR)
library(nflreadr)
library(nflfastR)
library(nflseedR)

library(nflendzoneModel)
library(nflendzonePipeline)

# read-in-data ----
archive_dir <- file.path("artifacts", "data-archive")
archive_names <- list.files(archive_dir)

# walk through entire archive dir reading the rds file within
archive_names |>
  set_names() |>
  walk(\(x) {
    file_name <- paste0(x, ".rds")
    file_path <- file.path(archive_dir, x, file_name)
    data <- readRDS(file = file_path)
    assign(x, data, envir = .GlobalEnv)
    message(glue("Read in {x} data ✅"))
  })

id_cols <- c(
  "game_id",
  "season",
  "week",
  "team",
  "opponent",
  "location",
  "player_id",
  "player_name",
  "player_display_name",
  "position",
  "position_group",
  "headshot_url",
  "season_type"
)

rb_stats <- nfl_stats_week_player_regpost |>
  filter(position_group %in% c("RB", "QB")) |>
  select(
    all_of(id_cols),
    carries,
    contains("rushing"),
    contains("fantasy")
  )
rb_stats |> glimpse()

rb_stats |>
  filter(carries > 0, season >= get_current_season()) |>
  ggplot(aes(x = rushing_yards)) +
  geom_bar() +
  geom_area() +
  facet_wrap(~position) +
  theme_minimal()
