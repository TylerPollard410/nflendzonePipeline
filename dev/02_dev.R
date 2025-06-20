# dev/02_dev.R
#
# Keep track of development for nflendzonePipeline

# ---- Dependencies ----

usethis::use_package("arrow")
usethis::use_package("dplyr")
usethis::use_package("future")
usethis::use_package("glue")
usethis::use_package("nflfastR")
usethis::use_package("nflreadr")
usethis::use_package("nflseedR")
usethis::use_package("piggyback")
usethis::use_package("progressr")
usethis::use_package("purrr")
usethis::use_package("readr")
usethis::use_package("slider")
usethis::use_package("stats")
usethis::use_package("stringr")
usethis::use_package("tidyr")
usethis::use_package("tidyselect")
usethis::use_package("duckdb")
usethis::use_package("duckdbfs")
usethis::use_package("DBI")
usethis::use_package("tibble")

# ---- Folders ----

usethis::use_directory("data-raw")   # For ETL scripts


