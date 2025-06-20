# dev/01_start.R
#
# Initialization and infrastructure setup for nflendzonePipeline
# Run this script once at the start of your project

# ---- Package Infrastructure ----

usethis::use_description(fields = list(
  Title = "EndZone Analytics NFL Data Pipeline",
  Description = "Provides data processing, transformation, and ETL scripts for the EndZone Analytics ecosystem. Outputs ready-to-use datasets as Parquet files for downstream analysis and app consumption.",
  URL = "https://github.com/TylerPollard410/nflendzonePipeline",
  BugReports = "https://github.com/TylerPollard410/nflendzonePipeline/issues",
  Version = "0.1.0"
))
usethis::use_author(
  given = "Tyler", # Your First Name
  family = "Pollard", # Your Last Name
  email = "tylerpollard410@gmail.com", # Your email
  role = c("aut", "cre") # Your role (here author/creator)
)
usethis::use_mit_license("Tyler Pollard")

usethis::use_readme_rmd()
devtools::build_readme()
usethis::use_code_of_conduct(contact = "Tyler Pollard")
usethis::use_lifecycle_badge("Experimental")
devtools::build_readme()

usethis::use_news_md(open = FALSE)
usethis::use_testthat()

# ---- Print Done ----
message("nflendzonePipeline: Infrastructure setup complete!")

