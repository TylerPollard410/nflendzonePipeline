golem::detach_all_attached()
packrat::clean()

devtools::document() # builds NAMESPACE/man once

attachment::att_amend_desc(
  document = FALSE, # don't run roxygen again
  use.config = FALSE, # deterministic, no hidden yaml overrides
  dir.r = "R",
  dir.v = "vignettes",
  dir.t = "tests",
  normalize = TRUE
)

# local/dev check (fast):
devtools::check(
  document = FALSE, # you already ran document()
  cran = FALSE, # faster local check
  manual = FALSE,
  vignettes = FALSE, # skip heavy Quarto vignette build locally
  force_suggests = FALSE,
  run_dont_test = FALSE,
  args = "--timings",
  error_on = "warning"
)

# full/pre-release check:
devtools::check(
  document = FALSE,
  cran = TRUE,
  manual = FALSE,
  vignettes = TRUE,
  force_suggests = TRUE,
  run_dont_test = TRUE,
  args = c("--as-cran", "--timings"),
  error_on = "warning"
)
