test_that("parse_crossref_entry_unified preserves author name-only fallback", {
  message <- list(
    source = "zenodo",
    DOI = "10.5281/zenodo.18008872",
    title = "Agentic AI for Autonomous, Explainable, and Real-Time Credit Risk Decision-Making",
    author = list(list(name = "Kubam, Chandra Sekhar")),
    issued = list(`date-parts` = list(c(2024L, 12L, 28L))),
    `container-title` = "International Journal of Intelligent Systems and Applications in Engineering",
    publisher = "Zenodo",
    volume = "12",
    issue = "23s",
    page = "3669-3676",
    URL = "https://doi.org/10.5281/zenodo.18008872"
  )

  out <- litxr::parse_crossref_entry_unified(message)

  expect_identical(out$source[[1]], "zenodo")
  expect_identical(out$authors[[1]], "Kubam, Chandra Sekhar")
  expect_identical(out$authors_list[[1]], "Kubam, Chandra Sekhar")
  expect_identical(out$journal[[1]], "International Journal of Intelligent Systems and Applications in Engineering")
  expect_identical(out$volume[[1]], "12")
  expect_identical(out$issue[[1]], "23s")
  expect_identical(out$pages[[1]], "3669-3676")
})

test_that("invalid fallback container titles are routed to unclassified doi", {
  td <- tempfile("litxr-doi-config-")
  dir.create(td)
  config_path <- file.path(td, "config.yaml")
  cfg <- list(
    collections = list()
  )
  attr(cfg, "config_path") <- config_path
  attr(cfg, "config_root") <- td
  message <- list(
    source = "zenodo",
    DOI = "10.5281/zenodo.18008872",
    `container-title` = "23",
    publisher = "Zenodo",
    ISSN = character()
  )

  registered <- litxr:::.litxr_register_crossref_journal(cfg, message)

  expect_identical(registered$journal$collection_id, "unclassified_doi")
  expect_identical(registered$journal$title, "Unclassified DOI")
})
