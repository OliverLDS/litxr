test_that("narrow-first exact-key lookup still returns hydrated rows", {
  td <- tempfile("litxr-narrow-first-")
  dir.create(td)
  config_path <- file.path(td, ".litxr", "config.yaml")

  old_litxr_config <- Sys.getenv("LITXR_CONFIG", unset = NA_character_)
  Sys.setenv(LITXR_CONFIG = config_path)
  on.exit({
    if (is.na(old_litxr_config)) {
      Sys.unsetenv("LITXR_CONFIG")
    } else {
      Sys.setenv(LITXR_CONFIG = old_litxr_config)
    }
  }, add = TRUE)

  litxr::litxr_init()
  cfg <- litxr::litxr_read_config()
  journal <- cfg$journals[[1]]
  journal_local_path <- litxr:::.litxr_resolve_local_path(cfg, journal$local_path)
  arxiv_collection <- Filter(
    function(collection) identical(collection$remote_channel, "arxiv"),
    cfg$collections
  )[[1]]
  arxiv_local_path <- litxr:::.litxr_resolve_local_path(cfg, arxiv_collection$local_path)

  make_record <- function(ref_id, source, source_id, title, collection_id, collection_title) {
    data.table::data.table(
      ref_id = ref_id,
      source = source,
      source_id = source_id,
      title = title,
      abstract = NA_character_,
      authors = "Jane Doe; John Smith",
      authors_list = list(c("Jane Doe", "John Smith")),
      pub_date = as.POSIXct("2025-01-03", tz = "UTC"),
      year = 2025L,
      month = 1L,
      day = 3L,
      journal = "Example Journal",
      container_title = "Example Journal",
      publisher = "Example Publisher",
      volume = "12",
      issue = "3",
      pages = "100-110",
      doi = NA_character_,
      note = NA_character_,
      subject_primary = "AI",
      subject_all = "AI",
      url_landing = NA_character_,
      url_pdf = NA_character_,
      arxiv_version = NA_integer_,
      arxiv_primary_category = NA_character_,
      arxiv_categories_raw = "",
      arxiv_comment = NA_character_,
      arxiv_journal_ref = NA_character_,
      raw_entry = list(NULL),
      collection_id = collection_id,
      collection_title = collection_title
    )
  }

  arxiv_record <- make_record(
    "arxiv:2501.00001",
    "arxiv",
    "2501.00001",
    "ArXiv Example Paper",
    arxiv_collection$collection_id,
    arxiv_collection$title
  )
  arxiv_record$abstract[[1]] <- "An example arXiv paper."
  arxiv_record$url_landing[[1]] <- "https://arxiv.org/abs/2501.00001v1"
  arxiv_record$url_pdf[[1]] <- "https://arxiv.org/pdf/2501.00001v1"
  arxiv_record$arxiv_version[[1]] <- 1L
  arxiv_record$arxiv_primary_category[[1]] <- "cs.AI"
  arxiv_record$arxiv_categories_raw[[1]] <- "cs.AI"

  second_arxiv_record <- make_record(
    "arxiv:2501.00002",
    "arxiv",
    "2501.00002",
    "ArXiv Example Paper Two",
    arxiv_collection$collection_id,
    arxiv_collection$title
  )
  second_arxiv_record$abstract[[1]] <- "A second example arXiv paper."
  second_arxiv_record$url_landing[[1]] <- "https://arxiv.org/abs/2501.00002v1"
  second_arxiv_record$url_pdf[[1]] <- "https://arxiv.org/pdf/2501.00002v1"
  second_arxiv_record$arxiv_version[[1]] <- 1L
  second_arxiv_record$arxiv_primary_category[[1]] <- "cs.AI"
  second_arxiv_record$arxiv_categories_raw[[1]] <- "cs.AI"

  litxr:::.litxr_write_journal_records(arxiv_record, arxiv_local_path, arxiv_collection, cfg = cfg)
  litxr:::.litxr_write_journal_records(second_arxiv_record, arxiv_local_path, arxiv_collection, cfg = cfg)

  linked_rows <- litxr::litxr_find_refs(ref_id = "arxiv:2501.00001", config = cfg)
  expect_equal(nrow(linked_rows), 1L)
  expect_true("authors_list" %in% names(linked_rows))
  expect_true("abstract" %in% names(linked_rows))
  expect_identical(linked_rows$ref_id[[1]], "arxiv:2501.00001")
})

test_that("wide projection aborts above the configured runtime limit", {
  td <- tempfile("litxr-wide-guard-")
  dir.create(td)
  config_path <- file.path(td, ".litxr", "config.yaml")

  old_litxr_config <- Sys.getenv("LITXR_CONFIG", unset = NA_character_)
  Sys.setenv(LITXR_CONFIG = config_path)
  on.exit({
    if (is.na(old_litxr_config)) {
      Sys.unsetenv("LITXR_CONFIG")
    } else {
      Sys.setenv(LITXR_CONFIG = old_litxr_config)
    }
  }, add = TRUE)

  litxr::litxr_init()
  cfg <- litxr::litxr_read_config()
  arxiv_collection <- Filter(
    function(collection) identical(collection$remote_channel, "arxiv"),
    cfg$collections
  )[[1]]

  expect_error(
    litxr:::.litxr_hydrate_project_projection_rows(
      cfg,
      data.table::data.table(ref_id = c("arxiv:2501.00001", "arxiv:2501.00002")),
      wide_projection_limit = 1L
    ),
    "wide-projection limit"
  )
})
