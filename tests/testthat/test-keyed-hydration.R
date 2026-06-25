test_that("collection hydration handles missing, partial, and duplicate JSON candidates", {
  td <- tempfile("litxr-json-hydration-")
  dir.create(td)
  json_dir <- file.path(td, "json")
  dir.create(json_dir)

  complete_payload <- list(
    ref_id = "arxiv:2501.00001",
    doi = NULL,
    abstract = "Complete abstract",
    authors_list = list(c("Jane Doe", "John Smith"))
  )
  partial_payload <- list(
    ref_id = "arxiv:2501.00002",
    doi = NULL,
    abstract = "Partial abstract"
  )
  jsonlite::write_json(complete_payload, file.path(json_dir, "arxiv_2501_00001.json"), auto_unbox = TRUE, pretty = TRUE, null = "null")
  jsonlite::write_json(partial_payload, file.path(json_dir, "arxiv_2501_00002.json"), auto_unbox = TRUE, pretty = TRUE, null = "null")

  rows <- data.table::data.table(
    ref_id = c("arxiv:2501.00001", "arxiv:2501.00001", "arxiv:2501.00002", "arxiv:2501.00003"),
    doi = c(NA_character_, NA_character_, NA_character_, NA_character_)
  )
  hydrated <- litxr:::.litxr_hydrate_rows_from_json_dir(rows, json_dir)
  expect_equal(nrow(hydrated), 4L)
  expect_true("abstract" %in% names(hydrated))
  expect_true("authors_list" %in% names(hydrated))
  expect_identical(hydrated$abstract[[1]], "Complete abstract")
  expect_identical(hydrated$abstract[[2]], "Complete abstract")
  expect_identical(hydrated$abstract[[3]], "Partial abstract")
  expect_true(is.na(hydrated$abstract[[4]]))
  expect_identical(hydrated$authors_list[[1]], c("Jane Doe", "John Smith"))
  expect_identical(hydrated$authors_list[[2]], c("Jane Doe", "John Smith"))
  expect_true(is.null(hydrated$authors_list[[3]]) || length(hydrated$authors_list[[3]]) == 0L)
})

test_that("collection hydration tolerates duplicate slug files and prefers the first match", {
  td <- tempfile("litxr-dup-slug-files-")
  dir.create(td)
  json_dir <- file.path(td, "json")
  alt_dir <- file.path(td, "alt")
  dir.create(json_dir)
  dir.create(alt_dir)

  path_a <- file.path(json_dir, "arxiv_2501_00001.json")
  path_b <- file.path(alt_dir, "arxiv_2501_00001.json")
  jsonlite::write_json(
    list(
      ref_id = "arxiv:2501.00001",
      doi = NULL,
      abstract = "First duplicate slug file",
      authors_list = list(c("Jane Doe"))
    ),
    path_a,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )
  jsonlite::write_json(
    list(
      ref_id = "arxiv:2501.00001",
      doi = NULL,
      abstract = "Second duplicate slug file",
      authors_list = list(c("John Smith"))
    ),
    path_b,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )

  rows <- data.table::data.table(
    ref_id = "arxiv:2501.00001",
    doi = NA_character_
  )
  hydrated <- testthat::with_mocked_bindings(
    list.files = function(...) c(path_a, path_b),
    litxr:::.litxr_hydrate_rows_from_json_dir(rows, json_dir),
    .package = "base"
  )

  expect_equal(nrow(hydrated), 1L)
  expect_identical(hydrated$abstract[[1]], "First duplicate slug file")
  expect_identical(hydrated$authors_list[[1]], "Jane Doe")
})

test_that("keyed journal lookup batches large key sets and deduplicates duplicate canonical ids", {
  td <- tempfile("litxr-keyed-read-")
  dir.create(td)
  config_path <- file.path(td, "config.yaml")

  old_litxr_config <- Sys.getenv("LITXR_DATA_ROOT", unset = NA_character_)
  Sys.setenv(LITXR_DATA_ROOT = dirname(config_path))
  on.exit({
    if (is.na(old_litxr_config)) {
      Sys.unsetenv("LITXR_DATA_ROOT")
    } else {
      Sys.setenv(LITXR_DATA_ROOT = old_litxr_config)
    }
  }, add = TRUE)

  litxr::litxr_init()
  cfg <- litxr::litxr_read_config()
  journal <- cfg$journals[[1]]
  local_path <- litxr:::.litxr_resolve_local_path(cfg, journal$local_path)

  make_record <- function(ref_id, title) {
    data.table::data.table(
      ref_id = ref_id,
      source = "arxiv",
      source_id = sub("^arxiv:", "", ref_id),
      title = title,
      abstract = sprintf("Abstract for %s", ref_id),
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
      url_landing = "https://arxiv.org/abs/2501.00001v1",
      url_pdf = "https://arxiv.org/pdf/2501.00001v1",
      arxiv_version = 1L,
      arxiv_primary_category = "cs.AI",
      arxiv_categories_raw = "cs.AI",
      arxiv_comment = NA_character_,
      arxiv_journal_ref = NA_character_,
      raw_entry = list(NULL),
      collection_id = journal$journal_id,
      collection_title = journal$title
    )
  }

  record_1 <- make_record("arxiv:2501.00001", "Keyed Lookup Paper")
  record_2 <- make_record("arxiv:2501.00002", "Keyed Lookup Paper Two")
  litxr:::.litxr_write_journal_records(data.table::rbindlist(list(record_1, record_2), fill = TRUE), local_path, journal, cfg = cfg)

  looked_up <- litxr:::.litxr_read_journal_records_by_keys(
    local_path,
    c("arxiv:2501.00001", "arxiv:2501.00001", "arxiv:2501.00002"),
    keyed_fst_read_threshold = 1L
  )
  expect_equal(nrow(looked_up), 2L)
  expect_true("authors_list" %in% names(looked_up))
  expect_identical(sort(as.character(looked_up$ref_id)), c("arxiv:2501.00001", "arxiv:2501.00002"))
})

test_that("project rows linked to multiple collections keep collection ids and wide projection guardrails", {
  td <- tempfile("litxr-multi-collection-")
  dir.create(td)
  config_path <- file.path(td, "config.yaml")

  old_litxr_config <- Sys.getenv("LITXR_DATA_ROOT", unset = NA_character_)
  Sys.setenv(LITXR_DATA_ROOT = dirname(config_path))
  on.exit({
    if (is.na(old_litxr_config)) {
      Sys.unsetenv("LITXR_DATA_ROOT")
    } else {
      Sys.setenv(LITXR_DATA_ROOT = old_litxr_config)
    }
  }, add = TRUE)

  litxr::litxr_init()
  cfg <- litxr::litxr_read_config()
  journal <- cfg$journals[[1]]
  arxiv_collection <- Filter(function(collection) identical(collection$remote_channel, "arxiv"), cfg$collections)[[1]]
  journal_path <- litxr:::.litxr_resolve_local_path(cfg, journal$local_path)
  arxiv_path <- litxr:::.litxr_resolve_local_path(cfg, arxiv_collection$local_path)

  shared_record <- data.table::data.table(
    ref_id = "arxiv:2501.99999",
    source = "arxiv",
    source_id = "2501.99999",
    title = "Shared Record",
    abstract = "Shared abstract",
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
    url_landing = "https://arxiv.org/abs/2501.99999v1",
    url_pdf = "https://arxiv.org/pdf/2501.99999v1",
    arxiv_version = 1L,
    arxiv_primary_category = "cs.AI",
    arxiv_categories_raw = "cs.AI",
    arxiv_comment = NA_character_,
    arxiv_journal_ref = NA_character_,
    raw_entry = list(NULL),
    collection_id = journal$journal_id,
    collection_title = journal$title
  )
  shared_record_arxiv <- data.table::copy(shared_record)
  shared_record_arxiv$collection_id[[1]] <- arxiv_collection$collection_id
  shared_record_arxiv$collection_title[[1]] <- arxiv_collection$title

  litxr:::.litxr_write_journal_records(shared_record, journal_path, journal, cfg = cfg)
  litxr:::.litxr_write_journal_records(shared_record_arxiv, arxiv_path, arxiv_collection, cfg = cfg)

  status <- litxr::litxr_read_research_schema_status(cfg, ref_ids = "arxiv:2501.99999")
  expect_equal(nrow(status), 1L)
  expect_true(grepl(journal$journal_id, status$collection_ids[[1]], fixed = TRUE))
  expect_true(grepl(arxiv_collection$collection_id, status$collection_ids[[1]], fixed = TRUE))

  expect_error(
    litxr:::.litxr_hydrate_project_projection_rows(
      cfg,
      data.table::data.table(ref_id = c("arxiv:2501.99998", "arxiv:2501.99999")),
      wide_projection_limit = 1L
    ),
    "wide-projection limit"
  )
})
