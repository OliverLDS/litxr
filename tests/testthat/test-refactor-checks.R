run_json_script <- function(script_path, args = character(), config_path = NULL) {
  stderr_path <- tempfile("litxr-script-stderr-", fileext = ".log")
  on.exit(unlink(stderr_path), add = TRUE)
  env <- character()
  if (!is.null(config_path)) {
    env <- c(env, paste0("LITXR_CONFIG=", config_path))
  }
  out <- system2(
    "Rscript",
    c(script_path, args),
    stdout = TRUE,
    stderr = stderr_path,
    env = env
  )
  status <- attr(out, "status")
  if (!is.null(status) && !identical(status, 0L)) {
    stop(
      "Script failed (status=", status, "): ",
      paste(readLines(stderr_path, warn = FALSE), collapse = "\n"),
      call. = FALSE
    )
  }
  jsonlite::fromJSON(paste(out, collapse = "\n"), simplifyVector = TRUE)
}

make_record <- function(ref_id, source, source_id, title, collection_id, collection_title) {
  data.table::data.table(
    ref_id = ref_id,
    source = source,
    source_id = source_id,
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

make_temp_refactor_project <- function() {
  td <- tempfile("litxr-refactor-report-")
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
  arxiv_collection <- Filter(function(collection) identical(collection$remote_channel, "arxiv"), cfg$collections)[[1]]

  journal_local_path <- litxr:::.litxr_resolve_local_path(cfg, journal$local_path)
  arxiv_local_path <- litxr:::.litxr_resolve_local_path(cfg, arxiv_collection$local_path)

  doi_record <- make_record(
    "doi:10.1000/published-example",
    "crossref",
    "10.1000/published-example",
    "Published Example Paper",
    journal$journal_id,
    journal$title
  )
  doi_record$doi[[1]] <- "10.1000/published-example"
  doi_record$url_landing[[1]] <- "https://doi.org/10.1000/published-example"

  arxiv_record_1 <- make_record(
    "arxiv:2501.00001",
    "arxiv",
    "2501.00001",
    "Search Example Paper One",
    arxiv_collection$collection_id,
    arxiv_collection$title
  )
  arxiv_record_1$url_landing[[1]] <- "https://arxiv.org/abs/2501.00001v1"
  arxiv_record_1$url_pdf[[1]] <- "https://arxiv.org/pdf/2501.00001v1"
  arxiv_record_1$arxiv_version[[1]] <- 1L
  arxiv_record_1$arxiv_primary_category[[1]] <- "cs.AI"
  arxiv_record_1$arxiv_categories_raw[[1]] <- "cs.AI"

  arxiv_record_2 <- make_record(
    "arxiv:2501.00002",
    "arxiv",
    "2501.00002",
    "Search Example Paper Two",
    arxiv_collection$collection_id,
    arxiv_collection$title
  )
  arxiv_record_2$url_landing[[1]] <- "https://arxiv.org/abs/2501.00002v1"
  arxiv_record_2$url_pdf[[1]] <- "https://arxiv.org/pdf/2501.00002v1"
  arxiv_record_2$arxiv_version[[1]] <- 1L
  arxiv_record_2$arxiv_primary_category[[1]] <- "cs.AI"
  arxiv_record_2$arxiv_categories_raw[[1]] <- "cs.AI"

  suppressWarnings({
    litxr:::.litxr_write_journal_records(doi_record, journal_local_path, journal, cfg = cfg)
    litxr:::.litxr_write_journal_records(
      data.table::rbindlist(list(arxiv_record_1, arxiv_record_2), fill = TRUE),
      arxiv_local_path,
      arxiv_collection,
      cfg = cfg
    )

    mock_embed <- function(texts) {
      cbind(
        neural = as.numeric(grepl("neural", tolower(texts), fixed = TRUE)),
        finance = as.numeric(grepl("finance", tolower(texts), fixed = TRUE)),
        length = nchar(texts) / 100
      )
    }

    litxr::litxr_build_embedding_index(
      arxiv_collection$collection_id,
      cfg,
      field = "abstract",
      embed_fun = mock_embed,
      model = "mock-embedding-search-v1",
      provider = "mock",
      batch_size = 1L,
      overwrite = TRUE
    )

    arxiv_record_3 <- make_record(
      "arxiv:2501.00003",
      "arxiv",
      "2501.00003",
      "Search Example Paper Three",
      arxiv_collection$collection_id,
      arxiv_collection$title
    )
    arxiv_record_3$url_landing[[1]] <- "https://arxiv.org/abs/2501.00003v1"
    arxiv_record_3$url_pdf[[1]] <- "https://arxiv.org/pdf/2501.00003v1"
    arxiv_record_3$arxiv_version[[1]] <- 1L
    arxiv_record_3$arxiv_primary_category[[1]] <- "cs.AI"
    arxiv_record_3$arxiv_categories_raw[[1]] <- "cs.AI"
    litxr:::.litxr_write_journal_records(arxiv_record_3, arxiv_local_path, arxiv_collection, cfg = cfg)

    litxr::litxr_embed_collection_delta(
      arxiv_collection$collection_id,
      cfg,
      field = "abstract",
      embed_fun = mock_embed,
      model = "mock-embedding-search-v1",
      provider = "mock",
      batch_size = 1L,
      overwrite = FALSE,
      limit = 1L
    )

    pending_path <- litxr:::.litxr_ref_local_pending_path(cfg)
    pending_row <- data.table::data.table(
      ref_id = "local:pending-note",
      key_type = "local_pending",
      key_value = "local:pending-note"
    )
    fst::write_fst(as.data.frame(pending_row), pending_path)
  })

  list(
    root = td,
    config_path = config_path,
    cfg = cfg,
    journal = journal,
    arxiv_collection = arxiv_collection,
    journal_local_path = journal_local_path,
    arxiv_local_path = arxiv_local_path
  )
}

make_temp_sync_project <- function() {
  td <- tempfile("litxr-refactor-sync-")
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
  cfg$project$data_root <- file.path(td, "data", "literature")
  cfg$collections[[1]]$local_path <- file.path(cfg$project$data_root, "journal_of_finance")
  cfg$collections[[2]]$local_path <- file.path(cfg$project$data_root, "journal_of_financial_economics")
  cfg$collections[[3]]$local_path <- file.path(cfg$project$data_root, "arxiv_cs_ai")
  dir.create(dirname(cfg$collections[[1]]$local_path), recursive = TRUE, showWarnings = FALSE)
  yaml::write_yaml(cfg, config_path)
  cfg <- litxr::litxr_read_config(config_path)

  arxiv_collection <- Filter(function(collection) identical(collection$remote_channel, "arxiv"), cfg$collections)[[1]]
  arxiv_local_path <- litxr:::.litxr_resolve_local_path(cfg, arxiv_collection$local_path)
  dir.create(file.path(arxiv_local_path, "ref_json"), recursive = TRUE, showWarnings = FALSE)

  list(
    root = td,
    config_path = config_path,
    cfg = cfg,
    arxiv_collection = arxiv_collection,
    arxiv_local_path = arxiv_local_path
  )
}

test_that("completeness scoring prefers the most complete duplicate record", {
  complete <- data.table::data.table(
    ref_id = "arxiv:2501.00001",
    source = "arxiv",
    source_id = "2501.00001",
    title = "Complete Paper",
    authors = "Jane Doe; John Smith",
    authors_list = list(c("Jane Doe", "John Smith")),
    year = 2025L,
    doi = NA_character_,
    note = "complete"
  )
  sparse <- data.table::copy(complete)
  sparse$title[[1]] <- "Sparse Paper"
  sparse$authors[[1]] <- NA_character_
  sparse$authors_list[[1]] <- character()
  sparse$note[[1]] <- NA_character_

  scores <- litxr:::.litxr_record_completeness_score(data.table::rbindlist(list(complete, sparse), fill = TRUE))
  expect_equal(length(scores), 2L)
  expect_gt(scores[[1]], scores[[2]])

  preferred <- litxr:::.litxr_prefer_complete_records(
    data.table::rbindlist(list(complete, sparse), fill = TRUE)
  )
  expect_equal(nrow(preferred), 1L)
  expect_identical(preferred$title[[1]], "Complete Paper")
})

test_that("BibTeX export scalarizes list-valued fields without leaking vector syntax", {
  bib <- litxr::row_to_bibtex(data.table::data.table(
    ref_id = "book:test",
    source = "book",
    source_id = "booktest",
    entry_type = "book",
    title = list("Scalarized Title"),
    authors = "Jane Doe; John Smith",
    authors_list = list(c("Jane Doe", "John Smith")),
    year = 2024L,
    month = 1L,
    day = 1L,
    journal = NA_character_,
    container_title = NA_character_,
    publisher = "Example Press",
    volume = NA_character_,
    issue = NA_character_,
    pages = NA_character_,
    doi = NA_character_,
    isbn = list("9780262046305"),
    issn = NA_character_,
    url = list("https://example.org/book"),
    note = list("scalar note"),
    url_landing = NA_character_,
    url_pdf = NA_character_
  ))
  expect_true(any(grepl("@book\\{booktest, title = \\{Scalarized Title\\},", bib, fixed = FALSE)))
  expect_true(any(grepl("author = \\{Jane Doe and John Smith\\}", bib)))
  expect_true(any(grepl("isbn = \\{9780262046305\\}", bib)))
  expect_true(any(grepl("note = \\{scalar note\\}", bib)))
  expect_false(any(grepl("Ignored", bib, fixed = TRUE)))
  expect_false(any(grepl("c\\(", bib, fixed = FALSE)))
  expect_false(any(grepl("list\\(", bib, fixed = FALSE)))
})

test_that("legacy delta migration no longer creates or mutates retired projection files", {
  project <- make_temp_refactor_project()
  old_litxr_config <- Sys.getenv("LITXR_CONFIG", unset = NA_character_)
  Sys.setenv(LITXR_CONFIG = project$config_path)
  on.exit({
    if (is.na(old_litxr_config)) {
      Sys.unsetenv("LITXR_CONFIG")
    } else {
      Sys.setenv(LITXR_CONFIG = old_litxr_config)
    }
  }, add = TRUE)

  migration <- litxr::litxr_sync_thin_ref_stores_from_json(project$cfg)
  expect_true(is.list(migration))
  expect_true(is.list(migration$project_paths))
  expect_false(file.exists(file.path(litxr:::.litxr_project_root(project$cfg), "index", "references.fst")))
  expect_false(file.exists(file.path(litxr:::.litxr_project_root(project$cfg), "index", "reference_collections.fst")))
  expect_true(nrow(litxr::litxr_read_references(project$cfg)) >= 1L)
  expect_true(nrow(litxr::litxr_read_reference_collections(project$cfg)) >= 1L)
})

test_that("refactor audit report and projection scripts parse and expose their node names", {
  refactor_script <- normalizePath(file.path("..", "..", "scripts", "refactor_audit_nodes.R"), mustWork = TRUE)
  projection_script <- normalizePath(file.path("..", "..", "scripts", "measure_reference_projection_size.R"), mustWork = TRUE)

  expect_silent(parse(file = refactor_script))
  expect_silent(parse(file = projection_script))

  refactor_help <- system2("Rscript", c(refactor_script, "--help"), stdout = TRUE, stderr = tempfile())
  expect_true(any(grepl("derived_append_shard_state", refactor_help, fixed = TRUE)))
  expect_true(any(grepl("legacy_delta_presence", refactor_help, fixed = TRUE)))
  expect_true(any(grepl("embedding_search_shard_timing", refactor_help, fixed = TRUE)))
  expect_true(any(grepl("projection_size_reduction", refactor_help, fixed = TRUE)))

  projection_help <- system2("Rscript", c(projection_script, "--help"), stdout = TRUE, stderr = tempfile())
  expect_true(any(grepl("projection", projection_help, fixed = TRUE)))
})
