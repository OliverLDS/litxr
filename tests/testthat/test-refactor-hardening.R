test_that("refactor hardening paths work through alias/entity layer", {
  td <- tempfile("litxr-refactor-hardening-")
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

  find_script <- function(...) {
    rel <- file.path(...)
    candidates <- file.path(c(".", "..", "../.."), rel)
    hit <- candidates[file.exists(candidates)]
    stopifnot(length(hit) >= 1L)
    hit[[1L]]
  }

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

  litxr:::.litxr_write_journal_records(doi_record, journal_local_path, journal, cfg = cfg)
  litxr:::.litxr_write_journal_records(arxiv_record, arxiv_local_path, arxiv_collection, cfg = cfg)

  link_result <- litxr::litxr_enrich_arxiv_with_doi(
    arxiv_ref_id = "arxiv:2501.00001",
    doi = "10.1000/published-example",
    config = cfg,
    add_doi = FALSE
  )
  expect_identical(link_result$preferred_citation_ref_id, "doi:10.1000/published-example")

  diag <- litxr::litxr_refactor_diagnostics(cfg, oversized_mb = 0.0001)
  expect_true(is.list(diag))
  expect_true(all(c("summary", "reference_cache", "entity_indexes", "entity_status") %in% names(diag)))
  expect_equal(nrow(diag$summary), 1L)

  migration <- litxr::litxr_migrate_refactor_indexes(cfg)
  expect_true(is.list(migration))
  expect_true(any(migration$selected_collection_ids == "arxiv_cs_ai"))
  expect_true(any(migration$selected_collection_ids == journal$journal_id))
  expect_true(file.exists(migration$project_paths$ref_aliases))
  expect_true(file.exists(migration$project_paths$entity_status))

  prompt_text <- litxr::litxr_llm_digest_prompt(
    "2501.00001",
    config = cfg,
    mode = "create",
    schema_version = "v4",
    prompt_version = "v4.0"
  )
  expect_match(prompt_text, "https://arxiv.org/html/2501.00001", fixed = TRUE)

  bib_path <- file.path(td, "refs.bib")
  litxr::litxr_export_bib(bib_path, keys = "2501.00001", config = cfg)
  expect_true(file.exists(bib_path))
  bib_text <- paste(readLines(bib_path, warn = FALSE), collapse = "\n")
  expect_match(bib_text, "Published Example Paper", fixed = TRUE)
  expect_match(bib_text, "doi = \\{10.1000/published-example\\}")

  diag_script <- find_script("scripts", "diagnose_refactor_store.R")
  migrate_script <- find_script("scripts", "migrate_refactor_indexes.R")
  human_enrich_script <- find_script("scripts", "human", "enrich_arxiv_with_doi.R")
  expect_silent(parse(file = diag_script))
  expect_silent(parse(file = migrate_script))
  expect_silent(parse(file = human_enrich_script))

  doi_record2 <- make_record(
    "doi:10.1000/published-example-two",
    "crossref",
    "10.1000/published-example-two",
    "Published Example Paper Two",
    journal$journal_id,
    journal$title
  )
  doi_record2$doi[[1]] <- "10.1000/published-example-two"
  doi_record2$url_landing[[1]] <- "https://doi.org/10.1000/published-example-two"

  arxiv_record2 <- make_record(
    "arxiv:2501.00002",
    "arxiv",
    "2501.00002",
    "ArXiv Example Paper Two",
    arxiv_collection$collection_id,
    arxiv_collection$title
  )
  arxiv_record2$abstract[[1]] <- "A second example arXiv paper."
  arxiv_record2$url_landing[[1]] <- "https://arxiv.org/abs/2501.00002v1"
  arxiv_record2$url_pdf[[1]] <- "https://arxiv.org/pdf/2501.00002v1"
  arxiv_record2$arxiv_version[[1]] <- 1L
  arxiv_record2$arxiv_primary_category[[1]] <- "cs.AI"
  arxiv_record2$arxiv_categories_raw[[1]] <- "cs.AI"

  litxr:::.litxr_write_journal_records(doi_record2, journal_local_path, journal, cfg = cfg)
  litxr:::.litxr_write_journal_records(arxiv_record2, arxiv_local_path, arxiv_collection, cfg = cfg)

  second_link <- litxr::litxr_enrich_arxiv_with_doi(
    arxiv_ref_id = "arxiv:2501.00002",
    doi = "10.1000/published-example-two",
    config = cfg,
    add_doi = FALSE
  )
  expect_identical(second_link$preferred_citation_ref_id, "doi:10.1000/published-example-two")
  linked_rows <- litxr::litxr_find_refs(ref_id = "arxiv:2501.00002", config = cfg)
  expect_equal(nrow(linked_rows), 1L)
  expect_identical(linked_rows$linked_doi_ref_id[[1]], "doi:10.1000/published-example-two")
})
