test_that("authoritative projections own rich payload boundaries and rebuild compatibility caches", {
  td <- tempfile("litxr-refactor-architecture-")
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
  arxiv_collection <- Filter(
    function(collection) identical(collection$remote_channel, "arxiv"),
    cfg$collections
  )[[1]]
  journal_local_path <- litxr:::.litxr_resolve_local_path(cfg, journal$local_path)
  arxiv_local_path <- litxr:::.litxr_resolve_local_path(cfg, arxiv_collection$local_path)

  make_record <- function(ref_id, source, source_id, title, collection_id, collection_title) {
    data.table::data.table(
      ref_id = ref_id,
      source = source,
      source_id = source_id,
      title = title,
      abstract = sprintf("Abstract for %s", ref_id),
      authors = "Jane Doe; John Smith",
      authors_list = list(c("Jane Doe", "John Smith")),
      pub_date = as.POSIXct("2025-02-01", tz = "UTC"),
      year = 2025L,
      month = 2L,
      day = 1L,
      journal = "Example Journal",
      container_title = "Example Journal",
      publisher = "Example Publisher",
      volume = "1",
      issue = "1",
      pages = "1-10",
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
      raw_entry = list(list(source = source, source_id = source_id)),
      collection_id = collection_id,
      collection_title = collection_title
    )
  }

  doi_record <- make_record(
    "doi:10.1000/architecture-example",
    "crossref",
    "10.1000/architecture-example",
    "Architecture Example Published Paper",
    journal$journal_id,
    journal$title
  )
  doi_record$doi[[1]] <- "10.1000/architecture-example"
  doi_record$url_landing[[1]] <- "https://doi.org/10.1000/architecture-example"

  arxiv_record <- make_record(
    "arxiv:2502.00001",
    "arxiv",
    "2502.00001",
    "Architecture Example ArXiv Paper",
    arxiv_collection$collection_id,
    arxiv_collection$title
  )
  arxiv_record$url_landing[[1]] <- "https://arxiv.org/abs/2502.00001v1"
  arxiv_record$url_pdf[[1]] <- "https://arxiv.org/pdf/2502.00001v1"
  arxiv_record$arxiv_version[[1]] <- 1L
  arxiv_record$arxiv_primary_category[[1]] <- "cs.AI"
  arxiv_record$arxiv_categories_raw[[1]] <- "cs.AI"

  litxr:::.litxr_write_journal_records(doi_record, journal_local_path, journal, cfg = cfg)
  litxr:::.litxr_write_journal_records(arxiv_record, arxiv_local_path, arxiv_collection, cfg = cfg)

  authoritative_refs <- litxr:::.litxr_authoritative_project_records(cfg)
  expect_true(nrow(authoritative_refs) >= 2L)
  expect_true(all(c("ref_id", "title", "authors", "pub_date") %in% names(authoritative_refs)))
  expect_false("abstract" %in% names(authoritative_refs))
  expect_false("authors_list" %in% names(authoritative_refs))
  expect_false("raw_entry" %in% names(authoritative_refs))

  authoritative_links <- litxr:::.litxr_authoritative_project_reference_links(cfg)
  expect_true(nrow(authoritative_links) >= 2L)
  expect_true(all(c("ref_id", "collection_id", "collection_title", "recorded_at") %in% names(authoritative_links)))

  stopifnot(!file.exists(file.path(litxr:::.litxr_project_root(cfg), "index", "references.fst")))
  stopifnot(!file.exists(file.path(litxr:::.litxr_project_root(cfg), "index", "reference_collections.fst")))

  build_result <- litxr::litxr_build_entity_indexes(cfg)
  expect_true(is.list(build_result))
  expect_true("ref_identity_map_path" %in% names(build_result))
  expect_true(file.exists(build_result$ref_identity_map_path))
  expect_false("entities_path" %in% names(build_result))
  expect_false(file.exists(file.path(litxr:::.litxr_project_root(cfg), "index", "ref_entities.fst")))

  expect_silent(refs <- litxr::litxr_read_references(cfg))
  links <- litxr::litxr_read_reference_collections(cfg)

  expect_true(nrow(refs) >= 2L)
  expect_true(nrow(links) >= 2L)
  expect_true(any(as.character(refs$ref_id) == "doi:10.1000/architecture-example"))
  expect_true(any(as.character(refs$ref_id) == "arxiv:2502.00001"))
  expect_true(any(as.character(links$collection_id) == journal$journal_id))
  expect_true(any(as.character(links$collection_id) == arxiv_collection$collection_id))
})
