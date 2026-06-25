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
  dir.create(file.path(cfg$project$data_root, "index"), recursive = TRUE, showWarnings = FALSE)

  list(
    root = td,
    config_path = config_path,
    cfg = cfg,
    arxiv_collection = arxiv_collection,
    arxiv_local_path = arxiv_local_path
  )
}

test_that("thin ref store sync collapses arxiv versions before inferring identity links", {
  project <- make_temp_sync_project()
  arxiv_json_dir <- file.path(project$arxiv_local_path, "ref_json")

  arxiv_v1 <- list(
    ref_id = "arxiv:2501.99999",
    source_id = "2501.99999v1",
    arxiv_id_versioned = "2501.99999v1",
    arxiv_version = 1L,
    doi = "10.1000/old-link",
    linked_doi_ref_id = NA_character_,
    linked_arxiv_ref_id = NA_character_,
    year = NA_integer_
  )
  arxiv_v3 <- list(
    ref_id = "arxiv:2501.99999",
    source_id = "2501.99999v3",
    arxiv_id_versioned = "2501.99999v3",
    arxiv_version = 3L,
    doi = "10.1000/new-link",
    linked_doi_ref_id = NA_character_,
    linked_arxiv_ref_id = NA_character_,
    year = NA_integer_
  )

  jsonlite::write_json(
    arxiv_v1,
    file.path(arxiv_json_dir, "arxiv_2501_99999v1.json"),
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )
  jsonlite::write_json(
    arxiv_v3,
    file.path(arxiv_json_dir, "arxiv_2501_99999v3.json"),
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )

  migration <- litxr::litxr_sync_thin_ref_stores_from_json(
    project$cfg,
    collection_ids = project$arxiv_collection$collection_id
  )
  expect_true(is.list(migration))
  arxiv_store <- data.table::as.data.table(fst::read_fst(litxr:::.litxr_ref_arxiv_path(project$cfg), as.data.table = TRUE))
  identity_store <- data.table::as.data.table(fst::read_fst(litxr:::.litxr_project_ref_identity_index_path(project$cfg), as.data.table = TRUE))
  expect_identical(names(arxiv_store), c("arxiv_id", "collection_index", "json_filename"))
  expect_identical(sort(names(identity_store)), c("arxiv_id", "doi"))
  expect_true(is.list(migration$diff_paths))
  expect_true(all(c("ref_identity_map", "ref_arxiv", "ref_doi") %in% names(migration$diff_paths)))
  expect_true(all(c("added", "removed") %in% names(migration$diff_paths$ref_arxiv)))

  identity_map <- data.table::as.data.table(litxr::litxr_read_ref_identity_map(project$cfg))
  target_row <- identity_map[arxiv_id == "2501.99999"]
  expect_equal(nrow(target_row), 1L)
  expect_equal(target_row$doi[[1]], "10.1000/new-link")
  expect_false(any(identity_map$doi == "10.1000/old-link"))
})

test_that("incremental thin ref store sync preserves existing rows and emits no removals", {
  project <- make_temp_sync_project()
  arxiv_json_dir <- file.path(project$arxiv_local_path, "ref_json")

  arxiv_record <- list(
    ref_id = "arxiv:2502.12345",
    source_id = "2502.12345v1",
    arxiv_id_versioned = "2502.12345v1",
    arxiv_version = 1L,
    doi = "10.1000/incremental-link",
    linked_doi_ref_id = NA_character_,
    linked_arxiv_ref_id = NA_character_,
    year = NA_integer_
  )

  jsonlite::write_json(
    arxiv_record,
    file.path(arxiv_json_dir, "arxiv_2502_12345v1.json"),
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )

  full_sync <- litxr::litxr_sync_thin_ref_stores_from_json(
    project$cfg,
    collection_ids = project$arxiv_collection$collection_id
  )
  expect_true(full_sync$row_counts$ref_arxiv >= 1L)

  incremental_sync <- litxr::litxr_sync_thin_ref_stores_from_json(
    project$cfg,
    collection_ids = project$arxiv_collection$collection_id,
    json_mtime_after = Sys.time() + 3600
  )

  expect_identical(incremental_sync$mode, "incremental")
  expect_identical(incremental_sync$diff_paths$ref_arxiv$removed, NA_character_)
  expect_identical(incremental_sync$diff_paths$ref_doi$removed, NA_character_)
  expect_identical(incremental_sync$diff_paths$ref_isbn$removed, NA_character_)
  expect_identical(incremental_sync$project_paths$ref_arxiv_removed, NA_character_)
  expect_identical(incremental_sync$project_paths$ref_doi_removed, NA_character_)
  expect_identical(incremental_sync$project_paths$ref_isbn_removed, NA_character_)
  expect_true(incremental_sync$diffs$ref_arxiv$removed == 0L)
  expect_true(incremental_sync$diffs$ref_doi$removed == 0L)
  expect_true(incremental_sync$diffs$ref_isbn$removed == 0L)
  expect_equal(incremental_sync$row_counts$ref_arxiv, full_sync$row_counts$ref_arxiv)
})

test_that("arxiv-side identity extraction ignores blank DOI values", {
  project <- make_temp_sync_project()
  arxiv_json_dir <- file.path(project$arxiv_local_path, "ref_json")

  arxiv_record <- list(
    ref_id = "arxiv:2502.54321",
    source_id = "2502.54321v1",
    arxiv_id_versioned = "2502.54321v1",
    arxiv_version = 1L,
    doi = "   ",
    linked_doi_ref_id = NA_character_,
    linked_arxiv_ref_id = NA_character_,
    year = NA_integer_
  )

  jsonlite::write_json(
    arxiv_record,
    file.path(arxiv_json_dir, "arxiv_2502_54321v1.json"),
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )

  migration <- litxr::litxr_sync_thin_ref_stores_from_json(
    project$cfg,
    collection_ids = project$arxiv_collection$collection_id
  )

  expect_true(migration$row_counts$ref_arxiv >= 1L)
  identity_map <- data.table::as.data.table(litxr::litxr_read_ref_identity_map(project$cfg))
  expect_false(any(identity_map$arxiv_id == "2502.54321"))
  expect_false(any(identity_map$doi == ""))
})
