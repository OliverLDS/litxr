make_temp_sync_project <- function() {
  td <- tempfile("litxr-refactor-sync-")
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
  cfg$project$data_root <- file.path(td, "data", "literature")
  cfg$collections[[1]]$local_path <- file.path(cfg$project$data_root, "ref", "journal_of_finance")
  cfg$collections[[2]]$local_path <- file.path(cfg$project$data_root, "ref", "journal_of_financial_economics")
  cfg$collections[[3]]$local_path <- file.path(cfg$project$data_root, "ref", "arxiv_cs_ai")
  dir.create(dirname(cfg$collections[[1]]$local_path), recursive = TRUE, showWarnings = FALSE)
  yaml::write_yaml(cfg, config_path)
  cfg <- litxr::litxr_read_config(config_path)

  arxiv_collection <- Filter(function(collection) identical(collection$remote_channel, "arxiv"), cfg$collections)[[1]]
  arxiv_local_path <- litxr:::.litxr_resolve_local_path(cfg, arxiv_collection$local_path)
  dir.create(arxiv_local_path, recursive = TRUE, showWarnings = FALSE)
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
  arxiv_json_dir <- project$arxiv_local_path

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
  expect_identical(
    names(arxiv_store),
    c("arxiv_id", "arxiv_version", "collection_index", "json_filename", "doi")
  )
  expect_identical(sort(names(identity_store)), c("arxiv_id", "doi"))
  if (file.exists(litxr:::.litxr_ref_doi_path(project$cfg))) {
    doi_store <- data.table::as.data.table(fst::read_fst(litxr:::.litxr_ref_doi_path(project$cfg), as.data.table = TRUE))
    expect_identical(names(doi_store), c("doi"))
  }
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
  arxiv_json_dir <- project$arxiv_local_path

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
  arxiv_json_dir <- project$arxiv_local_path

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

test_that("thin sync preserves multi-subject arxiv membership in local indexes", {
  project <- make_temp_sync_project()
  cfg <- project$cfg
  cs_lg <- project$arxiv_collection
  cs_lg$collection_id <- "arxiv_cs_lg"
  cs_lg$title <- "arXiv cs.LG"
  cs_lg$local_path <- file.path(cfg$project$data_root, "ref", "arxiv_cs_lg")
  cs_lg$metadata$category <- "cs.LG"
  cs_lg$sync$search_query <- "cat:cs.LG"
  cfg$collections[[length(cfg$collections) + 1L]] <- cs_lg
  yaml::write_yaml(cfg, project$config_path)
  cfg <- litxr::litxr_read_config(project$config_path)

  cs_ai_dir <- litxr:::.litxr_collection_ref_dir(cfg, "arxiv_cs_ai")
  cs_lg_dir <- litxr:::.litxr_collection_ref_dir(cfg, "arxiv_cs_lg")
  dir.create(cs_lg_dir, recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(
    list(ref_id = "arxiv:2501.12345", source_id = "2501.12345v1", arxiv_version = 1L),
    file.path(cs_ai_dir, "arxiv_2501_12345_cs_ai.json"),
    auto_unbox = TRUE,
    null = "null"
  )
  jsonlite::write_json(
    list(ref_id = "arxiv:2501.12345", source_id = "2501.12345v2", arxiv_version = 2L),
    file.path(cs_lg_dir, "arxiv_2501_12345_cs_lg.json"),
    auto_unbox = TRUE,
    null = "null"
  )

  litxr::litxr_sync_thin_ref_stores_from_json(cfg)
  global_rows <- fst::read_fst(litxr:::.litxr_ref_arxiv_path(cfg), as.data.table = TRUE)
  cs_ai_rows <- fst::read_fst(litxr:::.litxr_ref_arxiv_collection_path(cfg, "arxiv_cs_ai"), as.data.table = TRUE)
  cs_lg_rows <- fst::read_fst(litxr:::.litxr_ref_arxiv_collection_path(cfg, "arxiv_cs_lg"), as.data.table = TRUE)

  expect_equal(sum(global_rows$arxiv_id == "2501.12345"), 1L)
  expect_identical(global_rows[arxiv_id == "2501.12345", json_filename], "arxiv_2501_12345_cs_lg.json")
  expect_identical(cs_ai_rows$arxiv_id, "2501.12345")
  expect_identical(cs_ai_rows$json_filename, "arxiv_2501_12345_cs_ai.json")
  expect_identical(cs_lg_rows$arxiv_id, "2501.12345")
  expect_identical(cs_lg_rows$json_filename, "arxiv_2501_12345_cs_lg.json")
})

test_that("arxiv fetch history cutoff ignores trailing zero-count days", {
  project <- make_temp_sync_project()
  history_path <- litxr:::.litxr_collection_fetch_history_path(project$cfg, "arxiv_cs_ai")
  history <- data.table::data.table(
    completed_collection_date = c("2026-06-18", "2026-06-19", "2026-06-20", "2026-06-21"),
    total_ref_jsons = c(12L, 7L, 0L, 0L)
  )
  litxr:::.litxr_write_collection_fetch_history(project$cfg, "arxiv_cs_ai", history)
  expect_identical(
    litxr:::.litxr_latest_collection_fetch_completed_date(project$cfg, "arxiv_cs_ai"),
    "2026-06-21"
  )
  expect_identical(
    litxr:::.litxr_latest_collection_fetch_completed_date_nonzero(project$cfg, "arxiv_cs_ai"),
    "2026-06-19"
  )
  expect_true(file.exists(history_path))
})
