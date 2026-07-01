test_that("raw embedding metadata repair only fills narrow missing arxiv rows", {
  root <- tempfile("litxr-raw-repair-")
  dir.create(root, recursive = TRUE, showWarnings = FALSE)
  cfg <- list(
    project = list(data_root = root),
    collections = list(list(
      collection_id = "arxiv_cs_ai",
      remote_channel = "arxiv"
    ))
  )
  attr(cfg, "config_root") <- root

  ref_dir <- file.path(root, "ref", "arxiv_cs_ai")
  dir.create(ref_dir, recursive = TRUE, showWarnings = FALSE)
  index_dir <- file.path(root, "index")
  dir.create(index_dir, recursive = TRUE, showWarnings = FALSE)
  raw_dir <- file.path(root, "corpus", "arxiv_cs_ai", "abstract", "raw")
  dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)

  ref_rows <- data.table::data.table(
    arxiv_id = c("2401.00001", "2401.00002"),
    collection_index = c(1L, 1L),
    json_filename = c("2401_00001.json", "2401_00002.json")
  )
  fst::write_fst(ref_rows, file.path(index_dir, "ref_arxiv.fst"))

  jsonlite::write_json(
    list(ref_id = "2401.00001", abstract = "alpha abstract"),
    file.path(ref_dir, "2401_00001.json"),
    auto_unbox = TRUE,
    pretty = FALSE,
    null = "null"
  )
  jsonlite::write_json(
    list(ref_id = "2401.00002", abstract = "beta abstract"),
    file.path(ref_dir, "2401_00002.json"),
    auto_unbox = TRUE,
    pretty = FALSE,
    null = "null"
  )

  raw_rows <- data.table::data.table(
    arxiv_id = "2401.00001",
    abstract = NA_character_
  )
  fst::write_fst(raw_rows, file.path(raw_dir, "metadata.fst"))

  result <- litxr:::.litxr_repair_embedding_raw_metadata_index(
    "arxiv_cs_ai",
    cfg,
    field = "abstract"
  )
  expect_true(is.list(result))
  repaired <- fst::read_fst(file.path(raw_dir, "metadata.fst"), as.data.table = TRUE)
  expect_identical(names(repaired), c("arxiv_id", "abstract"))
  expect_equal(nrow(repaired), 2L)
  expect_true(all(sort(repaired$arxiv_id) == c("2401.00001", "2401.00002")))
  expect_true(any(is.na(repaired$abstract)))
  expect_true(any(repaired$abstract == "beta abstract", na.rm = TRUE))
  expect_true(any(repaired$arxiv_id == "2401.00002"))

  second_pass <- litxr:::.litxr_repair_embedding_raw_metadata_index(
    "arxiv_cs_ai",
    cfg,
    field = "abstract"
  )
  expect_false(second_pass$changed)
  expect_equal(second_pass$total, 2L)

  fst::write_fst(
    data.table::data.table(arxiv_id = c("2401.00001", "9999.99999"), abstract = c("alpha abstract", "oops")),
    file.path(raw_dir, "metadata.fst")
  )
  expect_error(
    litxr:::.litxr_repair_embedding_raw_metadata_index("arxiv_cs_ai", cfg, field = "abstract"),
    "not present in ref_arxiv.fst"
  )
})

test_that("raw embedding metadata repair accepts legacy ref_id raw key", {
  root <- tempfile("litxr-raw-repair-legacy-")
  dir.create(root, recursive = TRUE, showWarnings = FALSE)
  cfg <- list(
    project = list(data_root = root),
    collections = list(list(
      collection_id = "arxiv_cs_ai",
      remote_channel = "arxiv"
    ))
  )
  attr(cfg, "config_root") <- root

  ref_dir <- file.path(root, "ref", "arxiv_cs_ai")
  dir.create(ref_dir, recursive = TRUE, showWarnings = FALSE)
  index_dir <- file.path(root, "index")
  dir.create(index_dir, recursive = TRUE, showWarnings = FALSE)
  raw_dir <- file.path(root, "corpus", "arxiv_cs_ai", "abstract", "raw")
  dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)

  fst::write_fst(
    data.table::data.table(
      arxiv_id = c("2401.00001"),
      collection_index = c(1L),
      json_filename = c("2401_00001.json")
    ),
    file.path(index_dir, "ref_arxiv.fst")
  )
  jsonlite::write_json(
    list(ref_id = "2401.00001", abstract = "alpha abstract"),
    file.path(ref_dir, "2401_00001.json"),
    auto_unbox = TRUE,
    pretty = FALSE,
    null = "null"
  )
  jsonlite::write_json(
    list(ref_id = "2401.00002", abstract = "beta abstract"),
    file.path(ref_dir, "2401_00002.json"),
    auto_unbox = TRUE,
    pretty = FALSE,
    null = "null"
  )
  fst::write_fst(
    data.table::data.table(ref_id = "2401.00001", abstract = NA_character_),
    file.path(raw_dir, "metadata.fst")
  )
  fst::write_fst(
    data.table::data.table(
      arxiv_id = c("2401.00001", "2401.00002"),
      collection_index = c(1L, 1L),
      json_filename = c("2401_00001.json", "2401_00002.json")
    ),
    file.path(index_dir, "ref_arxiv.fst")
  )

  result <- litxr:::.litxr_repair_embedding_raw_metadata_index("arxiv_cs_ai", cfg, field = "abstract")
  expect_true(result$changed)
  repaired <- fst::read_fst(file.path(raw_dir, "metadata.fst"), as.data.table = TRUE)
  expect_identical(names(repaired), c("arxiv_id", "abstract"))
  expect_identical(sort(repaired$arxiv_id), c("2401.00001", "2401.00002"))
  expect_true(any(is.na(repaired$abstract)))
  expect_true(any(repaired$abstract == "beta abstract", na.rm = TRUE))
})
