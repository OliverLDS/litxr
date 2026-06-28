test_that("BM25 ranks the richer retrieval document first", {
  td <- tempfile("litxr-bm25-")
  dir.create(td)
  shard_dir <- file.path(td, "2026")
  dir.create(shard_dir)
  shard_path <- file.path(shard_dir, "metadata.fst")
  index_dir <- file.path(td, "index")

  corpus <- data.table::data.table(
    doc_id = c("d1", "d2", "d3"),
    abstract = c(
      "retrieval augmented generation retrieval large language model",
      "retrieval method",
      "graph neural network"
    )
  )
  fst::write_fst(as.data.frame(corpus), shard_path)

  litxr::litxr_bm25_build_index(
    shard_paths = c("2026" = shard_path),
    index_dir = index_dir,
    overwrite = TRUE,
    verbose = FALSE
  )
  index <- litxr::litxr_bm25_load_index(index_dir)
  out <- litxr::litxr_bm25_search(
    query = "retrieval augmented generation large language model",
    index = index,
    top_k = 10L
  )

  expect_true(nrow(out) >= 2L)
  expect_identical(out$doc_id[[1L]], "d1")
  expect_false("d3" %in% out$doc_id)
})

test_that("BM25 restrict_doc_ids narrows candidates", {
  td <- tempfile("litxr-bm25-")
  dir.create(td)
  shard_dir <- file.path(td, "2026")
  dir.create(shard_dir)
  shard_path <- file.path(shard_dir, "metadata.fst")
  index_dir <- file.path(td, "index")

  corpus <- data.table::data.table(
    doc_id = c("d1", "d2"),
    abstract = c(
      "retrieval augmented generation retrieval large language model",
      "retrieval method"
    )
  )
  fst::write_fst(as.data.frame(corpus), shard_path)

  litxr::litxr_bm25_build_index(
    shard_paths = c("2026" = shard_path),
    index_dir = index_dir,
    overwrite = TRUE,
    verbose = FALSE
  )
  index <- litxr::litxr_bm25_load_index(index_dir)
  out <- litxr::litxr_bm25_search(
    query = "retrieval augmented generation large language model",
    index = index,
    top_k = 10L,
    restrict_doc_ids = "d2"
  )

  expect_identical(out$doc_id, "d2")
})
