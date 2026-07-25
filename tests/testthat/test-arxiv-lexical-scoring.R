test_that("arXiv lexical category scoring returns narrow bare-id rows", {
  root <- tempfile("litxr-arxiv-lexical-")
  dir.create(root)
  previous_root <- Sys.getenv("LITXR_DATA_ROOT", unset = NA_character_)
  Sys.setenv(LITXR_DATA_ROOT = root)
  on.exit({
    if (is.na(previous_root)) Sys.unsetenv("LITXR_DATA_ROOT") else Sys.setenv(LITXR_DATA_ROOT = previous_root)
  }, add = TRUE)

  litxr::litxr_init()
  cfg <- litxr::litxr_read_config()
  lexical_dir <- litxr:::.litxr_project_corpus_field_lexical_dir(cfg, "arxiv_cs_ai", "abstract")
  query_dir <- file.path(litxr:::.litxr_project_queries_dir(cfg), "lexical")
  dir.create(lexical_dir, recursive = TRUE)
  dir.create(query_dir, recursive = TRUE)

  fst::write_fst(
    data.table::data.table(
      doc_int = 1:3,
      arxiv_id = c("2401.00001", "2401.00002", "2401.00003")
    ),
    file.path(lexical_dir, "metadata.fst")
  )
  fst::write_fst(
    data.table::data.table(
      doc_int = c(1L, 1L, 1L, 1L, 2L, 2L),
      term = c("large", "language", "model", "retrieval", "large", "language")
    ),
    file.path(lexical_dir, "postings.fst")
  )
  data.table::fwrite(
    data.table::data.table(
      category_id = c("language_models", "no_matches"),
      lexical_keywords = c("large language model|retrieval", "graph neural network")
    ),
    file.path(query_dir, "ai_category_keywords_v1.csv")
  )

  scores <- litxr::litxr_score_arxiv_lexical_categories("arxiv_cs_ai")
  expect_identical(names(scores), c("ref_id", "category_id", "score_max"))
  expect_identical(scores$ref_id, "2401.00001")
  expect_identical(scores$category_id, "language_models")
  expect_identical(scores$score_max, 2L)
  expect_true(all(grepl("^[0-9]{4}\\.[0-9]{4,5}$", scores$ref_id)))

  strict_scores <- litxr::litxr_score_arxiv_lexical_categories(
    "arxiv_cs_ai",
    min_keywords_per_category = 3L
  )
  expect_identical(strict_scores, data.table::data.table(
    ref_id = character(), category_id = character(), score_max = integer()
  ))
})
