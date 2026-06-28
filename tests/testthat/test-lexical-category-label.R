test_that("litxr_lexical_keyword_pattern uses whole-term boundaries", {
  pattern_ai <- litxr::litxr_lexical_keyword_pattern("ai")
  pattern_rag <- litxr::litxr_lexical_keyword_pattern("rag")
  pattern_phrase <- litxr::litxr_lexical_keyword_pattern("retrieval augmented generation")

  expect_false(stringi::stri_detect_regex("chair", pattern_ai))
  expect_true(stringi::stri_detect_regex("rag", pattern_rag))
  expect_true(stringi::stri_detect_regex(
    litxr::litxr_lexical_normalize_text("retrieval augmented generation"),
    pattern_phrase
  ))
})

test_that("litxr_lexical_label_categories labels a small shard", {
  td <- tempfile("litxr-lexical-")
  dir.create(td)
  shard_dir <- file.path(td, "2026")
  dir.create(shard_dir)
  shard_path <- file.path(shard_dir, "metadata.fst")

  corpus <- data.table::data.table(
    doc_id = c("a1", "a2", "a3"),
    abstract = c(
      "We study retrieval augmented generation for large language models.",
      "This paper studies graph neural networks.",
      "Financial sentiment improves stock return prediction."
    )
  )
  fst::write_fst(as.data.frame(corpus), shard_path)

  query_sets <- list(
    ai_methods = list(
      llm = c("large language model", "large language models", "LLM"),
      retrieval = c("retrieval augmented generation", "RAG")
    ),
    finance_ai = list(
      forecasting = c("stock return prediction"),
      sentiment = c("financial sentiment")
    )
  )

  out <- litxr::litxr_lexical_label_categories(
    shard_paths = c("2026" = shard_path),
    query_sets = query_sets,
    min_keywords_per_category = 1L,
    min_categories_per_set = 1L,
    verbose = FALSE
  )

  expect_true("a1" %in% out$set_matches$doc_id)
  expect_true("a3" %in% out$set_matches$doc_id)
  expect_false("a2" %in% out$set_matches$doc_id)
})
