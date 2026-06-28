test_that("litxr_lexical_normalize_text normalizes basic punctuation and case", {
  out <- litxr::litxr_lexical_normalize_text("Retrieval-Augmented Generation (RAG) for LLMs")
  expect_identical(out, "retrieval augmented generation rag for llms")
})

test_that("litxr_lexical_tokenize keeps technical tokens", {
  tokens <- litxr::litxr_lexical_tokenize(c("GPT-4 with C++ and C# for RAG"))[[1]]
  expect_true("gpt-4" %in% tokens)
  expect_true("c++" %in% tokens)
  expect_true("c#" %in% tokens)
  expect_true("rag" %in% tokens)
})

test_that("litxr_lexical_flatten_query_sets returns long keyword table", {
  query_sets <- list(
    ai_methods = list(
      llm = c("large language model", "LLM"),
      retrieval = c("retrieval augmented generation", "RAG")
    )
  )
  out <- litxr::litxr_lexical_flatten_query_sets(query_sets)
  expect_s3_class(out, "data.table")
  expect_identical(names(out), c("query_set", "category", "keyword"))
  expect_equal(nrow(out), 4L)
})
