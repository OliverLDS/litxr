#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(litxr)
})

script_path <- {
  args_full <- commandArgs(trailingOnly = FALSE)
  file_flag <- "--file="
  hit <- grep(file_flag, args_full, value = TRUE)
  if (length(hit)) sub(file_flag, "", hit[[1]]) else ""
}

repo_root <- if (nzchar(script_path)) {
  normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = FALSE)
} else {
  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

query_set_path <- system.file("extdata", "ai_category_query_set.json", package = "litxr")
if (!nzchar(query_set_path)) {
  query_set_path <- file.path(repo_root, "inst", "extdata", "ai_category_query_set.json")
}
if (!file.exists(query_set_path)) {
  stop("Category query set not found: ", query_set_path, call. = FALSE)
}

if (!requireNamespace("inferencer", quietly = TRUE)) {
  stop("Package `inferencer` is required for this script.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
embed_model <- "nvidia/llama-nemotron-embed-vl-1b-v2:free"
embed_fun <- function(texts) {
  inferencer::embed_openrouter(texts, model = embed_model)
}

delta_meta <- litxr::litxr_embed_collection_delta(
  "arxiv_cs_ai",
  cfg,
  field = "abstract",
  embed_fun = embed_fun,
  model = embed_model,
  provider = "openrouter",
  batch_size = 64,
  limit = 640L
)

query_index <- litxr::litxr_build_label_query_index(
  query_set = query_set_path,
  query_set_id = "ai-category-query-set-v1",
  config = cfg,
  embed_fun = embed_fun,
  model = embed_model,
  provider = "openrouter",
  batch_size = 32L
)

scores <- litxr::litxr_score_collection_categories(
  "arxiv_cs_ai",
  query_set_id = "ai-category-query-set-v1",
  config = cfg,
  field = "abstract",
  model = embed_model,
  aggregations = "max"
)

if (!nrow(scores)) {
  cat("No category scores were produced.\n")
  quit(save = "no", status = 0)
}

top3 <- scores[order(category_id, -score_max, title)][
  ,
  head(.SD, 3L),
  by = category_id
]

top3_out <- top3[, c("category_id", "ref_id", "title", "year", "score_max"), with = FALSE]

cat(sprintf("delta_rows=%d\n", nrow(delta_meta)))
cat(sprintf("query_rows=%d\n", nrow(query_index$metadata)))
print(top3_out)
