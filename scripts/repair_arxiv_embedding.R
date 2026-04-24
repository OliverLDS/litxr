#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

read_flag <- function(flag, default = NULL) {
  idx <- match(flag, args)
  if (is.na(idx)) {
    return(default)
  }
  if (idx == length(args)) {
    stop("Missing value for ", flag, call. = FALSE)
  }
  args[[idx + 1L]]
}

collection_id <- read_flag("--collection-id", "arxiv_cs_ai")
embed_model <- read_flag("--model", "nvidia/llama-nemotron-embed-vl-1b-v2:free")
batch_size <- as.integer(read_flag("--batch-size", "64"))
limit <- as.integer(read_flag("--limit", "640"))

if (is.na(batch_size) || batch_size <= 0L) {
  stop("`--batch-size` must be a positive integer.", call. = FALSE)
}
if (is.na(limit) || limit < 0L) {
  stop("`--limit` must be a non-negative integer.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()

embed_fun <- function(texts) {
  inferencer::embed_openrouter(texts, model = embed_model)
}

before <- litxr::litxr_read_embedding_state(
  collection_id,
  cfg,
  field = "abstract",
  model = embed_model
)

cat(sprintf(
  "before: total=%s embedded_main=%s embedded_delta=%s embedded_unique=%s missing=%s coverage=%.6f\n",
  before$records_total[[1]],
  before$embedded_main[[1]],
  before$embedded_delta[[1]],
  before$embedded_unique[[1]],
  before$missing[[1]],
  before$coverage_pct[[1]]
))

embedded <- litxr::litxr_embed_collection_delta(
  collection_id,
  cfg,
  field = "abstract",
  embed_fun = embed_fun,
  model = embed_model,
  provider = "openrouter",
  batch_size = batch_size,
  limit = limit
)

after <- litxr::litxr_read_embedding_state(
  collection_id,
  cfg,
  field = "abstract",
  model = embed_model
)

cat(sprintf(
  "after: total=%s embedded_main=%s embedded_delta=%s embedded_unique=%s missing=%s coverage=%.6f\n",
  after$records_total[[1]],
  after$embedded_main[[1]],
  after$embedded_delta[[1]],
  after$embedded_unique[[1]],
  after$missing[[1]],
  after$coverage_pct[[1]]
))

cat(sprintf(
  "delta_run: rows_visible_after_run=%s requested_limit=%s\n",
  nrow(embedded),
  limit
))
