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
field <- read_flag("--field", "abstract")
embed_model <- read_flag("--model", "nvidia/llama-nemotron-embed-vl-1b-v2:free")
provider <- read_flag("--provider", "openrouter")

cfg <- litxr::litxr_read_config()

before <- litxr::litxr_read_embedding_state(
  collection_id,
  cfg,
  field = field,
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

compacted <- litxr::litxr_compact_embedding_delta(
  collection_id,
  cfg,
  field = field,
  model = embed_model,
  provider = provider
)

after <- litxr::litxr_read_embedding_state(
  collection_id,
  cfg,
  field = field,
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
  "compact complete: collection_id=%s field=%s model=%s rows_visible_after_compact=%s\n",
  collection_id,
  field,
  embed_model,
  nrow(compacted)
))
