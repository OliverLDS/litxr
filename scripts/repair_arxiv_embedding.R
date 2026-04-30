#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(args) {
  out <- list(
    show_help = FALSE,
    collection_id = "arxiv_cs_ai",
    model = "nvidia/llama-nemotron-embed-vl-1b-v2:free",
    batch_size = "64",
    limit = "640"
  )
  i <- 1L

  while (i <= length(args)) {
    key <- args[[i]]
    if (!startsWith(key, "--") && !identical(key, "-h")) {
      stop("Unexpected positional argument: ", key, call. = FALSE)
    }
    if (identical(key, "-h") || identical(key, "--help")) {
      out$show_help <- TRUE
      i <- i + 1L
      next
    }
    if (i == length(args)) {
      stop("Missing value for ", key, call. = FALSE)
    }
    value <- args[[i + 1L]]
    if (identical(key, "--collection-id")) {
      out$collection_id <- value
    } else if (identical(key, "--model")) {
      out$model <- value
    } else if (identical(key, "--batch-size")) {
      out$batch_size <- value
    } else if (identical(key, "--limit")) {
      out$limit <- value
    } else {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    i <- i + 2L
  }

  out
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/repair_arxiv_embedding.R [--collection-id arxiv_cs_ai] [--model MODEL] [--batch-size 64] [--limit 640]",
      "",
      "Options:",
      "  --collection-id ID   Collection id to embed. Default: arxiv_cs_ai.",
      "  --model MODEL        Embedding model name.",
      "  --batch-size N       Batch size passed to the embedding function.",
      "  --limit N            Maximum number of missing refs to embed into delta.",
      "  -h, --help           Show this help message.",
      "",
      "Notes:",
      "  - This script uses `litxr_embed_collection_delta()` rather than full compaction.",
      "  - It prints before/after embedding coverage from `litxr_read_embedding_state()`.",
      sep = "\n"
    )
  )
}

parsed <- parse_args(args)

if (isTRUE(parsed$show_help)) {
  usage()
  quit(save = "no", status = 0L)
}

collection_id <- parsed$collection_id
embed_model <- parsed$model
batch_size <- as.integer(parsed$batch_size)
limit <- as.integer(parsed$limit)

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
