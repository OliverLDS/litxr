#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

emit_json <- function(x) {
  writeLines(
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE),
    con = stdout()
  )
}

parse_args <- function(args) {
  out <- list(
    show_help = FALSE,
    collection_id = "arxiv_cs_ai",
    field = "abstract",
    model = "nvidia/llama-nemotron-embed-vl-1b-v2:free",
    provider = "openrouter"
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
    } else if (identical(key, "--field")) {
      out$field <- value
    } else if (identical(key, "--model")) {
      out$model <- value
    } else if (identical(key, "--provider")) {
      out$provider <- value
    } else {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    i <- i + 2L
  }

  out
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/compact_arxiv_embedding_delta.R [--collection-id arxiv_cs_ai] [--field abstract] [--model MODEL] [--provider openrouter]",
      "",
      "Options:",
      "  --collection-id ID   Collection id to compact. Default: arxiv_cs_ai.",
      "  --field FIELD        Embedded field to compact. Default: abstract.",
      "  --model MODEL        Embedding model name.",
      "  --provider NAME      Embedding provider label stored in the manifest.",
      "  -h, --help           Show this help message.",
      "",
      "Output:",
      "  Progress logs are written to stderr; compact JSON is written to stdout.",
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
field <- parsed$field
embed_model <- parsed$model
provider <- parsed$provider

cfg <- litxr::litxr_read_config()

before <- litxr::litxr_read_embedding_state(
  collection_id,
  cfg,
  field = field,
  model = embed_model
)

log_line(sprintf(
  "before: total=%s embedded_main=%s embedded_delta=%s embedded_unique=%s missing=%s coverage=%.6f",
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

log_line(sprintf(
  "after: total=%s embedded_main=%s embedded_delta=%s embedded_unique=%s missing=%s coverage=%.6f",
  after$records_total[[1]],
  after$embedded_main[[1]],
  after$embedded_delta[[1]],
  after$embedded_unique[[1]],
  after$missing[[1]],
  after$coverage_pct[[1]]
))

log_line(sprintf(
  "compact complete: collection_id=%s field=%s model=%s rows_visible_after_compact=%s",
  collection_id,
  field,
  embed_model,
  nrow(compacted)
))

emit_json(list(
  status = "ok",
  collection_id = collection_id,
  field = field,
  model = embed_model,
  provider = provider,
  before = as.list(before[1]),
  after = as.list(after[1]),
  rows_visible_after_compact = nrow(compacted)
))
