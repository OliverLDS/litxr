#!/usr/bin/env Rscript

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
    collection_id = "arxiv_cs_ai"
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
      "  Rscript scripts/sync_arxiv_abstract_raw.R [--collection-id arxiv_cs_ai]",
      "",
      "Options:",
      "  --collection-id ID   Collection id whose raw abstract cache should be repaired.",
      "  -h, --help           Show this help message.",
      "",
      "Behavior:",
      "  - Compares bare arXiv ids in corpus/<collection>/abstract/raw/metadata.fst",
      "    against ref_arxiv.fst for that collection.",
      "  - Rehydrates only missing raw rows from the collection JSON files.",
      "  - Writes bare arXiv ids and raw abstracts to the raw metadata fst.",
      "  - Errors if raw metadata contains arXiv ids not present in ref_arxiv.fst.",
      "  - Does not touch embedding shards or compaction state.",
      sep = "\n"
    )
  )
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

parsed <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(parsed$show_help)) {
  usage()
  quit(save = "no", status = 0L)
}

cfg <- litxr::litxr_read_config()
log_line("syncing arxiv abstract raw metadata")
log_line("collection_id=", parsed$collection_id)

result <- litxr:::.litxr_repair_embedding_raw_metadata_index(
  parsed$collection_id,
  cfg,
  field = "abstract"
)
if (!is.list(result)) {
  result <- list(
    raw_path = NA_character_,
    added_arxiv_ids = character(),
    updated_arxiv_ids = character(),
    raw_only_arxiv_ids = character(),
    ref_only_arxiv_ids = character(),
    changed = FALSE,
    total = NA_integer_
  )
}

emit_json(list(
  status = "ok",
  collection_id = parsed$collection_id,
  field = "abstract",
  raw_path = if (length(result$raw_path)) result$raw_path else NA_character_,
  added_arxiv_ids = if (length(result$added_arxiv_ids)) result$added_arxiv_ids else character(),
  updated_arxiv_ids = if (length(result$updated_arxiv_ids)) result$updated_arxiv_ids else character(),
  raw_only_arxiv_ids = if (length(result$raw_only_arxiv_ids)) result$raw_only_arxiv_ids else character(),
  ref_only_arxiv_ids = if (length(result$ref_only_arxiv_ids)) result$ref_only_arxiv_ids else character(),
  changed = isTRUE(result$changed),
  total = if (length(result$total)) result$total else NA_integer_
))
