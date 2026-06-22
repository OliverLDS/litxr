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

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/migrate_refactor_indexes.R [--collection-id ID1,ID2,...] [--no-rebuild-collection-indexes]",
      "",
      "Options:",
      "  --collection-id IDS            Optional comma-separated collection ids to refresh.",
      "                                 Default: all configured collections.",
      "  --no-rebuild-collection-indexes",
      "                                 Reuse current collection projection indexes",
      "                                 instead of rebuilding them from ref_json.",
      "  -h, --help                     Show this help message.",
      "",
      "Behavior:",
      "  - Rebuilds thin refactor indexes for mixed or older stores.",
      "  - Writes progress logs to stderr and compact JSON to stdout.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    collection_ids = NULL,
    rebuild_collection_indexes = TRUE
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(key, "--no-rebuild-collection-indexes")) {
      out$rebuild_collection_indexes <- FALSE
      i <- i + 1L
      next
    }
    if (identical(key, "--collection-id")) {
      if (i == length(args)) stop("Missing value for --collection-id", call. = FALSE)
      ids <- trimws(strsplit(args[[i + 1L]], ",", fixed = TRUE)[[1]])
      ids <- ids[nzchar(ids)]
      out$collection_ids <- unique(ids)
      i <- i + 2L
      next
    }
    stop("Unknown argument: ", key, call. = FALSE)
  }
  out
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

parsed <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}

cfg <- litxr::litxr_read_config()
log_line("migrating refactor indexes")
report <- litxr::litxr_migrate_refactor_indexes(
  cfg,
  collection_ids = parsed$collection_ids,
  rebuild_collection_indexes = parsed$rebuild_collection_indexes
)
emit_json(c(list(status = "ok"), report))
