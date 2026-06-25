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
      "  Rscript scripts/sync_thin_ref_stores.R [--collection-id ID1,ID2,...] [--json-mtime-after TIMESTAMP]",
      "",
      "Options:",
      "  --collection-id IDS   Optional comma-separated collection ids to sync.",
      "                        Default: all configured collections.",
      "  --json-mtime-after T  Optional cutoff timestamp. When supplied, only",
      "                        JSON files modified after this time are used.",
      "  -h, --help            Show this help message.",
      "",
      "Behavior:",
      "  - Rebuilds the thin canonical reference stores directly from local JSON.",
      "  - Writes progress logs to stderr and compact JSON to stdout.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    collection_ids = NULL,
    json_mtime_after = NULL
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$help <- TRUE
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
    if (identical(key, "--json-mtime-after")) {
      if (i == length(args)) stop("Missing value for --json-mtime-after", call. = FALSE)
      out$json_mtime_after <- args[[i + 1L]]
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
log_line("syncing thin ref stores from json")
report <- litxr::litxr_sync_thin_ref_stores_from_json(
  cfg,
  collection_ids = parsed$collection_ids,
  json_mtime_after = parsed$json_mtime_after
)
emit_json(c(list(status = "ok"), report))
