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
      "  Rscript scripts/refresh_project_index.R --collection-id COLLECTION_ID",
      "",
      "Options:",
      "  --collection-id ID  Collection whose records should refresh the project index.",
      "  --journal-id ID     Compatibility alias for --collection-id.",
      "  -h, --help          Show this help message.",
      "",
      "Behavior:",
      "  - Reads the current local collection records for the requested collection.",
      "  - Refreshes the project-level reference and collection-membership indexes only.",
      "  - Does not rebuild the collection fst index itself.",
      "  - Progress logs are written to stderr; compact JSON is written to stdout.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    collection_id = NULL,
    journal_id = NULL
  )
  i <- 1L

  while (i <= length(args)) {
    key <- args[[i]]
    if (!startsWith(key, "--")) {
      stop("Unexpected positional argument: ", key, call. = FALSE)
    }

    name <- sub("^--", "", key)
    if (name %in% c("help", "h")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }

    if (!name %in% c("collection-id", "journal-id")) {
      stop("Unknown argument: ", key, call. = FALSE)
    }

    if (i == length(args)) {
      stop("Missing value for argument: ", key, call. = FALSE)
    }

    out[[name]] <- args[[i + 1L]]
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

parsed <- parse_args(commandArgs(trailingOnly = TRUE))

if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}

collection_id <- parsed$collection_id
if (is.null(collection_id) || !nzchar(collection_id)) {
  collection_id <- parsed$journal_id
}
if (is.null(collection_id) || !nzchar(collection_id)) {
  usage()
  stop("`--collection-id` is required.", call. = FALSE)
}

script_arg <- commandArgs(trailingOnly = FALSE)
script_file <- sub("^--file=", "", script_arg[grep("^--file=", script_arg)][1])
repo_root <- normalizePath(file.path(dirname(script_file), ".."), winslash = "/", mustWork = TRUE)

if (requireNamespace("pkgload", quietly = TRUE)) {
  pkgload::load_all(repo_root, quiet = TRUE, export_all = FALSE, helpers = FALSE)
} else {
  suppressPackageStartupMessages(library(litxr))
}

cfg <- litxr::litxr_read_config()
journal <- litxr:::.litxr_get_journal(cfg, collection_id)
records <- litxr::litxr_read_collection(collection_id, cfg)

if (!nrow(records)) {
  stop("No local records were found for collection: ", collection_id, call. = FALSE)
}

log_line(sprintf(
  "refreshing project index: collection_id=%s records=%s",
  collection_id,
  nrow(records)
))

litxr:::.litxr_update_project_indexes(cfg, journal, records)

project_index_path <- file.path(
  litxr:::.litxr_project_index_dir(cfg),
  "references.fst"
)

collection_links_path <- file.path(
  litxr:::.litxr_project_index_dir(cfg),
  "reference_collections.fst"
)

emit_json(list(
  status = "ok",
  collection_id = collection_id,
  records = nrow(records),
  project_index_path = project_index_path,
  project_links_path = collection_links_path
))

