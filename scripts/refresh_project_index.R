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
      "  --collection-id ID  Collection whose project projection should be refreshed.",
      "  --journal-id ID     Compatibility name for --collection-id.",
      "  -h, --help          Show this help message.",
      "",
      "Behavior:",
      "  - Refreshes the normalized scaffold and entity indexes for one collection.",
      "  - Does not rebuild project-reference compatibility projections.",
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

    if (identical(name, "collection-id")) {
      out$collection_id <- args[[i + 1L]]
    } else {
      out$journal_id <- args[[i + 1L]]
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
log_line(sprintf("refreshing normalized scaffold for collection_id=%s", collection_id))

result <- litxr:::.litxr_refresh_project_index_for_collection(cfg, collection_id, repair_collection_index = TRUE)

emit_json(c(
  list(
    status = "ok",
    project_index_path = NA_character_,
    project_links_path = NA_character_
  ),
  result
))
