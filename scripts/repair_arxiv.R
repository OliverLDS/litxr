#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(args) {
  out <- list()
  i <- 1L

  while (i <= length(args)) {
    key <- args[[i]]
    if (!startsWith(key, "--")) {
      stop("Unexpected positional argument: ", key, call. = FALSE)
    }

    name <- sub("^--", "", key)
    if (identical(name, "help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }

    if (i == length(args)) {
      stop("Missing value for argument: ", key, call. = FALSE)
    }

    out[[name]] <- args[[i + 1L]]
    i <- i + 2L
  }

  out
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/repair_arxiv.R --journal-id arxiv_cs_ai [--submitted-from YYYY-MM-DD] [--submitted-to YYYY-MM-DD] [--start 0] [--limit 500] [--search-query 'cat:cs.AI']",
      "",
      "Notes:",
      "  - LITXR_CONFIG must be set in the environment.",
      "  - This script calls litxr_repair_journal() and then litxr_rebuild_journal_index().",
      sep = "\n"
    )
  )
}

parsed <- parse_args(args)

if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}

if (is.null(parsed[["journal-id"]]) || !nzchar(parsed[["journal-id"]])) {
  usage()
  stop("`--journal-id` is required.", call. = FALSE)
}

script_arg <- commandArgs(trailingOnly = FALSE)
script_file <- sub("^--file=", "", script_arg[grep("^--file=", script_arg)][1])
repo_root <- normalizePath(file.path(dirname(script_file), ".."), winslash = "/", mustWork = TRUE)

if (requireNamespace("pkgload", quietly = TRUE)) {
  pkgload::load_all(repo_root, quiet = TRUE, export_all = FALSE, helpers = FALSE)
} else {
  suppressPackageStartupMessages(library(litxr))
}

cfg <- litxr_read_config()

records <- litxr_repair_journal(
  journal_id = parsed[["journal-id"]],
  config = cfg,
  search_query = parsed[["search-query"]],
  submitted_from = parsed[["submitted-from"]],
  submitted_to = parsed[["submitted-to"]],
  start = if (is.null(parsed$start)) NULL else as.integer(parsed$start),
  limit = if (is.null(parsed$limit)) NULL else as.integer(parsed$limit)
)

litxr_rebuild_journal_index(parsed[["journal-id"]], cfg)

cat(
  sprintf(
    "repair complete: journal_id=%s rows=%s\n",
    parsed[["journal-id"]],
    nrow(records)
  )
)
