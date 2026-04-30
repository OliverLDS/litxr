#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(args) {
  out <- list()
  i <- 1L
  valid_value_args <- c("collection-id", "journal-id", "basis", "date-to", "page-size", "sleep-seconds", "search-query")

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
    if (identical(name, "force")) {
      out$force <- TRUE
      i <- i + 1L
      next
    }
    if (identical(name, "flush-each-day")) {
      out[["flush-each-day"]] <- TRUE
      i <- i + 1L
      next
    }
    if (identical(name, "refresh-project-index")) {
      out[["refresh-project-index"]] <- TRUE
      i <- i + 1L
      next
    }
    if (!name %in% valid_value_args) {
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

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/repair_arxiv_latest.R --collection-id arxiv_cs_ai [--basis collection_index] [--date-to YYYY-MM-DD] [--page-size 200] [--sleep-seconds 10] [--search-query 'cat:cs.AI'] [--force] [--flush-each-day] [--refresh-project-index]",
      "",
      "Notes:",
      "  - LITXR_CONFIG must be set in the environment.",
      "  - `--basis` is `collection_index` by default; use `sync_state` to continue",
      "    from the latest successful day-level sync ledger row instead.",
      "  - This script computes the next repair window and calls",
      "    `scripts/repair_arxiv_range.R` through an absolute path.",
      "  - `--journal-id` still works as a compatibility alias.",
      sep = "\n"
    )
  )
}

script_arg <- commandArgs(trailingOnly = FALSE)
script_file <- sub("^--file=", "", script_arg[grep("^--file=", script_arg)][1])
repo_root <- normalizePath(file.path(dirname(script_file), ".."), winslash = "/", mustWork = TRUE)
range_script <- file.path(repo_root, "scripts", "repair_arxiv_range.R")

parsed <- parse_args(args)

if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}

collection_id <- parsed[["collection-id"]]
if (is.null(collection_id) || !nzchar(collection_id)) {
  collection_id <- parsed[["journal-id"]]
}

if (is.null(collection_id) || !nzchar(collection_id)) {
  usage()
  stop("`--collection-id` is required.", call. = FALSE)
}

if (requireNamespace("pkgload", quietly = TRUE)) {
  pkgload::load_all(repo_root, quiet = TRUE, export_all = FALSE, helpers = FALSE)
} else {
  suppressPackageStartupMessages(library(litxr))
}

cfg <- litxr_read_config()
basis <- if (is.null(parsed$basis)) "collection_index" else parsed$basis
date_to <- if (is.null(parsed[["date-to"]])) Sys.Date() else parsed[["date-to"]]
state <- litxr_next_arxiv_repair_range(
  collection_id,
  cfg,
  basis = basis,
  date_to = date_to
)

if (!isTRUE(state$needs_repair[[1]])) {
  cat(sprintf(
    "latest repair not needed: collection_id=%s basis=%s latest_date=%s date_to=%s\n",
    collection_id,
    basis,
    as.character(state$latest_date[[1]]),
    as.character(state$date_to[[1]])
  ))
  quit(save = "no", status = 0L)
}

cmd_args <- c(
  range_script,
  "--collection-id", collection_id,
  "--date-from", as.character(state$date_from[[1]]),
  "--date-to", as.character(state$date_to[[1]])
)

for (name in c("page-size", "sleep-seconds", "search-query")) {
  if (!is.null(parsed[[name]])) {
    cmd_args <- c(cmd_args, paste0("--", name), parsed[[name]])
  }
}

for (name in c("force", "flush-each-day", "refresh-project-index")) {
  if (isTRUE(parsed[[name]])) {
    cmd_args <- c(cmd_args, paste0("--", name))
  }
}

cat(sprintf(
  "latest repair: collection_id=%s basis=%s date_from=%s date_to=%s\n",
  collection_id,
  basis,
  as.character(state$date_from[[1]]),
  as.character(state$date_to[[1]])
))

status <- system2("Rscript", cmd_args)
if (!identical(status, 0L)) {
  quit(save = "no", status = status)
}
