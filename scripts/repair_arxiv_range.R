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
      "  Rscript scripts/repair_arxiv_range.R --journal-id arxiv_cs_ai --date-from 2026-01-01 --date-to 2026-01-31 [--page-size 200] [--sleep-seconds 10] [--search-query 'cat:cs.AI']",
      "",
      "Notes:",
      "  - LITXR_CONFIG must be set in the environment.",
      "  - The script iterates day by day, paginates within each day with `start`,",
      "    and rebuilds the journal index at the end.",
      sep = "\n"
    )
  )
}

parse_date <- function(x, arg_name) {
  out <- as.Date(x)
  if (is.na(out)) {
    stop("Unable to parse ", arg_name, ": ", x, call. = FALSE)
  }
  out
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
if (is.null(parsed[["date-from"]]) || is.null(parsed[["date-to"]])) {
  usage()
  stop("`--date-from` and `--date-to` are required.", call. = FALSE)
}

date_from <- parse_date(parsed[["date-from"]], "--date-from")
date_to <- parse_date(parsed[["date-to"]], "--date-to")
if (date_to < date_from) {
  stop("`--date-to` must be on or after `--date-from`.", call. = FALSE)
}

page_size <- if (is.null(parsed[["page-size"]])) 200L else as.integer(parsed[["page-size"]])
sleep_seconds <- if (is.null(parsed[["sleep-seconds"]])) 10 else as.numeric(parsed[["sleep-seconds"]])
if (is.na(page_size) || page_size <= 0L) {
  stop("`--page-size` must be a positive integer.", call. = FALSE)
}
if (is.na(sleep_seconds) || sleep_seconds < 0) {
  stop("`--sleep-seconds` must be non-negative.", call. = FALSE)
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
journal <- litxr:::.litxr_get_journal(cfg, parsed[["journal-id"]])
if (!identical(journal$remote_channel, "arxiv")) {
  stop("This script only supports arXiv journals.", call. = FALSE)
}

base_query <- if (is.null(parsed[["search-query"]])) {
  journal$sync$search_query
} else {
  parsed[["search-query"]]
}

local_path <- litxr:::.litxr_resolve_local_path(cfg, journal$local_path)
day_seq <- seq(date_from, date_to, by = "day")
total_incoming <- 0L

for (i in seq_along(day_seq)) {
  day <- day_seq[[i]]
  day_text <- format(as.Date(day, origin = "1970-01-01"), "%Y-%m-%d")
  start <- 0L

  repeat {
    day_journal <- journal
    day_journal$sync$search_query <- litxr:::.litxr_build_arxiv_search_query(
      base_query,
      submitted_from = day_text,
      submitted_to = day_text
    )
    day_journal$sync$start <- start
    day_journal$sync$rows <- page_size
    day_journal$sync$limit <- page_size

    incoming <- litxr:::.litxr_sync_arxiv_journal(day_journal)
    page_n <- nrow(incoming)

    cat(sprintf(
      "date=%s start=%s fetched=%s\n",
      day_text, start, page_n
    ))

    if (page_n == 0L) {
      break
    }

    existing <- litxr:::.litxr_read_journal_records(local_path)
    records <- litxr:::.litxr_upsert_journal_records(existing, incoming, local_path = local_path)
    litxr:::.litxr_write_journal_records(records, local_path, day_journal)

    total_incoming <- total_incoming + page_n

    if (page_n < page_size) {
      break
    }

    start <- start + page_size
    if (sleep_seconds > 0) {
      Sys.sleep(sleep_seconds)
    }
  }

  if (sleep_seconds > 0) {
    Sys.sleep(sleep_seconds)
  }
}

litxr_rebuild_journal_index(parsed[["journal-id"]], cfg)

cat(sprintf(
  "range repair complete: journal_id=%s date_from=%s date_to=%s total_incoming=%s\n",
  parsed[["journal-id"]],
  format(date_from, "%Y-%m-%d"),
  format(date_to, "%Y-%m-%d"),
  total_incoming
))
