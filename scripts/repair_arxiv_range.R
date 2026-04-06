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
    if (identical(name, "force")) {
      out$force <- TRUE
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
      "  Rscript scripts/repair_arxiv_range.R --collection-id arxiv_cs_ai --date-from 2026-01-01 --date-to 2026-01-31 [--page-size 200] [--sleep-seconds 10] [--search-query 'cat:cs.AI'] [--force]",
      "",
      "Notes:",
      "  - LITXR_CONFIG must be set in the environment.",
      "  - The script iterates day by day, paginates within each day with `start`,",
      "    records successful days in the project sync ledger, and rebuilds the",
      "    collection index at the end.",
      "  - `--journal-id` still works as a compatibility alias.",
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

collection_id <- parsed[["collection-id"]]
if (is.null(collection_id) || !nzchar(collection_id)) {
  collection_id <- parsed[["journal-id"]]
}

if (is.null(collection_id) || !nzchar(collection_id)) {
  usage()
  stop("`--collection-id` is required.", call. = FALSE)
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
journal <- litxr:::.litxr_get_journal(cfg, collection_id)
if (!identical(journal$remote_channel, "arxiv")) {
  stop("This script only supports arXiv collections.", call. = FALSE)
}
range_started_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)

base_query <- if (is.null(parsed[["search-query"]])) {
  journal$sync$search_query
} else {
  parsed[["search-query"]]
}
sync_state <- litxr_read_sync_state(cfg, collection_id)

local_path <- litxr:::.litxr_resolve_local_path(cfg, journal$local_path)
day_seq <- seq(date_from, date_to, by = "day")
total_incoming <- 0L

for (i in seq_along(day_seq)) {
  day <- day_seq[[i]]
  day_text <- format(as.Date(day, origin = "1970-01-01"), "%Y-%m-%d")
  day_done <- !isTRUE(parsed$force) &&
    nrow(sync_state) &&
    any(
      sync_state$collection_id == collection_id &
        sync_state$remote_channel == "arxiv" &
        sync_state$sync_type == "repair_range_day" &
        sync_state$status == "success" &
        sync_state$range_from == day_text &
        sync_state$range_to == day_text &
        sync_state$query == base_query
    )

  if (day_done) {
    cat(sprintf("date=%s skipped=already_completed\n", day_text))
    next
  }

  day_started_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  start <- 0L
  day_total <- 0L

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
    litxr:::.litxr_write_journal_records(records, local_path, day_journal, cfg = cfg)

    total_incoming <- total_incoming + page_n
    day_total <- day_total + page_n

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

  litxr:::.litxr_append_sync_state(cfg, litxr:::.litxr_make_sync_state_row(
    collection_id = collection_id,
    remote_channel = "arxiv",
    sync_type = "repair_range_day",
    status = "success",
    started_at = day_started_at,
    completed_at = format(Sys.time(), tz = "UTC", usetz = TRUE),
    query = base_query,
    range_from = day_text,
    range_to = day_text,
    page_start = 0L,
    page_size = page_size,
    records_fetched = day_total,
    records_after = NA_integer_,
    notes = ""
  ))
}

litxr_rebuild_collection_index(collection_id, cfg)

litxr:::.litxr_append_sync_state(cfg, litxr:::.litxr_make_sync_state_row(
  collection_id = collection_id,
  remote_channel = "arxiv",
  sync_type = "repair_range",
  status = "success",
  started_at = range_started_at,
  completed_at = format(Sys.time(), tz = "UTC", usetz = TRUE),
  query = base_query,
  range_from = format(date_from, "%Y-%m-%d"),
  range_to = format(date_to, "%Y-%m-%d"),
  page_start = 0L,
  page_size = page_size,
  records_fetched = total_incoming,
  records_after = NA_integer_,
  notes = if (isTRUE(parsed$force)) "force=TRUE" else ""
))

cat(sprintf(
  "range repair complete: collection_id=%s date_from=%s date_to=%s total_incoming=%s\n",
  collection_id,
  format(date_from, "%Y-%m-%d"),
  format(date_to, "%Y-%m-%d"),
  total_incoming
))
