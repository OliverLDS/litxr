#!/usr/bin/env Rscript

emit_json <- function(x) {
  writeLines(
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE),
    con = stdout()
  )
}

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

has_text <- function(x) {
  isTRUE(length(x) == 1L && !is.na(x[[1L]]) && nzchar(trimws(as.character(x[[1L]]))))
}

normalize_date_arg <- function(x, arg_name) {
  if (is.null(x) || !has_text(x)) {
    return(NA_character_)
  }
  parsed <- suppressWarnings(as.Date(x[[1L]]))
  if (is.na(parsed)) {
    stop("Invalid date for --", arg_name, ": ", x[[1L]], call. = FALSE)
  }
  as.character(parsed)
}

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    collection = NA_character_,
    start = NA_character_,
    end = NA_character_,
    page_size = NULL,
    sleep_seconds = NULL,
    search_query = NA_character_,
    force = FALSE
  )

  i <- 1L
  valid_value_args <- c("collection", "collection-id", "journal-id", "start", "end", "page-size", "sleep-seconds", "search-query")
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
    if (!name %in% valid_value_args) {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    if (i == length(args)) {
      stop("Missing value for argument: ", key, call. = FALSE)
    }
    value <- args[[i + 1L]]
    if (identical(name, "collection") || identical(name, "collection-id") || identical(name, "journal-id")) {
      out$collection <- value
    } else if (identical(name, "start")) {
      out$start <- value
    } else if (identical(name, "end")) {
      out$end <- value
    } else if (identical(name, "page-size")) {
      out$page_size <- value
    } else if (identical(name, "sleep-seconds")) {
      out$sleep_seconds <- value
    } else if (identical(name, "search-query")) {
      out$search_query <- value
    }
    i <- i + 2L
  }

  out
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/sync_arxiv_collection_json.R --collection COLLECTION_ID [--start YYYY-MM-DD --end YYYY-MM-DD]",
      "",
      "Options:",
      "  --collection ID    arXiv-backed collection id to sync.",
      "  --start DATE       Inclusive start date for arXiv submittedDate filtering.",
      "  --end DATE         Inclusive end date for arXiv submittedDate filtering.",
      "  --page-size N      Page size for arXiv API calls.",
      "  --sleep-seconds S  Delay between arXiv requests.",
      "  --search-query Q   Override the configured arXiv search query.",
      "  --force            Re-run days already recorded in the collection history.",
      "  -h, --help         Show this help message.",
      "",
      "Behavior:",
      "  - Default mode syncs from the latest completed day recorded in",
      "    log/<collection>_collection_fetch_history.tsv.",
      "  - If `--start` and `--end` are supplied, the script syncs that date range.",
      "  - If no history exists yet, the script performs a one-time bootstrap fetch",
      "    using the configured arXiv query, writes JSON files, and infers the",
      "    collection history from the fetched records.",
      "  - Successful days are recorded in the per-collection fetch-history TSV.",
      "  - JSON files are written directly under ref/COLLECTION_ID.",
      sep = "\n"
    )
  )
}

infer_history_from_records <- function(records) {
  if (is.null(records) || !nrow(records) || !("pub_date" %in% names(records))) {
    return(data.table::data.table(
      completed_collection_date = character(),
      total_ref_jsons = integer()
    ))
  }
  pub_dates <- as.Date(records$pub_date)
  pub_dates <- pub_dates[!is.na(pub_dates)]
  if (!length(pub_dates)) {
    return(data.table::data.table(
      completed_collection_date = character(),
      total_ref_jsons = integer()
    ))
  }
  days <- sort(unique(pub_dates))
  data.table::data.table(
    completed_collection_date = as.character(days),
    total_ref_jsons = as.integer(vapply(days, function(day) sum(pub_dates == day), integer(1)))
  )
}

write_history_if_needed <- function(cfg, collection_id, records) {
  history <- infer_history_from_records(records)
  if (nrow(history)) {
    litxr:::.litxr_write_collection_fetch_history(cfg, collection_id, history)
  }
  invisible(history)
}

sync_records_by_query <- function(journal, collection_ref_dir, query, page_size, sleep_seconds) {
  day_journal <- journal
  day_journal$sync$search_query <- query
  day_journal$sync$start <- if (is.null(day_journal$sync$start)) 0L else as.integer(day_journal$sync$start)
  day_journal$sync$rows <- page_size
  day_journal$sync$limit <- page_size

  start <- day_journal$sync$start[[1L]]
  total_written <- 0L
  repeat {
    day_journal$sync$start <- start
    if (sleep_seconds > 0) {
      Sys.sleep(sleep_seconds)
    }
    incoming <- litxr:::.litxr_sync_arxiv_journal(day_journal)
    page_n <- nrow(incoming)
    if (!page_n) {
      break
    }
    litxr:::.litxr_write_journal_record_files(incoming, collection_ref_dir, day_journal)
    total_written <- total_written + page_n
    if (page_n < page_size) {
      break
    }
    start <- start + page_size
  }
  total_written
}

sync_one_day <- function(cfg, journal, collection_id, collection_ref_dir, base_query, day_text, page_size, sleep_seconds) {
  query <- litxr:::.litxr_build_arxiv_search_query(
    base_query,
    submitted_from = day_text,
    submitted_to = day_text
  )
  total_written <- sync_records_by_query(journal, collection_ref_dir, query, page_size, sleep_seconds)
  litxr:::.litxr_append_collection_fetch_history(cfg, collection_id, day_text, total_written)
  total_written
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

collection_id <- as.character(parsed$collection)[[1L]]
if (is.na(collection_id) || !nzchar(trimws(collection_id))) {
  stop("`--collection` is required.", call. = FALSE)
}

if (has_text(parsed$start) != has_text(parsed$end)) {
  stop("`--start` and `--end` must be provided together.", call. = FALSE)
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
if (!identical(journal$remote_channel, "arxiv")) {
  stop("Collection is not arXiv-backed: ", collection_id, call. = FALSE)
}

collection_ref_dir <- litxr:::.litxr_collection_ref_dir(cfg, collection_id)
litxr:::.litxr_ensure_collection_ref_dir(cfg, collection_id)
history_path <- litxr:::.litxr_collection_fetch_history_path(cfg, collection_id)

page_size <- if (is.null(parsed$page_size)) {
  if (is.null(journal$sync$rows)) 200L else as.integer(journal$sync$rows)
} else {
  as.integer(parsed$page_size)
}
if (is.na(page_size) || page_size <= 0L) {
  stop("`--page-size` must be a positive integer.", call. = FALSE)
}
sleep_seconds <- if (is.null(parsed$sleep_seconds)) {
  if (is.null(journal$sync$delay_seconds)) 3 else as.numeric(journal$sync$delay_seconds)
} else {
  as.numeric(parsed$sleep_seconds)
}
if (is.na(sleep_seconds) || sleep_seconds < 0) {
  stop("`--sleep-seconds` must be non-negative.", call. = FALSE)
}

base_query <- if (has_text(parsed$search_query)) parsed$search_query else journal$sync$search_query
if (!has_text(base_query)) {
  stop("arXiv collection requires a non-empty search query.", call. = FALSE)
}

log_line("syncing arxiv collection from arXiv")
log_line("collection_id=", collection_id)
log_line("collection_ref_dir=", collection_ref_dir)
log_line("history_path=", history_path)

history <- litxr:::.litxr_read_collection_fetch_history(cfg, collection_id)
latest_done <- litxr:::.litxr_latest_collection_fetch_completed_date(cfg, collection_id)

if (has_text(parsed$start)) {
  start_date <- normalize_date_arg(parsed$start, "start")
  end_date <- normalize_date_arg(parsed$end, "end")
  fetch_mode <- "range"
} else if (has_text(latest_done)) {
  start_date <- as.character(as.Date(latest_done) + 1L)
  end_date <- format(Sys.Date(), "%Y-%m-%d")
  fetch_mode <- "since_latest"
} else {
  start_date <- NA_character_
  end_date <- NA_character_
  fetch_mode <- "bootstrap"
}

log_line("fetch_mode=", fetch_mode)
log_line("start_date=", ifelse(is.na(start_date) || !nzchar(start_date), "none", start_date))
log_line("end_date=", ifelse(is.na(end_date) || !nzchar(end_date), "none", end_date))
log_line("latest_history_date=", ifelse(is.na(latest_done) || !nzchar(latest_done), "none", latest_done))

total_written <- 0L
days_recorded <- 0L
days_skipped <- 0L
overall_fetched_from <- NA_character_
overall_fetched_to <- NA_character_

if (identical(fetch_mode, "bootstrap")) {
  total_written <- sync_records_by_query(journal, collection_ref_dir, base_query, page_size, sleep_seconds)
  incoming <- tryCatch(
    litxr:::.litxr_read_journal_records_from_json(collection_ref_dir),
    error = function(e) data.table::data.table()
  )
  history_rows <- write_history_if_needed(cfg, collection_id, incoming)
  days_recorded <- nrow(history_rows)
  overall_fetched_from <- if (nrow(history_rows)) min(history_rows$completed_collection_date) else NA_character_
  overall_fetched_to <- if (nrow(history_rows)) max(history_rows$completed_collection_date) else NA_character_
} else {
  start_day <- as.Date(start_date)
  end_day <- as.Date(end_date)
  if (is.na(start_day) || is.na(end_day)) {
    stop("Unable to resolve arXiv sync day range.", call. = FALSE)
  }
  if (end_day < start_day) {
    emit_json(list(
      status = "ok",
      collection_id = collection_id,
      collection_ref_dir = collection_ref_dir,
      history_path = history_path,
      fetch_mode = fetch_mode,
      start_date = start_date,
      end_date = end_date,
      total_written = 0L,
      days_recorded = 0L,
      days_skipped = 0L,
      latest_history_date = ifelse(is.na(latest_done) || !nzchar(latest_done), NA_character_, latest_done),
      overall_fetched_from = NA_character_,
      overall_fetched_to = NA_character_
    ))
    quit(save = "no", status = 0L)
  }

  history_lookup <- if (nrow(history)) {
    stats::setNames(history$total_ref_jsons, history$completed_collection_date)
  } else {
    numeric()
  }

  day_seq <- seq(start_day, end_day, by = "day")
  for (day in day_seq) {
    day_text <- format(day, "%Y-%m-%d")
    history_hit <- if (length(history_lookup) && !is.null(names(history_lookup)) && day_text %in% names(history_lookup)) {
      history_lookup[[day_text]]
    } else {
      NULL
    }
    if (!isTRUE(parsed$force) && !is.null(history_hit) && !is.na(history_hit)) {
      log_line("date=", day_text, " skipped=already_completed")
      days_skipped <- days_skipped + 1L
      next
    }

    total_day_written <- sync_one_day(
      cfg = cfg,
      journal = journal,
      collection_id = collection_id,
      collection_ref_dir = collection_ref_dir,
      base_query = base_query,
      day_text = day_text,
      page_size = page_size,
      sleep_seconds = sleep_seconds
    )

    total_written <- total_written + total_day_written
    days_recorded <- days_recorded + 1L
    if (is.na(overall_fetched_from) || day_text < overall_fetched_from) {
      overall_fetched_from <- day_text
    }
    if (is.na(overall_fetched_to) || day_text > overall_fetched_to) {
      overall_fetched_to <- day_text
    }
    log_line("date=", day_text, " fetched=", total_day_written)
  }
}

emit_json(list(
  status = "ok",
  collection_id = collection_id,
  collection_ref_dir = collection_ref_dir,
  history_path = history_path,
  fetch_mode = fetch_mode,
  start_date = ifelse(is.na(start_date) || !nzchar(start_date), NA_character_, start_date),
  end_date = ifelse(is.na(end_date) || !nzchar(end_date), NA_character_, end_date),
  total_written = total_written,
  days_recorded = days_recorded,
  days_skipped = days_skipped,
  latest_history_date = ifelse(is.na(latest_done) || !nzchar(latest_done), NA_character_, latest_done),
  overall_fetched_from = overall_fetched_from,
  overall_fetched_to = overall_fetched_to
))
