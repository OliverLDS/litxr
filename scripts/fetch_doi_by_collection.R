#!/usr/bin/env Rscript

emit_json <- function(x) {
  writeLines(
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE),
    con = stdout()
  )
}

nonempty_or <- function(x, y) {
  if (is.null(x) || (length(x) == 1L && (is.na(x[[1L]]) || !nzchar(as.character(x[[1L]]))))) {
    y
  } else {
    x
  }
}

has_text <- function(x) {
  isTRUE(length(x) == 1L && !is.na(x[[1L]]) && nzchar(trimws(as.character(x[[1L]]))))
}

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/fetch_doi_by_collection.R --collection COLLECTION_ID",
      "",
      "Options:",
      "  --collection ID  Crossref-backed collection id to sync.",
      "  --start DATE     Inclusive start date for update-date filtering.",
      "  --end DATE       Inclusive end date for update-date filtering.",
      "  --full-time      Fetch without any Crossref date filter.",
      "  -h, --help       Show this help message.",
      "",
      "Behavior:",
      "  - Uses Crossref ISSN filters when the collection has ISSNs.",
      "  - Falls back to a Crossref container-title query when the collection has no ISSNs.",
      "  - Uses the latest update timestamp log when present.",
      "  - If the log is missing, infers the cutoff from existing JSON pub_date values in the collection folder.",
      "  - Existing DOIs already present in index/ref_doi.fst are skipped before writing JSON.",
      "  - By default, the script fetches records updated after the current cutoff timestamp.",
      "  - `--start` / `--end` switch the fetch into a date-range mode.",
      "  - `--full-time` fetches the full Crossref journal history.",
      "  - Writes fetched records as JSON files directly under ref/COLLECTION_ID.",
      "  - Updates a project-level latest-update log under `log/` after a successful run.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(help = FALSE, collection = NA_character_, start = NA_character_, end = NA_character_, full_time = FALSE)
  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (identical(arg, "-h") || identical(arg, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(arg, "--collection")) {
      if (i == length(args)) stop("Missing value for --collection", call. = FALSE)
      out$collection <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(arg, "--start")) {
      if (i == length(args)) stop("Missing value for --start", call. = FALSE)
      out$start <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(arg, "--end")) {
      if (i == length(args)) stop("Missing value for --end", call. = FALSE)
      out$end <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(arg, "--full-time")) {
      out$full_time <- TRUE
      i <- i + 1L
      next
    }
    stop("Unknown argument: ", arg, call. = FALSE)
  }
  out
}

infer_cutoff_timestamp <- function(local_path) {
  records <- tryCatch(
    litxr:::.litxr_read_collection_records_from_json(local_path),
    error = function(e) data.table::data.table()
  )
  if (!nrow(records) || !("pub_date" %in% names(records))) {
    return(NA_character_)
  }
  pub_dates <- as.POSIXct(records$pub_date, tz = "UTC")
  pub_dates <- pub_dates[!is.na(pub_dates)]
  if (!length(pub_dates)) {
    return(NA_character_)
  }
  format(max(pub_dates), tz = "UTC", usetz = TRUE)
}

normalize_date_arg <- function(x, arg_name) {
  if (is.null(x) || !nzchar(trimws(x))) {
    return(NA_character_)
  }
  parsed <- suppressWarnings(as.Date(x))
  if (is.na(parsed)) {
    stop("Invalid date for --", arg_name, ": ", x, call. = FALSE)
  }
  as.character(parsed)
}

fetch_crossref_journal_works_filtered <- function(issn, start_date = NULL, end_date = NULL, rows = 1000L) {
  has_scalar_text <- function(x) {
    isTRUE(length(x) == 1L && !is.na(x[[1L]]) && nzchar(as.character(x[[1L]])))
  }
  cursor <- "*"
  out <- list()
  total <- 0L
  filter_parts <- paste0("issn:", issn)
  if (has_scalar_text(start_date)) {
    filter_parts <- c(filter_parts, paste0("from-update-date:", start_date))
  }
  if (has_scalar_text(end_date)) {
    filter_parts <- c(filter_parts, paste0("until-update-date:", end_date))
  }
  filter_string <- paste(filter_parts, collapse = ",")

  repeat {
    req <- httr2::request("https://api.crossref.org/works") |>
      httr2::req_url_query(rows = as.integer(rows), cursor = cursor) |>
      httr2::req_url_query(filter = filter_string)

    resp <- httr2::req_perform(req)
    payload <- httr2::resp_body_json(resp, simplifyVector = FALSE)
    items <- payload$message$items
    if (!length(items)) {
      break
    }

    out <- c(out, items)
    total <- total + length(items)

    if (length(items) < as.integer(rows)) {
      break
    }

    cursor <- payload$message$`next-cursor`
    if (is.null(cursor) || !nzchar(cursor)) {
      break
    }
  }

  out
}

fetch_crossref_container_works_filtered <- function(container_title, start_date = NULL, end_date = NULL, rows = 1000L) {
  has_scalar_text <- function(x) {
    isTRUE(length(x) == 1L && !is.na(x[[1L]]) && nzchar(as.character(x[[1L]])))
  }
  cursor <- "*"
  out <- list()
  total <- 0L
  req_query <- list(
    rows = as.integer(rows),
    cursor = cursor,
    `query.container-title` = container_title
  )
  filter_parts <- character()
  if (has_scalar_text(start_date)) {
    filter_parts <- c(filter_parts, paste0("from-update-date:", start_date))
  }
  if (has_scalar_text(end_date)) {
    filter_parts <- c(filter_parts, paste0("until-update-date:", end_date))
  }
  filter_string <- if (length(filter_parts)) paste(filter_parts, collapse = ",") else NULL

  repeat {
    req <- httr2::request("https://api.crossref.org/works")
    req <- do.call(httr2::req_url_query, c(list(req), req_query))
    if (!is.null(filter_string)) {
      req <- httr2::req_url_query(req, filter = filter_string)
    }

    resp <- httr2::req_perform(req)
    payload <- httr2::resp_body_json(resp, simplifyVector = FALSE)
    items <- payload$message$items
    if (!length(items)) {
      break
    }

    out <- c(out, items)
    total <- total + length(items)

    if (length(items) < as.integer(rows)) {
      break
    }

    cursor <- payload$message$`next-cursor`
    if (is.null(cursor) || !nzchar(cursor)) {
      break
    }
    req_query$cursor <- cursor
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
if (is.na(parsed$collection) || !nzchar(trimws(parsed$collection))) {
  stop("`--collection` is required.", call. = FALSE)
}
if (isTRUE(parsed$full_time) && (has_text(parsed$start) || has_text(parsed$end))) {
  stop("`--full-time` cannot be combined with `--start` or `--end`.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
journal <- litxr:::.litxr_get_journal(cfg, parsed$collection)
if (!identical(journal$remote_channel, "crossref")) {
  stop("Collection is not Crossref-backed: ", parsed$collection, call. = FALSE)
}

collection_ref_dir <- litxr:::.litxr_collection_ref_dir(cfg, parsed$collection)
if (is.na(collection_ref_dir) || !nzchar(collection_ref_dir)) {
  stop("Unable to resolve ref directory for collection: ", parsed$collection, call. = FALSE)
}

paths <- litxr:::.litxr_ensure_collection_ref_dir(cfg, parsed$collection)
update_log_path <- litxr:::.litxr_project_doi_collection_fetch_log_path(cfg)
should_record_latest_update <- !has_text(parsed$end)

log_line("syncing doi collection from Crossref")
log_line("collection_id=", parsed$collection)
log_line("collection_ref_dir=", collection_ref_dir)

cutoff_source <- NA_character_
cutoff_timestamp <- NA_character_
cutoff_date <- NA_character_
fetch_mode <- "incremental"
fetch_start <- NA_character_
fetch_end <- NA_character_
if (isTRUE(parsed$full_time)) {
  fetch_mode <- "full_time"
} else if (has_text(parsed$start) || has_text(parsed$end)) {
  fetch_mode <- "range"
  fetch_start <- normalize_date_arg(parsed$start, "start")
  fetch_end <- normalize_date_arg(parsed$end, "end")
} else {
  cutoff_timestamp <- litxr:::.litxr_latest_collection_sync_timestamp(cfg, parsed$collection, path = update_log_path)
  if (!is.na(cutoff_timestamp) && nzchar(cutoff_timestamp)) {
    cutoff_source <- "log"
  } else {
    cutoff_timestamp <- infer_cutoff_timestamp(collection_ref_dir)
    if (!is.na(cutoff_timestamp) && nzchar(cutoff_timestamp)) {
      cutoff_source <- "pub_date"
    }
  }

  cutoff_posix <- if (!is.na(cutoff_timestamp) && nzchar(cutoff_timestamp)) {
    suppressWarnings(as.POSIXct(cutoff_timestamp, tz = "UTC"))
  } else {
    as.POSIXct(NA, tz = "UTC")
  }
  cutoff_date <- if (!is.na(cutoff_posix)) {
    as.character(as.Date(cutoff_posix))
  } else {
    NA_character_
  }
  if (is.na(cutoff_date) || !nzchar(cutoff_date)) {
    cutoff_source <- NA_character_
    cutoff_timestamp <- NA_character_
    cutoff_date <- NA_character_
  }
}

issns <- litxr:::.litxr_journal_issns(journal)
collection_title <- if (!is.null(journal$title) && length(journal$title) && !is.na(journal$title[[1L]]) && nzchar(as.character(journal$title[[1L]]))) {
  as.character(journal$title[[1L]])
} else {
  parsed$collection
}
use_container_query <- !length(issns)
if (use_container_query && !nzchar(trimws(collection_title))) {
  stop("Crossref-backed collection has no ISSNs and no usable title: ", parsed$collection, call. = FALSE)
}

log_line("fetch_mode=", fetch_mode)
log_line("cutoff_source=", ifelse(is.na(cutoff_source), "none", cutoff_source))
log_line("cutoff_timestamp=", ifelse(is.na(cutoff_timestamp), "none", cutoff_timestamp))
log_line("cutoff_date=", ifelse(is.na(cutoff_date), "none", cutoff_date))
log_line("fetch_start=", ifelse(is.na(fetch_start), "none", fetch_start))
log_line("fetch_end=", ifelse(is.na(fetch_end), "none", fetch_end))
log_line("query_mode=", if (use_container_query) "container-title" else "issn")

items <- if (use_container_query) {
  if (identical(fetch_mode, "full_time")) {
    fetch_crossref_container_works_filtered(container_title = collection_title)
  } else if (identical(fetch_mode, "range")) {
    fetch_crossref_container_works_filtered(container_title = collection_title, start_date = fetch_start, end_date = fetch_end)
  } else {
    fetch_crossref_container_works_filtered(container_title = collection_title, start_date = if (is.na(cutoff_date)) NULL else cutoff_date)
  }
} else {
  unlist(lapply(issns, function(issn) {
    if (identical(fetch_mode, "full_time")) {
      fetch_crossref_journal_works_filtered(issn = issn)
    } else if (identical(fetch_mode, "range")) {
      fetch_crossref_journal_works_filtered(issn = issn, start_date = fetch_start, end_date = fetch_end)
    } else {
      fetch_crossref_journal_works_filtered(issn = issn, start_date = if (is.na(cutoff_date)) NULL else cutoff_date)
    }
  }), recursive = FALSE)
}

if (!length(items)) {
  now <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  if (should_record_latest_update) {
    litxr:::.litxr_upsert_collection_sync_log(cfg, parsed$collection, now, path = update_log_path)
  }
  emit_json(list(
    status = "ok",
    collection_id = parsed$collection,
    fetched = 0L,
    written = 0L,
    skipped_existing = 0L,
    fetch_mode = fetch_mode,
    cutoff_source = nonempty_or(cutoff_source, "none"),
    cutoff_timestamp = nonempty_or(cutoff_timestamp, NA_character_),
    update_log_path = update_log_path
  ))
  quit(save = "no", status = 0L)
}

rows <- lapply(items, function(item) {
  row <- litxr::parse_crossref_entry_unified(item)
  row[["collection_id"]] <- if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id
  row[["collection_title"]] <- journal$title
  row
})
records <- data.table::rbindlist(rows, fill = TRUE)
if (nrow(records)) {
  records <- litxr:::.litxr_deduplicate_records(records)
}

existing_doi_store <- litxr:::.litxr_read_scaffold_table_safe(litxr:::.litxr_ref_doi_path(cfg))
existing_dois <- character()
if (nrow(existing_doi_store) && "doi" %in% names(existing_doi_store)) {
  existing_dois <- trimws(as.character(existing_doi_store$doi))
  existing_dois <- existing_dois[!is.na(existing_dois) & nzchar(existing_dois)]
  existing_dois <- unique(existing_dois)
}
skipped_existing <- 0L
if (nrow(records) && "doi" %in% names(records)) {
  record_dois <- trimws(as.character(records$doi))
  keep_new <- !is.na(record_dois) & nzchar(record_dois)
  if (length(existing_dois)) {
    keep_new <- keep_new & !(record_dois %in% existing_dois)
  }
  skipped_existing <- sum(!keep_new)
  records <- records[keep_new, ]
}

if (!nrow(records)) {
  now <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  if (should_record_latest_update) {
    litxr:::.litxr_upsert_collection_sync_log(cfg, parsed$collection, now, path = update_log_path)
  }
  emit_json(list(
    status = "ok",
    collection_id = parsed$collection,
    fetched = length(items),
    written = 0L,
    skipped_existing = skipped_existing,
    fetch_mode = fetch_mode,
    cutoff_source = nonempty_or(cutoff_source, "none"),
    cutoff_timestamp = nonempty_or(cutoff_timestamp, NA_character_),
    update_log_path = update_log_path
  ))
  quit(save = "no", status = 0L)
}

written_paths <- litxr:::.litxr_write_journal_record_files(records, collection_ref_dir, journal)
latest_update_timestamp <- litxr:::.litxr_latest_collection_ref_json_mtime(cfg, parsed$collection)
if (is.na(latest_update_timestamp) || !nzchar(latest_update_timestamp)) {
  latest_update_timestamp <- format(Sys.time(), tz = "UTC", usetz = TRUE)
}
if (should_record_latest_update) {
  litxr:::.litxr_upsert_collection_sync_log(cfg, parsed$collection, latest_update_timestamp, path = update_log_path)
}

emit_json(list(
  status = "ok",
  collection_id = parsed$collection,
  fetched = length(items),
  written = length(written_paths),
  skipped_existing = skipped_existing,
  fetch_mode = fetch_mode,
  cutoff_source = nonempty_or(cutoff_source, "none"),
  cutoff_timestamp = nonempty_or(cutoff_timestamp, NA_character_),
  update_log_path = update_log_path,
  collection_ref_dir = collection_ref_dir,
  json_dir = paths
))
