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

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    collection = "arxiv_cs_ai",
    arxiv_ids = character(),
    batch_size = 50L,
    force = FALSE
  )

  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (!startsWith(key, "--") && !identical(key, "-h")) {
      stop("Unexpected positional argument: ", key, call. = FALSE)
    }
    if (identical(key, "-h") || identical(key, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(key, "--force")) {
      out$force <- TRUE
      i <- i + 1L
      next
    }
    if (!(key %in% c("--collection", "--arxiv-id", "--arxiv-ids", "--batch-size"))) {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    if (i == length(args)) {
      stop("Missing value for ", key, call. = FALSE)
    }
    value <- args[[i + 1L]]
    if (identical(key, "--collection")) {
      out$collection <- value
    } else if (identical(key, "--batch-size")) {
      out$batch_size <- value
    } else {
      ids <- trimws(strsplit(value, ",", fixed = TRUE)[[1L]])
      ids <- ids[nzchar(ids)]
      out$arxiv_ids <- unique(c(out$arxiv_ids, ids))
    }
    i <- i + 2L
  }

  out
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/fetch_arxiv_ref_json_by_ids.R --arxiv-id ID[,ID2,...] [--collection COLLECTION_ID]",
      "",
      "Options:",
      "  --arxiv-id IDS     One or more arXiv ids, comma-separated.",
      "  --collection ID    arXiv-backed collection id. Default: arxiv_cs_ai.",
      "  --batch-size N     Number of arXiv ids per API request. Default: 50.",
      "  --force            Overwrite existing JSON files if they already exist.",
      "  -h, --help         Show this help message.",
      "",
      "Behavior:",
      "  - Fetches arXiv XML by id list and writes JSON files directly under",
      "    ref/COLLECTION_ID.",
      "  - Existing files are skipped unless --force is supplied.",
      "  - Progress logs are written to stderr; compact JSON is written to stdout.",
      sep = "\n"
    )
  )
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

if (!has_text(parsed$collection)) {
  stop("`--collection` is required.", call. = FALSE)
}
if (!length(parsed$arxiv_ids)) {
  stop("At least one arXiv id must be supplied via `--arxiv-id`.", call. = FALSE)
}

batch_size <- suppressWarnings(as.integer(parsed$batch_size[[1L]]))
if (is.na(batch_size) || batch_size < 1L) {
  stop("`--batch-size` must be a positive integer.", call. = FALSE)
}

script_arg <- commandArgs(trailingOnly = FALSE)
script_file <- sub("^--file=", "", script_arg[grep("^--file=", script_arg)][1L])
repo_root <- normalizePath(file.path(dirname(script_file), ".."), winslash = "/", mustWork = TRUE)

if (requireNamespace("pkgload", quietly = TRUE)) {
  pkgload::load_all(repo_root, quiet = TRUE, export_all = FALSE, helpers = FALSE)
} else {
  suppressPackageStartupMessages(library(litxr))
}

cfg <- litxr::litxr_read_config()
journal <- litxr:::.litxr_get_journal(cfg, parsed$collection)
if (!identical(journal$remote_channel, "arxiv")) {
  stop("Collection is not arXiv-backed: ", parsed$collection, call. = FALSE)
}

collection_ref_dir <- litxr:::.litxr_ensure_collection_ref_dir(cfg, parsed$collection)
log_line("fetching arxiv ref json by id")
log_line("collection_id=", parsed$collection)
log_line("collection_ref_dir=", collection_ref_dir)
log_line("requested_ids=", paste(parsed$arxiv_ids, collapse = ","))

requested_ids <- unique(parsed$arxiv_ids)
requested_ids <- gsub("^arxiv:", "", requested_ids, ignore.case = TRUE)
requested_ids <- sub("v[0-9]+$", "", requested_ids, ignore.case = TRUE)
requested_ids <- requested_ids[nzchar(requested_ids)]

existing <- litxr:::.litxr_read_collection_records_from_json(collection_ref_dir)
existing_ids <- if (nrow(existing) && "ref_id" %in% names(existing)) {
  sub("^arxiv:", "", as.character(existing$ref_id), ignore.case = TRUE)
} else {
  character()
}
requested_ids <- setdiff(requested_ids, existing_ids)

if (!length(requested_ids)) {
  emit_json(list(
    status = "ok",
    collection_id = parsed$collection,
    collection_ref_dir = collection_ref_dir,
    requested = 0L,
    fetched = 0L,
    written = 0L,
    skipped_existing = length(parsed$arxiv_ids)
  ))
  quit(save = "no", status = 0L)
}

fetch_batches <- split(requested_ids, ceiling(seq_along(requested_ids) / batch_size))
records_list <- list()
found_ids <- character()
for (batch in fetch_batches) {
  feed <- litxr::fetch_arxiv_xml(id_vec = batch)
  entries <- xml2::xml_find_all(feed, ".//*[local-name()='entry']")
  if (!length(entries)) {
    next
  }
  for (entry in entries) {
    record <- litxr::parse_arxiv_entry_unified(entry)
    record$collection_id <- parsed$collection
    record$collection_title <- journal$title
    if (!nrow(record)) {
      next
    }
    records_list[[length(records_list) + 1L]] <- record
    found_ids <- c(found_ids, sub("^arxiv:", "", as.character(record$ref_id[[1L]]), ignore.case = TRUE))
  }
}

if (!length(records_list)) {
  emit_json(list(
    status = "ok",
    collection_id = parsed$collection,
    collection_ref_dir = collection_ref_dir,
    requested = length(parsed$arxiv_ids),
    fetched = 0L,
    written = 0L,
    skipped_existing = length(parsed$arxiv_ids),
    missing = requested_ids
  ))
  quit(save = "no", status = 0L)
}

records <- data.table::rbindlist(records_list, fill = TRUE)
records <- records[!duplicated(records$ref_id), ]

if (!isTRUE(parsed$force)) {
  existing_ref_ids <- if (nrow(existing) && "ref_id" %in% names(existing)) as.character(existing$ref_id) else character()
  records <- records[!(records$ref_id %in% existing_ref_ids), ]
}

if (!nrow(records)) {
  emit_json(list(
    status = "ok",
    collection_id = parsed$collection,
    collection_ref_dir = collection_ref_dir,
    requested = length(parsed$arxiv_ids),
    fetched = length(found_ids),
    written = 0L,
    skipped_existing = length(parsed$arxiv_ids),
    missing = setdiff(requested_ids, found_ids)
  ))
  quit(save = "no", status = 0L)
}

written <- litxr:::.litxr_write_journal_record_files(records, collection_ref_dir, journal)
missing_ids <- setdiff(requested_ids, found_ids)
log_line("written=", length(written))
if (length(missing_ids)) {
  log_line("missing_ids=", paste(missing_ids, collapse = ","))
}

emit_json(list(
  status = "ok",
  collection_id = parsed$collection,
  collection_ref_dir = collection_ref_dir,
  requested = length(parsed$arxiv_ids),
  fetched = length(found_ids),
  written = length(written),
  skipped_existing = if (isTRUE(parsed$force)) 0L else length(parsed$arxiv_ids) - length(requested_ids),
  missing = missing_ids,
  written_paths = as.character(written)
))
