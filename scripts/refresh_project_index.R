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
      "  - Reads the current collection fst index for the requested collection.",
      "  - Refreshes the project-level reference and collection-membership indexes only.",
      "  - Only loads full collection rows that are newer than the project index.",
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
    } else if (identical(name, "journal-id")) {
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
journal <- litxr:::.litxr_get_journal(cfg, collection_id)
local_path <- litxr:::.litxr_resolve_local_path(cfg, journal$local_path)
collection_index_path <- litxr:::.litxr_index_path(local_path)
project_index_path <- litxr:::.litxr_project_references_index_path(cfg)
project_links_path <- litxr:::.litxr_project_reference_collections_index_path(cfg)

read_index_columns <- function(path, columns) {
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  fst::read_fst(path, columns = columns, as.data.table = TRUE)
}

collection_meta <- read_index_columns(collection_index_path, c("ref_id", "pub_date"))
if (!nrow(collection_meta)) {
  stop("No local records were found for collection: ", collection_id, call. = FALSE)
}

project_meta <- read_index_columns(project_index_path, c("ref_id", "pub_date"))
project_ref_ids <- if (nrow(project_meta) && "ref_id" %in% names(project_meta)) as.character(project_meta$ref_id) else character()
project_dates <- if (nrow(project_meta) && "pub_date" %in% names(project_meta)) as.Date(project_meta$pub_date) else as.Date(character())
project_max_date <- if (length(project_dates[!is.na(project_dates)])) max(project_dates, na.rm = TRUE) else as.Date("1900-01-01")

collection_dates <- if ("pub_date" %in% names(collection_meta)) as.Date(collection_meta$pub_date) else rep(as.Date(NA), nrow(collection_meta))
needs_refresh <- !collection_meta$ref_id %in% project_ref_ids |
  (!is.na(collection_dates) & collection_dates > project_max_date)
candidate_idx <- which(needs_refresh)

log_line(sprintf(
  "refreshing project index: collection_id=%s collection_rows=%s candidate_rows=%s project_max_date=%s",
  collection_id,
  nrow(collection_meta),
  length(candidate_idx),
  as.character(project_max_date)
))

if (!length(candidate_idx)) {
  emit_json(list(
    status = "ok",
    collection_id = collection_id,
    collection_rows = nrow(collection_meta),
    candidate_rows = 0L,
    project_index_path = project_index_path,
    project_links_path = project_links_path,
    project_max_date = as.character(project_max_date),
    refreshed = FALSE
  ))
  quit(save = "no", status = 0L)
}

candidate_ref_ids <- as.character(collection_meta$ref_id[candidate_idx])
candidate_ref_ids <- candidate_ref_ids[nzchar(candidate_ref_ids)]
candidate_ref_ids <- unique(candidate_ref_ids)

if (length(candidate_idx) == nrow(collection_meta)) {
  candidate_records <- litxr::litxr_read_collection(collection_id, cfg)
} else {
  idx_start <- min(candidate_idx)
  idx_end <- max(candidate_idx)
  candidate_records <- fst::read_fst(collection_index_path, from = idx_start, to = idx_end, as.data.table = TRUE)
  if (nrow(candidate_records) && length(candidate_ref_ids)) {
    candidate_records <- candidate_records[candidate_records$ref_id %in% candidate_ref_ids, ]
  }
}

if (!nrow(candidate_records)) {
  emit_json(list(
    status = "ok",
    collection_id = collection_id,
    collection_rows = nrow(collection_meta),
    candidate_rows = 0L,
    project_index_path = project_index_path,
    project_links_path = project_links_path,
    project_max_date = as.character(project_max_date),
    refreshed = FALSE
  ))
  quit(save = "no", status = 0L)
}

incoming_refs <- litxr:::.litxr_project_references_from_collection_records(candidate_records)
existing_refs <- litxr:::.litxr_read_project_references_index(cfg)
if (nrow(existing_refs)) {
  existing_key <- litxr:::.litxr_upsert_key(existing_refs)
  incoming_key <- litxr:::.litxr_upsert_key(incoming_refs)
  existing_refs <- existing_refs[!existing_key %in% incoming_key, ]
}
merged_refs <- data.table::rbindlist(list(existing_refs, incoming_refs), fill = TRUE)
merged_refs <- merged_refs[!duplicated(litxr:::.litxr_upsert_key(merged_refs))]
litxr:::.litxr_write_project_references_index(cfg, merged_refs)

incoming_links <- data.table::data.table(
  ref_id = candidate_ref_ids,
  collection_id = collection_id,
  collection_title = journal$title,
  recorded_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
)
incoming_links <- incoming_links[!duplicated(incoming_links$ref_id)]
existing_links <- litxr:::.litxr_read_project_reference_collections_index(cfg)
if (nrow(existing_links)) {
  keep_old <- !(existing_links$collection_id == collection_id & existing_links$ref_id %in% incoming_links$ref_id)
  merged_links <- data.table::rbindlist(list(existing_links[keep_old, ], incoming_links), fill = TRUE)
} else {
  merged_links <- incoming_links
}
link_key <- paste(merged_links$ref_id, merged_links$collection_id, sep = "\r")
merged_links <- merged_links[!duplicated(link_key)]
litxr:::.litxr_write_project_reference_collections_index(cfg, merged_links)

emit_json(list(
  status = "ok",
  collection_id = collection_id,
  collection_rows = nrow(collection_meta),
  candidate_rows = nrow(candidate_records),
  project_index_path = project_index_path,
  project_links_path = project_links_path,
  project_max_date = as.character(project_max_date),
  refreshed = TRUE
))
