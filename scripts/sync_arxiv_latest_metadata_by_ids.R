#!/usr/bin/env Rscript

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

emit_json <- function(value) {
  writeLines(jsonlite::toJSON(value, auto_unbox = TRUE, null = "null", pretty = FALSE), stdout())
}

parse_ids <- function(values) {
  values <- as.character(values)
  values <- values[!is.na(values) & nzchar(values)]
  if (!length(values)) return(character())
  ids <- trimws(unlist(strsplit(values, "[,;[:space:]]+", perl = TRUE), use.names = FALSE))
  ids <- ids[nzchar(ids)]
  ids <- vapply(ids, function(id) litxr:::.litxr_bare_arxiv_id(ref_id = id), character(1L))
  ids <- ids[!is.na(ids) & nzchar(ids)]
  unique(ids)
}

parse_args <- function(args) {
  out <- list(help = FALSE, arxiv_ids = character(), batch_size = 50L)
  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (identical(arg, "-h") || identical(arg, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(arg, "--arxiv-id") || identical(arg, "--arxiv-ids")) {
      if (i == length(args)) stop("Missing value for ", arg, call. = FALSE)
      out$arxiv_ids <- c(out$arxiv_ids, args[[i + 1L]])
      i <- i + 2L
      next
    }
    if (identical(arg, "--batch-size")) {
      if (i == length(args)) stop("Missing value for --batch-size", call. = FALSE)
      out$batch_size <- suppressWarnings(as.integer(args[[i + 1L]]))
      i <- i + 2L
      next
    }
    stop("Unknown argument: ", arg, call. = FALSE)
  }
  out
}

usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript scripts/sync_arxiv_latest_metadata_by_ids.R --arxiv-ids ID1,ID2,...",
    "",
    "Options:",
    "  --arxiv-ids IDS  Comma/space-separated bare or canonical arXiv IDs.",
    "  --arxiv-id IDS   Alias for --arxiv-ids.",
    "  --batch-size N   arXiv IDs per API request. Default: 50.",
    "  -h, --help       Show this help message.",
    "",
    "Behavior:",
    "  - Fetches current arXiv API metadata only for the requested IDs.",
    "  - Updates an existing local JSON only when the remote arXiv version is higher.",
    "  - Keeps each record in its existing local collection and syncs thin stores only for changed JSON.",
    "  - Reports checked, updated, unchanged, missing_local, and not_found_remote counts.",
    sep = "\n"
  ))
}

options(error = function() {
  message_text <- trimws(geterrmessage())
  if (!nzchar(message_text)) message_text <- "Unknown error"
  emit_json(list(status = "error", error = message_text))
  quit(save = "no", status = 1L)
})

args <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(args$help)) {
  usage()
  quit(save = "no", status = 0L)
}

arxiv_ids <- parse_ids(args$arxiv_ids)
if (!length(arxiv_ids)) stop("At least one arXiv id is required.", call. = FALSE)
if (is.na(args$batch_size) || args$batch_size < 1L) stop("--batch-size must be a positive integer.", call. = FALSE)

cfg <- litxr::litxr_read_config()
locations <- litxr:::.litxr_ref_json_locations_from_thin_stores(cfg, arxiv_ids)
locations <- locations[
  !is.na(locations$ref_id) & nzchar(locations$ref_id) &
    !is.na(locations$json_path) & nzchar(locations$json_path) & file.exists(locations$json_path),
  ,
  drop = FALSE
]
locations$ref_id <- vapply(locations$ref_id, function(id) litxr:::.litxr_bare_arxiv_id(ref_id = id), character(1L))
locations <- locations[!duplicated(locations$ref_id), ]
local_hit <- match(arxiv_ids, locations$ref_id)
missing_local_ids <- arxiv_ids[is.na(local_hit)]

remote_rows <- list()
remote_count <- 0L
for (start in seq.int(1L, length(arxiv_ids), by = args$batch_size)) {
  batch_ids <- arxiv_ids[start:min(start + args$batch_size - 1L, length(arxiv_ids))]
  feed <- litxr::fetch_arxiv_xml(id_vec = batch_ids)
  entries <- xml2::xml_find_all(feed, ".//*[local-name()='entry']")
  for (entry in entries) {
    row <- litxr::parse_arxiv_entry_unified(entry)
    arxiv_id <- litxr:::.litxr_bare_arxiv_id(ref_id = row$ref_id[[1L]])
    if (!is.na(arxiv_id) && arxiv_id %in% arxiv_ids) {
      remote_count <- remote_count + 1L
      remote_rows[[remote_count]] <- row
    }
  }
}

remote <- if (length(remote_rows)) data.table::rbindlist(remote_rows, fill = TRUE) else data.table::data.table()
if (nrow(remote)) {
  remote$arxiv_id <- vapply(remote$ref_id, function(id) litxr:::.litxr_bare_arxiv_id(ref_id = id), character(1L))
  remote$arxiv_version <- suppressWarnings(as.integer(remote$arxiv_version))
  data.table::setorder(remote, arxiv_id, -arxiv_version)
  remote <- remote[!duplicated(remote$arxiv_id), ]
}
remote_hit <- match(arxiv_ids, if (nrow(remote)) remote$arxiv_id else character())
not_found_remote_ids <- arxiv_ids[is.na(remote_hit)]

collections <- litxr::litxr_list_collections(cfg)
sync_cutoff <- Sys.time() - 1
updated_ids <- character()
unchanged_ids <- character()
changed_collection_ids <- character()

for (arxiv_id in arxiv_ids[!is.na(local_hit) & !is.na(remote_hit)]) {
  location <- locations[match(arxiv_id, locations$ref_id), ]
  local_payload <- litxr:::.litxr_storage_payload_as_list(
    location$json_path[[1L]],
    fields = c("arxiv_version", "arxiv_id_versioned")
  )
  local_version <- litxr:::.litxr_arxiv_version_value(
    version = local_payload$arxiv_version,
    versioned_id = local_payload$arxiv_id_versioned
  )
  remote_row <- remote[match(arxiv_id, remote$arxiv_id), ]
  remote_version <- suppressWarnings(as.integer(remote_row$arxiv_version[[1L]]))
  if (is.na(local_version)) local_version <- 0L
  if (is.na(remote_version) || remote_version <= local_version) {
    unchanged_ids <- c(unchanged_ids, arxiv_id)
    next
  }

  collection_id <- as.character(location$collection_id[[1L]])
  collection_index <- match(collection_id, collections$collection_id)
  if (is.na(collection_index)) stop("Local collection is not registered: ", collection_id, call. = FALSE)
  collection <- collections[collection_index, ]
  remote_row$collection_id <- collection_id
  remote_row$collection_title <- as.character(collection$title[[1L]])
  payload <- litxr:::.litxr_row_to_storage_payload(remote_row, as.list(collection))
  jsonlite::write_json(payload, location$json_path[[1L]], auto_unbox = TRUE, pretty = TRUE, null = "null")
  updated_ids <- c(updated_ids, arxiv_id)
  changed_collection_ids <- unique(c(changed_collection_ids, collection_id))
}

thin_sync <- NULL
if (length(updated_ids)) {
  thin_sync <- litxr::litxr_sync_thin_ref_stores_from_json(
    cfg,
    collection_ids = changed_collection_ids,
    json_mtime_after = sync_cutoff
  )
}

emit_json(list(
  status = "ok",
  checked = length(arxiv_ids),
  updated = length(updated_ids),
  unchanged = length(unchanged_ids),
  missing_local = length(missing_local_ids),
  not_found_remote = length(not_found_remote_ids),
  updated_ids = updated_ids,
  unchanged_ids = unchanged_ids,
  missing_local_ids = missing_local_ids,
  not_found_remote_ids = not_found_remote_ids,
  thin_store_synced = length(updated_ids) > 0L,
  changed_collection_ids = changed_collection_ids
))
