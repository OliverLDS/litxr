.litxr_project_root <- function(cfg) {
  .litxr_resolve_from_config_root(cfg, cfg$project$data_root)
}

.litxr_path_size <- function(path) {
  if (!file.exists(path)) {
    return(NA_real_)
  }
  as.numeric(file.info(path)$size)
}

.litxr_rds_header_ok <- function(path) {
  if (!file.exists(path)) {
    return(FALSE)
  }
  tryCatch({
    infoRDS(path)
    TRUE
  }, error = function(e) FALSE)
}

.litxr_embedding_cache_file_row <- function(file_type, path, is_dir = FALSE) {
  exists <- if (isTRUE(is_dir)) dir.exists(path) else file.exists(path)
  info <- if (exists) file.info(path) else NULL
  data.table::data.table(
    file_type = as.character(file_type),
    path = as.character(path),
    exists = exists,
    is_dir = isTRUE(is_dir),
    size_bytes = if (exists) as.numeric(info$size) else NA_real_,
    mtime = if (exists) format(as.POSIXct(info$mtime, tz = "UTC"), tz = "UTC", usetz = TRUE) else NA_character_
  )
}

`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

.litxr_project_index_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "index")
}

.litxr_project_log_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "log")
}

.litxr_project_collection_sync_log_path <- function(cfg) {
  file.path(.litxr_project_log_dir(cfg), "collection_sync_latest.tsv")
}

.litxr_project_llm_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "digest", "llm")
}

.litxr_project_llm_history_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "digest", "llm_history")
}

.litxr_project_md_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "md")
}

.litxr_project_ref_identity_index_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "ref_identity_map.fst")
}

.litxr_sync_state_index_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "sync_state.fst")
}

.litxr_ensure_project_index_dir <- function(cfg) {
  dir_path <- .litxr_project_index_dir(cfg)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  dir_path
}

.litxr_ensure_project_log_dir <- function(cfg) {
  dir_path <- .litxr_project_log_dir(cfg)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  dir_path
}

.litxr_empty_collection_sync_log <- function() {
  data.table::data.table(
    collection_id = character(),
    latest_update_timestamp = character()
  )
}

.litxr_read_collection_sync_log <- function(cfg) {
  path <- .litxr_project_collection_sync_log_path(cfg)
  if (!file.exists(path)) {
    return(.litxr_empty_collection_sync_log())
  }
  rows <- tryCatch(
    data.table::fread(path, sep = "\t", header = TRUE, na.strings = c("", "NA")),
    error = function(e) .litxr_empty_collection_sync_log()
  )
  rows <- data.table::as.data.table(rows)
  if (!nrow(rows)) {
    return(.litxr_empty_collection_sync_log())
  }
  if (!("collection_id" %in% names(rows))) rows$collection_id <- character()
  if (!("latest_update_timestamp" %in% names(rows))) rows$latest_update_timestamp <- character()
  rows <- rows[, c("collection_id", "latest_update_timestamp"), with = FALSE]
  rows$collection_id <- as.character(rows$collection_id)
  rows$latest_update_timestamp <- as.character(rows$latest_update_timestamp)
  rows <- rows[!is.na(rows$collection_id) & nzchar(rows$collection_id)]
  rows[!duplicated(rows$collection_id, fromLast = TRUE), ]
}

.litxr_write_collection_sync_log <- function(cfg, rows) {
  path <- .litxr_project_collection_sync_log_path(cfg)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  rows <- data.table::as.data.table(rows)
  if (!nrow(rows)) {
    data.table::fwrite(.litxr_empty_collection_sync_log(), path, sep = "\t")
    return(invisible(path))
  }
  if (!("collection_id" %in% names(rows))) {
    stop("Collection sync log rows require `collection_id`.", call. = FALSE)
  }
  if (!("latest_update_timestamp" %in% names(rows))) {
    stop("Collection sync log rows require `latest_update_timestamp`.", call. = FALSE)
  }
  rows <- rows[, .(
    collection_id = as.character(collection_id),
    latest_update_timestamp = as.character(latest_update_timestamp)
  )]
  rows <- rows[!is.na(collection_id) & nzchar(collection_id)]
  if (nrow(rows)) {
    rows <- rows[!duplicated(collection_id, fromLast = TRUE)]
  }
  data.table::fwrite(rows, path, sep = "\t")
  invisible(path)
}

.litxr_upsert_collection_sync_log <- function(cfg, collection_id, latest_update_timestamp) {
  collection_id <- as.character(collection_id)[[1L]]
  latest_update_timestamp <- as.character(latest_update_timestamp)[[1L]]
  if (is.na(collection_id) || !nzchar(collection_id)) {
    stop("`collection_id` must be non-empty.", call. = FALSE)
  }
  rows <- .litxr_read_collection_sync_log(cfg)
  if (!nrow(rows)) {
    rows <- .litxr_empty_collection_sync_log()
  }
  hit <- which(rows$collection_id == collection_id)
  if (length(hit)) {
    rows$latest_update_timestamp[hit[1L]] <- latest_update_timestamp
  } else {
    rows <- data.table::rbindlist(
      list(
        rows,
        data.table::data.table(collection_id = collection_id, latest_update_timestamp = latest_update_timestamp)
      ),
      fill = TRUE
    )
  }
  .litxr_write_collection_sync_log(cfg, rows)
  invisible(latest_update_timestamp)
}

.litxr_latest_collection_sync_timestamp <- function(cfg, collection_id) {
  collection_id <- as.character(collection_id)[[1L]]
  if (is.na(collection_id) || !nzchar(collection_id)) {
    return(NA_character_)
  }
  rows <- .litxr_read_collection_sync_log(cfg)
  if (!nrow(rows)) {
    return(NA_character_)
  }
  hit <- rows[rows$collection_id == collection_id, ]
  if (!nrow(hit)) {
    return(NA_character_)
  }
  ts <- as.character(hit$latest_update_timestamp[[nrow(hit)]])
  if (!nzchar(ts)) NA_character_ else ts
}

.litxr_refresh_ref_identity_map <- function(cfg, refs = NULL, identities = NULL) {
  if (is.null(identities)) {
    if (is.null(refs)) {
      refs <- .litxr_authoritative_project_records(cfg)
    }
    identities <- .litxr_build_ref_identity_index(cfg, refs = refs)
  }
  .litxr_write_project_ref_identity_index(cfg, identities)
  invisible(identities)
}

.litxr_ensure_project_llm_dir <- function(cfg) {
  dir_path <- .litxr_project_llm_dir(cfg)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  dir_path
}

.litxr_ensure_project_llm_history_dir <- function(cfg) {
  dir_path <- .litxr_project_llm_history_dir(cfg)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  dir_path
}

.litxr_ensure_project_md_dir <- function(cfg) {
  dir_path <- .litxr_project_md_dir(cfg)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  dir_path
}

.litxr_llm_digest_path <- function(cfg, ref_id) {
  file.path(.litxr_project_llm_dir(cfg), paste0(.litxr_record_slug(data.table::data.table(ref_id = ref_id, doi = NA_character_)), ".json"))
}

.litxr_llm_history_ref_dir <- function(cfg, ref_id) {
  file.path(.litxr_project_llm_history_dir(cfg), .litxr_record_slug(data.table::data.table(ref_id = ref_id, doi = NA_character_)))
}

.litxr_llm_digest_history_path <- function(cfg, ref_id, digest) {
  digest <- .litxr_postprocess_llm_digest_read(digest)
  ref_dir <- .litxr_llm_history_ref_dir(cfg, ref_id)
  timestamp <- as.character(digest$updated_at %||% digest$generated_at %||% format(Sys.time(), tz = "UTC", usetz = TRUE))
  timestamp <- gsub(":", "-", timestamp, fixed = TRUE)
  timestamp <- gsub(" ", "T", timestamp, fixed = TRUE)
  timestamp <- gsub("[^A-Za-z0-9._-]+", "_", timestamp)
  revision <- suppressWarnings(as.integer(.litxr_first_nonnull(digest$digest_revision, 1L)))
  if (length(revision) != 1L || is.na(revision[[1]]) || revision[[1]] < 1L) {
    revision <- 1L
  } else {
    revision <- revision[[1]]
  }
  schema_version <- as.character(digest$schema_version[[1]] %||% digest$schema_version %||% "v1")
  file.path(ref_dir, sprintf("%s__rev%03d__%s.json", timestamp, revision, schema_version))
}

.litxr_md_path <- function(cfg, ref_id) {
  file.path(.litxr_project_md_dir(cfg), paste0(.litxr_record_slug(data.table::data.table(ref_id = ref_id, doi = NA_character_)), ".md"))
}

.litxr_enrichment_status_index_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "enrichment_status.fst")
}

.litxr_empty_sync_state <- function() {
  data.table::data.table(
    collection_id = character(),
    remote_channel = character(),
    sync_type = character(),
    status = character(),
    started_at = character(),
    completed_at = character(),
    query = character(),
    range_from = character(),
    range_to = character(),
    fetched_from = character(),
    fetched_to = character(),
    page_start = integer(),
    page_size = integer(),
    records_fetched = integer(),
    records_after = integer(),
    notes = character()
  )
}

.litxr_make_sync_state_row <- function(
  collection_id,
  remote_channel,
  sync_type,
  status,
  started_at,
  completed_at,
  query = NA_character_,
  range_from = NA_character_,
  range_to = NA_character_,
  fetched_from = NA_character_,
  fetched_to = NA_character_,
  page_start = NA_integer_,
  page_size = NA_integer_,
  records_fetched = NA_integer_,
  records_after = NA_integer_,
  notes = NA_character_
) {
  data.table::data.table(
    collection_id = as.character(collection_id %||% NA_character_),
    remote_channel = as.character(remote_channel %||% NA_character_),
    sync_type = as.character(sync_type %||% NA_character_),
    status = as.character(status %||% NA_character_),
    started_at = as.character(started_at %||% NA_character_),
    completed_at = as.character(completed_at %||% NA_character_),
    query = as.character(query %||% NA_character_),
    range_from = as.character(range_from %||% NA_character_),
    range_to = as.character(range_to %||% NA_character_),
    fetched_from = as.character(fetched_from %||% NA_character_),
    fetched_to = as.character(fetched_to %||% NA_character_),
    page_start = as.integer(page_start %||% NA_integer_),
    page_size = as.integer(page_size %||% NA_integer_),
    records_fetched = as.integer(records_fetched %||% NA_integer_),
    records_after = as.integer(records_after %||% NA_integer_),
    notes = as.character(notes %||% NA_character_)
  )
}

.litxr_infer_collection_sync_state <- function(cfg, collection) {
  local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
  paths <- .litxr_journal_paths(local_path)

  record_count <- 0L
  timestamp <- NA_character_
  notes <- character()

  if (dir.exists(paths$json)) {
    json_files <- sort(list.files(paths$json, pattern = "\\.json$", full.names = TRUE))
    json_files <- json_files[basename(json_files) != "_upsert_conflicts.jsonl"]
    if (length(json_files)) {
      info <- file.info(json_files)
      timestamp <- format(as.POSIXct(max(info$mtime, na.rm = TRUE), tz = "UTC"), tz = "UTC", usetz = TRUE)
      record_count <- length(json_files)
      notes <- c(notes, "inferred_from=json_files")
    }
  }

  if (!isTRUE(record_count > 0L) || is.na(timestamp) || !nzchar(timestamp)) {
    return(.litxr_empty_sync_state())
  }

  .litxr_make_sync_state_row(
    collection_id = collection$collection_id,
    remote_channel = collection$remote_channel,
    sync_type = "inferred_rebuild",
    status = "inferred",
    started_at = timestamp,
    completed_at = timestamp,
    query = .litxr_sync_query_text(collection),
    range_from = NA_character_,
    range_to = NA_character_,
    fetched_from = NA_character_,
    fetched_to = NA_character_,
    page_start = NA_integer_,
    page_size = collection$sync$rows %||% if (identical(collection$remote_channel, "crossref")) 1000L else 100L,
    records_fetched = as.integer(record_count),
    records_after = as.integer(record_count),
    notes = paste(notes, collapse = ";")
  )
}

.litxr_records_date_range <- function(records) {
  if (is.null(records) || !nrow(records)) {
    return(list(from = NA_character_, to = NA_character_))
  }

  if ("pub_date" %in% names(records)) {
    dates <- records$pub_date
    dates <- dates[!is.na(dates)]
    if (length(dates)) {
      return(list(
        from = format(min(dates), "%Y-%m-%d"),
        to = format(max(dates), "%Y-%m-%d")
      ))
    }
  }

  if ("year" %in% names(records)) {
    years <- records$year
    years <- years[!is.na(years)]
    if (length(years)) {
      return(list(
        from = as.character(min(years)),
        to = as.character(max(years))
      ))
    }
  }

  list(from = NA_character_, to = NA_character_)
}

.litxr_read_sync_state_index <- function(cfg) {
  path <- .litxr_sync_state_index_path(cfg)
  if (!file.exists(path)) {
    return(.litxr_empty_sync_state())
  }
  fst::read_fst(path, as.data.table = TRUE)
}

.litxr_write_sync_state_index <- function(cfg, rows) {
  .litxr_ensure_project_index_dir(cfg)
  if (is.null(rows) || !nrow(rows)) {
    rows <- .litxr_empty_sync_state()
  }
  fst::write_fst(as.data.frame(rows), .litxr_sync_state_index_path(cfg))
  invisible(.litxr_sync_state_index_path(cfg))
}

.litxr_append_sync_state <- function(cfg, row) {
  existing <- .litxr_read_sync_state_index(cfg)
  incoming <- if (is.null(row) || !nrow(row)) .litxr_empty_sync_state() else row
  merged <- data.table::rbindlist(list(existing, incoming), fill = TRUE)
  .litxr_write_sync_state_index(cfg, merged)
}

.litxr_sync_query_text <- function(journal) {
  if (identical(journal$remote_channel, "arxiv")) {
    return(as.character(journal$sync$search_query %||% NA_character_))
  }
  if (identical(journal$remote_channel, "crossref")) {
    issns <- .litxr_journal_issns(journal)
    if (!length(issns)) return(NA_character_)
    return(paste(issns, collapse = ","))
  }
  NA_character_
}
