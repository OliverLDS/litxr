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

.litxr_normalize_calendar_date <- function(x) {
  if (is.null(x) || !length(x)) {
    return(NA_character_)
  }
  if (inherits(x, "Date")) {
    return(as.character(x[[1L]]))
  }
  if (inherits(x, c("POSIXct", "POSIXt"))) {
    return(as.character(as.Date(x[[1L]], tz = "UTC")))
  }

  text <- as.character(x[[1L]])
  text <- trimws(text)
  if (!nzchar(text) || is.na(text)) {
    return(NA_character_)
  }
  if (grepl("^[0-9]{8}$", text)) {
    parsed <- suppressWarnings(as.Date(text, format = "%Y%m%d"))
    if (!is.na(parsed)) return(as.character(parsed))
  }
  if (grepl("^[0-9]+$", text) && nchar(text) <= 7L) {
    parsed <- suppressWarnings(as.Date(as.integer(text), origin = "1970-01-01"))
    if (!is.na(parsed)) return(as.character(parsed))
  }
  parsed <- suppressWarnings(as.Date(text))
  if (!is.na(parsed)) {
    return(as.character(parsed))
  }
  NA_character_
}

.litxr_project_index_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "index")
}

.litxr_project_log_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "log")
}

.litxr_project_collection_thin_sync_log_path <- function(cfg) {
  file.path(.litxr_project_log_dir(cfg), "collection_thin_sync_latest.tsv")
}

.litxr_project_doi_collection_fetch_log_path <- function(cfg) {
  file.path(.litxr_project_log_dir(cfg), "doi_collection_fetch_latest.tsv")
}

.litxr_collection_ref_dir <- function(cfg, collection_id) {
  collection_id <- as.character(collection_id)[[1L]]
  file.path(.litxr_project_root(cfg), "ref", collection_id)
}

.litxr_ensure_collection_ref_dir <- function(cfg, collection_id) {
  dir_path <- .litxr_collection_ref_dir(cfg, collection_id)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  dir_path
}

.litxr_latest_collection_ref_json_mtime <- function(cfg, collection_id) {
  dir_path <- .litxr_collection_ref_dir(cfg, collection_id)
  if (!dir.exists(dir_path)) {
    return(NA_character_)
  }
  files <- sort(list.files(dir_path, pattern = "\\.json$", full.names = TRUE))
  if (!length(files)) {
    return(NA_character_)
  }
  info <- file.info(files)
  mtimes <- info$mtime[!is.na(info$mtime)]
  if (!length(mtimes)) {
    return(NA_character_)
  }
  format(max(as.POSIXct(mtimes, tz = "UTC")), tz = "UTC", usetz = TRUE)
}

.litxr_collection_fetch_history_path <- function(cfg, collection_id) {
  collection_id <- as.character(collection_id)[[1L]]
  file.path(.litxr_project_log_dir(cfg), paste0(collection_id, "_collection_fetch_history.tsv"))
}

.litxr_empty_collection_fetch_history <- function() {
  data.table::data.table(
    completed_collection_date = character(),
    total_ref_jsons = integer()
  )
}

.litxr_infer_collection_fetch_history <- function(cfg, collection_id) {
  collection_id <- as.character(collection_id)[[1L]]
  dir_path <- .litxr_collection_ref_dir(cfg, collection_id)
  if (!dir.exists(dir_path)) {
    return(.litxr_empty_collection_fetch_history())
  }

  records <- tryCatch(
    .litxr_read_journal_records_from_json(dir_path),
    error = function(e) data.table::data.table()
  )
  if (!nrow(records) || !("pub_date" %in% names(records))) {
    return(.litxr_empty_collection_fetch_history())
  }

  pub_dates <- as.Date(records$pub_date)
  pub_dates <- pub_dates[!is.na(pub_dates)]
  if (!length(pub_dates)) {
    return(.litxr_empty_collection_fetch_history())
  }

  days <- sort(unique(pub_dates))
  counts <- data.table::data.table(
    completed_collection_date = as.character(days),
    total_ref_jsons = as.integer(vapply(days, function(day) {
      sum(pub_dates == day)
    }, integer(1)))
  )
  data.table::setorder(counts, completed_collection_date)
  counts
}

.litxr_read_collection_fetch_history <- function(cfg, collection_id) {
  path <- .litxr_collection_fetch_history_path(cfg, collection_id)
  if (!file.exists(path)) {
    history <- .litxr_infer_collection_fetch_history(cfg, collection_id)
    if (nrow(history)) {
      .litxr_write_collection_fetch_history(cfg, collection_id, history)
    }
    return(history)
  }
  rows <- tryCatch(
    data.table::fread(path, sep = "\t", header = TRUE, na.strings = c("", "NA")),
    error = function(e) .litxr_empty_collection_fetch_history()
  )
  rows <- data.table::as.data.table(rows)
  required_cols <- c("completed_collection_date", "total_ref_jsons")
  if (!nrow(rows) || !all(required_cols %in% names(rows))) {
    return(.litxr_empty_collection_fetch_history())
  }
  rows <- rows[, required_cols, with = FALSE]
  rows$completed_collection_date <- vapply(rows$completed_collection_date, .litxr_normalize_calendar_date, character(1))
  rows$total_ref_jsons <- suppressWarnings(as.integer(rows$total_ref_jsons))
  keep <- !is.na(rows[["completed_collection_date"]]) & nzchar(rows[["completed_collection_date"]])
  rows <- rows[which(keep), , drop = FALSE]
  if (nrow(rows)) {
    rows <- rows[which(!duplicated(rows[["completed_collection_date"]], fromLast = TRUE)), , drop = FALSE]
    data.table::setorder(rows, completed_collection_date)
  }
  rows
}

.litxr_write_collection_fetch_history <- function(cfg, collection_id, rows) {
  path <- .litxr_collection_fetch_history_path(cfg, collection_id)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  rows <- data.table::as.data.table(rows)
  if (!nrow(rows)) {
    data.table::fwrite(.litxr_empty_collection_fetch_history(), path, sep = "\t")
    return(invisible(path))
  }
  if (!("completed_collection_date" %in% names(rows))) {
    stop("Collection fetch history rows require `completed_collection_date`.", call. = FALSE)
  }
  if (!("total_ref_jsons" %in% names(rows))) {
    stop("Collection fetch history rows require `total_ref_jsons`.", call. = FALSE)
  }
  rows <- data.table::data.table(
    completed_collection_date = vapply(rows$completed_collection_date, .litxr_normalize_calendar_date, character(1)),
    total_ref_jsons = suppressWarnings(as.integer(rows$total_ref_jsons))
  )
  rows <- rows[!is.na(rows$completed_collection_date) & nzchar(rows$completed_collection_date), , drop = FALSE]
  if (nrow(rows)) {
    rows <- rows[!duplicated(rows$completed_collection_date, fromLast = TRUE), , drop = FALSE]
    data.table::setorder(rows, completed_collection_date)
  }
  data.table::fwrite(rows, path, sep = "\t")
  invisible(path)
}

.litxr_append_collection_fetch_history <- function(cfg, collection_id, completed_collection_date, total_ref_jsons) {
  collection_id <- as.character(collection_id)[[1L]]
  completed_collection_date <- .litxr_normalize_calendar_date(completed_collection_date)
  total_ref_jsons <- suppressWarnings(as.integer(total_ref_jsons))
  if (length(total_ref_jsons) < 1L || is.na(total_ref_jsons[[1L]])) {
    total_ref_jsons <- NA_integer_
  } else {
    total_ref_jsons <- total_ref_jsons[[1L]]
  }
  if (is.na(completed_collection_date) || !nzchar(completed_collection_date)) {
    stop("`completed_collection_date` must be non-empty.", call. = FALSE)
  }
  rows <- .litxr_read_collection_fetch_history(cfg, collection_id)
  hit <- which(rows$completed_collection_date == completed_collection_date)
  if (length(hit)) {
    rows$total_ref_jsons[hit[1L]] <- total_ref_jsons
  } else {
    rows <- data.table::rbindlist(
      list(
        rows,
        data.table::data.table(
          completed_collection_date = completed_collection_date,
          total_ref_jsons = total_ref_jsons
        )
      ),
      fill = TRUE
    )
  }
  .litxr_write_collection_fetch_history(cfg, collection_id, rows)
  invisible(total_ref_jsons)
}

.litxr_latest_collection_fetch_completed_date <- function(cfg, collection_id) {
  rows <- .litxr_read_collection_fetch_history(cfg, collection_id)
  if (!nrow(rows)) {
    return(NA_character_)
  }
  .litxr_normalize_calendar_date(rows$completed_collection_date[[nrow(rows)]])
}

.litxr_latest_collection_fetch_completed_date_nonzero <- function(cfg, collection_id) {
  rows <- .litxr_read_collection_fetch_history(cfg, collection_id)
  if (!nrow(rows)) {
    return(NA_character_)
  }
  rows$total_ref_jsons <- suppressWarnings(as.integer(rows$total_ref_jsons))
  keep <- !is.na(rows$total_ref_jsons) & rows$total_ref_jsons > 0L
  if (!any(keep)) {
    return(NA_character_)
  }
  rows <- rows[keep, , drop = FALSE]
  .litxr_normalize_calendar_date(rows$completed_collection_date[[nrow(rows)]])
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

.litxr_read_collection_sync_log <- function(cfg, path = .litxr_project_collection_thin_sync_log_path(cfg)) {
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

.litxr_write_collection_sync_log <- function(cfg, rows, path = .litxr_project_collection_thin_sync_log_path(cfg)) {
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
  rows <- data.table::data.table(
    collection_id = as.character(rows$collection_id),
    latest_update_timestamp = as.character(rows$latest_update_timestamp)
  )
  rows <- rows[!is.na(rows$collection_id) & nzchar(rows$collection_id), , drop = FALSE]
  if (nrow(rows)) {
    rows <- rows[!duplicated(rows$collection_id, fromLast = TRUE), , drop = FALSE]
  }
  data.table::fwrite(rows, path, sep = "\t")
  invisible(path)
}

.litxr_upsert_collection_sync_log <- function(cfg, collection_id, latest_update_timestamp, path = .litxr_project_collection_thin_sync_log_path(cfg)) {
  collection_id <- as.character(collection_id)[[1L]]
  latest_update_timestamp <- as.character(latest_update_timestamp)[[1L]]
  if (is.na(collection_id) || !nzchar(collection_id)) {
    stop("`collection_id` must be non-empty.", call. = FALSE)
  }
  rows <- .litxr_read_collection_sync_log(cfg, path = path)
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
  .litxr_write_collection_sync_log(cfg, rows, path = path)
  invisible(latest_update_timestamp)
}

.litxr_latest_collection_sync_timestamp <- function(cfg, collection_id, path = .litxr_project_collection_thin_sync_log_path(cfg)) {
  collection_id <- as.character(collection_id)[[1L]]
  if (is.na(collection_id) || !nzchar(collection_id)) {
    return(NA_character_)
  }
  rows <- .litxr_read_collection_sync_log(cfg, path = path)
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
