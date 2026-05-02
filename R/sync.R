#' Sync one configured collection
#'
#' Fetches records from the configured remote channel, parses them into the
#' unified reference schema, and stores one JSON metadata file per record under
#' the collection's local path.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of synced records.
#' @export
litxr_sync_collection <- function(collection_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  started_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  incoming <- switch(
    journal$remote_channel,
    crossref = .litxr_sync_crossref_journal(journal),
    arxiv = .litxr_sync_arxiv_journal(journal),
    stop("Unsupported remote channel: ", journal$remote_channel, call. = FALSE)
  )

  existing <- .litxr_read_journal_records(local_path)
  records <- .litxr_upsert_journal_records(existing, incoming, local_path = local_path)

  .litxr_write_journal_records(records, local_path, journal, cfg = cfg)
  .litxr_append_sync_state(cfg, .litxr_make_sync_state_row(
    collection_id = if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id,
    remote_channel = journal$remote_channel,
    sync_type = "full",
    status = "success",
    started_at = started_at,
    completed_at = format(Sys.time(), tz = "UTC", usetz = TRUE),
    query = .litxr_sync_query_text(journal),
    range_from = NA_character_,
    range_to = NA_character_,
    fetched_from = .litxr_records_date_range(incoming)$from,
    fetched_to = .litxr_records_date_range(incoming)$to,
    page_start = if (identical(journal$remote_channel, "arxiv")) journal$sync$start %||% 0L else NA_integer_,
    page_size = journal$sync$rows %||% if (identical(journal$remote_channel, "crossref")) 1000L else 100L,
    records_fetched = nrow(incoming),
    records_after = nrow(records),
    notes = ""
  ))
  records
}

#' Sync one configured journal
#'
#' Backward-compatible wrapper around `litxr_sync_collection()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of synced records.
#' @export
litxr_sync_journal <- function(journal_id, config = NULL) {
  litxr_sync_collection(journal_id, config = config)
}

#' Sync all configured collections
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Named list of synced `data.table`s.
#' @export
litxr_sync_all <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  collections <- .litxr_config_collections(cfg)
  stats::setNames(
    lapply(collections, function(journal) litxr_sync_collection(journal$collection_id, cfg)),
    vapply(collections, `[[`, character(1), "collection_id")
  )
}

#' Read locally stored records for one configured collection
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of local records.
#' @export
litxr_read_collection <- function(collection_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  records <- .litxr_read_journal_records(local_path)
  delta <- .litxr_read_collection_delta(local_path)
  if (nrow(delta)) {
    return(.litxr_upsert_records(records, delta))
  }
  records
}

#' Summarize publication-date coverage for one collection
#'
#' Returns counts by day, month, or year based on `pub_date`, with summary
#' attributes describing the observed date range and any missing calendar days
#' within that observed range.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param by One of `"day"`, `"month"`, or `"year"`.
#'
#' @return `data.table` with `date` and `n`, plus attributes
#'   `date_min`, `date_max`, and `missing_dates`.
#' @export
litxr_collection_date_stats <- function(collection_id, config = NULL, by = c("day", "month", "year")) {
  by <- match.arg(by)
  records <- litxr_read_collection(collection_id, config = config)

  empty_out <- data.table::data.table(date = character(), n = integer())
  attr(empty_out, "date_min") <- NA_character_
  attr(empty_out, "date_max") <- NA_character_
  attr(empty_out, "missing_dates") <- as.Date(character())

  if (!nrow(records) || !("pub_date" %in% names(records))) {
    return(empty_out)
  }

  pub_dates <- as.Date(records$pub_date)
  pub_dates <- pub_dates[!is.na(pub_dates)]
  if (!length(pub_dates)) {
    return(empty_out)
  }

  bucket_dates <- switch(
    by,
    day = pub_dates,
    month = as.Date(format(pub_dates, "%Y-%m-01")),
    year = as.Date(paste0(format(pub_dates, "%Y"), "-01-01"))
  )

  counts <- sort(table(bucket_dates))
  out <- data.table::data.table(
    date = as.Date(names(counts)),
    n = as.integer(counts)
  )
  data.table::setorder(out, date)

  attr(out, "date_min") <- as.character(min(pub_dates))
  attr(out, "date_max") <- as.character(max(pub_dates))
  attr(out, "missing_dates") <- if (identical(by, "day")) {
    setdiff(seq(min(pub_dates), max(pub_dates), by = "day"), sort(unique(pub_dates)))
  } else {
    as.Date(character())
  }

  out
}

#' Summarize embedding coverage for one collection field
#'
#' Returns one summary row showing how many collection records are currently
#' covered by the main embedding cache, pending delta shards, and their union.
#' This helper reads embedding metadata only, so it still works when the main
#' embedding matrix is unreadable.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Embedded text field.
#' @param model Exact embedding model name used when building the cache.
#'
#' @return One-row `data.table`.
#' @export
litxr_read_embedding_state <- function(collection_id, config = NULL, field = "abstract", model) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }

  records <- litxr_read_collection(collection_id, cfg)
  collection_ref_ids <- unique(as.character(records$ref_id[!is.na(records$ref_id) & nzchar(records$ref_id)]))

  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  main <- .litxr_read_embedding_index_parts(paths, read_matrix = FALSE)
  delta <- .litxr_read_embedding_delta_parts(paths, read_matrix = FALSE)

  main_ref_ids <- unique(as.character(main$metadata$ref_id[!is.na(main$metadata$ref_id) & nzchar(main$metadata$ref_id)]))
  delta_ref_ids <- unique(as.character(delta$metadata$ref_id[!is.na(delta$metadata$ref_id) & nzchar(delta$metadata$ref_id)]))
  embedded_ref_ids <- unique(c(main_ref_ids, delta_ref_ids))
  embedded_in_collection <- intersect(collection_ref_ids, embedded_ref_ids)
  missing_ref_ids <- setdiff(collection_ref_ids, embedded_ref_ids)

  data.table::data.table(
    collection_id = as.character(collection_id),
    field = as.character(field),
    model = as.character(model),
    records_total = length(collection_ref_ids),
    embedded_main = length(intersect(collection_ref_ids, main_ref_ids)),
    embedded_delta = length(intersect(collection_ref_ids, delta_ref_ids)),
    embedded_unique = length(embedded_in_collection),
    missing = length(missing_ref_ids),
    coverage_pct = if (length(collection_ref_ids)) length(embedded_in_collection) / length(collection_ref_ids) else NA_real_
  )
}

#' Diagnose one embedding cache on disk
#'
#' Reports the current on-disk embedding-cache layout, including whether the
#' cache is still on the legacy monolithic `matrix.rds` layout, whether newer
#' float32 shard files exist, how many pending delta shards are present, and
#' whether a non-overwrite compaction should be expected to succeed.
#'
#' This helper reads collection refs plus embedding metadata only. It does not
#' load the full embedding matrix into memory.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Embedded text field.
#' @param model Exact embedding model name used when building the cache.
#'
#' @return List with `summary` and `files`.
#' @export
litxr_diagnose_embedding_cache <- function(collection_id, config = NULL, field = "abstract", model) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }

  records <- litxr_read_collection(collection_id, cfg)
  collection_ref_ids <- unique(as.character(records$ref_id[!is.na(records$ref_id) & nzchar(records$ref_id)]))

  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  manifest <- if (file.exists(paths$manifest)) jsonlite::fromJSON(paths$manifest, simplifyVector = FALSE) else list()
  main <- .litxr_read_embedding_index_parts(paths, read_matrix = FALSE)
  delta <- .litxr_read_embedding_delta_parts(paths, read_matrix = FALSE)

  main_ref_ids <- unique(as.character(main$metadata$ref_id[!is.na(main$metadata$ref_id) & nzchar(main$metadata$ref_id)]))
  delta_ref_ids <- unique(as.character(delta$metadata$ref_id[!is.na(delta$metadata$ref_id) & nzchar(delta$metadata$ref_id)]))
  embedded_ref_ids <- unique(c(main_ref_ids, delta_ref_ids))
  shard_keys <- .litxr_embedding_shard_keys(paths)
  delta_shards <- .litxr_embedding_delta_shard_paths(paths)

  legacy_matrix_size <- .litxr_path_size(paths$matrix)
  matrix_f32_size <- .litxr_path_size(paths$matrix_f32)
  storage_layout <- if (length(shard_keys)) {
    "sharded_float32"
  } else if (file.exists(paths$matrix_f32)) {
    "monolithic_float32"
  } else if (file.exists(paths$matrix)) {
    "legacy_rds"
  } else if (nrow(main$metadata)) {
    "metadata_only"
  } else {
    "missing"
  }
  legacy_rds_ok <- if (identical(storage_layout, "legacy_rds")) .litxr_rds_header_ok(paths$matrix) else NA
  main_matrix_status <- if (identical(storage_layout, "sharded_float32")) {
    paste0("sharded_float32:", length(shard_keys), "_shards")
  } else if (identical(storage_layout, "monolithic_float32")) {
    if (is.na(matrix_f32_size)) "float32_unknown_size" else paste0("float32_bytes:", matrix_f32_size)
  } else if (identical(storage_layout, "legacy_rds")) {
    if (is.na(legacy_matrix_size)) {
      "legacy_rds_unknown_size"
    } else if (legacy_matrix_size == 0) {
      "legacy_rds_zero_bytes"
    } else if (!isTRUE(legacy_rds_ok)) {
      "legacy_rds_invalid"
    } else {
      paste0("legacy_rds_bytes:", legacy_matrix_size)
    }
  } else if (identical(storage_layout, "metadata_only")) {
    "metadata_only"
  } else {
    "missing"
  }
  compact_without_overwrite_ok <- if (!nrow(main$metadata)) {
    TRUE
  } else if (identical(storage_layout, "sharded_float32") || identical(storage_layout, "monolithic_float32")) {
    TRUE
  } else if (identical(storage_layout, "legacy_rds")) {
    is.finite(legacy_matrix_size) && legacy_matrix_size > 0 && isTRUE(legacy_rds_ok)
  } else {
    FALSE
  }
  recommended_action <- if (!compact_without_overwrite_ok && nrow(main$metadata)) {
    "rebuild_from_collection_overwrite_true"
  } else if (length(delta_ref_ids)) {
    "compact_delta"
  } else {
    "none"
  }

  summary <- data.table::data.table(
    collection_id = as.character(collection_id),
    field = as.character(field),
    model = as.character(model),
    storage_layout = storage_layout,
    manifest_storage_version = as.integer(manifest$storage_version %||% NA_integer_),
    manifest_records = as.integer(manifest$records %||% NA_integer_),
    manifest_dimension = as.integer(manifest$dimension %||% NA_integer_),
    main_metadata_rows = nrow(main$metadata),
    main_extra_vs_collection = length(setdiff(main_ref_ids, collection_ref_ids)),
    main_matrix_status = main_matrix_status,
    delta_shard_count = length(delta_shards),
    delta_rows = nrow(delta$metadata),
    delta_unique_ref_id = length(delta_ref_ids),
    records_total = length(collection_ref_ids),
    embedded_main = length(intersect(collection_ref_ids, main_ref_ids)),
    embedded_delta = length(intersect(collection_ref_ids, delta_ref_ids)),
    embedded_unique = length(intersect(collection_ref_ids, embedded_ref_ids)),
    missing = length(setdiff(collection_ref_ids, embedded_ref_ids)),
    compact_without_overwrite_ok = compact_without_overwrite_ok,
    recommended_action = recommended_action
  )

  files <- data.table::rbindlist(list(
    .litxr_embedding_cache_file_row("dir", paths$dir, FALSE),
    .litxr_embedding_cache_file_row("manifest", paths$manifest, FALSE),
    .litxr_embedding_cache_file_row("metadata", paths$metadata, FALSE),
    .litxr_embedding_cache_file_row("matrix_rds", paths$matrix, FALSE),
    .litxr_embedding_cache_file_row("matrix_f32", paths$matrix_f32, FALSE),
    .litxr_embedding_cache_file_row("shards_dir", paths$shards_dir, TRUE),
    .litxr_embedding_cache_file_row("delta_dir", paths$delta_dir, TRUE),
    .litxr_embedding_cache_file_row("delta_metadata", paths$delta_metadata, FALSE),
    .litxr_embedding_cache_file_row("delta_matrix", paths$delta_matrix, FALSE),
    .litxr_embedding_cache_file_row("delta_manifest", paths$delta_manifest, FALSE)
  ), fill = TRUE)

  list(summary = summary, files = files)
}

#' Reset one embedding cache while optionally preserving delta shards
#'
#' Removes the main on-disk embedding cache for one collection field and model.
#' By default, pending delta shards are preserved so any successfully embedded
#' batches remain available for later recovery or compaction. This is useful
#' when the main cache is stale or corrupted, such as a zero-byte legacy
#' `matrix.rds`.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Embedded text field.
#' @param model Exact embedding model name used when building the cache.
#' @param preserve_delta Whether to keep existing delta shards.
#'
#' @return List with `before`, `after`, and `removed`.
#' @export
litxr_reset_embedding_cache <- function(
  collection_id,
  config = NULL,
  field = "abstract",
  model,
  preserve_delta = TRUE
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }

  before <- litxr_diagnose_embedding_cache(
    collection_id = collection_id,
    config = cfg,
    field = field,
    model = model
  )
  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  removed <- .litxr_remove_embedding_main(paths)
  if (!isTRUE(preserve_delta)) {
    .litxr_clear_embedding_delta(paths)
    removed <- c(removed, paths$delta_dir, paths$delta_metadata, paths$delta_matrix, paths$delta_manifest)
  }
  removed <- unique(removed[nzchar(removed)])
  after <- litxr_diagnose_embedding_cache(
    collection_id = collection_id,
    config = cfg,
    field = field,
    model = model
  )

  list(
    before = before,
    after = after,
    removed = removed
  )
}

#' Summarize embedding coverage by publication date
#'
#' Returns counts by day, month, or year for total collection records, embedded
#' records, missing records, and embedding coverage. Coverage is computed from
#' the union of the main embedding cache and pending delta shards. This helper
#' reads embedding metadata only.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Embedded text field.
#' @param model Exact embedding model name used when building the cache.
#' @param by One of `"day"`, `"month"`, or `"year"`.
#'
#' @return `data.table` with `date`, `records_total`, `embedded_unique`,
#'   `missing`, and `coverage_pct`, sorted ascending by `date`.
#' @importFrom stats aggregate
#' @export
litxr_embedding_date_stats <- function(
  collection_id,
  config = NULL,
  field = "abstract",
  model,
  by = c("day", "month", "year")
) {
  by <- match.arg(by)
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }

  records <- litxr_read_collection(collection_id, cfg)
  empty_out <- data.table::data.table(
    date = as.Date(character()),
    records_total = integer(),
    embedded_unique = integer(),
    missing = integer(),
    coverage_pct = numeric()
  )
  attr(empty_out, "date_min") <- NA_character_
  attr(empty_out, "date_max") <- NA_character_
  attr(empty_out, "missing_dates") <- as.Date(character())

  if (!nrow(records) || !("pub_date" %in% names(records))) {
    return(empty_out)
  }

  pub_dates <- as.Date(records$pub_date)
  keep <- !is.na(pub_dates) & !is.na(records$ref_id) & nzchar(records$ref_id)
  if (!any(keep)) {
    return(empty_out)
  }
  records <- records[keep, , drop = FALSE]
  pub_dates <- pub_dates[keep]

  bucket_dates <- switch(
    by,
    day = pub_dates,
    month = as.Date(format(pub_dates, "%Y-%m-01")),
    year = as.Date(paste0(format(pub_dates, "%Y"), "-01-01"))
  )
  records$date <- bucket_dates

  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  main <- .litxr_read_embedding_index_parts(paths, read_matrix = FALSE)
  delta <- .litxr_read_embedding_delta_parts(paths, read_matrix = FALSE)
  embedded_ref_ids <- unique(c(
    as.character(main$metadata$ref_id[!is.na(main$metadata$ref_id) & nzchar(main$metadata$ref_id)]),
    as.character(delta$metadata$ref_id[!is.na(delta$metadata$ref_id) & nzchar(delta$metadata$ref_id)])
  ))

  records$embedded <- records$ref_id %in% embedded_ref_ids
  total_counts <- stats::aggregate(ref_id ~ date, data = records, FUN = length)
  embedded_counts <- stats::aggregate(embedded ~ date, data = records, FUN = sum)
  out <- merge(total_counts, embedded_counts, by = "date", all = TRUE, sort = TRUE)
  names(out) <- c("date", "records_total", "embedded_unique")
  out$records_total <- as.integer(out$records_total)
  out$embedded_unique <- as.integer(out$embedded_unique)
  out$missing <- out$records_total - out$embedded_unique
  out$coverage_pct <- ifelse(out$records_total > 0L, out$embedded_unique / out$records_total, NA_real_)
  out <- data.table::as.data.table(out)
  data.table::setorder(out, date)

  attr(out, "date_min") <- as.character(min(pub_dates))
  attr(out, "date_max") <- as.character(max(pub_dates))
  attr(out, "missing_dates") <- if (identical(by, "day")) {
    setdiff(seq(min(pub_dates), max(pub_dates), by = "day"), sort(unique(pub_dates)))
  } else {
    as.Date(character())
  }

  out
}

#' Compute the next arXiv repair date range
#'
#' Returns the next day range to pass to `scripts/repair_arxiv_range.R`.
#' `basis = "sync_state"` uses the latest successful arXiv `repair_range_day`
#' row. `basis = "collection_index"` uses the maximum `pub_date` currently
#' visible in the collection index.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param basis Whether to use the sync ledger or collection index to choose the
#'   latest completed date.
#' @param date_to Last date to include in the proposed repair range. Defaults to
#'   `Sys.Date()`.
#'
#' @return One-row `data.table` with `collection_id`, `basis`, `latest_date`,
#'   `date_from`, `date_to`, and `needs_repair`.
#' @export
litxr_next_arxiv_repair_range <- function(
  collection_id,
  config = NULL,
  basis = c("sync_state", "collection_index"),
  date_to = Sys.Date()
) {
  basis <- match.arg(basis)
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  if (!identical(journal$remote_channel, "arxiv")) {
    stop("`litxr_next_arxiv_repair_range()` only supports arXiv collections.", call. = FALSE)
  }

  date_to <- as.Date(date_to)
  if (is.na(date_to)) {
    stop("`date_to` must be parseable as a Date.", call. = FALSE)
  }

  latest_date <- switch(
    basis,
    sync_state = {
      state <- litxr_read_sync_state(config = cfg, collection_id = collection_id)
      done <- state[
        state$remote_channel == "arxiv" &
          state$sync_type == "repair_range_day" &
          state$status == "success" &
          !is.na(state$range_to),
      ]
      dates <- suppressWarnings(as.Date(done$range_to))
      dates <- dates[!is.na(dates)]
      if (length(dates)) max(dates) else as.Date(NA)
    },
    collection_index = {
      stats_day <- litxr_collection_date_stats(collection_id, cfg, by = "day")
      dates <- suppressWarnings(as.Date(stats_day$date))
      dates <- dates[!is.na(dates)]
      if (length(dates)) max(dates) else as.Date(NA)
    }
  )

  date_from <- if (is.na(latest_date)) as.Date(NA) else latest_date + 1L
  data.table::data.table(
    collection_id = collection_id,
    basis = basis,
    latest_date = latest_date,
    date_from = date_from,
    date_to = date_to,
    needs_repair = !is.na(date_from) && date_from <= date_to
  )
}

#' Read locally stored records for one journal
#'
#' Backward-compatible wrapper around `litxr_read_collection()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of local records.
#' @export
litxr_read_journal <- function(journal_id, config = NULL) {
  litxr_read_collection(journal_id, config = config)
}

#' Repair or incrementally fill a collection's local store
#'
#' Intended for bounded repair runs that should respect remote rate limits. For
#' arXiv journals, this can narrow the sync to a submitted-date window and/or a
#' smaller batch.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param search_query Optional replacement query for this repair run.
#' @param submitted_from Optional lower bound date/time for arXiv submittedDate.
#' @param submitted_to Optional upper bound date/time for arXiv submittedDate.
#' @param start Optional arXiv result offset for this repair run.
#' @param limit Optional maximum number of records for this repair run.
#'
#' @return `data.table` of repaired or newly synced records.
#' @export
litxr_repair_collection <- function(
  collection_id,
  config = NULL,
  search_query = NULL,
  submitted_from = NULL,
  submitted_to = NULL,
  start = NULL,
  limit = NULL
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  started_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)

  if (identical(journal$remote_channel, "arxiv")) {
    base_query <- if (is.null(search_query)) journal$sync$search_query else search_query
    journal$sync$search_query <- .litxr_build_arxiv_search_query(base_query, submitted_from, submitted_to)
    if (!is.null(start)) journal$sync$start <- as.integer(start)
    if (!is.null(limit)) journal$sync$limit <- as.integer(limit)
  } else {
    if (!is.null(limit)) journal$sync$limit <- as.integer(limit)
  }

  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  incoming <- switch(
    journal$remote_channel,
    crossref = .litxr_sync_crossref_journal(journal),
    arxiv = .litxr_sync_arxiv_journal(journal),
    stop("Unsupported remote channel: ", journal$remote_channel, call. = FALSE)
  )

  existing <- .litxr_read_journal_records(local_path)
  records <- .litxr_write_journal_upserted_records(existing, incoming, local_path, journal, cfg = cfg)
  .litxr_append_sync_state(cfg, .litxr_make_sync_state_row(
    collection_id = if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id,
    remote_channel = journal$remote_channel,
    sync_type = "repair",
    status = "success",
    started_at = started_at,
    completed_at = format(Sys.time(), tz = "UTC", usetz = TRUE),
    query = .litxr_sync_query_text(journal),
    range_from = submitted_from %||% NA_character_,
    range_to = submitted_to %||% NA_character_,
    fetched_from = .litxr_records_date_range(incoming)$from,
    fetched_to = .litxr_records_date_range(incoming)$to,
    page_start = if (identical(journal$remote_channel, "arxiv")) journal$sync$start %||% 0L else NA_integer_,
    page_size = journal$sync$rows %||% if (identical(journal$remote_channel, "crossref")) 1000L else 100L,
    records_fetched = nrow(incoming),
    records_after = nrow(records),
    notes = ""
  ))
  records
}

#' Repair or incrementally fill a journal's local store
#'
#' Backward-compatible wrapper around `litxr_repair_collection()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param search_query Optional replacement query for this repair run.
#' @param submitted_from Optional lower bound date/time for arXiv submittedDate.
#' @param submitted_to Optional upper bound date/time for arXiv submittedDate.
#' @param start Optional arXiv result offset for this repair run.
#' @param limit Optional maximum number of records for this repair run.
#'
#' @return `data.table` of repaired or newly synced records.
#' @export
litxr_repair_journal <- function(
  journal_id,
  config = NULL,
  search_query = NULL,
  submitted_from = NULL,
  submitted_to = NULL,
  start = NULL,
  limit = NULL
) {
  litxr_repair_collection(
    journal_id,
    config = config,
    search_query = search_query,
    submitted_from = submitted_from,
    submitted_to = submitted_to,
    start = start,
    limit = limit
  )
}

#' Rebuild the local collection fst index from JSON files
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the rebuilt fst index path.
#' @export
litxr_rebuild_collection_index <- function(collection_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  records <- .litxr_read_journal_records_from_json(local_path)
  index_path <- .litxr_write_journal_index(records, local_path)
  .litxr_update_project_indexes(cfg, journal, records)
  invisible(index_path)
}

#' Refresh the local collection fst index from recently changed JSON files
#'
#' This is a fast, mtime-based refresh for the common case where JSON record
#' files were written after `index/references.fst` was last updated. It does
#' not replace `litxr_rebuild_collection_index()`, which remains the
#' correctness-first full rebuild for schema migrations and legacy cleanup.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the refreshed fst index path.
#' @export
litxr_refresh_collection_index <- function(collection_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  paths <- .litxr_journal_paths(local_path)
  index_path <- .litxr_index_path(local_path)

  existing <- .litxr_read_journal_index(local_path)
  if (is.null(existing) || !file.exists(index_path)) {
    return(litxr_rebuild_collection_index(collection_id, cfg))
  }

  if (!dir.exists(paths$json)) {
    return(invisible(index_path))
  }

  files <- sort(list.files(paths$json, pattern = "\\.json$", full.names = TRUE))
  if (!length(files)) {
    return(invisible(index_path))
  }

  index_mtime <- file.info(index_path)$mtime
  changed_files <- files[file.info(files)$mtime > index_mtime]
  if (!length(changed_files)) {
    return(invisible(index_path))
  }

  incoming <- .litxr_prefer_complete_records(
    data.table::rbindlist(lapply(changed_files, .litxr_storage_payload_to_row), fill = TRUE)
  )
  records <- .litxr_upsert_records(
    existing,
    incoming,
    conflict_path = file.path(paths$json, "_upsert_conflicts.jsonl")
  )
  index_path <- .litxr_write_journal_index(records, local_path)
  .litxr_update_project_indexes(cfg, journal, records)
  invisible(index_path)
}

#' Compact pending collection delta records into the main fst index
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param refresh_project_index Whether to refresh project-level canonical
#'   reference indexes after compaction.
#'
#' @return Invisibly returns the compacted fst index path.
#' @export
litxr_compact_collection_index <- function(collection_id, config = NULL, refresh_project_index = FALSE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)

  records <- .litxr_read_journal_records(local_path)
  delta <- .litxr_read_collection_delta(local_path)
  delta_keys <- character()
  if (nrow(delta)) {
    delta_keys <- .litxr_upsert_key(delta)
    records <- .litxr_upsert_records(
      records,
      delta,
      conflict_path = file.path(.litxr_journal_paths(local_path)$json, "_upsert_conflicts.jsonl")
    )
  }

  index_path <- .litxr_write_journal_index(records, local_path)
  if (length(delta_keys)) {
    records_keys <- .litxr_upsert_key(records)
    touched_records <- records[records_keys %in% delta_keys, ]
    .litxr_write_journal_record_files(touched_records, local_path, journal)
  }
  .litxr_clear_collection_delta(local_path)

  if (isTRUE(refresh_project_index)) {
    .litxr_update_project_indexes(cfg, journal, records)
  }

  invisible(index_path)
}

#' Rebuild the local journal fst index from JSON files
#'
#' Backward-compatible wrapper around `litxr_rebuild_collection_index()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the rebuilt fst index path.
#' @export
litxr_rebuild_journal_index <- function(journal_id, config = NULL) {
  litxr_rebuild_collection_index(journal_id, config = config)
}

#' Export local references to a BibTeX file
#'
#' @param output Output `.bib` file path.
#' @param journal_ids Optional character vector of collection ids to export. The
#'   argument name is kept for backward compatibility.
#' @param keys Optional character vector of record identifiers to export. Keys
#'   are matched against `doi`, `ref_id`, or `source_id`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the output path.
#' @export
litxr_export_bib <- function(output, journal_ids = NULL, keys = NULL, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  selected <- .litxr_config_collections(cfg)

  if (!is.null(journal_ids)) {
    keep <- vapply(selected, function(x) x$collection_id %in% journal_ids, logical(1))
    selected <- selected[keep]
  }

  if (!length(selected)) {
    writeLines(character(), output)
    return(invisible(output))
  }

  rows <- data.table::rbindlist(lapply(selected, function(journal) {
    .litxr_read_journal_records(.litxr_resolve_local_path(cfg, journal$local_path))
  }), fill = TRUE)

  if (!is.null(keys) && length(keys)) {
    key_values <- unique(as.character(keys))
    filtered <- .litxr_filter_records_by_keys(rows, key_values)
    missing_keys <- setdiff(key_values, filtered$litxr_matched_key__)
    if (length(missing_keys)) {
      warning(
        "The following record keys were not found and were ignored: ",
        paste(missing_keys, collapse = ", "),
        call. = FALSE
      )
    }
    filtered[["litxr_matched_key__"]] <- NULL
    rows <- filtered
  }

  if (!nrow(rows)) {
    writeLines(character(), output)
    return(invisible(output))
  }

  bib_lines <- unlist(lapply(seq_len(nrow(rows)), function(i) {
    c(row_to_bibtex(rows[i, ]), "")
  }), use.names = FALSE)

  writeLines(bib_lines, output)
  invisible(output)
}

#' Read the canonical project-level reference index
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of canonical references across collections.
#' @export
litxr_read_references <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_read_project_references_index(cfg)
}

#' Read the project-level reference-collection membership index
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of `ref_id` to `collection_id` links.
#' @export
litxr_read_reference_collections <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_read_project_reference_collections_index(cfg)
}

#' Find references in the canonical project-level store
#'
#' Provides simple deterministic filtering over the project-level reference
#' index, with optional collection membership filtering and substring matching.
#' Exact `ref_id` searches fall back to configured collection indexes when the
#' project-level index has not yet been refreshed.
#'
#' @param query Optional substring query matched against `title`, `authors`,
#'   `journal`, `container_title`, and `url`.
#' @param entry_type Optional BibTeX entry type filter.
#' @param year Optional year filter.
#' @param collection_id Optional collection membership filter.
#' @param doi Optional DOI filter.
#' @param ref_id Optional reference id filter. For arXiv records, both
#'   canonical ids such as `"arxiv:2405.03710"` and bare ids such as
#'   `"2405.03710"` are accepted.
#' @param isbn Optional ISBN filter.
#' @param issn Optional ISSN substring filter.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Filtered `data.table` of references.
#' @export
litxr_find_refs <- function(
  query = NULL,
  entry_type = NULL,
  year = NULL,
  collection_id = NULL,
  doi = NULL,
  ref_id = NULL,
  isbn = NULL,
  issn = NULL,
  config = NULL
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  ref_keys <- if (!is.null(ref_id) && any(nzchar(as.character(ref_id)))) {
    .litxr_expand_reference_keys(ref_id)
  } else {
    character()
  }
  exact_ref_only <- length(ref_keys) &&
    is.null(query) &&
    is.null(entry_type) &&
    is.null(year) &&
    is.null(collection_id) &&
    is.null(doi) &&
    is.null(isbn) &&
    is.null(issn)
  if (isTRUE(exact_ref_only)) {
    exact <- .litxr_read_project_references_by_keys(cfg, ref_keys)
    if (nrow(exact)) {
      return(exact)
    }
    return(.litxr_find_collection_refs_by_keys(cfg, ref_keys))
  }

  refs <- litxr_read_references(cfg)
  if (!nrow(refs)) {
    if (length(ref_keys)) {
      return(.litxr_find_collection_refs_by_keys(cfg, ref_keys, collection_id = collection_id))
    }
    return(refs)
  }

  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    links <- litxr_read_reference_collections(cfg)
    keep_ref_ids <- unique(links$ref_id[links$collection_id == collection_id])
    refs <- refs[refs$ref_id %in% keep_ref_ids, ]
  }

  if (!is.null(entry_type) && nzchar(as.character(entry_type))) {
    refs <- refs[refs$entry_type %in% entry_type, ]
  }
  if (!is.null(year)) {
    refs <- refs[refs$year %in% as.integer(year), ]
  }
  if (!is.null(doi) && nzchar(as.character(doi))) {
    refs <- refs[refs$doi %in% doi, ]
  }
  if (!is.null(ref_id) && any(nzchar(as.character(ref_id)))) {
    source_id <- if ("source_id" %in% names(refs)) refs$source_id else rep(NA_character_, nrow(refs))
    refs <- refs[refs$ref_id %in% ref_keys | source_id %in% ref_keys, ]
    if (!nrow(refs)) {
      return(.litxr_find_collection_refs_by_keys(cfg, ref_keys, collection_id = collection_id))
    }
  }
  if (!is.null(isbn) && nzchar(as.character(isbn))) {
    refs <- refs[refs$isbn %in% isbn, ]
  }
  if (!is.null(issn) && nzchar(as.character(issn))) {
    refs <- refs[!is.na(refs$issn) & grepl(as.character(issn), refs$issn, fixed = TRUE), ]
  }

  if (!is.null(query) && nzchar(as.character(query))) {
    q <- tolower(as.character(query[[1]]))
    title <- if ("title" %in% names(refs)) refs$title else rep(NA_character_, nrow(refs))
    authors <- if ("authors" %in% names(refs)) refs$authors else rep(NA_character_, nrow(refs))
    journal <- if ("journal" %in% names(refs)) refs$journal else rep(NA_character_, nrow(refs))
    container_title <- if ("container_title" %in% names(refs)) refs$container_title else rep(NA_character_, nrow(refs))
    url <- if ("url" %in% names(refs)) refs$url else rep(NA_character_, nrow(refs))

    haystack_match <- function(x) !is.na(x) & grepl(q, tolower(x), fixed = TRUE)
    keep <- haystack_match(title) |
      haystack_match(authors) |
      haystack_match(journal) |
      haystack_match(container_title) |
      haystack_match(url)
    refs <- refs[keep, ]
  }

  refs
}

#' Build a cached embedding index for one collection field
#'
#' Embeds one text field from `litxr_read_collection()` in batches and caches the
#' resulting numeric matrix under `project.data_root/embeddings/`. The embedding
#' function is user supplied so callers can use providers such as
#' `inferencer::embed_openrouter()` or `inferencer::embed_gemini()`. Completed
#' batches are written to append-only delta shards and compacted into the main
#' embedding index once at the end.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Text field to embed, such as `"abstract"`.
#' @param embed_fun Function taking a character vector and returning embeddings
#'   as a matrix, data frame, or list of numeric vectors.
#' @param model Exact embedding model name. The same value must be used for
#'   search queries.
#' @param provider Optional provider label, such as `"openrouter"` or
#'   `"gemini"`.
#' @param batch_size Number of texts per embedding request.
#' @param overwrite Whether to rebuild the cache from scratch.
#' @param limit Optional maximum number of new texts to embed in this run.
#'
#' @return Metadata `data.table` for cached embeddings.
#' @export
litxr_embed_collection_delta <- function(
  collection_id,
  config = NULL,
  field = "abstract",
  embed_fun,
  model,
  provider = NA_character_,
  batch_size = 64L,
  overwrite = FALSE,
  limit = NULL
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  prepared <- .litxr_prepare_embedding_targets(
    collection_id = collection_id,
    cfg = cfg,
    field = field,
    model = model,
    overwrite = overwrite,
    limit = limit
  )
  if (!nrow(prepared$targets)) {
    return(prepared$delta$metadata)
  }

  .litxr_write_embedding_delta_batches(
    targets = prepared$targets,
    paths = prepared$paths,
    collection_id = collection_id,
    field = field,
    embed_fun = embed_fun,
    model = as.character(model),
    provider = as.character(provider),
    batch_size = batch_size,
    existing = prepared$existing,
    existing_delta = prepared$delta
  )
}

#' Build a cached embedding index for one collection field
#'
#' Embeds one text field from `litxr_read_collection()` in batches and caches the
#' resulting numeric matrix under `project.data_root/embeddings/`. The embedding
#' function is user supplied so callers can use providers such as
#' `inferencer::embed_openrouter()` or `inferencer::embed_gemini()`. Completed
#' batches are written to append-only delta shards and compacted into the main
#' embedding index once at the end.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Text field to embed, such as `"abstract"`.
#' @param embed_fun Function taking a character vector and returning embeddings
#'   as a matrix, data frame, or list of numeric vectors.
#' @param model Exact embedding model name. The same value must be used for
#'   search queries.
#' @param provider Optional provider label, such as `"openrouter"` or
#'   `"gemini"`.
#' @param batch_size Number of texts per embedding request.
#' @param overwrite Whether to rebuild the cache from scratch.
#' @param limit Optional maximum number of new texts to embed in this run.
#'
#' @return Metadata `data.table` for cached embeddings.
#' @export
litxr_build_embedding_index <- function(
  collection_id,
  config = NULL,
  field = "abstract",
  embed_fun,
  model,
  provider = NA_character_,
  batch_size = 64L,
  overwrite = FALSE,
  limit = NULL
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (!is.function(embed_fun)) {
    stop("`embed_fun` must be a function that accepts a character vector.", call. = FALSE)
  }
  delta <- litxr_embed_collection_delta(
    collection_id = collection_id,
    config = cfg,
    field = field,
    embed_fun = embed_fun,
    model = model,
    provider = provider,
    batch_size = batch_size,
    overwrite = overwrite,
    limit = limit
  )

  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  if (!nrow(delta) && !length(.litxr_embedding_delta_shard_paths(paths))) {
    return(.litxr_compact_embedding_index(
      paths = paths,
      collection_id = collection_id,
      field = field,
      model = as.character(model),
      provider = as.character(provider),
      overwrite = isTRUE(overwrite)
    ))
  }

  tryCatch(
    litxr_compact_embedding_delta(
      collection_id = collection_id,
      config = cfg,
      field = field,
      model = model,
      provider = provider,
      overwrite = overwrite
    ),
    error = function(e) {
      if (!isTRUE(overwrite) && .litxr_is_embedding_matrix_read_error(e)) {
        warning(
          "Main embedding cache is unreadable, so newly embedded rows were left in delta shards and were not compacted. ",
          "Run `litxr_build_embedding_index(..., overwrite = TRUE)` to rebuild the main cache, or ",
          "`litxr_compact_embedding_delta(..., overwrite = TRUE)` if you want to compact from delta only. ",
          "Original error: ", conditionMessage(e),
          call. = FALSE
        )
        return(.litxr_read_embedding_delta_parts(paths)$metadata)
      }
      stop(e)
    }
  )
}

#' Compact pending embedding delta shards into the main embedding cache
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Embedded text field.
#' @param model Exact embedding model name used when building the cache.
#' @param provider Optional provider label used in the resulting manifest.
#' @param overwrite Whether the compacted result should ignore any existing main
#'   embedding cache and rebuild it from delta only.
#'
#' @return Metadata `data.table` for the compacted main embedding index.
#' @export
litxr_compact_embedding_delta <- function(
  collection_id,
  config = NULL,
  field = "abstract",
  model,
  provider = NA_character_,
  overwrite = FALSE
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }
  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  .litxr_compact_embedding_index(
    paths = paths,
    collection_id = collection_id,
    field = field,
    model = as.character(model),
    provider = as.character(provider),
    overwrite = isTRUE(overwrite)
  )
}

#' Search embedding delta shards instead of the compacted main cache
#'
#' Embeds the query with the supplied `embed_fun`, or uses a precomputed
#' `query_vec`, then ranks records from one or more selected delta shard files
#' by cosine similarity. Use `date` to select shard files whose timestamp prefix
#' matches a `YYYY-MM-DD` day, or `shard` to point at one specific shard file
#' directly.
#'
#' @param query Query text. Ignored when `query_vec` is supplied.
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Embedded text field.
#' @param embed_fun Function taking a character vector and returning embeddings.
#'   Required only when `query_vec` is not supplied.
#' @param query_vec Optional precomputed numeric query embedding vector.
#' @param model Exact embedding model name. Must match the selected delta shard
#'   model.
#' @param date Optional date used to filter shard filenames. Accepts values
#'   coercible by `as.Date()`.
#' @param shard Optional shard filename or full shard path under the collection's
#'   embedding delta directory.
#' @param top_n Number of matches to return.
#'
#' @return `data.table` of matches with `score`.
#' @export
litxr_search_embedding_delta <- function(
  query,
  collection_id,
  config = NULL,
  field = "abstract",
  embed_fun = NULL,
  query_vec = NULL,
  model,
  date = NULL,
  shard = NULL,
  top_n = 20L
) {
  top_n <- as.integer(top_n)
  if (is.na(top_n) || top_n < 0L) {
    stop("`top_n` must be a non-negative integer.", call. = FALSE)
  }

  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }

  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  shard_paths <- .litxr_select_embedding_delta_shards(paths, shard = shard, date = date)
  if (!length(shard_paths)) {
    return(data.table::data.table())
  }

  delta <- .litxr_read_embedding_delta_shards(shard_paths)
  if (!nrow(delta$metadata) || !nrow(delta$matrix)) {
    return(data.table::data.table())
  }
  manifest_model <- delta$manifest$model %||% NA_character_
  if (!identical(as.character(model), as.character(manifest_model))) {
    stop("Query model does not match selected delta shard model: ", manifest_model, call. = FALSE)
  }

  query_vector <- .litxr_resolve_query_vector(
    query = query,
    embed_fun = embed_fun,
    query_vec = query_vec
  )
  scores <- litxr_cosine_similarity(query_vector, delta$matrix)
  out <- data.table::copy(delta$metadata)
  out[["score"]] <- scores
  data.table::setorderv(out, "score", order = -1L)
  out[seq_len(min(nrow(out), as.integer(top_n))), ]
}

#' Read a cached embedding index
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Embedded text field.
#' @param model Exact embedding model name used when building the cache.
#'
#' @return List with `metadata`, `matrix`, and `manifest`.
#' @export
litxr_read_embedding_index <- function(collection_id, config = NULL, field = "abstract", model) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }
  .litxr_read_embedding_index_parts(.litxr_embedding_index_paths(cfg, collection_id, field, model))
}

#' Search cached collection embeddings
#'
#' Embeds the query with the supplied `embed_fun`, or uses a precomputed
#' `query_vec`, checks that `model` matches the cached corpus model, and ranks
#' cached records by cosine similarity.
#'
#' @param query Query text. Ignored when `query_vec` is supplied.
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Embedded text field.
#' @param embed_fun Function taking a character vector and returning embeddings.
#'   Required only when `query_vec` is not supplied.
#' @param query_vec Optional precomputed numeric query embedding vector.
#' @param model Exact embedding model name. Must match the cached corpus model.
#' @param top_n Number of matches to return.
#'
#' @return `data.table` of matches with `score`.
#' @export
litxr_search_embeddings <- function(
  query,
  collection_id,
  config = NULL,
  field = "abstract",
  embed_fun = NULL,
  query_vec = NULL,
  model,
  top_n = 20L
) {
  top_n <- as.integer(top_n)
  if (is.na(top_n) || top_n < 0L) {
    stop("`top_n` must be a non-negative integer.", call. = FALSE)
  }
  index <- litxr_read_embedding_index(collection_id, config = config, field = field, model = model)
  if (!nrow(index$metadata) || !nrow(index$matrix)) {
    return(data.table::data.table())
  }
  manifest_model <- index$manifest$model %||% NA_character_
  if (!identical(as.character(model), as.character(manifest_model))) {
    stop("Query model does not match cached corpus model: ", manifest_model, call. = FALSE)
  }

  query_vector <- .litxr_resolve_query_vector(
    query = query,
    embed_fun = embed_fun,
    query_vec = query_vec
  )
  scores <- litxr_cosine_similarity(query_vector, index$matrix)
  out <- data.table::copy(index$metadata)
  out[["score"]] <- scores
  data.table::setorderv(out, "score", order = -1L)
  out[seq_len(min(nrow(out), as.integer(top_n))), ]
}

#' Build a cached category-query embedding index
#'
#' Builds and caches embeddings for a set of category-defining query sentences.
#' The query set can be supplied as a named R list or a JSON/YAML file path.
#'
#' @param query_set Named list mapping category ids to character vectors of
#'   query sentences, or a path to a JSON/YAML file containing that structure.
#' @param query_set_id Identifier for this category query set.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param embed_fun Function taking a character vector and returning embeddings.
#' @param model Exact embedding model name.
#' @param provider Optional provider label, such as `"openrouter"` or
#'   `"gemini"`.
#' @param batch_size Number of query sentences per embedding request.
#' @param overwrite Whether to rebuild the cached query index from scratch.
#'
#' @return List with `metadata`, `matrix`, and `manifest`.
#' @export
litxr_build_label_query_index <- function(
  query_set,
  query_set_id = "default",
  config = NULL,
  embed_fun,
  model,
  provider = NA_character_,
  batch_size = 64L,
  overwrite = FALSE
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (!is.function(embed_fun)) {
    stop("`embed_fun` must be a function that accepts a character vector.", call. = FALSE)
  }
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }
  batch_size <- as.integer(batch_size)
  if (is.na(batch_size) || batch_size <= 0L) {
    stop("`batch_size` must be a positive integer.", call. = FALSE)
  }

  queries <- .litxr_normalize_label_query_set(query_set)
  paths <- .litxr_label_query_index_paths(cfg, query_set_id, model)
  existing <- .litxr_read_label_query_index_parts(paths, read_matrix = FALSE)
  if (isTRUE(overwrite)) {
    existing <- list(metadata = .litxr_empty_label_query_metadata(), matrix = NULL, manifest = list())
  }

  existing_ids <- if (nrow(existing$metadata)) existing$metadata$query_id else character()
  todo <- if (isTRUE(overwrite)) queries else queries[!(queries$query_id %in% existing_ids), ]

  parts <- if (isTRUE(overwrite)) {
    list(
      metadata = .litxr_empty_label_query_metadata(),
      matrix = matrix(numeric(), nrow = 0L, ncol = 0L),
      manifest = list()
    )
  } else {
    .litxr_read_label_query_index_parts(paths, read_matrix = TRUE)
  }

  if (nrow(todo)) {
    for (start in seq(1L, nrow(todo), by = batch_size)) {
      end <- min(start + batch_size - 1L, nrow(todo))
      batch <- todo[start:end, ]
      batch_matrix <- .litxr_as_embedding_matrix(embed_fun(batch$query_text))
      if (nrow(batch_matrix) != nrow(batch)) {
        stop("`embed_fun` returned ", nrow(batch_matrix), " embeddings for ", nrow(batch), " query sentences.", call. = FALSE)
      }

      existing_dimension <- .litxr_label_query_existing_dimension(parts)
      if (!is.na(existing_dimension) && existing_dimension != ncol(batch_matrix)) {
        stop("Label query embedding dimension changed from ", existing_dimension, " to ", ncol(batch_matrix), ".", call. = FALSE)
      }

      batch_meta <- data.table::copy(batch)

      parts$metadata <- data.table::rbindlist(list(parts$metadata, batch_meta), fill = TRUE)
      parts$matrix <- if (!nrow(parts$matrix)) {
        batch_matrix
      } else {
        rbind(parts$matrix, batch_matrix)
      }
      keep <- !duplicated(parts$metadata$query_id, fromLast = TRUE)
      parts$metadata <- parts$metadata[keep, ]
      parts$matrix <- parts$matrix[keep, , drop = FALSE]
    }
  }

  manifest <- list(
    query_set_id = as.character(query_set_id),
    model = as.character(model),
    provider = as.character(provider),
    dimension = if (nrow(parts$matrix)) as.integer(ncol(parts$matrix)) else as.integer(existing$manifest$dimension %||% 0L),
    records = as.integer(nrow(parts$metadata)),
    updated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )
  .litxr_write_label_query_index(paths, parts$metadata, parts$matrix, manifest)
  .litxr_read_label_query_index_parts(paths, read_matrix = TRUE)
}

#' Score one collection against category-query embeddings
#'
#' Scores each collection record against all query sentences in a cached category
#' query index, then aggregates within category by `max`, `mean`, or
#' `top_k_mean`.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param query_set_id Identifier of a cached category query set.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Embedded text field. Must match the collection embedding index.
#' @param model Exact embedding model name. Must match both corpus and query
#'   indexes.
#' @param aggregations Character vector of aggregation names chosen from
#'   `"max"`, `"mean"`, and `"top_k_mean"`.
#' @param top_k Number of top query scores to average when
#'   `aggregations` includes `"top_k_mean"`.
#' @param ref_ids Optional subset of collection `ref_id`s to score.
#' @param date_from Optional inclusive lower publication-date bound. Accepts
#'   values coercible by `as.Date()`.
#' @param date_to Optional inclusive upper publication-date bound. Accepts
#'   values coercible by `as.Date()`.
#' @param chunk_size Positive integer chunk size used when scoring large
#'   corpora. Smaller values reduce peak memory use.
#' @param include_query_scores Whether to include the full per-query score table.
#'
#' @return If `include_query_scores = FALSE`, a `data.table` with one row per
#'   `ref_id` and `category_id`. Otherwise a list with `category_scores` and
#'   `query_scores`.
#' @export
litxr_score_collection_categories <- function(
  collection_id,
  query_set_id = "default",
  config = NULL,
  field = "abstract",
  model,
  aggregations = c("max", "mean"),
  top_k = 3L,
  ref_ids = NULL,
  date_from = NULL,
  date_to = NULL,
  chunk_size = 5000L,
  include_query_scores = FALSE
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }

  aggregations <- unique(as.character(aggregations))
  valid_aggregations <- c("max", "mean", "top_k_mean")
  if (!length(aggregations) || !all(aggregations %in% valid_aggregations)) {
    stop("`aggregations` must be chosen from: ", paste(valid_aggregations, collapse = ", "), call. = FALSE)
  }
  top_k <- as.integer(top_k)
  if (is.na(top_k) || top_k <= 0L) {
    stop("`top_k` must be a positive integer.", call. = FALSE)
  }
  chunk_size <- as.integer(chunk_size)
  if (is.na(chunk_size) || chunk_size <= 0L) {
    stop("`chunk_size` must be a positive integer.", call. = FALSE)
  }

  target_ref_ids <- .litxr_select_score_ref_ids(
    collection_id = collection_id,
    cfg = cfg,
    ref_ids = ref_ids,
    date_from = date_from,
    date_to = date_to
  )
  if (!length(target_ref_ids)) {
    out <- data.table::data.table()
    if (isTRUE(include_query_scores)) {
      return(list(category_scores = out, query_scores = out))
    }
    return(out)
  }

  queries <- .litxr_read_label_query_index(query_set_id, cfg, model = model)
  if (!nrow(queries$metadata) || !nrow(queries$matrix)) {
    if (!nrow(queries$metadata) || !nrow(queries$matrix)) {
      .litxr_warn_label_query_model_mismatch(cfg, query_set_id, model)
    }
    out <- data.table::data.table()
    if (isTRUE(include_query_scores)) {
      return(list(category_scores = out, query_scores = out))
    }
    return(out)
  }

  if (!identical(as.character(model), as.character(queries$manifest$model %||% NA_character_))) {
    stop("Category query index model does not match `model`: ", queries$manifest$model %||% NA_character_, call. = FALSE)
  }
  parts <- .litxr_collect_embedding_score_parts(
    collection_id = collection_id,
    cfg = cfg,
    field = field,
    model = model,
    target_ref_ids = target_ref_ids,
    query_meta = queries$metadata,
    query_matrix = queries$matrix,
    aggregations = aggregations,
    top_k = top_k,
    chunk_size = chunk_size,
    include_query_scores = include_query_scores
  )
  category_scores <- parts$category_scores
  query_score_dt <- parts$query_scores

  if (isTRUE(include_query_scores)) {
    return(list(category_scores = category_scores, query_scores = query_score_dt))
  }
  category_scores
}

#' Score cached category queries against embedding delta shards
#'
#' Scores a cached category-query index against one or more selected embedding
#' delta shards for the same collection field and model. This uses locally
#' cached query vectors only and does not call any external embedding API.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param query_set_id Identifier of a cached category query set.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param field Embedded text field. Must match the delta shard model cache.
#' @param model Exact embedding model name used for both the collection delta
#'   shards and the cached query set.
#' @param aggregations Character vector of aggregation rules to compute. Choose
#'   from `"max"`, `"mean"`, and `"top_k_mean"`.
#' @param top_k Positive integer used when `aggregations` includes
#'   `"top_k_mean"`.
#' @param ref_ids Optional subset of `ref_id` values to score.
#' @param date Optional date used to filter delta shard filenames. Accepts
#'   values coercible by `as.Date()`.
#' @param shard Optional shard filename or full shard path under the collection's
#'   embedding delta directory.
#' @param include_query_scores Whether to also return per-query scores before
#'   aggregation.
#'
#' @return Aggregated category-score `data.table`, or a list with
#'   `category_scores` and `query_scores` when `include_query_scores = TRUE`.
#' @export
litxr_score_collection_categories_delta <- function(
  collection_id,
  query_set_id = "default",
  config = NULL,
  field = "abstract",
  model,
  aggregations = c("max", "mean"),
  top_k = 3L,
  ref_ids = NULL,
  date = NULL,
  shard = NULL,
  include_query_scores = FALSE
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }

  aggregations <- unique(as.character(aggregations))
  valid_aggregations <- c("max", "mean", "top_k_mean")
  if (!length(aggregations) || !all(aggregations %in% valid_aggregations)) {
    stop("`aggregations` must be chosen from: ", paste(valid_aggregations, collapse = ", "), call. = FALSE)
  }
  top_k <- as.integer(top_k)
  if (is.na(top_k) || top_k <= 0L) {
    stop("`top_k` must be a positive integer.", call. = FALSE)
  }

  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  shard_paths <- .litxr_select_embedding_delta_shards(paths, shard = shard, date = date)
  delta <- if (length(shard_paths)) .litxr_read_embedding_delta_shards(shard_paths) else list(
    metadata = .litxr_empty_embedding_metadata(),
    matrix = matrix(numeric(), nrow = 0L, ncol = 0L),
    manifest = list()
  )
  queries <- .litxr_read_label_query_index(query_set_id, cfg, model = model)
  if (!nrow(delta$metadata) || !nrow(delta$matrix) || !nrow(queries$metadata) || !nrow(queries$matrix)) {
    if (!nrow(queries$metadata) || !nrow(queries$matrix)) {
      .litxr_warn_label_query_model_mismatch(cfg, query_set_id, model)
    }
    out <- data.table::data.table()
    if (isTRUE(include_query_scores)) {
      return(list(category_scores = out, query_scores = out))
    }
    return(out)
  }

  manifest_model <- delta$manifest$model %||% NA_character_
  if (!identical(as.character(model), as.character(manifest_model))) {
    stop("Delta shard model does not match `model`: ", manifest_model, call. = FALSE)
  }
  if (!identical(as.character(model), as.character(queries$manifest$model %||% NA_character_))) {
    stop("Category query index model does not match `model`: ", queries$manifest$model %||% NA_character_, call. = FALSE)
  }
  if (ncol(delta$matrix) != ncol(queries$matrix)) {
    stop("Delta shard and query embedding dimensions do not match.", call. = FALSE)
  }

  corpus_meta <- data.table::copy(delta$metadata)
  corpus_matrix <- delta$matrix
  if (!is.null(ref_ids)) {
    keep <- corpus_meta$ref_id %in% as.character(ref_ids)
    corpus_meta <- corpus_meta[keep, ]
    corpus_matrix <- corpus_matrix[keep, , drop = FALSE]
  }
  if (!nrow(corpus_meta)) {
    out <- data.table::data.table()
    if (isTRUE(include_query_scores)) {
      return(list(category_scores = out, query_scores = out))
    }
    return(out)
  }

  score_matrix <- .litxr_cosine_similarity_matrix(corpus_matrix, queries$matrix)
  query_score_dt <- .litxr_query_score_table(corpus_meta, queries$metadata, score_matrix)
  category_scores <- .litxr_aggregate_category_scores(query_score_dt, aggregations = aggregations, top_k = top_k)

  if (isTRUE(include_query_scores)) {
    return(list(category_scores = category_scores, query_scores = query_score_dt))
  }
  category_scores
}

#' Label collection records from aggregated category scores
#'
#' Applies simple threshold rules to aggregated category scores produced by
#' `litxr_score_collection_categories()`.
#'
#' @param category_scores Aggregated score table from
#'   `litxr_score_collection_categories()`.
#' @param score_col Score column used for labeling, such as `"score_max"` or
#'   `"score_mean"`.
#' @param threshold Minimum score required for assignment. May be a scalar or a
#'   named numeric vector keyed by `category_id`.
#' @param multi_label Whether to return all categories above threshold for each
#'   record instead of a single best category.
#' @param margin Optional minimum gap between the top-ranked and second-ranked
#'   categories in single-label mode.
#'
#' @return In single-label mode, one row per `ref_id`. In multi-label mode, one
#'   row per assigned `ref_id` and `category_id`.
#' @export
litxr_label_collection_by_category <- function(
  category_scores,
  score_col = "score_max",
  threshold = 0,
  multi_label = FALSE,
  margin = NULL
) {
  dt <- data.table::as.data.table(category_scores)
  if (!nrow(dt)) {
    return(dt)
  }
  required_cols <- c("ref_id", "category_id", score_col)
  missing_cols <- setdiff(required_cols, names(dt))
  if (length(missing_cols)) {
    stop("Missing required columns in `category_scores`: ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  dt <- data.table::copy(dt)
  dt[["score"]] <- as.numeric(dt[[score_col]])
  dt[["threshold"]] <- .litxr_threshold_by_category(dt$category_id, threshold)
  dt[["above_threshold"]] <- !is.na(dt$score) & dt$score >= dt$threshold

  if (isTRUE(multi_label)) {
    keep_cols <- intersect(c("ref_id", "title", "category_id", "score", "threshold"), names(dt))
    out <- dt[dt$above_threshold, keep_cols, with = FALSE]
    if ("title" %in% names(dt)) {
      data.table::setcolorder(out, c("ref_id", "title", "category_id", "score", "threshold"))
    }
    return(out)
  }

  dt_ranked <- data.table::copy(dt)
  data.table::setorderv(dt_ranked, c("ref_id", "score", "category_id"), order = c(1L, -1L, 1L), na.last = TRUE)
  dt_ranked[["row_in_ref"]] <- stats::ave(seq_len(nrow(dt_ranked)), dt_ranked$ref_id, FUN = seq_along)

  top_rows <- dt_ranked[dt_ranked$row_in_ref == 1L, ]
  second_rows <- dt_ranked[dt_ranked$row_in_ref == 2L, c("ref_id", "score"), with = FALSE]
  data.table::setnames(second_rows, "score", "second_score")
  out <- merge(top_rows, second_rows, by = "ref_id", all.x = TRUE, sort = FALSE)

  out[["margin_to_second"]] <- out$score - out$second_score
  out[["margin_pass"]] <- if (is.null(margin)) TRUE else {
    margin_value <- as.numeric(margin[[1]])
    if (is.na(margin_value) || margin_value < 0) {
      stop("`margin` must be NULL or a non-negative number.", call. = FALSE)
    }
    is.na(out$second_score) | out$margin_to_second >= margin_value
  }
  out[["assigned"]] <- out$above_threshold & out$margin_pass
  out[["assigned_category_id"]] <- ifelse(out$assigned, out$category_id, NA_character_)
  out[["assigned_score"]] <- ifelse(out$assigned, out$score, NA_real_)

  keep_cols <- c("ref_id", "assigned_category_id", "assigned_score", "threshold", "above_threshold", "second_score", "margin_to_second", "margin_pass", "assigned")
  if ("title" %in% names(out)) {
    keep_cols <- append(keep_cols, "title", after = 1L)
  }
  out[, keep_cols, with = FALSE]
}

#' Compute cosine similarity against an embedding matrix
#'
#' @param query_vec Numeric query embedding vector.
#' @param embedding_matrix Numeric matrix with one embedding per row.
#'
#' @return Numeric vector of cosine similarity scores.
#' @export
litxr_cosine_similarity <- function(query_vec, embedding_matrix) {
  query_vec <- as.numeric(query_vec)
  embedding_matrix <- as.matrix(embedding_matrix)
  storage.mode(embedding_matrix) <- "double"
  if (ncol(embedding_matrix) != length(query_vec)) {
    stop("Query vector length does not match embedding matrix columns.", call. = FALSE)
  }
  numerator <- as.numeric(embedding_matrix %*% query_vec)
  denom <- sqrt(rowSums(embedding_matrix^2)) * sqrt(sum(query_vec^2))
  out <- numerator / denom
  out[is.na(out) | is.infinite(out)] <- 0
  out
}

.litxr_resolve_query_vector <- function(query = NULL, embed_fun = NULL, query_vec = NULL) {
  if (!is.null(query_vec)) {
    query_matrix <- .litxr_as_embedding_matrix(query_vec)
    if (nrow(query_matrix) != 1L) {
      stop("`query_vec` must be a single embedding vector.", call. = FALSE)
    }
    return(as.numeric(query_matrix[1, ]))
  }

  if (missing(query) || is.null(query) || !nzchar(as.character(query[[1]]))) {
    stop("`query` must be supplied and non-empty when `query_vec` is not supplied.", call. = FALSE)
  }
  if (!is.function(embed_fun)) {
    stop("`embed_fun` must be a function when `query_vec` is not supplied.", call. = FALSE)
  }

  query_matrix <- .litxr_as_embedding_matrix(embed_fun(as.character(query[[1]])))
  if (nrow(query_matrix) != 1L) {
    stop("`embed_fun` must return exactly one embedding for the query.", call. = FALSE)
  }
  as.numeric(query_matrix[1, ])
}

.litxr_normalize_label_query_set <- function(query_set) {
  if (is.character(query_set) && length(query_set) == 1L && file.exists(query_set)) {
    ext <- tolower(tools::file_ext(query_set))
    query_set <- switch(
      ext,
      json = jsonlite::fromJSON(query_set, simplifyVector = FALSE),
      yml = yaml::read_yaml(query_set),
      yaml = yaml::read_yaml(query_set),
      stop("Unsupported label query set file extension: ", ext, call. = FALSE)
    )
  }

  if (!is.list(query_set) || !length(query_set) || is.null(names(query_set)) || any(!nzchar(names(query_set)))) {
    stop("`query_set` must be a named list or a JSON/YAML file path containing a named list.", call. = FALSE)
  }

  rows <- unlist(lapply(names(query_set), function(category_id) {
    queries <- as.character(unlist(query_set[[category_id]], use.names = FALSE))
    queries <- queries[!is.na(queries) & nzchar(trimws(queries))]
    if (!length(queries)) {
      return(list())
    }
    lapply(seq_along(queries), function(i) {
      data.table::data.table(
        category_id = as.character(category_id),
        query_id = paste0(as.character(category_id), "__", sprintf("%03d", i)),
        query_order = as.integer(i),
        query_text = trimws(queries[[i]])
      )
    })
  }), recursive = FALSE)

  if (!length(rows)) {
    return(.litxr_empty_label_query_metadata()[, c("category_id", "query_id", "query_order", "query_text"), with = FALSE])
  }
  data.table::rbindlist(rows, fill = TRUE)
}

.litxr_label_query_index_paths <- function(cfg, query_set_id, model) {
  base_dir <- file.path(
    .litxr_project_embeddings_dir(cfg),
    "label_queries",
    .litxr_embedding_slug(query_set_id),
    .litxr_embedding_slug(model)
  )
  list(
    dir = base_dir,
    metadata = file.path(base_dir, "metadata.fst"),
    matrix = file.path(base_dir, "matrix.rds"),
    matrix_f32 = file.path(base_dir, "matrix.f32"),
    manifest = file.path(base_dir, "manifest.json")
  )
}

.litxr_empty_label_query_metadata <- function() {
  data.table::data.table(
    category_id = character(),
    query_id = character(),
    query_order = integer(),
    query_text = character()
  )
}

.litxr_read_label_query_index_parts <- function(paths, read_matrix = TRUE) {
  metadata <- if (file.exists(paths$metadata)) {
    fst::read_fst(paths$metadata, as.data.table = TRUE)
  } else {
    .litxr_empty_label_query_metadata()
  }
  matrix_data <- if (isTRUE(read_matrix) && file.exists(paths$matrix_f32)) {
    .litxr_read_float32_matrix(
      path = paths$matrix_f32,
      nrow = nrow(metadata),
      ncol = as.integer((jsonlite::fromJSON(paths$manifest, simplifyVector = FALSE)$dimension %||% 0L)),
      context = paths$dir
    )
  } else if (isTRUE(read_matrix) && file.exists(paths$matrix)) {
    tryCatch(
      readRDS(paths$matrix),
      error = function(e) {
        stop("Failed to read label query matrix cache at ", paths$matrix, ": ", conditionMessage(e), call. = FALSE)
      }
    )
  } else if (isTRUE(read_matrix)) {
    matrix(numeric(), nrow = 0L, ncol = 0L)
  } else {
    NULL
  }
  if (!is.null(matrix_data)) {
    matrix_data <- as.matrix(matrix_data)
    storage.mode(matrix_data) <- "double"
    if (nrow(metadata) != nrow(matrix_data)) {
      stop("Label query metadata rows do not match matrix rows in cache: ", paths$dir, call. = FALSE)
    }
  }
  manifest <- if (file.exists(paths$manifest)) {
    jsonlite::fromJSON(paths$manifest, simplifyVector = FALSE)
  } else {
    list()
  }
  list(metadata = metadata, matrix = matrix_data, manifest = manifest)
}

.litxr_write_label_query_index <- function(paths, metadata, matrix, manifest) {
  dir.create(paths$dir, recursive = TRUE, showWarnings = FALSE)
  .litxr_write_fst_atomic(as.data.frame(metadata), paths$metadata)
  .litxr_write_float32_matrix_atomic(matrix, paths$matrix_f32)
  if (file.exists(paths$matrix)) unlink(paths$matrix)
  .litxr_write_json_atomic(manifest, paths$manifest)
  invisible(paths)
}

.litxr_read_label_query_index <- function(query_set_id, cfg, model) {
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }
  .litxr_read_label_query_index_parts(
    .litxr_label_query_index_paths(cfg, query_set_id, model),
    read_matrix = TRUE
  )
}

.litxr_label_query_available_models <- function(cfg, query_set_id) {
  query_root <- file.path(
    .litxr_project_embeddings_dir(cfg),
    "label_queries",
    .litxr_embedding_slug(query_set_id)
  )
  if (!dir.exists(query_root)) {
    return(character())
  }
  model_dirs <- list.dirs(query_root, recursive = FALSE, full.names = TRUE)
  if (!length(model_dirs)) {
    return(character())
  }
  models <- vapply(model_dirs, function(path) {
    manifest_path <- file.path(path, "manifest.json")
    if (file.exists(manifest_path)) {
      manifest <- jsonlite::fromJSON(manifest_path, simplifyVector = FALSE)
      return(as.character(manifest$model %||% basename(path)))
    }
    basename(path)
  }, character(1))
  unique(models[nzchar(models)])
}

.litxr_warn_label_query_model_mismatch <- function(cfg, query_set_id, requested_model) {
  available_models <- .litxr_label_query_available_models(cfg, query_set_id)
  available_models <- setdiff(available_models, as.character(requested_model))
  if (!length(available_models)) {
    return(invisible(NULL))
  }
  warning(
    "No cached category query index found for `query_set_id = \"", query_set_id,
    "\"` under model `", requested_model, "`. Available cached models for this query set: ",
    paste(sort(available_models), collapse = ", "),
    ". Rebuild the query cache with the requested model or use a matching model.",
    call. = FALSE
  )
  invisible(NULL)
}

.litxr_parse_optional_date <- function(x, arg_name) {
  if (is.null(x)) {
    return(NULL)
  }
  out <- as.Date(x[[1]])
  if (is.na(out)) {
    stop(arg_name, " must be coercible by `as.Date()`.", call. = FALSE)
  }
  out
}

.litxr_select_score_ref_ids <- function(collection_id, cfg, ref_ids = NULL, date_from = NULL, date_to = NULL) {
  records <- litxr_read_collection(collection_id, cfg)
  if (!nrow(records)) {
    return(character())
  }
  out <- data.table::as.data.table(records)
  out <- out[!is.na(out$ref_id) & nzchar(out$ref_id), ]
  if (!nrow(out)) {
    return(character())
  }
  if (!is.null(ref_ids)) {
    out <- out[out$ref_id %in% as.character(ref_ids), ]
  }
  date_from <- .litxr_parse_optional_date(date_from, "`date_from`")
  date_to <- .litxr_parse_optional_date(date_to, "`date_to`")
  if (!is.null(date_from) && !is.null(date_to) && date_to < date_from) {
    stop("`date_to` must be on or after `date_from`.", call. = FALSE)
  }
  if (!is.null(date_from) || !is.null(date_to)) {
    pub_dates <- as.Date(out$pub_date)
    keep <- !is.na(pub_dates)
    if (!is.null(date_from)) keep <- keep & pub_dates >= date_from
    if (!is.null(date_to)) keep <- keep & pub_dates <= date_to
    out <- out[keep, ]
  }
  unique(as.character(out$ref_id))
}

.litxr_read_embedding_shard_part <- function(paths, shard_key, read_matrix = TRUE) {
  shard_paths <- .litxr_embedding_shard_paths(paths, shard_key)
  shard_manifest <- if (file.exists(shard_paths$manifest)) jsonlite::fromJSON(shard_paths$manifest, simplifyVector = FALSE) else list()
  top_manifest <- if (file.exists(paths$manifest)) jsonlite::fromJSON(paths$manifest, simplifyVector = FALSE) else list()
  metadata <- if (file.exists(shard_paths$metadata)) {
    .litxr_normalize_embedding_metadata(fst::read_fst(shard_paths$metadata, as.data.table = TRUE))
  } else {
    .litxr_empty_embedding_metadata()
  }
  matrix_data <- if (!isTRUE(read_matrix)) {
    NULL
  } else if (file.exists(shard_paths$matrix_f32)) {
    .litxr_read_float32_matrix(
      path = shard_paths$matrix_f32,
      nrow = nrow(metadata),
      ncol = as.integer(shard_manifest$dimension %||% top_manifest$dimension %||% 0L),
      context = shard_paths$dir
    )
  } else {
    matrix(numeric(), nrow = 0L, ncol = as.integer(shard_manifest$dimension %||% top_manifest$dimension %||% 0L))
  }
  list(metadata = metadata, matrix = matrix_data, manifest = shard_manifest)
}

.litxr_score_corpus_chunk <- function(corpus_meta, corpus_matrix, query_meta, query_matrix, aggregations, top_k, include_query_scores) {
  if (!nrow(corpus_meta) || !nrow(corpus_matrix)) {
    return(list(category_scores = data.table::data.table(), query_scores = data.table::data.table()))
  }
  if (ncol(corpus_matrix) != ncol(query_matrix)) {
    stop("Collection and query embedding dimensions do not match.", call. = FALSE)
  }
  score_matrix <- .litxr_cosine_similarity_matrix(corpus_matrix, query_matrix)
  query_score_dt <- if (isTRUE(include_query_scores)) {
    .litxr_query_score_table(corpus_meta, query_meta, score_matrix)
  } else {
    data.table::data.table()
  }
  category_scores <- .litxr_aggregate_category_scores_from_matrix(
    corpus_meta = corpus_meta,
    query_meta = query_meta,
    score_matrix = score_matrix,
    aggregations = aggregations,
    top_k = top_k
  )
  list(category_scores = category_scores, query_scores = query_score_dt)
}

.litxr_aggregate_category_scores_from_matrix <- function(corpus_meta, query_meta, score_matrix, aggregations = c("max", "mean"), top_k = 3L) {
  if (!nrow(corpus_meta) || !nrow(score_matrix) || !nrow(query_meta)) {
    return(data.table::data.table())
  }
  categories <- unique(as.character(query_meta$category_id))
  rows <- vector("list", length(categories))
  for (i in seq_along(categories)) {
    category_id <- categories[[i]]
    idx <- which(as.character(query_meta$category_id) == category_id)
    category_scores <- score_matrix[, idx, drop = FALSE]
    values <- list(
      ref_id = as.character(corpus_meta$ref_id),
      category_id = rep(category_id, nrow(corpus_meta)),
      query_n = length(idx)
    )
    if ("title" %in% names(corpus_meta)) values[["title"]] <- as.character(corpus_meta$title)
    if ("year" %in% names(corpus_meta)) values[["year"]] <- as.integer(corpus_meta$year)
    if ("max" %in% aggregations) values[["score_max"]] <- apply(category_scores, 1L, max)
    if ("mean" %in% aggregations) values[["score_mean"]] <- rowMeans(category_scores)
    if ("top_k_mean" %in% aggregations) {
      values[["score_top_k_mean"]] <- apply(category_scores, 1L, function(x) mean(utils::head(sort(x, decreasing = TRUE), top_k)))
    }
    rows[[i]] <- data.table::as.data.table(values)
  }
  data.table::rbindlist(rows, fill = TRUE)
}

.litxr_collect_embedding_score_parts <- function(collection_id, cfg, field, model, target_ref_ids, query_meta, query_matrix, aggregations, top_k, chunk_size, include_query_scores) {
  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  target_ref_ids <- unique(as.character(target_ref_ids))
  if (!length(target_ref_ids)) {
    out <- data.table::data.table()
    return(list(category_scores = out, query_scores = out))
  }

  category_parts <- list()
  query_parts <- list()

  append_part <- function(part) {
    if (nrow(part$category_scores)) {
      category_parts[[length(category_parts) + 1L]] <<- part$category_scores
    }
    if (isTRUE(include_query_scores) && nrow(part$query_scores)) {
      query_parts[[length(query_parts) + 1L]] <<- part$query_scores
    }
  }

  if (.litxr_embedding_has_shards(paths)) {
    records <- data.table::as.data.table(litxr_read_collection(collection_id, cfg))
    shard_keys <- unique(.litxr_embedding_shard_key(
      records$year[match(target_ref_ids, records$ref_id)]
    ))
    shard_keys <- shard_keys[nzchar(shard_keys)]
    shard_keys <- intersect(shard_keys, .litxr_embedding_shard_keys(paths))
    for (key in shard_keys) {
      shard <- .litxr_read_embedding_shard_part(paths, key, read_matrix = TRUE)
      if (!nrow(shard$metadata) || !nrow(shard$matrix)) next
      keep <- shard$metadata$ref_id %in% target_ref_ids
      if (!any(keep)) next
      shard_meta <- shard$metadata[keep, ]
      shard_matrix <- shard$matrix[keep, , drop = FALSE]
      for (start in seq(1L, nrow(shard_meta), by = chunk_size)) {
        end <- min(start + chunk_size - 1L, nrow(shard_meta))
        append_part(.litxr_score_corpus_chunk(
          corpus_meta = shard_meta[start:end, ],
          corpus_matrix = shard_matrix[start:end, , drop = FALSE],
          query_meta = query_meta,
          query_matrix = query_matrix,
          aggregations = aggregations,
          top_k = top_k,
          include_query_scores = include_query_scores
        ))
      }
    }
  } else {
    corpus <- litxr_read_embedding_index(collection_id, cfg, field = field, model = model)
    if (nrow(corpus$metadata) && nrow(corpus$matrix)) {
      keep <- corpus$metadata$ref_id %in% target_ref_ids
      corpus_meta <- corpus$metadata[keep, ]
      corpus_matrix <- corpus$matrix[keep, , drop = FALSE]
      for (start in seq(1L, nrow(corpus_meta), by = chunk_size)) {
        end <- min(start + chunk_size - 1L, nrow(corpus_meta))
        append_part(.litxr_score_corpus_chunk(
          corpus_meta = corpus_meta[start:end, ],
          corpus_matrix = corpus_matrix[start:end, , drop = FALSE],
          query_meta = query_meta,
          query_matrix = query_matrix,
          aggregations = aggregations,
          top_k = top_k,
          include_query_scores = include_query_scores
        ))
      }
    }
  }

  list(
    category_scores = if (length(category_parts)) data.table::rbindlist(category_parts, fill = TRUE) else data.table::data.table(),
    query_scores = if (isTRUE(include_query_scores) && length(query_parts)) data.table::rbindlist(query_parts, fill = TRUE) else data.table::data.table()
  )
}

.litxr_label_query_existing_dimension <- function(parts) {
  if (!is.null(parts$matrix) && nrow(parts$matrix)) {
    return(ncol(parts$matrix))
  }
  manifest_dimension <- suppressWarnings(as.integer(parts$manifest$dimension %||% NA_integer_))
  if (!is.na(manifest_dimension) && manifest_dimension > 0L) {
    return(manifest_dimension)
  }
  NA_integer_
}

.litxr_cosine_similarity_matrix <- function(embedding_matrix, query_matrix) {
  embedding_matrix <- as.matrix(embedding_matrix)
  query_matrix <- as.matrix(query_matrix)
  storage.mode(embedding_matrix) <- "double"
  storage.mode(query_matrix) <- "double"
  if (ncol(embedding_matrix) != ncol(query_matrix)) {
    stop("Embedding matrix and query matrix have different dimensions.", call. = FALSE)
  }

  embedding_norm <- sqrt(rowSums(embedding_matrix^2))
  query_norm <- sqrt(rowSums(query_matrix^2))
  denom <- embedding_norm %o% query_norm
  scores <- embedding_matrix %*% t(query_matrix)
  scores <- scores / denom
  scores[is.na(scores) | is.infinite(scores)] <- 0
  scores
}

.litxr_query_score_table <- function(corpus_meta, query_meta, score_matrix) {
  score_dt <- data.table::data.table(
    corpus_row = rep(seq_len(nrow(score_matrix)), times = ncol(score_matrix)),
    query_row = rep(seq_len(ncol(score_matrix)), each = nrow(score_matrix)),
    score = as.numeric(score_matrix)
  )

  corpus_map <- data.table::copy(corpus_meta)
  corpus_map[["corpus_row"]] <- seq_len(nrow(corpus_map))
  query_map <- data.table::copy(query_meta)
  query_map[["query_row"]] <- seq_len(nrow(query_map))

  out <- merge(score_dt, corpus_map, by = "corpus_row", all.x = TRUE, sort = FALSE)
  out <- merge(
    out,
    query_map[, c("query_row", "category_id", "query_id", "query_order", "query_text"), with = FALSE],
    by = "query_row",
    all.x = TRUE,
    sort = FALSE
  )
  keep_cols <- intersect(
    c("ref_id", "title", "year", "category_id", "query_id", "query_order", "query_text", "score"),
    names(out)
  )
  out[, keep_cols, with = FALSE]
}

.litxr_aggregate_category_scores <- function(query_score_dt, aggregations = c("max", "mean"), top_k = 3L) {
  dt <- data.table::copy(query_score_dt)
  if (!nrow(dt)) {
    return(data.table::data.table())
  }

  split_key <- paste(dt$ref_id, dt$category_id, sep = "\r")
  groups <- split(seq_len(nrow(dt)), split_key)
  rows <- lapply(groups, function(idx) {
    chunk <- dt[idx, , drop = FALSE]
    ordered_scores <- sort(chunk$score, decreasing = TRUE)
    values <- list(
      ref_id = chunk$ref_id[[1]],
      category_id = chunk$category_id[[1]],
      query_n = nrow(chunk)
    )
    if ("title" %in% names(chunk)) values[["title"]] <- chunk$title[[1]]
    if ("year" %in% names(chunk)) values[["year"]] <- chunk$year[[1]]
    if ("max" %in% aggregations) values[["score_max"]] <- max(chunk$score)
    if ("mean" %in% aggregations) values[["score_mean"]] <- mean(chunk$score)
    if ("top_k_mean" %in% aggregations) values[["score_top_k_mean"]] <- mean(utils::head(ordered_scores, top_k))
    data.table::as.data.table(values)
  })
  data.table::rbindlist(rows, fill = TRUE)
}

.litxr_threshold_by_category <- function(category_id, threshold) {
  if (length(threshold) == 1L && is.null(names(threshold))) {
    value <- as.numeric(threshold[[1]])
    if (is.na(value)) stop("`threshold` must be numeric.", call. = FALSE)
    return(rep(value, length(category_id)))
  }
  threshold_names <- names(threshold)
  if (is.null(threshold_names) || any(!nzchar(threshold_names))) {
    stop("`threshold` must be a scalar or a named numeric vector keyed by `category_id`.", call. = FALSE)
  }
  threshold <- stats::setNames(as.numeric(threshold), threshold_names)
  out <- unname(threshold[as.character(category_id)])
  out[is.na(out)] <- 0
  out
}

#' Create a default structured LLM digest template
#'
#' @param ref_id Reference identifier.
#' @param schema_version Digest schema version. Supported values are `"v2"`
#'   and `"v3"`.
#'
#' @return Named list representing the default digest schema.
#' @export
litxr_llm_digest_template <- function(ref_id, schema_version = "v2") {
  if (!identical(schema_version, "v2") && !identical(schema_version, "v3")) {
    stop("Only `schema_version = \"v2\"` or `\"v3\"` is supported for new templates in the current package version.", call. = FALSE)
  }
  if (identical(schema_version, "v3")) {
    return(.litxr_llm_digest_template_v3(ref_id))
  }
  .litxr_llm_digest_template_v2(ref_id)
}

#' Write one LLM digest for a reference
#'
#' Stores a structured JSON file under the project-level `llm/` directory using
#' `ref_id` as the canonical key.
#'
#' @param ref_id Reference identifier.
#' @param digest Named list containing digest fields.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param keep_history Whether to archive the previous current digest into
#'   `project.data_root/llm_history/` before replacing it.
#' @param bump_revision Whether to increment the digest revision when an
#'   existing current digest is replaced.
#'
#' @return Invisibly returns the written JSON path.
#' @export
litxr_write_llm_digest <- function(ref_id, digest, config = NULL, keep_history = TRUE, bump_revision = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  refs <- .litxr_read_project_references_by_keys(cfg, .litxr_expand_reference_keys(ref_id))
  if (nrow(refs) && !(ref_id %in% refs$ref_id)) {
    warning("Reference id not found in canonical store: ", ref_id, call. = FALSE)
  }

  existing <- litxr_read_llm_digest(ref_id, cfg)
  payload <- .litxr_normalize_llm_digest_for_write(digest, ref_id = ref_id)
  if (identical(.litxr_llm_digest_schema_version(payload), "v2")) {
    if (is.null(existing)) {
      existing_revision <- 0L
    } else if (!identical(.litxr_llm_digest_schema_version(existing), "v2")) {
      existing_revision <- 1L
    } else {
      existing_revision <- suppressWarnings(as.integer(existing$digest_revision %||% 1L))
      if (!length(existing_revision) || is.na(existing_revision[[1]]) || existing_revision[[1]] < 1L) {
        existing_revision <- 1L
      } else {
        existing_revision <- existing_revision[[1]]
      }
    }
    if (is.null(existing)) {
      payload$digest_revision <- 1L
      payload$derived_from_revision <- NULL
      payload$generated_at <- as.character(.litxr_first_nonnull(payload$generated_at, format(Sys.time(), tz = "UTC", usetz = TRUE)))
    } else if (isTRUE(bump_revision)) {
      payload$digest_revision <- existing_revision + 1L
      payload$derived_from_revision <- existing_revision
      payload$generated_at <- as.character(.litxr_first_nonnull(payload$generated_at, format(Sys.time(), tz = "UTC", usetz = TRUE)))
    } else {
      payload$digest_revision <- existing_revision
      payload$derived_from_revision <- existing$derived_from_revision %||% NULL
      payload$generated_at <- as.character(.litxr_first_nonnull(payload$generated_at, existing$generated_at, format(Sys.time(), tz = "UTC", usetz = TRUE)))
    }
    payload$updated_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  }
  litxr_validate_llm_digest(payload)

  path <- .litxr_llm_digest_path(cfg, ref_id)
  .litxr_ensure_project_llm_dir(cfg)
  if (!is.null(existing) && isTRUE(keep_history)) {
    .litxr_ensure_project_llm_history_dir(cfg)
    history_dir <- .litxr_llm_history_ref_dir(cfg, ref_id)
    dir.create(history_dir, recursive = TRUE, showWarnings = FALSE)
    history_path <- .litxr_llm_digest_history_path(cfg, ref_id, existing)
    .litxr_write_json_atomic(existing, history_path)
  }
  .litxr_write_json_atomic(payload, path)
  .litxr_update_enrichment_status_ref(cfg, ref_id)
  invisible(path)
}

#' Build one LLM digest from markdown
#'
#' Reads the canonical reference metadata and project-level markdown for one
#' `ref_id`, then calls a user-supplied builder function that returns digest
#' fields. The resulting digest is validated and optionally written to disk.
#'
#' @param ref_id Reference identifier.
#' @param builder Function taking arguments `ref`, `markdown`, and `template`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param overwrite Whether to overwrite an existing digest.
#' @param write Whether to write the digest to disk. If `FALSE`, returns the
#'   validated digest without writing.
#'
#' @return Named list digest.
#' @export
litxr_build_llm_digest <- function(ref_id, builder, config = NULL, overwrite = FALSE, write = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  if (!is.function(builder)) {
    stop("`builder` must be a function.", call. = FALSE)
  }

  existing <- litxr_read_llm_digest(ref_id, cfg)
  if (!is.null(existing) && !isTRUE(overwrite)) {
    stop("LLM digest already exists for ref_id: ", ref_id, ". Set `overwrite = TRUE` to replace it.", call. = FALSE)
  }

  refs <- litxr_read_references(cfg)
  ref_match <- refs[refs$ref_id == ref_id, ]
  if (!nrow(ref_match)) {
    stop("Reference id not found in canonical store: ", ref_id, call. = FALSE)
  }

  markdown <- litxr_read_md(ref_id, cfg)
  if (is.null(markdown)) {
    stop("Markdown not found for ref_id: ", ref_id, call. = FALSE)
  }

  template <- litxr_llm_digest_template(ref_id)
  built <- builder(ref = ref_match[1, ], markdown = markdown, template = template)
  if (is.null(built) || !is.list(built)) {
    stop("`builder` must return a named list of digest fields.", call. = FALSE)
  }

  payload <- .litxr_normalize_llm_digest_for_write(built, ref_id = ref_id)
  litxr_validate_llm_digest(payload)

  if (isTRUE(write)) {
    litxr_write_llm_digest(ref_id, payload, cfg)
  }
  payload
}

#' Build LLM digests in batch for references with markdown
#'
#' Selects references from the enrichment status index and builds digests for
#' those with markdown content and no digest yet, unless explicit `ref_ids` are
#' supplied.
#'
#' @param builder Function taking arguments `ref`, `markdown`, and `template`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param ref_ids Optional explicit character vector of reference ids to build.
#' @param overwrite Whether to overwrite existing digests.
#' @param limit Optional maximum number of digests to build in this run.
#'
#' @return Named list of built digests.
#' @export
litxr_build_llm_digests <- function(builder, config = NULL, ref_ids = NULL, overwrite = FALSE, limit = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  targets <- if (!is.null(ref_ids) && length(ref_ids)) {
    unique(as.character(ref_ids))
  } else {
    status <- litxr_read_enrichment_status(cfg)
    status <- status[status$has_md & (overwrite | !status$has_llm_digest), ]
    status$ref_id
  }

  if (!length(targets)) {
    return(list())
  }

  if (!is.null(limit)) {
    targets <- targets[seq_len(min(length(targets), as.integer(limit)))]
  }

  out <- stats::setNames(vector("list", length(targets)), targets)
  for (i in seq_along(targets)) {
    out[[i]] <- litxr_build_llm_digest(
      ref_id = targets[[i]],
      builder = builder,
      config = cfg,
      overwrite = overwrite,
      write = TRUE
    )
  }
  out
}

#' Read one LLM digest by reference id
#'
#' @param ref_id Reference identifier.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Named list digest, or `NULL` if not found.
#' @export
litxr_read_llm_digest <- function(ref_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  path <- .litxr_llm_digest_path(cfg, ref_id)
  if (!file.exists(path)) {
    return(NULL)
  }
  digest <- .litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE))
  litxr_validate_llm_digest(digest)
  digest
}

#' Read all project-level LLM digests
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param ref_ids Optional character vector of reference ids to keep.
#'
#' @return `data.table` of digest summaries with selected structured fields.
#' @export
litxr_read_llm_digests <- function(config = NULL, ref_ids = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  llm_dir <- .litxr_project_llm_dir(cfg)
  if (!dir.exists(llm_dir)) {
    return(data.table::data.table())
  }

  files <- list.files(llm_dir, pattern = "\\.json$", full.names = TRUE)
  if (!length(files)) {
    return(data.table::data.table())
  }

  rows <- lapply(files, function(path) {
    x <- .litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE))
    data.table::data.table(
      schema_version = x$schema_version %||% "v1",
      ref_id = x$ref_id %||% NA_character_,
      digest_revision = suppressWarnings(as.integer(x$digest_revision %||% 1L)),
      derived_from_revision = suppressWarnings(as.integer(x$derived_from_revision %||% NA_integer_)),
      extraction_mode = x$extraction_mode %||% NA_character_,
      prompt_version = x$prompt_version %||% NA_character_,
      model_hint = x$model_hint %||% NA_character_,
      paper_type = x$paper_type %||% NA_character_,
      summary = x$summary %||% NA_character_,
      motivation = x$motivation %||% NA_character_,
      research_questions = list(unlist(x$research_questions %||% character(), use.names = FALSE)),
      paper_structure = list(unlist(x$paper_structure %||% character(), use.names = FALSE)),
      methods = list(unlist(x$methods %||% character(), use.names = FALSE)),
      research_data = list(x$research_data %||% NULL),
      identification_strategy = x$identification_strategy %||% NA_character_,
      main_variables = list(x$main_variables %||% NULL),
      key_findings = list(unlist(x$key_findings %||% character(), use.names = FALSE)),
      limitations = list(unlist(x$limitations %||% character(), use.names = FALSE)),
      theoretical_mechanism = x$theoretical_mechanism %||% NA_character_,
      empirical_setting = x$empirical_setting %||% NA_character_,
      descriptive_statistics_summary = x$descriptive_statistics_summary %||% NA_character_,
      standardized_findings_summary = x$standardized_findings_summary %||% NA_character_,
      contribution_type = list(unlist(x$contribution_type %||% character(), use.names = FALSE)),
      evidence_strength = x$evidence_strength %||% NA_character_,
      anchor_references = list(x$anchor_references %||% list()),
      citation_logic_nodes = list(x$citation_logic_nodes %||% list()),
      keywords = list(unlist(x$keywords %||% character(), use.names = FALSE)),
      notes = x$notes %||% NA_character_,
      generated_at = x$generated_at %||% NA_character_,
      updated_at = x$updated_at %||% NA_character_
    )
  })

  out <- data.table::rbindlist(rows, fill = TRUE)
  if (!is.null(ref_ids) && length(ref_ids)) {
    out <- out[out$ref_id %in% ref_ids, ]
  }
  out
}

#' Find LLM digests by text and optional collection membership
#'
#' @param query Optional substring query matched across digest text fields.
#' @param collection_id Optional collection membership filter via `ref_id`.
#' @param ref_id Optional direct reference id filter.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Filtered `data.table` of digests.
#' @export
litxr_find_llm <- function(query = NULL, collection_id = NULL, ref_id = NULL, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  digests <- litxr_read_llm_digests(cfg, ref_ids = ref_id)
  if (!nrow(digests)) {
    return(digests)
  }

  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    links <- litxr_read_reference_collections(cfg)
    keep_ref_ids <- unique(links$ref_id[links$collection_id == collection_id])
    digests <- digests[digests$ref_id %in% keep_ref_ids, ]
  }

  if (!is.null(query) && nzchar(as.character(query))) {
    q <- tolower(as.character(query[[1]]))
    flat_text <- vapply(seq_len(nrow(digests)), function(i) {
      row <- digests[i, ]
      flatten_cell <- function(x) {
        if (is.null(x) || length(x) == 0L) return(character())
        if (is.list(x) && length(x) == 1L) x <- x[[1]]
        if (is.null(x) || length(x) == 0L) return(character())
        as.character(unlist(x, use.names = FALSE))
      }
      paste(
        c(
          flatten_cell(row$summary[[1]]),
          flatten_cell(row$motivation[[1]]),
          flatten_cell(row$notes[[1]]),
          flatten_cell(row$research_questions[[1]]),
          flatten_cell(row$paper_structure[[1]]),
          flatten_cell(row$methods[[1]]),
          flatten_cell(row$research_data[[1]]),
          flatten_cell(row$identification_strategy[[1]]),
          flatten_cell(row$main_variables[[1]]),
          flatten_cell(row$key_findings[[1]]),
          flatten_cell(row$limitations[[1]]),
          flatten_cell(row$theoretical_mechanism[[1]]),
          flatten_cell(row$empirical_setting[[1]]),
          flatten_cell(row$descriptive_statistics_summary[[1]]),
          flatten_cell(row$standardized_findings_summary[[1]]),
          flatten_cell(row$anchor_references[[1]]),
          flatten_cell(row$citation_logic_nodes[[1]]),
          flatten_cell(row$contribution_type[[1]]),
          flatten_cell(row$evidence_strength[[1]]),
          flatten_cell(row$keywords[[1]])
        ),
        collapse = " "
      )
    }, character(1))
    keep <- !is.na(flat_text) & grepl(q, tolower(flat_text), fixed = TRUE)
    digests <- digests[keep, ]
  }

  digests
}

#' List current and historical LLM digest revisions for one reference
#'
#' @param ref_id Reference identifier.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` describing available current and archived digest
#'   revisions.
#' @export
litxr_list_llm_digest_revisions <- function(ref_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  rows <- list()
  current_path <- .litxr_llm_digest_path(cfg, ref_id)
  if (file.exists(current_path)) {
    current <- litxr_read_llm_digest(ref_id, cfg)
    rows[[length(rows) + 1L]] <- data.table::data.table(
      ref_id = ref_id,
      is_current = TRUE,
      path = current_path,
      schema_version = current$schema_version %||% "v1",
      digest_revision = suppressWarnings(as.integer(current$digest_revision %||% 1L)),
      derived_from_revision = suppressWarnings(as.integer(current$derived_from_revision %||% NA_integer_)),
      extraction_mode = current$extraction_mode %||% NA_character_,
      prompt_version = current$prompt_version %||% NA_character_,
      model_hint = current$model_hint %||% NA_character_,
      generated_at = current$generated_at %||% NA_character_,
      updated_at = current$updated_at %||% NA_character_
    )
  }

  history_dir <- .litxr_llm_history_ref_dir(cfg, ref_id)
  if (dir.exists(history_dir)) {
    history_files <- list.files(history_dir, pattern = "\\.json$", full.names = TRUE)
    history_rows <- lapply(history_files, function(path) {
      digest <- .litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE))
      litxr_validate_llm_digest(digest)
      data.table::data.table(
        ref_id = ref_id,
        is_current = FALSE,
        path = path,
        schema_version = digest$schema_version %||% "v1",
        digest_revision = suppressWarnings(as.integer(digest$digest_revision %||% 1L)),
        derived_from_revision = suppressWarnings(as.integer(digest$derived_from_revision %||% NA_integer_)),
        extraction_mode = digest$extraction_mode %||% NA_character_,
        prompt_version = digest$prompt_version %||% NA_character_,
        model_hint = digest$model_hint %||% NA_character_,
        generated_at = digest$generated_at %||% NA_character_,
        updated_at = digest$updated_at %||% NA_character_
      )
    })
    rows <- c(rows, history_rows)
  }

  if (!length(rows)) {
    return(data.table::data.table(
      ref_id = character(),
      is_current = logical(),
      path = character(),
      schema_version = character(),
      digest_revision = integer(),
      derived_from_revision = integer(),
      extraction_mode = character(),
      prompt_version = character(),
      model_hint = character(),
      generated_at = character(),
      updated_at = character()
    ))
  }

  out <- data.table::rbindlist(rows, fill = TRUE)
  out[order(-out$is_current, -out$digest_revision, out$path), ]
}

#' Read historical LLM digests for one reference
#'
#' @param ref_id Reference identifier.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param include_current Whether to include the current digest in the returned
#'   history table.
#'
#' @return `data.table` with digest metadata plus a list-column `digest`.
#' @export
litxr_read_llm_digest_history <- function(ref_id, config = NULL, include_current = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  revisions <- litxr_list_llm_digest_revisions(ref_id, cfg)
  if (!nrow(revisions)) {
    revisions$digest <- list()
    return(revisions)
  }
  if (!isTRUE(include_current)) {
    revisions <- revisions[!revisions$is_current, ]
  }
  if (!nrow(revisions)) {
    revisions$digest <- list()
    return(revisions)
  }

  digests <- lapply(seq_len(nrow(revisions)), function(i) {
    path <- revisions$path[[i]]
    .litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE))
  })
  revisions$digest <- digests
  revisions
}

#' Write markdown content for one reference
#'
#' Stores markdown under the project-level `md/` directory using `ref_id` as the
#' canonical key.
#'
#' @param ref_id Reference identifier.
#' @param text Markdown text.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the written markdown path.
#' @export
litxr_write_md <- function(ref_id, text, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_ensure_project_md_dir(cfg)
  path <- .litxr_md_path(cfg, ref_id)
  writeLines(as.character(text), path, useBytes = TRUE)
  .litxr_update_enrichment_status_ref(cfg, ref_id)
  invisible(path)
}

#' Read markdown content for one reference
#'
#' @param ref_id Reference identifier.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Character scalar markdown text, or `NULL` if not found.
#' @export
litxr_read_md <- function(ref_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  path <- .litxr_md_path(cfg, ref_id)
  if (!file.exists(path)) {
    return(NULL)
  }
  paste(readLines(path, warn = FALSE), collapse = "\n")
}

#' Validate one LLM digest against the litxr schema
#'
#' @param digest Named list digest.
#'
#' @return Invisibly returns `TRUE` on success, otherwise errors.
#' @export
litxr_validate_llm_digest <- function(digest) {
  schema_version <- .litxr_llm_digest_schema_version(digest)
  required <- .litxr_llm_digest_required_fields(schema_version)
  missing <- setdiff(required, names(digest))
  if (length(missing)) {
    stop("LLM digest is missing required fields: ", paste(missing, collapse = ", "), call. = FALSE)
  }

  if (identical(schema_version, "v2") || identical(schema_version, "v3")) {
    litxr_validate_paper_type(digest$paper_type)
    digest_revision <- suppressWarnings(as.integer(digest$digest_revision[[1]] %||% digest$digest_revision))
    if (length(digest_revision) != 1L || is.na(digest_revision) || digest_revision < 1L) {
      stop("LLM digest field `digest_revision` must be a positive integer.", call. = FALSE)
    }
    if (!is.null(digest$derived_from_revision) && length(digest$derived_from_revision)) {
      derived <- suppressWarnings(as.integer(digest$derived_from_revision[[1]]))
      if (length(derived) != 1L || is.na(derived) || derived < 1L) {
        stop("LLM digest field `derived_from_revision` must be NULL or a positive integer.", call. = FALSE)
      }
    }
    extraction_mode <- as.character(digest$extraction_mode[[1]] %||% digest$extraction_mode)
    if (!length(extraction_mode) || is.na(extraction_mode[[1]]) || !nzchar(extraction_mode[[1]])) {
      stop("LLM digest field `extraction_mode` must be a non-empty character scalar.", call. = FALSE)
    }
    .litxr_validate_list_fields(
      digest,
      c(
        "research_questions", "paper_structure", "methods", "key_findings",
        "limitations", "contribution_type", "keywords"
      )
    )
    .litxr_validate_named_list_fields(
      digest$research_data,
      required_fields = c("data_sources", "sample_period", "sample_region", "unit_of_observation", "sample_size"),
      field_name = "research_data",
      character_vector_fields = "data_sources",
      numeric_scalar_fields = "sample_size"
    )
    .litxr_validate_named_list_fields(
      digest$main_variables,
      required_fields = c(
        "dependent_variables", "independent_variables",
        "control_variables", "mechanism_variables"
      ),
      field_name = "main_variables",
      character_vector_fields = c(
        "dependent_variables", "independent_variables",
        "control_variables", "mechanism_variables"
      )
    )
    if ("anchor_references" %in% names(digest)) {
      .litxr_validate_inline_llm_table_field(
        digest$anchor_references,
        "anchor_references",
        litxr_validate_anchor_references,
        ref_id = digest$ref_id %||% NULL
      )
    }
    if ("citation_logic_nodes" %in% names(digest)) {
      .litxr_validate_inline_llm_table_field(
        digest$citation_logic_nodes,
        "citation_logic_nodes",
        litxr_validate_citation_logic_nodes,
        ref_id = digest$ref_id %||% NULL
      )
    }
  } else {
    if (!is.list(digest$sample)) {
      stop("LLM digest field `sample` must be a named list.", call. = FALSE)
    }
    .litxr_validate_list_fields(
      digest,
      c("research_questions", "methods", "key_findings", "limitations", "keywords")
    )
  }
  invisible(TRUE)
}

#' Read project-level enrichment status
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` with one row per reference and enrichment flags.
#' @export
litxr_read_enrichment_status <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_read_enrichment_status_index(cfg)
}

#' Read project-level sync state
#'
#' Returns the project sync ledger used to track completed sync and repair runs.
#' This state lives under `project.data_root/index/` and is separate from
#' `config.yaml`.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param collection_id Optional collection filter.
#'
#' @return `data.table` of sync ledger rows.
#' @export
litxr_read_sync_state <- function(config = NULL, collection_id = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  out <- .litxr_read_sync_state_index(cfg)
  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    out <- out[out$collection_id == as.character(collection_id[[1]]), ]
  }
  out
}

#' Rebuild a first-pass sync ledger from existing local data
#'
#' Infers one sync ledger row per collection from existing local storage and
#' index file timestamps. This is intended for backfilling `sync_state.fst` when
#' local data already exists from older package versions that did not record
#' sync history.
#'
#' The inferred rows are approximate. They record that local data exists and
#' when the local collection index was last modified, but they do not recover
#' exact remote cursors, arXiv day windows, or original sync arguments.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param overwrite Whether to replace the existing sync ledger instead of only
#'   adding inferred rows for collections not already present.
#'
#' @return `data.table` of the rebuilt sync ledger.
#' @export
litxr_rebuild_sync_state <- function(config = NULL, overwrite = FALSE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  collections <- .litxr_config_collections(cfg)
  existing <- .litxr_read_sync_state_index(cfg)
  inferred <- lapply(collections, function(collection) {
    .litxr_infer_collection_sync_state(cfg, collection)
  })
  inferred <- data.table::rbindlist(inferred, fill = TRUE)

  if (!nrow(inferred)) {
    if (isTRUE(overwrite)) {
      .litxr_write_sync_state_index(cfg, .litxr_empty_sync_state())
      return(.litxr_empty_sync_state())
    }
    return(existing)
  }

  if (isTRUE(overwrite) || !nrow(existing)) {
    out <- inferred
  } else {
    covered <- unique(existing$collection_id)
    out <- data.table::rbindlist(
      list(existing, inferred[!(inferred$collection_id %in% covered), ]),
      fill = TRUE
    )
  }

  .litxr_write_sync_state_index(cfg, out)
  out
}

#' List enrichment candidates and exclusion reasons
#'
#' Combines the canonical reference store, collection memberships, and
#' enrichment status into a simple table that shows which references are ready
#' for digest building and why others are excluded.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param collection_id Optional collection membership filter.
#' @param ref_ids Optional character vector of reference ids to keep.
#' @param include_ready Whether to keep rows that are currently ready for digest
#'   building.
#'
#' @return `data.table` with eligibility flags and reasons.
#' @export
litxr_list_enrichment_candidates <- function(config = NULL, collection_id = NULL, ref_ids = NULL, include_ready = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  refs <- litxr_read_references(cfg)
  status <- litxr_read_enrichment_status(cfg)
  links <- litxr_read_reference_collections(cfg)

  if (!nrow(refs)) {
    return(data.table::data.table(
      ref_id = character(),
      title = character(),
      entry_type = character(),
      collection_ids = character(),
      has_md = logical(),
      has_llm_digest = logical(),
      eligible = logical(),
      reason = character()
    ))
  }

  collection_map <- if (nrow(links)) {
    split_ids <- split(as.character(links$collection_id), as.character(links$ref_id))
    data.table::data.table(
      ref_id = names(split_ids),
      collection_ids = vapply(
        split_ids,
        function(x) paste(sort(unique(x)), collapse = ","),
        character(1)
      )
    )
  } else {
    data.table::data.table(ref_id = character(), collection_ids = character())
  }

  ref_view <- data.table::data.table(
    ref_id = as.character(refs$ref_id),
    title = as.character(refs$title),
    entry_type = as.character(refs$entry_type)
  )
  status_view <- data.table::data.table(
    ref_id = as.character(status$ref_id),
    has_md = as.logical(status$has_md),
    has_llm_digest = as.logical(status$has_llm_digest)
  )

  out <- merge(ref_view, status_view, by = "ref_id", all.x = TRUE, sort = FALSE)
  out <- merge(out, collection_map, by = "ref_id", all.x = TRUE, sort = FALSE)
  out$has_md[is.na(out$has_md)] <- FALSE
  out$has_llm_digest[is.na(out$has_llm_digest)] <- FALSE
  out$collection_ids[is.na(out$collection_ids)] <- ""

  if (!is.null(ref_ids) && length(ref_ids)) {
    keep_ids <- unique(as.character(ref_ids))
    out <- out[out$ref_id %in% keep_ids, ]
  }

  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    needle <- as.character(collection_id[[1]])
    out <- out[
      vapply(
        strsplit(out$collection_ids, ",", fixed = TRUE),
        function(x) needle %in% x,
        logical(1)
      ),
    ]
  }

  out$eligible <- out$has_md & !out$has_llm_digest
  out$reason <- ifelse(
    !out$has_md,
    "missing_md",
    ifelse(out$has_llm_digest, "digest_exists", "ready")
  )

  if (!isTRUE(include_ready)) {
    out <- out[!out$eligible, ]
  }

  data.table::setcolorder(
    out,
    c("ref_id", "title", "entry_type", "collection_ids", "has_md", "has_llm_digest", "eligible", "reason")
  )
  out[]
}

#' Add references by DOI and auto-register missing collections
#'
#' Fetches Crossref metadata for a DOI vector, writes the records into the local
#' collection stores, and auto-registers Crossref collections in `config.yaml`
#' when needed.
#'
#' @param dois Character vector of DOIs.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param auto_register Whether to auto-register missing collections into
#'   `config.yaml`.
#'
#' @return `data.table` of fetched records.
#' @export
litxr_add_dois <- function(dois, config = NULL, auto_register = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  doi_values <- unique(trimws(as.character(dois)))
  doi_values <- doi_values[nzchar(doi_values)]
  if (!length(doi_values)) {
    return(data.table::data.table())
  }

  messages <- fetch_crossref_messages(doi_values)
  found <- !vapply(messages, is.null, logical(1))
  missing_dois <- names(messages)[!found]
  if (length(missing_dois)) {
    warning(
      "The following DOIs were not found and were ignored: ",
      paste(missing_dois, collapse = ", "),
      call. = FALSE
    )
  }

  messages <- messages[found]
  if (!length(messages)) {
    return(data.table::data.table())
  }

  parsed <- lapply(messages, parse_crossref_entry_unified)
  records <- data.table::rbindlist(parsed, fill = TRUE)
  if (!nrow(records)) {
    return(records)
  }

  assignment <- .litxr_assign_crossref_journals(cfg, records, messages, auto_register = auto_register)
  cfg <- assignment$cfg
  records <- assignment$records

  by_journal <- split(records, records$collection_id)
  out <- lapply(names(by_journal), function(journal_id) {
    journal <- .litxr_get_journal(cfg, journal_id)
    local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
    incoming <- data.table::as.data.table(by_journal[[journal_id]])
    existing <- .litxr_read_journal_records(local_path)
    .litxr_write_journal_upserted_records(existing, incoming, local_path, journal, cfg = cfg)
    incoming
  })

  data.table::rbindlist(out, fill = TRUE)
}

#' Add manually supplied references to a collection
#'
#' Accepts a normalized reference table and writes the rows into the target
#' local collection store. This is the main manual-ingest path for books,
#' reports, news, conference papers, web references, and other non-DOI sources.
#'
#' @param refs A `data.frame` or `data.table` of normalized reference fields.
#' @param collection_id Target collection identifier.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param auto_register Whether to create the target collection automatically
#'   when it does not already exist.
#' @param collection_title Optional collection title used when auto-registering.
#'
#' @return `data.table` of ingested records.
#' @export
litxr_add_refs <- function(
  refs,
  collection_id,
  config = NULL,
  auto_register = TRUE,
  collection_title = NULL
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  records <- .litxr_normalize_manual_refs(refs)
  if (!nrow(records)) {
    return(records)
  }

  collection <- tryCatch(
    .litxr_get_journal(cfg, collection_id),
    error = function(e) NULL
  )

  if (is.null(collection)) {
    if (!isTRUE(auto_register)) {
      stop("Collection not found in config: ", collection_id, call. = FALSE)
    }
    registered <- .litxr_register_manual_collection(cfg, collection_id, collection_title = collection_title)
    cfg <- registered$cfg
    collection <- registered$collection
  }

  records[["collection_id"]] <- collection$collection_id
  records[["collection_title"]] <- collection$title

  local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
  existing <- .litxr_read_journal_records(local_path)
  .litxr_write_journal_upserted_records(existing, records, local_path, collection, cfg = cfg)
  records
}

.litxr_filter_records_by_keys <- function(records, keys) {
  if (!nrow(records) || !length(keys)) {
    return(records[0, ])
  }

  keys <- .litxr_expand_reference_keys(keys)
  out <- data.table::copy(records)
  doi <- if ("doi" %in% names(out)) out[["doi"]] else rep(NA_character_, nrow(out))
  ref_id <- if ("ref_id" %in% names(out)) out[["ref_id"]] else rep(NA_character_, nrow(out))
  source_id <- if ("source_id" %in% names(out)) out[["source_id"]] else rep(NA_character_, nrow(out))

  matched_key <- rep(NA_character_, nrow(out))
  doi_match <- !is.na(doi) & doi %in% keys
  matched_key[doi_match] <- doi[doi_match]

  ref_id_match <- !is.na(ref_id) & ref_id %in% keys & is.na(matched_key)
  matched_key[ref_id_match] <- ref_id[ref_id_match]

  source_id_match <- !is.na(source_id) & source_id %in% keys & is.na(matched_key)
  matched_key[source_id_match] <- source_id[source_id_match]

  keep <- !is.na(matched_key)
  out[["litxr_matched_key__"]] <- matched_key

  out[keep, ]
}

.litxr_expand_reference_keys <- function(keys) {
  keys <- unique(as.character(unlist(keys, use.names = FALSE)))
  keys <- keys[!is.na(keys) & nzchar(keys)]
  if (!length(keys)) {
    return(character())
  }

  arxiv_base <- keys
  arxiv_base <- sub("^arxiv:", "", arxiv_base, ignore.case = TRUE)
  arxiv_base <- sub("v[0-9]+$", "", arxiv_base)
  arxiv_like <- grepl("^[0-9]{4}\\.[0-9]{4,5}$", arxiv_base) |
    grepl("^[a-z.-]+/[0-9]{7}$", arxiv_base, ignore.case = TRUE)
  unique(c(keys, arxiv_base[arxiv_like], paste0("arxiv:", arxiv_base[arxiv_like])))
}

.litxr_keys_include_arxiv <- function(keys) {
  keys <- as.character(keys)
  any(
    grepl("^arxiv:", keys, ignore.case = TRUE) |
      grepl("^[0-9]{4}\\.[0-9]{4,5}(v[0-9]+)?$", keys) |
      grepl("^[a-z.-]+/[0-9]{7}(v[0-9]+)?$", keys, ignore.case = TRUE)
  )
}

.litxr_find_collection_refs_by_keys <- function(cfg, keys, collection_id = NULL) {
  collections <- .litxr_config_collections(cfg)
  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    keep_id <- as.character(collection_id[[1]])
    collections <- Filter(function(collection) {
      identical(collection$collection_id, keep_id) || identical(collection$journal_id, keep_id)
    }, collections)
  } else if (.litxr_keys_include_arxiv(keys)) {
    collections <- Filter(function(collection) {
      identical(collection$remote_channel, "arxiv")
    }, collections)
  }
  if (!length(collections)) {
    return(data.table::data.table())
  }

  rows <- lapply(collections, function(collection) {
    local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
    records <- .litxr_read_journal_records_by_keys(local_path, keys)
    delta <- .litxr_read_collection_delta(local_path)
    if (nrow(delta)) {
      delta_source_id <- if ("source_id" %in% names(delta)) delta$source_id else rep(NA_character_, nrow(delta))
      delta <- delta[delta$ref_id %in% keys | delta_source_id %in% keys, ]
      if (nrow(delta)) {
        records <- .litxr_upsert_records(records, delta)
      }
    }
    if (!nrow(records)) {
      return(records)
    }
    records
  })
  out <- data.table::rbindlist(rows, fill = TRUE)
  if (!nrow(out)) {
    return(out)
  }
  key <- .litxr_upsert_key(out)
  out[!duplicated(key), ]
}

.litxr_normalize_manual_refs <- function(refs) {
  if (is.null(refs)) {
    return(data.table::data.table())
  }

  out <- data.table::as.data.table(refs)
  if (!nrow(out)) {
    return(out)
  }

  if (!("authors_list" %in% names(out))) {
    if ("authors" %in% names(out)) {
      out[["authors_list"]] <- lapply(out[["authors"]], function(x) {
        if (is.null(x) || is.na(x) || !nzchar(x)) return(character())
        trimws(strsplit(as.character(x), ";", fixed = TRUE)[[1]])
      })
    } else {
      out[["authors_list"]] <- rep(list(character()), nrow(out))
    }
  }

  if (!("authors" %in% names(out))) {
    out[["authors"]] <- vapply(out[["authors_list"]], function(x) {
      paste(x, collapse = "; ")
    }, character(1))
  }

  if (!("entry_type" %in% names(out))) {
    out[["entry_type"]] <- if ("source" %in% names(out)) {
      vapply(out[["source"]], .litxr_entry_type_from_source, character(1))
    } else {
      rep("misc", nrow(out))
    }
  }

  if (!("source" %in% names(out))) out[["source"]] <- rep("manual", nrow(out))
  if (!("source_id" %in% names(out))) out[["source_id"]] <- rep(NA_character_, nrow(out))
  if (!("doi" %in% names(out))) out[["doi"]] <- rep(NA_character_, nrow(out))
  if (!("isbn" %in% names(out))) out[["isbn"]] <- rep(NA_character_, nrow(out))
  if (!("issn" %in% names(out))) out[["issn"]] <- rep(NA_character_, nrow(out))
  if (!("url" %in% names(out))) out[["url"]] <- rep(NA_character_, nrow(out))
  if (!("url_landing" %in% names(out))) out[["url_landing"]] <- out[["url"]]
  if (!("url_pdf" %in% names(out))) out[["url_pdf"]] <- rep(NA_character_, nrow(out))
  if (!("note" %in% names(out))) out[["note"]] <- rep(NA_character_, nrow(out))
  if (!("abstract" %in% names(out))) out[["abstract"]] <- rep(NA_character_, nrow(out))
  if (!("subject_primary" %in% names(out))) out[["subject_primary"]] <- rep(NA_character_, nrow(out))
  if (!("subject_all" %in% names(out))) out[["subject_all"]] <- rep(NA_character_, nrow(out))
  if (!("journal" %in% names(out))) out[["journal"]] <- rep(NA_character_, nrow(out))
  if (!("container_title" %in% names(out))) out[["container_title"]] <- rep(NA_character_, nrow(out))
  if (!("publisher" %in% names(out))) out[["publisher"]] <- rep(NA_character_, nrow(out))
  if (!("volume" %in% names(out))) out[["volume"]] <- rep(NA_character_, nrow(out))
  if (!("issue" %in% names(out))) out[["issue"]] <- rep(NA_character_, nrow(out))
  if (!("pages" %in% names(out))) out[["pages"]] <- rep(NA_character_, nrow(out))
  if (!("arxiv_version" %in% names(out))) out[["arxiv_version"]] <- rep(NA_integer_, nrow(out))
  if (!("arxiv_primary_category" %in% names(out))) out[["arxiv_primary_category"]] <- rep(NA_character_, nrow(out))
  if (!("arxiv_categories_raw" %in% names(out))) out[["arxiv_categories_raw"]] <- rep("", nrow(out))
  if (!("arxiv_comment" %in% names(out))) out[["arxiv_comment"]] <- rep(NA_character_, nrow(out))
  if (!("arxiv_journal_ref" %in% names(out))) out[["arxiv_journal_ref"]] <- rep(NA_character_, nrow(out))
  if (!("raw_entry" %in% names(out))) out[["raw_entry"]] <- rep(list(NULL), nrow(out))

  if (!("pub_date" %in% names(out))) {
    out[["pub_date"]] <- as.POSIXct(rep(NA_character_, nrow(out)), tz = "UTC")
  }
  if (!inherits(out[["pub_date"]], "POSIXct")) {
    out[["pub_date"]] <- as.POSIXct(out[["pub_date"]], tz = "UTC")
  }

  if (!("year" %in% names(out))) out[["year"]] <- rep(NA_integer_, nrow(out))
  if (!("month" %in% names(out))) out[["month"]] <- rep(NA_integer_, nrow(out))
  if (!("day" %in% names(out))) out[["day"]] <- rep(NA_integer_, nrow(out))

  need_date_parts <- !is.na(out[["pub_date"]])
  out[["year"]][need_date_parts & is.na(out[["year"]])] <- as.integer(format(out[["pub_date"]][need_date_parts & is.na(out[["year"]])], "%Y"))
  out[["month"]][need_date_parts & is.na(out[["month"]])] <- as.integer(format(out[["pub_date"]][need_date_parts & is.na(out[["month"]])], "%m"))
  out[["day"]][need_date_parts & is.na(out[["day"]])] <- as.integer(format(out[["pub_date"]][need_date_parts & is.na(out[["day"]])], "%d"))

  if (!("ref_id" %in% names(out))) out[["ref_id"]] <- rep(NA_character_, nrow(out))

  for (i in seq_len(nrow(out))) {
    if (is.na(out$source_id[[i]]) || !nzchar(out$source_id[[i]])) {
      out$source_id[[i]] <- .litxr_manual_source_id(out[i, ])
    }
    if (is.na(out$ref_id[[i]]) || !nzchar(out$ref_id[[i]])) {
      out$ref_id[[i]] <- .litxr_manual_ref_id(out[i, ])
    }
  }

  out
}

.litxr_manual_source_id <- function(row) {
  doi <- row[["doi"]]
  isbn <- row[["isbn"]]
  url <- row[["url"]]
  title <- row[["title"]]
  year <- row[["year"]]

  if (!is.na(doi) && nzchar(doi)) return(doi)
  if (!is.na(isbn) && nzchar(isbn)) return(isbn)
  if (!is.na(url) && nzchar(url)) return(url)

  slug <- gsub("[^A-Za-z0-9]+", "", substr(ifelse(is.na(title), "ref", title), 1, 24))
  paste0("manual_", ifelse(is.na(year), "na", year), "_", slug)
}

.litxr_manual_ref_id <- function(row) {
  doi <- row[["doi"]]
  isbn <- row[["isbn"]]
  url <- row[["url"]]
  source <- row[["source"]]
  source_id <- row[["source_id"]]

  if (!is.na(doi) && nzchar(doi)) return(paste0("doi:", doi))
  if (!is.na(isbn) && nzchar(isbn)) return(paste0("isbn:", isbn))
  if (!is.na(url) && nzchar(url)) return(paste0("url:", url))
  paste0(source, ":", source_id)
}

.litxr_register_manual_collection <- function(cfg, collection_id, collection_title = NULL) {
  title <- if (is.null(collection_title) || !nzchar(collection_title)) collection_id else collection_title
  collection <- list(
    collection_id = collection_id,
    collection_type = "manual_batch",
    title = title,
    remote_channel = "manual",
    local_path = file.path(cfg$project$data_root, collection_id),
    metadata = list(),
    sync = list()
  )

  collections <- .litxr_config_collections(cfg)
  collections[[length(collections) + 1L]] <- collection
  cfg$collections <- collections
  cfg <- .litxr_normalize_config_schema(cfg)
  .litxr_write_config(cfg)
  list(cfg = cfg, collection = collection)
}

.litxr_assign_crossref_journals <- function(cfg, records, messages, auto_register = TRUE) {
  out_cfg <- cfg
  out_records <- data.table::copy(records)

  for (i in seq_len(nrow(out_records))) {
    message <- messages[[i]]
    journal <- .litxr_match_crossref_journal(out_cfg, message)

    if (is.null(journal)) {
      if (!isTRUE(auto_register)) {
        stop(
          "Crossref journal is not registered in config.yaml for DOI ",
          out_records$doi[[i]],
          ". Set `auto_register = TRUE` to add it automatically.",
          call. = FALSE
        )
      }
      registered <- .litxr_register_crossref_journal(out_cfg, message)
      out_cfg <- registered$cfg
      journal <- registered$journal
    }

    out_records$collection_id[[i]] <- journal$collection_id
    out_records$collection_title[[i]] <- journal$title
  }

  list(cfg = out_cfg, records = out_records)
}

.litxr_match_crossref_journal <- function(cfg, cr_message) {
  journal_title <- .litxr_crossref_journal_title(cr_message)
  issns <- .litxr_crossref_issns(cr_message)

  for (journal in .litxr_config_collections(cfg)) {
    if (!identical(journal$remote_channel, "crossref")) next
    title_match <- !is.na(journal_title) && identical(journal$title, journal_title)
    issn_match <- length(intersect(.litxr_journal_issns(journal), issns)) > 0
    if (title_match || issn_match) {
      return(journal)
    }
  }

  NULL
}

.litxr_register_crossref_journal <- function(cfg, cr_message) {
  journal_title <- .litxr_crossref_journal_title(cr_message)
  if (is.na(journal_title) || !nzchar(journal_title)) {
    journal_title <- "Crossref Unclassified"
  }

  base_id <- .litxr_make_journal_id(journal_title)
  journal_id <- .litxr_unique_journal_id(cfg, base_id)
  metadata <- .litxr_crossref_journal_metadata(cr_message)
  local_path <- file.path(cfg$project$data_root, journal_id)
  sync_issn <- metadata$issn_print
  if (is.null(sync_issn) || is.na(sync_issn) || !nzchar(sync_issn)) {
    sync_issn <- metadata$issn_electronic
  }

  journal <- list(
    collection_id = journal_id,
    collection_type = "journal",
    title = journal_title,
    remote_channel = "crossref",
    local_path = local_path,
    metadata = metadata,
    sync = list(
      filters = list(
        issn = if (!is.null(sync_issn) && nzchar(sync_issn)) sync_issn else NA_character_
      )
    )
  )

  collections <- .litxr_config_collections(cfg)
  collections[[length(collections) + 1L]] <- journal
  cfg$collections <- collections
  cfg <- .litxr_normalize_config_schema(cfg)
  .litxr_write_config(cfg)
  list(cfg = cfg, journal = journal)
}

.litxr_crossref_journal_title <- function(cr_message) {
  title <- cr_message$`container-title`
  if (is.null(title) || length(title) == 0) return(NA_character_)
  title <- as.character(title[[1]])
  if (!nzchar(title)) NA_character_ else title
}

.litxr_crossref_issns <- function(cr_message) {
  issn <- cr_message$ISSN
  if (is.null(issn) || !length(issn)) return(character())
  unique(stats::na.omit(as.character(unlist(issn, use.names = FALSE))))
}

.litxr_crossref_journal_metadata <- function(cr_message) {
  issn_print <- NA_character_
  issn_electronic <- NA_character_

  if (!is.null(cr_message$`issn-type`) && length(cr_message$`issn-type`)) {
    for (entry in cr_message$`issn-type`) {
      if (is.null(entry$type) || is.null(entry$value)) next
      if (identical(entry$type, "print")) issn_print <- as.character(entry$value)
      if (identical(entry$type, "electronic")) issn_electronic <- as.character(entry$value)
    }
  }

  issn_all <- .litxr_crossref_issns(cr_message)
  if ((is.na(issn_print) || !nzchar(issn_print)) && length(issn_all)) {
    issn_print <- issn_all[[1]]
  }
  if ((is.na(issn_electronic) || !nzchar(issn_electronic)) && length(issn_all) >= 2L) {
    issn_electronic <- issn_all[[2]]
  }

  list(
    publisher = if (is.null(cr_message$publisher)) NA_character_ else as.character(cr_message$publisher[[1]]),
    issn_print = issn_print,
    issn_electronic = issn_electronic
  )
}

.litxr_make_journal_id <- function(x) {
  id <- tolower(as.character(x[[1]]))
  id <- gsub("[^a-z0-9]+", "_", id)
  id <- gsub("^_+|_+$", "", id)
  if (!nzchar(id)) id <- "crossref_journal"
  id
}

.litxr_unique_journal_id <- function(cfg, base_id) {
  existing_ids <- vapply(.litxr_config_collections(cfg), `[[`, character(1), "collection_id")
  if (!(base_id %in% existing_ids)) {
    return(base_id)
  }

  i <- 2L
  repeat {
    candidate <- paste0(base_id, "_", i)
    if (!(candidate %in% existing_ids)) {
      return(candidate)
    }
    i <- i + 1L
  }
}

.litxr_get_journal <- function(cfg, journal_id) {
  collections <- .litxr_config_collections(cfg)
  matches <- Filter(function(x) identical(x$collection_id, journal_id) || identical(x$journal_id, journal_id), collections)
  if (!length(matches)) {
    stop("Collection not found in config: ", journal_id, call. = FALSE)
  }
  matches[[1]]
}

.litxr_sync_crossref_journal <- function(journal) {
  issns <- .litxr_journal_issns(journal)
  if (!length(issns)) {
    stop("crossref journal entries require at least one ISSN in config.yaml.", call. = FALSE)
  }

  limit <- if (is.null(journal$sync$limit)) Inf else as.integer(journal$sync$limit)
  rows <- if (is.null(journal$sync$rows)) 1000L else as.integer(journal$sync$rows)

  items <- unlist(lapply(issns, function(issn) {
    fetch_crossref_journal_works(issn = issn, limit = limit, rows = rows)
  }), recursive = FALSE)

  if (!length(items)) {
    return(data.table::data.table())
  }

  rows <- lapply(items, function(item) {
    row <- parse_crossref_entry_unified(item)
    row[["collection_id"]] <- if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id
    row[["collection_title"]] <- journal$title
    row
  })

  records <- .litxr_deduplicate_records(data.table::rbindlist(rows, fill = TRUE))
  if (is.finite(limit) && nrow(records) > limit) {
    records <- records[seq_len(limit), ]
  }

  records
}

.litxr_sync_arxiv_journal <- function(journal) {
  search_query <- journal$sync$search_query
  if (is.null(search_query) || !nzchar(search_query)) {
    stop("arxiv journal entries require `sync.search_query` in config.yaml.", call. = FALSE)
  }

  limit <- if (is.null(journal$sync$limit)) Inf else as.integer(journal$sync$limit)
  batch_size <- if (is.null(journal$sync$rows)) 100L else as.integer(journal$sync$rows)
  delay_seconds <- if (is.null(journal$sync$delay_seconds)) 3 else as.numeric(journal$sync$delay_seconds)
  start <- if (is.null(journal$sync$start)) 0L else as.integer(journal$sync$start)
  entries <- list()

  repeat {
    request_n <- if (is.finite(limit)) min(batch_size, limit - length(entries)) else batch_size
    if (request_n <= 0L) break

    .litxr_arxiv_delay(delay_seconds)

    feed <- fetch_arxiv_xml(
      search_query = search_query,
      start = start,
      max_results = request_n
    )

    page_entries <- xml2::xml_find_all(feed, ".//*[local-name()='entry']")
    if (!length(page_entries)) break

    entries <- c(entries, as.list(page_entries))
    if (length(page_entries) < request_n) break

    start <- start + length(page_entries)
  }

  if (!length(entries)) {
    return(data.table::data.table())
  }

  rows <- lapply(entries, function(entry) {
    row <- parse_arxiv_entry_unified(entry)
    row[["collection_id"]] <- if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id
    row[["collection_title"]] <- journal$title
    row
  })

  records <- data.table::rbindlist(rows, fill = TRUE)
  if (is.finite(limit) && nrow(records) > limit) {
    records <- records[seq_len(limit), ]
  }
  records
}

.litxr_journal_paths <- function(local_path) {
  list(
    root = local_path,
    index = file.path(local_path, "index"),
    json = file.path(local_path, "ref_json"),
    md = file.path(local_path, "fulltxt_md"),
    llm = file.path(local_path, "llm_json"),
    legacy_json = file.path(local_path, "json"),
    legacy_md = file.path(local_path, "md"),
    legacy_llm = file.path(local_path, "llm"),
    legacy_pdf = file.path(local_path, "pdf")
  )
}

.litxr_existing_collection_dir <- function(primary_path, legacy_path = NULL) {
  if (!is.null(primary_path) && dir.exists(primary_path)) {
    return(primary_path)
  }
  if (!is.null(legacy_path) && dir.exists(legacy_path)) {
    return(legacy_path)
  }
  primary_path
}

.litxr_is_absolute_path <- function(path) {
  grepl("^(/|~(?:/|$)|[A-Za-z]:[/\\\\])", path)
}

.litxr_resolve_from_config_root <- function(cfg, path) {
  if (.litxr_is_absolute_path(path)) {
    return(path.expand(path))
  }

  root <- attr(cfg, "config_root", exact = TRUE)
  if (is.null(root) || !nzchar(root)) {
    root <- "."
  }

  file.path(root, path)
}

.litxr_resolve_local_path <- function(cfg, local_path) {
  if (.litxr_is_absolute_path(local_path)) {
    return(path.expand(local_path))
  }

  file.path(.litxr_project_root(cfg), local_path)
}

.litxr_build_arxiv_search_query <- function(base_query, submitted_from = NULL, submitted_to = NULL) {
  if (is.null(submitted_from) && is.null(submitted_to)) {
    return(base_query)
  }

  from_text <- if (is.null(submitted_from)) "*" else .litxr_format_arxiv_datetime(submitted_from, end = FALSE)
  to_text <- if (is.null(submitted_to)) "*" else .litxr_format_arxiv_datetime(submitted_to, end = TRUE)
  paste0("(", base_query, ") AND submittedDate:[", from_text, " TO ", to_text, "]")
}

.litxr_format_arxiv_datetime <- function(x, end = FALSE) {
  text <- as.character(x)
  if (grepl("^[0-9]{4}$", text)) {
    return(if (end) paste0(text, "12312359") else paste0(text, "01010000"))
  }
  if (grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", text)) {
    compact <- gsub("-", "", text)
    return(if (end) paste0(compact, "2359") else paste0(compact, "0000"))
  }
  if (grepl("^[0-9]{8}$", text)) {
    return(if (end) paste0(text, "2359") else paste0(text, "0000"))
  }
  if (grepl("^[0-9]{12}$", text)) {
    return(text)
  }

  parsed <- as.POSIXct(text, tz = "UTC")
  if (is.na(parsed)) {
    stop("Unable to parse arXiv submittedDate value: ", text, call. = FALSE)
  }
  format(parsed, "%Y%m%d%H%M")
}

.litxr_arxiv_delay <- local({
  last_request_time <- NULL

  function(delay_seconds = 3) {
    if (is.null(delay_seconds) || is.na(delay_seconds) || delay_seconds <= 0) {
      return(invisible(NULL))
    }

    if (!is.null(last_request_time)) {
      elapsed <- as.numeric(difftime(Sys.time(), last_request_time, units = "secs"))
      remaining <- delay_seconds - elapsed
      if (remaining > 0) {
        Sys.sleep(remaining)
      }
    }

    last_request_time <<- Sys.time()
    invisible(NULL)
  }
})

.litxr_ensure_journal_dirs <- function(local_path) {
  paths <- .litxr_journal_paths(local_path)
  for (path in unname(paths[c("root", "index", "json", "md", "llm")])) {
    if (!dir.exists(path)) {
      dir.create(path, recursive = TRUE, showWarnings = FALSE)
    }
  }
  paths
}

.litxr_record_slug <- function(row) {
  doi <- row[["doi"]]
  ref_id <- row[["ref_id"]]
  core <- if (length(doi) && !is.na(doi) && nzchar(doi)) doi else ref_id
  core <- tolower(core)
  core <- gsub("[^a-z0-9]+", "_", core)
  core <- gsub("^_+|_+$", "", core)
  if (!nzchar(core)) core <- "record"
  core
}

.litxr_record_key <- function(records) {
  records <- .litxr_normalize_record_identity(records)
  doi <- records[["doi"]]
  source <- if ("source" %in% names(records)) records[["source"]] else rep(NA_character_, nrow(records))
  ref_id <- records[["ref_id"]]

  has_doi <- !is.na(doi) & nzchar(doi)
  keys <- ref_id
  keys[has_doi & (is.na(source) | source != "arxiv")] <- paste0("doi:", doi[has_doi & (is.na(source) | source != "arxiv")])
  keys
}

.litxr_deduplicate_records <- function(records) {
  if (!nrow(records)) {
    return(records)
  }

  out <- data.table::copy(records)
  out[["litxr_record_key__"]] <- .litxr_record_key(out)
  out <- out[!duplicated(out[["litxr_record_key__"]]), ]
  out[["litxr_record_key__"]] <- NULL
  out
}

.litxr_record_completeness_score <- function(records) {
  if (!nrow(records)) return(integer())

  score_fields <- setdiff(
    names(records),
    c("raw_entry", "authors_list", "journal_config")
  )

  vapply(seq_len(nrow(records)), function(i) {
    row <- records[i, ]
    sum(vapply(score_fields, function(field) {
      value <- row[[field]]
      !.litxr_nullish(value)
    }, logical(1)))
  }, integer(1))
}

.litxr_prefer_complete_records <- function(records, key_fun = .litxr_upsert_key) {
  if (!nrow(records)) {
    return(records)
  }

  out <- .litxr_normalize_record_identity(records)
  out[["litxr_record_key__"]] <- key_fun(out)
  out[["litxr_completeness__"]] <- .litxr_record_completeness_score(out)
  out[["litxr_arxiv_version__"]] <- if ("arxiv_version" %in% names(out)) {
    version <- suppressWarnings(as.integer(out[["arxiv_version"]]))
    version[is.na(version)] <- -1L
    version
  } else {
    rep(-1L, nrow(out))
  }

  ord <- order(
    out[["litxr_record_key__"]],
    -out[["litxr_arxiv_version__"]],
    -out[["litxr_completeness__"]]
  )
  out <- out[ord, ]
  out <- out[!duplicated(out[["litxr_record_key__"]]), ]
  out[["litxr_record_key__"]] <- NULL
  out[["litxr_completeness__"]] <- NULL
  out[["litxr_arxiv_version__"]] <- NULL
  out
}

.litxr_upsert_key <- function(records) {
  records <- .litxr_normalize_record_identity(records)
  doi <- records[["doi"]]
  source <- records[["source"]]
  source_id <- records[["source_id"]]
  ref_id <- records[["ref_id"]]

  keys <- ref_id
  has_source_key <- !is.na(source) & nzchar(source) & !is.na(source_id) & nzchar(source_id)
  keys[has_source_key] <- paste0(source[has_source_key], ":", source_id[has_source_key])

  has_doi <- !is.na(doi) & nzchar(doi)
  keys[has_doi & (is.na(source) | source != "arxiv")] <- paste0("doi:", doi[has_doi & (is.na(source) | source != "arxiv")])
  keys
}

.litxr_normalize_record_identity <- function(records) {
  if (!nrow(records)) {
    return(records)
  }

  out <- data.table::copy(records)
  if (!("doi" %in% names(out))) out[["doi"]] <- rep(NA_character_, nrow(out))

  source <- if ("source" %in% names(out)) out[["source"]] else rep(NA_character_, nrow(out))
  source_id <- if ("source_id" %in% names(out)) out[["source_id"]] else rep(NA_character_, nrow(out))
  ref_id <- if ("ref_id" %in% names(out)) out[["ref_id"]] else rep(NA_character_, nrow(out))
  doi <- out[["doi"]]

  missing_doi <- is.na(doi) | !nzchar(doi)
  crossref_source_id <- missing_doi & !is.na(source) & source == "crossref" & !is.na(source_id) & nzchar(source_id)
  doi[crossref_source_id] <- source_id[crossref_source_id]

  crossref_ref_id <- missing_doi & !is.na(source) & source == "crossref" & !is.na(ref_id) & grepl("^doi:", ref_id)
  doi[crossref_ref_id] <- sub("^doi:", "", ref_id[crossref_ref_id])

  arxiv_rows <- !is.na(source) & source == "arxiv"
  if (any(arxiv_rows)) {
    arxiv_base <- rep(NA_character_, nrow(out))

    if ("arxiv_id_base" %in% names(out)) {
      arxiv_base <- out[["arxiv_id_base"]]
    }

    missing_base <- is.na(arxiv_base) | !nzchar(arxiv_base)
    if (any(missing_base)) {
      from_source_id <- !is.na(source_id) & grepl("^.+v[0-9]+$", source_id)
      arxiv_base[missing_base & from_source_id] <- sub("v[0-9]+$", "", source_id[missing_base & from_source_id])
    }

    missing_base <- is.na(arxiv_base) | !nzchar(arxiv_base)
    if (any(missing_base)) {
      from_ref_id <- !is.na(ref_id) & grepl("^arxiv:.+v[0-9]+$", ref_id)
      arxiv_base[missing_base & from_ref_id] <- sub("^arxiv:", "", sub("v[0-9]+$", "", ref_id[missing_base & from_ref_id]))
    }

    missing_base <- is.na(arxiv_base) | !nzchar(arxiv_base)
    if (any(missing_base)) {
      keep_source_id <- !is.na(source_id) & nzchar(source_id)
      arxiv_base[missing_base & keep_source_id] <- source_id[missing_base & keep_source_id]
    }

    source_id[arxiv_rows & !is.na(arxiv_base) & nzchar(arxiv_base)] <- arxiv_base[arxiv_rows & !is.na(arxiv_base) & nzchar(arxiv_base)]
    ref_id[arxiv_rows & !is.na(arxiv_base) & nzchar(arxiv_base)] <- paste0("arxiv:", arxiv_base[arxiv_rows & !is.na(arxiv_base) & nzchar(arxiv_base)])

    if (!("arxiv_id_base" %in% names(out))) {
      out[["arxiv_id_base"]] <- rep(NA_character_, nrow(out))
    }
    out[["arxiv_id_base"]][arxiv_rows & !is.na(arxiv_base) & nzchar(arxiv_base)] <- arxiv_base[arxiv_rows & !is.na(arxiv_base) & nzchar(arxiv_base)]
  }

  out[["source_id"]] <- source_id
  out[["ref_id"]] <- ref_id
  out[["doi"]] <- doi
  out
}

.litxr_nullish <- function(x) {
  length(x) == 0L || is.null(x) || (length(x) == 1L && is.na(x)) || identical(x, "")
}

.litxr_scalar_equal <- function(a, b) {
  if (.litxr_nullish(a) && .litxr_nullish(b)) return(TRUE)
  if (inherits(a, "POSIXct") || inherits(b, "POSIXct")) {
    return(identical(as.character(a), as.character(b)))
  }
  identical(a, b)
}

.litxr_merge_field <- function(existing, incoming, field, key, conflict_env) {
  local_priority_fields <- c("note")

  existing_value <- existing[[field]]
  incoming_value <- incoming[[field]]

  if (field %in% local_priority_fields) {
    if (!.litxr_nullish(existing_value)) return(existing_value)
    return(incoming_value)
  }

  if (.litxr_nullish(incoming_value)) return(existing_value)

  if (!.litxr_nullish(existing_value) && !.litxr_scalar_equal(existing_value, incoming_value)) {
    conflict_env$rows[[length(conflict_env$rows) + 1L]] <- data.table::as.data.table(list(
      key = key,
      field = field,
      old_value = paste(as.character(existing_value), collapse = "; "),
      new_value = paste(as.character(incoming_value), collapse = "; "),
      recorded_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
    ))
  }

  incoming_value
}

.litxr_merge_record_row <- function(existing_row, incoming_row, key, conflict_env) {
  existing_source <- existing_row[["source"]]
  incoming_source <- incoming_row[["source"]]
  existing_version <- suppressWarnings(as.integer(existing_row[["arxiv_version"]]))
  incoming_version <- suppressWarnings(as.integer(incoming_row[["arxiv_version"]]))
  version_order <- 0L
  if (
    !.litxr_nullish(existing_source) &&
    !.litxr_nullish(incoming_source) &&
    identical(as.character(existing_source), "arxiv") &&
    identical(as.character(incoming_source), "arxiv") &&
    !is.na(existing_version) &&
    !is.na(incoming_version)
  ) {
    if (incoming_version > existing_version) version_order <- 1L
    if (incoming_version < existing_version) version_order <- -1L
  }

  existing_row <- stats::setNames(lapply(names(existing_row), function(name) existing_row[[name]]), names(existing_row))
  incoming_row <- stats::setNames(lapply(names(incoming_row), function(name) incoming_row[[name]]), names(incoming_row))
  fields <- union(names(existing_row), names(incoming_row))
  values <- stats::setNames(vector("list", length(fields)), fields)

  for (field in fields) {
    existing_value <- existing_row[[field]]
    incoming_value <- incoming_row[[field]]

    if (field %in% c("authors_list", "raw_entry")) {
      if (version_order < 0L && !.litxr_nullish(existing_value)) {
        values[[field]] <- existing_value
      } else {
        values[[field]] <- if (!.litxr_nullish(incoming_value)) incoming_value else existing_value
      }
    } else if (version_order < 0L && !(field %in% c("note"))) {
      values[[field]] <- if (!.litxr_nullish(existing_value)) existing_value else incoming_value
    } else {
      values[[field]] <- .litxr_merge_field(existing_row, incoming_row, field, key, conflict_env)
    }
  }

  data.table::as.data.table(values)
}

.litxr_write_upsert_conflicts <- function(conflicts, local_path = NULL, conflict_path = NULL) {
  if (!length(conflicts$rows)) return(invisible(NULL))

  if (is.null(conflict_path)) {
    if (is.null(local_path)) {
      stop("Either `local_path` or `conflict_path` must be supplied.", call. = FALSE)
    }
    conflict_path <- file.path(.litxr_journal_paths(local_path)$json, "_upsert_conflicts.jsonl")
  }
  lines <- vapply(conflicts$rows, function(row) {
    jsonlite::toJSON(as.list(row), auto_unbox = TRUE, null = "null")
  }, character(1))

  write(lines, file = conflict_path, append = TRUE)
  invisible(conflict_path)
}

.litxr_upsert_records <- function(existing, incoming, conflict_path = NULL) {
  if (!nrow(existing)) {
    return(.litxr_prefer_complete_records(incoming))
  }
  if (!nrow(incoming)) {
    return(.litxr_prefer_complete_records(existing))
  }

  existing <- .litxr_prefer_complete_records(existing)
  incoming <- .litxr_prefer_complete_records(incoming)

  existing_keys <- .litxr_upsert_key(existing)
  incoming_keys <- .litxr_upsert_key(incoming)
  all_keys <- union(existing_keys, incoming_keys)
  conflicts <- new.env(parent = emptyenv())
  conflicts$rows <- list()

  merged <- lapply(all_keys, function(key) {
    existing_idx <- match(key, existing_keys)
    incoming_idx <- match(key, incoming_keys)

    if (is.na(existing_idx)) return(incoming[incoming_idx, ])
    if (is.na(incoming_idx)) return(existing[existing_idx, ])

    .litxr_merge_record_row(existing[existing_idx, ], incoming[incoming_idx, ], key, conflicts)
  })

  .litxr_write_upsert_conflicts(conflicts, conflict_path = conflict_path)
  data.table::rbindlist(merged, fill = TRUE)
}

.litxr_upsert_journal_records <- function(existing, incoming, local_path) {
  .litxr_upsert_records(
    existing,
    incoming,
    conflict_path = file.path(.litxr_journal_paths(local_path)$json, "_upsert_conflicts.jsonl")
  )
}

.litxr_write_journal_upserted_records <- function(existing, incoming, local_path, journal, cfg = NULL) {
  records <- .litxr_upsert_journal_records(existing, incoming, local_path = local_path)
  incoming_keys <- .litxr_upsert_key(incoming)
  records_keys <- .litxr_upsert_key(records)
  touched_records <- records[records_keys %in% incoming_keys, ]

  .litxr_write_journal_record_files(touched_records, local_path, journal)
  .litxr_write_journal_index(records, local_path)
  if (!is.null(cfg)) .litxr_update_project_indexes(cfg, journal, records)

  records
}

.litxr_write_journal_records <- function(records, local_path, journal, cfg = NULL) {
  if (!nrow(records)) {
    .litxr_ensure_journal_dirs(local_path)
    .litxr_write_journal_index(records, local_path)
    if (!is.null(cfg)) .litxr_update_project_indexes(cfg, journal, records)
    return(invisible(character()))
  }

  written <- .litxr_write_journal_record_files(records, local_path, journal)

  .litxr_write_journal_index(records, local_path)
  if (!is.null(cfg)) .litxr_update_project_indexes(cfg, journal, records)
  invisible(written)
}

.litxr_write_journal_record_files <- function(records, local_path, journal) {
  paths <- .litxr_ensure_journal_dirs(local_path)
  if (!nrow(records)) {
    return(invisible(character()))
  }

  written <- unlist(lapply(seq_len(nrow(records)), function(i) {
    row <- records[i, ]
    payload <- .litxr_row_to_storage_payload(row, journal)
    json_path <- file.path(paths$json, paste0(.litxr_record_slug(row), ".json"))
    jsonlite::write_json(payload, json_path, auto_unbox = TRUE, pretty = TRUE, null = "null")
    json_path
  }), use.names = FALSE)

  invisible(written)
}

.litxr_read_journal_records <- function(local_path) {
  indexed <- .litxr_read_journal_index(local_path)
  if (!is.null(indexed)) {
    return(indexed)
  }

  records <- .litxr_read_journal_records_from_json(local_path)
  .litxr_write_journal_index(records, local_path)
  records
}

.litxr_read_journal_records_from_json <- function(local_path) {
  paths <- .litxr_journal_paths(local_path)
  json_dir <- .litxr_existing_collection_dir(paths$json, paths$legacy_json)
  if (!dir.exists(json_dir)) {
    return(data.table::data.table())
  }

  files <- sort(list.files(json_dir, pattern = "\\.json$", full.names = TRUE))
  if (!length(files)) {
    return(data.table::data.table())
  }

  .litxr_prefer_complete_records(
    data.table::rbindlist(lapply(files, .litxr_storage_payload_to_row), fill = TRUE)
  )
}

.litxr_index_path <- function(local_path) {
  file.path(.litxr_journal_paths(local_path)$index, "references.fst")
}

.litxr_delta_index_path <- function(local_path) {
  file.path(.litxr_journal_paths(local_path)$index, "references_delta.fst")
}

.litxr_index_encode <- function(records) {
  out <- data.table::copy(records)

  if ("authors_list" %in% names(out)) {
    out[["authors_list_json"]] <- vapply(out[["authors_list"]], function(x) {
      jsonlite::toJSON(unname(x), auto_unbox = TRUE, null = "null")
    }, character(1))
    out[["authors_list"]] <- NULL
  }

  if ("raw_entry" %in% names(out)) {
    out[["raw_entry_json"]] <- rep(NA_character_, nrow(out))
    out[["raw_entry"]] <- NULL
  }

  if ("pub_date" %in% names(out)) {
    out[["pub_date"]] <- ifelse(
      is.na(out[["pub_date"]]),
      NA_character_,
      format(out[["pub_date"]], tz = "UTC", usetz = TRUE)
    )
  }

  out
}

.litxr_index_decode <- function(records) {
  if (!nrow(records)) {
    return(data.table::as.data.table(records))
  }

  out <- data.table::as.data.table(records)

  if ("authors_list_json" %in% names(out)) {
    out[["authors_list"]] <- lapply(out[["authors_list_json"]], function(x) {
      if (is.na(x) || !nzchar(x)) return(character())
      unlist(jsonlite::fromJSON(x, simplifyVector = TRUE), use.names = FALSE)
    })
    out[["authors_list_json"]] <- NULL
  } else if (!("authors_list" %in% names(out))) {
    out[["authors_list"]] <- rep(list(character()), nrow(out))
  }

  if ("raw_entry_json" %in% names(out)) {
    out[["raw_entry"]] <- rep(list(NULL), nrow(out))
    out[["raw_entry_json"]] <- NULL
  } else if (!("raw_entry" %in% names(out))) {
    out[["raw_entry"]] <- rep(list(NULL), nrow(out))
  }

  if ("pub_date" %in% names(out)) {
    out[["pub_date"]] <- as.POSIXct(out[["pub_date"]], tz = "UTC")
  }

  out
}

.litxr_write_journal_index <- function(records, local_path) {
  paths <- .litxr_ensure_journal_dirs(local_path)
  index_path <- .litxr_index_path(local_path)
  encoded <- .litxr_index_encode(records)
  fst::write_fst(as.data.frame(encoded), index_path)
  invisible(index_path)
}

.litxr_read_journal_index <- function(local_path) {
  index_path <- .litxr_index_path(local_path)
  if (!file.exists(index_path)) {
    return(NULL)
  }

  .litxr_index_decode(fst::read_fst(index_path, as.data.table = TRUE))
}

.litxr_read_journal_records_by_keys <- function(local_path, keys) {
  index_path <- .litxr_index_path(local_path)
  if (!file.exists(index_path)) {
    records <- .litxr_read_journal_records(local_path)
    if (!nrow(records)) {
      return(records)
    }
    source_id <- if ("source_id" %in% names(records)) records$source_id else rep(NA_character_, nrow(records))
    return(records[records$ref_id %in% keys | source_id %in% keys, ])
  }

  index_columns <- fst::metadata_fst(index_path)$columnNames
  if (!("ref_id" %in% index_columns) && !("source_id" %in% index_columns)) {
    records <- .litxr_read_journal_index(local_path)
    if (is.null(records) || !nrow(records)) {
      return(data.table::data.table())
    }
    source_id <- if ("source_id" %in% names(records)) records$source_id else rep(NA_character_, nrow(records))
    return(records[records$ref_id %in% keys | source_id %in% keys, ])
  }

  idx <- integer()
  if ("ref_id" %in% index_columns) {
    ref_data <- fst::read_fst(index_path, columns = "ref_id", as.data.table = TRUE)
    idx <- which(ref_data$ref_id %in% keys)
  }
  if (!length(idx) && "source_id" %in% index_columns) {
    source_data <- fst::read_fst(index_path, columns = "source_id", as.data.table = TRUE)
    idx <- which(source_data$source_id %in% keys)
  }
  if (!length(idx)) {
    return(data.table::data.table())
  }

  rows <- lapply(idx, function(i) {
    .litxr_index_decode(fst::read_fst(index_path, from = i, to = i, as.data.table = TRUE))
  })
  out <- data.table::rbindlist(rows, fill = TRUE)
  key <- .litxr_upsert_key(out)
  out[!duplicated(key), ]
}

.litxr_read_collection_delta <- function(local_path) {
  path <- .litxr_delta_index_path(local_path)
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  .litxr_index_decode(fst::read_fst(path, as.data.table = TRUE))
}

.litxr_append_collection_delta <- function(records, local_path) {
  paths <- .litxr_ensure_journal_dirs(local_path)
  if (!nrow(records)) {
    return(invisible(.litxr_delta_index_path(local_path)))
  }

  existing <- .litxr_read_collection_delta(local_path)
  delta <- .litxr_upsert_records(existing, records)
  fst::write_fst(
    as.data.frame(.litxr_index_encode(delta)),
    .litxr_delta_index_path(local_path)
  )
  invisible(.litxr_delta_index_path(local_path))
}

.litxr_clear_collection_delta <- function(local_path) {
  path <- .litxr_delta_index_path(local_path)
  if (file.exists(path)) {
    unlink(path)
  }
  invisible(path)
}

.litxr_embedding_slug <- function(x) {
  x <- tolower(as.character(x))
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  if (!nzchar(x)) x <- "default"
  x
}

.litxr_project_embeddings_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "embeddings")
}

.litxr_ensure_project_embeddings_dir <- function(cfg) {
  dir_path <- .litxr_project_embeddings_dir(cfg)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  dir_path
}

.litxr_embedding_index_paths <- function(cfg, collection_id, field, model) {
  base_dir <- file.path(
    .litxr_project_embeddings_dir(cfg),
    .litxr_embedding_slug(collection_id),
    .litxr_embedding_slug(field),
    .litxr_embedding_slug(model)
  )
  list(
    dir = base_dir,
    metadata = file.path(base_dir, "metadata.fst"),
    matrix = file.path(base_dir, "matrix.rds"),
    matrix_f32 = file.path(base_dir, "matrix.f32"),
    manifest = file.path(base_dir, "manifest.json"),
    shards_dir = file.path(base_dir, "shards"),
    delta_dir = file.path(base_dir, "delta"),
    delta_metadata = file.path(base_dir, "delta_metadata.fst"),
    delta_matrix = file.path(base_dir, "delta_matrix.rds"),
    delta_manifest = file.path(base_dir, "delta_manifest.json")
  )
}

.litxr_empty_embedding_metadata <- function() {
  data.table::data.table(
    ref_id = character(),
    title = character(),
    year = integer()
  )
}

.litxr_as_embedding_matrix <- function(x) {
  if (is.data.frame(x)) {
    x <- as.matrix(x)
  }
  if (is.matrix(x)) {
    storage.mode(x) <- "double"
    return(x)
  }
  if (is.list(x) && !is.null(x$embeddings)) {
    return(.litxr_as_embedding_matrix(x$embeddings))
  }
  if (is.list(x) && !is.null(x$data)) {
    return(.litxr_as_embedding_matrix(x$data))
  }
  if (is.list(x) && length(x) && all(vapply(x, is.numeric, logical(1)))) {
    out <- do.call(rbind, lapply(x, as.numeric))
    storage.mode(out) <- "double"
    return(out)
  }
  if (is.list(x) && length(x) && all(vapply(x, function(row) is.list(row) && !is.null(row$embedding), logical(1)))) {
    out <- do.call(rbind, lapply(x, function(row) as.numeric(row$embedding)))
    storage.mode(out) <- "double"
    return(out)
  }
  if (is.numeric(x)) {
    out <- matrix(as.numeric(x), nrow = 1L)
    storage.mode(out) <- "double"
    return(out)
  }
  stop("Unable to coerce embedding output to a numeric matrix.", call. = FALSE)
}

.litxr_embedding_manifest <- function(collection_id, field, model, provider, dimension, records) {
  list(
    collection_id = as.character(collection_id),
    field = as.character(field),
    model = as.character(model),
    provider = as.character(provider),
    dimension = as.integer(dimension),
    records = as.integer(records),
    updated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )
}

.litxr_embedding_metadata_from_records <- function(records, collection_id, field, model, provider) {
  value_or_na <- function(name, type = "character") {
    if (name %in% names(records)) {
      return(records[[name]])
    }
    switch(
      type,
      integer = rep(NA_integer_, nrow(records)),
      rep(NA_character_, nrow(records))
    )
  }

  data.table::data.table(
    ref_id = value_or_na("ref_id"),
    title = value_or_na("title"),
    year = as.integer(value_or_na("year", "integer"))
  )
}

.litxr_normalize_embedding_metadata <- function(metadata) {
  metadata <- data.table::as.data.table(metadata)
  if (!nrow(metadata)) {
    return(.litxr_empty_embedding_metadata())
  }
  out <- data.table::data.table(
    ref_id = if ("ref_id" %in% names(metadata)) as.character(metadata$ref_id) else rep(NA_character_, nrow(metadata)),
    title = if ("title" %in% names(metadata)) as.character(metadata$title) else rep(NA_character_, nrow(metadata)),
    year = if ("year" %in% names(metadata)) as.integer(metadata$year) else rep(NA_integer_, nrow(metadata))
  )
  out
}

.litxr_prepare_embedding_targets <- function(collection_id, cfg, field, model, overwrite = FALSE, limit = NULL) {
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }

  records <- litxr_read_collection(collection_id, cfg)
  if (!nrow(records)) {
    return(list(
      targets = data.table::data.table(),
      paths = .litxr_embedding_index_paths(cfg, collection_id, field, model),
      existing = list(metadata = .litxr_empty_embedding_metadata(), matrix = NULL, manifest = list()),
      delta = list(metadata = .litxr_empty_embedding_metadata(), matrix = matrix(numeric(), nrow = 0L, ncol = 0L), manifest = list())
    ))
  }
  if (!(field %in% names(records))) {
    stop("Field not found in collection records: ", field, call. = FALSE)
  }

  text <- as.character(records[[field]])
  text[is.na(text)] <- ""
  text <- trimws(text)
  keep <- !is.na(records$ref_id) & nzchar(records$ref_id) & nzchar(text)
  targets <- records[keep, ]
  targets[["litxr_embedding_text__"]] <- text[keep]

  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  existing <- .litxr_read_embedding_index_parts(paths, read_matrix = FALSE)
  if (isTRUE(overwrite)) {
    .litxr_clear_embedding_delta(paths)
    existing <- list(metadata = .litxr_empty_embedding_metadata(), matrix = NULL, manifest = list())
  }
  delta <- .litxr_read_embedding_delta_parts(paths)

  existing_ref_ids <- unique(c(
    if (nrow(existing$metadata)) existing$metadata$ref_id else character(),
    if (nrow(delta$metadata)) delta$metadata$ref_id else character()
  ))
  targets <- if (isTRUE(overwrite)) targets else targets[!(targets$ref_id %in% existing_ref_ids), ]
  if (!is.null(limit)) {
    limit <- as.integer(limit)
    if (is.na(limit) || limit < 0L) {
      stop("`limit` must be a non-negative integer.", call. = FALSE)
    }
    targets <- targets[seq_len(min(nrow(targets), limit)), ]
  }

  list(targets = targets, paths = paths, existing = existing, delta = delta)
}

.litxr_write_embedding_delta_batches <- function(
  targets,
  paths,
  collection_id,
  field,
  embed_fun,
  model,
  provider,
  batch_size,
  existing,
  existing_delta
) {
  if (!is.function(embed_fun)) {
    stop("`embed_fun` must be a function that accepts a character vector.", call. = FALSE)
  }
  batch_size <- as.integer(batch_size)
  if (is.na(batch_size) || batch_size <= 0L) {
    stop("`batch_size` must be a positive integer.", call. = FALSE)
  }
  if (!nrow(targets)) {
    return(existing_delta$metadata)
  }

  for (start in seq(1L, nrow(targets), by = batch_size)) {
    end <- min(start + batch_size - 1L, nrow(targets))
    batch <- targets[start:end, ]
    batch_matrix <- .litxr_as_embedding_matrix(embed_fun(batch$litxr_embedding_text__))
    if (nrow(batch_matrix) != nrow(batch)) {
      stop("`embed_fun` returned ", nrow(batch_matrix), " embeddings for ", nrow(batch), " texts.", call. = FALSE)
    }

    existing_dimension <- .litxr_embedding_existing_dimension(existing, existing_delta)
    if (!is.na(existing_dimension) && existing_dimension != ncol(batch_matrix)) {
      stop("Embedding dimension changed from ", existing_dimension, " to ", ncol(batch_matrix), ".", call. = FALSE)
    }

    batch_metadata <- .litxr_embedding_metadata_from_records(
      batch,
      collection_id = collection_id,
      field = field,
      model = as.character(model),
      provider = as.character(provider)
    )

    .litxr_append_embedding_delta(
      paths = paths,
      metadata = batch_metadata,
      embeddings = batch_matrix,
      manifest = .litxr_embedding_manifest(
        collection_id = collection_id,
        field = field,
        model = as.character(model),
        provider = as.character(provider),
        dimension = ncol(batch_matrix),
        records = nrow(batch_metadata)
      )
    )
    existing_delta <- .litxr_merge_embedding_parts(
      existing_delta,
      list(
        metadata = batch_metadata,
        matrix = batch_matrix,
        manifest = .litxr_embedding_manifest(
          collection_id = collection_id,
          field = field,
          model = as.character(model),
          provider = as.character(provider),
          dimension = ncol(batch_matrix),
          records = nrow(batch_metadata)
        )
      )
    )
  }

  existing_delta$metadata
}

.litxr_read_embedding_index_parts <- function(paths, read_matrix = TRUE) {
  if (.litxr_embedding_has_shards(paths)) {
    return(.litxr_read_embedding_sharded_parts(paths, read_matrix = read_matrix))
  }
  .litxr_read_embedding_parts(
    metadata_path = paths$metadata,
    matrix_path = paths$matrix,
    matrix_f32_path = paths$matrix_f32,
    manifest_path = paths$manifest,
    cache_dir = paths$dir,
    read_matrix = read_matrix
  )
}

.litxr_read_embedding_delta_parts <- function(paths, read_matrix = TRUE) {
  delta <- .litxr_read_embedding_parts(
    metadata_path = paths$delta_metadata,
    matrix_path = paths$delta_matrix,
    matrix_f32_path = NULL,
    manifest_path = paths$delta_manifest,
    cache_dir = paths$dir,
    read_matrix = read_matrix
  )
  .litxr_merge_embedding_parts(delta, .litxr_read_embedding_delta_shards(
    .litxr_embedding_delta_shard_paths(paths),
    read_matrix = read_matrix
  ))
}

.litxr_read_embedding_delta_shards <- function(shard_paths, read_matrix = TRUE) {
  if (!length(shard_paths)) {
    return(list(
      metadata = .litxr_empty_embedding_metadata(),
      matrix = if (isTRUE(read_matrix)) matrix(numeric(), nrow = 0L, ncol = 0L) else NULL,
      manifest = list()
    ))
  }

  delta <- list(
    metadata = .litxr_empty_embedding_metadata(),
    matrix = if (isTRUE(read_matrix)) matrix(numeric(), nrow = 0L, ncol = 0L) else NULL,
    manifest = list()
  )
  for (shard_path in shard_paths) {
    shard <- readRDS(shard_path)
    if (!is.list(shard) || is.null(shard$metadata) || is.null(shard$matrix)) {
      stop("Invalid embedding delta shard: ", shard_path, call. = FALSE)
    }
    shard$metadata <- data.table::as.data.table(shard$metadata)
    shard$matrix <- if (isTRUE(read_matrix)) .litxr_as_embedding_matrix(shard$matrix) else NULL
    shard$manifest <- shard$manifest %||% list()
    delta <- .litxr_merge_embedding_parts(delta, shard)
  }
  delta
}

.litxr_read_embedding_parts <- function(metadata_path, matrix_path, matrix_f32_path = NULL, manifest_path, cache_dir, read_matrix = TRUE) {
  metadata <- if (file.exists(metadata_path)) {
    .litxr_normalize_embedding_metadata(fst::read_fst(metadata_path, as.data.table = TRUE))
  } else {
    .litxr_empty_embedding_metadata()
  }
  manifest <- if (file.exists(manifest_path)) {
    jsonlite::fromJSON(manifest_path, simplifyVector = FALSE)
  } else {
    list()
  }
  embeddings <- if (!isTRUE(read_matrix)) {
    NULL
  } else if (!is.null(matrix_f32_path) && file.exists(matrix_f32_path)) {
    .litxr_read_float32_matrix(
      path = matrix_f32_path,
      nrow = nrow(metadata),
      ncol = as.integer(manifest$dimension %||% 0L),
      context = cache_dir
    )
  } else if (file.exists(matrix_path)) {
    tryCatch(
      readRDS(matrix_path),
      error = function(e) {
        stop(
          "Failed to read embedding matrix cache at ", matrix_path,
          ". The file may be corrupted by an interrupted write. ",
          "For delta-only embedding, update to the latest package and retry. ",
          "To rebuild the main cache, use `overwrite = TRUE` after preserving any needed delta shards. ",
          "Underlying error: ", conditionMessage(e),
          call. = FALSE
        )
      }
    )
  } else {
    matrix(numeric(), nrow = 0L, ncol = 0L)
  }
  if (!is.null(embeddings)) {
    embeddings <- as.matrix(embeddings)
    storage.mode(embeddings) <- "double"
  }
  if (!is.null(embeddings) && nrow(metadata) != nrow(embeddings)) {
    stop("Embedding metadata rows do not match matrix rows in cache: ", cache_dir, call. = FALSE)
  }
  list(metadata = metadata, matrix = embeddings, manifest = manifest)
}

.litxr_is_embedding_matrix_read_error <- function(e) {
  inherits(e, "error") && grepl("Failed to read embedding matrix cache at ", conditionMessage(e), fixed = TRUE)
}

.litxr_save_rds_atomic <- function(object, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  tmp_path <- paste0(path, ".tmp")
  saveRDS(object, tmp_path)
  if (!file.rename(tmp_path, path)) {
    unlink(tmp_path)
    stop("Failed to atomically write RDS file: ", path, call. = FALSE)
  }
  invisible(path)
}

.litxr_write_float32_matrix_atomic <- function(matrix, path) {
  matrix <- as.matrix(matrix)
  storage.mode(matrix) <- "double"
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  tmp_path <- paste0(path, ".tmp")
  con <- file(tmp_path, open = "wb")
  on.exit(try(close(con), silent = TRUE), add = TRUE)
  writeBin(as.numeric(matrix), con, size = 4L, endian = "little")
  close(con)
  if (!file.rename(tmp_path, path)) {
    unlink(tmp_path)
    stop("Failed to atomically write float32 matrix file: ", path, call. = FALSE)
  }
  invisible(path)
}

.litxr_read_float32_matrix <- function(path, nrow, ncol, context) {
  nrow <- as.integer(nrow)
  ncol <- as.integer(ncol)
  if (is.na(nrow) || is.na(ncol) || nrow < 0L || ncol < 0L) {
    stop("Invalid float32 matrix shape for cache: ", context, call. = FALSE)
  }
  if (nrow == 0L || ncol == 0L) {
    return(matrix(numeric(), nrow = nrow, ncol = ncol))
  }
  expected <- as.double(nrow) * as.double(ncol)
  con <- file(path, open = "rb")
  on.exit(close(con), add = TRUE)
  values <- tryCatch(
    readBin(con, what = numeric(), n = expected, size = 4L, endian = "little"),
    error = function(e) {
      stop("Failed to read float32 embedding matrix cache at ", path, ": ", conditionMessage(e), call. = FALSE)
    }
  )
  if (length(values) != expected) {
    stop("Float32 embedding matrix cache has unexpected length at ", path, ".", call. = FALSE)
  }
  matrix(values, nrow = nrow, ncol = ncol, byrow = FALSE)
}

.litxr_write_json_atomic <- function(object, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  tmp_path <- paste0(path, ".tmp")
  jsonlite::write_json(object, tmp_path, auto_unbox = TRUE, pretty = TRUE, null = "null")
  if (!file.rename(tmp_path, path)) {
    unlink(tmp_path)
    stop("Failed to atomically write JSON file: ", path, call. = FALSE)
  }
  invisible(path)
}

.litxr_write_fst_atomic <- function(object, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  tmp_path <- paste0(path, ".tmp")
  fst::write_fst(object, tmp_path)
  if (!file.rename(tmp_path, path)) {
    unlink(tmp_path)
    stop("Failed to atomically write fst file: ", path, call. = FALSE)
  }
  invisible(path)
}

.litxr_write_embedding_index <- function(paths, metadata, embeddings, manifest) {
  if (!dir.exists(paths$dir)) {
    dir.create(paths$dir, recursive = TRUE, showWarnings = FALSE)
  }
  .litxr_write_embedding_sharded_index(paths, metadata, embeddings, manifest)
  invisible(paths)
}

.litxr_write_embedding_delta <- function(paths, metadata, embeddings, manifest) {
  if (!dir.exists(paths$dir)) {
    dir.create(paths$dir, recursive = TRUE, showWarnings = FALSE)
  }
  .litxr_write_fst_atomic(as.data.frame(metadata), paths$delta_metadata)
  .litxr_save_rds_atomic(embeddings, paths$delta_matrix)
  .litxr_write_json_atomic(manifest, paths$delta_manifest)
  invisible(paths)
}

.litxr_append_embedding_delta <- function(paths, metadata, embeddings, manifest) {
  if (!dir.exists(paths$delta_dir)) {
    dir.create(paths$delta_dir, recursive = TRUE, showWarnings = FALSE)
  }
  shard_path <- .litxr_embedding_delta_shard_path(paths)
  tmp_path <- paste0(shard_path, ".tmp")
  saveRDS(
    list(metadata = metadata, matrix = embeddings, manifest = manifest),
    tmp_path
  )
  if (!file.rename(tmp_path, shard_path)) {
    unlink(tmp_path)
    stop("Failed to write embedding delta shard: ", shard_path, call. = FALSE)
  }
  invisible(shard_path)
}

.litxr_embedding_delta_shard_paths <- function(paths) {
  if (is.null(paths$delta_dir) || !dir.exists(paths$delta_dir)) {
    return(character())
  }
  sort(list.files(paths$delta_dir, pattern = "\\.rds$", full.names = TRUE))
}

.litxr_select_embedding_delta_shards <- function(paths, shard = NULL, date = NULL) {
  shard_paths <- .litxr_embedding_delta_shard_paths(paths)
  if (!length(shard_paths)) {
    return(character())
  }

  if (!is.null(shard)) {
    shard <- as.character(shard[[1]])
    shard_full <- if (grepl("^/", shard)) shard else file.path(paths$delta_dir, shard)
    shard_match <- normalizePath(shard_full, winslash = "/", mustWork = FALSE)
    shard_paths_norm <- normalizePath(shard_paths, winslash = "/", mustWork = FALSE)
    keep <- shard_paths_norm %in% shard_match | basename(shard_paths) %in% basename(shard)
    return(shard_paths[keep])
  }

  if (!is.null(date)) {
    date <- as.Date(date[[1]])
    if (is.na(date)) {
      stop("`date` must be coercible by `as.Date()`.", call. = FALSE)
    }
    date_key <- format(date, "%Y%m%d")
    shard_stamp <- sub("^batch_([0-9]{8}).*$", "\\1", basename(shard_paths))
    return(shard_paths[shard_stamp %in% date_key])
  }

  shard_paths
}

.litxr_embedding_delta_shard_path <- function(paths) {
  stamp <- format(Sys.time(), "%Y%m%d%H%M%OS6", tz = "UTC")
  stamp <- gsub("[^0-9]", "", stamp)
  token <- paste(sample(c(letters, LETTERS, 0:9), 10L, replace = TRUE), collapse = "")
  file.path(paths$delta_dir, paste0("batch_", stamp, "_", Sys.getpid(), "_", token, ".rds"))
}

.litxr_embedding_existing_dimension <- function(existing, delta) {
  if (!is.null(delta$matrix) && nrow(delta$matrix)) {
    return(ncol(delta$matrix))
  }
  if (!is.null(existing$matrix) && nrow(existing$matrix)) {
    return(ncol(existing$matrix))
  }
  manifest_dimension <- suppressWarnings(as.integer(existing$manifest$dimension %||% NA_integer_))
  if (!is.na(manifest_dimension) && manifest_dimension > 0L) {
    return(manifest_dimension)
  }
  NA_integer_
}

.litxr_merge_embedding_parts <- function(existing, incoming) {
  if (!nrow(existing$metadata)) {
    return(incoming)
  }
  if (!nrow(incoming$metadata)) {
    return(existing)
  }
  if (is.null(existing$matrix) || is.null(incoming$matrix)) {
    combined_metadata <- data.table::rbindlist(list(existing$metadata, incoming$metadata), fill = TRUE)
    keep <- !duplicated(combined_metadata$ref_id, fromLast = TRUE)
    return(list(
      metadata = combined_metadata[keep, ],
      matrix = NULL,
      manifest = incoming$manifest %||% existing$manifest
    ))
  }
  if (ncol(existing$matrix) != ncol(incoming$matrix)) {
    stop("Embedding dimension changed from ", ncol(existing$matrix), " to ", ncol(incoming$matrix), ".", call. = FALSE)
  }

  combined_metadata <- data.table::rbindlist(list(existing$metadata, incoming$metadata), fill = TRUE)
  combined_matrix <- rbind(existing$matrix, incoming$matrix)
  keep <- !duplicated(combined_metadata$ref_id, fromLast = TRUE)
  list(
    metadata = combined_metadata[keep, ],
    matrix = combined_matrix[keep, , drop = FALSE],
    manifest = incoming$manifest
  )
}

.litxr_compact_embedding_index <- function(paths, collection_id, field, model, provider, overwrite = FALSE) {
  delta <- .litxr_read_embedding_delta_parts(paths)
  existing <- if (isTRUE(overwrite)) {
    list(metadata = .litxr_empty_embedding_metadata(), matrix = matrix(numeric(), nrow = 0L, ncol = 0L), manifest = list())
  } else {
    .litxr_read_embedding_index_parts(paths)
  }
  if (!nrow(delta$metadata)) {
    return(existing$metadata)
  }
  merged <- .litxr_merge_embedding_parts(existing, delta)
  if (!nrow(merged$metadata)) {
    return(merged$metadata)
  }
  manifest <- .litxr_embedding_manifest(
    collection_id = collection_id,
    field = field,
    model = model,
    provider = provider,
    dimension = ncol(merged$matrix),
    records = nrow(merged$metadata)
  )
  .litxr_write_embedding_index(paths, merged$metadata, merged$matrix, manifest)
  .litxr_clear_embedding_delta(paths)
  merged$metadata
}

.litxr_clear_embedding_delta <- function(paths) {
  for (path in c(paths$delta_metadata, paths$delta_matrix, paths$delta_manifest)) {
    if (file.exists(path)) {
      unlink(path)
    }
  }
  if (!is.null(paths$delta_dir) && dir.exists(paths$delta_dir)) {
    unlink(paths$delta_dir, recursive = TRUE)
  }
  invisible(paths)
}

.litxr_remove_embedding_main <- function(paths) {
  removed <- character()
  for (path in c(paths$metadata, paths$matrix, paths$matrix_f32, paths$manifest)) {
    if (file.exists(path)) {
      unlink(path)
      removed <- c(removed, path)
    }
  }
  if (!is.null(paths$shards_dir) && dir.exists(paths$shards_dir)) {
    unlink(paths$shards_dir, recursive = TRUE)
    removed <- c(removed, paths$shards_dir)
  }
  removed
}

.litxr_embedding_has_shards <- function(paths) {
  !is.null(paths$shards_dir) && dir.exists(paths$shards_dir) && length(list.files(paths$shards_dir, full.names = FALSE)) > 0L
}

.litxr_embedding_shard_key <- function(year) {
  year <- suppressWarnings(as.integer(year))
  out <- ifelse(!is.na(year) & year >= 1000L & year <= 9999L, sprintf("%04d", year), "unknown")
  as.character(out)
}

.litxr_embedding_shard_paths <- function(paths, shard_key) {
  shard_dir <- file.path(paths$shards_dir, shard_key)
  list(
    dir = shard_dir,
    metadata = file.path(shard_dir, "metadata.fst"),
    matrix_f32 = file.path(shard_dir, "matrix.f32"),
    manifest = file.path(shard_dir, "manifest.json")
  )
}

.litxr_embedding_shard_keys <- function(paths) {
  if (is.null(paths$shards_dir) || !dir.exists(paths$shards_dir)) {
    return(character())
  }
  entries <- list.files(paths$shards_dir, full.names = FALSE)
  entries[dir.exists(file.path(paths$shards_dir, entries))]
}

.litxr_read_embedding_sharded_parts <- function(paths, read_matrix = TRUE) {
  shard_keys <- .litxr_embedding_shard_keys(paths)
  manifest <- if (file.exists(paths$manifest)) jsonlite::fromJSON(paths$manifest, simplifyVector = FALSE) else list()
  if (!length(shard_keys)) {
    return(list(metadata = .litxr_empty_embedding_metadata(), matrix = if (isTRUE(read_matrix)) matrix(numeric(), nrow = 0L, ncol = 0L) else NULL, manifest = manifest))
  }

  parts <- lapply(shard_keys, function(key) {
    shard_paths <- .litxr_embedding_shard_paths(paths, key)
    shard_manifest <- if (file.exists(shard_paths$manifest)) jsonlite::fromJSON(shard_paths$manifest, simplifyVector = FALSE) else list()
    metadata <- if (file.exists(shard_paths$metadata)) {
      .litxr_normalize_embedding_metadata(fst::read_fst(shard_paths$metadata, as.data.table = TRUE))
    } else {
      .litxr_empty_embedding_metadata()
    }
    matrix_data <- if (!isTRUE(read_matrix)) {
      NULL
    } else if (file.exists(shard_paths$matrix_f32)) {
      .litxr_read_float32_matrix(
        path = shard_paths$matrix_f32,
        nrow = nrow(metadata),
        ncol = as.integer(shard_manifest$dimension %||% manifest$dimension %||% 0L),
        context = shard_paths$dir
      )
    } else {
      matrix(numeric(), nrow = 0L, ncol = as.integer(shard_manifest$dimension %||% manifest$dimension %||% 0L))
    }
    list(metadata = metadata, matrix = matrix_data, manifest = shard_manifest)
  })

  metadata <- data.table::rbindlist(lapply(parts, `[[`, "metadata"), fill = TRUE)
  matrix_data <- if (isTRUE(read_matrix)) {
    mats <- lapply(parts, `[[`, "matrix")
    mats <- mats[vapply(mats, function(x) !is.null(x), logical(1))]
    if (!length(mats)) matrix(numeric(), nrow = 0L, ncol = as.integer(manifest$dimension %||% 0L)) else do.call(rbind, mats)
  } else {
    NULL
  }
  list(metadata = metadata, matrix = matrix_data, manifest = manifest)
}

.litxr_write_embedding_sharded_index <- function(paths, metadata, embeddings, manifest) {
  dir.create(paths$dir, recursive = TRUE, showWarnings = FALSE)
  if (dir.exists(paths$shards_dir)) unlink(paths$shards_dir, recursive = TRUE)
  dir.create(paths$shards_dir, recursive = TRUE, showWarnings = FALSE)

  shard_key <- .litxr_embedding_shard_key(metadata$year)
  split_idx <- split(seq_len(nrow(metadata)), shard_key)
  shard_manifest <- list()
  for (key in names(split_idx)) {
    idx <- split_idx[[key]]
    shard_paths <- .litxr_embedding_shard_paths(paths, key)
    dir.create(shard_paths$dir, recursive = TRUE, showWarnings = FALSE)
    shard_meta <- metadata[idx, , drop = FALSE]
    shard_matrix <- embeddings[idx, , drop = FALSE]
    local_manifest <- list(
      shard_key = key,
      year = if (identical(key, "unknown")) NA_integer_ else as.integer(key),
      dimension = as.integer(ncol(shard_matrix)),
      records = as.integer(nrow(shard_meta)),
      matrix_format = "float32",
      updated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
    )
    .litxr_write_fst_atomic(as.data.frame(shard_meta), shard_paths$metadata)
    .litxr_write_float32_matrix_atomic(shard_matrix, shard_paths$matrix_f32)
    .litxr_write_json_atomic(local_manifest, shard_paths$manifest)
    shard_manifest[[key]] <- local_manifest
  }

  manifest$storage_version <- 2L
  manifest$matrix_format <- "float32"
  manifest$shard_by <- "year"
  manifest$shards <- shard_manifest
  .litxr_write_json_atomic(manifest, paths$manifest)
  if (file.exists(paths$metadata)) unlink(paths$metadata)
  if (file.exists(paths$matrix)) unlink(paths$matrix)
  if (file.exists(paths$matrix_f32)) unlink(paths$matrix_f32)
  invisible(paths)
}

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

.litxr_project_llm_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "llm")
}

.litxr_project_llm_history_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "llm_history")
}

.litxr_project_md_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "md")
}

.litxr_project_references_index_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "references.fst")
}

.litxr_project_reference_collections_index_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "reference_collections.fst")
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
  revision <- suppressWarnings(as.integer(digest$digest_revision[[1]] %||% digest$digest_revision))
  if (is.na(revision) || revision < 1L) revision <- 1L
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
  index_path <- .litxr_index_path(local_path)

  record_count <- 0L
  timestamp <- NA_character_
  notes <- character()

  if (file.exists(index_path)) {
    info <- file.info(index_path)
    timestamp <- format(as.POSIXct(info$mtime, tz = "UTC"), tz = "UTC", usetz = TRUE)
    index_dt <- tryCatch(.litxr_read_journal_index(local_path), error = function(e) NULL)
    record_count <- if (is.null(index_dt)) 0L else nrow(index_dt)
    notes <- c(notes, "inferred_from=collection_index")
  } else if (dir.exists(paths$json) || dir.exists(paths$legacy_json)) {
    json_dir <- .litxr_existing_collection_dir(paths$json, paths$legacy_json)
    json_files <- sort(list.files(json_dir, pattern = "\\.json$", full.names = TRUE))
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

.litxr_build_enrichment_status_index <- function(cfg) {
  refs <- .litxr_read_project_references_index(cfg)
  if (!nrow(refs)) {
    return(data.table::data.table(
      ref_id = character(),
      has_md = logical(),
      has_llm_digest = logical(),
      updated_at = character()
    ))
  }

  md_dir <- .litxr_project_md_dir(cfg)
  llm_dir <- .litxr_project_llm_dir(cfg)

  status <- data.table::data.table(
    ref_id = refs$ref_id,
    has_md = vapply(refs$ref_id, function(x) file.exists(.litxr_md_path(cfg, x)), logical(1)),
    has_llm_digest = vapply(refs$ref_id, function(x) file.exists(.litxr_llm_digest_path(cfg, x)), logical(1)),
    updated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )

  status
}

.litxr_write_enrichment_status_index <- function(cfg) {
  .litxr_ensure_project_index_dir(cfg)
  status <- .litxr_build_enrichment_status_index(cfg)
  fst::write_fst(as.data.frame(status), .litxr_enrichment_status_index_path(cfg))
  invisible(.litxr_enrichment_status_index_path(cfg))
}

.litxr_update_enrichment_status_ref <- function(cfg, ref_id) {
  path <- .litxr_enrichment_status_index_path(cfg)
  if (!file.exists(path)) {
    return(.litxr_write_enrichment_status_index(cfg))
  }

  status <- fst::read_fst(path, as.data.table = TRUE)
  if (!nrow(status)) {
    return(.litxr_write_enrichment_status_index(cfg))
  }

  row <- data.table::data.table(
    ref_id = as.character(ref_id),
    has_md = file.exists(.litxr_md_path(cfg, ref_id)),
    has_llm_digest = file.exists(.litxr_llm_digest_path(cfg, ref_id)),
    updated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )

  hit <- match(row$ref_id[[1]], status$ref_id)
  if (is.na(hit)) {
    status <- data.table::rbindlist(list(status, row), fill = TRUE)
  } else {
    data.table::set(status, i = hit, j = "has_md", value = row$has_md[[1]])
    data.table::set(status, i = hit, j = "has_llm_digest", value = row$has_llm_digest[[1]])
    data.table::set(status, i = hit, j = "updated_at", value = row$updated_at[[1]])
  }

  fst::write_fst(as.data.frame(status), path)
  invisible(path)
}

.litxr_read_enrichment_status_index <- function(cfg) {
  path <- .litxr_enrichment_status_index_path(cfg)
  if (!file.exists(path)) {
    .litxr_write_enrichment_status_index(cfg)
  }
  fst::read_fst(path, as.data.table = TRUE)
}

.litxr_project_reference_columns <- function(records) {
  setdiff(names(records), c("collection_id", "collection_title"))
}

.litxr_project_references_from_collection_records <- function(records) {
  if (!nrow(records)) {
    return(data.table::data.table())
  }
  cols <- .litxr_project_reference_columns(records)
  data.table::copy(records)[, cols, with = FALSE]
}

.litxr_project_reference_links_from_collection_records <- function(records, journal) {
  if (!nrow(records)) {
    return(data.table::data.table(
      ref_id = character(),
      collection_id = character(),
      collection_title = character(),
      recorded_at = character()
    ))
  }

  collection_id <- if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id
  links <- data.table::data.table(
    ref_id = records$ref_id,
    collection_id = collection_id,
    collection_title = journal$title,
    recorded_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )
  links[!duplicated(links$ref_id), ]
}

.litxr_read_project_references_index <- function(cfg) {
  path <- .litxr_project_references_index_path(cfg)
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  .litxr_index_decode(fst::read_fst(path, as.data.table = TRUE))
}

.litxr_read_project_references_by_keys <- function(cfg, keys) {
  path <- .litxr_project_references_index_path(cfg)
  if (!file.exists(path)) {
    return(data.table::data.table())
  }

  index_columns <- fst::metadata_fst(path)$columnNames
  if (!("ref_id" %in% index_columns) && !("source_id" %in% index_columns)) {
    refs <- .litxr_read_project_references_index(cfg)
    source_id <- if ("source_id" %in% names(refs)) refs$source_id else rep(NA_character_, nrow(refs))
    return(refs[refs$ref_id %in% keys | source_id %in% keys, ])
  }

  idx <- integer()
  if ("ref_id" %in% index_columns) {
    ref_data <- fst::read_fst(path, columns = "ref_id", as.data.table = TRUE)
    idx <- which(ref_data$ref_id %in% keys)
  }
  if (!length(idx) && "source_id" %in% index_columns) {
    source_data <- fst::read_fst(path, columns = "source_id", as.data.table = TRUE)
    idx <- which(source_data$source_id %in% keys)
  }
  if (!length(idx)) {
    return(data.table::data.table())
  }

  rows <- lapply(idx, function(i) {
    .litxr_index_decode(fst::read_fst(path, from = i, to = i, as.data.table = TRUE))
  })
  out <- data.table::rbindlist(rows, fill = TRUE)
  key <- .litxr_upsert_key(out)
  out[!duplicated(key), ]
}

.litxr_write_project_references_index <- function(cfg, records) {
  .litxr_ensure_project_index_dir(cfg)
  fst::write_fst(as.data.frame(.litxr_index_encode(records)), .litxr_project_references_index_path(cfg))
  invisible(.litxr_project_references_index_path(cfg))
}

.litxr_read_project_reference_collections_index <- function(cfg) {
  path <- .litxr_project_reference_collections_index_path(cfg)
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  fst::read_fst(path, as.data.table = TRUE)
}

.litxr_write_project_reference_collections_index <- function(cfg, links) {
  .litxr_ensure_project_index_dir(cfg)
  fst::write_fst(as.data.frame(links), .litxr_project_reference_collections_index_path(cfg))
  invisible(.litxr_project_reference_collections_index_path(cfg))
}

.litxr_update_project_indexes <- function(cfg, journal, records) {
  .litxr_ensure_project_index_dir(cfg)

  incoming_refs <- .litxr_project_references_from_collection_records(records)
  existing_refs <- .litxr_read_project_references_index(cfg)
  merged_refs <- .litxr_upsert_records(
    existing_refs,
    incoming_refs,
    conflict_path = file.path(.litxr_project_index_dir(cfg), "_reference_conflicts.jsonl")
  )
  .litxr_write_project_references_index(cfg, merged_refs)

  existing_links <- .litxr_read_project_reference_collections_index(cfg)
  incoming_links <- .litxr_project_reference_links_from_collection_records(records, journal)
  if (!nrow(incoming_links)) {
    merged_links <- existing_links
  } else if (nrow(existing_links)) {
    keep_old <- !(existing_links$collection_id == incoming_links$collection_id[[1]] &
      existing_links$ref_id %in% incoming_links$ref_id)
    merged_links <- data.table::rbindlist(list(existing_links[keep_old, ], incoming_links), fill = TRUE)
  } else {
    merged_links <- incoming_links
  }
  link_key <- paste(merged_links$ref_id, merged_links$collection_id, sep = "\r")
  merged_links <- merged_links[!duplicated(link_key), ]
  .litxr_write_project_reference_collections_index(cfg, merged_links)
  invisible(NULL)
}

.litxr_row_to_storage_payload <- function(row, journal) {
  values <- stats::setNames(lapply(names(row), function(name) row[[name]]), names(row))
  values$authors_list <- unname(values$authors_list[[1]])
  values$pub_date <- if (!length(values$pub_date) || is.na(values$pub_date)) {
    NA_character_
  } else {
    format(values$pub_date, tz = "UTC", usetz = TRUE)
  }
  values$raw_entry <- .litxr_serialize_raw_entry(values$raw_entry)
  values$journal_config <- list(
    collection_id = if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id,
    collection_type = journal$collection_type,
    title = journal$title,
    remote_channel = journal$remote_channel
  )
  values
}

.litxr_serialize_raw_entry <- function(raw_entry) {
  if (is.null(raw_entry)) {
    return(NULL)
  }

  if (is.list(raw_entry) && length(raw_entry) == 1L) {
    raw_entry <- raw_entry[[1]]
  }

  if (inherits(raw_entry, c("xml_node", "xml_document"))) {
    return(as.character(raw_entry))
  }

  raw_entry
}

.litxr_storage_payload_to_row <- function(path) {
  payload <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  values <- payload[setdiff(names(payload), "journal_config")]

  if (!is.null(values$pub_date) && !is.na(values$pub_date) && nzchar(values$pub_date)) {
    values$pub_date <- as.POSIXct(values$pub_date, tz = "UTC")
  } else {
    values$pub_date <- as.POSIXct(NA)
  }

  if (is.null(values$authors_list)) {
    values$authors_list <- list(character())
  } else {
    values$authors_list <- list(unlist(values$authors_list, use.names = FALSE))
  }

  if (is.null(values$raw_entry)) {
    values$raw_entry <- list(NULL)
  } else {
    values$raw_entry <- list(values$raw_entry)
  }

  data.table::as.data.table(values)
}
