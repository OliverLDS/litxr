#' Sync one configured collection
#'
#' Fetches records from the configured remote channel, parses them into the
#' unified reference schema, and stores one JSON metadata file per record under
#' the collection's local path.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#'
#' @return `data.table` of synced records.
#' @export
litxr_sync_collection <- function(
  collection_id,
  config = NULL,
  refresh_entity_indexes = TRUE,
  refresh_ref_identity_map = TRUE
) {
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

  existing <- .litxr_read_journal_records_authoritative(local_path)
  records <- .litxr_write_journal_upserted_records(
    existing,
    incoming,
    local_path,
    journal,
    cfg = cfg,
    refresh_entity_indexes = refresh_entity_indexes,
    refresh_ref_identity_map = refresh_ref_identity_map
  )
  records
}

#' Sync one configured journal
#'
#' Backward-compatible wrapper around `litxr_sync_collection()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#'
#' @return `data.table` of synced records.
#' @export
litxr_sync_journal <- function(journal_id, config = NULL) {
  litxr_sync_collection(journal_id, config = config)
}

#' Sync all configured collections
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#'
#' @return Named list of synced `data.table`s.
#' @export
litxr_sync_all <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  collections <- .litxr_config_collections(cfg)
  results <- stats::setNames(
    lapply(collections, function(journal) {
      litxr_sync_collection(
        journal$collection_id,
        cfg,
        refresh_entity_indexes = FALSE,
        refresh_ref_identity_map = FALSE
      )
    }),
    vapply(collections, `[[`, character(1), "collection_id")
  )
  if (length(results)) {
    project_refs <- data.table::rbindlist(results, fill = TRUE)
    project_refs <- .litxr_project_references_from_collection_records(project_refs)
    .litxr_refresh_ref_identity_map(cfg, refs = project_refs)
  } else {
    .litxr_refresh_ref_identity_map(cfg)
  }
  results
}

#' Read locally stored records for one configured collection
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#'
#' @return `data.table` of local records.
#' @export
litxr_read_collection <- function(collection_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  .litxr_read_journal_records_authoritative(local_path)
}

#' Summarize publication-date coverage for one collection
#'
#' Returns counts by day, month, or year based on `pub_date`, with summary
#' attributes describing the observed date range and any missing calendar days
#' within that observed range.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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

#' Read locally stored records for one journal
#'
#' Backward-compatible wrapper around `litxr_read_collection()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
  limit = NULL,
  refresh_entity_indexes = TRUE
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

  existing <- .litxr_read_journal_records_authoritative(local_path)
  records <- .litxr_write_journal_upserted_records(
    existing,
    incoming,
    local_path,
    journal,
    cfg = cfg,
    refresh_entity_indexes = refresh_entity_indexes
  )
  records
}

#' Repair or incrementally fill a journal's local store
#'
#' Backward-compatible wrapper around `litxr_repair_collection()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
  limit = NULL,
  refresh_entity_indexes = TRUE
) {
  litxr_repair_collection(
    journal_id,
    config = config,
    search_query = search_query,
    submitted_from = submitted_from,
    submitted_to = submitted_to,
    start = start,
    limit = limit,
    refresh_entity_indexes = refresh_entity_indexes
  )
}

#' Rebuild the local collection fst index from JSON files
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#'
#' @return Invisibly returns the rebuilt fst index path.
#' @export
litxr_rebuild_collection_index <- function(collection_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  records <- .litxr_read_journal_records_from_json(local_path)
  identities <- .litxr_build_ref_identity_index(cfg, refs = records)
  .litxr_refresh_ref_identity_map(cfg, identities = identities)
  .litxr_refresh_normalized_reference_scaffold(cfg, records = records, refresh_entity_indexes = FALSE)
  invisible(local_path)
}

#' Refresh the collection-derived reference stores from recently changed JSON files
#'
#' This is a fast, mtime-based refresh for the common case where JSON record
#' files were written after the thin authoritative stores were last updated.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#'
#' @return Invisibly returns the refreshed fst index path.
#' @export
litxr_refresh_collection_index <- function(collection_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  records <- .litxr_read_journal_records_from_json(local_path)
  .litxr_update_project_indexes(cfg, journal, records, refresh_entity_indexes = TRUE)
  invisible(local_path)
}

#' Compact pending collection delta records into the main fst index
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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

  records <- .litxr_read_journal_records_authoritative(local_path)
  .litxr_update_project_indexes(cfg, journal, records, refresh_entity_indexes = TRUE)
  invisible(local_path)
}

#' Rebuild the local journal fst index from JSON files
#'
#' Backward-compatible wrapper around `litxr_rebuild_collection_index()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#'
#' @return Invisibly returns the rebuilt fst index path.
#' @export
litxr_rebuild_journal_index <- function(journal_id, config = NULL) {
  litxr_rebuild_collection_index(journal_id, config = config)
}

#' Create a default structured LLM digest template
#'
#' @param ref_id Reference identifier.
#' @param schema_version Digest schema version. Supported values are `"v2"`,
#'   `"v3"`, and `"v4"`.
#'
#' @return Named list representing the default digest schema.
#' @export
litxr_llm_digest_template <- function(ref_id, schema_version = "v2") {
  if (!identical(schema_version, "v2") && !identical(schema_version, "v3") && !identical(schema_version, "v4")) {
    stop("Only `schema_version = \"v2\"`, `\"v3\"`, or `\"v4\"` is supported for new templates in the current package version.", call. = FALSE)
  }
  if (identical(schema_version, "v4")) {
    return(.litxr_llm_digest_template_v4(ref_id))
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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

  refs <- .litxr_read_normalized_reference_rows_by_keys(cfg, .litxr_expand_reference_keys(ref_id))
  if (nrow(refs) && !(ref_id %in% refs$ref_id)) {
    warning("Reference id not found in canonical store: ", ref_id, call. = FALSE)
  }

  existing <- litxr_read_llm_digest(ref_id, cfg)
  payload <- .litxr_normalize_llm_digest_for_write(digest, ref_id = ref_id)
  if (identical(.litxr_llm_digest_schema_version(payload), "v2") || identical(.litxr_llm_digest_schema_version(payload), "v3") || identical(.litxr_llm_digest_schema_version(payload), "v4")) {
    if (is.null(existing)) {
      existing_revision <- 0L
    } else if (!identical(.litxr_llm_digest_schema_version(existing), .litxr_llm_digest_schema_version(payload))) {
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
  if (nrow(ref_match)) {
    ref_match <- ref_match[1L, ]
  }
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param ref_ids Optional character vector of reference ids to keep.
#'
#' @return `data.table` of digest summaries with selected structured fields.
#' @export
litxr_read_llm_digests <- function(config = NULL, ref_ids = NULL, columns = NULL) {
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
      ranked_contributions = list(x$ranked_contributions %||% list()),
      likely_reader_misconceptions = list(unlist(x$likely_reader_misconceptions %||% character(), use.names = FALSE)),
      business_relevance_pathway = list(unlist(x$business_relevance_pathway %||% character(), use.names = FALSE)),
      tables = list(x$tables %||% list()),
      research_target_github_links = list(x$research_target_github_links %||% list()),
      evidence_strength = x$evidence_strength %||% NA_character_,
      evidence_shape = list(x$evidence_shape %||% NULL),
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
  if (!is.null(columns) && length(columns)) {
    keep <- intersect(unique(as.character(columns)), names(out))
    out <- out[, keep, with = FALSE]
  }
  out
}

#' Find LLM digests by text and optional collection membership
#'
#' @param query Optional substring query matched across digest text fields.
#' @param collection_id Optional collection membership filter via `ref_id`.
#' @param ref_id Optional direct reference id filter.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
          flatten_cell(row$ranked_contributions[[1]]),
          flatten_cell(row$likely_reader_misconceptions[[1]]),
          flatten_cell(row$business_relevance_pathway[[1]]),
          flatten_cell(row$tables[[1]]),
          flatten_cell(row$research_target_github_links[[1]]),
          flatten_cell(row$evidence_strength[[1]]),
          flatten_cell(row$evidence_shape[[1]]),
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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

  if (identical(schema_version, "v2") || identical(schema_version, "v3") || identical(schema_version, "v4")) {
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
      required_fields = c("data_sources", "sample_period", "sample_region", "unit_of_observation"),
      field_name = "research_data",
      character_vector_fields = "data_sources"
    )
    sample_size <- digest$research_data$sample_size %||% NULL
    if (is.list(sample_size) && !length(sample_size)) {
      sample_size <- NULL
    }
    if (is.list(sample_size)) {
      sample_size <- unlist(sample_size, use.names = FALSE)
    }
    if (!(is.null(sample_size) || is.numeric(sample_size) || is.integer(sample_size) || is.character(sample_size))) {
      stop("LLM digest field `research_data$sample_size` must be numeric, character, or NA.", call. = FALSE)
    }
    if ("sample_size_note" %in% names(digest$research_data)) {
      sample_size_note <- digest$research_data$sample_size_note
      if (is.list(sample_size_note)) {
        sample_size_note <- unlist(sample_size_note, use.names = FALSE)
      }
      if (!(is.character(sample_size_note) || is.null(sample_size_note))) {
        stop("LLM digest field `research_data$sample_size_note` must be a character vector.", call. = FALSE)
      }
    }
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
    if (identical(schema_version, "v3") || identical(schema_version, "v4")) {
      .litxr_warn_v3_missing_empirical_fields(digest)
    }
    if (identical(schema_version, "v4")) {
      .litxr_validate_list_fields(
        digest,
        c("likely_reader_misconceptions", "business_relevance_pathway")
      )
      .litxr_validate_ranked_contributions(digest$ranked_contributions)
      .litxr_validate_digest_tables(digest$tables)
      .litxr_validate_research_target_github_links(digest$research_target_github_links)
      .litxr_validate_evidence_shape(digest$evidence_shape)
    }
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#'
#' @return `data.table` with one row per reference and enrichment flags.
#' @export
litxr_read_enrichment_status <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_read_enrichment_status_index(cfg)
}

#' List enrichment candidates and exclusion reasons
#'
#' Combines the canonical reference store, collection memberships, and
#' enrichment status into a simple table that shows which references are ready
#' for digest building and why others are excluded.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
  entity_status <- litxr_read_entity_status(cfg)
  ref_identity_map <- data.table::as.data.table(litxr_read_ref_identity_map(cfg))
  collection_links <- data.table::as.data.table(litxr_read_reference_collections(cfg))

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

  ref_view <- data.table::data.table(
    ref_id = as.character(refs$ref_id),
    title = as.character(refs$title),
    entry_type = as.character(refs$entry_type)
  )
  ref_identity_map <- ref_identity_map[!is.na(ref_identity_map$ref_id) & nzchar(ref_identity_map$ref_id), ]
  idx <- match(ref_view$ref_id, ref_identity_map$ref_id)
  keep <- !is.na(idx)
  out <- ref_view[keep, ]
  if (!nrow(out)) {
    out <- ref_view[0, ]
  } else {
    out$entity_id <- ref_identity_map$entity_id[idx[keep]]
  }
  if (nrow(entity_status)) {
    status_view <- data.table::data.table(
      entity_id = as.character(entity_status$entity_id),
      has_md = as.logical(entity_status$has_fulltxt_md),
      has_llm_digest = as.logical(entity_status$has_llm_digest)
    )
    idx <- match(out$entity_id, status_view$entity_id)
    out$has_md <- status_view$has_md[idx]
    out$has_llm_digest <- status_view$has_llm_digest[idx]
  } else {
    out$has_md <- FALSE
    out$has_llm_digest <- FALSE
  }
  collection_map <- if (nrow(collection_links) && nrow(ref_identity_map)) {
    link_dt <- data.table::copy(collection_links[, c("ref_id", "collection_id"), with = FALSE])
    link_dt <- link_dt[link_dt$ref_id %in% out$ref_id, ]
    if (nrow(link_dt)) {
      link_dt$entity_id <- ref_identity_map$entity_id[match(link_dt$ref_id, ref_identity_map$ref_id)]
      link_dt <- link_dt[!is.na(link_dt$entity_id) & nzchar(link_dt$entity_id), ]
      if (nrow(link_dt)) {
        collection_map_df <- stats::aggregate(
          collection_id ~ entity_id,
          data = as.data.frame(link_dt),
          FUN = function(x) paste(sort(unique(as.character(x))), collapse = ",")
        )
        collection_map_dt <- data.table::as.data.table(collection_map_df)
        if ("collection_id" %in% names(collection_map_dt)) {
          data.table::setnames(collection_map_dt, "collection_id", "collection_ids")
        }
        collection_map_dt
      } else {
        data.table::data.table(entity_id = character(), collection_ids = character())
      }
    } else {
      data.table::data.table(entity_id = character(), collection_ids = character())
    }
  } else {
    data.table::data.table(entity_id = character(), collection_ids = character())
  }
  if (nrow(collection_map)) {
    idx <- match(out$entity_id, collection_map$entity_id)
    out$collection_ids <- collection_map$collection_ids[idx]
  } else {
    out$collection_ids <- ""
  }
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

  if ("entity_id" %in% names(out)) {
    out$entity_id <- NULL
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
    .litxr_write_journal_upserted_records(
      NULL,
      incoming,
      local_path,
      journal,
      cfg = cfg,
      refresh_entity_indexes = FALSE
    )
    incoming
  })

  .litxr_refresh_ref_identity_map(cfg)
  data.table::rbindlist(out, fill = TRUE)
}

.litxr_normalize_arxiv_ref_id <- function(ref_id) {
  ref_id <- as.character(ref_id)
  if (!length(ref_id) || is.na(ref_id[[1]])) {
    return(NA_character_)
  }
  ref_id <- trimws(ref_id[[1]])
  if (!nzchar(ref_id)) {
    return(NA_character_)
  }
  if (grepl("^arxiv:", ref_id, ignore.case = TRUE)) {
    base <- sub("^arxiv:", "", ref_id, ignore.case = TRUE)
  } else {
    if (!grepl("^(?:[0-9]{4}\\.[0-9]{4,5})(v[0-9]+)?$", ref_id)) {
      return(NA_character_)
    }
    base <- ref_id
  }
  base <- sub("v[0-9]+$", "", base)
  paste0("arxiv:", base)
}

.litxr_normalize_doi_ref_id <- function(doi) {
  doi <- as.character(doi)
  if (!length(doi) || is.na(doi[[1]])) {
    return(NA_character_)
  }
  doi <- trimws(doi[[1]])
  if (!nzchar(doi)) {
    return(NA_character_)
  }
  if (grepl("^https?://(dx\\.)?doi\\.org/", doi, ignore.case = TRUE)) {
    doi <- sub("^https?://(dx\\.)?doi\\.org/", "", doi, ignore.case = TRUE)
  }
  doi <- sub("^doi:", "", doi, ignore.case = TRUE)
  if (!nzchar(doi)) {
    return(NA_character_)
  }
  paste0("doi:", doi)
}

.litxr_scalar_chr <- function(x) {
  if (is.null(x) || !length(x)) {
    return(NA_character_)
  }
  if (is.list(x)) {
    if (!length(x)) {
      return(NA_character_)
    }
    x <- x[[1]]
  } else {
    x <- x[[1]]
  }
  if (is.null(x) || !length(x) || is.na(x)) {
    return(NA_character_)
  }
  x <- as.character(x)
  if (!length(x) || is.na(x[[1]]) || !nzchar(x[[1]])) {
    return(NA_character_)
  }
  x[[1]]
}

.litxr_find_unique_collection_row <- function(cfg, ref_id, expected_source = NULL) {
  rows <- .litxr_find_collection_refs_by_keys(cfg, ref_id)
  if (!nrow(rows)) {
    stop("Reference not found in local collection records: ", ref_id, call. = FALSE)
  }
  if (!is.null(expected_source)) {
    rows <- rows[rows$source == expected_source, ]
  }
  if (!nrow(rows)) {
    stop("Reference found, but not as source `", expected_source, "`: ", ref_id, call. = FALSE)
  }
  if (nrow(rows) != 1L) {
    stop("Expected exactly one local collection row for ", ref_id, " but found ", nrow(rows), ".", call. = FALSE)
  }
  rows
}

.litxr_update_collection_row <- function(cfg, row, refresh_entity_indexes = TRUE) {
  collection_id <- .litxr_scalar_chr(row$collection_id)
  if (is.na(collection_id) || !nzchar(collection_id)) {
    stop("Updated reference row is missing collection_id.", call. = FALSE)
  }
  collection <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
  .litxr_write_journal_upserted_records(
    NULL,
    row,
    local_path,
    collection,
    cfg = cfg,
    refresh_entity_indexes = refresh_entity_indexes
  )
}

#' Link a published DOI record to an existing arXiv reference
#'
#' Keeps `ref_id` canonical keys unchanged while establishing an explicit link
#' between an existing arXiv record and a DOI-backed published record. The arXiv
#' row receives the DOI value plus `linked_doi_ref_id`; the DOI row receives
#' `linked_arxiv_ref_id`.
#'
#' @param arxiv_ref_id Canonical arXiv ref id or bare arXiv id.
#' @param doi DOI string, DOI URL, or canonical `doi:` ref id.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param add_doi Whether to ingest the DOI from Crossref when it is not already
#'   present locally.
#' @param auto_register Whether missing Crossref collections may be
#'   auto-registered when `add_doi = TRUE`.
#'
#' @return Named list with `arxiv_ref_id`, `doi_ref_id`, `arxiv_title`, and
#'   `doi_title`.
#' @export
litxr_enrich_arxiv_with_doi <- function(arxiv_ref_id, doi, config = NULL, add_doi = TRUE, auto_register = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  config_path <- if (is.character(config)) {
    config
  } else {
    attr(cfg, "config_path", exact = TRUE)
  }

  arxiv_ref_id <- .litxr_normalize_arxiv_ref_id(arxiv_ref_id)
  doi_ref_id <- .litxr_normalize_doi_ref_id(doi)
  if (is.na(arxiv_ref_id) || !nzchar(arxiv_ref_id)) {
    stop("`arxiv_ref_id` must be a non-empty arXiv ref id.", call. = FALSE)
  }
  if (is.na(doi_ref_id) || !nzchar(doi_ref_id)) {
    stop("`doi` must be a non-empty DOI value.", call. = FALSE)
  }
  doi_value <- sub("^doi:", "", doi_ref_id)

  arxiv_row <- .litxr_find_unique_collection_row(cfg, arxiv_ref_id, expected_source = "arxiv")
  existing_doi <- .litxr_scalar_chr(arxiv_row$doi)
  existing_link <- .litxr_scalar_chr(arxiv_row$linked_doi_ref_id)

  if (!is.na(existing_doi) && nzchar(existing_doi) && !identical(existing_doi, doi_value)) {
    stop(
      "ArXiv record already has a different DOI: ",
      existing_doi,
      ". Refusing to overwrite with ",
      doi_value,
      call. = FALSE
    )
  }
  if (!is.na(existing_link) && nzchar(existing_link) && !identical(existing_link, doi_ref_id)) {
    stop(
      "ArXiv record already links to a different DOI ref_id: ",
      existing_link,
      ". Refusing to overwrite with ",
      doi_ref_id,
      call. = FALSE
    )
  }

  doi_row <- .litxr_find_collection_refs_by_keys(cfg, doi_ref_id)
  doi_row <- doi_row[doi_row$ref_id == doi_ref_id & doi_row$source != "arxiv", ]
  if (!nrow(doi_row) && isTRUE(add_doi)) {
    litxr_add_dois(doi_value, config = cfg, auto_register = auto_register)
    cfg <- if (!is.null(config_path) && nzchar(config_path)) litxr_read_config(config_path) else litxr_read_config()
    doi_row <- .litxr_find_collection_refs_by_keys(cfg, doi_ref_id)
    doi_row <- doi_row[doi_row$ref_id == doi_ref_id & doi_row$source != "arxiv", ]
  }
  if (!nrow(doi_row)) {
    stop("Published DOI record is not available locally: ", doi_ref_id, call. = FALSE)
  }
  if (nrow(doi_row) != 1L) {
    stop("Expected exactly one DOI collection row for ", doi_ref_id, " but found ", nrow(doi_row), ".", call. = FALSE)
  }

  doi_existing_link <- .litxr_scalar_chr(doi_row$linked_arxiv_ref_id)
  if (!is.na(doi_existing_link) && nzchar(doi_existing_link) && !identical(doi_existing_link, arxiv_ref_id)) {
    stop(
      "DOI record already links to a different arXiv ref_id: ",
      doi_existing_link,
      ". Refusing to overwrite with ",
      arxiv_ref_id,
      call. = FALSE
    )
  }

  if (!("doi" %in% names(arxiv_row))) arxiv_row[["doi"]] <- NA_character_
  if (!("linked_doi_ref_id" %in% names(arxiv_row))) arxiv_row[["linked_doi_ref_id"]] <- NA_character_
  if (!("linked_arxiv_ref_id" %in% names(doi_row))) doi_row[["linked_arxiv_ref_id"]] <- NA_character_

  arxiv_row$doi[[1]] <- doi_value
  arxiv_row$linked_doi_ref_id[[1]] <- doi_ref_id
  doi_row$linked_arxiv_ref_id[[1]] <- arxiv_ref_id

  .litxr_update_collection_row(cfg, arxiv_row, refresh_entity_indexes = FALSE)
  .litxr_update_collection_row(cfg, doi_row, refresh_entity_indexes = FALSE)

  .litxr_refresh_ref_identity_map(cfg)
  identity_map <- litxr_read_ref_identity_map(cfg)
  linked_entity_ids <- unique(as.character(identity_map$entity_id[
    identity_map$ref_id %in% c(arxiv_ref_id, doi_ref_id)
  ]))
  if (length(linked_entity_ids) != 1L) {
    stop(
      "DOI/arXiv link update did not converge to a single entity_id for ",
      arxiv_ref_id,
      " and ",
      doi_ref_id,
      call. = FALSE
    )
  }

  linked_entity <- .litxr_read_project_ref_identity_index(cfg)
  linked_entity <- linked_entity[linked_entity$entity_id == linked_entity_ids[[1]], ]
  primary_ref_id <- if (nrow(linked_entity)) .litxr_scalar_chr(linked_entity$ref_id[linked_entity$is_primary_ref_id %in% TRUE]) else NA_character_

  list(
    arxiv_ref_id = arxiv_ref_id,
    doi_ref_id = doi_ref_id,
    arxiv_title = .litxr_scalar_chr(arxiv_row$title),
    doi_title = .litxr_scalar_chr(doi_row$title),
    entity_id = linked_entity_ids[[1]],
    primary_ref_id = primary_ref_id,
    preferred_citation_ref_id = doi_ref_id
  )
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
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
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
  .litxr_write_journal_upserted_records(
    NULL,
    records,
    local_path,
    collection,
    cfg = cfg,
    refresh_entity_indexes = TRUE
  )
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

  arxiv_keys <- unique(na.omit(vapply(keys, .litxr_normalize_arxiv_ref_id, character(1))))
  arxiv_base <- sub("^arxiv:", "", arxiv_keys, ignore.case = TRUE)
  unique(c(keys, arxiv_base, arxiv_keys))
}

.litxr_record_lookup_keys <- function(records) {
  if (!nrow(records)) {
    return(character())
  }

  records <- data.table::as.data.table(records)
  keys <- character()

  if ("ref_id" %in% names(records)) {
    keys <- c(keys, as.character(records$ref_id))
  }
  if ("source_id" %in% names(records)) {
    keys <- c(keys, as.character(records$source_id))
  }
  if ("doi" %in% names(records)) {
    doi <- as.character(records$doi)
    doi <- doi[!is.na(doi) & nzchar(doi)]
    keys <- c(keys, doi, paste0("doi:", doi))
  }

  .litxr_expand_reference_keys(keys)
}

.litxr_subset_records_by_lookup_keys <- function(records, keys) {
  records <- data.table::as.data.table(records)
  if (!nrow(records) || !length(keys)) {
    return(records[0, ])
  }

  keys <- .litxr_expand_reference_keys(keys)
  ref_id <- if ("ref_id" %in% names(records)) as.character(records$ref_id) else rep(NA_character_, nrow(records))
  source_id <- if ("source_id" %in% names(records)) as.character(records$source_id) else rep(NA_character_, nrow(records))
  doi <- if ("doi" %in% names(records)) as.character(records$doi) else rep(NA_character_, nrow(records))
  doi_prefixed <- ifelse(!is.na(doi) & nzchar(doi), paste0("doi:", doi), NA_character_)

  keep <- ref_id %in% keys | source_id %in% keys | doi %in% keys | doi_prefixed %in% keys
  records[keep, ]
}

.litxr_normalize_lookup_value <- function(x) {
  x <- as.character(x)
  if (!length(x) || is.na(x[[1L]])) return(NA_character_)
  x <- trimws(x[[1L]])
  if (!nzchar(x)) return(NA_character_)

  if (grepl("^https?://(dx\\.)?doi\\.org/", x, ignore.case = TRUE)) {
    x <- sub("^https?://(dx\\.)?doi\\.org/", "", x, ignore.case = TRUE)
  }
  if (grepl("^doi:", x, ignore.case = TRUE)) {
    x <- sub("^doi:", "", x, ignore.case = TRUE)
  }
  if (grepl("^[0-9]{4}_[0-9]{4,5}(v[0-9]+)?$", x)) {
    x <- paste0("arxiv:", sub("_", ".", x, fixed = TRUE))
  } else if (grepl("^[0-9]{4}\\.[0-9]{4,5}(v[0-9]+)?$", x)) {
    x <- paste0("arxiv:", x)
  }
  x
}

.litxr_lookup_candidates <- function(x) {
  x <- .litxr_normalize_lookup_value(x)
  if (is.na(x) || !nzchar(x)) {
    return(character())
  }
  candidates <- c(x)
  if (grepl("^[0-9]{4}\\.[0-9]{4,5}(v[0-9]+)?$", x)) {
    candidates <- c(candidates, paste0("arxiv:", x))
  } else if (grepl("/", x, fixed = TRUE) && !startsWith(x, "doi:")) {
    candidates <- c(candidates, paste0("doi:", x))
  }
  unique(stats::na.omit(candidates))
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
    out <- .litxr_read_journal_records_by_keys(local_path, keys)
    if (!nrow(out)) {
      return(out)
    }
    if (!("collection_id" %in% names(out))) {
      out[["collection_id"]] <- rep(as.character(collection$collection_id), nrow(out))
    } else {
      missing_collection_id <- is.na(out$collection_id) | !nzchar(as.character(out$collection_id))
      out$collection_id[missing_collection_id] <- as.character(collection$collection_id)
    }
    if (!("collection_title" %in% names(out))) {
      out[["collection_title"]] <- rep(as.character(collection$title), nrow(out))
    } else {
      missing_collection_title <- is.na(out$collection_title) | !nzchar(as.character(out$collection_title))
      out$collection_title[missing_collection_title] <- as.character(collection$title)
    }
    out
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
  if (!("linked_doi_ref_id" %in% names(out))) out[["linked_doi_ref_id"]] <- rep(NA_character_, nrow(out))
  if (!("linked_arxiv_ref_id" %in% names(out))) out[["linked_arxiv_ref_id"]] <- rep(NA_character_, nrow(out))
  if (!("raw_entry" %in% names(out))) out[["raw_entry"]] <- rep(list(NULL), nrow(out))

  if (!("pub_date" %in% names(out))) {
    out[["pub_date"]] <- as.POSIXct(rep(NA_character_, nrow(out)), tz = "UTC")
  }
  if (!inherits(out[["pub_date"]], "POSIXct")) {
    out[["pub_date"]] <- .litxr_parse_arxiv_posixct(out[["pub_date"]])
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
  if (!("collection_id" %in% names(out_records))) {
    out_records[["collection_id"]] <- rep(NA_character_, nrow(out_records))
  }
  if (!("collection_title" %in% names(out_records))) {
    out_records[["collection_title"]] <- rep(NA_character_, nrow(out_records))
  }

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

    data.table::set(out_records, i = i, j = "collection_id", value = as.character(journal$collection_id))
    data.table::set(out_records, i = i, j = "collection_title", value = as.character(journal$title))
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
    json = local_path
  )
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
  if (inherits(x, "Date")) {
    text <- format(x[[1L]], "%Y-%m-%d")
  } else if (inherits(x, c("POSIXct", "POSIXt"))) {
    text <- format(as.Date(x[[1L]], tz = "UTC"), "%Y-%m-%d")
  } else if (is.numeric(x) && length(x) == 1L && is.finite(x[[1L]])) {
    maybe_date <- suppressWarnings(as.Date(x[[1L]], origin = "1970-01-01"))
    text <- if (!is.na(maybe_date)) format(maybe_date, "%Y-%m-%d") else as.character(x[[1L]])
  } else {
    text <- as.character(x[[1L]])
  }
  if (grepl("^[0-9]+$", text) && nchar(text) <= 7L) {
    maybe_date <- suppressWarnings(as.Date(as.integer(text), origin = "1970-01-01"))
    if (!is.na(maybe_date)) {
      return(if (end) {
        paste0(format(maybe_date, "%Y%m%d"), "2359")
      } else {
        paste0(format(maybe_date, "%Y%m%d"), "0000")
      })
    }
  }
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

  parsed <- .litxr_parse_arxiv_posixct(text)
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

.litxr_index_encode <- function(records) {
  out <- data.table::copy(records)

  encode_json_value <- function(x) {
    if (is.null(x)) return(NA_character_)
    if (!length(x)) return(NA_character_)
    if (is.atomic(x) && length(x) == 1L && is.na(x[[1]])) return(NA_character_)
    if (inherits(x, c("xml_node", "xml_document"))) {
      return(as.character(x))
    }
    if (is.factor(x)) {
      return(as.character(x))
    }
    if (is.list(x)) {
      x <- lapply(x, encode_json_value)
      return(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"))
    }
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
  }

  for (name in names(out)) {
    column <- out[[name]]

    if (inherits(column, c("POSIXct", "POSIXt", "Date"))) {
      out[[name]] <- ifelse(
        is.na(column),
        NA_character_,
        format(column, tz = "UTC", usetz = TRUE)
      )
      next
    }

    if (is.factor(column)) {
      out[[name]] <- as.character(column)
      next
    }

    if (is.list(column)) {
      out[[paste0(name, "_json")]] <- vapply(column, encode_json_value, character(1))
      out[[name]] <- NULL
      next
    }
  }

  out
}

.litxr_reference_projection_columns <- function() {
  c(
    "ref_id",
    "source",
    "source_id",
    "entry_type",
    "title",
    "authors",
    "pub_date",
    "year",
    "month",
    "day",
    "journal",
    "container_title",
    "publisher",
    "volume",
    "issue",
    "pages",
    "doi",
    "isbn",
    "issn",
    "url",
    "url_landing",
    "url_pdf",
    "note",
    "subject_primary",
    "arxiv_version",
    "arxiv_primary_category",
    "arxiv_categories_raw",
    "arxiv_journal_ref",
    "linked_doi_ref_id",
    "linked_arxiv_ref_id"
  )
}

.litxr_reference_projection <- function(records) {
  records <- data.table::as.data.table(records)
  if (!nrow(records)) {
    return(data.table::data.table())
  }
  keep <- intersect(.litxr_reference_projection_columns(), names(records))
  data.table::copy(records)[, keep, with = FALSE]
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
    out[["pub_date"]] <- .litxr_parse_arxiv_posixct(out[["pub_date"]])
  }

  out
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

.litxr_write_yaml_atomic <- function(object, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  tmp_path <- paste0(path, ".tmp")
  writeLines(.litxr_format_label_query_yaml(object), tmp_path, useBytes = TRUE)
  if (!file.rename(tmp_path, path)) {
    unlink(tmp_path)
    stop("Failed to atomically write YAML file: ", path, call. = FALSE)
  }
  invisible(path)
}

.litxr_format_label_query_yaml <- function(query_set) {
  if (!is.list(query_set) || !length(query_set)) {
    return(character())
  }
  if (is.null(names(query_set)) || any(!nzchar(names(query_set)))) {
    stop("Label query YAML requires a named list.", call. = FALSE)
  }

  escape_scalar <- function(x) {
    x <- as.character(x)
    x[is.na(x)] <- ""
    x <- gsub("'", "''", x, fixed = TRUE)
    paste0("'", x, "'")
  }

  lines <- character()
  category_ids <- names(query_set)
  for (i in seq_along(category_ids)) {
    category_id <- category_ids[[i]]
    queries <- as.character(unlist(query_set[[i]], use.names = FALSE))
    lines <- c(lines, paste0(category_id, ":"))
    if (length(queries)) {
      lines <- c(lines, paste0("  - ", escape_scalar(queries)))
    } else {
      lines <- c(lines, "  []")
    }
    if (i < length(category_ids)) {
      lines <- c(lines, "")
    }
  }
  lines
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


.litxr_build_enrichment_status_index <- function(cfg) {
  refs <- .litxr_authoritative_project_records(cfg)
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

.litxr_refresh_project_index_for_collection <- function(cfg, collection_id, repair_collection_index = TRUE) {
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  records <- .litxr_read_journal_records_authoritative(local_path)
  projection <- .litxr_project_references_from_collection_records(records)
  links <- .litxr_project_reference_links_from_collection_records(records, journal)
  .litxr_refresh_normalized_reference_scaffold(cfg, records = projection, refresh_entity_indexes = TRUE)

  list(
    collection_id = collection_id,
    mode = "collection_refresh",
    refreshed = TRUE,
    refs_written = nrow(projection),
    links_written = nrow(links),
    refs_removed = 0L
  )
}

.litxr_upsert_project_references_index <- function(cfg, records) {
  .litxr_refresh_normalized_reference_scaffold(cfg, records = .litxr_reference_projection(records), refresh_entity_indexes = TRUE)
  invisible(.litxr_reference_projection(records))
}

.litxr_upsert_project_reference_collections_index <- function(cfg, links) {
  invisible(links)
}

.litxr_update_project_indexes <- function(
  cfg,
  journal,
  records,
  refresh_entity_indexes = TRUE,
  refresh_ref_identity_map = TRUE,
  identities = NULL
) {
  .litxr_ensure_project_index_dir(cfg)
  incoming_refs <- .litxr_project_references_from_collection_records(records)
  if (nrow(incoming_refs)) {
    .litxr_refresh_normalized_reference_scaffold(cfg, records = incoming_refs, refresh_entity_indexes = FALSE)
  }
  if (isTRUE(refresh_ref_identity_map)) {
    if (is.null(identities)) {
      .litxr_refresh_ref_identity_map(cfg)
    } else {
      .litxr_refresh_ref_identity_map(cfg, identities = identities)
    }
  }
  invisible(NULL)
}

.litxr_ref_id_type <- function(ref_id) {
  ref_id <- as.character(ref_id %||% NA_character_)
  if (is.na(ref_id) || !nzchar(ref_id)) {
    return(NA_character_)
  }
  sub(":.*$", "", ref_id)
}

.litxr_ref_id_priority <- function(ref_id) {
  identity_type <- .litxr_ref_id_type(ref_id)
  if (identical(identity_type, "arxiv")) return(1L)
  if (identical(identity_type, "doi")) return(2L)
  if (identical(identity_type, "isbn")) return(3L)
  if (identical(identity_type, "url")) return(4L)
  9L
}

.litxr_entity_id_from_root_ref <- function(ref_id) {
  paste0(
    "ent:",
    .litxr_record_slug(data.table::data.table(ref_id = as.character(ref_id), doi = NA_character_))
  )
}

.litxr_first_author <- function(row) {
  authors_list <- row$authors_list[[1]] %||% character()
  if (length(authors_list)) {
    return(as.character(authors_list[[1]]))
  }
  authors <- as.character(row$authors[[1]] %||% NA_character_)
  if (is.na(authors) || !nzchar(authors)) {
    return(NA_character_)
  }
  trimws(strsplit(authors, ";", fixed = TRUE)[[1]][1])
}

.litxr_author_short <- function(row) {
  authors_list <- row$authors_list[[1]] %||% character()
  first_author <- .litxr_first_author(row)
  if (is.na(first_author) || !nzchar(first_author)) {
    return(NA_character_)
  }
  if (length(authors_list) > 1L) {
    paste0(first_author, " et al.")
  } else {
    first_author
  }
}

.litxr_choose_entity_root_ref <- function(ref_ids) {
  ref_ids <- unique(as.character(ref_ids))
  ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(ref_ids)]
  if (!length(ref_ids)) {
    return(NA_character_)
  }
  priority <- vapply(ref_ids, .litxr_ref_id_priority, integer(1))
  ref_ids[order(priority, ref_ids)][[1]]
}

.litxr_collect_identity_component <- function(seed, neighbors, seen) {
  queue <- seed
  component <- character()
  while (length(queue)) {
    current <- queue[[1]]
    queue <- queue[-1]
    if (current %in% seen) {
      next
    }
    seen <- c(seen, current)
    component <- c(component, current)
    next_neighbors <- neighbors[[current]]
    if (length(next_neighbors)) {
      queue <- c(queue, setdiff(next_neighbors, seen))
    }
  }
  list(component = unique(component), seen = unique(seen))
}

.litxr_build_ref_identity_index <- function(cfg, refs = NULL) {
  if (is.null(refs)) {
    refs <- .litxr_authoritative_project_records(cfg)
  }
  if (!nrow(refs)) {
    out <- data.table::data.table(
      arxiv_id = character(),
      doi = character(),
      ref_id = character(),
      entity_id = character()
    )
    return(out)
  }

  refs <- data.table::copy(refs)
  if (!("linked_doi_ref_id" %in% names(refs))) refs[["linked_doi_ref_id"]] <- rep(NA_character_, nrow(refs))
  if (!("linked_arxiv_ref_id" %in% names(refs))) refs[["linked_arxiv_ref_id"]] <- rep(NA_character_, nrow(refs))
  refs$ref_id <- as.character(refs$ref_id)
  refs$linked_doi_ref_id <- as.character(refs$linked_doi_ref_id)
  refs$linked_arxiv_ref_id <- as.character(refs$linked_arxiv_ref_id)

  ref_type <- vapply(refs$ref_id, .litxr_ref_id_type, character(1))
  arxiv_rows <- ref_type == "arxiv" & !is.na(refs$linked_doi_ref_id) & nzchar(refs$linked_doi_ref_id)
  doi_rows <- ref_type == "doi" & !is.na(refs$linked_arxiv_ref_id) & nzchar(refs$linked_arxiv_ref_id)

  pair_links <- data.table::rbindlist(list(
    data.table::data.table(
      arxiv_id = sub("^arxiv:", "", as.character(refs$ref_id[arxiv_rows])),
      doi = sub("^doi:", "", as.character(refs$linked_doi_ref_id[arxiv_rows]))
    ),
    data.table::data.table(
      arxiv_id = sub("^arxiv:", "", as.character(refs$linked_arxiv_ref_id[doi_rows])),
      doi = sub("^doi:", "", as.character(refs$ref_id[doi_rows]))
    )
  ), fill = TRUE)

  pair_links <- pair_links[!is.na(pair_links$arxiv_id) & nzchar(pair_links$arxiv_id) &
    !is.na(pair_links$doi) & nzchar(pair_links$doi), ]
  if (nrow(pair_links)) {
    pair_links <- data.table::as.data.table(pair_links)
    pair_links[["pair_key"]] <- paste(pair_links$arxiv_id, pair_links$doi, sep = "\r")
    pair_links <- pair_links[!duplicated(pair_links$pair_key), ]

    arxiv_counts <- stats::aggregate(
      doi ~ arxiv_id,
      data = as.data.frame(pair_links[, c("arxiv_id", "doi"), with = FALSE]),
      FUN = function(x) length(unique(x))
    )
    doi_counts <- stats::aggregate(
      arxiv_id ~ doi,
      data = as.data.frame(pair_links[, c("doi", "arxiv_id"), with = FALSE]),
      FUN = function(x) length(unique(x))
    )
    arxiv_conflicts <- data.table::as.data.table(arxiv_counts[arxiv_counts$doi > 1L, , drop = FALSE])
    doi_conflicts <- data.table::as.data.table(doi_counts[doi_counts$arxiv_id > 1L, , drop = FALSE])
    if (nrow(arxiv_conflicts) || nrow(doi_conflicts)) {
      conflict_text <- c(
        if (nrow(arxiv_conflicts)) paste0("arxiv_id(s): ", paste(arxiv_conflicts$arxiv_id, collapse = ", ")),
        if (nrow(doi_conflicts)) paste0("doi(s): ", paste(doi_conflicts$doi, collapse = ", "))
      )
      stop("Ambiguous direct arxiv/doi link(s): ", paste(conflict_text, collapse = "; "), call. = FALSE)
    }
  }

  if (!nrow(pair_links)) {
    return(data.table::data.table(
      arxiv_id = character(),
      doi = character(),
      ref_id = character(),
      entity_id = character()
    ))
  }

  pair_links$ref_id <- paste0("arxiv:", pair_links$arxiv_id)
  pair_links$entity_id <- paste0("arxiv:", pair_links$arxiv_id)
  pair_links[, c("arxiv_id", "doi", "ref_id", "entity_id"), with = FALSE]
}

.litxr_build_entities_index_fast <- function(cfg, refs = NULL, identities = NULL) {
  if (is.null(refs)) refs <- .litxr_authoritative_project_records(cfg)
  if (is.null(identities)) identities <- .litxr_build_ref_identity_index(cfg, refs = refs)
  if (!nrow(refs) || !nrow(identities)) {
    out <- data.table::data.table(
      entity_id = character(),
      primary_ref_id = character(),
      preferred_citation_ref_id = character(),
      display_title = character(),
      year = integer(),
      first_author = character(),
      author_short = character(),
      entry_type = character(),
      primary_source = character(),
      published_source = character(),
      has_md = logical(),
      has_llm_digest = logical(),
      has_published_metadata = logical(),
      updated_at = character()
    )
    return(out)
  }

  .litxr_ref_field_at <- function(refs_dt, field, idx, default = NA_character_) {
    n <- length(idx)
    if (!n) return(vector(typeof(default), 0L))
    if (!(field %in% names(refs_dt))) return(rep(default, n))
    value <- refs_dt[[field]][idx]
    if (is.null(value) || !length(value)) return(rep(default, n))
    if (length(value) == n) return(value)
    rep_len(value, n)
  }
  .litxr_ref_first_author_at <- function(refs_dt, idx) {
    n <- length(idx)
    if (!n) return(character())
    if (!("authors" %in% names(refs_dt))) return(rep(NA_character_, n))
    authors <- refs_dt[["authors"]][idx]
    if (!length(authors)) return(rep(NA_character_, n))
    first <- vapply(authors, function(x) {
      x <- as.character(x)
      if (!length(x) || all(is.na(x))) return(NA_character_)
      x[[1L]]
    }, FUN.VALUE = character(1L), USE.NAMES = FALSE)
    out <- rep(NA_character_, length(first))
    ok <- !is.na(first) & nzchar(first)
    if (any(ok)) out[ok] <- trimws(sub(";.*$", "", first[ok]))
    out
  }
  .litxr_ref_author_short_at <- function(first_author, authors) {
    n <- length(first_author)
    if (!n) return(character())
    out <- rep(NA_character_, n)
    ok <- !is.na(first_author) & nzchar(first_author)
    if (any(ok)) {
      multi <- rep(FALSE, n)
      if (length(authors) == n) {
        multi[ok] <- vapply(authors[ok], function(x) {
          x <- as.character(x)
          length(x) > 1L || (length(x) == 1L && grepl(";", x[[1L]], fixed = TRUE))
        }, FUN.VALUE = logical(1L), USE.NAMES = FALSE)
      }
      out[ok] <- ifelse(multi[ok], paste0(first_author[ok], " et al."), first_author[ok])
    }
    out
  }

  entities <- .litxr_ref_entities_projection(cfg, refs = refs, identity_map = identities)
  if (!nrow(entities)) {
    return(entities)
  }

  now <- format(Sys.time(), tz = "UTC", usetz = TRUE)

  entity_map <- .litxr_ref_entity_resolution_map(cfg, entities = entities)
  artifact_presence_for_ref_ids <- function(ref_ids, dir_path, pattern) {
    ref_ids <- unique(as.character(ref_ids))
    ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(ref_ids)]
    if (!length(ref_ids)) {
      return(data.table::data.table(ref_id = character(), present = logical()))
    }
    files <- if (dir.exists(dir_path)) basename(list.files(dir_path, pattern = pattern, full.names = FALSE)) else character()
    slugs <- vapply(ref_ids, function(x) .litxr_record_slug(data.table::data.table(ref_id = x, doi = NA_character_)), character(1))
    data.table::data.table(ref_id = ref_ids, present = slugs %in% files)
  }
  out <- data.table::copy(entities)
  data.table::setkey(out, entity_id)
  out$year <- NA_integer_
  out$first_author <- NA_character_
  out$author_short <- NA_character_
  out$entry_type <- NA_character_
  out$primary_source <- NA_character_
  out$published_source <- NA_character_
  out$has_md <- FALSE
  out$has_llm_digest <- FALSE
  out$has_published_metadata <- !is.na(out$doi) & nzchar(out$doi)
  out$updated_at <- now
  if (nrow(entity_map)) {
    artifact_map <- data.table::data.table(
      ref_id = as.character(entity_map$ref_id),
      entity_id = as.character(entity_map$entity_id)
    )
    artifact_map <- artifact_map[!duplicated(paste(artifact_map$ref_id, artifact_map$entity_id, sep = "\r")), ]
    md_presence <- artifact_presence_for_ref_ids(artifact_map$ref_id, .litxr_project_md_dir(cfg), "\\.md$")
    llm_presence <- artifact_presence_for_ref_ids(artifact_map$ref_id, .litxr_project_llm_dir(cfg), "\\.json$")
    artifact_map$has_md <- artifact_map$ref_id %in% md_presence$ref_id[md_presence$present %in% TRUE]
    artifact_map$has_llm_digest <- artifact_map$ref_id %in% llm_presence$ref_id[llm_presence$present %in% TRUE]
    artifact_by_entity <- stats::aggregate(
      cbind(has_md, has_llm_digest) ~ entity_id,
      data = as.data.frame(artifact_map),
      FUN = function(x) any(as.logical(x))
    )
    artifact_by_entity <- data.table::as.data.table(artifact_by_entity)
    hit <- match(out$entity_id, artifact_by_entity$entity_id)
    if (any(!is.na(hit))) {
      idx <- !is.na(hit)
      out$has_md[idx] <- artifact_by_entity$has_md[hit[idx]]
      out$has_llm_digest[idx] <- artifact_by_entity$has_llm_digest[hit[idx]]
    }
  }

  if (nrow(refs)) {
    refs_dt <- data.table::as.data.table(refs)
    primary_idx <- match(out$primary_ref_id, refs_dt$ref_id)
    if (any(!is.na(primary_idx))) {
      primary_ok <- !is.na(primary_idx)
      primary_rows <- primary_idx[primary_ok]
      primary_author <- .litxr_ref_field_at(refs_dt, "authors", primary_rows, default = NA_character_)
      primary_first <- .litxr_ref_first_author_at(refs_dt, primary_rows)
      primary_short <- .litxr_ref_author_short_at(primary_first, primary_author)
      primary_join <- data.table::data.table(
        entity_id = as.character(out$entity_id[primary_ok]),
        year = suppressWarnings(as.integer(.litxr_ref_field_at(refs_dt, "year", primary_rows, default = NA_character_))),
        first_author = primary_first,
        author_short = primary_short,
        entry_type = .litxr_ref_field_at(refs_dt, "entry_type", primary_rows, default = NA_character_),
        primary_source = .litxr_ref_field_at(refs_dt, "source", primary_rows, default = NA_character_)
      )
      primary_lookup <- primary_join[!duplicated(primary_join$entity_id), ]
      hit <- match(out$entity_id, primary_lookup$entity_id)
      if (any(!is.na(hit))) {
        idx <- !is.na(hit)
        out$year[idx] <- primary_lookup$year[hit[idx]]
        out$first_author[idx] <- primary_lookup$first_author[hit[idx]]
        out$author_short[idx] <- primary_lookup$author_short[hit[idx]]
        out$entry_type[idx] <- primary_lookup$entry_type[hit[idx]]
        out$primary_source[idx] <- primary_lookup$primary_source[hit[idx]]
      }
    }

    preferred_idx <- match(out$preferred_citation_ref_id, refs_dt$ref_id)
    if (any(!is.na(preferred_idx))) {
      preferred_ok <- !is.na(preferred_idx)
      preferred_rows <- preferred_idx[preferred_ok]
      preferred_author <- .litxr_ref_field_at(refs_dt, "authors", preferred_rows, default = NA_character_)
      preferred_first <- .litxr_ref_first_author_at(refs_dt, preferred_rows)
      preferred_short <- .litxr_ref_author_short_at(preferred_first, preferred_author)
      preferred_join <- data.table::data.table(
        entity_id = as.character(out$entity_id[preferred_ok]),
        preferred_year = suppressWarnings(as.integer(.litxr_ref_field_at(refs_dt, "year", preferred_rows, default = NA_character_))),
        preferred_first_author = preferred_first,
        preferred_author_short = preferred_short,
        preferred_entry_type = .litxr_ref_field_at(refs_dt, "entry_type", preferred_rows, default = NA_character_),
        published_source = .litxr_ref_field_at(refs_dt, "source", preferred_rows, default = NA_character_)
      )
      preferred_lookup <- preferred_join[!duplicated(preferred_join$entity_id), ]
      hit <- match(out$entity_id, preferred_lookup$entity_id)
      if (any(!is.na(hit))) {
        idx <- !is.na(hit)
        pref_year <- preferred_lookup$preferred_year[hit[idx]]
        pref_first <- preferred_lookup$preferred_first_author[hit[idx]]
        pref_short <- preferred_lookup$preferred_author_short[hit[idx]]
        pref_entry <- preferred_lookup$preferred_entry_type[hit[idx]]
        pref_source <- preferred_lookup$published_source[hit[idx]]
        out$year[idx] <- ifelse(is.na(pref_year), out$year[idx], pref_year)
        out$first_author[idx] <- ifelse(is.na(pref_first) | !nzchar(pref_first), out$first_author[idx], pref_first)
        out$author_short[idx] <- ifelse(is.na(pref_short) | !nzchar(pref_short), out$author_short[idx], pref_short)
        out$entry_type[idx] <- ifelse(is.na(pref_entry) | !nzchar(pref_entry), out$entry_type[idx], pref_entry)
        out$published_source[idx] <- pref_source
      }
    }
  }

  out
}

.litxr_build_entities_index <- function(cfg, refs = NULL, identities = NULL) {
  return(.litxr_build_entities_index_fast(cfg, refs = refs, identities = identities))
}

.litxr_build_entity_collections_index <- function(cfg, identities = NULL, links = NULL) {
  return(.litxr_build_entity_collections_index_fast(cfg, identities = identities, links = links))
}

.litxr_entity_status_empty <- function() {
  data.table::data.table(
    entity_id = character(),
    has_ref_json = logical(),
    has_fulltxt_md = logical(),
    has_llm_digest = logical(),
    llm_paper_type = character(),
    has_standardized_findings = logical(),
    n_standardized_findings = integer(),
    has_descriptive_stats = logical(),
    n_descriptive_stats = integer(),
    has_anchor_references = logical(),
    n_anchor_references = integer(),
    has_citation_logic_nodes = logical(),
    n_citation_logic_nodes = integer(),
    digest_schema_version = character(),
    digest_revision_latest = integer(),
    updated_at = character()
  )
}

.litxr_build_entity_collections_index_fast <- function(cfg, identities = NULL, links = NULL) {
  if (is.null(identities)) identities <- .litxr_ref_entity_resolution_map(cfg)
  if (is.null(links)) links <- .litxr_authoritative_project_reference_links(cfg)
  if (!nrow(identities) || !nrow(links)) {
    out <- data.table::data.table(
      entity_id = character(),
      collection_id = character(),
      recorded_at = character()
    )
    return(out)
  }

  identity_map <- data.table::as.data.table(identities)[, c("ref_id", "entity_id"), with = FALSE]
  link_map <- data.table::as.data.table(links)[, c("ref_id", "collection_id", "recorded_at"), with = FALSE]
  hit <- match(link_map$ref_id, identity_map$ref_id)
  merged <- link_map[!is.na(hit), , drop = FALSE]
  merged$entity_id <- as.character(identity_map$entity_id[hit[!is.na(hit)]])
  if (!nrow(merged)) {
    out <- data.table::data.table(
      entity_id = character(),
      collection_id = character(),
      recorded_at = character()
    )
    return(out)
  }
  out <- stats::aggregate(
    recorded_at ~ entity_id + collection_id,
    data = as.data.frame(merged[, c("entity_id", "collection_id", "recorded_at")]),
    FUN = function(x) as.character(max(as.character(x), na.rm = TRUE))
  )
  out <- data.table::as.data.table(out)
  out
}

.litxr_entity_ids_for_ref_ids <- function(cfg, ref_ids, identities = NULL) {
  keys <- .litxr_expand_reference_keys(ref_ids)
  if (!length(keys)) {
    return(character())
  }
  entity_map <- if (!is.null(identities) && nrow(identities) && all(c("ref_id", "entity_id") %in% names(identities))) {
    data.table::as.data.table(identities)
  } else {
    .litxr_ref_entity_resolution_map(cfg)
  }
  if (!nrow(entity_map)) {
    return(character())
  }
  matched <- entity_map[entity_map$ref_id %in% keys, ]
  if (!nrow(matched)) {
    return(character())
  }
  unique(as.character(matched$entity_id))
}

.litxr_entity_status_rows_for_entities_fast <- function(cfg, entity_ids) {
  entity_ids <- unique(as.character(entity_ids))
  entity_ids <- entity_ids[!is.na(entity_ids) & nzchar(entity_ids)]
  if (!length(entity_ids)) {
    return(.litxr_entity_status_empty())
  }

  identity_map <- data.table::as.data.table(litxr_read_ref_identity_map(cfg))
  if (!nrow(identity_map)) {
    return(.litxr_entity_status_empty())
  }
  identity_map <- identity_map[identity_map$entity_id %in% entity_ids, ]
  if (!nrow(identity_map)) {
    return(.litxr_entity_status_empty())
  }

  entity_map <- identity_map[!is.na(identity_map$ref_id) & nzchar(identity_map$ref_id), ]
  entity_map <- entity_map[!duplicated(paste(entity_map$entity_id, entity_map$ref_id, sep = "\r")), ]
  if (!nrow(entity_map)) {
    return(.litxr_entity_status_empty())
  }

  status <- data.table::data.table(entity_id = unique(as.character(identity_map$entity_id)))
  data.table::setkey(status, entity_id)
  status$has_ref_json <- TRUE
  status$has_fulltxt_md <- FALSE
  status$has_llm_digest <- FALSE
  status$llm_paper_type <- NA_character_
  status$has_standardized_findings <- FALSE
  status$n_standardized_findings <- 0L
  status$has_descriptive_stats <- FALSE
  status$n_descriptive_stats <- 0L
  status$has_anchor_references <- FALSE
  status$n_anchor_references <- 0L
  status$has_citation_logic_nodes <- FALSE
  status$n_citation_logic_nodes <- 0L
  status$digest_schema_version <- NA_character_
  status$digest_revision_latest <- NA_integer_
  status$updated_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)

  artifact_presence_for_ref_ids <- function(ref_ids, dir_path, pattern) {
    ref_ids <- unique(as.character(ref_ids))
    ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(ref_ids)]
    if (!length(ref_ids)) {
      return(data.table::data.table(ref_id = character(), present = logical()))
    }
    files <- if (dir.exists(dir_path)) basename(list.files(dir_path, pattern = pattern, full.names = FALSE)) else character()
    slugs <- vapply(ref_ids, function(x) .litxr_record_slug(data.table::data.table(ref_id = x, doi = NA_character_)), character(1))
    data.table::data.table(ref_id = ref_ids, present = slugs %in% files)
  }

  artifact_map <- data.table::data.table(
    ref_id = as.character(entity_map$ref_id),
    entity_id = as.character(entity_map$entity_id)
  )
  artifact_map <- artifact_map[!is.na(artifact_map$ref_id) & nzchar(artifact_map$ref_id), ]
  artifact_map <- artifact_map[!duplicated(paste(artifact_map$entity_id, artifact_map$ref_id, sep = "\r")), ]
  md_presence <- artifact_presence_for_ref_ids(artifact_map$ref_id, .litxr_project_md_dir(cfg), "\\.md$")
  llm_presence <- artifact_presence_for_ref_ids(artifact_map$ref_id, .litxr_project_llm_dir(cfg), "\\.json$")
  artifact_map$has_fulltxt_md <- artifact_map$ref_id %in% md_presence$ref_id[md_presence$present %in% TRUE]
  artifact_map$has_llm_digest <- artifact_map$ref_id %in% llm_presence$ref_id[llm_presence$present %in% TRUE]
  artifact_by_entity <- stats::aggregate(
    cbind(has_fulltxt_md, has_llm_digest) ~ entity_id,
    data = as.data.frame(artifact_map),
    FUN = function(x) any(as.logical(x))
  )
  artifact_by_entity <- data.table::as.data.table(artifact_by_entity)
  if (nrow(artifact_by_entity)) {
    hit <- match(status$entity_id, artifact_by_entity$entity_id)
    if (any(!is.na(hit))) {
      idx <- !is.na(hit)
      status$has_fulltxt_md[idx] <- artifact_by_entity$has_fulltxt_md[hit[idx]]
      status$has_llm_digest[idx] <- artifact_by_entity$has_llm_digest[hit[idx]]
    }
  }

  digests <- tryCatch(
    litxr_read_llm_digests(cfg, columns = c("schema_version", "ref_id", "digest_revision", "updated_at", "paper_type")),
    error = function(e) data.table::data.table()
  )
  digest_join <- data.table::data.table()
  if (nrow(digests)) {
    digests <- data.table::as.data.table(digests)
    digests$ref_id <- vapply(seq_len(nrow(digests)), function(i) .litxr_scalar_chr(digests$ref_id[i]), character(1))
    digests <- digests[!is.na(digests$ref_id) & nzchar(digests$ref_id), ]
    if (nrow(digests)) {
      digest_join <- data.table::copy(digests)
      digest_join$entity_id <- entity_map$entity_id[match(digest_join$ref_id, entity_map$ref_id)]
      digest_join <- digest_join[!is.na(digest_join$entity_id) & nzchar(digest_join$entity_id), ]
    }
    if (nrow(digest_join)) {
      digest_join$digest_revision <- suppressWarnings(as.integer(digest_join$digest_revision))
      digest_join$updated_at <- as.character(digest_join$updated_at)
      data.table::setorder(digest_join, entity_id, -digest_revision, -updated_at, ref_id)
      digest_latest <- digest_join[!duplicated(digest_join$entity_id), ]
      digest_status <- data.table::data.table(
        entity_id = as.character(digest_latest$entity_id),
        digest_schema_version = as.character(digest_latest$schema_version),
        digest_revision_latest = suppressWarnings(as.integer(digest_latest$digest_revision)),
        llm_paper_type = as.character(digest_latest$paper_type)
      )
      hit <- match(status$entity_id, digest_status$entity_id)
      if (any(!is.na(hit))) {
        idx <- !is.na(hit)
        status$digest_schema_version[idx] <- digest_status$digest_schema_version[hit[idx]]
        status$digest_revision_latest[idx] <- digest_status$digest_revision_latest[hit[idx]]
        status$llm_paper_type[idx] <- digest_status$llm_paper_type[hit[idx]]
      }
    }
  }

  count_by_entity <- function(dt, count_name) {
    if (!nrow(dt) || !("ref_id" %in% names(dt))) {
      return(data.table::data.table(entity_id = character(), value = integer())[0])
    }
    idx <- match(dt$ref_id, entity_map$ref_id)
    idx <- idx[!is.na(idx)]
    if (!length(idx)) {
      return(data.table::data.table(entity_id = character(), value = integer())[0])
    }
    mapped <- data.table::data.table(entity_id = entity_map$entity_id[idx])
    if (!nrow(mapped)) {
      return(data.table::data.table(entity_id = character(), value = integer())[0])
    }
    out <- mapped[, data.table::data.table(value = .N), by = entity_id]
    data.table::setnames(out, "value", count_name)
    out
  }

  findings <- tryCatch(
    .litxr_read_key_table(
      .litxr_standardized_findings_paths(cfg)$main,
      .litxr_standardized_findings_paths(cfg)$delta,
      .litxr_empty_standardized_findings,
      c("ref_id", "finding_id"),
      columns = "ref_id"
    ),
    error = function(e) data.table::data.table()
  )
  desc_stats <- tryCatch(
    .litxr_read_key_table(
      .litxr_descriptive_stats_paths(cfg)$main,
      .litxr_descriptive_stats_paths(cfg)$delta,
      .litxr_empty_descriptive_stats,
      c("ref_id", "table_id", "variable"),
      columns = "ref_id"
    ),
    error = function(e) data.table::data.table()
  )
  anchors <- tryCatch(
    .litxr_read_key_table(
      .litxr_anchor_references_paths(cfg)$main,
      .litxr_anchor_references_paths(cfg)$delta,
      .litxr_empty_anchor_references,
      c("ref_id", "anchor_rank"),
      columns = "ref_id"
    ),
    error = function(e) data.table::data.table()
  )
  nodes <- tryCatch(
    .litxr_read_key_table(
      .litxr_citation_logic_nodes_paths(cfg)$main,
      .litxr_citation_logic_nodes_paths(cfg)$delta,
      .litxr_empty_citation_logic_nodes,
      c("ref_id", "node_id"),
      columns = "ref_id"
    ),
    error = function(e) data.table::data.table()
  )

  counts <- list(
    count_by_entity(findings, "n_standardized_findings"),
    count_by_entity(desc_stats, "n_descriptive_stats"),
    count_by_entity(anchors, "n_anchor_references"),
    count_by_entity(nodes, "n_citation_logic_nodes")
  )
  for (count_dt in counts) {
    if (nrow(count_dt)) {
      count_name <- setdiff(names(count_dt), "entity_id")[1L]
      hit <- match(status$entity_id, count_dt$entity_id)
      if (any(!is.na(hit))) {
        idx <- !is.na(hit)
        status[[count_name]][idx] <- count_dt[[count_name]][hit[idx]]
      }
    }
  }

  status <- data.table::as.data.table(status)
  status$has_standardized_findings <- status$n_standardized_findings > 0L
  status$has_descriptive_stats <- status$n_descriptive_stats > 0L
  status$has_anchor_references <- status$n_anchor_references > 0L
  status$has_citation_logic_nodes <- status$n_citation_logic_nodes > 0L
  status$has_ref_json[is.na(status$has_ref_json)] <- FALSE
  status$has_fulltxt_md[is.na(status$has_fulltxt_md)] <- FALSE
  status$has_llm_digest[is.na(status$has_llm_digest)] <- FALSE
  status$llm_paper_type[is.na(status$llm_paper_type) | !nzchar(status$llm_paper_type)] <- "unknown"
  data.table::setcolorder(status, names(.litxr_entity_status_empty()))
  status
}

.litxr_entity_status_rows_for_entities <- function(cfg, entity_ids, identities = NULL, refs = NULL) {
  return(.litxr_entity_status_rows_for_entities_fast(cfg, entity_ids = entity_ids))
}

.litxr_build_entity_status_index <- function(cfg, identities = NULL, refs = NULL) {
  identity_map <- data.table::as.data.table(litxr_read_ref_identity_map(cfg))
  entity_ids <- if (nrow(identity_map)) unique(as.character(identity_map$entity_id)) else character()
  out <- .litxr_entity_status_rows_for_entities_fast(cfg, entity_ids = entity_ids)
  if (!nrow(out)) out <- .litxr_entity_status_empty()
  out
}

.litxr_build_entity_indexes <- function(cfg) {
  .litxr_ensure_project_index_dir(cfg)
  inputs <- .litxr_authoritative_entity_inputs(cfg)
  refs <- inputs$refs
  links <- inputs$links
  identities <- .litxr_build_ref_identity_index(cfg, refs = refs)
  entity_status <- .litxr_entity_status_rows_for_entities_fast(
    cfg,
    entity_ids = if (nrow(identities)) unique(as.character(identities$entity_id)) else character()
  )

  list(
    ref_identity_map_path = .litxr_project_ref_identity_index_path(cfg),
    n_ref_identity_map = nrow(identities),
    n_entities = nrow(identities),
    n_entity_collections = nrow(links),
    n_entity_status = nrow(entity_status)
  )
}

.litxr_read_project_ref_identity_index <- function(cfg, columns = NULL) {
  path <- .litxr_project_ref_identity_index_path(cfg)
  records <- .litxr_read_fst_subset(path, columns = columns)
  records <- data.table::as.data.table(records)
  if (!nrow(records)) {
    return(data.table::data.table(
      arxiv_id = character(),
      doi = character(),
      ref_id = character(),
      entity_id = character()
    ))
  }
  if (!("arxiv_id" %in% names(records))) records[["arxiv_id"]] <- rep(NA_character_, nrow(records))
  if (!("doi" %in% names(records))) records[["doi"]] <- rep(NA_character_, nrow(records))
  if (all(is.na(records$arxiv_id)) && all(is.na(records$doi)) && "ref_id" %in% names(records)) {
    ref_ids <- as.character(records$ref_id)
    arxiv_mask <- grepl("^arxiv:", ref_ids)
    doi_mask <- grepl("^doi:", ref_ids)
    records$arxiv_id[arxiv_mask] <- sub("^arxiv:", "", ref_ids[arxiv_mask])
    records$doi[doi_mask] <- sub("^doi:", "", ref_ids[doi_mask])
  }
  records$arxiv_id <- as.character(records$arxiv_id)
  records$doi <- as.character(records$doi)
  records$ref_id <- ifelse(!is.na(records$arxiv_id) & nzchar(records$arxiv_id), paste0("arxiv:", records$arxiv_id), paste0("doi:", records$doi))
  records$entity_id <- records$ref_id
  records[, c("arxiv_id", "doi", "ref_id", "entity_id"), with = FALSE]
}

.litxr_write_project_ref_identity_index <- function(cfg, records) {
  .litxr_ensure_project_index_dir(cfg)
  records <- data.table::as.data.table(records)
  keep <- intersect(c("arxiv_id", "doi"), names(records))
  out <- if (length(keep)) records[, keep, with = FALSE] else data.table::data.table(arxiv_id = character(), doi = character())
  if (!("arxiv_id" %in% names(out))) out[["arxiv_id"]] <- character()
  if (!("doi" %in% names(out))) out[["doi"]] <- character()
  out <- out[!is.na(out$arxiv_id) & nzchar(out$arxiv_id) & !is.na(out$doi) & nzchar(out$doi), ]
  if (nrow(out)) {
    out <- out[!duplicated(paste(out$arxiv_id, out$doi, sep = "\r")), ]
  }
  fst::write_fst(as.data.frame(out), .litxr_project_ref_identity_index_path(cfg))
  invisible(.litxr_project_ref_identity_index_path(cfg))
}
