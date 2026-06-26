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

  targets <- .litxr_embedding_target_rows_from_thin_ref_stores(cfg, collection_id)
  collection_ref_ids <- unique(as.character(targets$ref_id[!is.na(targets$ref_id) & nzchar(targets$ref_id)]))

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

.litxr_read_fst_table_safe <- function(path, columns = NULL) {
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  tryCatch(
    data.table::as.data.table(fst::read_fst(path, as.data.table = TRUE, columns = columns)),
    error = function(e) data.table::data.table()
  )
}

.litxr_collection_index_for_id <- function(cfg, collection_id) {
  collection_id <- as.character(collection_id)[[1L]]
  collections <- .litxr_config_collections(cfg)
  if (!length(collections)) {
    return(NA_integer_)
  }
  collection_ids <- vapply(collections, function(collection) {
    as.character(collection$collection_id %||% collection$journal_id %||% NA_character_)
  }, character(1))
  idx <- match(collection_id, collection_ids)
  suppressWarnings(as.integer(idx[[1L]]))
}

.litxr_embedding_target_rows_from_thin_ref_stores <- function(cfg, collection_id) {
  collection_index <- .litxr_collection_index_for_id(cfg, collection_id)
  if (is.na(collection_index) || collection_index < 1L) {
    stop("Collection not found in config: ", collection_id, call. = FALSE)
  }
  collections <- .litxr_config_collections(cfg)
  collection_ref_dir <- if (collection_index <= length(collections)) {
    .litxr_collection_ref_dir(cfg, as.character(collections[[collection_index]]$collection_id %||% collections[[collection_index]]$journal_id))
  } else {
    NA_character_
  }

  arxiv_rows <- .litxr_read_fst_table_safe(
    .litxr_ref_arxiv_path(cfg),
    columns = c("arxiv_id", "collection_index", "json_filename")
  )
  doi_rows <- .litxr_read_fst_table_safe(
    .litxr_ref_doi_path(cfg),
    columns = c("doi", "collection_index", "json_filename")
  )

  parts <- list()
  if (nrow(arxiv_rows) && "arxiv_id" %in% names(arxiv_rows)) {
    arxiv_rows <- arxiv_rows[
      !is.na(arxiv_rows$collection_index) &
        arxiv_rows$collection_index == collection_index &
        !is.na(arxiv_rows$arxiv_id) &
        nzchar(arxiv_rows$arxiv_id),
      ,
      drop = FALSE
    ]
    if (nrow(arxiv_rows)) {
      parts[[length(parts) + 1L]] <- data.table::data.table(
        ref_id = vapply(arxiv_rows$arxiv_id, .litxr_normalize_arxiv_ref_id, character(1)),
        collection_id = as.character(collection_id),
        collection_index = as.integer(arxiv_rows$collection_index),
        json_filename = as.character(arxiv_rows$json_filename),
        json_path = if (is.na(collection_ref_dir) || !nzchar(collection_ref_dir)) {
          NA_character_
        } else {
          file.path(collection_ref_dir, as.character(arxiv_rows$json_filename))
        },
        key_type = "arxiv_id"
      )
    }
  }
  if (nrow(doi_rows) && "doi" %in% names(doi_rows)) {
    doi_rows <- doi_rows[
      !is.na(doi_rows$collection_index) &
        doi_rows$collection_index == collection_index &
        !is.na(doi_rows$doi) &
        nzchar(doi_rows$doi),
      ,
      drop = FALSE
    ]
    if (nrow(doi_rows)) {
      parts[[length(parts) + 1L]] <- data.table::data.table(
        ref_id = vapply(doi_rows$doi, .litxr_normalize_doi_ref_id, character(1)),
        collection_id = as.character(collection_id),
        collection_index = as.integer(doi_rows$collection_index),
        json_filename = as.character(doi_rows$json_filename),
        json_path = if (is.na(collection_ref_dir) || !nzchar(collection_ref_dir)) {
          NA_character_
        } else {
          file.path(collection_ref_dir, as.character(doi_rows$json_filename))
        },
        key_type = "doi"
      )
    }
  }

  if (!length(parts)) {
    return(data.table::data.table(
      ref_id = character(),
      collection_id = character(),
      collection_index = integer(),
      json_filename = character(),
      json_path = character(),
      key_type = character()
    ))
  }

  targets <- data.table::rbindlist(parts, fill = TRUE)
  targets <- targets[
    !is.na(targets$ref_id) &
      nzchar(targets$ref_id) &
      !is.na(targets$json_filename) &
      nzchar(targets$json_filename) &
      !is.na(targets$json_path) &
      nzchar(targets$json_path),
    ,
    drop = FALSE
  ]
  if (!nrow(targets)) {
    return(targets)
  }
  targets <- targets[!duplicated(targets$ref_id), ]
  targets
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

  targets <- .litxr_embedding_target_rows_from_thin_ref_stores(cfg, collection_id)
  collection_ref_ids <- unique(as.character(targets$ref_id[!is.na(targets$ref_id) & nzchar(targets$ref_id)]))

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

  records <- .litxr_read_collection_records_from_json(.litxr_collection_ref_dir(cfg, collection_id))
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

#' Rewrite legacy embedding metadata files to the narrow two-column shape
#'
#' This helper rewrites any `metadata.fst` file found under one embedding cache
#' directory so that persisted metadata only contains `ref_id` and `abstract`.
#' The `abstract` values are rehydrated from the collection JSON files using the
#' shard `ref_id` values, and existing extra columns are dropped.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param field Embedded text field.
#' @param model Exact embedding model name used when building the cache.
#' @param recurse Whether to scan recursively for `metadata.fst` files.
#'
#' @return Character vector of rewritten paths.
#' @export
litxr_migrate_embedding_metadata_files <- function(
  collection_id,
  config = NULL,
  field = "abstract",
  model,
  recurse = TRUE
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }
  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  root_dir <- paths$dir
  if (!dir.exists(root_dir)) {
    return(character())
  }

  metadata_files <- list.files(root_dir, pattern = "metadata\\.fst$", full.names = TRUE, recursive = isTRUE(recurse))
  if (!length(metadata_files)) {
    return(character())
  }
  targets <- .litxr_embedding_target_rows_from_thin_ref_stores(cfg, collection_id)
  if (!nrow(targets)) {
    return(character())
  }

  rewritten <- character()
  for (path in metadata_files) {
    if (!file.exists(path)) {
      next
    }
    metadata <- .litxr_normalize_embedding_metadata(fst::read_fst(path, as.data.table = TRUE))
    if (nrow(metadata)) {
      target_map <- targets[match(as.character(metadata$ref_id), targets$ref_id), c("ref_id", "json_path"), with = FALSE]
      hydrated <- .litxr_hydrate_rows_from_json_paths(
        data.table::data.table(ref_id = as.character(metadata$ref_id)),
        target_map,
        fields = "abstract"
      )
      metadata$abstract <- if ("abstract" %in% names(hydrated)) as.character(hydrated$abstract) else rep(NA_character_, nrow(metadata))
    } else {
      metadata$abstract <- character()
    }
    metadata <- metadata[, c("ref_id", "abstract"), drop = FALSE]
    .litxr_write_fst_atomic(as.data.frame(metadata), path)
    rewritten <- c(rewritten, path)
  }
  unique(rewritten)
}
