.litxr_read_index_columns_safe <- function(path) {
  tryCatch(
    fst::metadata_fst(path)$columnNames,
    error = function(e) NULL
  )
}

.litxr_hydrate_rows_from_json_dir <- function(rows, json_dir) {
  rows <- data.table::as.data.table(rows)
  if (!nrow(rows) || !dir.exists(json_dir)) {
    return(rows)
  }

  json_paths <- list.files(json_dir, pattern = "\\.json$", full.names = TRUE)
  if (!length(json_paths)) {
    return(rows)
  }

  file_map <- data.table::data.table(
    slug = sub("\\.json$", "", basename(json_paths)),
    path = json_paths
  )
  row_map <- data.table::data.table(
    ref_id = if ("ref_id" %in% names(rows)) as.character(rows$ref_id) else rep(NA_character_, nrow(rows)),
    slug = .litxr_record_slugs(
      if ("ref_id" %in% names(rows)) rows$ref_id else rep(NA_character_, nrow(rows)),
      if ("doi" %in% names(rows)) rows$doi else NULL
    ),
    row_idx = seq_len(nrow(rows))
  )
  matched_idx <- match(row_map$slug, file_map$slug)
  matched_idx <- matched_idx[!is.na(matched_idx)]
  if (!length(matched_idx)) {
    return(rows)
  }
  matched <- data.table::data.table(
    ref_id = row_map$ref_id[!is.na(row_map$slug) & !is.na(match(row_map$slug, file_map$slug))],
    slug = row_map$slug[!is.na(row_map$slug) & !is.na(match(row_map$slug, file_map$slug))],
    path = file_map$path[matched_idx]
  )

  unique_paths <- unique(matched$path)
  updates <- lapply(unique_paths, function(json_path) {
    tryCatch(.litxr_storage_payload_as_list(json_path), error = function(e) NULL)
  })
  updates <- updates[!vapply(updates, is.null, logical(1))]
  if (!length(updates)) {
    return(rows)
  }

  scalar_chr <- function(value) {
    if (is.null(value) || !length(value)) {
      return(NA_character_)
    }
    if (is.list(value)) {
      value <- value[[1L]]
    } else {
      value <- value[[1L]]
    }
    if (is.null(value) || !length(value) || (length(value) == 1L && is.na(value[[1L]]))) {
      return(NA_character_)
    }
    as.character(value[[1L]])
  }

  updates <- data.table::data.table(
    ref_id = vapply(updates, function(payload) scalar_chr(if ("ref_id" %in% names(payload)) payload$ref_id else NULL), character(1)),
    abstract = vapply(updates, function(payload) scalar_chr(if ("abstract" %in% names(payload)) payload$abstract else NULL), character(1)),
    authors_list = lapply(updates, function(payload) {
      if ("authors_list" %in% names(payload) && !is.null(payload$authors_list)) {
        unlist(payload$authors_list, use.names = FALSE)
      } else {
        character()
      }
    })
  )
  updates <- updates[!duplicated(as.character(updates$ref_id)), ]
  if (!nrow(updates)) {
    return(rows)
  }

  out <- data.table::copy(rows)
  data.table::setkey(updates, ref_id)
  match_idx <- match(out$ref_id, updates$ref_id)
  hit_idx <- which(!is.na(match_idx))
  if (!length(hit_idx)) {
    return(rows)
  }

  for (field in c("abstract", "authors_list")) {
    if (!(field %in% names(updates))) {
      next
    }
    if (!(field %in% names(out))) {
      out[[field]] <- if (is.list(updates[[field]])) rep(list(NULL), nrow(out)) else rep(NA_character_, nrow(out))
    }
    value <- updates[[field]][match_idx[hit_idx]]
    column <- out[[field]]
    column[hit_idx] <- value
    out[[field]] <- column
  }

  attr(out, "hydrated_ref_ids") <- unique(as.character(updates$ref_id))
  out
}

.litxr_hydrate_collection_projection_rows <- function(local_path, rows) {
  needed_fields <- c("abstract", "authors_list")
  rows <- data.table::as.data.table(rows)
  if (!nrow(rows) || all(needed_fields %in% names(rows))) {
    return(rows)
  }

  paths <- .litxr_journal_paths(local_path)
  json_dir <- .litxr_existing_collection_dir(paths$json, paths$legacy_json)
  .litxr_hydrate_rows_from_json_dir(rows, json_dir)
}

.litxr_runtime_wide_projection_limit <- function(limit = getOption("litxr.runtime_wide_projection_limit", 300L)) {
  if (missing(limit) || is.null(limit) || !length(limit)) {
    return(300L)
  }
  if (is.numeric(limit) && length(limit) && is.infinite(limit[[1L]])) {
    return(Inf)
  }
  limit <- suppressWarnings(as.integer(limit[[1L]]))
  if (is.na(limit) || limit < 1L) {
    return(300L)
  }
  limit
}

.litxr_keyed_fst_read_threshold <- function(limit = getOption("litxr.keyed_fst_read_threshold", 32L)) {
  if (missing(limit) || is.null(limit) || !length(limit)) {
    return(32L)
  }
  limit <- suppressWarnings(as.integer(limit[[1L]]))
  if (is.na(limit) || limit < 1L) {
    return(32L)
  }
  limit
}

.litxr_hydrate_project_projection_rows <- function(cfg, rows, wide_projection_limit = getOption("litxr.runtime_wide_projection_limit", 300L)) {
  rows <- data.table::as.data.table(rows)
  if (!nrow(rows)) {
    return(rows)
  }

  wide_projection_limit <- .litxr_runtime_wide_projection_limit(wide_projection_limit)
  if (is.finite(wide_projection_limit) && nrow(rows) > wide_projection_limit) {
    stop(
      "Refusing to materialize ",
      nrow(rows),
      " wide project-reference rows because it exceeds the runtime wide-projection limit of ",
      wide_projection_limit,
      ". Filter more narrowly or raise the limit explicitly for this call.",
      call. = FALSE
    )
  }

  needed_fields <- c("abstract", "authors_list")
  if (all(needed_fields %in% names(rows))) {
    return(rows)
  }

  links <- .litxr_authoritative_project_reference_links(cfg)
  if (!nrow(links)) {
    return(rows)
  }

  collections <- .litxr_config_collections(cfg)
  if (!length(collections)) {
    return(rows)
  }

  out <- data.table::copy(rows)
  unresolved <- rep(TRUE, nrow(out))
  for (collection in collections) {
    if (!any(unresolved)) {
      break
    }
    collection_id <- as.character(collection$collection_id %||% collection$journal_id)
    local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
    if (!length(local_path) || is.na(local_path[[1]]) || !nzchar(local_path[[1]])) {
      next
    }
    ref_ids <- if (nrow(links) && all(c("collection_id", "ref_id") %in% names(links))) {
      unique(as.character(links$ref_id[as.character(links$collection_id) == collection_id]))
    } else {
      character()
    }
    ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(ref_ids)]
    if (!length(ref_ids)) {
      next
    }
    batch_idx <- which(unresolved & out$ref_id %in% ref_ids)
    if (!length(batch_idx)) {
      next
    }
    candidate <- out[batch_idx, ]
    hydrated <- .litxr_hydrate_collection_projection_rows(local_path[[1]], candidate)
    if (!nrow(hydrated)) {
      next
    }
    hit_ref_ids <- unique(as.character(hydrated$ref_id))
    hit_idx <- batch_idx[out$ref_id[batch_idx] %in% hit_ref_ids]
    if (!length(hit_idx)) {
      next
    }
    hydrated_hits <- hydrated[hydrated$ref_id %in% out$ref_id[hit_idx], ]
    if (!nrow(hydrated_hits)) {
      next
    }
    data.table::setkey(hydrated_hits, ref_id)
    match_idx <- match(out$ref_id[hit_idx], hydrated_hits$ref_id)
    for (field in setdiff(names(hydrated_hits), "ref_id")) {
      if (!(field %in% names(out))) {
        if (is.list(hydrated_hits[[field]])) {
          out[[field]] <- rep(list(NULL), nrow(out))
        } else if (inherits(hydrated_hits[[field]], c("POSIXct", "POSIXt"))) {
          out[[field]] <- rep(as.POSIXct(NA, tz = "UTC"), nrow(out))
        } else if (is.integer(hydrated_hits[[field]])) {
          out[[field]] <- rep(NA_integer_, nrow(out))
        } else if (is.numeric(hydrated_hits[[field]])) {
          out[[field]] <- rep(NA_real_, nrow(out))
        } else {
          out[[field]] <- rep(NA_character_, nrow(out))
        }
      }
      value <- hydrated_hits[[field]][match_idx]
      column <- out[[field]]
      column[hit_idx] <- value
      out[[field]] <- column
    }
    unresolved[hit_idx] <- FALSE
  }
  out
}

.litxr_read_journal_records_by_keys <- function(local_path, keys, keyed_fst_read_threshold = getOption("litxr.keyed_fst_read_threshold", 32L)) {
  keys <- .litxr_expand_reference_keys(keys)
  records <- .litxr_read_journal_records(local_path)
  if (!nrow(records)) {
    return(records)
  }
  out <- .litxr_subset_records_by_lookup_keys(records, keys)
  key <- .litxr_upsert_key(out)
  out <- out[!duplicated(key), ]
  .litxr_hydrate_collection_projection_rows(local_path, out)
}

.litxr_read_collection_delta <- function(local_path) {
  stop("collection delta ingest is retired and is no longer read.", call. = FALSE)
}

.litxr_read_existing_records_for_incoming <- function(local_path, incoming) {
  incoming <- data.table::as.data.table(incoming)
  if (!nrow(incoming)) {
    return(incoming[0, ])
  }
  keys <- .litxr_record_lookup_keys(incoming)
  if (!length(keys)) {
    return(incoming[0, ])
  }
  .litxr_read_journal_records_by_keys(local_path, keys)
}

.litxr_project_reference_conflict_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "_project_reference_upsert_conflicts.jsonl")
}
