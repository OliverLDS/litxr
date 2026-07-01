.litxr_read_index_columns_safe <- function(path) {
  tryCatch(
    fst::metadata_fst(path)$columnNames,
    error = function(e) NULL
  )
}

.litxr_collection_index_for_local_path <- function(cfg, local_path) {
  collections <- .litxr_config_collections(cfg)
  if (!length(collections)) {
    return(NA_integer_)
  }
  resolved <- vapply(collections, function(collection) {
    as.character(.litxr_resolve_local_path(cfg, collection$local_path))
  }, character(1))
  idx <- match(
    normalizePath(path.expand(local_path), winslash = "/", mustWork = FALSE),
    normalizePath(resolved, winslash = "/", mustWork = FALSE)
  )
  suppressWarnings(as.integer(idx[[1L]]))
}

.litxr_ref_json_locations_from_thin_stores <- function(cfg, ref_ids, collection_id = NULL) {
  ref_ids <- unique(as.character(unlist(ref_ids, use.names = FALSE)))
  ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(ref_ids)]
  if (!length(ref_ids)) {
    return(data.table::data.table(
      ref_id = character(),
      collection_id = character(),
      collection_index = integer(),
      json_filename = character(),
      json_path = character()
    ))
  }

  collections <- .litxr_config_collections(cfg)
  collection_ref_dirs <- vapply(collections, function(collection) {
    as.character(.litxr_collection_ref_dir(cfg, collection$collection_id %||% collection$journal_id))
  }, character(1))

  collection_filter <- NULL
  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    collection_filter <- match(
      as.character(collection_id[[1L]]),
      vapply(collections, function(collection) {
        as.character(collection$collection_id %||% collection$journal_id %||% NA_character_)
      }, character(1))
    )
    if (is.na(collection_filter) || collection_filter < 1L) {
      return(data.table::data.table(
        ref_id = character(),
        collection_id = character(),
        collection_index = integer(),
        json_filename = character(),
        json_path = character()
      ))
    }
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
        !is.na(arxiv_rows$arxiv_id) &
        nzchar(arxiv_rows$arxiv_id) &
        as.character(arxiv_rows$arxiv_id) %in% ref_ids,
      ,
      drop = FALSE
    ]
    if (!is.null(collection_filter)) {
      arxiv_rows <- arxiv_rows[arxiv_rows$collection_index == collection_filter, , drop = FALSE]
    }
    if (nrow(arxiv_rows)) {
      json_path <- vapply(seq_len(nrow(arxiv_rows)), function(i) {
        idx <- suppressWarnings(as.integer(arxiv_rows$collection_index[[i]]))
        if (is.na(idx) || idx < 1L || idx > length(collection_ref_dirs)) {
          return(NA_character_)
        }
        file.path(collection_ref_dirs[[idx]], as.character(arxiv_rows$json_filename[[i]]))
      }, character(1))
      parts[[length(parts) + 1L]] <- data.table::data.table(
        ref_id = vapply(arxiv_rows$arxiv_id, .litxr_normalize_arxiv_ref_id, character(1)),
        collection_id = vapply(seq_len(nrow(arxiv_rows)), function(i) {
          idx <- suppressWarnings(as.integer(arxiv_rows$collection_index[[i]]))
          if (is.na(idx) || idx < 1L || idx > length(collections)) return(NA_character_)
          as.character(collections[[idx]]$collection_id %||% collections[[idx]]$journal_id %||% NA_character_)
        }, character(1)),
        collection_index = as.integer(arxiv_rows$collection_index),
        json_filename = as.character(arxiv_rows$json_filename),
        json_path = json_path
      )
    }
  }
  if (nrow(doi_rows) && "doi" %in% names(doi_rows)) {
    doi_rows <- doi_rows[
      !is.na(doi_rows$collection_index) &
        !is.na(doi_rows$doi) &
        nzchar(doi_rows$doi) &
        as.character(doi_rows$doi) %in% ref_ids,
      ,
      drop = FALSE
    ]
    if (!is.null(collection_filter)) {
      doi_rows <- doi_rows[doi_rows$collection_index == collection_filter, , drop = FALSE]
    }
    if (nrow(doi_rows)) {
      json_path <- vapply(seq_len(nrow(doi_rows)), function(i) {
        idx <- suppressWarnings(as.integer(doi_rows$collection_index[[i]]))
        if (is.na(idx) || idx < 1L || idx > length(collection_ref_dirs)) {
          return(NA_character_)
        }
        file.path(collection_ref_dirs[[idx]], as.character(doi_rows$json_filename[[i]]))
      }, character(1))
      parts[[length(parts) + 1L]] <- data.table::data.table(
        ref_id = vapply(doi_rows$doi, .litxr_normalize_doi_ref_id, character(1)),
        collection_id = vapply(seq_len(nrow(doi_rows)), function(i) {
          idx <- suppressWarnings(as.integer(doi_rows$collection_index[[i]]))
          if (is.na(idx) || idx < 1L || idx > length(collections)) return(NA_character_)
          as.character(collections[[idx]]$collection_id %||% collections[[idx]]$journal_id %||% NA_character_)
        }, character(1)),
        collection_index = as.integer(doi_rows$collection_index),
        json_filename = as.character(doi_rows$json_filename),
        json_path = json_path
      )
    }
  }

  if (!length(parts)) {
    return(data.table::data.table(
      ref_id = character(),
      collection_id = character(),
      collection_index = integer(),
      json_filename = character(),
      json_path = character()
    ))
  }
  out <- data.table::rbindlist(parts, fill = TRUE)
  out <- out[!is.na(out$ref_id) & nzchar(out$ref_id) & !is.na(out$json_path) & nzchar(out$json_path), ]
  if (!nrow(out)) {
    return(out)
  }
  out <- out[!duplicated(out$ref_id), ]
  out
}

.litxr_hydrate_rows_from_json_paths <- function(rows, json_paths, fields = c("abstract", "authors_list")) {
  rows <- data.table::as.data.table(rows)
  if (!nrow(rows)) {
    return(rows)
  }
  key_col <- if ("ref_id" %in% names(rows)) {
    "ref_id"
  } else if ("arxiv_id" %in% names(rows)) {
    "arxiv_id"
  } else {
    NULL
  }
  if (is.null(key_col)) {
    stop("`rows` must contain `ref_id` or `arxiv_id`.", call. = FALSE)
  }
  fields <- unique(as.character(fields))
  fields <- fields[nzchar(fields)]
  if (!length(fields)) {
    fields <- "abstract"
  }
  for (field in fields) {
    if (!(field %in% names(rows))) {
      if (field %in% c("authors_list", "raw_entry")) {
        rows[[field]] <- rep(list(NULL), nrow(rows))
      } else if (field == "pub_date") {
        rows[[field]] <- rep(as.POSIXct(NA, tz = "UTC"), nrow(rows))
      } else if (field %in% c("year", "month", "day", "arxiv_version")) {
        rows[[field]] <- rep(NA_integer_, nrow(rows))
      } else {
        rows[[field]] <- rep(NA_character_, nrow(rows))
      }
    }
  }
  if (is.atomic(json_paths) && !is.null(names(json_paths))) {
    json_paths <- data.table::data.table(ref_id = names(json_paths), json_path = as.character(json_paths))
  } else {
    json_paths <- data.table::as.data.table(json_paths)
  }
  if (!nrow(json_paths)) {
    return(rows)
  }
  if (!("json_path" %in% names(json_paths))) {
    return(rows)
  }
  if (!(key_col %in% names(json_paths))) {
    if ("ref_id" %in% names(json_paths) && key_col != "ref_id") {
      data.table::setnames(json_paths, "ref_id", key_col)
    } else {
      return(rows)
    }
  }
  json_paths <- json_paths[
    !is.na(json_paths[[key_col]]) & nzchar(json_paths[[key_col]]) &
      !is.na(json_paths$json_path) & nzchar(json_paths$json_path),
    ,
    drop = FALSE
  ]
  if (!nrow(json_paths)) {
    return(rows)
  }
  json_paths <- json_paths[!duplicated(json_paths[[key_col]]), ]
  hit <- match(as.character(rows[[key_col]]), as.character(json_paths[[key_col]]))
  keep <- which(!is.na(hit))
  if (!length(keep)) {
    return(rows)
  }

  json_paths <- json_paths[hit[keep], , drop = FALSE]
  json_paths$row_idx <- keep
  data.table::setorder(json_paths, json_path, row_idx)
  run_starts <- c(1L, which(json_paths$json_path[-1L] != json_paths$json_path[-nrow(json_paths)]) + 1L)
  run_ends <- c(run_starts[-1L] - 1L, nrow(json_paths))
  for (run_id in seq_along(run_starts)) {
    idx <- json_paths$row_idx[run_starts[[run_id]]:run_ends[[run_id]]]
    json_path <- json_paths$json_path[[run_starts[[run_id]]]]
    payload <- tryCatch(
      .litxr_storage_payload_as_list(json_path, fields = fields),
      error = function(e) NULL
    )
    if (is.null(payload) || !length(payload)) {
      next
    }
    for (field in intersect(fields, names(payload))) {
      value <- payload[[field]]
      if (is.list(rows[[field]])) {
        if (!is.list(value)) {
          value <- list(value)
        }
        data.table::set(rows, i = idx, j = field, value = rep(list(value[[1L]]), length(idx)))
      } else {
        if (is.list(value)) {
          value <- value[[1L]]
        }
        data.table::set(rows, i = idx, j = field, value = rep(value, length(idx)))
      }
    }
  }
  rows
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
    targets <- .litxr_embedding_target_rows_from_thin_ref_stores(cfg, collection_id)
    target_paths <- targets[match(candidate$ref_id, targets$ref_id), c("ref_id", "json_path"), with = FALSE]
    hydrated <- .litxr_hydrate_rows_from_json_paths(candidate, target_paths, fields = c("abstract", "authors_list"))
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
  if (!length(keys)) {
    return(data.table::data.table())
  }
  out <- .litxr_read_collection_records_from_json(local_path)
  if (!nrow(out)) {
    return(out)
  }
  key_cols <- intersect(c("ref_id", "source_id", "doi"), names(out))
  if (!length(key_cols)) {
    return(out[0, , drop = FALSE])
  }
  key_mask <- Reduce(`|`, lapply(key_cols, function(col) as.character(out[[col]]) %in% keys))
  out <- out[key_mask, , drop = FALSE]
  if (!nrow(out)) {
    return(out)
  }
  key <- .litxr_upsert_key(out)
  out <- out[!duplicated(key), , drop = FALSE]
  out
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
