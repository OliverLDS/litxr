.litxr_ref_arxiv_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "ref_arxiv.fst")
}

.litxr_ref_doi_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "ref_doi.fst")
}

.litxr_ref_isbn_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "ref_isbn.fst")
}

.litxr_ref_local_pending_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "ref_local_pending.fst")
}

.litxr_bare_arxiv_id <- function(ref_id = NULL, source_id = NULL, arxiv_versioned = NULL) {
  candidates <- c(source_id, arxiv_versioned, ref_id)
  candidates <- as.character(candidates)
  candidates <- candidates[!is.na(candidates) & nzchar(candidates)]
  if (!length(candidates)) {
    return(NA_character_)
  }
  for (candidate in candidates) {
    candidate <- sub("^arxiv:", "", candidate, ignore.case = TRUE)
    candidate <- sub("v[0-9]+$", "", candidate)
    candidate <- trimws(candidate)
    if (nzchar(candidate)) return(candidate)
  }
  NA_character_
}

.litxr_bare_doi <- function(ref_id = NULL, doi = NULL, source_id = NULL) {
  candidates <- c(doi, source_id, ref_id)
  candidates <- as.character(candidates)
  candidates <- candidates[!is.na(candidates) & nzchar(candidates)]
  if (!length(candidates)) {
    return(NA_character_)
  }
  for (candidate in candidates) {
    candidate <- sub("^doi:", "", candidate, ignore.case = TRUE)
    candidate <- trimws(candidate)
    if (nzchar(candidate)) return(candidate)
  }
  NA_character_
}

.litxr_arxiv_version_value <- function(ref_id = NULL, version = NULL, versioned_id = NULL, source_id = NULL) {
  candidates <- list(version, versioned_id, source_id, ref_id)
  for (candidate in candidates) {
    if (is.null(candidate)) next
    candidate <- as.character(candidate)
    candidate <- candidate[!is.na(candidate) & nzchar(candidate)]
    if (!length(candidate)) next
    candidate <- candidate[[1]]
    if (grepl("^arxiv:", candidate, ignore.case = TRUE)) {
      candidate <- sub("^arxiv:", "", candidate, ignore.case = TRUE)
    }
    m <- regexec("^(.+?)v(\\d+)$", candidate)
    reg <- regmatches(candidate, m)[[1]]
    if (length(reg) == 3L) {
      value <- suppressWarnings(as.integer(reg[[3]]))
      if (!is.na(value)) return(value)
    }
    value <- suppressWarnings(as.integer(candidate))
    if (!is.na(value)) return(value)
  }
  0L
}

.litxr_validate_arxiv_id <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) {
    stop("`arxiv_id` must be non-empty.", call. = FALSE)
  }
  norm <- vapply(x, .litxr_normalize_arxiv_ref_id, character(1))
  ok <- !is.na(norm) & grepl("^arxiv:[0-9]{4}\\.[0-9]{4,5}(v[0-9]+)?$", norm)
  if (!all(ok)) {
    bad <- unique(x[!ok])
    stop("Invalid arXiv id value(s): ", paste(bad, collapse = ", "), call. = FALSE)
  }
  invisible(TRUE)
}

.litxr_validate_doi <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) {
    stop("`doi` must be non-empty.", call. = FALSE)
  }
  norm <- vapply(x, .litxr_normalize_doi_ref_id, character(1))
  ok <- !is.na(norm) & grepl("^doi:10\\.[^[:space:]/]+/.+", norm)
  if (!all(ok)) {
    bad <- unique(x[!ok])
    stop("Invalid DOI value(s): ", paste(bad, collapse = ", "), call. = FALSE)
  }
  invisible(TRUE)
}

.litxr_validate_entity_id <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) {
    stop("`entity_id` must be non-empty.", call. = FALSE)
  }
  invisible(TRUE)
}

.litxr_route_ref_id <- function(ref_id) {
  ref_id <- as.character(ref_id)
  ref_id <- ref_id[[1]]
  if (is.na(ref_id)) {
    return(list(key_type = "local_pending", key_value = NA_character_, ref_id = NA_character_))
  }
  ref_id <- trimws(ref_id)
  if (!nzchar(ref_id)) {
    return(list(key_type = "local_pending", key_value = NA_character_, ref_id = NA_character_))
  }

  is_doi_like <- grepl("^doi:", ref_id, ignore.case = TRUE) ||
    grepl("^https?://(dx\\.)?doi\\.org/", ref_id, ignore.case = TRUE) ||
    grepl("^10\\.", ref_id)
  if (is_doi_like) {
    doi_ref <- .litxr_normalize_doi_ref_id(ref_id)
  } else {
    doi_ref <- NA_character_
  }
  if (!is.na(doi_ref) && nzchar(doi_ref)) {
    return(list(key_type = "doi", key_value = doi_ref, ref_id = doi_ref))
  }

  arxiv_id <- .litxr_normalize_arxiv_ref_id(ref_id)
  if (!is.na(arxiv_id) && nzchar(arxiv_id)) {
    return(list(key_type = "arxiv_id", key_value = arxiv_id, ref_id = arxiv_id))
  }

  list(key_type = "local_pending", key_value = ref_id, ref_id = ref_id)
}

.litxr_route_ref_ids <- function(ref_ids) {
  ref_ids <- as.character(ref_ids)
  if (!length(ref_ids)) {
    return(data.table::data.table(
      ref_id = character(),
      key_type = character(),
      key_value = character()
    ))
  }
  routed <- lapply(ref_ids, .litxr_route_ref_id)
  data.table::data.table(
    ref_id = vapply(routed, `[[`, character(1), "ref_id"),
    key_type = vapply(routed, `[[`, character(1), "key_type"),
    key_value = vapply(routed, `[[`, character(1), "key_value")
  )
}

.litxr_normalized_payload_projection <- function(records, key_type = c("arxiv_id", "doi", "local_pending"), arxiv_collection_ids = NULL) {
  key_type <- match.arg(key_type)
  records <- data.table::as.data.table(records)
  if (!nrow(records)) {
    out <- switch(
      key_type,
      arxiv_id = data.table::data.table(arxiv_id = character()),
      doi = data.table::data.table(doi = character()),
      local_pending = data.table::data.table(ref_id = character())
    )
    return(out)
  }

  if (identical(key_type, "arxiv_id")) {
    arxiv_rows <- grepl("^arxiv:", as.character(records$ref_id))
    if (!is.null(arxiv_collection_ids) && length(arxiv_collection_ids) && "collection_id" %in% names(records)) {
      arxiv_rows <- arxiv_rows & as.character(records$collection_id) %in% arxiv_collection_ids
    } else if ("collection_type" %in% names(records)) {
      arxiv_rows <- arxiv_rows & as.character(records$collection_type) == "arxiv_category"
    }
    idx <- which(arxiv_rows)
    if (!length(idx)) {
      return(data.table::data.table(arxiv_id = character()))
    }
    arxiv_rows_dt <- data.table::data.table(
      arxiv_id = vapply(idx, function(i) {
      .litxr_bare_arxiv_id(
        ref_id = if ("ref_id" %in% names(records)) records$ref_id[[i]] else NULL,
        source_id = if ("source_id" %in% names(records)) records$source_id[[i]] else NULL,
        arxiv_versioned = if ("arxiv_id_versioned" %in% names(records)) records$arxiv_id_versioned[[i]] else NULL
      )
    }, character(1)),
      arxiv_version = vapply(idx, function(i) {
        .litxr_arxiv_version_value(
          ref_id = if ("ref_id" %in% names(records)) records$ref_id[[i]] else NULL,
          version = if ("arxiv_version" %in% names(records)) records$arxiv_version[[i]] else NULL,
          versioned_id = if ("arxiv_id_versioned" %in% names(records)) records$arxiv_id_versioned[[i]] else NULL,
          source_id = if ("source_id" %in% names(records)) records$source_id[[i]] else NULL
        )
      }, integer(1))
    )
    arxiv_rows_dt <- arxiv_rows_dt[!is.na(arxiv_rows_dt$arxiv_id) & nzchar(arxiv_rows_dt$arxiv_id), ]
    if (!nrow(arxiv_rows_dt)) {
      return(data.table::data.table(arxiv_id = character()))
    }
    data.table::setorder(arxiv_rows_dt, arxiv_id, -arxiv_version)
    arxiv_rows_dt <- arxiv_rows_dt[!duplicated(arxiv_rows_dt$arxiv_id), ]
    arxiv_rows_dt$arxiv_version <- NULL
    return(data.table::data.table(arxiv_id = arxiv_rows_dt$arxiv_id))
  }

  if (identical(key_type, "doi")) {
    doi_rows <- if ("doi" %in% names(records)) !is.na(records$doi) & nzchar(records$doi) else grepl("^doi:", as.character(records$ref_id))
    if (!is.null(arxiv_collection_ids) && length(arxiv_collection_ids) && "collection_id" %in% names(records)) {
      doi_rows <- doi_rows & !(as.character(records$collection_id) %in% arxiv_collection_ids)
    }
    idx <- which(doi_rows)
    if (!length(idx)) {
      return(data.table::data.table(doi = character()))
    }
    doi <- vapply(idx, function(i) {
      .litxr_bare_doi(
        ref_id = if ("ref_id" %in% names(records)) records$ref_id[[i]] else NULL,
        doi = if ("doi" %in% names(records)) records$doi[[i]] else NULL,
        source_id = if ("source_id" %in% names(records)) records$source_id[[i]] else NULL
      )
    }, character(1))
    keep <- !is.na(doi) & nzchar(doi)
    doi <- doi[keep]
    if (length(doi)) {
      dup <- duplicated(doi)
      if (any(dup)) {
        stop("Duplicate DOI id(s) found while rebuilding thin DOI store: ", paste(unique(doi[dup]), collapse = ", "), call. = FALSE)
      }
    }
    return(data.table::data.table(doi = doi))
  }

  pending <- data.table::data.table(
    ref_id = if ("ref_id" %in% names(records)) as.character(records$ref_id) else rep(NA_character_, nrow(records))
  )
  pending <- pending[!is.na(pending$ref_id) & nzchar(pending$ref_id), , drop = FALSE]
  if (nrow(pending) > 0L) {
    pending <- pending[!duplicated(pending$ref_id), , drop = FALSE]
  }
  pending
}

.litxr_ref_entity_resolution_map <- function(cfg, entities = NULL, ref_ids = NULL, entity_ids = NULL) {
  identity_map <- data.table::as.data.table(litxr_read_ref_identity_map(cfg))
  if (!nrow(identity_map)) {
    return(data.table::data.table(
      ref_id = character(),
      entity_id = character()
    ))
  }

  identity_map <- identity_map[!is.na(identity_map$ref_id) & nzchar(identity_map$ref_id), ]
  if (!nrow(identity_map)) {
    return(data.table::data.table(
      ref_id = character(),
      entity_id = character()
    ))
  }

  if (!is.null(ref_ids) && length(ref_ids)) {
    ref_ids <- unique(as.character(ref_ids))
    ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(ref_ids)]
    if (length(ref_ids)) {
      identity_map <- identity_map[identity_map$ref_id %in% ref_ids, ]
    }
  }
  if (!is.null(entity_ids) && length(entity_ids) && nrow(identity_map)) {
    entity_ids <- unique(as.character(entity_ids))
    entity_ids <- entity_ids[!is.na(entity_ids) & nzchar(entity_ids)]
    if (length(entity_ids)) {
      identity_map <- identity_map[identity_map$entity_id %in% entity_ids, ]
    }
  }

  if (!nrow(identity_map)) {
    return(data.table::data.table(
      ref_id = character(),
      entity_id = character()
    ))
  }

  rows <- identity_map[, c("ref_id", "entity_id"), with = FALSE]
  rows <- rows[!duplicated(paste(rows$ref_id, rows$entity_id, sep = "\r")), ]
  rows
}

.litxr_write_scaffold_table <- function(path, records, key_cols) {
  records <- data.table::as.data.table(records)
  if (!nrow(records)) {
    fst::write_fst(as.data.frame(records), path)
    return(invisible(path))
  }

  if (!missing(key_cols) && length(key_cols) && !all(as.character(key_cols) %in% names(records))) {
    stop("Missing key column(s) for scaffold write: ", paste(setdiff(as.character(key_cols), names(records)), collapse = ", "), call. = FALSE)
  }

  fst::write_fst(as.data.frame(records), path)
  invisible(path)
}

.litxr_refresh_normalized_reference_scaffold <- function(cfg, records = NULL, refresh_entity_indexes = TRUE, refs = NULL, identity_map = NULL) {
  .litxr_ensure_project_index_dir(cfg)
  if (is.null(records)) {
    records <- if (!is.null(refs)) refs else .litxr_authoritative_project_records(cfg)
  }
  records <- data.table::as.data.table(records)

  arxiv_collection_ids <- .litxr_collection_ids_by_remote_channel(cfg, "arxiv")
  arxiv_records <- if (nrow(records) && "collection_id" %in% names(records)) {
    records[grepl("^arxiv:", as.character(records$ref_id)) & as.character(records$collection_id) %in% arxiv_collection_ids, ]
  } else if (nrow(records) && "collection_type" %in% names(records)) {
    records[grepl("^arxiv:", as.character(records$ref_id)) & as.character(records$collection_type) == "arxiv_category", ]
  } else {
    records[grepl("^arxiv:", as.character(records$ref_id)), ]
  }
  arxiv_rows <- .litxr_normalized_payload_projection(arxiv_records, key_type = "arxiv_id", arxiv_collection_ids = arxiv_collection_ids)
  doi_records <- if (nrow(records) && "collection_id" %in% names(records)) {
    records[!(as.character(records$collection_id) %in% arxiv_collection_ids), ]
  } else {
    records
  }
  doi_rows <- .litxr_normalized_payload_projection(doi_records, key_type = "doi", arxiv_collection_ids = arxiv_collection_ids)
  pending_rows <- .litxr_normalized_payload_projection(records, key_type = "local_pending")

  .litxr_write_scaffold_table(.litxr_ref_arxiv_path(cfg), arxiv_rows, key_cols = "arxiv_id")
  .litxr_write_scaffold_table(.litxr_ref_doi_path(cfg), doi_rows, key_cols = "doi")
  .litxr_write_scaffold_table(.litxr_ref_local_pending_path(cfg), pending_rows, key_cols = "ref_id")

  invisible(list(
    ref_arxiv = .litxr_ref_arxiv_path(cfg),
    ref_doi = .litxr_ref_doi_path(cfg),
    ref_local_pending = .litxr_ref_local_pending_path(cfg)
  ))
}

.litxr_read_scaffold_table_safe <- function(path) {
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  tryCatch(
    data.table::as.data.table(fst::read_fst(path, as.data.table = TRUE)),
    error = function(e) data.table::data.table()
  )
}

.litxr_read_normalized_reference_rows_by_keys <- function(cfg, keys, columns = NULL) {
  keys <- unique(as.character(unlist(keys, use.names = FALSE)))
  keys <- keys[!is.na(keys) & nzchar(keys)]
  if (!length(keys)) {
    return(data.table::data.table())
  }

  cfg <- if (is.character(cfg)) litxr_read_config(cfg) else cfg
  if (is.null(cfg)) cfg <- litxr_read_config()
  out <- .litxr_authoritative_project_records(cfg)
  if (!nrow(out)) {
    return(data.table::data.table())
  }
  key_cols <- intersect(c("ref_id", "source_id", "doi"), names(out))
  if (length(key_cols)) {
    key_mask <- Reduce(`|`, lapply(key_cols, function(col) as.character(out[[col]]) %in% keys))
    out <- out[key_mask, ]
  } else {
    out <- out[0, ]
  }
  if (!nrow(out)) {
    return(out)
  }
  out <- out[!duplicated(as.character(out$ref_id)), ]
  if (!is.null(columns) && length(columns)) {
    keep <- intersect(unique(as.character(columns)), names(out))
    out <- out[, keep, with = FALSE]
  }
  out
}

.litxr_normalized_duplicate_identity_conflicts <- function(entities) {
  entities <- data.table::as.data.table(entities)
  if (!nrow(entities) || !("ref_id" %in% names(entities))) {
    return(data.table::data.table(
      id_type = character(),
      id_value = character(),
      n_entities = integer(),
      entity_ids = character()
    ))
  }
  ref_ids <- as.character(entities$ref_id)
  ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(ref_ids)]
  entity_ids <- as.character(entities$entity_id)[!is.na(as.character(entities$ref_id)) & nzchar(as.character(entities$ref_id))]
  if (!length(ref_ids)) {
    return(data.table::data.table(
      id_type = character(),
      id_value = character(),
      n_entities = integer(),
      entity_ids = character()
    ))
  }
  grouped <- split(entity_ids, ref_ids)
  counts <- vapply(grouped, function(x) data.table::uniqueN(x), integer(1))
  keep <- counts > 1L
  if (!any(keep)) {
    return(data.table::data.table(
      id_type = character(),
      id_value = character(),
      n_entities = integer(),
      entity_ids = character()
    ))
  }
  data.table::data.table(
    id_type = "ref_id",
    id_value = names(counts)[keep],
    n_entities = unname(counts[keep]),
    entity_ids = vapply(grouped[names(counts)[keep]], function(x) paste(sort(unique(x)), collapse = ", "), character(1))
  )
}

.litxr_normalized_orphan_payload_rows <- function(payload, entity_ref_ids, id_type = c("arxiv", "doi")) {
  id_type <- match.arg(id_type)
  payload <- data.table::as.data.table(payload)
  if (!nrow(payload)) {
    return(payload[0, ])
  }
  if (!("ref_id" %in% names(payload))) {
    return(payload[0, ])
  }
  entity_ref_ids <- unique(as.character(entity_ref_ids))
  entity_ref_ids <- entity_ref_ids[!is.na(entity_ref_ids) & nzchar(entity_ref_ids)]
  keep <- as.character(payload$ref_id)
  keep <- keep[!is.na(keep) & nzchar(keep)]
  payload[!(payload$ref_id %in% entity_ref_ids), ]
}

.litxr_normalized_authoritative_state_audit <- function(cfg) {
  identity_map <- data.table::as.data.table(litxr_read_ref_identity_map(cfg))
  entity_ref_ids <- unique(as.character(identity_map$ref_id))
  entity_ref_ids <- entity_ref_ids[!is.na(entity_ref_ids) & nzchar(entity_ref_ids)]

  arxiv_payload <- .litxr_read_scaffold_table_safe(.litxr_ref_arxiv_path(cfg))
  doi_payload <- .litxr_read_scaffold_table_safe(.litxr_ref_doi_path(cfg))
  pending_payload <- .litxr_read_scaffold_table_safe(.litxr_ref_isbn_path(cfg))
  compatibility_state <- .litxr_audit_reference_cache_state(cfg)

  list(
    duplicate_identity_conflicts = .litxr_normalized_duplicate_identity_conflicts(identity_map),
    orphan_arxiv_payload_rows = .litxr_normalized_orphan_payload_rows(arxiv_payload, entity_ref_ids, id_type = "arxiv"),
    orphan_doi_payload_rows = .litxr_normalized_orphan_payload_rows(doi_payload, entity_ref_ids, id_type = "doi"),
    unresolved_isbn_rows = pending_payload,
    compatibility_runtime_output_stale = compatibility_state$project_reference_cache
  )
}
