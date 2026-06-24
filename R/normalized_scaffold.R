.litxr_ref_arxiv_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "ref_arxiv.fst")
}

.litxr_ref_doi_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "ref_doi.fst")
}

.litxr_ref_local_pending_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "ref_local_pending.fst")
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

.litxr_normalized_payload_projection <- function(records, key_type = c("arxiv_id", "doi", "local_pending")) {
  key_type <- match.arg(key_type)
  records <- data.table::as.data.table(records)
  if (!nrow(records)) {
    out <- data.table::data.table(ref_id = character(), key_type = character(), key_value = character())
    return(out)
  }

  keep_cols <- intersect(
    .litxr_reference_projection_columns(),
    names(records)
  )
  keep <- data.table::copy(records)[, keep_cols, with = FALSE]
  route <- .litxr_route_ref_ids(keep$ref_id)
  keep[["key_type"]] <- route$key_type
  keep[["key_value"]] <- route$key_value
  keep <- keep[keep$key_type == key_type, ]
  keep
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

  arxiv_rows <- .litxr_normalized_payload_projection(records, key_type = "arxiv_id")
  doi_rows <- .litxr_normalized_payload_projection(records, key_type = "doi")
  pending_rows <- .litxr_normalized_payload_projection(records, key_type = "local_pending")

  if (nrow(arxiv_rows)) {
    .litxr_write_scaffold_table(.litxr_ref_arxiv_path(cfg), arxiv_rows, key_cols = "ref_id")
  }
  if (nrow(doi_rows)) {
    .litxr_write_scaffold_table(.litxr_ref_doi_path(cfg), doi_rows, key_cols = "ref_id")
  }
  if (nrow(pending_rows)) {
    .litxr_write_scaffold_table(.litxr_ref_local_pending_path(cfg), pending_rows, key_cols = "ref_id")
  }

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

  payloads <- list(
    .litxr_read_scaffold_table_safe(.litxr_ref_arxiv_path(cfg)),
    .litxr_read_scaffold_table_safe(.litxr_ref_doi_path(cfg)),
    .litxr_read_scaffold_table_safe(.litxr_ref_local_pending_path(cfg))
  )
  payloads <- payloads[vapply(payloads, nrow, integer(1L)) > 0L]
  if (!length(payloads)) {
    return(data.table::data.table())
  }

  matched <- lapply(payloads, function(payload) {
    if (!("ref_id" %in% names(payload))) {
      return(payload[0, ])
    }
    hit <- as.character(payload$ref_id) %in% keys
    if (!any(hit)) {
      return(payload[0, ])
    }
    payload[hit, ]
  })
  matched <- matched[vapply(matched, nrow, integer(1L)) > 0L]
  if (!length(matched)) {
    return(data.table::data.table())
  }

  out <- data.table::rbindlist(matched, fill = TRUE)
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
  pending_payload <- .litxr_read_scaffold_table_safe(.litxr_ref_local_pending_path(cfg))
  compatibility_state <- .litxr_audit_reference_cache_state(cfg)

  list(
    duplicate_identity_conflicts = .litxr_normalized_duplicate_identity_conflicts(identity_map),
    orphan_arxiv_payload_rows = .litxr_normalized_orphan_payload_rows(arxiv_payload, entity_ref_ids, id_type = "arxiv"),
    orphan_doi_payload_rows = .litxr_normalized_orphan_payload_rows(doi_payload, entity_ref_ids, id_type = "doi"),
    unresolved_local_pending_rows = pending_payload,
    compatibility_runtime_output_stale = compatibility_state$project_reference_cache
  )
}
