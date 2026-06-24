.litxr_ref_entities_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "ref_entities.fst")
}

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

.litxr_ref_entities_projection <- function(cfg, refs = NULL, identity_map = NULL) {
  if (is.null(refs)) {
    refs <- .litxr_authoritative_project_records(cfg)
  }
  if (!nrow(refs)) {
    return(data.table::data.table(
      entity_id = character(),
      arxiv_id = character(),
      doi = character(),
      primary_ref_id = character(),
      preferred_citation_ref_id = character(),
      display_title = character(),
      updated_at = character()
    ))
  }

  if (is.null(identity_map)) {
    identity_map <- data.table::data.table()
  }
  if (!nrow(identity_map)) {
    return(data.table::data.table(
      entity_id = character(),
      arxiv_id = character(),
      doi = character(),
      primary_ref_id = character(),
      preferred_citation_ref_id = character(),
      display_title = character(),
      updated_at = character()
    ))
  }

  refs <- data.table::as.data.table(refs)
  identity_map <- data.table::as.data.table(identity_map)
  identity_view <- data.table::data.table(
    ref_id = as.character(identity_map$ref_id),
    entity_id = as.character(identity_map$entity_id),
    is_primary_ref_id = as.logical(identity_map$is_primary_ref_id),
    is_published_form = as.logical(identity_map$is_published_form)
  )
  data.table::setkey(identity_view, ref_id)
  ref_hit <- match(as.character(refs$ref_id), identity_view$ref_id)
  rows <- data.table::copy(refs)
  rows$entity_id <- identity_view$entity_id[ref_hit]
  rows$is_primary_ref_id <- identity_view$is_primary_ref_id[ref_hit]
  rows$is_published_form <- identity_view$is_published_form[ref_hit]
  rows <- rows[!is.na(rows$entity_id) & nzchar(rows$entity_id), ]
  if (!nrow(rows)) {
    return(data.table::data.table(
      entity_id = character(),
      arxiv_id = character(),
      doi = character(),
      primary_ref_id = character(),
      preferred_citation_ref_id = character(),
      display_title = character(),
      updated_at = character()
    ))
  }

  rows$primary_rank <- as.integer(rows$is_primary_ref_id %in% TRUE)
  rows$preferred_rank <- as.integer(rows$is_published_form %in% TRUE)
  rows$arxiv_surface <- ifelse(grepl("^arxiv:", rows$ref_id), rows$ref_id, NA_character_)
  rows$doi_surface <- ifelse(grepl("^doi:", rows$ref_id), sub("^doi:", "", rows$ref_id, ignore.case = TRUE), NA_character_)

  primary_rows <- data.table::copy(rows)
  data.table::setorder(primary_rows, entity_id, -primary_rank, ref_id)
  primary_rows <- primary_rows[!duplicated(primary_rows$entity_id), ]

  preferred_rows <- data.table::copy(rows)
  data.table::setorder(preferred_rows, entity_id, -preferred_rank, ref_id)
  preferred_rows <- preferred_rows[!duplicated(preferred_rows$entity_id), ]

  entity_surface_source <- data.table::data.table(
    entity_id = as.character(rows$entity_id),
    arxiv_surface = as.character(rows$arxiv_surface),
    doi_surface = as.character(rows$doi_surface)
  )
  if (!nrow(entity_surface_source)) {
    entity_surface <- data.table::data.table(
      entity_id = character(),
      arxiv_id = character(),
      doi = character()
    )
  } else {
    first_non_empty <- function(x) {
      x <- as.character(x)
      x <- x[!is.na(x) & nzchar(x)]
      if (length(x)) x[[1L]] else NA_character_
    }
    entity_ids <- unique(entity_surface_source$entity_id)
    entity_surface <- data.table::data.table(
      entity_id = entity_ids,
      arxiv_id = vapply(entity_ids, function(id) first_non_empty(entity_surface_source$arxiv_surface[entity_surface_source$entity_id == id]), character(1)),
      doi = vapply(entity_ids, function(id) first_non_empty(entity_surface_source$doi_surface[entity_surface_source$entity_id == id]), character(1))
    )
  }

  out <- merge(
    primary_rows,
    data.table::data.table(
      entity_id = as.character(preferred_rows$entity_id),
      preferred_citation_ref_id = as.character(preferred_rows$ref_id),
      preferred_title = as.character(preferred_rows$title)
    ),
    by = "entity_id",
    all.x = TRUE,
    sort = FALSE
  )
  out <- merge(out, entity_surface, by = "entity_id", all.x = TRUE, sort = FALSE)
  out <- data.table::as.data.table(out)
  out$primary_ref_id <- as.character(out$ref_id)
  out$preferred_citation_ref_id <- as.character(ifelse(
    is.na(out$preferred_citation_ref_id) | !nzchar(out$preferred_citation_ref_id),
    out$ref_id,
    out$preferred_citation_ref_id
  ))
  out$display_title <- as.character(ifelse(
    is.na(out$preferred_title) | !nzchar(out$preferred_title),
    ifelse(is.na(out$title) | !nzchar(out$title), NA_character_, out$title),
    out$preferred_title
  ))
  out$updated_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  scalar_chr <- function(x, n) {
    if (is.null(x) || !length(x)) {
      return(rep(NA_character_, n))
    }
    if (is.list(x)) {
      vapply(x, function(y) {
        y <- as.character(y)
        if (length(y)) y[[1L]] else NA_character_
      }, character(1))
    } else {
      x <- as.character(x)
      if (length(x) == n) x else rep(x[[1L]], n)
    }
  }
  n_out <- nrow(out)
  out$entity_id <- scalar_chr(out$entity_id, n_out)
  out$arxiv_id <- scalar_chr(out$arxiv_id, n_out)
  out$doi <- scalar_chr(out$doi, n_out)
  out$primary_ref_id <- scalar_chr(out$primary_ref_id, n_out)
  out$preferred_citation_ref_id <- scalar_chr(out$preferred_citation_ref_id, n_out)
  out$display_title <- scalar_chr(out$display_title, n_out)
  out$updated_at <- scalar_chr(out$updated_at, n_out)

  if (!nrow(out)) {
    return(data.table::data.table(
      entity_id = character(),
      arxiv_id = character(),
      doi = character(),
      primary_ref_id = character(),
      preferred_citation_ref_id = character(),
      display_title = character(),
      updated_at = character()
    ))
  }
  out[!is.na(out$arxiv_id) | !is.na(out$doi), ]
}

.litxr_write_scaffold_table <- function(path, records, key_cols) {
  key_cols <- as.character(key_cols)
  records <- data.table::as.data.table(records)
  if (!nrow(records)) {
    fst::write_fst(as.data.frame(records), path)
    return(invisible(path))
  }

  if (!file.exists(path)) {
    fst::write_fst(as.data.frame(records), path)
    return(invisible(path))
  }

  existing <- tryCatch(fst::read_fst(path, as.data.table = TRUE), error = function(e) data.table::data.table())
  if (!nrow(existing)) {
    merged <- records
  } else {
    if (!all(key_cols %in% names(records))) {
      stop("Missing key column(s) for scaffold upsert: ", paste(setdiff(key_cols, names(records)), collapse = ", "), call. = FALSE)
    }
    existing <- data.table::as.data.table(existing)
    merged <- data.table::rbindlist(list(existing, records), fill = TRUE)
    merged_key <- do.call(paste, c(as.list(merged[, key_cols, with = FALSE]), sep = "\r"))
    merged <- merged[!duplicated(merged_key, fromLast = TRUE), ]
  }

  fst::write_fst(as.data.frame(merged), path)
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

  if (isTRUE(refresh_entity_indexes)) {
    entities <- .litxr_ref_entities_projection(cfg, refs = if (!is.null(refs)) refs else NULL, identity_map = identity_map)
    .litxr_write_scaffold_table(.litxr_ref_entities_path(cfg), entities, key_cols = "entity_id")
  }

  invisible(list(
    ref_entities = .litxr_ref_entities_path(cfg),
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
