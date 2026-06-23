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

  keep <- .litxr_reference_projection(records)
  route <- .litxr_route_ref_ids(keep$ref_id)
  keep[["key_type"]] <- route$key_type
  keep[["key_value"]] <- route$key_value
  keep <- keep[keep$key_type == key_type, ]
  keep
}

.litxr_ref_entities_projection <- function(cfg, refs = NULL, aliases = NULL) {
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

  if (is.null(aliases)) {
    aliases <- .litxr_build_ref_aliases_index(cfg, refs = refs)
  }
  if (!nrow(aliases)) {
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

  alias_view <- data.table::data.table(
    ref_id = aliases$ref_id,
    entity_id = aliases$entity_id,
    is_primary_ref_id = aliases$is_primary_ref_id,
    is_published_form = aliases$is_published_form
  )
  rows <- merge(refs, alias_view, by = "ref_id", all.x = TRUE, sort = FALSE)
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

  entity_ids <- unique(as.character(rows$entity_id))
  out <- data.table::rbindlist(lapply(entity_ids, function(entity_id) {
    entity_rows <- rows[rows$entity_id == entity_id, ]
    primary_idx <- which(entity_rows$is_primary_ref_id %in% TRUE)
    primary_row <- if (length(primary_idx)) entity_rows[primary_idx[[1]], ] else entity_rows[1L, ]
    preferred_idx <- which(entity_rows$is_published_form %in% TRUE)
    preferred_row <- if (length(preferred_idx)) entity_rows[preferred_idx[[1]], ] else primary_row
    arxiv_id <- entity_rows$ref_id[grepl("^arxiv:", entity_rows$ref_id)]
    doi_id <- entity_rows$ref_id[grepl("^doi:", entity_rows$ref_id)]
    if (!length(arxiv_id) && !length(doi_id)) {
      return(NULL)
    }
    data.table::data.table(
      entity_id = entity_id,
      arxiv_id = if (length(arxiv_id)) as.character(arxiv_id[[1]]) else NA_character_,
      doi = if (length(doi_id)) sub("^doi:", "", as.character(doi_id[[1]]), ignore.case = TRUE) else NA_character_,
      primary_ref_id = as.character(primary_row$ref_id[[1]] %||% NA_character_),
      preferred_citation_ref_id = as.character(preferred_row$ref_id[[1]] %||% NA_character_),
      display_title = as.character(preferred_row$title[[1]] %||% primary_row$title[[1]] %||% NA_character_),
      updated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
    )
  }), fill = TRUE)

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

.litxr_refresh_normalized_reference_scaffold <- function(cfg, records = NULL, refresh_entity_indexes = TRUE, refs = NULL, aliases = NULL) {
  .litxr_ensure_project_index_dir(cfg)
  if (is.null(records)) {
    records <- if (!is.null(refs)) refs else .litxr_read_project_references_index(cfg)
  }
  records <- data.table::as.data.table(records)

  arxiv_rows <- .litxr_normalized_payload_projection(records, key_type = "arxiv_id")
  doi_rows <- .litxr_normalized_payload_projection(records, key_type = "doi")
  pending_rows <- .litxr_normalized_payload_projection(records, key_type = "local_pending")

  if (nrow(arxiv_rows)) {
    .litxr_write_scaffold_table(.litxr_ref_arxiv_path(cfg), arxiv_rows, key_cols = "ref_id")
  } else if (!file.exists(.litxr_ref_arxiv_path(cfg))) {
    fst::write_fst(as.data.frame(arxiv_rows), .litxr_ref_arxiv_path(cfg))
  }
  if (nrow(doi_rows)) {
    .litxr_write_scaffold_table(.litxr_ref_doi_path(cfg), doi_rows, key_cols = "ref_id")
  } else if (!file.exists(.litxr_ref_doi_path(cfg))) {
    fst::write_fst(as.data.frame(doi_rows), .litxr_ref_doi_path(cfg))
  }
  if (nrow(pending_rows)) {
    .litxr_write_scaffold_table(.litxr_ref_local_pending_path(cfg), pending_rows, key_cols = "ref_id")
  } else if (!file.exists(.litxr_ref_local_pending_path(cfg))) {
    fst::write_fst(as.data.frame(pending_rows), .litxr_ref_local_pending_path(cfg))
  }

  if (isTRUE(refresh_entity_indexes)) {
    entities <- .litxr_ref_entities_projection(cfg, refs = if (!is.null(refs)) refs else NULL, aliases = aliases)
    .litxr_write_scaffold_table(.litxr_ref_entities_path(cfg), entities, key_cols = "entity_id")
  }

  invisible(list(
    ref_entities = .litxr_ref_entities_path(cfg),
    ref_arxiv = .litxr_ref_arxiv_path(cfg),
    ref_doi = .litxr_ref_doi_path(cfg),
    ref_local_pending = .litxr_ref_local_pending_path(cfg)
  ))
}
