.litxr_ensure_journal_dirs <- function(local_path) {
  paths <- .litxr_journal_paths(local_path)
  for (path in unname(paths[c("root", "json", "md", "llm")])) {
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

.litxr_record_slugs <- function(ref_id, doi = NULL) {
  ref_id <- as.character(ref_id)
  if (is.null(doi)) {
    doi <- rep(NA_character_, length(ref_id))
  } else {
    doi <- as.character(doi)
    if (length(doi) != length(ref_id)) {
      doi <- rep(doi[[1L]], length(ref_id))
    }
  }
  core <- ifelse(!is.na(doi) & nzchar(doi), doi, ref_id)
  core <- tolower(core)
  core <- gsub("[^a-z0-9]+", "_", core)
  core <- gsub("^_+|_+$", "", core)
  core[!nzchar(core)] <- "record"
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

  if (!length(score_fields)) {
    return(integer(nrow(records)))
  }

  scores <- integer(nrow(records))
  for (field in score_fields) {
    value <- records[[field]]
    if (is.list(value)) {
      scores <- scores + vapply(value, function(x) !.litxr_nullish(x), logical(1))
    } else if (inherits(value, "POSIXct")) {
      scores <- scores + !is.na(value)
    } else {
      scores <- scores + !(is.na(value) | !nzchar(as.character(value)))
    }
  }
  scores
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

.litxr_column_missing_mask <- function(x) {
  if (is.list(x)) {
    return(vapply(x, .litxr_nullish, logical(1)))
  }
  if (inherits(x, c("POSIXct", "POSIXt"))) {
    return(is.na(x))
  }
  if (is.character(x)) {
    return(is.na(x) | !nzchar(x))
  }
  if (is.factor(x)) {
    x <- as.character(x)
    return(is.na(x) | !nzchar(x))
  }
  is.na(x)
}

.litxr_missing_column_like <- function(field, template, n) {
  if (identical(field, "authors_list")) {
    return(rep(list(character()), n))
  }
  if (identical(field, "raw_entry")) {
    return(rep(list(NULL), n))
  }
  if (is.list(template)) {
    return(rep(list(NULL), n))
  }
  if (inherits(template, c("POSIXct", "POSIXt"))) {
    return(rep(as.POSIXct(NA, tz = "UTC"), n))
  }
  if (is.integer(template)) {
    return(rep(NA_integer_, n))
  }
  if (is.numeric(template)) {
    return(rep(NA_real_, n))
  }
  if (is.logical(template)) {
    return(rep(NA, n))
  }
  rep(NA_character_, n)
}

.litxr_column_equal_mask <- function(existing, incoming) {
  if (is.list(existing) || is.list(incoming)) {
    return(vapply(seq_along(existing), function(i) {
      .litxr_scalar_equal(existing[[i]], incoming[[i]])
    }, logical(1)))
  }

  if (inherits(existing, c("POSIXct", "POSIXt")) || inherits(incoming, c("POSIXct", "POSIXt"))) {
    existing <- as.character(existing)
    incoming <- as.character(incoming)
  } else if (is.factor(existing)) {
    existing <- as.character(existing)
  } else if (is.factor(incoming)) {
    incoming <- as.character(incoming)
  }

  equal <- existing == incoming
  equal[is.na(existing) & is.na(incoming)] <- TRUE
  equal[is.na(equal)] <- FALSE
  equal
}

.litxr_upsert_match_masks <- function(existing_rows, incoming_rows, key) {
  fields <- setdiff(union(names(existing_rows), names(incoming_rows)), c(key))
  if (!length(fields) || !nrow(existing_rows)) {
    return(list(
      identical = logical(nrow(existing_rows)),
      simple_coalesce = logical(nrow(existing_rows)),
      conflict = logical(nrow(existing_rows))
    ))
  }

  identical_mask <- rep(TRUE, nrow(existing_rows))
  conflict_mask <- rep(FALSE, nrow(existing_rows))
  local_priority_fields <- c("note")
  conflict_ignored_fields <- c("authors_list", "raw_entry")

  for (field in fields) {
    existing_col <- if (field %in% names(existing_rows)) existing_rows[[field]] else .litxr_missing_column_like(field, incoming_rows[[field]], nrow(existing_rows))
    incoming_col <- if (field %in% names(incoming_rows)) incoming_rows[[field]] else .litxr_missing_column_like(field, existing_rows[[field]], nrow(existing_rows))
    existing_missing <- .litxr_column_missing_mask(existing_col)
    incoming_missing <- .litxr_column_missing_mask(incoming_col)
    equal_mask <- .litxr_column_equal_mask(existing_col, incoming_col)

    identical_mask <- identical_mask & equal_mask

    if (field %in% local_priority_fields || field %in% conflict_ignored_fields || is.list(existing_col) || is.list(incoming_col)) {
      next
    }

    version_order <- rep(0L, nrow(existing_rows))
    if (
      "source" %in% names(existing_rows) &&
      "source" %in% names(incoming_rows) &&
      "arxiv_version" %in% names(existing_rows) &&
      "arxiv_version" %in% names(incoming_rows)
    ) {
      existing_source <- existing_rows[["source"]]
      incoming_source <- incoming_rows[["source"]]
      existing_version <- suppressWarnings(as.integer(existing_rows[["arxiv_version"]]))
      incoming_version <- suppressWarnings(as.integer(incoming_rows[["arxiv_version"]]))
      arxiv_rows <- !.litxr_column_missing_mask(existing_source) &
        !.litxr_column_missing_mask(incoming_source) &
        as.character(existing_source) == "arxiv" &
        as.character(incoming_source) == "arxiv"
      version_order[arxiv_rows] <- ifelse(
        !is.na(existing_version[arxiv_rows]) &
          !is.na(incoming_version[arxiv_rows]) &
          incoming_version[arxiv_rows] > existing_version[arxiv_rows],
        1L,
        ifelse(
          !is.na(existing_version[arxiv_rows]) &
            !is.na(incoming_version[arxiv_rows]) &
            incoming_version[arxiv_rows] < existing_version[arxiv_rows],
          -1L,
          0L
        )
      )
    }

    conflict_mask <- conflict_mask | (!existing_missing & !incoming_missing & !equal_mask & version_order >= 0L)
  }

  list(
    identical = identical_mask,
    simple_coalesce = !identical_mask & !conflict_mask,
    conflict = conflict_mask
  )
}

.litxr_merge_record_rows_vectorized <- function(existing_rows, incoming_rows, key, prefer_existing_rows) {
  if (!nrow(existing_rows)) {
    return(incoming_rows)
  }
  out <- data.table::copy(existing_rows)
  fields <- setdiff(union(names(existing_rows), names(incoming_rows)), c(key))

  for (field in fields) {
    existing_col <- if (field %in% names(existing_rows)) existing_rows[[field]] else .litxr_missing_column_like(field, incoming_rows[[field]], nrow(existing_rows))
    incoming_col <- if (field %in% names(incoming_rows)) incoming_rows[[field]] else .litxr_missing_column_like(field, existing_rows[[field]], nrow(existing_rows))

    if (field %in% c("note")) {
      use_existing <- !.litxr_column_missing_mask(existing_col)
    } else {
      existing_missing <- .litxr_column_missing_mask(existing_col)
      incoming_missing <- .litxr_column_missing_mask(incoming_col)
      use_existing <- (prefer_existing_rows & !existing_missing) | (!prefer_existing_rows & incoming_missing)
    }

    result <- incoming_col
    if (any(use_existing)) {
      result[use_existing] <- existing_col[use_existing]
    }
    out[[field]] <- result
  }

  out
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
  existing <- data.table::as.data.table(existing)
  incoming <- data.table::as.data.table(incoming)

  key_col <- ".__litxr_upsert_key__"
  existing_keys <- .litxr_upsert_key(existing)
  incoming_keys <- .litxr_upsert_key(incoming)
  conflicts <- new.env(parent = emptyenv())
  conflicts$rows <- list()
  if (!length(existing_keys) && !length(incoming_keys)) {
    return(data.table::data.table())
  }

  existing_map <- data.table::data.table(
    `.__litxr_upsert_key__` = existing_keys,
    existing_row_id__ = seq_len(nrow(existing))
  )
  incoming_map <- data.table::data.table(
    `.__litxr_upsert_key__` = incoming_keys,
    incoming_row_id__ = seq_len(nrow(incoming))
  )

  matched_idx <- merge(
    existing_map,
    incoming_map,
    by = key_col,
    all = FALSE,
    sort = FALSE
  )

  existing_only_idx <- which(!(existing_keys %in% incoming_keys))
  incoming_only_idx <- which(!(incoming_keys %in% existing_keys))
  matched_existing_idx <- matched_idx[["existing_row_id__"]]
  matched_incoming_idx <- matched_idx[["incoming_row_id__"]]

  out <- list()
  if (length(existing_only_idx)) {
    out[[length(out) + 1L]] <- existing[existing_only_idx, ]
  }

  if (length(matched_existing_idx)) {
    matched_existing <- existing[matched_existing_idx, ]
    matched_incoming <- incoming[matched_incoming_idx, ]
    prefer_existing_rows <- rep(FALSE, nrow(matched_existing))
    if ("source" %in% names(matched_existing) && "source" %in% names(matched_incoming) &&
        "arxiv_version" %in% names(matched_existing) && "arxiv_version" %in% names(matched_incoming)) {
      existing_source <- matched_existing[["source"]]
      incoming_source <- matched_incoming[["source"]]
      existing_version <- suppressWarnings(as.integer(matched_existing[["arxiv_version"]]))
      incoming_version <- suppressWarnings(as.integer(matched_incoming[["arxiv_version"]]))
      is_arxiv_pair <- !.litxr_column_missing_mask(existing_source) &
        !.litxr_column_missing_mask(incoming_source) &
        as.character(existing_source) == "arxiv" &
        as.character(incoming_source) == "arxiv"
      prefer_existing_rows <- is_arxiv_pair & !is.na(existing_version) & !is.na(incoming_version) & incoming_version < existing_version
    }

    masks <- .litxr_upsert_match_masks(matched_existing, matched_incoming, key_col)
    merged_matched <- .litxr_merge_record_rows_vectorized(
      matched_existing,
      matched_incoming,
      key = key_col,
      prefer_existing_rows = prefer_existing_rows
    )

    if (any(masks$conflict)) {
      conflict_idx <- which(masks$conflict)
      conflict_rows <- data.table::rbindlist(lapply(conflict_idx, function(i) {
        .litxr_merge_record_row(matched_existing[i, ], matched_incoming[i, ], matched_idx[[".__litxr_upsert_key__"]][[i]], conflicts)
      }), fill = TRUE)
      merged_matched[[".__litxr_order__"]] <- seq_len(nrow(merged_matched))
      conflict_rows[[".__litxr_order__"]] <- conflict_idx
      merged_matched <- data.table::rbindlist(
        list(merged_matched[!masks$conflict, ], conflict_rows),
        fill = TRUE
      )
      data.table::setorder(merged_matched, ".__litxr_order__")
      merged_matched[[".__litxr_order__"]] <- NULL
    }

    out[[length(out) + 1L]] <- merged_matched
  }

  if (length(incoming_only_idx)) {
    out[[length(out) + 1L]] <- incoming[incoming_only_idx, ]
  }

  merged <- data.table::rbindlist(out, fill = TRUE)
  .litxr_write_upsert_conflicts(conflicts, conflict_path = conflict_path)
  merged
}

.litxr_upsert_journal_records <- function(existing, incoming, local_path) {
  .litxr_upsert_records(
    existing,
    incoming,
    conflict_path = file.path(.litxr_journal_paths(local_path)$json, "_upsert_conflicts.jsonl")
  )
}

.litxr_read_journal_records_authoritative <- function(local_path) {
  .litxr_read_journal_records_from_json(local_path)
}

.litxr_write_journal_upserted_records <- function(
  existing,
  incoming,
  local_path,
  journal,
  cfg = NULL,
  refresh_entity_indexes = TRUE,
  refresh_ref_identity_map = TRUE
) {
  if (missing(existing) || is.null(existing)) {
    existing <- .litxr_read_existing_records_for_incoming(local_path, incoming)
  }
  records <- .litxr_upsert_journal_records(existing, incoming, local_path = local_path)
  incoming_keys <- .litxr_upsert_key(incoming)
  records_keys <- .litxr_upsert_key(records)
  touched_records <- records[records_keys %in% incoming_keys, ]

  .litxr_write_journal_record_files(touched_records, local_path, journal)
  if (!is.null(cfg)) .litxr_update_project_indexes(
    cfg,
    journal,
    touched_records,
    refresh_entity_indexes = refresh_entity_indexes,
    refresh_ref_identity_map = refresh_ref_identity_map
  )

  records
}

.litxr_write_journal_records <- function(
  records,
  local_path,
  journal,
  cfg = NULL,
  refresh_entity_indexes = TRUE,
  refresh_ref_identity_map = TRUE
) {
  if (!nrow(records)) {
    .litxr_ensure_journal_dirs(local_path)
    if (!is.null(cfg)) .litxr_update_project_indexes(
      cfg,
      journal,
      records,
      refresh_entity_indexes = refresh_entity_indexes,
      refresh_ref_identity_map = refresh_ref_identity_map
    )
    return(invisible(character()))
  }

  written <- .litxr_write_journal_record_files(records, local_path, journal)

  if (!is.null(cfg)) .litxr_update_project_indexes(
    cfg,
    journal,
    records,
    refresh_entity_indexes = refresh_entity_indexes,
    refresh_ref_identity_map = refresh_ref_identity_map
  )
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
  .litxr_read_journal_records_from_json(local_path)
}

.litxr_read_journal_records_from_json <- function(
  local_path,
  json_files = NULL,
  modified_after = NULL,
  chunk_size = getOption("litxr.json_read_chunk_size", 2000L)
) {
  paths <- .litxr_journal_paths(local_path)
  json_dir <- .litxr_existing_collection_dir(paths$json, paths$legacy_json)
  if (!dir.exists(json_dir)) {
    return(data.table::data.table())
  }

  files <- if (is.null(json_files)) {
    sort(list.files(json_dir, pattern = "\\.json$", full.names = TRUE))
  } else {
    unique(as.character(json_files))
  }
  if (!length(files)) {
    return(data.table::data.table())
  }

  files <- files[file.exists(files)]
  if (!length(files)) {
    return(data.table::data.table())
  }

  if (!is.null(modified_after)) {
    if (inherits(modified_after, "POSIXt")) {
      cutoff <- as.POSIXct(modified_after, tz = "UTC")
    } else {
      cutoff <- suppressWarnings(as.POSIXct(modified_after, tz = "UTC"))
    }
    if (!is.na(cutoff)) {
      info <- file.info(files)
      keep <- !is.na(info$mtime) & as.POSIXct(info$mtime, tz = "UTC") > cutoff
      files <- files[keep]
    }
  }
  if (!length(files)) {
    return(data.table::data.table())
  }

  chunk_size <- suppressWarnings(as.integer(chunk_size[[1]]))
  if (is.na(chunk_size) || chunk_size < 1L) {
    chunk_size <- length(files)
  }

  chunk_count <- as.integer(ceiling(length(files) / chunk_size))
  chunks <- vector("list", chunk_count)
  chunk_id <- 1L
  for (start_idx in seq.int(1L, length(files), by = chunk_size)) {
    end_idx <- min(start_idx + chunk_size - 1L, length(files))
    rows <- vector("list", end_idx - start_idx + 1L)
    row_id <- 1L
    for (file_idx in start_idx:end_idx) {
      rows[[row_id]] <- .litxr_storage_payload_to_projection_row(files[[file_idx]])
      row_id <- row_id + 1L
    }
    chunks[[chunk_id]] <- data.table::rbindlist(rows, fill = TRUE)
    chunk_id <- chunk_id + 1L
  }
  chunks <- chunks[vapply(chunks, nrow, integer(1L)) > 0L]
  if (!length(chunks)) {
    return(data.table::data.table())
  }

  .litxr_prefer_complete_records(data.table::rbindlist(chunks, fill = TRUE))
}
