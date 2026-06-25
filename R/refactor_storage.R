.litxr_project_reference_columns <- function(records) {
  intersect(.litxr_reference_projection_columns(), setdiff(names(records), c("collection_id", "collection_title")))
}

.litxr_project_references_from_collection_records <- function(records) {
  if (!nrow(records)) {
    return(data.table::data.table())
  }
  cols <- .litxr_project_reference_columns(records)
  data.table::copy(records)[, cols, with = FALSE]
}

.litxr_project_reference_lookup_columns <- function(extra = NULL) {
  unique(c(
    "ref_id",
    "source",
    "source_id",
    "entry_type",
    "title",
    "authors",
    "year",
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
    "linked_doi_ref_id",
    "linked_arxiv_ref_id",
    "arxiv_version",
    "arxiv_id_base",
    "collection_id",
    "collection_title",
    extra
  ))
}

.litxr_authoritative_project_records <- function(cfg) {
  collections <- .litxr_config_collections(cfg)
  if (!length(collections)) {
    return(data.table::data.table())
  }

  refs_list <- lapply(collections, function(collection) {
    local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
    records <- .litxr_read_journal_records_authoritative(local_path)
    .litxr_project_references_from_collection_records(records)
  })
  refs_list <- refs_list[vapply(refs_list, nrow, integer(1)) > 0L]
  if (!length(refs_list)) {
    return(data.table::data.table())
  }

  refs <- data.table::rbindlist(refs_list, fill = TRUE)
  .litxr_upsert_records(
    refs[0, ],
    refs,
    conflict_path = .litxr_project_reference_conflict_path(cfg)
  )
}

.litxr_authoritative_project_reference_links <- function(cfg) {
  collections <- .litxr_config_collections(cfg)
  empty_links <- data.table::data.table(
    ref_id = character(),
    collection_id = character(),
    collection_title = character(),
    recorded_at = character()
  )
  if (!length(collections)) {
    return(empty_links)
  }

  links_list <- lapply(collections, function(collection) {
    local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
    records <- .litxr_read_journal_records_authoritative(local_path)
    .litxr_project_reference_links_from_collection_records(records, collection)
  })
  links_list <- links_list[vapply(links_list, nrow, integer(1)) > 0L]
  if (!length(links_list)) {
    return(empty_links)
  }

  links <- data.table::rbindlist(links_list, fill = TRUE)
  link_key <- paste(links$ref_id, links$collection_id, sep = "\r")
  links[!duplicated(link_key), ]
}

.litxr_sync_scalar_chr <- function(x) {
  if (is.null(x) || !length(x)) {
    return(NA_character_)
  }
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) {
    return(NA_character_)
  }
  x[[1L]]
}

.litxr_sync_scalar_int <- function(x, default = NA_integer_) {
  if (is.null(x) || !length(x)) {
    return(default)
  }
  x <- as.character(x[[1L]])
  if (!nzchar(x) || is.na(x)) {
    return(default)
  }
  value <- suppressWarnings(as.integer(x))
  if (is.na(value)) default else value
}

.litxr_sync_json_files_after <- function(json_dir, json_mtime_after = NULL) {
  if (!dir.exists(json_dir)) {
    return(character())
  }
  files <- sort(list.files(json_dir, pattern = "\\.json$", full.names = TRUE))
  if (!length(files)) {
    return(character())
  }
  if (!is.null(json_mtime_after)) {
    cutoff <- if (inherits(json_mtime_after, "POSIXt")) {
      as.POSIXct(json_mtime_after, tz = "UTC")
    } else {
      suppressWarnings(as.POSIXct(json_mtime_after, tz = "UTC"))
    }
    if (!is.na(cutoff)) {
      info <- file.info(files)
      keep <- !is.na(info$mtime) & as.POSIXct(info$mtime, tz = "UTC") > cutoff
      files <- files[keep]
    }
  }
  files[file.exists(files)]
}

.litxr_sync_collection_folder_names <- function(cfg, collection_ids = NULL) {
  root <- .litxr_project_root(cfg)
  system_folders <- c("index", "llm", "llm_history", "embeddings")
  folders <- list.dirs(root, recursive = FALSE, full.names = FALSE)
  folders <- folders[!folders %in% system_folders]
  folders <- folders[dir.exists(file.path(root, folders))]
  if (!is.null(collection_ids) && length(collection_ids)) {
    collection_ids <- unique(as.character(collection_ids))
    collection_ids <- collection_ids[!is.na(collection_ids) & nzchar(collection_ids)]
    unknown_ids <- setdiff(collection_ids, folders)
    if (length(unknown_ids)) {
      stop("Unknown collection_id value(s): ", paste(unknown_ids, collapse = ", "), call. = FALSE)
    }
    folders <- folders[folders %in% collection_ids]
  }
  folders
}

.litxr_sync_thin_rows_from_payload <- function(payload, branch = c("arxiv", "doi")) {
  branch <- match.arg(branch)
  ref_id <- .litxr_sync_scalar_chr(payload$ref_id)
  source_id <- .litxr_sync_scalar_chr(payload$source_id)
  arxiv_versioned <- .litxr_sync_scalar_chr(payload$arxiv_id_versioned)

  if (identical(branch, "arxiv")) {
    arxiv_value <- .litxr_bare_arxiv_id(ref_id = ref_id, source_id = source_id, arxiv_versioned = arxiv_versioned)
    if (is.na(arxiv_value) || !nzchar(arxiv_value)) {
      return(NULL)
    }
    doi_value <- .litxr_sync_scalar_chr(payload$doi)
    if (is.na(doi_value) || !nzchar(doi_value)) {
      doi_value <- NA_character_
    }
    return(list(
      arxiv_id = arxiv_value,
      arxiv_version = .litxr_arxiv_version_value(
        ref_id = ref_id,
        version = payload$arxiv_version,
        versioned_id = arxiv_versioned,
        source_id = source_id
      ),
      doi = doi_value
    ))
  }

  doi_value <- .litxr_bare_doi(doi = payload$doi)
  if (is.na(doi_value) || !nzchar(doi_value)) {
    return(NULL)
  }

  list(
    doi = doi_value,
    year = .litxr_sync_scalar_int(payload$year, default = NA_integer_)
  )
}

.litxr_authoritative_entity_inputs <- function(cfg, collection_ids = NULL, json_mtime_after = NULL) {
  collections <- .litxr_config_collections(cfg)
  if (!is.null(collection_ids) && length(collection_ids)) {
    collection_ids <- unique(as.character(collection_ids))
    collections <- collections[vapply(collections, function(collection) {
      collection$collection_id %in% collection_ids
    }, logical(1))]
  }
  empty_links <- data.table::data.table(
    ref_id = character(),
    collection_id = character(),
    collection_title = character(),
    recorded_at = character()
  )
  if (!length(collections)) {
    return(list(refs = data.table::data.table(), links = empty_links))
  }

  refs_list <- vector("list", length(collections))
  links_list <- vector("list", length(collections))
  refs_count <- 0L
  links_count <- 0L
  for (collection in collections) {
    local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
    records <- .litxr_read_journal_records_from_json(local_path, modified_after = json_mtime_after)
    if (nrow(records)) {
      records[["collection_id"]] <- rep(as.character(collection$collection_id), nrow(records))
      records[["collection_title"]] <- rep(as.character(collection$title), nrow(records))
    }
    refs <- records
    links <- .litxr_project_reference_links_from_collection_records(records, collection)
    if (nrow(refs)) {
      refs_count <- refs_count + 1L
      refs_list[[refs_count]] <- refs
    }
    if (nrow(links)) {
      links_count <- links_count + 1L
      links_list[[links_count]] <- links
    }
  }

  refs_list <- refs_list[seq_len(refs_count)]
  links_list <- links_list[seq_len(links_count)]
  refs <- if (length(refs_list)) data.table::rbindlist(refs_list, fill = TRUE) else data.table::data.table()
  links <- if (length(links_list)) data.table::rbindlist(links_list, fill = TRUE) else empty_links
  if (nrow(links)) {
    link_key <- paste(links$ref_id, links$collection_id, sep = "\r")
    links <- links[!duplicated(link_key), ]
  }
  list(refs = refs, links = links)
}

.litxr_upsert_project_ref_identity_map <- function(cfg, identities, diff_dir = NULL) {
  identities <- data.table::as.data.table(identities)
  existing <- .litxr_read_project_ref_identity_index(cfg)
  identity_keys <- if (nrow(identities)) paste(identities$arxiv_id, identities$doi, sep = "\r") else character()
  existing_keys <- if (nrow(existing)) paste(existing$arxiv_id, existing$doi, sep = "\r") else character()
  if (!nrow(existing)) {
    .litxr_write_project_ref_identity_index(cfg, identities)
    added <- if (nrow(identities)) unique(identity_keys) else character()
    removed <- character()
    added_path <- removed_path <- NA_character_
    if (!is.null(diff_dir) && length(added)) {
      added_path <- file.path(diff_dir, "ref_identity_map_added.tsv")
      utils::write.table(identities, file = added_path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
    }
    if (!is.null(diff_dir) && length(removed)) {
      removed_path <- file.path(diff_dir, "ref_identity_map_removed.tsv")
      utils::write.table(data.table::data.table(arxiv_id = character(), doi = character()), file = removed_path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
    }
    return(list(
      rows = identities,
      added = added,
      removed = removed,
      added_path = added_path,
      removed_path = removed_path
    ))
  }
  if (!nrow(identities)) {
    .litxr_write_project_ref_identity_index(cfg, existing[0, ])
    removed <- unique(paste(existing$arxiv_id, existing$doi, sep = "\r"))
    removed_path <- NA_character_
    if (!is.null(diff_dir) && length(removed)) {
      removed_path <- file.path(diff_dir, "ref_identity_map_removed.tsv")
      utils::write.table(data.table::data.table(arxiv_id = existing$arxiv_id, doi = existing$doi), file = removed_path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
    }
    return(list(rows = existing[0, ], added = character(), removed = removed, added_path = NA_character_, removed_path = removed_path))
  }

  if (!("arxiv_id" %in% names(existing))) existing[["arxiv_id"]] <- rep(NA_character_, nrow(existing))
  if (!("doi" %in% names(existing))) existing[["doi"]] <- rep(NA_character_, nrow(existing))
  if (!("arxiv_id" %in% names(identities))) identities[["arxiv_id"]] <- rep(NA_character_, nrow(identities))
  if (!("doi" %in% names(identities))) identities[["doi"]] <- rep(NA_character_, nrow(identities))

  existing$arxiv_id <- as.character(existing$arxiv_id)
  existing$doi <- as.character(existing$doi)
  identities$arxiv_id <- as.character(identities$arxiv_id)
  identities$doi <- as.character(identities$doi)

  updated_arxiv <- unique(identities$arxiv_id)
  updated_doi <- unique(identities$doi)
  updated_arxiv <- updated_arxiv[!is.na(updated_arxiv) & nzchar(updated_arxiv)]
  updated_doi <- updated_doi[!is.na(updated_doi) & nzchar(updated_doi)]

  keep_existing <- !(existing$arxiv_id %in% updated_arxiv | existing$doi %in% updated_doi)
  out <- data.table::rbindlist(list(existing[keep_existing, ], identities), fill = TRUE)
  out <- out[!duplicated(paste(out$arxiv_id, out$doi, sep = "\r")), ]
  .litxr_write_project_ref_identity_index(cfg, out)
  added_rows <- if (nrow(identities)) identities[!(identity_keys %in% existing_keys), , drop = FALSE] else identities
  removed_rows <- if (nrow(existing)) existing[existing_keys %in% setdiff(existing_keys, identity_keys), , drop = FALSE] else existing
  added <- if (nrow(added_rows)) unique(paste(added_rows$arxiv_id, added_rows$doi, sep = "\r")) else character()
  removed <- if (nrow(removed_rows)) unique(paste(removed_rows$arxiv_id, removed_rows$doi, sep = "\r")) else character()
  added_path <- removed_path <- NA_character_
  if (!is.null(diff_dir) && length(added)) {
    added_path <- file.path(diff_dir, "ref_identity_map_added.tsv")
    utils::write.table(added_rows, file = added_path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
  }
  if (!is.null(diff_dir) && length(removed)) {
    removed_path <- file.path(diff_dir, "ref_identity_map_removed.tsv")
    utils::write.table(removed_rows, file = removed_path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
  }
  list(
    rows = out,
    added = added,
    removed = removed,
    added_path = added_path,
    removed_path = removed_path
  )
}

.litxr_sync_thin_ref_store_inputs <- function(cfg, collection_ids = NULL, json_mtime_after = NULL) {
  root <- .litxr_project_root(cfg)
  folders <- .litxr_sync_collection_folder_names(cfg, collection_ids = collection_ids)
  arxiv_branch_folders <- intersect(folders, c("arxiv_cs_ai", "manual_arxiv_refs"))
  doi_branch_folders <- setdiff(folders, arxiv_branch_folders)

  arxiv_rows_list <- list()
  doi_rows_list <- list()

  arxiv_count <- 0L
  doi_count <- 0L

  for (folder in arxiv_branch_folders) {
    files <- .litxr_sync_json_files_after(file.path(root, folder, "ref_json"), json_mtime_after = json_mtime_after)
    for (path in files) {
      rows <- .litxr_sync_thin_rows_from_payload(jsonlite::fromJSON(path, simplifyVector = FALSE), branch = "arxiv")
      if (!is.null(rows)) {
        arxiv_count <- arxiv_count + 1L
        arxiv_rows_list[[arxiv_count]] <- rows
      }
    }
  }

  for (folder in doi_branch_folders) {
    files <- .litxr_sync_json_files_after(file.path(root, folder, "ref_json"), json_mtime_after = json_mtime_after)
    for (path in files) {
      rows <- .litxr_sync_thin_rows_from_payload(jsonlite::fromJSON(path, simplifyVector = FALSE), branch = "doi")
      if (!is.null(rows)) {
        doi_count <- doi_count + 1L
        doi_rows_list[[doi_count]] <- rows
      }
    }
  }

  arxiv_rows <- if (length(arxiv_rows_list)) data.table::rbindlist(arxiv_rows_list, fill = TRUE) else data.table::data.table(arxiv_id = character(), arxiv_version = integer(), doi = character())
  doi_rows <- if (length(doi_rows_list)) data.table::rbindlist(doi_rows_list, fill = TRUE) else data.table::data.table(doi = character(), year = integer())

  if (nrow(arxiv_rows)) {
    arxiv_rows$arxiv_version <- suppressWarnings(as.integer(arxiv_rows$arxiv_version))
    arxiv_rows$arxiv_version[is.na(arxiv_rows$arxiv_version)] <- 0L
    data.table::setorder(arxiv_rows, arxiv_id, -arxiv_version)
    arxiv_rows <- arxiv_rows[!duplicated(arxiv_rows$arxiv_id), ]
  }
  if (nrow(doi_rows)) {
    dup_doi <- duplicated(doi_rows$doi)
    if (any(dup_doi)) {
      stop(
        "Duplicate DOI id(s) found while rebuilding thin DOI store: ",
        paste(unique(doi_rows$doi[dup_doi]), collapse = ", "),
        call. = FALSE
      )
    }
  }
  list(
    selected_collection_ids = folders,
    arxiv_folders = arxiv_branch_folders,
    doi_folders = doi_branch_folders,
    arxiv_rows = arxiv_rows,
    doi_rows = doi_rows
  )
}

.litxr_rebuild_project_reference_indexes <- function(cfg, repair_collection_indexes = TRUE) {
  collections <- .litxr_config_collections(cfg)
  if (!length(collections)) {
    .litxr_refresh_normalized_reference_scaffold(cfg, records = data.table::data.table(), refresh_entity_indexes = FALSE)
    return(invisible(NULL))
  }

  refs_list <- list()

  for (collection in collections) {
    local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
    records <- .litxr_read_journal_records_authoritative(local_path)
    projection <- .litxr_project_references_from_collection_records(records)
    if (nrow(projection)) {
      refs_list[[length(refs_list) + 1L]] <- projection
    }
  }

  refs <- if (length(refs_list)) data.table::rbindlist(refs_list, fill = TRUE) else data.table::data.table()
  identities <- .litxr_build_ref_identity_index(cfg, refs = refs)
  .litxr_refresh_ref_identity_map(cfg, identities = identities)
  .litxr_refresh_normalized_reference_scaffold(cfg, refs = refs, refresh_entity_indexes = FALSE)
  invisible(NULL)
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

.litxr_scaffold_row_keys <- function(records, key_cols) {
  records <- data.table::as.data.table(records)
  key_cols <- intersect(as.character(key_cols), names(records))
  if (!length(key_cols) || !nrow(records)) {
    return(character())
  }
  if (length(key_cols) == 1L) {
    return(as.character(records[[key_cols[[1L]]]]))
  }
  do.call(
    paste,
    c(data.table::copy(records)[, key_cols, with = FALSE], sep = "\r")
  )
}

.litxr_upsert_scaffold_rows <- function(path, rows, key_cols) {
  rows <- data.table::as.data.table(rows)
  key_cols <- as.character(key_cols)
  if (!length(key_cols)) {
    stop("`key_cols` must be supplied for scaffold upsert.", call. = FALSE)
  }
  if (!nrow(rows)) {
    return(list(
      written = FALSE,
      added = character(),
      removed = character(),
      rows = .litxr_read_scaffold_table_safe(path)
    ))
  }

  existing <- .litxr_read_scaffold_table_safe(path)
  new_keys <- .litxr_scaffold_row_keys(rows, key_cols)
  if (!nrow(existing)) {
    rows <- rows[!duplicated(new_keys), ]
    fst::write_fst(as.data.frame(rows), path)
    return(list(
      written = TRUE,
      added = unique(new_keys),
      removed = character(),
      rows = rows
    ))
  }

  existing_keys <- .litxr_scaffold_row_keys(existing, key_cols)
  keep_existing <- !(existing_keys %in% new_keys)
  kept <- existing[keep_existing, , drop = FALSE]
  out <- data.table::rbindlist(list(kept, rows), fill = TRUE)
  out_keys <- .litxr_scaffold_row_keys(out, key_cols)
  if (length(out_keys)) {
    out <- out[!duplicated(out_keys), ]
  }
  fst::write_fst(as.data.frame(out), path)

  list(
    written = TRUE,
    added = setdiff(unique(new_keys), unique(existing_keys)),
    removed = setdiff(unique(existing_keys), unique(new_keys)),
    rows = out
  )
}

.litxr_read_project_references_by_keys <- function(cfg, keys, columns = NULL, hydrate = FALSE, wide_projection_limit = 300L, keyed_fst_read_threshold = getOption("litxr.keyed_fst_read_threshold", 32L)) {
  read_columns <- .litxr_project_reference_lookup_columns(columns)
  refs <- .litxr_read_normalized_reference_rows_by_keys(cfg, keys, columns = read_columns)
  if (!nrow(refs)) {
    return(refs)
  }
  source_id <- if ("source_id" %in% names(refs)) refs$source_id else rep(NA_character_, nrow(refs))
  out <- refs[refs$ref_id %in% keys | source_id %in% keys, ]
  key <- .litxr_upsert_key(out)
  out <- out[!duplicated(key), ]
  if (isTRUE(hydrate)) {
    return(.litxr_hydrate_project_projection_rows(cfg, out, wide_projection_limit = wide_projection_limit))
  }
  out
}

.litxr_sync_thin_ref_stores_from_json <- function(cfg, collection_ids = NULL, json_mtime_after = NULL) {
  inputs <- .litxr_sync_thin_ref_store_inputs(cfg, collection_ids = collection_ids, json_mtime_after = json_mtime_after)
  selected_ids <- inputs$selected_collection_ids
  arxiv_rows <- inputs$arxiv_rows
  doi_rows <- inputs$doi_rows
  current_mode <- if (is.null(json_mtime_after)) "full" else "incremental"
  diff_dir <- .litxr_project_index_dir(cfg)
  identities <- if (nrow(arxiv_rows)) {
    keep <- !is.na(arxiv_rows$doi) & nzchar(arxiv_rows$doi)
    arxiv_rows[keep, c("arxiv_id", "doi"), with = FALSE]
  } else {
    data.table::data.table(arxiv_id = character(), doi = character())
  }
  if (is.null(json_mtime_after)) {
    arxiv_store <- .litxr_upsert_scaffold_rows(.litxr_ref_arxiv_path(cfg), arxiv_rows, "arxiv_id")
    doi_store <- .litxr_upsert_scaffold_rows(.litxr_ref_doi_path(cfg), doi_rows, "doi")
    identity_store <- .litxr_upsert_project_ref_identity_map(cfg, identities, diff_dir = diff_dir)
  } else {
    arxiv_store <- .litxr_upsert_scaffold_rows(.litxr_ref_arxiv_path(cfg), arxiv_rows, "arxiv_id")
    doi_store <- .litxr_upsert_scaffold_rows(.litxr_ref_doi_path(cfg), doi_rows, "doi")
    identity_store <- .litxr_upsert_project_ref_identity_map(cfg, identities, diff_dir = diff_dir)
  }

  if (length(arxiv_store$removed)) {
    warning(
      "ref_arxiv.fst will drop arXiv id(s) missing from current local JSON: ",
      paste(arxiv_store$removed, collapse = ", "),
      call. = FALSE
    )
  }
  if (length(doi_store$removed)) {
    warning(
      "ref_doi.fst will drop DOI id(s) missing from current local JSON: ",
      paste(doi_store$removed, collapse = ", "),
      call. = FALSE
    )
  }

  arxiv_removed_path <- if (length(arxiv_store$removed)) {
    path <- file.path(diff_dir, "ref_arxiv_removed.tsv")
    utils::write.table(data.table::data.table(arxiv_id = arxiv_store$removed), file = path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
    path
  } else {
    NA_character_
  }
  arxiv_added_path <- if (length(arxiv_store$added)) {
    path <- file.path(diff_dir, "ref_arxiv_added.tsv")
    utils::write.table(data.table::data.table(arxiv_id = arxiv_store$added), file = path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
    path
  } else {
    NA_character_
  }
  doi_removed_path <- if (length(doi_store$removed)) {
    path <- file.path(diff_dir, "ref_doi_removed.tsv")
    utils::write.table(data.table::data.table(doi = doi_store$removed), file = path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
    path
  } else {
    NA_character_
  }
  doi_added_path <- if (length(doi_store$added)) {
    path <- file.path(diff_dir, "ref_doi_added.tsv")
    utils::write.table(data.table::data.table(doi = doi_store$added), file = path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
    path
  } else {
    NA_character_
  }
  identity_removed_path <- if (length(identity_store$removed)) {
    path <- file.path(diff_dir, "ref_identity_map_removed.tsv")
    path
  } else {
    NA_character_
  }
  identity_added_path <- if (length(identity_store$added)) {
    path <- file.path(diff_dir, "ref_identity_map_added.tsv")
    path
  } else {
    NA_character_
  }

  list(
    selected_collection_ids = selected_ids,
    mode = current_mode,
    collection_results = list(
      list(collection_id = NA_character_, rebuilt_collection_index = FALSE, collection_local_path = NA_character_, refs_written = nrow(arxiv_rows) + nrow(doi_rows), links_written = nrow(identities), refs_removed = length(arxiv_store$removed) + length(doi_store$removed))
    ),
    diff_paths = list(
      ref_identity_map = list(
        added = identity_store$added_path %||% NA_character_,
        removed = identity_store$removed_path %||% NA_character_
      ),
      ref_arxiv = list(
        added = arxiv_added_path,
        removed = arxiv_removed_path
      ),
      ref_doi = list(
        added = doi_added_path,
        removed = doi_removed_path
      )
    ),
    project_paths = list(
      ref_identity_map = .litxr_project_ref_identity_index_path(cfg),
      ref_arxiv = .litxr_ref_arxiv_path(cfg),
      ref_doi = .litxr_ref_doi_path(cfg),
      ref_arxiv_removed = arxiv_removed_path,
      ref_arxiv_added = arxiv_added_path,
      ref_doi_removed = doi_removed_path,
      ref_doi_added = doi_added_path,
      ref_identity_map_removed = identity_removed_path,
      ref_identity_map_added = identity_added_path
    ),
    row_counts = list(
      project_references = nrow(arxiv_rows) + nrow(doi_rows),
      project_reference_collections = nrow(identities),
      identities = nrow(identity_store$rows),
      ref_arxiv = nrow(arxiv_store$rows),
      ref_doi = nrow(doi_store$rows)
    ),
    diffs = list(
      ref_arxiv = list(added = length(arxiv_store$added), removed = length(arxiv_store$removed)),
      ref_doi = list(added = length(doi_store$added), removed = length(doi_store$removed))
    )
  )
}

.litxr_refactor_diagnostics <- function(cfg, oversized_mb = 25) {
  reference_cache <- .litxr_audit_reference_cache_state(cfg)
  entity_indexes <- .litxr_audit_entity_indexes(cfg, oversized_mb = oversized_mb)
  entity_status <- .litxr_audit_entity_status_state(cfg)

  collection_cache <- reference_cache$collection_reference_cache
  project_cache <- reference_cache$project_reference_cache
  index_health <- entity_indexes$index_health
  thin_index_health <- entity_indexes$thin_index_health
  compatibility_projection_health <- entity_indexes$compatibility_projection_health

  summary <- data.table::data.table(
    collection_cache_scopes_with_main_mismatch = if (nrow(collection_cache)) {
      as.integer(sum(collection_cache$main_missing_n > 0L | collection_cache$main_extra_n > 0L))
    } else 0L,
    collection_cache_scopes_with_merged_mismatch = if (nrow(collection_cache)) {
      as.integer(sum(collection_cache$merged_missing_n > 0L | collection_cache$merged_extra_n > 0L))
    } else 0L,
    project_cache_scopes_with_main_mismatch = if (nrow(project_cache)) {
      as.integer(sum(project_cache$main_missing_n > 0L | project_cache$main_extra_n > 0L))
    } else 0L,
    project_cache_scopes_with_merged_mismatch = if (nrow(project_cache)) {
      as.integer(sum(project_cache$merged_missing_n > 0L | project_cache$merged_extra_n > 0L))
    } else 0L,
    identity_split_entities = as.integer(nrow(entity_indexes$identity_splits)),
    ambiguous_identity_joins = as.integer(nrow(entity_indexes$ambiguous_identity_joins)),
    orphan_artifacts = as.integer(nrow(entity_indexes$orphan_artifacts)),
    damaged_or_missing_indexes = if (nrow(index_health)) {
      as.integer(sum(index_health$status %in% c("damaged", "missing")))
    } else 0L,
    damaged_or_missing_thin_indexes = if (nrow(thin_index_health)) {
      as.integer(sum(thin_index_health$status %in% c("damaged", "missing")))
    } else 0L,
    damaged_or_missing_compatibility_indexes = if (nrow(compatibility_projection_health)) {
      as.integer(sum(compatibility_projection_health$status %in% c("damaged", "missing")))
    } else 0L,
    oversized_indexes = if (nrow(index_health)) {
      as.integer(sum(index_health$status %in% c("oversized")))
    } else 0L,
    missing_entity_status_rows = as.integer(nrow(entity_status$missing_entity_status)),
    orphan_entity_status_rows = as.integer(nrow(entity_status$orphan_entity_status)),
    stale_entity_status_rows = as.integer(nrow(entity_status$stale_entity_status)),
    digest_revision_mismatches = as.integer(nrow(entity_status$digest_revision_mismatch))
  )

  list(
    summary = summary,
    reference_cache = reference_cache,
    entity_indexes = entity_indexes,
    entity_status = entity_status
  )
}
