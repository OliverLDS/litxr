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
  root <- file.path(.litxr_project_root(cfg), "ref")
  if (!dir.exists(root)) {
    return(character())
  }
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

.litxr_sync_thin_rows_from_payload <- function(payload, branch = c("arxiv", "doi"), collection_index = NA_integer_, json_filename = NA_character_) {
  branch <- match.arg(branch, c("arxiv", "doi", "isbn"))
  ref_id <- .litxr_sync_scalar_chr(payload$ref_id)
  source_id <- .litxr_sync_scalar_chr(payload$source_id)
  arxiv_versioned <- .litxr_sync_scalar_chr(payload$arxiv_id_versioned)
  collection_index <- suppressWarnings(as.integer(collection_index))
  if (length(collection_index) != 1L || is.na(collection_index) || collection_index < 1L) {
    collection_index <- NA_integer_
  }
  json_filename <- .litxr_sync_scalar_chr(json_filename)

  if (identical(branch, "arxiv")) {
    arxiv_value <- .litxr_bare_arxiv_id(ref_id = ref_id, source_id = source_id, arxiv_versioned = arxiv_versioned)
    if (is.na(arxiv_value) || !nzchar(arxiv_value)) {
      return(NULL)
    }
    doi_raw <- payload[["doi"]]
    if (is.null(doi_raw) || !length(doi_raw)) {
      doi_value <- NA_character_
    } else {
      doi_value <- trimws(as.character(doi_raw)[[1L]])
      if (!nzchar(doi_value)) {
        doi_value <- NA_character_
      }
    }
    return(list(
      arxiv_id = arxiv_value,
      arxiv_version = .litxr_arxiv_version_value(
        ref_id = ref_id,
        version = payload$arxiv_version,
        versioned_id = arxiv_versioned,
        source_id = source_id
      ),
      collection_index = collection_index,
      json_filename = json_filename,
      doi = doi_value
    ))
  }

  doi_value <- .litxr_bare_doi(doi = payload$doi)
  if (is.na(doi_value) || !nzchar(doi_value)) {
    if (identical(branch, "isbn")) {
      isbn_value <- .litxr_sync_scalar_chr(payload$isbn)
      if (is.na(isbn_value) || !nzchar(isbn_value)) {
        return(NULL)
      }
      return(list(isbn = isbn_value))
    }
    return(NULL)
  }

  if (identical(branch, "isbn")) {
    isbn_value <- .litxr_sync_scalar_chr(payload$isbn)
    if (is.na(isbn_value) || !nzchar(isbn_value)) {
      return(NULL)
    }
    return(list(isbn = isbn_value))
  }

  list(
    doi = doi_value,
    collection_index = collection_index,
    json_filename = json_filename
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

.litxr_upsert_project_ref_identity_map <- function(cfg, identities, diff_dir = NULL, remove_missing = FALSE) {
  identities <- data.table::as.data.table(identities)
  remove_missing <- isTRUE(remove_missing)
  existing <- .litxr_read_project_ref_identity_index(cfg)
  identity_keys <- if (nrow(identities)) paste(identities$arxiv_id, identities$doi, sep = "\r") else character()
  existing_keys <- if (nrow(existing)) paste(existing$arxiv_id, existing$doi, sep = "\r") else character()
  if (!("arxiv_id" %in% names(existing))) existing[["arxiv_id"]] <- rep(NA_character_, nrow(existing))
  if (!("doi" %in% names(existing))) existing[["doi"]] <- rep(NA_character_, nrow(existing))
  if (!("arxiv_id" %in% names(identities))) identities[["arxiv_id"]] <- rep(NA_character_, nrow(identities))
  if (!("doi" %in% names(identities))) identities[["doi"]] <- rep(NA_character_, nrow(identities))

  existing$arxiv_id <- as.character(existing$arxiv_id)
  existing$doi <- as.character(existing$doi)
  identities$arxiv_id <- as.character(identities$arxiv_id)
  identities$doi <- as.character(identities$doi)

  if (remove_missing) {
    if (nrow(identities)) {
      identity_keys <- identity_keys[!is.na(identity_keys) & nzchar(identity_keys)]
      identities <- identities[!duplicated(identity_keys), , drop = FALSE]
    }
    out <- identities
    if (!nrow(out)) {
      out <- existing[0, c("arxiv_id", "doi"), drop = FALSE]
    }
    .litxr_write_project_ref_identity_index(cfg, out)
    added <- if (nrow(out)) setdiff(unique(paste(out$arxiv_id, out$doi, sep = "\r")), existing_keys) else character()
    removed <- if (nrow(existing)) setdiff(existing_keys, unique(paste(out$arxiv_id, out$doi, sep = "\r"))) else character()
    added_rows <- if (length(added)) out[paste(out$arxiv_id, out$doi, sep = "\r") %in% added, , drop = FALSE] else out[0, ]
    removed_rows <- if (length(removed)) existing[existing_keys %in% removed, , drop = FALSE] else existing[0, ]
    added_path <- removed_path <- NA_character_
    if (!is.null(diff_dir) && length(added_rows)) {
      added_path <- file.path(diff_dir, "ref_identity_map_added.tsv")
      utils::write.table(added_rows, file = added_path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
    }
    if (!is.null(diff_dir) && length(removed_rows)) {
      removed_path <- file.path(diff_dir, "ref_identity_map_removed.tsv")
      utils::write.table(removed_rows, file = removed_path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
    }
    return(list(rows = out, added = added, removed = removed, added_path = added_path, removed_path = removed_path))
  }

  if (!nrow(existing)) {
    out <- identities[!duplicated(identity_keys), , drop = FALSE]
  } else {
    keep_existing <- !(existing$arxiv_id %in% identities$arxiv_id | existing$doi %in% identities$doi)
    out <- data.table::rbindlist(list(existing[keep_existing, ], identities), fill = TRUE)
    out <- out[!duplicated(paste(out$arxiv_id, out$doi, sep = "\r")), ]
  }
  .litxr_write_project_ref_identity_index(cfg, out)
  added_rows <- if (nrow(identities)) identities[!(identity_keys %in% existing_keys), , drop = FALSE] else identities
  added <- if (nrow(added_rows)) unique(paste(added_rows$arxiv_id, added_rows$doi, sep = "\r")) else character()
  added_path <- removed_path <- NA_character_
  if (!is.null(diff_dir) && length(added)) {
    added_path <- file.path(diff_dir, "ref_identity_map_added.tsv")
    utils::write.table(added_rows, file = added_path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
  }
  removed <- character()
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
  collections <- .litxr_config_collections(cfg)
  collection_ids_cfg <- vapply(collections, function(collection) as.character(collection$collection_id %||% collection$journal_id %||% NA_character_), character(1))
  isbn_branch_ids <- vapply(collections, function(collection) identical(collection$remote_channel, "isbn"), logical(1))
  isbn_branch_folders <- intersect(folders, vapply(collections[isbn_branch_ids], `[[`, character(1), "collection_id"))

  arxiv_rows_list <- list()
  doi_rows_list <- list()
  isbn_rows_list <- list()

  arxiv_count <- 0L
  doi_count <- 0L
  isbn_count <- 0L

  for (folder in arxiv_branch_folders) {
    collection_index <- match(folder, collection_ids_cfg)
    files <- .litxr_sync_json_files_after(file.path(root, folder), json_mtime_after = json_mtime_after)
    for (path in files) {
      rows <- .litxr_sync_thin_rows_from_payload(
        jsonlite::fromJSON(path, simplifyVector = FALSE),
        branch = "arxiv",
        collection_index = collection_index,
        json_filename = basename(path)
      )
      if (!is.null(rows)) {
        arxiv_count <- arxiv_count + 1L
        arxiv_rows_list[[arxiv_count]] <- rows
      }
    }
  }

  for (folder in doi_branch_folders) {
    collection_index <- match(folder, collection_ids_cfg)
    files <- .litxr_sync_json_files_after(file.path(root, folder), json_mtime_after = json_mtime_after)
    for (path in files) {
      rows <- .litxr_sync_thin_rows_from_payload(
        jsonlite::fromJSON(path, simplifyVector = FALSE),
        branch = "doi",
        collection_index = collection_index,
        json_filename = basename(path)
      )
      if (!is.null(rows)) {
        doi_count <- doi_count + 1L
        doi_rows_list[[doi_count]] <- rows
      }
    }
  }

  for (folder in isbn_branch_folders) {
    collection_index <- match(folder, collection_ids_cfg)
    files <- .litxr_sync_json_files_after(file.path(root, folder), json_mtime_after = json_mtime_after)
    for (path in files) {
      rows <- .litxr_sync_thin_rows_from_payload(
        jsonlite::fromJSON(path, simplifyVector = FALSE),
        branch = "isbn",
        collection_index = collection_index,
        json_filename = basename(path)
      )
      if (!is.null(rows)) {
        isbn_count <- isbn_count + 1L
        isbn_rows_list[[isbn_count]] <- rows
      }
    }
  }

  arxiv_rows <- if (length(arxiv_rows_list)) data.table::rbindlist(arxiv_rows_list, fill = TRUE) else data.table::data.table(arxiv_id = character(), arxiv_version = integer(), collection_index = integer(), json_filename = character(), doi = character())
  doi_rows <- if (length(doi_rows_list)) data.table::rbindlist(doi_rows_list, fill = TRUE) else data.table::data.table(doi = character(), collection_index = integer(), json_filename = character())
  isbn_rows <- if (length(isbn_rows_list)) data.table::rbindlist(isbn_rows_list, fill = TRUE) else data.table::data.table(isbn = character(), collection_index = integer(), json_filename = character())

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
  if (nrow(isbn_rows)) {
    dup_isbn <- duplicated(isbn_rows$isbn)
    if (any(dup_isbn)) {
      stop(
        "Duplicate ISBN id(s) found while rebuilding thin ISBN store: ",
        paste(unique(isbn_rows$isbn[dup_isbn]), collapse = ", "),
        call. = FALSE
      )
    }
  }
  list(
    selected_collection_ids = folders,
    arxiv_folders = arxiv_branch_folders,
    doi_folders = doi_branch_folders,
    isbn_folders = isbn_branch_folders,
    arxiv_rows = arxiv_rows,
    doi_rows = doi_rows,
    isbn_rows = isbn_rows
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

.litxr_upsert_scaffold_rows <- function(path, rows, key_cols, remove_missing = FALSE) {
  rows <- data.table::as.data.table(rows)
  key_cols <- as.character(key_cols)
  remove_missing <- isTRUE(remove_missing)
  if (!length(key_cols)) {
    stop("`key_cols` must be supplied for scaffold upsert.", call. = FALSE)
  }

  existing <- .litxr_read_scaffold_table_safe(path)
  new_keys <- .litxr_scaffold_row_keys(rows, key_cols)
  if (!nrow(rows)) {
    if (remove_missing) {
      empty_rows <- if (nrow(existing)) existing[0, , drop = FALSE] else rows
      fst::write_fst(as.data.frame(empty_rows), path)
      return(list(
        written = TRUE,
        added = character(),
        removed = if (nrow(existing)) unique(.litxr_scaffold_row_keys(existing, key_cols)) else character(),
        rows = empty_rows
      ))
    }
    return(list(
      written = FALSE,
      added = character(),
      removed = character(),
      rows = existing
    ))
  }

  if (remove_missing) {
    rows <- rows[!duplicated(new_keys), , drop = FALSE]
    fst::write_fst(as.data.frame(rows), path)
    return(list(
      written = TRUE,
      added = unique(new_keys),
      removed = if (nrow(existing)) setdiff(unique(.litxr_scaffold_row_keys(existing, key_cols)), unique(new_keys)) else character(),
      rows = rows
    ))
  }

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
    out <- out[!duplicated(out_keys), , drop = FALSE]
  }
  fst::write_fst(as.data.frame(out), path)

  list(
    written = TRUE,
    added = setdiff(unique(new_keys), unique(existing_keys)),
    removed = character(),
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
  isbn_rows <- inputs$isbn_rows
  current_mode <- if (is.null(json_mtime_after) && is.null(collection_ids)) "full" else "incremental"
  remove_missing <- is.null(json_mtime_after) && is.null(collection_ids)
  diff_dir <- .litxr_ensure_project_log_dir(cfg)
  if (nrow(arxiv_rows)) {
    data.table::set(arxiv_rows, j = "arxiv_version", value = NULL)
  }
  identities <- if (nrow(arxiv_rows) && "doi" %in% names(arxiv_rows)) {
    identities <- data.table::data.table(
      arxiv_id = arxiv_rows$arxiv_id,
      doi = arxiv_rows$doi
    )
    identities$doi <- trimws(as.character(identities$doi))
    identities <- identities[!is.na(identities$doi) & nzchar(identities$doi), ]
    if (nrow(identities)) {
      identities <- identities[!duplicated(paste(identities$arxiv_id, identities$doi, sep = "\r")), ]
    }
    identities
  } else {
    data.table::data.table(arxiv_id = character(), doi = character())
  }
  if (nrow(arxiv_rows) && "doi" %in% names(arxiv_rows)) {
    data.table::set(arxiv_rows, j = "doi", value = NULL)
  }
  if (nrow(arxiv_rows) && "arxiv_version" %in% names(arxiv_rows)) {
    data.table::set(arxiv_rows, j = "arxiv_version", value = NULL)
  }
  arxiv_store <- .litxr_upsert_scaffold_rows(.litxr_ref_arxiv_path(cfg), arxiv_rows, "arxiv_id", remove_missing = remove_missing)
  doi_store <- .litxr_upsert_scaffold_rows(.litxr_ref_doi_path(cfg), doi_rows, "doi", remove_missing = remove_missing)
  isbn_store <- .litxr_upsert_scaffold_rows(.litxr_ref_isbn_path(cfg), isbn_rows, "isbn", remove_missing = remove_missing)
  identity_store <- .litxr_upsert_project_ref_identity_map(cfg, identities, diff_dir = diff_dir, remove_missing = remove_missing)

  if (remove_missing && length(arxiv_store$removed)) {
    warning(
      "ref_arxiv.fst will drop arXiv id(s) missing from current local JSON: ",
      paste(arxiv_store$removed, collapse = ", "),
      call. = FALSE
    )
  }
  if (remove_missing && length(doi_store$removed)) {
    warning(
      "ref_doi.fst will drop DOI id(s) missing from current local JSON: ",
      paste(doi_store$removed, collapse = ", "),
      call. = FALSE
    )
  }
  if (remove_missing && length(isbn_store$removed)) {
    warning(
      "ref_isbn.fst will drop ISBN id(s) missing from current local JSON: ",
      paste(isbn_store$removed, collapse = ", "),
      call. = FALSE
    )
  }

  arxiv_removed_path <- if (remove_missing && length(arxiv_store$removed)) {
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
  doi_removed_path <- if (remove_missing && length(doi_store$removed)) {
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
  isbn_removed_path <- if (remove_missing && length(isbn_store$removed)) {
    path <- file.path(diff_dir, "ref_isbn_removed.tsv")
    utils::write.table(data.table::data.table(isbn = isbn_store$removed), file = path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
    path
  } else {
    NA_character_
  }
  isbn_added_path <- if (length(isbn_store$added)) {
    path <- file.path(diff_dir, "ref_isbn_added.tsv")
    utils::write.table(data.table::data.table(isbn = isbn_store$added), file = path, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
    path
  } else {
    NA_character_
  }
  identity_removed_path <- if (remove_missing && length(identity_store$removed)) {
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
      list(collection_id = NA_character_, rebuilt_collection_index = FALSE, collection_local_path = NA_character_, refs_written = nrow(arxiv_rows) + nrow(doi_rows) + nrow(isbn_rows), links_written = nrow(identities), refs_removed = length(arxiv_store$removed) + length(doi_store$removed) + length(isbn_store$removed))
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
      ),
      ref_isbn = list(
        added = isbn_added_path,
        removed = isbn_removed_path
      )
    ),
    update_log_path = .litxr_project_collection_sync_log_path(cfg),
    project_paths = list(
      ref_identity_map = .litxr_project_ref_identity_index_path(cfg),
      ref_arxiv = .litxr_ref_arxiv_path(cfg),
      ref_doi = .litxr_ref_doi_path(cfg),
      ref_isbn = .litxr_ref_isbn_path(cfg),
      ref_arxiv_removed = arxiv_removed_path,
      ref_arxiv_added = arxiv_added_path,
      ref_doi_removed = doi_removed_path,
      ref_doi_added = doi_added_path,
      ref_isbn_removed = isbn_removed_path,
      ref_isbn_added = isbn_added_path,
      ref_identity_map_removed = identity_removed_path,
      ref_identity_map_added = identity_added_path
    ),
    row_counts = list(
      project_references = nrow(arxiv_rows) + nrow(doi_rows) + nrow(isbn_rows),
      project_reference_collections = nrow(identities),
      identities = nrow(identity_store$rows),
      ref_arxiv = nrow(arxiv_store$rows),
      ref_doi = nrow(doi_store$rows),
      ref_isbn = nrow(isbn_store$rows)
    ),
    diffs = list(
      ref_arxiv = list(added = length(arxiv_store$added), removed = length(arxiv_store$removed)),
      ref_doi = list(added = length(doi_store$added), removed = length(doi_store$removed)),
      ref_isbn = list(added = length(isbn_store$added), removed = length(isbn_store$removed))
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
