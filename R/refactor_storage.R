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

.litxr_authoritative_entity_inputs <- function(cfg) {
  collections <- .litxr_config_collections(cfg)
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
    records <- .litxr_read_journal_records_authoritative(local_path)
    refs <- .litxr_project_references_from_collection_records(records)
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
  .litxr_refresh_ref_identity_map(cfg, refs = refs)
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

.litxr_migrate_refactor_indexes <- function(cfg, collection_ids = NULL, rebuild_collection_indexes = TRUE) {
  collections <- .litxr_config_collections(cfg)
  configured_ids <- vapply(collections, `[[`, character(1), "collection_id")
  selected_ids <- if (is.null(collection_ids) || !length(collection_ids)) {
    configured_ids
  } else {
    unique(as.character(collection_ids))
  }
  selected_ids <- selected_ids[!is.na(selected_ids) & nzchar(selected_ids)]
  unknown_ids <- setdiff(selected_ids, configured_ids)
  if (length(unknown_ids)) {
    stop(
      "Unknown collection_id value(s): ",
      paste(unknown_ids, collapse = ", "),
      call. = FALSE
    )
  }

  collection_results <- lapply(selected_ids, function(collection_id) {
    rebuilt_path <- NA_character_
    if (isTRUE(rebuild_collection_indexes)) {
      rebuilt_path <- litxr_rebuild_collection_index(collection_id, cfg)
    }
    c(
      list(
        collection_id = collection_id,
        rebuilt_collection_index = isTRUE(rebuild_collection_indexes),
        collection_local_path = if (isTRUE(rebuild_collection_indexes)) rebuilt_path else NA_character_
      ),
      list(
        refs_written = NA_integer_,
        links_written = NA_integer_,
        refs_removed = NA_integer_
      )
    )
  })

  inputs <- .litxr_authoritative_entity_inputs(cfg)
  refs <- inputs$refs
  links <- inputs$links
  identities <- .litxr_build_ref_identity_index(cfg, refs = refs)

  list(
    selected_collection_ids = selected_ids,
    collection_results = collection_results,
    project_paths = list(
      ref_identity_map = .litxr_project_ref_identity_index_path(cfg),
      ref_arxiv = .litxr_ref_arxiv_path(cfg),
      ref_doi = .litxr_ref_doi_path(cfg),
      ref_local_pending = .litxr_ref_local_pending_path(cfg)
    ),
    row_counts = list(
      project_references = nrow(refs),
      project_reference_collections = nrow(links),
      identities = nrow(identities)
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
