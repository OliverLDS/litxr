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

.litxr_runtime_project_compatibility_projection <- function(cfg) {
  list(
    refs = .litxr_authoritative_project_records(cfg),
    links = .litxr_authoritative_project_reference_links(cfg)
  )
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
    .litxr_write_project_references_index(cfg, data.table::data.table())
    .litxr_write_project_reference_collections_index(
      cfg,
      data.table::data.table(
        ref_id = character(),
        collection_id = character(),
        collection_title = character(),
        recorded_at = character()
      )
    )
    return(invisible(NULL))
  }

  refs_list <- list()
  links_list <- list()

  for (collection in collections) {
    projection <- .litxr_project_projection_from_collection(
      cfg,
      collection,
      repair_collection_index = repair_collection_indexes
    )
    if (nrow(projection$refs)) {
      refs_list[[length(refs_list) + 1L]] <- projection$refs
      links_list[[length(links_list) + 1L]] <- projection$links
    }
  }

  refs <- if (length(refs_list)) data.table::rbindlist(refs_list, fill = TRUE) else data.table::data.table()
  links <- if (length(links_list)) data.table::rbindlist(links_list, fill = TRUE) else data.table::data.table(
    ref_id = character(),
    collection_id = character(),
    collection_title = character(),
    recorded_at = character()
  )

  if (nrow(refs)) {
    refs <- .litxr_upsert_records(
      refs[0, ],
      refs,
      conflict_path = .litxr_project_reference_conflict_path(cfg)
    )
  }
  if (nrow(links)) {
    link_key <- paste(links$ref_id, links$collection_id, sep = "\r")
    links <- links[!duplicated(link_key), ]
  }

  .litxr_write_project_references_index(cfg, refs)
  .litxr_write_project_reference_collections_index(cfg, links)
  .litxr_clear_project_reference_deltas(cfg)
  .litxr_refresh_entity_indexes_from_project_indexes(cfg)
  .litxr_refresh_normalized_reference_scaffold(cfg, refresh_entity_indexes = TRUE)
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

.litxr_read_project_references_main_index <- function(cfg, columns = NULL) {
  path <- .litxr_project_references_index_path(cfg)
  .litxr_index_decode(.litxr_read_fst_subset(path, columns = columns))
}

.litxr_read_project_references_delta_index <- function(cfg, columns = NULL) {
  path <- .litxr_project_references_delta_index_path(cfg)
  .litxr_index_decode(.litxr_read_fst_subset(path, columns = columns))
}

.litxr_read_collection_projection_main_index_safe <- function(local_path) {
  path <- .litxr_index_path(local_path)
  if (!file.exists(path)) {
    return(NULL)
  }
  index_columns <- .litxr_read_index_columns_safe(path)
  if (is.null(index_columns)) {
    return(NULL)
  }
  .litxr_index_decode(fst::read_fst(path, as.data.table = TRUE))
}

.litxr_read_collection_projection_index_safe <- function(local_path) {
  main <- .litxr_read_collection_projection_main_index_safe(local_path)
  if (is.null(main)) {
    return(NULL)
  }
  delta <- .litxr_read_collection_delta(local_path)
  if (!nrow(delta)) {
    return(main)
  }
  .litxr_upsert_records(
    main,
    delta,
    conflict_path = file.path(.litxr_journal_paths(local_path)$json, "_upsert_conflicts.jsonl")
  )
}

.litxr_project_projection_from_collection <- function(cfg, journal, repair_collection_index = TRUE) {
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  projection <- .litxr_read_collection_projection_index_safe(local_path)

  if (is.null(projection)) {
    records <- .litxr_read_journal_records_authoritative(local_path)
    projection <- .litxr_project_references_from_collection_records(records)
    if (isTRUE(repair_collection_index)) {
      .litxr_write_journal_index(records, local_path)
    }
  } else {
    projection <- .litxr_project_references_from_collection_records(projection)
  }

  links <- .litxr_project_reference_links_from_collection_records(projection, journal)
  list(refs = projection, links = links)
}

.litxr_read_project_references_index <- function(cfg, columns = NULL) {
  refs <- .litxr_runtime_project_compatibility_projection(cfg)$refs
  if (!nrow(refs)) {
    return(refs)
  }
  if (is.null(columns) || !length(columns)) {
    return(refs)
  }
  keep <- intersect(columns, names(refs))
  if (!length(keep)) {
    return(refs[, 0, with = FALSE])
  }
  refs[, keep, with = FALSE]
}

.litxr_read_project_references_by_keys <- function(cfg, keys, columns = NULL, hydrate = FALSE, wide_projection_limit = 300L, keyed_fst_read_threshold = getOption("litxr.keyed_fst_read_threshold", 32L)) {
  read_columns <- .litxr_project_reference_lookup_columns(columns)
  refs <- .litxr_read_project_references_index(cfg, columns = read_columns)
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

.litxr_entity_index_maybe_refresh <- function(cfg) {
  entities_path <- .litxr_project_entities_index_path(cfg)
  entity_collections_path <- .litxr_project_entity_collections_index_path(cfg)
  entity_status_path <- .litxr_project_entity_status_index_path(cfg)
  collections <- .litxr_config_collections(cfg)
  needs_build <- !file.exists(entities_path) ||
    !file.exists(entity_collections_path) ||
    !file.exists(entity_status_path)
  if (!needs_build && length(collections)) {
    entity_mtime <- file.info(entities_path)$mtime
    collection_mtimes <- vapply(collections, function(collection) {
      local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
      index_path <- .litxr_index_path(local_path)
      if (!file.exists(index_path)) {
        return(as.POSIXct(NA))
      }
      file.info(index_path)$mtime
    }, as.POSIXct(NA))
    if (length(collection_mtimes)) {
      needs_build <- any(vapply(collection_mtimes, function(x) isTRUE(entity_mtime < x), logical(1)))
    }
  }
  if (isTRUE(needs_build)) {
    .litxr_build_entity_indexes(cfg)
  }
  invisible(NULL)
}

.litxr_write_project_references_index <- function(cfg, records) {
  .litxr_ensure_project_index_dir(cfg)
  fst::write_fst(
    as.data.frame(.litxr_index_encode(.litxr_reference_projection(records))),
    .litxr_project_references_index_path(cfg)
  )
  invisible(.litxr_project_references_index_path(cfg))
}

.litxr_append_project_references_delta <- function(cfg, records) {
  .litxr_ensure_project_index_dir(cfg)
  existing <- .litxr_read_project_references_delta_index(cfg)
  delta <- .litxr_upsert_records(
    existing,
    .litxr_reference_projection(records),
    conflict_path = .litxr_project_reference_conflict_path(cfg)
  )
  delta <- .litxr_reference_projection(delta)
  fst::write_fst(as.data.frame(.litxr_index_encode(delta)), .litxr_project_references_delta_index_path(cfg))
  invisible(.litxr_project_references_delta_index_path(cfg))
}

.litxr_read_project_reference_collections_main_index <- function(cfg, columns = NULL) {
  path <- .litxr_project_reference_collections_index_path(cfg)
  .litxr_read_fst_subset(path, columns = columns)
}

.litxr_read_project_reference_collections_delta_index <- function(cfg, columns = NULL) {
  path <- .litxr_project_reference_collections_delta_index_path(cfg)
  .litxr_read_fst_subset(path, columns = columns)
}

.litxr_read_project_reference_collections_index <- function(cfg, columns = NULL) {
  links <- .litxr_runtime_project_compatibility_projection(cfg)$links
  if (!nrow(links)) {
    return(links)
  }
  if (is.null(columns) || !length(columns)) {
    return(links)
  }
  keep <- intersect(columns, names(links))
  if (!length(keep)) {
    return(links[, 0, with = FALSE])
  }
  links[, keep, with = FALSE]
}

.litxr_write_project_reference_collections_index <- function(cfg, links) {
  .litxr_ensure_project_index_dir(cfg)
  fst::write_fst(as.data.frame(links), .litxr_project_reference_collections_index_path(cfg))
  invisible(.litxr_project_reference_collections_index_path(cfg))
}

.litxr_append_project_reference_collections_delta <- function(cfg, links) {
  .litxr_ensure_project_index_dir(cfg)
  existing <- .litxr_read_project_reference_collections_delta_index(cfg)
  if (!nrow(existing)) {
    merged <- links
  } else {
    merged <- data.table::rbindlist(list(existing, links), fill = TRUE)
    link_key <- paste(merged$ref_id, merged$collection_id, sep = "\r")
    merged <- merged[!duplicated(link_key, fromLast = TRUE), ]
  }
  fst::write_fst(as.data.frame(merged), .litxr_project_reference_collections_delta_index_path(cfg))
  invisible(.litxr_project_reference_collections_delta_index_path(cfg))
}

.litxr_clear_project_reference_deltas <- function(cfg) {
  for (path in c(
    .litxr_project_references_delta_index_path(cfg),
    .litxr_project_reference_collections_delta_index_path(cfg)
  )) {
    if (file.exists(path)) unlink(path)
  }
  invisible(TRUE)
}

.litxr_refresh_entity_indexes_from_project_indexes <- function(cfg) {
  inputs <- .litxr_authoritative_entity_inputs(cfg)
  refs <- inputs$refs
  links <- inputs$links
  aliases <- .litxr_build_ref_aliases_index(cfg, refs = refs)
  .litxr_build_entities_index(cfg, refs = refs, aliases = aliases)
  .litxr_build_entity_collections_index(cfg, aliases = aliases, links = links)
  .litxr_build_entity_status_index(cfg, aliases = aliases, refs = refs)
  invisible(NULL)
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
    refresh <- .litxr_refresh_project_index_for_collection(
      cfg,
      collection_id = collection_id,
      repair_collection_index = FALSE
    )
    c(
      list(
        collection_id = collection_id,
        rebuilt_collection_index = isTRUE(rebuild_collection_indexes),
        collection_index_path = if (isTRUE(rebuild_collection_indexes)) rebuilt_path else NA_character_
      ),
      refresh
    )
  })

  inputs <- .litxr_authoritative_entity_inputs(cfg)
  refs <- inputs$refs
  links <- inputs$links
  aliases <- .litxr_build_ref_aliases_index(cfg, refs = refs)
  entities <- .litxr_build_entities_index(cfg, refs = refs, aliases = aliases)
  entity_collections <- .litxr_build_entity_collections_index(cfg, aliases = aliases, links = links)
  entity_status <- .litxr_build_entity_status_index(cfg, aliases = aliases, refs = refs)

  list(
    selected_collection_ids = selected_ids,
    collection_results = collection_results,
    project_paths = list(
      references = .litxr_project_references_index_path(cfg),
      reference_collections = .litxr_project_reference_collections_index_path(cfg),
      ref_aliases = .litxr_project_ref_aliases_index_path(cfg),
      entities = .litxr_project_entities_index_path(cfg),
      entity_collections = .litxr_project_entity_collections_index_path(cfg),
      entity_status = .litxr_project_entity_status_index_path(cfg)
    ),
    row_counts = list(
      project_references = nrow(refs),
      project_reference_collections = nrow(links),
      ref_aliases = nrow(aliases),
      entities = nrow(entities),
      entity_collections = nrow(entity_collections),
      entity_status = nrow(entity_status)
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
    alias_split_entities = as.integer(nrow(entity_indexes$alias_splits)),
    ambiguous_alias_joins = as.integer(nrow(entity_indexes$ambiguous_alias_joins)),
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
