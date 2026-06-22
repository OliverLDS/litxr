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
  refs <- .litxr_authoritative_project_records(cfg)
  links <- .litxr_authoritative_project_reference_links(cfg)
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

.litxr_read_project_references_main_index <- function(cfg) {
  path <- .litxr_project_references_index_path(cfg)
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  .litxr_index_decode(fst::read_fst(path, as.data.table = TRUE))
}

.litxr_read_project_references_delta_index <- function(cfg) {
  path <- .litxr_project_references_delta_index_path(cfg)
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  .litxr_index_decode(fst::read_fst(path, as.data.table = TRUE))
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

.litxr_read_project_references_index <- function(cfg) {
  main <- .litxr_read_project_references_main_index(cfg)
  delta <- .litxr_read_project_references_delta_index(cfg)
  if (!nrow(delta)) {
    return(main)
  }
  .litxr_upsert_records(
    main,
    delta,
    conflict_path = .litxr_project_reference_conflict_path(cfg)
  )
}

.litxr_read_project_references_by_keys <- function(cfg, keys) {
  path <- .litxr_project_references_index_path(cfg)
  delta_path <- .litxr_project_references_delta_index_path(cfg)
  if (file.exists(delta_path)) {
    refs <- .litxr_read_project_references_index(cfg)
    source_id <- if ("source_id" %in% names(refs)) refs$source_id else rep(NA_character_, nrow(refs))
    out <- refs[refs$ref_id %in% keys | source_id %in% keys, ]
    return(.litxr_hydrate_project_projection_rows(cfg, out))
  }
  if (!file.exists(path)) {
    return(data.table::data.table())
  }

  index_columns <- .litxr_read_index_columns_safe(path)
  if (is.null(index_columns)) {
    warning(
      "Project reference index is damaged and will be rebuilt from collection indexes before keyed lookup: ",
      path,
      call. = FALSE
    )
    .litxr_rebuild_project_reference_indexes(cfg)
    refs <- .litxr_read_project_references_index(cfg)
    source_id <- if ("source_id" %in% names(refs)) refs$source_id else rep(NA_character_, nrow(refs))
    out <- refs[refs$ref_id %in% keys | source_id %in% keys, ]
    return(.litxr_hydrate_project_projection_rows(cfg, out))
  }
  if (!("ref_id" %in% index_columns) && !("source_id" %in% index_columns)) {
    refs <- .litxr_read_project_references_index(cfg)
    source_id <- if ("source_id" %in% names(refs)) refs$source_id else rep(NA_character_, nrow(refs))
    out <- refs[refs$ref_id %in% keys | source_id %in% keys, ]
    return(.litxr_hydrate_project_projection_rows(cfg, out))
  }

  idx <- integer()
  if ("ref_id" %in% index_columns) {
    ref_data <- fst::read_fst(path, columns = "ref_id", as.data.table = TRUE)
    idx <- which(ref_data$ref_id %in% keys)
  }
  if (!length(idx) && "source_id" %in% index_columns) {
    source_data <- fst::read_fst(path, columns = "source_id", as.data.table = TRUE)
    idx <- which(source_data$source_id %in% keys)
  }
  if (!length(idx)) {
    return(data.table::data.table())
  }

  rows <- lapply(idx, function(i) {
    .litxr_index_decode(fst::read_fst(path, from = i, to = i, as.data.table = TRUE))
  })
  out <- data.table::rbindlist(rows, fill = TRUE)
  key <- .litxr_upsert_key(out)
  out <- out[!duplicated(key), ]
  .litxr_hydrate_project_projection_rows(cfg, out)
}

.litxr_entity_index_maybe_refresh <- function(cfg) {
  alias_path <- .litxr_project_ref_aliases_index_path(cfg)
  entities_path <- .litxr_project_entities_index_path(cfg)
  entity_collections_path <- .litxr_project_entity_collections_index_path(cfg)
  entity_status_path <- .litxr_project_entity_status_index_path(cfg)
  refs_path <- .litxr_project_references_index_path(cfg)
  links_path <- .litxr_project_reference_collections_index_path(cfg)
  refs_delta_path <- .litxr_project_references_delta_index_path(cfg)
  links_delta_path <- .litxr_project_reference_collections_delta_index_path(cfg)
  needs_build <- !file.exists(alias_path) ||
    !file.exists(entities_path) ||
    !file.exists(entity_collections_path) ||
    !file.exists(entity_status_path)
  if (!needs_build && file.exists(refs_path) && file.exists(links_path)) {
    alias_mtime <- file.info(alias_path)$mtime
    refs_mtime <- file.info(refs_path)$mtime
    links_mtime <- file.info(links_path)$mtime
    needs_build <- isTRUE(alias_mtime < refs_mtime) || isTRUE(alias_mtime < links_mtime)
    if (!needs_build && file.exists(refs_delta_path)) {
      needs_build <- isTRUE(alias_mtime < file.info(refs_delta_path)$mtime)
    }
    if (!needs_build && file.exists(links_delta_path)) {
      needs_build <- isTRUE(alias_mtime < file.info(links_delta_path)$mtime)
    }
  }
  if (isTRUE(needs_build)) {
    .litxr_build_entity_indexes(cfg)
  }
  invisible(NULL)
}

.litxr_match_alias_rows_by_keys <- function(cfg, keys) {
  keys <- .litxr_expand_reference_keys(keys)
  if (!length(keys)) {
    return(data.table::data.table())
  }
  .litxr_entity_index_maybe_refresh(cfg)
  aliases <- .litxr_read_project_ref_aliases_index(cfg)
  if (!nrow(aliases)) {
    return(aliases)
  }
  source_id <- if ("source_id" %in% names(aliases)) aliases$source_id else rep(NA_character_, nrow(aliases))
  matched <- aliases[aliases$ref_id %in% keys | source_id %in% keys, ]
  if (!nrow(matched)) {
    return(matched)
  }
  matched_key <- rep(NA_character_, nrow(matched))
  ref_hit <- !is.na(matched$ref_id) & matched$ref_id %in% keys
  matched_key[ref_hit] <- matched$ref_id[ref_hit]
  source_hit <- !is.na(matched$source_id) &
    matched$source_id %in% keys &
    is.na(matched_key)
  if (any(source_hit)) {
    matched_key[source_hit] <- matched$source_id[source_hit]
  }
  matched[["litxr_matched_key__"]] <- matched_key

  entity_ids <- unique(as.character(matched$entity_id))
  entity_ids <- entity_ids[!is.na(entity_ids) & nzchar(entity_ids)]
  if (!length(entity_ids)) {
    return(matched)
  }

  expanded <- aliases[as.character(aliases$entity_id) %in% entity_ids, ]
  expanded[["litxr_matched_key__"]] <- rep(NA_character_, nrow(expanded))
  hit <- match(as.character(expanded$ref_id), as.character(matched$ref_id))
  matched_hit <- !is.na(hit)
  if (any(matched_hit)) {
    expanded$litxr_matched_key__[matched_hit] <- as.character(matched$litxr_matched_key__[hit[matched_hit]])
  }
  expanded
}

.litxr_filter_refs_by_alias_rows <- function(refs, alias_rows, keys) {
  if (!nrow(refs) || !nrow(alias_rows)) {
    return(refs[0, ])
  }
  keys <- .litxr_expand_reference_keys(keys)
  source_id <- if ("source_id" %in% names(refs)) refs$source_id else rep(NA_character_, nrow(refs))
  keep_ref_ids <- unique(as.character(alias_rows$ref_id))
  keep_source_ids <- unique(as.character(alias_rows$source_id))
  keep <- refs$ref_id %in% keep_ref_ids |
    source_id %in% keys |
    source_id %in% keep_source_ids
  refs[keep, ]
}

.litxr_read_project_references_by_alias_keys <- function(cfg, keys) {
  alias_rows <- .litxr_match_alias_rows_by_keys(cfg, keys)
  if (!nrow(alias_rows)) {
    return(data.table::data.table())
  }
  refs <- .litxr_read_project_references_index(cfg)
  if (!nrow(refs)) {
    return(refs)
  }
  out <- .litxr_filter_refs_by_alias_rows(refs, alias_rows, keys)
  if (!nrow(out)) {
    return(out)
  }
  key <- .litxr_upsert_key(out)
  out <- out[!duplicated(key), ]
  .litxr_hydrate_project_projection_rows(cfg, out)
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

.litxr_read_project_reference_collections_main_index <- function(cfg) {
  path <- .litxr_project_reference_collections_index_path(cfg)
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  fst::read_fst(path, as.data.table = TRUE)
}

.litxr_read_project_reference_collections_delta_index <- function(cfg) {
  path <- .litxr_project_reference_collections_delta_index_path(cfg)
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  fst::read_fst(path, as.data.table = TRUE)
}

.litxr_read_project_reference_collections_index <- function(cfg) {
  main <- .litxr_read_project_reference_collections_main_index(cfg)
  delta <- .litxr_read_project_reference_collections_delta_index(cfg)
  if (!nrow(delta)) {
    return(main)
  }
  if (!nrow(main)) {
    return(delta)
  }
  merged <- data.table::rbindlist(list(main, delta), fill = TRUE)
  link_key <- paste(merged$ref_id, merged$collection_id, sep = "\r")
  merged[!duplicated(link_key, fromLast = TRUE), ]
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
