#' Export local references to a BibTeX file
#'
#' @param output Output `.bib` file path.
#' @param journal_ids Optional character vector of collection ids to export. The
#'   argument name is kept for backward compatibility.
#' @param keys Optional character vector of record identifiers to export. Keys
#'   are matched against `doi`, `ref_id`, or `source_id`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the output path.
#' @export
litxr_export_bib <- function(output, journal_ids = NULL, keys = NULL, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  rows <- .litxr_export_bib_rows(cfg, journal_ids = journal_ids, keys = keys)

  if (!nrow(rows)) {
    writeLines(character(), output)
    return(invisible(output))
  }

  bib_lines <- unlist(lapply(seq_len(nrow(rows)), function(i) {
    c(row_to_bibtex(rows[i, ]), "")
  }), use.names = FALSE)

  writeLines(bib_lines, output)
  invisible(output)
}

.litxr_canonical_ref_rows_for_keys <- function(cfg, keys, journal_ids = NULL) {
  keys <- unique(unlist(lapply(keys, .litxr_lookup_candidates), use.names = FALSE))
  keys <- keys[!is.na(keys) & nzchar(keys)]
  if (!length(keys)) {
    return(data.table::data.table())
  }

  rows <- lapply(keys, function(key) {
    row <- tryCatch(
      litxr_find_refs(ref_id = key, config = cfg),
      error = function(e) data.table::data.table()
    )
    row
  })
  rows <- rows[vapply(rows, nrow, integer(1)) > 0L]
  if (!length(rows)) {
    return(data.table::data.table())
  }
  out <- data.table::rbindlist(rows, fill = TRUE)
  if (!nrow(out)) {
    return(out)
  }
  if (!is.null(journal_ids) && length(journal_ids) && "collection_id" %in% names(out)) {
    keep_collections <- unique(as.character(journal_ids))
    keep_collections <- keep_collections[!is.na(keep_collections) & nzchar(keep_collections)]
    if (length(keep_collections)) {
      out <- out[out$collection_id %in% keep_collections, ]
    } else {
      out <- out[0, ]
    }
  }
  if (nrow(out)) {
    out <- out[!duplicated(as.character(out$ref_id)), ]
  }
  out
}

.litxr_export_bib_rows <- function(cfg, journal_ids = NULL, keys = NULL) {
  if (!is.null(keys) && length(keys)) {
    refs <- .litxr_canonical_ref_rows_for_keys(cfg, keys = keys, journal_ids = journal_ids)
    if (!nrow(refs)) {
      return(refs[0, ])
    }
    return(refs)
  }

  if (!is.null(journal_ids) && length(journal_ids)) {
    refs_all <- data.table::rbindlist(lapply(unique(as.character(journal_ids)), function(journal_id) {
      if (is.na(journal_id) || !nzchar(journal_id)) {
        return(data.table::data.table())
      }
      tryCatch(
        litxr_read_collection(journal_id, cfg),
        error = function(e) data.table::data.table()
      )
    }), fill = TRUE)
  } else {
    refs_all <- .litxr_read_project_references_index(cfg)
  }

  if (!nrow(refs_all)) {
    return(refs_all[0, ])
  }
  refs_all[!duplicated(as.character(refs_all$ref_id)), ]
}

.litxr_preferred_rows_for_keys <- function(cfg, keys, journal_ids = NULL) {
  .litxr_canonical_ref_rows_for_keys(cfg, keys = keys, journal_ids = journal_ids)
}

.litxr_task_ref_row_for_keys <- function(cfg, keys, task = c("citation", "digest", "fulltext"), journal_ids = NULL) {
  task <- match.arg(task)
  rows <- .litxr_canonical_ref_rows_for_keys(cfg, keys = keys, journal_ids = journal_ids)
  if (!nrow(rows)) {
    return(rows)
  }
  if (identical(task, "citation")) {
    return(rows[1L, ])
  }

  ref_id <- as.character(rows$ref_id[[1L]])
  preferred_ref_id <- .litxr_task_ref_id(cfg, ref_id, task = task)
  if (is.na(preferred_ref_id) || !nzchar(preferred_ref_id)) {
    return(rows[1L, ])
  }

  hit <- rows[rows$ref_id == preferred_ref_id, ]
  if (nrow(hit)) {
    return(hit[1L, ])
  }
  rows[1L, ]
}

.litxr_task_ref_id <- function(cfg, ref_id, task = c("citation", "digest", "fulltext")) {
  task <- match.arg(task)
  ref_id <- as.character(ref_id %||% NA_character_)
  if (is.na(ref_id) || !nzchar(ref_id)) {
    return(NA_character_)
  }

  if (identical(task, "citation")) {
    rows <- .litxr_canonical_ref_rows_for_keys(cfg, ref_id)
    if (!nrow(rows)) return(NA_character_)
    return(as.character(rows$ref_id[[1L]]))
  }
  if (identical(task, "digest")) {
    digest <- tryCatch(litxr_read_llm_digest(ref_id, cfg), error = function(e) NULL)
    if (is.null(digest)) return(NA_character_)
    return(ref_id)
  }
  if (identical(task, "fulltext")) {
    rows <- .litxr_canonical_ref_rows_for_keys(cfg, ref_id)
    if (!nrow(rows)) return(NA_character_)
    if ("linked_arxiv_ref_id" %in% names(rows)) {
      linked <- as.character(rows$linked_arxiv_ref_id[[1L]])
      if (!is.na(linked) && nzchar(linked)) return(linked)
    }
    if (grepl("^arxiv:", ref_id, ignore.case = TRUE)) {
      return(ref_id)
    }
    return(NA_character_)
  }

  NA_character_
}

.litxr_attach_entity_ids_to_refs <- function(refs, aliases) {
  refs <- data.table::as.data.table(refs)
  if (!nrow(refs)) {
    if (!("entity_id" %in% names(refs))) {
      refs$entity_id <- character()
    }
    return(refs)
  }
  if ("entity_id" %in% names(refs)) {
    return(refs)
  }
  if (missing(aliases) || is.null(aliases) || !nrow(aliases)) {
    refs$entity_id <- rep(NA_character_, nrow(refs))
    return(refs)
  }
  alias_ref_ids <- as.character(aliases$ref_id)
  entity_ids <- as.character(aliases$entity_id)
  hit <- match(as.character(refs$ref_id), alias_ref_ids)
  refs$entity_id <- entity_ids[hit]
  refs
}

.litxr_filter_refs_by_collection_scope <- function(cfg, refs, collection_id, aliases = NULL, entity_links = NULL) {
  refs <- data.table::as.data.table(refs)
  collection_ids <- unique(as.character(collection_id))
  collection_ids <- collection_ids[!is.na(collection_ids) & nzchar(collection_ids)]
  if (!length(collection_ids) || !nrow(refs)) {
    return(refs[0, ])
  }
  if (is.null(aliases)) {
    aliases <- tryCatch(.litxr_ref_entity_resolution_map(cfg), error = function(e) data.table::data.table())
  }
  if (is.null(entity_links)) {
    entity_links <- .litxr_read_project_entity_collections_index(cfg)
  }
  if (!nrow(entity_links)) {
    return(refs[0, ])
  }
  scoped_entity_ids <- unique(as.character(
    entity_links$entity_id[entity_links$collection_id %in% collection_ids]
  ))
  scoped_entity_ids <- scoped_entity_ids[!is.na(scoped_entity_ids) & nzchar(scoped_entity_ids)]
  if (!length(scoped_entity_ids)) {
    return(refs[0, ])
  }
  out <- .litxr_attach_entity_ids_to_refs(refs, aliases = aliases)
  out <- out[out$entity_id %in% scoped_entity_ids, ]
  if (!nrow(out)) {
    return(out)
  }
  if ("collection_id" %in% names(out)) {
    collection_hit <- !is.na(out$collection_id) & out$collection_id %in% collection_ids
    if (any(collection_hit)) {
      out <- out[collection_hit, ]
      return(out)
    }
  }
  ref_links <- .litxr_read_project_compatibility_links_safe(cfg)
  if (nrow(ref_links)) {
    keep_ref_ids <- unique(as.character(ref_links$ref_id[ref_links$collection_id %in% collection_ids]))
    keep_ref_ids <- keep_ref_ids[!is.na(keep_ref_ids) & nzchar(keep_ref_ids)]
    if (length(keep_ref_ids)) {
      ref_hit <- as.character(out$ref_id) %in% keep_ref_ids
      if (any(ref_hit)) {
        out <- out[ref_hit, ]
      }
    }
  }
  out
}

.litxr_finalize_find_refs_rows <- function(cfg, rows, hydrate = FALSE) {
  out <- data.table::as.data.table(rows)
  if (!nrow(out)) {
    return(out)
  }
  key <- .litxr_upsert_key(out)
  out <- out[!duplicated(key), ]
  if ("entity_id" %in% names(out)) {
    out$entity_id <- NULL
  }
  if (isTRUE(hydrate)) {
    out <- .litxr_hydrate_project_projection_rows(cfg, out)
  }
  out
}

.litxr_read_project_compatibility_refs_safe <- function(cfg) {
  tryCatch(
    .litxr_read_project_references_index(cfg),
    error = function(e) {
      warning(
        "Project reference compatibility cache is unreadable and will be rebuilt from authoritative collection state: ",
        conditionMessage(e),
        call. = FALSE
      )
      .litxr_rebuild_project_reference_indexes(cfg)
      .litxr_read_project_references_index(cfg)
    }
  )
}

.litxr_read_project_compatibility_links_safe <- function(cfg) {
  tryCatch(
    .litxr_read_project_reference_collections_index(cfg),
    error = function(e) {
      warning(
        "Project reference-collection compatibility cache is unreadable and will be rebuilt from authoritative collection state: ",
        conditionMessage(e),
        call. = FALSE
      )
      .litxr_rebuild_project_reference_indexes(cfg)
      .litxr_read_project_reference_collections_index(cfg)
    }
  )
}

#' Read the canonical project-level reference index
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of canonical references across collections.
#' @export
litxr_read_references <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_read_project_compatibility_refs_safe(cfg)
}

#' Read the project-level reference-collection membership index
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of `ref_id` to `collection_id` links.
#' @export
litxr_read_reference_collections <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_read_project_compatibility_links_safe(cfg)
}

#' Read the thin alias-to-entity index
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` mapping surface `ref_id` aliases to internal
#'   `entity_id`s.
#' @export
litxr_read_ref_aliases <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_ref_entity_resolution_map(cfg)
}

#' Read the thin entity projection index
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of entity-level search and status projections.
#' @export
litxr_read_entities <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_read_project_entities_index(cfg)
}

#' Read the thin entity-to-collection membership index
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of `entity_id` to `collection_id` links.
#' @export
litxr_read_entity_collections <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_read_project_entity_collections_index(cfg)
}

#' Read the thin entity artifact-status index
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of entity-level artifact and digest status flags.
#' @export
litxr_read_entity_status <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_read_project_entity_status_index(cfg)
}

#' Build the thin entity indexes from current project stores
#'
#' This creates the first-stage v0.1.0 identity layer without changing current
#' reference read or search behavior. Existing scripts can continue using
#' `ref_id`, while internal migration work starts from `entity_id`-aware thin
#' indexes.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Named list describing the written index paths and row counts.
#' @export
litxr_build_entity_indexes <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_build_entity_indexes(cfg)
}

#' Audit reference cache state against authoritative JSON-backed storage
#'
#' Compares collection and project fst cache layers against the authoritative
#' JSON plus delta-backed record state used by the v0.1.0 refactor. This helps
#' identify stale or oversized caches before the old fst layers are fully
#' retired.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Named list of `data.table` audit reports.
#' @export
litxr_audit_reference_cache_state <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_audit_reference_cache_state(cfg)
}

#' Audit entity-status freshness against current project artifacts
#'
#' Compares `index/entity_status.fst` against the current md/digest/findings
#' artifact state derived from the authoritative alias/entity layer.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Named list of `data.table` audit reports.
#' @export
litxr_audit_entity_status_state <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_audit_entity_status_state(cfg)
}

#' Audit current stores against the thin entity-index design
#'
#' Reports current alias splits, ambiguous arXiv/DOI joins, orphan artifacts,
#' and index health. This is intended to guide the migration toward the v0.1.0
#' identity-first storage model.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param oversized_mb Size threshold in megabytes used to flag index files as
#'   oversized in the health report.
#'
#' @return Named list of `data.table` audit reports.
#' @export
litxr_audit_entity_indexes <- function(config = NULL, oversized_mb = 25) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_audit_entity_indexes(cfg, oversized_mb = oversized_mb)
}

#' Audit normalized authoritative store state
#'
#' Reports identity conflicts, orphan arXiv/DOI payload rows, unresolved local
#' pending rows, and runtime compatibility output freshness versus current
#' authoritative state.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Named list of `data.table` audit reports.
#' @export
litxr_audit_normalized_authoritative_state <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_normalized_authoritative_state_audit(cfg)
}

#' Diagnose current store readiness for the v0.1.0 refactor model
#'
#' Bundles the reference-cache, entity-index, and entity-status audits into one
#' operator-facing report so mixed or stale stores can be inspected without
#' calling low-level helpers one by one.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param oversized_mb Size threshold used when classifying indexes as
#'   oversized.
#'
#' @return Named list containing a one-row `summary` `data.table` plus the full
#'   audit payloads.
#' @export
litxr_refactor_diagnostics <- function(config = NULL, oversized_mb = 25) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_refactor_diagnostics(cfg, oversized_mb = oversized_mb)
}

#' Migrate current stores onto the thin v0.1.0 refactor indexes
#'
#' Rebuilds collection projection indexes, refreshes project projection caches,
#' and rebuilds alias/entity indexes so mixed older stores move onto the current
#' thin-index architecture without changing source JSON payloads.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param collection_ids Optional character vector of collection ids to refresh.
#'   Default is all configured collections.
#' @param rebuild_collection_indexes Whether to rebuild each selected collection
#'   index from `ref_json/` before refreshing project-level caches.
#'
#' @return Named list describing refreshed collections, written index paths, and
#'   resulting row counts.
#' @export
litxr_migrate_refactor_indexes <- function(config = NULL, collection_ids = NULL, rebuild_collection_indexes = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_migrate_refactor_indexes(
    cfg,
    collection_ids = collection_ids,
    rebuild_collection_indexes = rebuild_collection_indexes
  )
}

#' Find references in the canonical project-level store
#'
#' Provides simple deterministic filtering over the project-level reference
#' index, with optional collection membership filtering and substring matching.
#' Exact `ref_id` searches fall back to configured collection indexes when the
#' project-level index has not yet been refreshed.
#'
#' @param query Optional substring query matched against `title`, `authors`,
#'   `journal`, `container_title`, and `url`.
#' @param entry_type Optional BibTeX entry type filter.
#' @param year Optional year filter.
#' @param collection_id Optional collection membership filter.
#' @param doi Optional DOI filter.
#' @param ref_id Optional reference id filter. For arXiv records, both
#'   canonical ids such as `"arxiv:2405.03710"` and bare ids such as
#'   `"2405.03710"` are accepted.
#' @param isbn Optional ISBN filter.
#' @param issn Optional ISSN substring filter.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param runtime_wide_projection_limit Maximum number of project-reference rows
#'   that may be wide-materialized in one lookup before the call aborts. The
#'   default is `300`.
#'
#' @return Filtered `data.table` of references.
#' @export
litxr_find_refs <- function(
  query = NULL,
  entry_type = NULL,
  year = NULL,
  collection_id = NULL,
  doi = NULL,
  ref_id = NULL,
  isbn = NULL,
  issn = NULL,
  config = NULL,
  runtime_wide_projection_limit = 300L
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  old_wide_projection_limit <- getOption("litxr.runtime_wide_projection_limit", NULL)
  on.exit({
    if (is.null(old_wide_projection_limit)) {
      options(litxr.runtime_wide_projection_limit = NULL)
  } else {
    options(litxr.runtime_wide_projection_limit = old_wide_projection_limit)
  }
  }, add = TRUE)
  options(litxr.runtime_wide_projection_limit = runtime_wide_projection_limit)
  ref_keys <- if (!is.null(ref_id) && any(nzchar(as.character(ref_id)))) .litxr_expand_reference_keys(ref_id) else character()
  aliases <- if (length(ref_keys)) {
    tryCatch(.litxr_ref_entity_resolution_map(cfg, ref_ids = ref_keys), error = function(e) data.table::data.table())
  } else {
    tryCatch(.litxr_ref_entity_resolution_map(cfg), error = function(e) data.table::data.table())
  }
  entity_links <- .litxr_read_project_entity_collections_index(cfg)
  lookup_columns <- .litxr_project_reference_lookup_columns()
  target_collection <- NULL
  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    target_collection <- Filter(function(collection) {
      identical(collection$collection_id, as.character(collection_id[[1]])) ||
        identical(collection$journal_id, as.character(collection_id[[1]]))
    }, .litxr_config_collections(cfg))
  }
  exact_ref_only <- length(ref_keys) &&
    is.null(query) &&
    is.null(entry_type) &&
    is.null(year) &&
    is.null(collection_id) &&
    is.null(doi) &&
    is.null(isbn) &&
    is.null(issn)
  if (isTRUE(exact_ref_only)) {
    refs <- .litxr_read_project_references_by_keys(cfg, ref_keys, columns = lookup_columns, hydrate = FALSE)
    if (nrow(refs)) {
      refs <- .litxr_attach_entity_ids_to_refs(refs, aliases = aliases)
      if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
        refs <- .litxr_filter_refs_by_collection_scope(
          cfg,
          refs,
          collection_id = collection_id,
          aliases = aliases,
          entity_links = entity_links
        )
      }
      if (nrow(refs)) {
        return(.litxr_finalize_find_refs_rows(cfg, refs, hydrate = TRUE))
      }
    }
    return(data.table::data.table())
  }

  refs <- .litxr_read_project_references_index(cfg, columns = lookup_columns)
  if (!nrow(refs)) {
    if (length(ref_keys)) {
      return(.litxr_find_collection_refs_by_keys(cfg, ref_keys, collection_id = collection_id))
    }
    return(refs)
  }
  if (length(ref_keys)) {
    aliases <- tryCatch(.litxr_ref_entity_resolution_map(cfg, ref_ids = refs$ref_id), error = function(e) data.table::data.table())
  }
  refs <- .litxr_attach_entity_ids_to_refs(refs, aliases = aliases)

  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    refs <- .litxr_filter_refs_by_collection_scope(
      cfg,
      refs,
      collection_id = collection_id,
      aliases = aliases,
      entity_links = entity_links
    )
  }

  if (!is.null(entry_type) && nzchar(as.character(entry_type))) {
    refs <- refs[refs$entry_type %in% entry_type, ]
  }
  if (!is.null(year)) {
    refs <- refs[refs$year %in% as.integer(year), ]
  }
  if (!is.null(doi) && nzchar(as.character(doi))) {
    refs <- refs[refs$doi %in% doi, ]
  }
  if (!is.null(ref_id) && any(nzchar(as.character(ref_id)))) {
    direct_refs <- refs[refs$ref_id %in% ref_keys, ]
    if (nrow(direct_refs)) {
      refs <- direct_refs
    } else {
      return(data.table::data.table())
    }
  }
  if (!is.null(isbn) && nzchar(as.character(isbn))) {
    refs <- refs[refs$isbn %in% isbn, ]
  }
  if (!is.null(issn) && nzchar(as.character(issn))) {
    refs <- refs[!is.na(refs$issn) & grepl(as.character(issn), refs$issn, fixed = TRUE), ]
  }

  if (!is.null(query) && nzchar(as.character(query))) {
    q <- tolower(as.character(query[[1]]))
    title <- if ("title" %in% names(refs)) refs$title else rep(NA_character_, nrow(refs))
    authors <- if ("authors" %in% names(refs)) refs$authors else rep(NA_character_, nrow(refs))
    journal <- if ("journal" %in% names(refs)) refs$journal else rep(NA_character_, nrow(refs))
    container_title <- if ("container_title" %in% names(refs)) refs$container_title else rep(NA_character_, nrow(refs))
    url <- if ("url" %in% names(refs)) refs$url else rep(NA_character_, nrow(refs))

    haystack_match <- function(x) !is.na(x) & grepl(q, tolower(x), fixed = TRUE)
    keep <- haystack_match(title) |
      haystack_match(authors) |
      haystack_match(journal) |
      haystack_match(container_title) |
      haystack_match(url)
    refs <- refs[keep, ]
  }

  .litxr_finalize_find_refs_rows(cfg, refs, hydrate = TRUE)
}
