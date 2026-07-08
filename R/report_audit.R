.litxr_orphan_project_artifacts <- function(cfg, refs = NULL) {
  if (is.null(refs)) {
    refs <- .litxr_authoritative_project_records(cfg)
  }
  valid_ref_ids <- unique(as.character(refs$ref_id))
  valid_slugs <- if (length(valid_ref_ids)) {
    vapply(valid_ref_ids, function(x) .litxr_record_slug(data.table::data.table(ref_id = x, doi = NA_character_)), character(1))
  } else {
    character()
  }

  collect_files <- function(dir_path, pattern, artifact_type) {
    if (!dir.exists(dir_path)) {
      return(data.table::data.table(
        artifact_type = character(),
        artifact_ref_id = character(),
        path = character(),
        reason = character()
      ))
    }
    files <- list.files(dir_path, pattern = pattern, full.names = TRUE)
    if (!length(files)) {
      return(data.table::data.table(
        artifact_type = character(),
        artifact_ref_id = character(),
        path = character(),
        reason = character()
      ))
    }
    slugs <- sub(pattern, "", basename(files))
    keep <- !(slugs %in% valid_slugs)
    if (!any(keep)) {
      return(data.table::data.table(
        artifact_type = character(),
        artifact_ref_id = character(),
        path = character(),
        reason = character()
      ))
    }
    data.table::data.table(
      artifact_type = artifact_type,
      artifact_ref_id = NA_character_,
      path = files[keep],
      reason = "no_matching_ref_identity"
    )
  }

  data.table::rbindlist(list(
    collect_files(.litxr_project_llm_dir(cfg), "\\.json$", "llm_digest"),
    collect_files(.litxr_project_md_dir(cfg), "\\.md$", "fulltxt_md")
  ), fill = TRUE)
}

.litxr_audit_ambiguous_identity_joins <- function(refs) {
  if (!nrow(refs)) {
    return(data.table::data.table(
      join_type = character(),
      identity = character(),
      n_links = integer()
    ))
  }
  if (!("linked_doi_ref_id" %in% names(refs))) refs[["linked_doi_ref_id"]] <- rep(NA_character_, nrow(refs))
  if (!("linked_arxiv_ref_id" %in% names(refs))) refs[["linked_arxiv_ref_id"]] <- rep(NA_character_, nrow(refs))

  arxiv_keep <- grepl("^arxiv:", refs$ref_id) & !is.na(refs$linked_doi_ref_id) & nzchar(refs$linked_doi_ref_id)
  arxiv_to_doi <- data.table::data.table(
    join_type = rep("arxiv_to_doi", sum(arxiv_keep)),
    identity = refs$linked_doi_ref_id[arxiv_keep],
    linked_ref_id = refs$ref_id[arxiv_keep]
  )
  doi_keep <- grepl("^doi:", refs$ref_id) & !is.na(refs$linked_arxiv_ref_id) & nzchar(refs$linked_arxiv_ref_id)
  doi_to_arxiv <- data.table::data.table(
    join_type = rep("doi_to_arxiv", sum(doi_keep)),
    identity = refs$linked_arxiv_ref_id[doi_keep],
    linked_ref_id = refs$ref_id[doi_keep]
  )
  links <- data.table::rbindlist(list(arxiv_to_doi, doi_to_arxiv), fill = TRUE)
  if (!nrow(links)) {
    return(data.table::data.table(
      join_type = character(),
      identity = character(),
      n_links = integer()
    ))
  }
  split_keys <- paste(links$join_type, links$identity, sep = "\r")
  grouped <- split(links$linked_ref_id, split_keys)
  out <- data.table::rbindlist(lapply(names(grouped), function(key) {
    parts <- strsplit(key, "\r", fixed = TRUE)[[1]]
    data.table::data.table(
      join_type = parts[[1]],
      identity = parts[[2]],
      n_links = data.table::uniqueN(grouped[[key]])
    )
  }), fill = TRUE)
  out[which(out$n_links > 1L), , drop = FALSE]
}

.litxr_index_health_rows <- function(index_name, path, oversized_mb = 25, reader = c("fst", "encoded_fst")) {
  reader <- match.arg(reader)
  info <- if (file.exists(path)) file.info(path) else NULL
  readable <- if (!file.exists(path)) {
    FALSE
  } else if (identical(reader, "encoded_fst")) {
    !is.null(.litxr_read_index_columns_safe(path))
  } else {
    isTRUE(tryCatch({
      fst::metadata_fst(path)
      TRUE
    }, error = function(e) FALSE))
  }
  size_bytes <- if (is.null(info)) NA_real_ else as.numeric(info$size)
  data.table::data.table(
    index_name = index_name,
    path = path,
    exists = file.exists(path),
    readable = readable,
    size_bytes = size_bytes,
    size_mb = size_bytes / (1024^2),
    status = if (!file.exists(path)) "missing" else if (!readable) "damaged" else if (!is.na(size_bytes) && size_bytes / (1024^2) > oversized_mb) "oversized" else "ok"
  )
}

.litxr_thin_index_health_report <- function(cfg, oversized_mb = 25) {
  thin_paths <- data.table::data.table(
    index_name = c("ref_identity_map", "ref_arxiv", "ref_doi", "ref_isbn"),
    path = c(
      .litxr_project_ref_identity_index_path(cfg),
      .litxr_ref_arxiv_path(cfg),
      .litxr_ref_doi_path(cfg),
      .litxr_ref_isbn_path(cfg)
    ),
    reader = c("fst", "fst", "fst", "fst")
  )
  data.table::rbindlist(lapply(seq_len(nrow(thin_paths)), function(i) {
    .litxr_index_health_rows(
      index_name = as.character(thin_paths$index_name[[i]]),
      path = as.character(thin_paths$path[[i]]),
      oversized_mb = oversized_mb,
      reader = as.character(thin_paths$reader[[i]])
    )
  }), fill = TRUE)
}

.litxr_compatibility_projection_health_report <- function(cfg, oversized_mb = 25) {
  data.table::data.table(
    index_name = character(),
    path = character(),
    reader = character(),
    exists = logical(),
    readable = logical(),
    size_bytes = integer(),
    size_mb = numeric(),
    status = character()
  )
}

.litxr_audit_entity_indexes <- function(cfg, oversized_mb = 25) {
  refs <- .litxr_authoritative_project_records(cfg)
  identity_splits <- data.table::data.table(
    entity_id = character(),
    n_identities = integer(),
    ref_ids = character(),
    preferred_citation_ref_id = character()
  )

  thin_index_health <- .litxr_thin_index_health_report(cfg, oversized_mb = oversized_mb)
  compatibility_projection_health <- .litxr_compatibility_projection_health_report(cfg, oversized_mb = oversized_mb)

  list(
    identity_splits = identity_splits,
    ambiguous_identity_joins = .litxr_audit_ambiguous_identity_joins(refs),
    orphan_artifacts = .litxr_orphan_project_artifacts(cfg, refs = refs),
    index_health = thin_index_health,
    thin_index_health = thin_index_health,
    compatibility_projection_health = compatibility_projection_health
  )
}

.litxr_audit_reference_cache_state <- function(cfg) {
  collections <- .litxr_config_collections(cfg)
  collection_rows <- lapply(collections, function(collection) {
    cached_main <- tryCatch(.litxr_read_collection_records_for_collection(cfg, collection), error = function(e) data.table::data.table())
    authoritative <- tryCatch(.litxr_read_collection_records_for_collection(cfg, collection), error = function(e) data.table::data.table())
    cached_ref_ids <- if (is.null(cached_main) || !nrow(cached_main)) character() else unique(as.character(cached_main$ref_id))
    auth_ids <- if (!nrow(authoritative)) character() else unique(as.character(authoritative$ref_id))
    data.table::data.table(
      scope = "collection",
      scope_id = collection$collection_id,
      main_path = NA_character_,
      main_exists = FALSE,
      delta_exists = FALSE,
      authoritative_n = length(auth_ids),
      cached_main_n = length(cached_ref_ids),
      cached_delta_n = 0L,
      cached_merged_n = length(unique(c(cached_ref_ids))),
      main_missing_n = length(setdiff(auth_ids, cached_ref_ids)),
      main_extra_n = length(setdiff(cached_ref_ids, auth_ids)),
      merged_missing_n = length(setdiff(auth_ids, cached_ref_ids)),
      merged_extra_n = length(setdiff(cached_ref_ids, auth_ids))
    )
  })

  project_main <- tryCatch(.litxr_authoritative_project_records(cfg), error = function(e) data.table::data.table())
  project_authoritative <- tryCatch(.litxr_authoritative_project_records(cfg), error = function(e) data.table::data.table())
  main_ids <- if (!nrow(project_main)) character() else unique(as.character(project_main$ref_id))
  auth_ids <- if (!nrow(project_authoritative)) character() else unique(as.character(project_authoritative$ref_id))
  project_refs <- data.table::data.table(
    scope = "project_references",
    scope_id = "project",
    main_path = NA_character_,
    main_exists = FALSE,
    delta_exists = FALSE,
    authoritative_n = length(auth_ids),
    cached_main_n = length(main_ids),
    cached_delta_n = 0L,
    cached_merged_n = length(unique(main_ids)),
    main_missing_n = length(setdiff(auth_ids, main_ids)),
    main_extra_n = length(setdiff(main_ids, auth_ids)),
    merged_missing_n = length(setdiff(auth_ids, main_ids)),
    merged_extra_n = length(setdiff(main_ids, auth_ids))
  )

  project_links_main <- tryCatch(.litxr_authoritative_project_reference_links(cfg), error = function(e) data.table::data.table())
  project_links_merged <- project_links_main
  auth_links <- tryCatch(.litxr_authoritative_project_reference_links(cfg), error = function(e) data.table::data.table())
  link_key <- function(dt) if (!nrow(dt)) character() else paste(dt$ref_id, dt$collection_id, sep = "\r")
  main_link_keys <- link_key(project_links_main)
  merged_link_keys <- link_key(project_links_merged)
  auth_link_keys <- link_key(auth_links)
  project_links <- data.table::data.table(
    scope = "project_reference_collections",
    scope_id = "project",
    main_path = NA_character_,
    main_exists = FALSE,
    delta_exists = FALSE,
    authoritative_n = length(auth_link_keys),
    cached_main_n = length(main_link_keys),
    cached_delta_n = 0L,
    cached_merged_n = length(merged_link_keys),
    main_missing_n = length(setdiff(auth_link_keys, main_link_keys)),
    main_extra_n = length(setdiff(main_link_keys, auth_link_keys)),
    merged_missing_n = length(setdiff(auth_link_keys, merged_link_keys)),
    merged_extra_n = length(setdiff(merged_link_keys, auth_link_keys))
  )

  list(
    collection_reference_cache = data.table::rbindlist(collection_rows, fill = TRUE),
    project_reference_cache = data.table::rbindlist(list(project_refs, project_links), fill = TRUE)
  )
}

.litxr_audit_entity_status_state <- function(cfg) {
  identities <- data.table::as.data.table(litxr_read_ref_identity_map(cfg))
  refs <- .litxr_authoritative_project_records(cfg)
  if (nrow(refs)) {
    refs <- refs[, intersect(c("ref_id", "title", "doi", "linked_doi_ref_id", "linked_arxiv_ref_id", "entry_type", "year", "authors", "source"), names(refs)), with = FALSE]
  }
  expected <- .litxr_entity_status_rows_for_entities(
    cfg,
    entity_ids = if (nrow(identities)) unique(as.character(identities$entity_id)) else character(),
    refs = refs
  )
  actual <- expected

  expected_ids <- if (nrow(expected)) unique(as.character(expected$entity_id)) else character()
  actual_ids <- if (nrow(actual)) unique(as.character(actual$entity_id)) else character()

  missing <- if (length(setdiff(expected_ids, actual_ids))) {
    expected[expected$entity_id %in% setdiff(expected_ids, actual_ids), ]
  } else {
    .litxr_entity_status_empty()
  }
  orphan <- if (length(setdiff(actual_ids, expected_ids))) {
    actual[actual$entity_id %in% setdiff(actual_ids, expected_ids), ]
  } else {
    .litxr_entity_status_empty()
  }

  compare_cols <- c(
    "has_ref_json",
    "has_fulltxt_md",
    "has_llm_digest",
    "llm_paper_type",
    "has_standardized_findings",
    "n_standardized_findings",
    "has_descriptive_stats",
    "n_descriptive_stats",
    "has_anchor_references",
    "n_anchor_references",
    "has_citation_logic_nodes",
    "n_citation_logic_nodes",
    "digest_schema_version",
    "digest_revision_latest"
  )

  stale <- if (nrow(expected) && nrow(actual)) {
    merged <- merge(
      expected,
      actual,
      by = "entity_id",
      all = FALSE,
      suffixes = c(".expected", ".actual"),
      sort = FALSE
    )
    if (!nrow(merged)) {
      data.table::data.table()
    } else {
      stale_flags <- vapply(seq_len(nrow(merged)), function(i) {
        any(vapply(compare_cols, function(nm) {
          left <- merged[[paste0(nm, ".expected")]][[i]]
          right <- merged[[paste0(nm, ".actual")]][[i]]
          if (is.na(left) && is.na(right)) return(FALSE)
          !identical(left, right)
        }, logical(1)))
      }, logical(1))
      merged[stale_flags, ]
    }
  } else {
    data.table::data.table()
  }

  digest_mismatch <- if (nrow(stale)) {
    keep <- vapply(seq_len(nrow(stale)), function(i) {
      schema_diff <- {
        left <- stale$digest_schema_version.expected[[i]]
        right <- stale$digest_schema_version.actual[[i]]
        if (is.na(left) && is.na(right)) FALSE else !identical(left, right)
      }
      revision_diff <- {
        left <- stale$digest_revision_latest.expected[[i]]
        right <- stale$digest_revision_latest.actual[[i]]
        if (is.na(left) && is.na(right)) FALSE else !identical(left, right)
      }
      isTRUE(schema_diff) || isTRUE(revision_diff)
    }, logical(1))
    stale[keep, ]
  } else {
    data.table::data.table()
  }

  list(
    missing_entity_status = missing,
    orphan_entity_status = orphan,
    stale_entity_status = stale,
    digest_revision_mismatch = digest_mismatch
  )
}
