#' Sync one configured collection
#'
#' Fetches records from the configured remote channel, parses them into the
#' unified reference schema, and stores one JSON metadata file per record under
#' the collection's local path.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of synced records.
#' @export
litxr_sync_collection <- function(collection_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  started_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  incoming <- switch(
    journal$remote_channel,
    crossref = .litxr_sync_crossref_journal(journal),
    arxiv = .litxr_sync_arxiv_journal(journal),
    stop("Unsupported remote channel: ", journal$remote_channel, call. = FALSE)
  )

  existing <- .litxr_read_journal_records(local_path)
  records <- .litxr_upsert_journal_records(existing, incoming, local_path = local_path)

  .litxr_write_journal_records(records, local_path, journal, cfg = cfg)
  .litxr_append_sync_state(cfg, .litxr_make_sync_state_row(
    collection_id = if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id,
    remote_channel = journal$remote_channel,
    sync_type = "full",
    status = "success",
    started_at = started_at,
    completed_at = format(Sys.time(), tz = "UTC", usetz = TRUE),
    query = .litxr_sync_query_text(journal),
    range_from = NA_character_,
    range_to = NA_character_,
    fetched_from = .litxr_records_date_range(incoming)$from,
    fetched_to = .litxr_records_date_range(incoming)$to,
    page_start = if (identical(journal$remote_channel, "arxiv")) journal$sync$start %||% 0L else NA_integer_,
    page_size = journal$sync$rows %||% if (identical(journal$remote_channel, "crossref")) 1000L else 100L,
    records_fetched = nrow(incoming),
    records_after = nrow(records),
    notes = ""
  ))
  records
}

#' Sync one configured journal
#'
#' Backward-compatible wrapper around `litxr_sync_collection()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of synced records.
#' @export
litxr_sync_journal <- function(journal_id, config = NULL) {
  litxr_sync_collection(journal_id, config = config)
}

#' Sync all configured collections
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Named list of synced `data.table`s.
#' @export
litxr_sync_all <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  collections <- .litxr_config_collections(cfg)
  stats::setNames(
    lapply(collections, function(journal) litxr_sync_collection(journal$collection_id, cfg)),
    vapply(collections, `[[`, character(1), "collection_id")
  )
}

#' Read locally stored records for one configured collection
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of local records.
#' @export
litxr_read_collection <- function(collection_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  records <- .litxr_read_journal_records(local_path)
  delta <- .litxr_read_collection_delta(local_path)
  if (nrow(delta)) {
    return(.litxr_upsert_records(records, delta))
  }
  records
}

#' Summarize publication-date coverage for one collection
#'
#' Returns counts by day, month, or year based on `pub_date`, with summary
#' attributes describing the observed date range and any missing calendar days
#' within that observed range.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param by One of `"day"`, `"month"`, or `"year"`.
#'
#' @return `data.table` with `date` and `n`, plus attributes
#'   `date_min`, `date_max`, and `missing_dates`.
#' @export
litxr_collection_date_stats <- function(collection_id, config = NULL, by = c("day", "month", "year")) {
  by <- match.arg(by)
  records <- litxr_read_collection(collection_id, config = config)

  empty_out <- data.table::data.table(date = character(), n = integer())
  attr(empty_out, "date_min") <- NA_character_
  attr(empty_out, "date_max") <- NA_character_
  attr(empty_out, "missing_dates") <- as.Date(character())

  if (!nrow(records) || !("pub_date" %in% names(records))) {
    return(empty_out)
  }

  pub_dates <- as.Date(records$pub_date)
  pub_dates <- pub_dates[!is.na(pub_dates)]
  if (!length(pub_dates)) {
    return(empty_out)
  }

  bucket_dates <- switch(
    by,
    day = pub_dates,
    month = as.Date(format(pub_dates, "%Y-%m-01")),
    year = as.Date(paste0(format(pub_dates, "%Y"), "-01-01"))
  )

  counts <- sort(table(bucket_dates))
  out <- data.table::data.table(
    date = as.Date(names(counts)),
    n = as.integer(counts)
  )
  data.table::setorder(out, date)

  attr(out, "date_min") <- as.character(min(pub_dates))
  attr(out, "date_max") <- as.character(max(pub_dates))
  attr(out, "missing_dates") <- if (identical(by, "day")) {
    setdiff(seq(min(pub_dates), max(pub_dates), by = "day"), sort(unique(pub_dates)))
  } else {
    as.Date(character())
  }

  out
}

#' Read locally stored records for one journal
#'
#' Backward-compatible wrapper around `litxr_read_collection()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of local records.
#' @export
litxr_read_journal <- function(journal_id, config = NULL) {
  litxr_read_collection(journal_id, config = config)
}

#' Repair or incrementally fill a collection's local store
#'
#' Intended for bounded repair runs that should respect remote rate limits. For
#' arXiv journals, this can narrow the sync to a submitted-date window and/or a
#' smaller batch.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param search_query Optional replacement query for this repair run.
#' @param submitted_from Optional lower bound date/time for arXiv submittedDate.
#' @param submitted_to Optional upper bound date/time for arXiv submittedDate.
#' @param start Optional arXiv result offset for this repair run.
#' @param limit Optional maximum number of records for this repair run.
#'
#' @return `data.table` of repaired or newly synced records.
#' @export
litxr_repair_collection <- function(
  collection_id,
  config = NULL,
  search_query = NULL,
  submitted_from = NULL,
  submitted_to = NULL,
  start = NULL,
  limit = NULL
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  started_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)

  if (identical(journal$remote_channel, "arxiv")) {
    base_query <- if (is.null(search_query)) journal$sync$search_query else search_query
    journal$sync$search_query <- .litxr_build_arxiv_search_query(base_query, submitted_from, submitted_to)
    if (!is.null(start)) journal$sync$start <- as.integer(start)
    if (!is.null(limit)) journal$sync$limit <- as.integer(limit)
  } else {
    if (!is.null(limit)) journal$sync$limit <- as.integer(limit)
  }

  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  incoming <- switch(
    journal$remote_channel,
    crossref = .litxr_sync_crossref_journal(journal),
    arxiv = .litxr_sync_arxiv_journal(journal),
    stop("Unsupported remote channel: ", journal$remote_channel, call. = FALSE)
  )

  existing <- .litxr_read_journal_records(local_path)
  records <- .litxr_write_journal_upserted_records(existing, incoming, local_path, journal, cfg = cfg)
  .litxr_append_sync_state(cfg, .litxr_make_sync_state_row(
    collection_id = if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id,
    remote_channel = journal$remote_channel,
    sync_type = "repair",
    status = "success",
    started_at = started_at,
    completed_at = format(Sys.time(), tz = "UTC", usetz = TRUE),
    query = .litxr_sync_query_text(journal),
    range_from = submitted_from %||% NA_character_,
    range_to = submitted_to %||% NA_character_,
    fetched_from = .litxr_records_date_range(incoming)$from,
    fetched_to = .litxr_records_date_range(incoming)$to,
    page_start = if (identical(journal$remote_channel, "arxiv")) journal$sync$start %||% 0L else NA_integer_,
    page_size = journal$sync$rows %||% if (identical(journal$remote_channel, "crossref")) 1000L else 100L,
    records_fetched = nrow(incoming),
    records_after = nrow(records),
    notes = ""
  ))
  records
}

#' Repair or incrementally fill a journal's local store
#'
#' Backward-compatible wrapper around `litxr_repair_collection()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param search_query Optional replacement query for this repair run.
#' @param submitted_from Optional lower bound date/time for arXiv submittedDate.
#' @param submitted_to Optional upper bound date/time for arXiv submittedDate.
#' @param start Optional arXiv result offset for this repair run.
#' @param limit Optional maximum number of records for this repair run.
#'
#' @return `data.table` of repaired or newly synced records.
#' @export
litxr_repair_journal <- function(
  journal_id,
  config = NULL,
  search_query = NULL,
  submitted_from = NULL,
  submitted_to = NULL,
  start = NULL,
  limit = NULL
) {
  litxr_repair_collection(
    journal_id,
    config = config,
    search_query = search_query,
    submitted_from = submitted_from,
    submitted_to = submitted_to,
    start = start,
    limit = limit
  )
}

#' Rebuild the local collection fst index from JSON files
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the rebuilt fst index path.
#' @export
litxr_rebuild_collection_index <- function(collection_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  records <- .litxr_read_journal_records_from_json(local_path)
  index_path <- .litxr_write_journal_index(records, local_path)
  .litxr_update_project_indexes(cfg, journal, records)
  invisible(index_path)
}

#' Refresh the local collection fst index from recently changed JSON files
#'
#' This is a fast, mtime-based refresh for the common case where JSON record
#' files were written after `index/references.fst` was last updated. It does
#' not replace `litxr_rebuild_collection_index()`, which remains the
#' correctness-first full rebuild for schema migrations and legacy cleanup.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the refreshed fst index path.
#' @export
litxr_refresh_collection_index <- function(collection_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  paths <- .litxr_journal_paths(local_path)
  index_path <- .litxr_index_path(local_path)

  existing <- .litxr_read_journal_index(local_path)
  if (is.null(existing) || !file.exists(index_path)) {
    return(litxr_rebuild_collection_index(collection_id, cfg))
  }

  if (!dir.exists(paths$json)) {
    return(invisible(index_path))
  }

  files <- sort(list.files(paths$json, pattern = "\\.json$", full.names = TRUE))
  if (!length(files)) {
    return(invisible(index_path))
  }

  index_mtime <- file.info(index_path)$mtime
  changed_files <- files[file.info(files)$mtime > index_mtime]
  if (!length(changed_files)) {
    return(invisible(index_path))
  }

  incoming <- .litxr_prefer_complete_records(
    data.table::rbindlist(lapply(changed_files, .litxr_storage_payload_to_row), fill = TRUE)
  )
  records <- .litxr_upsert_records(
    existing,
    incoming,
    conflict_path = file.path(paths$json, "_upsert_conflicts.jsonl")
  )
  index_path <- .litxr_write_journal_index(records, local_path)
  .litxr_update_project_indexes(cfg, journal, records)
  invisible(index_path)
}

#' Compact pending collection delta records into the main fst index
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param refresh_project_index Whether to refresh project-level canonical
#'   reference indexes after compaction.
#'
#' @return Invisibly returns the compacted fst index path.
#' @export
litxr_compact_collection_index <- function(collection_id, config = NULL, refresh_project_index = FALSE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, collection_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)

  records <- .litxr_read_journal_records(local_path)
  delta <- .litxr_read_collection_delta(local_path)
  delta_keys <- character()
  if (nrow(delta)) {
    delta_keys <- .litxr_upsert_key(delta)
    records <- .litxr_upsert_records(
      records,
      delta,
      conflict_path = file.path(.litxr_journal_paths(local_path)$json, "_upsert_conflicts.jsonl")
    )
  }

  index_path <- .litxr_write_journal_index(records, local_path)
  if (length(delta_keys)) {
    records_keys <- .litxr_upsert_key(records)
    touched_records <- records[records_keys %in% delta_keys, ]
    .litxr_write_journal_record_files(touched_records, local_path, journal)
  }
  .litxr_clear_collection_delta(local_path)

  if (isTRUE(refresh_project_index)) {
    .litxr_update_project_indexes(cfg, journal, records)
  }

  invisible(index_path)
}

#' Rebuild the local journal fst index from JSON files
#'
#' Backward-compatible wrapper around `litxr_rebuild_collection_index()`.
#'
#' @param journal_id Collection identifier from `config.yaml`. The argument name
#'   is kept for backward compatibility.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the rebuilt fst index path.
#' @export
litxr_rebuild_journal_index <- function(journal_id, config = NULL) {
  litxr_rebuild_collection_index(journal_id, config = config)
}

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
  selected <- .litxr_config_collections(cfg)

  if (!is.null(journal_ids)) {
    keep <- vapply(selected, function(x) x$collection_id %in% journal_ids, logical(1))
    selected <- selected[keep]
  }

  if (!length(selected)) {
    writeLines(character(), output)
    return(invisible(output))
  }

  rows <- data.table::rbindlist(lapply(selected, function(journal) {
    .litxr_read_journal_records(.litxr_resolve_local_path(cfg, journal$local_path))
  }), fill = TRUE)

  if (!is.null(keys) && length(keys)) {
    key_values <- unique(as.character(keys))
    filtered <- .litxr_filter_records_by_keys(rows, key_values)
    missing_keys <- setdiff(key_values, filtered$litxr_matched_key__)
    if (length(missing_keys)) {
      warning(
        "The following record keys were not found and were ignored: ",
        paste(missing_keys, collapse = ", "),
        call. = FALSE
      )
    }
    filtered[["litxr_matched_key__"]] <- NULL
    rows <- filtered
  }

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
  .litxr_read_project_references_index(cfg)
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
  .litxr_read_project_reference_collections_index(cfg)
}

#' Find references in the canonical project-level store
#'
#' Provides simple deterministic filtering over the project-level reference
#' index, with optional collection membership filtering and substring matching.
#'
#' @param query Optional substring query matched against `title`, `authors`,
#'   `journal`, `container_title`, and `url`.
#' @param entry_type Optional BibTeX entry type filter.
#' @param year Optional year filter.
#' @param collection_id Optional collection membership filter.
#' @param doi Optional DOI filter.
#' @param ref_id Optional internal reference id filter.
#' @param isbn Optional ISBN filter.
#' @param issn Optional ISSN substring filter.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
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
  config = NULL
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  refs <- litxr_read_references(cfg)
  if (!nrow(refs)) {
    return(refs)
  }

  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    links <- litxr_read_reference_collections(cfg)
    keep_ref_ids <- unique(links$ref_id[links$collection_id == collection_id])
    refs <- refs[refs$ref_id %in% keep_ref_ids, ]
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
  if (!is.null(ref_id) && nzchar(as.character(ref_id))) {
    refs <- refs[refs$ref_id %in% ref_id, ]
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

  refs
}

#' Create a default structured LLM digest template
#'
#' @param ref_id Reference identifier.
#'
#' @return Named list representing the default digest schema.
#' @export
litxr_llm_digest_template <- function(ref_id) {
  list(
    ref_id = as.character(ref_id),
    summary = NA_character_,
    motivation = NA_character_,
    research_questions = character(),
    methods = character(),
    sample = list(
      description = NA_character_,
      size = NA_character_,
      period = NA_character_
    ),
    key_findings = character(),
    limitations = character(),
    keywords = character(),
    notes = NA_character_,
    generated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )
}

#' Write one LLM digest for a reference
#'
#' Stores a structured JSON file under the project-level `llm/` directory using
#' `ref_id` as the canonical key.
#'
#' @param ref_id Reference identifier.
#' @param digest Named list containing digest fields.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the written JSON path.
#' @export
litxr_write_llm_digest <- function(ref_id, digest, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  refs <- litxr_read_references(cfg)
  if (nrow(refs) && !(ref_id %in% refs$ref_id)) {
    warning("Reference id not found in canonical store: ", ref_id, call. = FALSE)
  }

  payload <- litxr_llm_digest_template(ref_id)
  for (name in intersect(names(payload), names(digest))) {
    payload[[name]] <- digest[[name]]
  }
  payload$ref_id <- as.character(ref_id)
  payload$generated_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  litxr_validate_llm_digest(payload)

  path <- .litxr_llm_digest_path(cfg, ref_id)
  .litxr_ensure_project_llm_dir(cfg)
  jsonlite::write_json(payload, path, auto_unbox = TRUE, pretty = TRUE, null = "null")
  .litxr_write_enrichment_status_index(cfg)
  invisible(path)
}

#' Build one LLM digest from markdown
#'
#' Reads the canonical reference metadata and project-level markdown for one
#' `ref_id`, then calls a user-supplied builder function that returns digest
#' fields. The resulting digest is validated and optionally written to disk.
#'
#' @param ref_id Reference identifier.
#' @param builder Function taking arguments `ref`, `markdown`, and `template`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param overwrite Whether to overwrite an existing digest.
#' @param write Whether to write the digest to disk. If `FALSE`, returns the
#'   validated digest without writing.
#'
#' @return Named list digest.
#' @export
litxr_build_llm_digest <- function(ref_id, builder, config = NULL, overwrite = FALSE, write = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  if (!is.function(builder)) {
    stop("`builder` must be a function.", call. = FALSE)
  }

  existing <- litxr_read_llm_digest(ref_id, cfg)
  if (!is.null(existing) && !isTRUE(overwrite)) {
    stop("LLM digest already exists for ref_id: ", ref_id, ". Set `overwrite = TRUE` to replace it.", call. = FALSE)
  }

  refs <- litxr_read_references(cfg)
  ref_match <- refs[refs$ref_id == ref_id, ]
  if (!nrow(ref_match)) {
    stop("Reference id not found in canonical store: ", ref_id, call. = FALSE)
  }

  markdown <- litxr_read_md(ref_id, cfg)
  if (is.null(markdown)) {
    stop("Markdown not found for ref_id: ", ref_id, call. = FALSE)
  }

  template <- litxr_llm_digest_template(ref_id)
  built <- builder(ref = ref_match[1, ], markdown = markdown, template = template)
  if (is.null(built) || !is.list(built)) {
    stop("`builder` must return a named list of digest fields.", call. = FALSE)
  }

  payload <- template
  for (name in intersect(names(payload), names(built))) {
    payload[[name]] <- built[[name]]
  }
  payload$ref_id <- as.character(ref_id)
  payload$generated_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  litxr_validate_llm_digest(payload)

  if (isTRUE(write)) {
    litxr_write_llm_digest(ref_id, payload, cfg)
  }
  payload
}

#' Build LLM digests in batch for references with markdown
#'
#' Selects references from the enrichment status index and builds digests for
#' those with markdown content and no digest yet, unless explicit `ref_ids` are
#' supplied.
#'
#' @param builder Function taking arguments `ref`, `markdown`, and `template`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param ref_ids Optional explicit character vector of reference ids to build.
#' @param overwrite Whether to overwrite existing digests.
#' @param limit Optional maximum number of digests to build in this run.
#'
#' @return Named list of built digests.
#' @export
litxr_build_llm_digests <- function(builder, config = NULL, ref_ids = NULL, overwrite = FALSE, limit = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  targets <- if (!is.null(ref_ids) && length(ref_ids)) {
    unique(as.character(ref_ids))
  } else {
    status <- litxr_read_enrichment_status(cfg)
    status <- status[status$has_md & (overwrite | !status$has_llm_digest), ]
    status$ref_id
  }

  if (!length(targets)) {
    return(list())
  }

  if (!is.null(limit)) {
    targets <- targets[seq_len(min(length(targets), as.integer(limit)))]
  }

  out <- stats::setNames(vector("list", length(targets)), targets)
  for (i in seq_along(targets)) {
    out[[i]] <- litxr_build_llm_digest(
      ref_id = targets[[i]],
      builder = builder,
      config = cfg,
      overwrite = overwrite,
      write = TRUE
    )
  }
  out
}

#' Read one LLM digest by reference id
#'
#' @param ref_id Reference identifier.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Named list digest, or `NULL` if not found.
#' @export
litxr_read_llm_digest <- function(ref_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  path <- .litxr_llm_digest_path(cfg, ref_id)
  if (!file.exists(path)) {
    return(NULL)
  }
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

#' Read all project-level LLM digests
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param ref_ids Optional character vector of reference ids to keep.
#'
#' @return `data.table` of digest summaries with selected structured fields.
#' @export
litxr_read_llm_digests <- function(config = NULL, ref_ids = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  llm_dir <- .litxr_project_llm_dir(cfg)
  if (!dir.exists(llm_dir)) {
    return(data.table::data.table())
  }

  files <- list.files(llm_dir, pattern = "\\.json$", full.names = TRUE)
  if (!length(files)) {
    return(data.table::data.table())
  }

  rows <- lapply(files, function(path) {
    x <- jsonlite::fromJSON(path, simplifyVector = FALSE)
    data.table::data.table(
      ref_id = x$ref_id %||% NA_character_,
      summary = x$summary %||% NA_character_,
      motivation = x$motivation %||% NA_character_,
      research_questions = list(unlist(x$research_questions %||% character(), use.names = FALSE)),
      methods = list(unlist(x$methods %||% character(), use.names = FALSE)),
      key_findings = list(unlist(x$key_findings %||% character(), use.names = FALSE)),
      limitations = list(unlist(x$limitations %||% character(), use.names = FALSE)),
      keywords = list(unlist(x$keywords %||% character(), use.names = FALSE)),
      notes = x$notes %||% NA_character_,
      generated_at = x$generated_at %||% NA_character_
    )
  })

  out <- data.table::rbindlist(rows, fill = TRUE)
  if (!is.null(ref_ids) && length(ref_ids)) {
    out <- out[out$ref_id %in% ref_ids, ]
  }
  out
}

#' Find LLM digests by text and optional collection membership
#'
#' @param query Optional substring query matched across digest text fields.
#' @param collection_id Optional collection membership filter via `ref_id`.
#' @param ref_id Optional direct reference id filter.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Filtered `data.table` of digests.
#' @export
litxr_find_llm <- function(query = NULL, collection_id = NULL, ref_id = NULL, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  digests <- litxr_read_llm_digests(cfg, ref_ids = ref_id)
  if (!nrow(digests)) {
    return(digests)
  }

  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    links <- litxr_read_reference_collections(cfg)
    keep_ref_ids <- unique(links$ref_id[links$collection_id == collection_id])
    digests <- digests[digests$ref_id %in% keep_ref_ids, ]
  }

  if (!is.null(query) && nzchar(as.character(query))) {
    q <- tolower(as.character(query[[1]]))
    flat_text <- vapply(seq_len(nrow(digests)), function(i) {
      row <- digests[i, ]
      flatten_cell <- function(x) {
        if (is.null(x) || length(x) == 0L) return(character())
        if (is.list(x) && length(x) == 1L) x <- x[[1]]
        if (is.null(x) || length(x) == 0L) return(character())
        as.character(unlist(x, use.names = FALSE))
      }
      paste(
        c(
          flatten_cell(row$summary[[1]]),
          flatten_cell(row$motivation[[1]]),
          flatten_cell(row$notes[[1]]),
          flatten_cell(row$research_questions[[1]]),
          flatten_cell(row$methods[[1]]),
          flatten_cell(row$key_findings[[1]]),
          flatten_cell(row$limitations[[1]]),
          flatten_cell(row$keywords[[1]])
        ),
        collapse = " "
      )
    }, character(1))
    keep <- !is.na(flat_text) & grepl(q, tolower(flat_text), fixed = TRUE)
    digests <- digests[keep, ]
  }

  digests
}

#' Write markdown content for one reference
#'
#' Stores markdown under the project-level `md/` directory using `ref_id` as the
#' canonical key.
#'
#' @param ref_id Reference identifier.
#' @param text Markdown text.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the written markdown path.
#' @export
litxr_write_md <- function(ref_id, text, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_ensure_project_md_dir(cfg)
  path <- .litxr_md_path(cfg, ref_id)
  writeLines(as.character(text), path, useBytes = TRUE)
  .litxr_write_enrichment_status_index(cfg)
  invisible(path)
}

#' Read markdown content for one reference
#'
#' @param ref_id Reference identifier.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Character scalar markdown text, or `NULL` if not found.
#' @export
litxr_read_md <- function(ref_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  path <- .litxr_md_path(cfg, ref_id)
  if (!file.exists(path)) {
    return(NULL)
  }
  paste(readLines(path, warn = FALSE), collapse = "\n")
}

#' Validate one LLM digest against the litxr schema
#'
#' @param digest Named list digest.
#'
#' @return Invisibly returns `TRUE` on success, otherwise errors.
#' @export
litxr_validate_llm_digest <- function(digest) {
  required <- c(
    "ref_id", "summary", "motivation", "research_questions", "methods",
    "sample", "key_findings", "limitations", "keywords", "notes", "generated_at"
  )
  missing <- setdiff(required, names(digest))
  if (length(missing)) {
    stop("LLM digest is missing required fields: ", paste(missing, collapse = ", "), call. = FALSE)
  }

  if (!is.list(digest$sample)) {
    stop("LLM digest field `sample` must be a named list.", call. = FALSE)
  }
  for (field in c("research_questions", "methods", "key_findings", "limitations", "keywords")) {
    value <- digest[[field]]
    if (is.list(value)) {
      value <- unlist(value, use.names = FALSE)
    }
    if (!(is.character(value) || is.null(value))) {
      stop("LLM digest field `", field, "` must be a character vector.", call. = FALSE)
    }
  }
  invisible(TRUE)
}

#' Read project-level enrichment status
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` with one row per reference and enrichment flags.
#' @export
litxr_read_enrichment_status <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_read_enrichment_status_index(cfg)
}

#' Read project-level sync state
#'
#' Returns the project sync ledger used to track completed sync and repair runs.
#' This state lives under `project.data_root/index/` and is separate from
#' `config.yaml`.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param collection_id Optional collection filter.
#'
#' @return `data.table` of sync ledger rows.
#' @export
litxr_read_sync_state <- function(config = NULL, collection_id = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  out <- .litxr_read_sync_state_index(cfg)
  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    out <- out[out$collection_id == as.character(collection_id[[1]]), ]
  }
  out
}

#' Rebuild a first-pass sync ledger from existing local data
#'
#' Infers one sync ledger row per collection from existing local storage and
#' index file timestamps. This is intended for backfilling `sync_state.fst` when
#' local data already exists from older package versions that did not record
#' sync history.
#'
#' The inferred rows are approximate. They record that local data exists and
#' when the local collection index was last modified, but they do not recover
#' exact remote cursors, arXiv day windows, or original sync arguments.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param overwrite Whether to replace the existing sync ledger instead of only
#'   adding inferred rows for collections not already present.
#'
#' @return `data.table` of the rebuilt sync ledger.
#' @export
litxr_rebuild_sync_state <- function(config = NULL, overwrite = FALSE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  collections <- .litxr_config_collections(cfg)
  existing <- .litxr_read_sync_state_index(cfg)
  inferred <- lapply(collections, function(collection) {
    .litxr_infer_collection_sync_state(cfg, collection)
  })
  inferred <- data.table::rbindlist(inferred, fill = TRUE)

  if (!nrow(inferred)) {
    if (isTRUE(overwrite)) {
      .litxr_write_sync_state_index(cfg, .litxr_empty_sync_state())
      return(.litxr_empty_sync_state())
    }
    return(existing)
  }

  if (isTRUE(overwrite) || !nrow(existing)) {
    out <- inferred
  } else {
    covered <- unique(existing$collection_id)
    out <- data.table::rbindlist(
      list(existing, inferred[!(inferred$collection_id %in% covered), ]),
      fill = TRUE
    )
  }

  .litxr_write_sync_state_index(cfg, out)
  out
}

#' List enrichment candidates and exclusion reasons
#'
#' Combines the canonical reference store, collection memberships, and
#' enrichment status into a simple table that shows which references are ready
#' for digest building and why others are excluded.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param collection_id Optional collection membership filter.
#' @param ref_ids Optional character vector of reference ids to keep.
#' @param include_ready Whether to keep rows that are currently ready for digest
#'   building.
#'
#' @return `data.table` with eligibility flags and reasons.
#' @export
litxr_list_enrichment_candidates <- function(config = NULL, collection_id = NULL, ref_ids = NULL, include_ready = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  refs <- litxr_read_references(cfg)
  status <- litxr_read_enrichment_status(cfg)
  links <- litxr_read_reference_collections(cfg)

  if (!nrow(refs)) {
    return(data.table::data.table(
      ref_id = character(),
      title = character(),
      entry_type = character(),
      collection_ids = character(),
      has_md = logical(),
      has_llm_digest = logical(),
      eligible = logical(),
      reason = character()
    ))
  }

  collection_map <- if (nrow(links)) {
    split_ids <- split(as.character(links$collection_id), as.character(links$ref_id))
    data.table::data.table(
      ref_id = names(split_ids),
      collection_ids = vapply(
        split_ids,
        function(x) paste(sort(unique(x)), collapse = ","),
        character(1)
      )
    )
  } else {
    data.table::data.table(ref_id = character(), collection_ids = character())
  }

  ref_view <- data.table::data.table(
    ref_id = as.character(refs$ref_id),
    title = as.character(refs$title),
    entry_type = as.character(refs$entry_type)
  )
  status_view <- data.table::data.table(
    ref_id = as.character(status$ref_id),
    has_md = as.logical(status$has_md),
    has_llm_digest = as.logical(status$has_llm_digest)
  )

  out <- merge(ref_view, status_view, by = "ref_id", all.x = TRUE, sort = FALSE)
  out <- merge(out, collection_map, by = "ref_id", all.x = TRUE, sort = FALSE)
  out$has_md[is.na(out$has_md)] <- FALSE
  out$has_llm_digest[is.na(out$has_llm_digest)] <- FALSE
  out$collection_ids[is.na(out$collection_ids)] <- ""

  if (!is.null(ref_ids) && length(ref_ids)) {
    keep_ids <- unique(as.character(ref_ids))
    out <- out[out$ref_id %in% keep_ids, ]
  }

  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    needle <- as.character(collection_id[[1]])
    out <- out[
      vapply(
        strsplit(out$collection_ids, ",", fixed = TRUE),
        function(x) needle %in% x,
        logical(1)
      ),
    ]
  }

  out$eligible <- out$has_md & !out$has_llm_digest
  out$reason <- ifelse(
    !out$has_md,
    "missing_md",
    ifelse(out$has_llm_digest, "digest_exists", "ready")
  )

  if (!isTRUE(include_ready)) {
    out <- out[!out$eligible, ]
  }

  data.table::setcolorder(
    out,
    c("ref_id", "title", "entry_type", "collection_ids", "has_md", "has_llm_digest", "eligible", "reason")
  )
  out[]
}

#' Add references by DOI and auto-register missing collections
#'
#' Fetches Crossref metadata for a DOI vector, writes the records into the local
#' collection stores, and auto-registers Crossref collections in `config.yaml`
#' when needed.
#'
#' @param dois Character vector of DOIs.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param auto_register Whether to auto-register missing collections into
#'   `config.yaml`.
#'
#' @return `data.table` of fetched records.
#' @export
litxr_add_dois <- function(dois, config = NULL, auto_register = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  doi_values <- unique(trimws(as.character(dois)))
  doi_values <- doi_values[nzchar(doi_values)]
  if (!length(doi_values)) {
    return(data.table::data.table())
  }

  messages <- fetch_crossref_messages(doi_values)
  found <- !vapply(messages, is.null, logical(1))
  missing_dois <- names(messages)[!found]
  if (length(missing_dois)) {
    warning(
      "The following DOIs were not found and were ignored: ",
      paste(missing_dois, collapse = ", "),
      call. = FALSE
    )
  }

  messages <- messages[found]
  if (!length(messages)) {
    return(data.table::data.table())
  }

  parsed <- lapply(messages, parse_crossref_entry_unified)
  records <- data.table::rbindlist(parsed, fill = TRUE)
  if (!nrow(records)) {
    return(records)
  }

  assignment <- .litxr_assign_crossref_journals(cfg, records, messages, auto_register = auto_register)
  cfg <- assignment$cfg
  records <- assignment$records

  by_journal <- split(records, records$collection_id)
  out <- lapply(names(by_journal), function(journal_id) {
    journal <- .litxr_get_journal(cfg, journal_id)
    local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
    incoming <- data.table::as.data.table(by_journal[[journal_id]])
    existing <- .litxr_read_journal_records(local_path)
    .litxr_write_journal_upserted_records(existing, incoming, local_path, journal, cfg = cfg)
    incoming
  })

  data.table::rbindlist(out, fill = TRUE)
}

#' Add manually supplied references to a collection
#'
#' Accepts a normalized reference table and writes the rows into the target
#' local collection store. This is the main manual-ingest path for books,
#' reports, news, conference papers, web references, and other non-DOI sources.
#'
#' @param refs A `data.frame` or `data.table` of normalized reference fields.
#' @param collection_id Target collection identifier.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param auto_register Whether to create the target collection automatically
#'   when it does not already exist.
#' @param collection_title Optional collection title used when auto-registering.
#'
#' @return `data.table` of ingested records.
#' @export
litxr_add_refs <- function(
  refs,
  collection_id,
  config = NULL,
  auto_register = TRUE,
  collection_title = NULL
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  records <- .litxr_normalize_manual_refs(refs)
  if (!nrow(records)) {
    return(records)
  }

  collection <- tryCatch(
    .litxr_get_journal(cfg, collection_id),
    error = function(e) NULL
  )

  if (is.null(collection)) {
    if (!isTRUE(auto_register)) {
      stop("Collection not found in config: ", collection_id, call. = FALSE)
    }
    registered <- .litxr_register_manual_collection(cfg, collection_id, collection_title = collection_title)
    cfg <- registered$cfg
    collection <- registered$collection
  }

  records[["collection_id"]] <- collection$collection_id
  records[["collection_title"]] <- collection$title

  local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
  existing <- .litxr_read_journal_records(local_path)
  .litxr_write_journal_upserted_records(existing, records, local_path, collection, cfg = cfg)
  records
}

.litxr_filter_records_by_keys <- function(records, keys) {
  if (!nrow(records) || !length(keys)) {
    return(records[0, ])
  }

  out <- data.table::copy(records)
  doi <- if ("doi" %in% names(out)) out[["doi"]] else rep(NA_character_, nrow(out))
  ref_id <- if ("ref_id" %in% names(out)) out[["ref_id"]] else rep(NA_character_, nrow(out))
  source_id <- if ("source_id" %in% names(out)) out[["source_id"]] else rep(NA_character_, nrow(out))

  matched_key <- rep(NA_character_, nrow(out))
  doi_match <- !is.na(doi) & doi %in% keys
  matched_key[doi_match] <- doi[doi_match]

  ref_id_match <- !is.na(ref_id) & ref_id %in% keys & is.na(matched_key)
  matched_key[ref_id_match] <- ref_id[ref_id_match]

  source_id_match <- !is.na(source_id) & source_id %in% keys & is.na(matched_key)
  matched_key[source_id_match] <- source_id[source_id_match]

  keep <- !is.na(matched_key)
  out[["litxr_matched_key__"]] <- matched_key

  out[keep, ]
}

.litxr_normalize_manual_refs <- function(refs) {
  if (is.null(refs)) {
    return(data.table::data.table())
  }

  out <- data.table::as.data.table(refs)
  if (!nrow(out)) {
    return(out)
  }

  if (!("authors_list" %in% names(out))) {
    if ("authors" %in% names(out)) {
      out[["authors_list"]] <- lapply(out[["authors"]], function(x) {
        if (is.null(x) || is.na(x) || !nzchar(x)) return(character())
        trimws(strsplit(as.character(x), ";", fixed = TRUE)[[1]])
      })
    } else {
      out[["authors_list"]] <- rep(list(character()), nrow(out))
    }
  }

  if (!("authors" %in% names(out))) {
    out[["authors"]] <- vapply(out[["authors_list"]], function(x) {
      paste(x, collapse = "; ")
    }, character(1))
  }

  if (!("entry_type" %in% names(out))) {
    out[["entry_type"]] <- if ("source" %in% names(out)) {
      vapply(out[["source"]], .litxr_entry_type_from_source, character(1))
    } else {
      rep("misc", nrow(out))
    }
  }

  if (!("source" %in% names(out))) out[["source"]] <- rep("manual", nrow(out))
  if (!("source_id" %in% names(out))) out[["source_id"]] <- rep(NA_character_, nrow(out))
  if (!("doi" %in% names(out))) out[["doi"]] <- rep(NA_character_, nrow(out))
  if (!("isbn" %in% names(out))) out[["isbn"]] <- rep(NA_character_, nrow(out))
  if (!("issn" %in% names(out))) out[["issn"]] <- rep(NA_character_, nrow(out))
  if (!("url" %in% names(out))) out[["url"]] <- rep(NA_character_, nrow(out))
  if (!("url_landing" %in% names(out))) out[["url_landing"]] <- out[["url"]]
  if (!("url_pdf" %in% names(out))) out[["url_pdf"]] <- rep(NA_character_, nrow(out))
  if (!("note" %in% names(out))) out[["note"]] <- rep(NA_character_, nrow(out))
  if (!("abstract" %in% names(out))) out[["abstract"]] <- rep(NA_character_, nrow(out))
  if (!("subject_primary" %in% names(out))) out[["subject_primary"]] <- rep(NA_character_, nrow(out))
  if (!("subject_all" %in% names(out))) out[["subject_all"]] <- rep(NA_character_, nrow(out))
  if (!("journal" %in% names(out))) out[["journal"]] <- rep(NA_character_, nrow(out))
  if (!("container_title" %in% names(out))) out[["container_title"]] <- rep(NA_character_, nrow(out))
  if (!("publisher" %in% names(out))) out[["publisher"]] <- rep(NA_character_, nrow(out))
  if (!("volume" %in% names(out))) out[["volume"]] <- rep(NA_character_, nrow(out))
  if (!("issue" %in% names(out))) out[["issue"]] <- rep(NA_character_, nrow(out))
  if (!("pages" %in% names(out))) out[["pages"]] <- rep(NA_character_, nrow(out))
  if (!("arxiv_version" %in% names(out))) out[["arxiv_version"]] <- rep(NA_integer_, nrow(out))
  if (!("arxiv_primary_category" %in% names(out))) out[["arxiv_primary_category"]] <- rep(NA_character_, nrow(out))
  if (!("arxiv_categories_raw" %in% names(out))) out[["arxiv_categories_raw"]] <- rep("", nrow(out))
  if (!("arxiv_comment" %in% names(out))) out[["arxiv_comment"]] <- rep(NA_character_, nrow(out))
  if (!("arxiv_journal_ref" %in% names(out))) out[["arxiv_journal_ref"]] <- rep(NA_character_, nrow(out))
  if (!("raw_entry" %in% names(out))) out[["raw_entry"]] <- rep(list(NULL), nrow(out))

  if (!("pub_date" %in% names(out))) {
    out[["pub_date"]] <- as.POSIXct(rep(NA_character_, nrow(out)), tz = "UTC")
  }
  if (!inherits(out[["pub_date"]], "POSIXct")) {
    out[["pub_date"]] <- as.POSIXct(out[["pub_date"]], tz = "UTC")
  }

  if (!("year" %in% names(out))) out[["year"]] <- rep(NA_integer_, nrow(out))
  if (!("month" %in% names(out))) out[["month"]] <- rep(NA_integer_, nrow(out))
  if (!("day" %in% names(out))) out[["day"]] <- rep(NA_integer_, nrow(out))

  need_date_parts <- !is.na(out[["pub_date"]])
  out[["year"]][need_date_parts & is.na(out[["year"]])] <- as.integer(format(out[["pub_date"]][need_date_parts & is.na(out[["year"]])], "%Y"))
  out[["month"]][need_date_parts & is.na(out[["month"]])] <- as.integer(format(out[["pub_date"]][need_date_parts & is.na(out[["month"]])], "%m"))
  out[["day"]][need_date_parts & is.na(out[["day"]])] <- as.integer(format(out[["pub_date"]][need_date_parts & is.na(out[["day"]])], "%d"))

  if (!("ref_id" %in% names(out))) out[["ref_id"]] <- rep(NA_character_, nrow(out))

  for (i in seq_len(nrow(out))) {
    if (is.na(out$source_id[[i]]) || !nzchar(out$source_id[[i]])) {
      out$source_id[[i]] <- .litxr_manual_source_id(out[i, ])
    }
    if (is.na(out$ref_id[[i]]) || !nzchar(out$ref_id[[i]])) {
      out$ref_id[[i]] <- .litxr_manual_ref_id(out[i, ])
    }
  }

  out
}

.litxr_manual_source_id <- function(row) {
  doi <- row[["doi"]]
  isbn <- row[["isbn"]]
  url <- row[["url"]]
  title <- row[["title"]]
  year <- row[["year"]]

  if (!is.na(doi) && nzchar(doi)) return(doi)
  if (!is.na(isbn) && nzchar(isbn)) return(isbn)
  if (!is.na(url) && nzchar(url)) return(url)

  slug <- gsub("[^A-Za-z0-9]+", "", substr(ifelse(is.na(title), "ref", title), 1, 24))
  paste0("manual_", ifelse(is.na(year), "na", year), "_", slug)
}

.litxr_manual_ref_id <- function(row) {
  doi <- row[["doi"]]
  isbn <- row[["isbn"]]
  url <- row[["url"]]
  source <- row[["source"]]
  source_id <- row[["source_id"]]

  if (!is.na(doi) && nzchar(doi)) return(paste0("doi:", doi))
  if (!is.na(isbn) && nzchar(isbn)) return(paste0("isbn:", isbn))
  if (!is.na(url) && nzchar(url)) return(paste0("url:", url))
  paste0(source, ":", source_id)
}

.litxr_register_manual_collection <- function(cfg, collection_id, collection_title = NULL) {
  title <- if (is.null(collection_title) || !nzchar(collection_title)) collection_id else collection_title
  collection <- list(
    collection_id = collection_id,
    collection_type = "manual_batch",
    title = title,
    remote_channel = "manual",
    local_path = file.path(cfg$project$data_root, collection_id),
    metadata = list(),
    sync = list()
  )

  collections <- .litxr_config_collections(cfg)
  collections[[length(collections) + 1L]] <- collection
  cfg$collections <- collections
  cfg <- .litxr_normalize_config_schema(cfg)
  .litxr_write_config(cfg)
  list(cfg = cfg, collection = collection)
}

.litxr_assign_crossref_journals <- function(cfg, records, messages, auto_register = TRUE) {
  out_cfg <- cfg
  out_records <- data.table::copy(records)

  for (i in seq_len(nrow(out_records))) {
    message <- messages[[i]]
    journal <- .litxr_match_crossref_journal(out_cfg, message)

    if (is.null(journal)) {
      if (!isTRUE(auto_register)) {
        stop(
          "Crossref journal is not registered in config.yaml for DOI ",
          out_records$doi[[i]],
          ". Set `auto_register = TRUE` to add it automatically.",
          call. = FALSE
        )
      }
      registered <- .litxr_register_crossref_journal(out_cfg, message)
      out_cfg <- registered$cfg
      journal <- registered$journal
    }

    out_records$collection_id[[i]] <- journal$collection_id
    out_records$collection_title[[i]] <- journal$title
  }

  list(cfg = out_cfg, records = out_records)
}

.litxr_match_crossref_journal <- function(cfg, cr_message) {
  journal_title <- .litxr_crossref_journal_title(cr_message)
  issns <- .litxr_crossref_issns(cr_message)

  for (journal in .litxr_config_collections(cfg)) {
    if (!identical(journal$remote_channel, "crossref")) next
    title_match <- !is.na(journal_title) && identical(journal$title, journal_title)
    issn_match <- length(intersect(.litxr_journal_issns(journal), issns)) > 0
    if (title_match || issn_match) {
      return(journal)
    }
  }

  NULL
}

.litxr_register_crossref_journal <- function(cfg, cr_message) {
  journal_title <- .litxr_crossref_journal_title(cr_message)
  if (is.na(journal_title) || !nzchar(journal_title)) {
    journal_title <- "Crossref Unclassified"
  }

  base_id <- .litxr_make_journal_id(journal_title)
  journal_id <- .litxr_unique_journal_id(cfg, base_id)
  metadata <- .litxr_crossref_journal_metadata(cr_message)
  local_path <- file.path(cfg$project$data_root, journal_id)
  sync_issn <- metadata$issn_print
  if (is.null(sync_issn) || is.na(sync_issn) || !nzchar(sync_issn)) {
    sync_issn <- metadata$issn_electronic
  }

  journal <- list(
    collection_id = journal_id,
    collection_type = "journal",
    title = journal_title,
    remote_channel = "crossref",
    local_path = local_path,
    metadata = metadata,
    sync = list(
      filters = list(
        issn = if (!is.null(sync_issn) && nzchar(sync_issn)) sync_issn else NA_character_
      )
    )
  )

  collections <- .litxr_config_collections(cfg)
  collections[[length(collections) + 1L]] <- journal
  cfg$collections <- collections
  cfg <- .litxr_normalize_config_schema(cfg)
  .litxr_write_config(cfg)
  list(cfg = cfg, journal = journal)
}

.litxr_crossref_journal_title <- function(cr_message) {
  title <- cr_message$`container-title`
  if (is.null(title) || length(title) == 0) return(NA_character_)
  title <- as.character(title[[1]])
  if (!nzchar(title)) NA_character_ else title
}

.litxr_crossref_issns <- function(cr_message) {
  issn <- cr_message$ISSN
  if (is.null(issn) || !length(issn)) return(character())
  unique(stats::na.omit(as.character(unlist(issn, use.names = FALSE))))
}

.litxr_crossref_journal_metadata <- function(cr_message) {
  issn_print <- NA_character_
  issn_electronic <- NA_character_

  if (!is.null(cr_message$`issn-type`) && length(cr_message$`issn-type`)) {
    for (entry in cr_message$`issn-type`) {
      if (is.null(entry$type) || is.null(entry$value)) next
      if (identical(entry$type, "print")) issn_print <- as.character(entry$value)
      if (identical(entry$type, "electronic")) issn_electronic <- as.character(entry$value)
    }
  }

  issn_all <- .litxr_crossref_issns(cr_message)
  if ((is.na(issn_print) || !nzchar(issn_print)) && length(issn_all)) {
    issn_print <- issn_all[[1]]
  }
  if ((is.na(issn_electronic) || !nzchar(issn_electronic)) && length(issn_all) >= 2L) {
    issn_electronic <- issn_all[[2]]
  }

  list(
    publisher = if (is.null(cr_message$publisher)) NA_character_ else as.character(cr_message$publisher[[1]]),
    issn_print = issn_print,
    issn_electronic = issn_electronic
  )
}

.litxr_make_journal_id <- function(x) {
  id <- tolower(as.character(x[[1]]))
  id <- gsub("[^a-z0-9]+", "_", id)
  id <- gsub("^_+|_+$", "", id)
  if (!nzchar(id)) id <- "crossref_journal"
  id
}

.litxr_unique_journal_id <- function(cfg, base_id) {
  existing_ids <- vapply(.litxr_config_collections(cfg), `[[`, character(1), "collection_id")
  if (!(base_id %in% existing_ids)) {
    return(base_id)
  }

  i <- 2L
  repeat {
    candidate <- paste0(base_id, "_", i)
    if (!(candidate %in% existing_ids)) {
      return(candidate)
    }
    i <- i + 1L
  }
}

.litxr_get_journal <- function(cfg, journal_id) {
  collections <- .litxr_config_collections(cfg)
  matches <- Filter(function(x) identical(x$collection_id, journal_id) || identical(x$journal_id, journal_id), collections)
  if (!length(matches)) {
    stop("Collection not found in config: ", journal_id, call. = FALSE)
  }
  matches[[1]]
}

.litxr_sync_crossref_journal <- function(journal) {
  issns <- .litxr_journal_issns(journal)
  if (!length(issns)) {
    stop("crossref journal entries require at least one ISSN in config.yaml.", call. = FALSE)
  }

  limit <- if (is.null(journal$sync$limit)) Inf else as.integer(journal$sync$limit)
  rows <- if (is.null(journal$sync$rows)) 1000L else as.integer(journal$sync$rows)

  items <- unlist(lapply(issns, function(issn) {
    fetch_crossref_journal_works(issn = issn, limit = limit, rows = rows)
  }), recursive = FALSE)

  if (!length(items)) {
    return(data.table::data.table())
  }

  rows <- lapply(items, function(item) {
    row <- parse_crossref_entry_unified(item)
    row[["collection_id"]] <- if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id
    row[["collection_title"]] <- journal$title
    row
  })

  records <- .litxr_deduplicate_records(data.table::rbindlist(rows, fill = TRUE))
  if (is.finite(limit) && nrow(records) > limit) {
    records <- records[seq_len(limit), ]
  }

  records
}

.litxr_sync_arxiv_journal <- function(journal) {
  search_query <- journal$sync$search_query
  if (is.null(search_query) || !nzchar(search_query)) {
    stop("arxiv journal entries require `sync.search_query` in config.yaml.", call. = FALSE)
  }

  limit <- if (is.null(journal$sync$limit)) Inf else as.integer(journal$sync$limit)
  batch_size <- if (is.null(journal$sync$rows)) 100L else as.integer(journal$sync$rows)
  delay_seconds <- if (is.null(journal$sync$delay_seconds)) 3 else as.numeric(journal$sync$delay_seconds)
  start <- if (is.null(journal$sync$start)) 0L else as.integer(journal$sync$start)
  entries <- list()

  repeat {
    request_n <- if (is.finite(limit)) min(batch_size, limit - length(entries)) else batch_size
    if (request_n <= 0L) break

    .litxr_arxiv_delay(delay_seconds)

    feed <- fetch_arxiv_xml(
      search_query = search_query,
      start = start,
      max_results = request_n
    )

    page_entries <- xml2::xml_find_all(feed, ".//*[local-name()='entry']")
    if (!length(page_entries)) break

    entries <- c(entries, as.list(page_entries))
    if (length(page_entries) < request_n) break

    start <- start + length(page_entries)
  }

  if (!length(entries)) {
    return(data.table::data.table())
  }

  rows <- lapply(entries, function(entry) {
    row <- parse_arxiv_entry_unified(entry)
    row[["collection_id"]] <- if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id
    row[["collection_title"]] <- journal$title
    row
  })

  records <- data.table::rbindlist(rows, fill = TRUE)
  if (is.finite(limit) && nrow(records) > limit) {
    records <- records[seq_len(limit), ]
  }
  records
}

.litxr_journal_paths <- function(local_path) {
  list(
    root = local_path,
    index = file.path(local_path, "index"),
    json = file.path(local_path, "json"),
    pdf = file.path(local_path, "pdf"),
    md = file.path(local_path, "md"),
    llm = file.path(local_path, "llm")
  )
}

.litxr_is_absolute_path <- function(path) {
  grepl("^(/|~(?:/|$)|[A-Za-z]:[/\\\\])", path)
}

.litxr_resolve_from_config_root <- function(cfg, path) {
  if (.litxr_is_absolute_path(path)) {
    return(path.expand(path))
  }

  root <- attr(cfg, "config_root", exact = TRUE)
  if (is.null(root) || !nzchar(root)) {
    root <- "."
  }

  file.path(root, path)
}

.litxr_resolve_local_path <- function(cfg, local_path) {
  if (.litxr_is_absolute_path(local_path)) {
    return(path.expand(local_path))
  }

  file.path(.litxr_project_root(cfg), local_path)
}

.litxr_build_arxiv_search_query <- function(base_query, submitted_from = NULL, submitted_to = NULL) {
  if (is.null(submitted_from) && is.null(submitted_to)) {
    return(base_query)
  }

  from_text <- if (is.null(submitted_from)) "*" else .litxr_format_arxiv_datetime(submitted_from, end = FALSE)
  to_text <- if (is.null(submitted_to)) "*" else .litxr_format_arxiv_datetime(submitted_to, end = TRUE)
  paste0("(", base_query, ") AND submittedDate:[", from_text, " TO ", to_text, "]")
}

.litxr_format_arxiv_datetime <- function(x, end = FALSE) {
  text <- as.character(x)
  if (grepl("^[0-9]{4}$", text)) {
    return(if (end) paste0(text, "12312359") else paste0(text, "01010000"))
  }
  if (grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", text)) {
    compact <- gsub("-", "", text)
    return(if (end) paste0(compact, "2359") else paste0(compact, "0000"))
  }
  if (grepl("^[0-9]{8}$", text)) {
    return(if (end) paste0(text, "2359") else paste0(text, "0000"))
  }
  if (grepl("^[0-9]{12}$", text)) {
    return(text)
  }

  parsed <- as.POSIXct(text, tz = "UTC")
  if (is.na(parsed)) {
    stop("Unable to parse arXiv submittedDate value: ", text, call. = FALSE)
  }
  format(parsed, "%Y%m%d%H%M")
}

.litxr_arxiv_delay <- local({
  last_request_time <- NULL

  function(delay_seconds = 3) {
    if (is.null(delay_seconds) || is.na(delay_seconds) || delay_seconds <= 0) {
      return(invisible(NULL))
    }

    if (!is.null(last_request_time)) {
      elapsed <- as.numeric(difftime(Sys.time(), last_request_time, units = "secs"))
      remaining <- delay_seconds - elapsed
      if (remaining > 0) {
        Sys.sleep(remaining)
      }
    }

    last_request_time <<- Sys.time()
    invisible(NULL)
  }
})

.litxr_ensure_journal_dirs <- function(local_path) {
  paths <- .litxr_journal_paths(local_path)
  for (path in unname(paths)) {
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

  vapply(seq_len(nrow(records)), function(i) {
    row <- records[i, ]
    sum(vapply(score_fields, function(field) {
      value <- row[[field]]
      !.litxr_nullish(value)
    }, logical(1)))
  }, integer(1))
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
    conflict_path <- file.path(local_path, "json", "_upsert_conflicts.jsonl")
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

  existing_keys <- .litxr_upsert_key(existing)
  incoming_keys <- .litxr_upsert_key(incoming)
  all_keys <- union(existing_keys, incoming_keys)
  conflicts <- new.env(parent = emptyenv())
  conflicts$rows <- list()

  merged <- lapply(all_keys, function(key) {
    existing_idx <- match(key, existing_keys)
    incoming_idx <- match(key, incoming_keys)

    if (is.na(existing_idx)) return(incoming[incoming_idx, ])
    if (is.na(incoming_idx)) return(existing[existing_idx, ])

    .litxr_merge_record_row(existing[existing_idx, ], incoming[incoming_idx, ], key, conflicts)
  })

  .litxr_write_upsert_conflicts(conflicts, conflict_path = conflict_path)
  data.table::rbindlist(merged, fill = TRUE)
}

.litxr_upsert_journal_records <- function(existing, incoming, local_path) {
  .litxr_upsert_records(
    existing,
    incoming,
    conflict_path = file.path(local_path, "json", "_upsert_conflicts.jsonl")
  )
}

.litxr_write_journal_upserted_records <- function(existing, incoming, local_path, journal, cfg = NULL) {
  records <- .litxr_upsert_journal_records(existing, incoming, local_path = local_path)
  incoming_keys <- .litxr_upsert_key(incoming)
  records_keys <- .litxr_upsert_key(records)
  touched_records <- records[records_keys %in% incoming_keys, ]

  .litxr_write_journal_record_files(touched_records, local_path, journal)
  .litxr_write_journal_index(records, local_path)
  if (!is.null(cfg)) .litxr_update_project_indexes(cfg, journal, records)

  records
}

.litxr_write_journal_records <- function(records, local_path, journal, cfg = NULL) {
  if (!nrow(records)) {
    .litxr_ensure_journal_dirs(local_path)
    .litxr_write_journal_index(records, local_path)
    if (!is.null(cfg)) .litxr_update_project_indexes(cfg, journal, records)
    return(invisible(character()))
  }

  written <- .litxr_write_journal_record_files(records, local_path, journal)

  .litxr_write_journal_index(records, local_path)
  if (!is.null(cfg)) .litxr_update_project_indexes(cfg, journal, records)
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
  indexed <- .litxr_read_journal_index(local_path)
  if (!is.null(indexed)) {
    return(indexed)
  }

  records <- .litxr_read_journal_records_from_json(local_path)
  .litxr_write_journal_index(records, local_path)
  records
}

.litxr_read_journal_records_from_json <- function(local_path) {
  paths <- .litxr_journal_paths(local_path)
  if (!dir.exists(paths$json)) {
    return(data.table::data.table())
  }

  files <- sort(list.files(paths$json, pattern = "\\.json$", full.names = TRUE))
  if (!length(files)) {
    return(data.table::data.table())
  }

  .litxr_prefer_complete_records(
    data.table::rbindlist(lapply(files, .litxr_storage_payload_to_row), fill = TRUE)
  )
}

.litxr_index_path <- function(local_path) {
  file.path(.litxr_journal_paths(local_path)$index, "references.fst")
}

.litxr_delta_index_path <- function(local_path) {
  file.path(.litxr_journal_paths(local_path)$index, "references_delta.fst")
}

.litxr_index_encode <- function(records) {
  out <- data.table::copy(records)

  if ("authors_list" %in% names(out)) {
    out[["authors_list_json"]] <- vapply(out[["authors_list"]], function(x) {
      jsonlite::toJSON(unname(x), auto_unbox = TRUE, null = "null")
    }, character(1))
    out[["authors_list"]] <- NULL
  }

  if ("raw_entry" %in% names(out)) {
    out[["raw_entry_json"]] <- rep(NA_character_, nrow(out))
    out[["raw_entry"]] <- NULL
  }

  if ("pub_date" %in% names(out)) {
    out[["pub_date"]] <- ifelse(
      is.na(out[["pub_date"]]),
      NA_character_,
      format(out[["pub_date"]], tz = "UTC", usetz = TRUE)
    )
  }

  out
}

.litxr_index_decode <- function(records) {
  if (!nrow(records)) {
    return(data.table::as.data.table(records))
  }

  out <- data.table::as.data.table(records)

  if ("authors_list_json" %in% names(out)) {
    out[["authors_list"]] <- lapply(out[["authors_list_json"]], function(x) {
      if (is.na(x) || !nzchar(x)) return(character())
      unlist(jsonlite::fromJSON(x, simplifyVector = TRUE), use.names = FALSE)
    })
    out[["authors_list_json"]] <- NULL
  } else if (!("authors_list" %in% names(out))) {
    out[["authors_list"]] <- rep(list(character()), nrow(out))
  }

  if ("raw_entry_json" %in% names(out)) {
    out[["raw_entry"]] <- rep(list(NULL), nrow(out))
    out[["raw_entry_json"]] <- NULL
  } else if (!("raw_entry" %in% names(out))) {
    out[["raw_entry"]] <- rep(list(NULL), nrow(out))
  }

  if ("pub_date" %in% names(out)) {
    out[["pub_date"]] <- as.POSIXct(out[["pub_date"]], tz = "UTC")
  }

  out
}

.litxr_write_journal_index <- function(records, local_path) {
  paths <- .litxr_ensure_journal_dirs(local_path)
  index_path <- .litxr_index_path(local_path)
  encoded <- .litxr_index_encode(records)
  fst::write_fst(as.data.frame(encoded), index_path)
  invisible(index_path)
}

.litxr_read_journal_index <- function(local_path) {
  index_path <- .litxr_index_path(local_path)
  if (!file.exists(index_path)) {
    return(NULL)
  }

  .litxr_index_decode(fst::read_fst(index_path, as.data.table = TRUE))
}

.litxr_read_collection_delta <- function(local_path) {
  path <- .litxr_delta_index_path(local_path)
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  .litxr_index_decode(fst::read_fst(path, as.data.table = TRUE))
}

.litxr_append_collection_delta <- function(records, local_path) {
  paths <- .litxr_ensure_journal_dirs(local_path)
  if (!nrow(records)) {
    return(invisible(.litxr_delta_index_path(local_path)))
  }

  existing <- .litxr_read_collection_delta(local_path)
  delta <- .litxr_upsert_records(existing, records)
  fst::write_fst(
    as.data.frame(.litxr_index_encode(delta)),
    .litxr_delta_index_path(local_path)
  )
  invisible(.litxr_delta_index_path(local_path))
}

.litxr_clear_collection_delta <- function(local_path) {
  path <- .litxr_delta_index_path(local_path)
  if (file.exists(path)) {
    unlink(path)
  }
  invisible(path)
}

.litxr_project_root <- function(cfg) {
  .litxr_resolve_from_config_root(cfg, cfg$project$data_root)
}

`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

.litxr_project_index_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "index")
}

.litxr_project_llm_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "llm")
}

.litxr_project_md_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "md")
}

.litxr_project_references_index_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "references.fst")
}

.litxr_project_reference_collections_index_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "reference_collections.fst")
}

.litxr_sync_state_index_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "sync_state.fst")
}

.litxr_ensure_project_index_dir <- function(cfg) {
  dir_path <- .litxr_project_index_dir(cfg)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  dir_path
}

.litxr_ensure_project_llm_dir <- function(cfg) {
  dir_path <- .litxr_project_llm_dir(cfg)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  dir_path
}

.litxr_ensure_project_md_dir <- function(cfg) {
  dir_path <- .litxr_project_md_dir(cfg)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  dir_path
}

.litxr_llm_digest_path <- function(cfg, ref_id) {
  file.path(.litxr_project_llm_dir(cfg), paste0(.litxr_record_slug(data.table::data.table(ref_id = ref_id, doi = NA_character_)), ".json"))
}

.litxr_md_path <- function(cfg, ref_id) {
  file.path(.litxr_project_md_dir(cfg), paste0(.litxr_record_slug(data.table::data.table(ref_id = ref_id, doi = NA_character_)), ".md"))
}

.litxr_enrichment_status_index_path <- function(cfg) {
  file.path(.litxr_project_index_dir(cfg), "enrichment_status.fst")
}

.litxr_empty_sync_state <- function() {
  data.table::data.table(
    collection_id = character(),
    remote_channel = character(),
    sync_type = character(),
    status = character(),
    started_at = character(),
    completed_at = character(),
    query = character(),
    range_from = character(),
    range_to = character(),
    fetched_from = character(),
    fetched_to = character(),
    page_start = integer(),
    page_size = integer(),
    records_fetched = integer(),
    records_after = integer(),
    notes = character()
  )
}

.litxr_make_sync_state_row <- function(
  collection_id,
  remote_channel,
  sync_type,
  status,
  started_at,
  completed_at,
  query = NA_character_,
  range_from = NA_character_,
  range_to = NA_character_,
  fetched_from = NA_character_,
  fetched_to = NA_character_,
  page_start = NA_integer_,
  page_size = NA_integer_,
  records_fetched = NA_integer_,
  records_after = NA_integer_,
  notes = NA_character_
) {
  data.table::data.table(
    collection_id = as.character(collection_id %||% NA_character_),
    remote_channel = as.character(remote_channel %||% NA_character_),
    sync_type = as.character(sync_type %||% NA_character_),
    status = as.character(status %||% NA_character_),
    started_at = as.character(started_at %||% NA_character_),
    completed_at = as.character(completed_at %||% NA_character_),
    query = as.character(query %||% NA_character_),
    range_from = as.character(range_from %||% NA_character_),
    range_to = as.character(range_to %||% NA_character_),
    fetched_from = as.character(fetched_from %||% NA_character_),
    fetched_to = as.character(fetched_to %||% NA_character_),
    page_start = as.integer(page_start %||% NA_integer_),
    page_size = as.integer(page_size %||% NA_integer_),
    records_fetched = as.integer(records_fetched %||% NA_integer_),
    records_after = as.integer(records_after %||% NA_integer_),
    notes = as.character(notes %||% NA_character_)
  )
}

.litxr_infer_collection_sync_state <- function(cfg, collection) {
  local_path <- .litxr_resolve_local_path(cfg, collection$local_path)
  paths <- .litxr_journal_paths(local_path)
  index_path <- .litxr_index_path(local_path)

  record_count <- 0L
  timestamp <- NA_character_
  notes <- character()

  if (file.exists(index_path)) {
    info <- file.info(index_path)
    timestamp <- format(as.POSIXct(info$mtime, tz = "UTC"), tz = "UTC", usetz = TRUE)
    index_dt <- tryCatch(.litxr_read_journal_index(local_path), error = function(e) NULL)
    record_count <- if (is.null(index_dt)) 0L else nrow(index_dt)
    notes <- c(notes, "inferred_from=collection_index")
  } else if (dir.exists(paths$json)) {
    json_files <- sort(list.files(paths$json, pattern = "\\.json$", full.names = TRUE))
    json_files <- json_files[basename(json_files) != "_upsert_conflicts.jsonl"]
    if (length(json_files)) {
      info <- file.info(json_files)
      timestamp <- format(as.POSIXct(max(info$mtime, na.rm = TRUE), tz = "UTC"), tz = "UTC", usetz = TRUE)
      record_count <- length(json_files)
      notes <- c(notes, "inferred_from=json_files")
    }
  }

  if (!isTRUE(record_count > 0L) || is.na(timestamp) || !nzchar(timestamp)) {
    return(.litxr_empty_sync_state())
  }

  .litxr_make_sync_state_row(
    collection_id = collection$collection_id,
    remote_channel = collection$remote_channel,
    sync_type = "inferred_rebuild",
    status = "inferred",
    started_at = timestamp,
    completed_at = timestamp,
    query = .litxr_sync_query_text(collection),
    range_from = NA_character_,
    range_to = NA_character_,
    fetched_from = NA_character_,
    fetched_to = NA_character_,
    page_start = NA_integer_,
    page_size = collection$sync$rows %||% if (identical(collection$remote_channel, "crossref")) 1000L else 100L,
    records_fetched = as.integer(record_count),
    records_after = as.integer(record_count),
    notes = paste(notes, collapse = ";")
  )
}

.litxr_records_date_range <- function(records) {
  if (is.null(records) || !nrow(records)) {
    return(list(from = NA_character_, to = NA_character_))
  }

  if ("pub_date" %in% names(records)) {
    dates <- records$pub_date
    dates <- dates[!is.na(dates)]
    if (length(dates)) {
      return(list(
        from = format(min(dates), "%Y-%m-%d"),
        to = format(max(dates), "%Y-%m-%d")
      ))
    }
  }

  if ("year" %in% names(records)) {
    years <- records$year
    years <- years[!is.na(years)]
    if (length(years)) {
      return(list(
        from = as.character(min(years)),
        to = as.character(max(years))
      ))
    }
  }

  list(from = NA_character_, to = NA_character_)
}

.litxr_read_sync_state_index <- function(cfg) {
  path <- .litxr_sync_state_index_path(cfg)
  if (!file.exists(path)) {
    return(.litxr_empty_sync_state())
  }
  fst::read_fst(path, as.data.table = TRUE)
}

.litxr_write_sync_state_index <- function(cfg, rows) {
  .litxr_ensure_project_index_dir(cfg)
  if (is.null(rows) || !nrow(rows)) {
    rows <- .litxr_empty_sync_state()
  }
  fst::write_fst(as.data.frame(rows), .litxr_sync_state_index_path(cfg))
  invisible(.litxr_sync_state_index_path(cfg))
}

.litxr_append_sync_state <- function(cfg, row) {
  existing <- .litxr_read_sync_state_index(cfg)
  incoming <- if (is.null(row) || !nrow(row)) .litxr_empty_sync_state() else row
  merged <- data.table::rbindlist(list(existing, incoming), fill = TRUE)
  .litxr_write_sync_state_index(cfg, merged)
}

.litxr_sync_query_text <- function(journal) {
  if (identical(journal$remote_channel, "arxiv")) {
    return(as.character(journal$sync$search_query %||% NA_character_))
  }
  if (identical(journal$remote_channel, "crossref")) {
    issns <- .litxr_journal_issns(journal)
    if (!length(issns)) return(NA_character_)
    return(paste(issns, collapse = ","))
  }
  NA_character_
}

.litxr_build_enrichment_status_index <- function(cfg) {
  refs <- .litxr_read_project_references_index(cfg)
  if (!nrow(refs)) {
    return(data.table::data.table(
      ref_id = character(),
      has_md = logical(),
      has_llm_digest = logical(),
      updated_at = character()
    ))
  }

  md_dir <- .litxr_project_md_dir(cfg)
  llm_dir <- .litxr_project_llm_dir(cfg)

  status <- data.table::data.table(
    ref_id = refs$ref_id,
    has_md = vapply(refs$ref_id, function(x) file.exists(.litxr_md_path(cfg, x)), logical(1)),
    has_llm_digest = vapply(refs$ref_id, function(x) file.exists(.litxr_llm_digest_path(cfg, x)), logical(1)),
    updated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )

  status
}

.litxr_write_enrichment_status_index <- function(cfg) {
  .litxr_ensure_project_index_dir(cfg)
  status <- .litxr_build_enrichment_status_index(cfg)
  fst::write_fst(as.data.frame(status), .litxr_enrichment_status_index_path(cfg))
  invisible(.litxr_enrichment_status_index_path(cfg))
}

.litxr_read_enrichment_status_index <- function(cfg) {
  path <- .litxr_enrichment_status_index_path(cfg)
  if (!file.exists(path)) {
    .litxr_write_enrichment_status_index(cfg)
  }
  fst::read_fst(path, as.data.table = TRUE)
}

.litxr_project_reference_columns <- function(records) {
  setdiff(names(records), c("collection_id", "collection_title"))
}

.litxr_project_references_from_collection_records <- function(records) {
  if (!nrow(records)) {
    return(data.table::data.table())
  }
  cols <- .litxr_project_reference_columns(records)
  data.table::copy(records)[, cols, with = FALSE]
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

.litxr_read_project_references_index <- function(cfg) {
  path <- .litxr_project_references_index_path(cfg)
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  .litxr_index_decode(fst::read_fst(path, as.data.table = TRUE))
}

.litxr_write_project_references_index <- function(cfg, records) {
  .litxr_ensure_project_index_dir(cfg)
  fst::write_fst(as.data.frame(.litxr_index_encode(records)), .litxr_project_references_index_path(cfg))
  invisible(.litxr_project_references_index_path(cfg))
}

.litxr_read_project_reference_collections_index <- function(cfg) {
  path <- .litxr_project_reference_collections_index_path(cfg)
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  fst::read_fst(path, as.data.table = TRUE)
}

.litxr_write_project_reference_collections_index <- function(cfg, links) {
  .litxr_ensure_project_index_dir(cfg)
  fst::write_fst(as.data.frame(links), .litxr_project_reference_collections_index_path(cfg))
  invisible(.litxr_project_reference_collections_index_path(cfg))
}

.litxr_update_project_indexes <- function(cfg, journal, records) {
  .litxr_ensure_project_index_dir(cfg)

  incoming_refs <- .litxr_project_references_from_collection_records(records)
  existing_refs <- .litxr_read_project_references_index(cfg)
  merged_refs <- .litxr_upsert_records(
    existing_refs,
    incoming_refs,
    conflict_path = file.path(.litxr_project_index_dir(cfg), "_reference_conflicts.jsonl")
  )
  .litxr_write_project_references_index(cfg, merged_refs)

  existing_links <- .litxr_read_project_reference_collections_index(cfg)
  incoming_links <- .litxr_project_reference_links_from_collection_records(records, journal)
  if (!nrow(incoming_links)) {
    merged_links <- existing_links
  } else if (nrow(existing_links)) {
    keep_old <- !(existing_links$collection_id == incoming_links$collection_id[[1]] &
      existing_links$ref_id %in% incoming_links$ref_id)
    merged_links <- data.table::rbindlist(list(existing_links[keep_old, ], incoming_links), fill = TRUE)
  } else {
    merged_links <- incoming_links
  }
  link_key <- paste(merged_links$ref_id, merged_links$collection_id, sep = "\r")
  merged_links <- merged_links[!duplicated(link_key), ]
  .litxr_write_project_reference_collections_index(cfg, merged_links)
  invisible(NULL)
}

.litxr_row_to_storage_payload <- function(row, journal) {
  values <- stats::setNames(lapply(names(row), function(name) row[[name]]), names(row))
  values$authors_list <- unname(values$authors_list[[1]])
  values$pub_date <- if (!length(values$pub_date) || is.na(values$pub_date)) {
    NA_character_
  } else {
    format(values$pub_date, tz = "UTC", usetz = TRUE)
  }
  values$raw_entry <- .litxr_serialize_raw_entry(values$raw_entry)
  values$journal_config <- list(
    collection_id = if (!is.null(journal$collection_id)) journal$collection_id else journal$journal_id,
    collection_type = journal$collection_type,
    title = journal$title,
    remote_channel = journal$remote_channel
  )
  values
}

.litxr_serialize_raw_entry <- function(raw_entry) {
  if (is.null(raw_entry)) {
    return(NULL)
  }

  if (is.list(raw_entry) && length(raw_entry) == 1L) {
    raw_entry <- raw_entry[[1]]
  }

  if (inherits(raw_entry, c("xml_node", "xml_document"))) {
    return(as.character(raw_entry))
  }

  raw_entry
}

.litxr_storage_payload_to_row <- function(path) {
  payload <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  values <- payload[setdiff(names(payload), "journal_config")]

  if (!is.null(values$pub_date) && !is.na(values$pub_date) && nzchar(values$pub_date)) {
    values$pub_date <- as.POSIXct(values$pub_date, tz = "UTC")
  } else {
    values$pub_date <- as.POSIXct(NA)
  }

  if (is.null(values$authors_list)) {
    values$authors_list <- list(character())
  } else {
    values$authors_list <- list(unlist(values$authors_list, use.names = FALSE))
  }

  if (is.null(values$raw_entry)) {
    values$raw_entry <- list(NULL)
  } else {
    values$raw_entry <- list(values$raw_entry)
  }

  data.table::as.data.table(values)
}
