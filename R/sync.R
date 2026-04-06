#' Sync one configured journal
#'
#' Fetches records from the configured remote channel, parses them into the
#' unified reference schema, and stores one JSON metadata file per record under
#' the journal's local path.
#'
#' @param journal_id Journal identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of synced records.
#' @export
litxr_sync_journal <- function(journal_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, journal_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  incoming <- switch(
    journal$remote_channel,
    crossref = .litxr_sync_crossref_journal(journal),
    arxiv = .litxr_sync_arxiv_journal(journal),
    stop("Unsupported remote channel: ", journal$remote_channel, call. = FALSE)
  )

  existing <- .litxr_read_journal_records(local_path)
  records <- .litxr_upsert_journal_records(existing, incoming, local_path = local_path)

  .litxr_write_journal_records(records, local_path, journal)
  records
}

#' Sync all configured journals
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Named list of synced `data.table`s.
#' @export
litxr_sync_all <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  stats::setNames(
    lapply(cfg$journals, function(journal) litxr_sync_journal(journal$journal_id, cfg)),
    vapply(cfg$journals, `[[`, character(1), "journal_id")
  )
}

#' Read locally stored records for one journal
#'
#' @param journal_id Journal identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return `data.table` of local records.
#' @export
litxr_read_journal <- function(journal_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, journal_id)
  .litxr_read_journal_records(.litxr_resolve_local_path(cfg, journal$local_path))
}

#' Repair or incrementally fill a journal's local store
#'
#' Intended for bounded repair runs that should respect remote rate limits. For
#' arXiv journals, this can narrow the sync to a submitted-date window and/or a
#' smaller batch.
#'
#' @param journal_id Journal identifier from `config.yaml`.
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
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, journal_id)

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
  records <- .litxr_upsert_journal_records(existing, incoming, local_path = local_path)
  .litxr_write_journal_records(records, local_path, journal)
  records
}

#' Rebuild the local journal fst index from JSON files
#'
#' @param journal_id Journal identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the rebuilt fst index path.
#' @export
litxr_rebuild_journal_index <- function(journal_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  journal <- .litxr_get_journal(cfg, journal_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  records <- .litxr_read_journal_records_from_json(local_path)
  .litxr_write_journal_index(records, local_path)
}

#' Export local references to a BibTeX file
#'
#' @param output Output `.bib` file path.
#' @param journal_ids Optional character vector of journal ids to export.
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
  selected <- cfg$journals

  if (!is.null(journal_ids)) {
    keep <- vapply(selected, function(x) x$journal_id %in% journal_ids, logical(1))
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

#' Add references by DOI and auto-register missing journals
#'
#' Fetches Crossref metadata for a DOI vector, writes the records into the local
#' journal stores, and auto-registers journals in `config.yaml` when needed.
#'
#' @param dois Character vector of DOIs.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param auto_register Whether to auto-register missing journals into
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
    merged <- .litxr_upsert_journal_records(existing, incoming, local_path = local_path)
    .litxr_write_journal_records(merged, local_path, journal)
    incoming
  })

  data.table::rbindlist(out, fill = TRUE)
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

    out_records$collection_id[[i]] <- journal$journal_id
    out_records$collection_title[[i]] <- journal$title
  }

  list(cfg = out_cfg, records = out_records)
}

.litxr_match_crossref_journal <- function(cfg, cr_message) {
  journal_title <- .litxr_crossref_journal_title(cr_message)
  issns <- .litxr_crossref_issns(cr_message)

  for (journal in cfg$journals) {
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
    journal_id = journal_id,
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

  cfg$journals[[length(cfg$journals) + 1L]] <- journal
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
  existing_ids <- vapply(cfg$journals, `[[`, character(1), "journal_id")
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
  matches <- Filter(function(x) identical(x$journal_id, journal_id), cfg$journals)
  if (!length(matches)) {
    stop("Journal not found in config: ", journal_id, call. = FALSE)
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
    row[["collection_id"]] <- journal$journal_id
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
    row[["collection_id"]] <- journal$journal_id
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

.litxr_resolve_local_path <- function(cfg, local_path) {
  if (grepl("^(/|[A-Za-z]:[/\\\\])", local_path)) {
    return(local_path)
  }

  root <- attr(cfg, "config_root", exact = TRUE)
  if (is.null(root) || !nzchar(root)) {
    root <- "."
  }

  file.path(root, local_path)
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
  ref_id <- records[["ref_id"]]

  has_doi <- !is.na(doi) & nzchar(doi)
  keys <- ref_id
  keys[has_doi] <- paste0("doi:", doi[has_doi])
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

  out <- data.table::copy(records)
  out[["litxr_record_key__"]] <- key_fun(out)
  out[["litxr_completeness__"]] <- .litxr_record_completeness_score(out)

  ord <- order(out[["litxr_record_key__"]], -out[["litxr_completeness__"]])
  out <- out[ord, ]
  out <- out[!duplicated(out[["litxr_record_key__"]]), ]
  out[["litxr_record_key__"]] <- NULL
  out[["litxr_completeness__"]] <- NULL
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
  keys[has_doi] <- paste0("doi:", doi[has_doi])
  keys
}

.litxr_normalize_record_identity <- function(records) {
  if (!nrow(records)) {
    return(records)
  }

  out <- data.table::copy(records)
  if (!("doi" %in% names(out))) {
    return(out)
  }

  source <- if ("source" %in% names(out)) out[["source"]] else rep(NA_character_, nrow(out))
  source_id <- if ("source_id" %in% names(out)) out[["source_id"]] else rep(NA_character_, nrow(out))
  ref_id <- if ("ref_id" %in% names(out)) out[["ref_id"]] else rep(NA_character_, nrow(out))
  doi <- out[["doi"]]

  missing_doi <- is.na(doi) | !nzchar(doi)
  crossref_source_id <- missing_doi & !is.na(source) & source == "crossref" & !is.na(source_id) & nzchar(source_id)
  doi[crossref_source_id] <- source_id[crossref_source_id]

  crossref_ref_id <- missing_doi & !is.na(source) & source == "crossref" & !is.na(ref_id) & grepl("^doi:", ref_id)
  doi[crossref_ref_id] <- sub("^doi:", "", ref_id[crossref_ref_id])

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
  existing_row <- stats::setNames(lapply(names(existing_row), function(name) existing_row[[name]]), names(existing_row))
  incoming_row <- stats::setNames(lapply(names(incoming_row), function(name) incoming_row[[name]]), names(incoming_row))
  fields <- union(names(existing_row), names(incoming_row))
  values <- stats::setNames(vector("list", length(fields)), fields)

  for (field in fields) {
    existing_value <- existing_row[[field]]
    incoming_value <- incoming_row[[field]]

    if (field %in% c("authors_list", "raw_entry")) {
      values[[field]] <- if (!.litxr_nullish(incoming_value)) incoming_value else existing_value
    } else {
      values[[field]] <- .litxr_merge_field(existing_row, incoming_row, field, key, conflict_env)
    }
  }

  data.table::as.data.table(values)
}

.litxr_write_upsert_conflicts <- function(conflicts, local_path) {
  if (!length(conflicts$rows)) return(invisible(NULL))

  conflict_path <- file.path(local_path, "json", "_upsert_conflicts.jsonl")
  lines <- vapply(conflicts$rows, function(row) {
    jsonlite::toJSON(as.list(row), auto_unbox = TRUE, null = "null")
  }, character(1))

  write(lines, file = conflict_path, append = TRUE)
  invisible(conflict_path)
}

.litxr_upsert_journal_records <- function(existing, incoming, local_path) {
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

  .litxr_write_upsert_conflicts(conflicts, local_path)
  data.table::rbindlist(merged, fill = TRUE)
}

.litxr_write_journal_records <- function(records, local_path, journal) {
  paths <- .litxr_ensure_journal_dirs(local_path)
  if (!nrow(records)) {
    .litxr_write_journal_index(records, local_path)
    return(invisible(character()))
  }

  written <- unlist(lapply(seq_len(nrow(records)), function(i) {
    row <- records[i, ]
    payload <- .litxr_row_to_storage_payload(row, journal)
    json_path <- file.path(paths$json, paste0(.litxr_record_slug(row), ".json"))
    jsonlite::write_json(payload, json_path, auto_unbox = TRUE, pretty = TRUE, null = "null")
    json_path
  }), use.names = FALSE)

  .litxr_write_journal_index(records, local_path)
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
    journal_id = journal$journal_id,
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
