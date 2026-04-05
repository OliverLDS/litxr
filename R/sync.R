#' Sync one configured journal
#'
#' Fetches records from the configured remote channel, parses them into the
#' unified reference schema, and stores one JSON metadata file per record under
#' the journal's local path.
#'
#' @param journal_id Journal identifier from `config.yaml`.
#' @param config Parsed config list or a path that `litxr_read_config()` accepts.
#'
#' @return `data.table` of synced records.
#' @export
litxr_sync_journal <- function(journal_id, config = ".") {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  journal <- .litxr_get_journal(cfg, journal_id)
  local_path <- .litxr_resolve_local_path(cfg, journal$local_path)
  records <- switch(
    journal$remote_channel,
    crossref = .litxr_sync_crossref_journal(journal),
    arxiv = .litxr_sync_arxiv_journal(journal),
    stop("Unsupported remote channel: ", journal$remote_channel, call. = FALSE)
  )

  .litxr_write_journal_records(records, local_path, journal)
  records
}

#' Sync all configured journals
#'
#' @param config Parsed config list or a path that `litxr_read_config()` accepts.
#'
#' @return Named list of synced `data.table`s.
#' @export
litxr_sync_all <- function(config = ".") {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  stats::setNames(
    lapply(cfg$journals, function(journal) litxr_sync_journal(journal$journal_id, cfg)),
    vapply(cfg$journals, `[[`, character(1), "journal_id")
  )
}

#' Read locally stored records for one journal
#'
#' @param journal_id Journal identifier from `config.yaml`.
#' @param config Parsed config list or a path that `litxr_read_config()` accepts.
#'
#' @return `data.table` of local records.
#' @export
litxr_read_journal <- function(journal_id, config = ".") {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  journal <- .litxr_get_journal(cfg, journal_id)
  .litxr_read_journal_records(.litxr_resolve_local_path(cfg, journal$local_path))
}

#' Export local references to a BibTeX file
#'
#' @param output Output `.bib` file path.
#' @param journal_ids Optional character vector of journal ids to export.
#' @param config Parsed config list or a path that `litxr_read_config()` accepts.
#'
#' @return Invisibly returns the output path.
#' @export
litxr_export_bib <- function(output, journal_ids = NULL, config = ".") {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
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
    records <- records[seq_len(limit)]
  }

  records
}

.litxr_sync_arxiv_journal <- function(journal) {
  search_query <- journal$sync$search_query
  if (is.null(search_query) || !nzchar(search_query)) {
    stop("arxiv journal entries require `sync.search_query` in config.yaml.", call. = FALSE)
  }

  max_results <- if (is.null(journal$sync$limit)) 100L else as.integer(journal$sync$limit)
  feed <- fetch_arxiv_xml(search_query = search_query, max_results = max_results)
  entries <- xml2::xml_find_all(feed, ".//entry")
  if (!length(entries)) {
    return(data.table::data.table())
  }

  rows <- lapply(entries, function(entry) {
    row <- parse_arxiv_entry_unified(entry)
    row[["collection_id"]] <- journal$journal_id
    row[["collection_title"]] <- journal$title
    row
  })

  data.table::rbindlist(rows, fill = TRUE)
}

.litxr_journal_paths <- function(local_path) {
  list(
    root = local_path,
    json = file.path(local_path, "json"),
    pdf = file.path(local_path, "pdf"),
    md = file.path(local_path, "md")
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

.litxr_write_journal_records <- function(records, local_path, journal) {
  paths <- .litxr_ensure_journal_dirs(local_path)
  if (!nrow(records)) {
    return(invisible(character()))
  }

  invisible(lapply(seq_len(nrow(records)), function(i) {
    row <- records[i, ]
    payload <- .litxr_row_to_storage_payload(row, journal)
    json_path <- file.path(paths$json, paste0(.litxr_record_slug(row), ".json"))
    jsonlite::write_json(payload, json_path, auto_unbox = TRUE, pretty = TRUE, null = "null")
    json_path
  }))
}

.litxr_read_journal_records <- function(local_path) {
  paths <- .litxr_journal_paths(local_path)
  if (!dir.exists(paths$json)) {
    return(data.table::data.table())
  }

  files <- sort(list.files(paths$json, pattern = "\\.json$", full.names = TRUE))
  if (!length(files)) {
    return(data.table::data.table())
  }

  data.table::rbindlist(lapply(files, .litxr_storage_payload_to_row), fill = TRUE)
}

.litxr_row_to_storage_payload <- function(row, journal) {
  values <- stats::setNames(lapply(names(row), function(name) row[[name]]), names(row))
  values$authors_list <- unname(values$authors_list[[1]])
  values$pub_date <- if (!length(values$pub_date) || is.na(values$pub_date)) {
    NA_character_
  } else {
    format(values$pub_date, tz = "UTC", usetz = TRUE)
  }
  values$journal_config <- list(
    journal_id = journal$journal_id,
    title = journal$title,
    remote_channel = journal$remote_channel
  )
  values
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
