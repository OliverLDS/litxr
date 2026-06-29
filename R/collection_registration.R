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
    local_path = file.path("ref", collection_id),
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

.litxr_unique_collection_id <- function(cfg, base_id) {
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

.litxr_collection_entry_by_id <- function(cfg, collection_id) {
  collection_id <- as.character(collection_id)[[1L]]
  if (is.na(collection_id) || !nzchar(collection_id)) {
    return(NULL)
  }

  collections <- .litxr_config_collections(cfg)
  for (collection in collections) {
    if (identical(as.character(collection$collection_id), collection_id) ||
        identical(as.character(collection$journal_id), collection_id)) {
      return(collection)
    }
  }

  NULL
}

.litxr_crossref_journal_title <- function(cr_message) {
  title <- cr_message$`container-title`
  if (is.null(title) || length(title) == 0) return(NA_character_)
  title <- as.character(title[[1]])
  if (!nzchar(title)) NA_character_ else title
}

.litxr_valid_crossref_journal_title <- function(title) {
  title <- as.character(title)[[1L]]
  if (is.na(title) || !nzchar(trimws(title))) {
    return(FALSE)
  }
  title <- trimws(title)
  if (grepl("^[0-9]+([a-z]+)?$", title, ignore.case = TRUE)) {
    return(FALSE)
  }
  if (nchar(title, type = "chars") <= 2L) {
    return(FALSE)
  }
  TRUE
}

.litxr_crossref_issns <- function(cr_message) {
  issn <- cr_message$ISSN
  if (is.null(issn) || !length(issn)) return(character())
  unique(stats::na.omit(as.character(unlist(issn, use.names = FALSE))))
}

.litxr_crossref_journal_metadata <- function(cr_message) {
  issns <- .litxr_crossref_issns(cr_message)
  list(
    publisher = .litxr_scalar_chr(cr_message$publisher),
    issn_print = if (length(issns)) issns[[1]] else NA_character_,
    issn_electronic = if (length(issns) >= 2L) issns[[2]] else NA_character_
  )
}

.litxr_match_crossref_journal <- function(cfg, cr_message) {
  journal_title <- .litxr_crossref_journal_title(cr_message)
  if (!.litxr_valid_crossref_journal_title(journal_title)) {
    journal_title <- NA_character_
  }
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
  if (!.litxr_valid_crossref_journal_title(journal_title)) {
    journal_title <- NA_character_
  }
  base_id <- if (is.na(journal_title) || !nzchar(journal_title)) {
    NA_character_
  } else {
    .litxr_make_journal_id(journal_title)
  }

  if (is.na(base_id) || !nzchar(base_id) || grepl("^[0-9]+$", base_id)) {
    return(.litxr_register_unclassified_doi_collection(cfg, cr_message))
  }

  journal_id <- .litxr_unique_collection_id(cfg, base_id)
  metadata <- .litxr_crossref_journal_metadata(cr_message)
  local_path <- file.path("ref", journal_id)
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

.litxr_register_unclassified_doi_collection <- function(cfg, cr_message = NULL) {
  existing <- .litxr_collection_entry_by_id(cfg, "unclassified_doi")
  if (!is.null(existing)) {
    return(list(cfg = cfg, journal = existing))
  }

  metadata <- list()
  if (!is.null(cr_message)) {
    metadata$publisher <- .litxr_scalar_chr(cr_message$publisher)
  }

  journal <- list(
    collection_id = "unclassified_doi",
    collection_type = "doi_collection",
    title = "Unclassified DOI",
    remote_channel = "crossref",
    local_path = file.path("ref", "unclassified_doi"),
    metadata = metadata,
    sync = list(
      filters = list(
        issn = NA_character_
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

.litxr_arxiv_category_collection_id <- function(category) {
  category <- as.character(category)[[1L]]
  if (is.na(category) || !nzchar(trimws(category))) {
    return(NA_character_)
  }
  category <- trimws(category)
  category <- gsub("[^A-Za-z0-9]+", "_", category)
  category <- tolower(category)
  category <- gsub("^_+|_+$", "", category)
  if (!nzchar(category)) {
    return(NA_character_)
  }
  paste0("arxiv_", category)
}

.litxr_find_arxiv_collection <- function(cfg, category) {
  category <- as.character(category)[[1L]]
  if (is.na(category) || !nzchar(trimws(category))) {
    return(NULL)
  }
  target_id <- .litxr_arxiv_category_collection_id(category)
  for (collection in .litxr_config_collections(cfg)) {
    if (identical(as.character(collection$collection_id), target_id)) {
      return(collection)
    }
    if (!is.null(collection$metadata$category) && identical(as.character(collection$metadata$category), category)) {
      return(collection)
    }
  }
  NULL
}

.litxr_register_arxiv_collection <- function(cfg, category) {
  category <- as.character(category)[[1L]]
  if (is.na(category) || !nzchar(trimws(category))) {
    stop("`category` must be a non-empty arXiv primary category.", call. = FALSE)
  }

  target_id <- .litxr_arxiv_category_collection_id(category)
  collection_id <- .litxr_unique_collection_id(cfg, target_id)
  title <- paste0("arXiv ", category)
  collection <- list(
    collection_id = collection_id,
    collection_type = "arxiv_category",
    title = title,
    remote_channel = "arxiv",
    local_path = file.path("ref", collection_id),
    metadata = list(
      archive = "arXiv",
      category = category
    ),
    sync = list(
      search_query = paste0("cat:", category)
    )
  )

  collections <- .litxr_config_collections(cfg)
  collections[[length(collections) + 1L]] <- collection
  cfg$collections <- collections
  cfg <- .litxr_normalize_config_schema(cfg)
  .litxr_write_config(cfg)
  list(cfg = cfg, collection = collection)
}

.litxr_make_journal_id <- function(x) {
  x <- as.character(x)[[1L]]
  x <- tolower(x)
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  if (!nzchar(x)) {
    return("journal")
  }
  x
}

.litxr_assign_crossref_journals <- function(cfg, records, messages, auto_register = TRUE) {
  out_cfg <- cfg
  out_records <- data.table::copy(records)
  if (!("collection_id" %in% names(out_records))) {
    out_records[["collection_id"]] <- rep(NA_character_, nrow(out_records))
  }
  if (!("collection_title" %in% names(out_records))) {
    out_records[["collection_title"]] <- rep(NA_character_, nrow(out_records))
  }

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

    data.table::set(out_records, i = i, j = "collection_id", value = as.character(journal$collection_id))
    data.table::set(out_records, i = i, j = "collection_title", value = as.character(journal$title))
  }

  list(cfg = out_cfg, records = out_records)
}
