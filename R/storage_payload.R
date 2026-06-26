.litxr_scalar_or_na <- function(value) {
  if (is.null(value) || !length(value)) {
    return(NA_character_)
  }
  if (is.list(value)) {
    value <- value[[1L]]
  }
  if (is.null(value) || !length(value)) {
    return(NA_character_)
  }
  if (length(value) == 1L && is.na(value[[1L]])) {
    return(NA_character_)
  }
  as.character(value[[1L]])
}

.litxr_storage_payload_character_fields <- c(
  "abstract",
  "note",
  "subject_primary",
  "subject_all",
  "source",
  "source_id",
  "entry_type",
  "title",
  "authors",
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
  "arxiv_id_versioned",
  "arxiv_id_base",
  "linked_doi_ref_id",
  "linked_arxiv_ref_id",
  "arxiv_primary_category",
  "arxiv_categories_raw",
  "arxiv_comment",
  "arxiv_journal_ref",
  "url_pdf",
  "url_landing",
  "arxiv_version",
  "collection_id",
  "collection_title"
)

.litxr_storage_payload_integer_fields <- c("year", "month", "day")

.litxr_storage_payload_list_fields <- c("authors_list", "raw_entry")

.litxr_storage_payload_field_groups <- function() {
  list(
    character = .litxr_storage_payload_character_fields,
    integer = .litxr_storage_payload_integer_fields,
    list = .litxr_storage_payload_list_fields
  )
}

.litxr_storage_payload_defaults <- function() {
  list(
    abstract = NA_character_,
    note = NA_character_,
    subject_primary = NA_character_,
    subject_all = NA_character_,
    source = NA_character_,
    source_id = NA_character_,
    entry_type = NA_character_,
    title = NA_character_,
    authors = NA_character_,
    journal = NA_character_,
    container_title = NA_character_,
    publisher = NA_character_,
    volume = NA_character_,
    issue = NA_character_,
    pages = NA_character_,
    doi = NA_character_,
    isbn = NA_character_,
    issn = NA_character_,
    url = NA_character_,
    arxiv_id_versioned = NA_character_,
    arxiv_id_base = NA_character_,
    linked_doi_ref_id = NA_character_,
    linked_arxiv_ref_id = NA_character_,
    arxiv_primary_category = NA_character_,
    arxiv_categories_raw = NA_character_,
    arxiv_comment = NA_character_,
    arxiv_journal_ref = NA_character_,
    url_pdf = NA_character_,
    url_landing = NA_character_,
    arxiv_version = NA_character_,
    collection_id = NA_character_,
    collection_title = NA_character_,
    year = NA_integer_,
    month = NA_integer_,
    day = NA_integer_,
    pub_date = as.POSIXct(NA, tz = "UTC"),
    authors_list = list(character()),
    raw_entry = list(NULL)
  )
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

.litxr_storage_payload_as_list <- function(path, fields = NULL) {
  payload <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  payload$journal_config <- NULL

  fields <- unique(as.character(fields))
  fields <- fields[nzchar(fields)]
  full_payload <- !length(fields)
  if (full_payload) {
    values <- payload
    fields <- unique(c(
      .litxr_storage_payload_character_fields,
      .litxr_storage_payload_integer_fields,
      "pub_date",
      .litxr_storage_payload_list_fields
    ))
  } else {
    values <- list()
  }

  defaults <- .litxr_storage_payload_defaults()
  groups <- .litxr_storage_payload_field_groups()
  source_payload <- if (full_payload) values else payload
  for (name in intersect(groups$character, fields)) {
    value <- source_payload[[name]]
    if (is.null(value) || !length(value)) {
      values[[name]] <- defaults[[name]]
    } else {
      values[[name]] <- .litxr_scalar_or_na(value)
    }
  }
  for (name in intersect(groups$integer, fields)) {
    value <- source_payload[[name]]
    if (is.null(value) || !length(value)) {
      values[[name]] <- defaults[[name]]
    } else {
      values[[name]] <- suppressWarnings(as.integer(value[[1L]]))
      if (is.na(values[[name]])) values[[name]] <- defaults[[name]]
    }
  }
  if ("pub_date" %in% fields) {
    pub_date <- source_payload$pub_date
    if (inherits(pub_date, c("POSIXct", "POSIXt"))) {
      values$pub_date <- pub_date
    } else {
      pub_date <- .litxr_scalar_or_na(pub_date)
      values$pub_date <- if (!is.na(pub_date) && nzchar(pub_date)) {
        .litxr_parse_arxiv_posixct(pub_date)
      } else {
        defaults$pub_date
      }
    }
  }

  if ("authors_list" %in% fields) {
    if (is.null(source_payload$authors_list) || !length(source_payload$authors_list)) {
      values$authors_list <- defaults$authors_list
    } else {
      values$authors_list <- list(unlist(source_payload$authors_list, use.names = FALSE))
    }
  }
  if ("raw_entry" %in% fields) {
    if (is.null(source_payload$raw_entry) || !length(source_payload$raw_entry)) {
      values$raw_entry <- defaults$raw_entry
    } else {
      values$raw_entry <- list(.litxr_serialize_raw_entry(source_payload$raw_entry))
    }
  }

  if (!full_payload) {
    requested <- fields
    if ("ref_id" %in% requested && !("ref_id" %in% names(values))) {
      ref_id <- payload$ref_id
      values$ref_id <- if (is.null(ref_id) || !length(ref_id)) {
        NA_character_
      } else {
        .litxr_scalar_or_na(ref_id)
      }
    }
    for (name in setdiff(requested, names(values))) {
      if (name %in% groups$integer) {
        values[[name]] <- defaults[[name]]
      } else if (name %in% groups$list) {
        values[[name]] <- defaults[[name]]
      } else if (identical(name, "pub_date")) {
        values[[name]] <- defaults$pub_date
      } else {
        values[[name]] <- NA_character_
      }
    }
    values <- values[requested]
  }

  values
}

.litxr_storage_payload_to_row <- function(path) {
  .litxr_storage_payload_as_list(path)
}

.litxr_storage_payload_to_projection_row <- function(path) {
  payload <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  defaults <- .litxr_storage_payload_defaults()
  cols <- .litxr_reference_projection_columns()

  values <- vector("list", length(cols))
  names(values) <- cols
  for (name in cols) {
    value <- payload[[name]]
    if (is.null(value) || !length(value)) {
      values[[name]] <- defaults[[name]]
    } else if (identical(name, "pub_date")) {
      if (inherits(value, c("POSIXct", "POSIXt"))) {
        values[[name]] <- value
      } else {
        scalar <- if (is.list(value)) value[[1L]] else value[[1L]]
        scalar <- as.character(scalar)
        values[[name]] <- if (!is.na(scalar) && nzchar(scalar)) {
          .litxr_parse_arxiv_posixct(scalar)
        } else {
          defaults[[name]]
        }
      }
    } else if (identical(name, "year") || identical(name, "month") || identical(name, "day")) {
      scalar <- if (is.list(value)) value[[1L]] else value[[1L]]
      values[[name]] <- suppressWarnings(as.integer(scalar))
      if (is.na(values[[name]])) values[[name]] <- defaults[[name]]
    } else if (is.list(value)) {
      values[[name]] <- value[[1L]]
    } else {
      values[[name]] <- as.character(value[[1L]])
    }
  }

  values
}
