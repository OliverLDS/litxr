#' Read BibTeX entries as canonical reference ids
#'
#' Reads a `.bib` file entry by entry and returns the canonical `ref_id` values
#' that can be recognized from each entry, using this priority:
#' arXiv first, then DOI, then ISBN. Entries without a recognizable id are
#' skipped.
#'
#' Recognition rules:
#' - arXiv ids are normalized to `arxiv:<id>`
#' - DOI values are normalized to `doi:<doi>`
#' - ISBN values are normalized to `isbn:<isbn>`
#'
#' @param path Path to a BibTeX `.bib` file.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param prefer_linked_arxiv If `TRUE` (default), DOI values are promoted to a
#'   linked arXiv ref_id when the identity map contains a matching DOI→arXiv
#'   pair. If `FALSE`, DOI values are returned as DOI ref_ids.
#'
#' @return Character vector of canonical reference ids in file order.
#' @export
read_bibtex_entries <- function(path, config = NULL, prefer_linked_arxiv = TRUE) {
  if (!file.exists(path)) {
    stop("BibTeX file not found: ", path, call. = FALSE)
  }

  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  link_maps <- NULL

  parsed <- .litxr_bibtex_parse_entries(path)
  if (!length(parsed$entries)) {
    return(character())
  }

  raw_ref_ids <- .litxr_bibtex_entry_ref_ids(parsed, link_maps = NULL, prefer_linked_arxiv = FALSE)
  if (isTRUE(prefer_linked_arxiv)) {
    candidates <- .litxr_bibtex_candidate_sets(raw_ref_ids)
    link_maps <- .litxr_bibtex_link_maps(cfg, arxiv_ids = candidates$arxiv_ids, doi_ids = candidates$doi_ids)
  }
  ref_ids <- .litxr_bibtex_entry_ref_ids(parsed, link_maps = link_maps, prefer_linked_arxiv = prefer_linked_arxiv)

  keep <- !is.na(ref_ids) & nzchar(ref_ids)
  ref_ids[keep]
}

.bibtex_escape <- function(x) {
  if (is.na(x) || x == "") return("")
  x <- gsub("\\\\", "\\\\textbackslash{}", x)
  x <- gsub("\\{", "\\\\{", x)
  x <- gsub("\\}", "\\\\}", x)
  x <- gsub("\"", "''", x)
  x
}

.make_citekey <- function(doi, source_id, ref_id) {
  if (!is.na(doi) && nzchar(doi)) {
    core <- sub(".*/", "", doi)
  } else if (!is.null(source_id) && nzchar(source_id)) {
    core <- source_id
  } else {
    core <- ref_id
  }
  core <- gsub("[^A-Za-z0-9]+", "_", core)
  core <- gsub("^_+|_+$", "", core)
  if (!nzchar(core)) core <- "ref"
  core
}

.make_arxiv_citekey <- function(arxiv_id) {
  arxiv_id <- .litxr_normalize_arxiv_ref_id(arxiv_id)
  if (is.na(arxiv_id) || !nzchar(arxiv_id)) {
    return("ref")
  }
  core <- sub("^arxiv:", "", arxiv_id, ignore.case = TRUE)
  core <- gsub("\\.", "_", core, fixed = TRUE)
  core <- gsub("[^A-Za-z0-9]+", "_", core)
  core <- gsub("^_+|_+$", "", core)
  if (!nzchar(core)) "ref" else core
}

.format_authors_bib <- function(authors_list) {
  if (length(authors_list) == 0L || all(is.na(authors_list))) return("")
  paste(authors_list, collapse = " and ")
}

.litxr_bibtex_parse_entries <- function(path) {
  lines <- readLines(path, warn = FALSE)
  if (!length(lines)) {
    return(list(entries = character(), keys = character(), types = character()))
  }

  entries <- vector("list", length(lines))
  keys <- vector("list", length(lines))
  types <- vector("list", length(lines))
  n_entries <- 0L
  i <- 1L
  while (i <= length(lines)) {
    line <- lines[[i]]
    if (!grepl("^\\s*@", line)) {
      i <- i + 1L
      next
    }

    key <- sub("^\\s*@\\w+\\s*\\{\\s*([^,]+),.*$", "\\1", line, perl = TRUE)
    if (!nzchar(key) || identical(key, line)) {
      stop("Unable to parse BibTeX entry key from line: ", line, call. = FALSE)
    }
    entry_type <- sub("^\\s*@([[:alnum:]]+)\\s*\\{.*$", "\\1", line, perl = TRUE)

    chunk <- vector("list", max(8L, min(64L, length(lines) - i + 1L)))
    chunk_n <- 0L
    depth <- 0L
    repeat {
      if (i > length(lines)) {
        stop("Unterminated BibTeX entry for key: ", key, call. = FALSE)
      }
      current <- lines[[i]]
      chunk_n <- chunk_n + 1L
      if (chunk_n > length(chunk)) {
        length(chunk) <- length(chunk) * 2L
      }
      chunk[[chunk_n]] <- current
      opens <- lengths(regmatches(current, gregexpr("\\{", current, perl = TRUE)))
      closes <- lengths(regmatches(current, gregexpr("\\}", current, perl = TRUE)))
      opens[opens < 0L] <- 0L
      closes[closes < 0L] <- 0L
      depth <- depth + opens - closes
      i <- i + 1L
      if (depth <= 0L) {
        break
      }
    }

    n_entries <- n_entries + 1L
    entries[[n_entries]] <- paste(unlist(chunk[seq_len(chunk_n)], use.names = FALSE), collapse = "\n")
    keys[[n_entries]] <- key
    types[[n_entries]] <- tolower(entry_type)
  }

  list(
    entries = unlist(entries[seq_len(n_entries)], use.names = FALSE),
    keys = unlist(keys[seq_len(n_entries)], use.names = FALSE),
    types = unlist(types[seq_len(n_entries)], use.names = FALSE)
  )
}

.litxr_bibtex_entry_ref_ids <- function(parsed, link_maps = NULL, prefer_linked_arxiv = TRUE) {
  n_entries <- length(parsed$entries)
  if (!n_entries) {
    return(character())
  }
  ref_ids <- rep(NA_character_, n_entries)
  for (i in seq_len(n_entries)) {
    ref_ids[[i]] <- .litxr_bibtex_entry_ref_id(
      parsed$entries[[i]],
      parsed$keys[[i]],
      link_maps = link_maps,
      prefer_linked_arxiv = prefer_linked_arxiv
    )
  }
  ref_ids
}

.litxr_bibtex_candidate_sets <- function(ref_ids) {
  ref_ids <- as.character(ref_ids)
  ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(trimws(ref_ids))]
  if (!length(ref_ids)) {
    return(list(arxiv_ids = character(), doi_ids = character(), isbn_ids = character()))
  }
  routes <- lapply(ref_ids, .litxr_route_ref_id)
  key_types <- vapply(routes, function(x) as.character(x$key_type), character(1))
  key_values <- vapply(routes, function(x) as.character(x$key_value), character(1))
  list(
    arxiv_ids = unique(vapply(key_values[key_types == "arxiv_id" & !is.na(key_values) & nzchar(key_values)], .litxr_bare_arxiv_id, character(1))),
    doi_ids = unique(vapply(key_values[key_types == "doi" & !is.na(key_values) & nzchar(key_values)], .litxr_bare_doi, character(1))),
    isbn_ids = unique(sub("^isbn:", "", vapply(key_values[key_types == "isbn" & !is.na(key_values) & nzchar(key_values)], .litxr_normalize_isbn_ref_id, character(1)), ignore.case = TRUE))
  )
}

.litxr_bibtex_entry_field <- function(entry, field) {
  field <- gsub("[^A-Za-z0-9_]", "", as.character(field))
  if (!nzchar(field)) {
    return(NA_character_)
  }

  patterns <- c(
    sprintf("(?is)\\b%s\\b\\s*=\\s*\\{(.*?)\\}", field),
    sprintf("(?is)\\b%s\\b\\s*=\\s*\"(.*?)\"", field),
    sprintf("(?is)\\b%s\\b\\s*=\\s*([^,\\n]+)", field)
  )

  for (pattern in patterns) {
    hit <- regmatches(entry, regexec(pattern, entry, perl = TRUE))[[1L]]
    if (length(hit) >= 2L) {
      value <- trimws(hit[[2L]])
      if (nzchar(value)) {
        return(value)
      }
    }
  }

  NA_character_
}

.litxr_bibtex_arxiv_candidate <- function(x) {
  x <- as.character(x)
  if (!length(x) || is.na(x[[1L]])) {
    return(NA_character_)
  }
  x <- trimws(x[[1L]])
  if (!nzchar(x)) {
    return(NA_character_)
  }

  if (grepl("^https?://(arxiv\\.org/(abs|pdf)/|export\\.arxiv\\.org/abs/)", x, ignore.case = TRUE)) {
    x <- sub("^https?://(arxiv\\.org/(abs|pdf)/|export\\.arxiv\\.org/abs/)", "", x, ignore.case = TRUE)
    x <- sub("\\.pdf$", "", x, ignore.case = TRUE)
  }

  if (grepl("^10\\.48550/arXiv\\.", x, ignore.case = TRUE)) {
    x <- sub("^10\\.48550/arXiv\\.", "", x, ignore.case = TRUE)
  }

  norm <- .litxr_normalize_arxiv_ref_id(x)
  if (!is.na(norm) && nzchar(norm)) {
    return(norm)
  }

  NA_character_
}

.litxr_bibtex_doi_candidate <- function(x) {
  x <- as.character(x)
  if (!length(x) || is.na(x[[1L]])) {
    return(NA_character_)
  }
  x <- trimws(x[[1L]])
  if (!nzchar(x)) {
    return(NA_character_)
  }

  if (grepl("^https?://(arxiv\\.org/(abs|pdf)/|export\\.arxiv\\.org/abs/)", x, ignore.case = TRUE)) {
    return(NA_character_)
  }

  if (grepl("^https?://(dx\\.)?doi\\.org/", x, ignore.case = TRUE)) {
    x <- sub("^https?://(dx\\.)?doi\\.org/", "", x, ignore.case = TRUE)
  }
  if (grepl("^doi:", x, ignore.case = TRUE)) {
    x <- sub("^doi:", "", x, ignore.case = TRUE)
  }

  norm <- .litxr_normalize_doi_ref_id(x)
  if (!is.na(norm) && nzchar(norm)) {
    return(norm)
  }

  NA_character_
}

.litxr_bibtex_isbn_candidate <- function(x) {
  x <- as.character(x)
  if (!length(x) || is.na(x[[1L]])) {
    return(NA_character_)
  }
  x <- trimws(x[[1L]])
  if (!nzchar(x)) {
    return(NA_character_)
  }
  x <- sub("^isbn\\s*:?\\s*", "", x, ignore.case = TRUE)
  x <- gsub("[[:space:]-]", "", x)
  if (!grepl("^[0-9Xx]{10,17}$", x)) {
    return(NA_character_)
  }
  paste0("isbn:", x)
}

.litxr_bibtex_key_ref_id <- function(key) {
  key <- as.character(key)
  if (!length(key) || is.na(key[[1L]])) {
    return(NA_character_)
  }
  key <- trimws(key[[1L]])
  if (!nzchar(key)) {
    return(NA_character_)
  }

  if (grepl("^https?://(arxiv\\.org/(abs|pdf)/|export\\.arxiv\\.org/abs/)", key, ignore.case = TRUE)) {
    return(.litxr_bibtex_arxiv_candidate(key))
  }
  if (grepl("^10\\.48550/arXiv\\.", key, ignore.case = TRUE)) {
    return(.litxr_bibtex_arxiv_candidate(key))
  }
  if (grepl("^arxiv:\\s*|^[0-9]{4}\\.[0-9]{4,5}(v[0-9]+)?$", key, ignore.case = TRUE)) {
    return(.litxr_bibtex_arxiv_candidate(key))
  }

  if (grepl("^https?://(dx\\.)?doi\\.org/", key, ignore.case = TRUE) ||
    grepl("^doi:\\s*|^10\\.", key, ignore.case = TRUE)) {
    return(.litxr_bibtex_doi_candidate(key))
  }

  if (grepl("^isbn:\\s*", key, ignore.case = TRUE) || grepl("^[0-9Xx][-0-9Xx ]{8,}$", key)) {
    return(.litxr_bibtex_isbn_candidate(key))
  }

  NA_character_
}

.litxr_bibtex_link_maps <- function(cfg, arxiv_ids = character(), doi_ids = character()) {
  arxiv_ids <- unique(vapply(as.character(arxiv_ids), .litxr_bare_arxiv_id, character(1)))
  arxiv_ids <- arxiv_ids[!is.na(arxiv_ids) & nzchar(arxiv_ids)]
  doi_ids <- unique(vapply(as.character(doi_ids), .litxr_bare_doi, character(1)))
  doi_ids <- doi_ids[!is.na(doi_ids) & nzchar(doi_ids)]
  if (!length(arxiv_ids) && !length(doi_ids)) {
    return(list(doi_to_arxiv = character(), arxiv_to_doi = character()))
  }
  identity_map <- tryCatch(fst::read_fst(.litxr_project_ref_identity_index_path(cfg), columns = c("arxiv_id", "doi"), as.data.table = TRUE), error = function(e) data.table::data.table())
  if (!nrow(identity_map) || !all(c("arxiv_id", "doi") %in% names(identity_map))) {
    return(list(doi_to_arxiv = character(), arxiv_to_doi = character()))
  }
  identity_map$arxiv_id <- as.character(identity_map$arxiv_id)
  identity_map$doi <- tolower(as.character(identity_map$doi))
  keep <- !is.na(identity_map$doi) & nzchar(identity_map$doi) & !is.na(identity_map$arxiv_id) & nzchar(identity_map$arxiv_id)
  if (!any(keep)) {
    return(list(doi_to_arxiv = character(), arxiv_to_doi = character()))
  }
  identity_map <- identity_map[keep, , drop = FALSE]
  keep <- rep(FALSE, nrow(identity_map))
  if (length(arxiv_ids)) {
    keep <- keep | identity_map$arxiv_id %in% arxiv_ids
  }
  if (length(doi_ids)) {
    keep <- keep | identity_map$doi %in% doi_ids
  }
  identity_map <- identity_map[keep, , drop = FALSE]
  if (!nrow(identity_map)) {
    return(list(doi_to_arxiv = character(), arxiv_to_doi = character()))
  }

  doi_to_arxiv <- identity_map$arxiv_id
  names(doi_to_arxiv) <- identity_map$doi
  doi_to_arxiv <- doi_to_arxiv[!duplicated(names(doi_to_arxiv))]

  arxiv_to_doi <- identity_map$doi
  names(arxiv_to_doi) <- identity_map$arxiv_id
  arxiv_to_doi <- arxiv_to_doi[!duplicated(names(arxiv_to_doi))]

  list(doi_to_arxiv = doi_to_arxiv, arxiv_to_doi = arxiv_to_doi)
}

.litxr_bibtex_linked_arxiv_ref_id <- function(link_maps, doi_ref_id) {
  doi_ref_id <- .litxr_bare_doi(doi = doi_ref_id)
  if (is.na(doi_ref_id) || !nzchar(doi_ref_id)) {
    return(NA_character_)
  }
  if (is.null(link_maps) || is.null(link_maps$doi_to_arxiv) || !length(link_maps$doi_to_arxiv)) {
    return(NA_character_)
  }
  hit <- unname(link_maps$doi_to_arxiv[doi_ref_id])
  if (!length(hit) || is.na(hit[[1L]]) || !nzchar(hit[[1L]])) {
    return(NA_character_)
  }
  arxiv_id <- paste0("arxiv:", as.character(hit[[1L]]))
  if (is.na(arxiv_id) || !nzchar(arxiv_id)) {
    return(NA_character_)
  }
  arxiv_id
}

.litxr_bibtex_entry_ref_id <- function(entry, key = NULL, link_maps = NULL, prefer_linked_arxiv = TRUE) {
  entry <- as.character(entry)
  if (!length(entry) || is.na(entry[[1L]])) {
    return(NA_character_)
  }
  entry <- entry[[1L]]
  key <- if (is.null(key) || !length(key)) NA_character_ else as.character(key)[[1L]]

  arxiv_fields <- c("eprint", "arxiv", "arxiv_id", "arxivid", "eprintid", "doi")
  for (field in arxiv_fields) {
    ref_id <- .litxr_bibtex_arxiv_candidate(.litxr_bibtex_entry_field(entry, field))
    if (!is.na(ref_id) && nzchar(ref_id)) {
      return(ref_id)
    }
  }

  url_candidate <- .litxr_bibtex_entry_field(entry, "url")
  if (!is.na(url_candidate) && nzchar(url_candidate)) {
    ref_id <- .litxr_bibtex_arxiv_candidate(url_candidate)
    if (!is.na(ref_id) && nzchar(ref_id)) {
      return(ref_id)
    }
  }

  doi_fields <- c("doi", "url")
  for (field in doi_fields) {
    candidate <- .litxr_bibtex_entry_field(entry, field)
    if (!is.na(candidate) && nzchar(candidate)) {
      ref_id <- .litxr_bibtex_doi_candidate(candidate)
      if (!is.na(ref_id) && nzchar(ref_id)) {
        if (isTRUE(prefer_linked_arxiv) && !is.null(link_maps)) {
          linked_arxiv <- .litxr_bibtex_linked_arxiv_ref_id(link_maps, ref_id)
          if (!is.na(linked_arxiv) && nzchar(linked_arxiv)) {
            return(linked_arxiv)
          }
        }
        return(ref_id)
      }
    }
  }

  isbn_fields <- c("isbn", "isbn13", "isbn_13", "isbn10", "isbn_10")
  for (field in isbn_fields) {
    candidate <- .litxr_bibtex_entry_field(entry, field)
    if (!is.na(candidate) && nzchar(candidate)) {
      ref_id <- .litxr_bibtex_isbn_candidate(candidate)
      if (!is.na(ref_id) && nzchar(ref_id)) {
        return(ref_id)
      }
    }
  }

  if (!is.na(key) && nzchar(key)) {
    ref_id <- .litxr_bibtex_key_ref_id(key)
    if (!is.na(ref_id) && nzchar(ref_id)) return(ref_id)
  }

  NA_character_
}

.litxr_normalize_isbn_ref_id <- function(isbn) {
  isbn <- as.character(isbn)
  if (!length(isbn) || is.na(isbn[[1L]])) {
    return(NA_character_)
  }
  isbn <- trimws(isbn[[1L]])
  if (!nzchar(isbn)) {
    return(NA_character_)
  }
  isbn <- sub("^isbn\\s*:?\\s*", "", isbn, ignore.case = TRUE)
  isbn <- gsub("[[:space:]-]", "", isbn)
  if (!grepl("^[0-9Xx]{10,17}$", isbn)) {
    return(NA_character_)
  }
  paste0("isbn:", isbn)
}

.litxr_entry_type_from_source <- function(source) {
  source <- .litxr_scalar_chr(source)
  switch(source,
    blog = "misc",
    web = "misc",
    report = "techreport",
    thesis = "phdthesis",
    magazine = "article",
    newspaper = "article",
    book = "book",
    booksection = "incollection",
    dataset = "misc",
    manuscript = "unpublished",
    conference = "inproceedings",
    encyclopedia = "incollection",
    arxiv = "unpublished",
    crossref = "article",
    "article"
  )
}

.litxr_bibtex_entry_type_from_source <- .litxr_entry_type_from_source

.litxr_bibtex_payload_to_row <- function(payload, prefer_doi_key = TRUE) {
  payload <- data.table::as.data.table(payload)
  if (!nrow(payload)) {
    return(payload)
  }
  payload$prefer_doi_key__ <- prefer_doi_key
  payload
}

.litxr_bibtex_payload_fields <- function() {
  c(
    "ref_id",
    "source",
    "source_id",
    "entry_type",
    "title",
    "authors",
    "authors_list",
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
    "note"
  )
}

.litxr_bibtex_json_path_from_scaffold_row <- function(cfg, row) {
  row <- data.table::as.data.table(row)
  if (!nrow(row) || !("collection_index" %in% names(row)) || !("json_filename" %in% names(row))) {
    return(NA_character_)
  }
  collection_index <- suppressWarnings(as.integer(row$collection_index[[1L]]))
  if (is.na(collection_index) || collection_index < 1L) {
    return(NA_character_)
  }
  collections <- .litxr_config_collections(cfg)
  if (collection_index > length(collections)) {
    return(NA_character_)
  }
  collection_id <- as.character(collections[[collection_index]]$collection_id %||% collections[[collection_index]]$journal_id %||% NA_character_)
  if (is.na(collection_id) || !nzchar(collection_id)) {
    return(NA_character_)
  }
  json_filename <- as.character(row$json_filename[[1L]])
  if (is.na(json_filename) || !nzchar(json_filename)) {
    return(NA_character_)
  }
  file.path(.litxr_collection_ref_dir(cfg, collection_id), json_filename)
}

.litxr_bibtex_scaffold_table_cached <- function(cfg, path, key_col, keys = character()) {
  keys <- unique(as.character(keys))
  keys <- keys[!is.na(keys) & nzchar(keys)]
  if (!length(keys)) {
    return(data.table::data.table())
  }
  if (identical(key_col, "arxiv_id")) {
    keys <- unique(vapply(keys, .litxr_bare_arxiv_id, character(1)))
  } else if (identical(key_col, "doi")) {
    keys <- unique(vapply(keys, .litxr_bare_doi, character(1)))
  } else if (identical(key_col, "isbn")) {
    keys <- sub("^isbn:", "", vapply(keys, .litxr_normalize_isbn_ref_id, character(1)), ignore.case = TRUE)
    keys <- unique(as.character(keys))
  }
  keys <- keys[!is.na(keys) & nzchar(keys)]
  if (!length(keys)) {
    return(data.table::data.table())
  }
  columns <- unique(c(key_col, "collection_index", "json_filename"))
  table <- tryCatch(fst::read_fst(path, columns = columns, as.data.table = TRUE), error = function(e) data.table::data.table())
  if (!nrow(table)) {
    return(table)
  }
  if (!(key_col %in% names(table)) || !("collection_index" %in% names(table)) || !("json_filename" %in% names(table))) {
    return(data.table::data.table())
  }
  if (identical(key_col, "arxiv_id")) {
    table$arxiv_id <- as.character(table$arxiv_id)
    table <- table[!is.na(table$arxiv_id) & nzchar(table$arxiv_id) & table$arxiv_id %in% keys, , drop = FALSE]
  } else if (identical(key_col, "doi")) {
    table$doi <- tolower(as.character(table$doi))
    table <- table[!is.na(table$doi) & nzchar(table$doi) & table$doi %in% keys, , drop = FALSE]
  } else if (identical(key_col, "isbn")) {
    table$isbn <- as.character(table$isbn)
    table <- table[!is.na(table$isbn) & nzchar(table$isbn) & table$isbn %in% keys, , drop = FALSE]
  }
  if (!nrow(table)) {
    return(data.table::data.table())
  }

  collections <- .litxr_config_collections(cfg)
  collection_index <- suppressWarnings(as.integer(table$collection_index))
  valid <- !is.na(collection_index) & collection_index >= 1L & collection_index <= length(collections)
  json_path <- rep(NA_character_, nrow(table))
  if (any(valid)) {
    collection_ids <- vapply(collections, function(col) {
      as.character(col$collection_id %||% col$journal_id %||% NA_character_)
    }, character(1))
    collection_dirs <- vapply(collection_ids, function(collection_id) {
      if (is.na(collection_id) || !nzchar(collection_id)) return(NA_character_)
      .litxr_collection_ref_dir(cfg, collection_id)
    }, character(1))

    json_filename <- as.character(table$json_filename)
    json_path[valid] <- file.path(
      collection_dirs[collection_index[valid]],
      json_filename[valid]
    )
  }
  table$json_path <- json_path
  if (nrow(table)) data.table::setkeyv(table, key_col)
  table
}

.litxr_bibtex_scaffold_cache <- function(cfg, arxiv_ids = character(), doi_ids = character(), isbn_ids = character()) {
  arxiv_ids <- unique(as.character(arxiv_ids))
  arxiv_ids <- arxiv_ids[!is.na(arxiv_ids) & nzchar(arxiv_ids)]
  doi_ids <- unique(tolower(as.character(doi_ids)))
  doi_ids <- doi_ids[!is.na(doi_ids) & nzchar(doi_ids)]
  isbn_ids <- unique(as.character(isbn_ids))
  isbn_ids <- isbn_ids[!is.na(isbn_ids) & nzchar(isbn_ids)]
  arxiv <- .litxr_bibtex_scaffold_table_cached(cfg, .litxr_ref_arxiv_path(cfg), "arxiv_id", arxiv_ids)
  doi <- .litxr_bibtex_scaffold_table_cached(cfg, .litxr_ref_doi_path(cfg), "doi", doi_ids)
  isbn <- .litxr_bibtex_scaffold_table_cached(cfg, .litxr_ref_isbn_path(cfg), "isbn", isbn_ids)
  list(arxiv = arxiv, doi = doi, isbn = isbn)
}

.litxr_bibtex_scaffold_key_value <- function(key_col, key) {
  key_col <- as.character(key_col)[[1L]]
  key <- as.character(key)[[1L]]
  if (is.na(key) || !nzchar(trimws(key))) {
    return(NA_character_)
  }
  if (identical(key_col, "arxiv_id")) {
    return(.litxr_bare_arxiv_id(ref_id = key))
  }
  if (identical(key_col, "doi")) {
    return(tolower(.litxr_bare_doi(ref_id = key)))
  }
  if (identical(key_col, "isbn")) {
    isbn_ref_id <- .litxr_normalize_isbn_ref_id(key)
    if (is.na(isbn_ref_id) || !nzchar(isbn_ref_id)) {
      return(NA_character_)
    }
    return(sub("^isbn:", "", isbn_ref_id, ignore.case = TRUE))
  }
  trimws(key)
}

.litxr_bibtex_row_from_json_path <- function(cfg, json_path, prefer_doi_key = TRUE) {
  if (is.na(json_path) || !nzchar(json_path) || !file.exists(json_path)) {
    return(NULL)
  }
  payload <- .litxr_storage_payload_as_list(json_path, fields = .litxr_bibtex_payload_fields())
  .litxr_bibtex_payload_to_row(payload, prefer_doi_key = prefer_doi_key)
}

.litxr_bibtex_row_from_scaffold_cache <- function(cfg, cache_table, key_col, key, prefer_doi_key = TRUE) {
  if (is.null(cache_table) || !nrow(cache_table) || !(key_col %in% names(cache_table))) {
    return(NULL)
  }
  cache_key <- .litxr_bibtex_scaffold_key_value(key_col, key)
  if (is.na(cache_key) || !nzchar(cache_key)) {
    return(NULL)
  }
  hit <- cache_table[as.character(cache_table[[key_col]]) == cache_key, , drop = FALSE]
  if (!nrow(hit)) {
    return(NULL)
  }
  json_path <- as.character(hit$json_path[[1L]])
  row <- .litxr_bibtex_row_from_json_path(cfg, json_path, prefer_doi_key = prefer_doi_key)
  if (!is.null(row) && nrow(row)) {
    return(row)
  }
  NULL
}

.litxr_bibtex_resolve_arxiv_row <- function(cfg, ref_id, scaffold_cache = NULL, link_maps = NULL, prefer_linked_doi = TRUE) {
  arxiv_id <- .litxr_normalize_arxiv_ref_id(ref_id)
  if (is.na(arxiv_id) || !nzchar(arxiv_id)) {
    return(list(row = NULL, resolved_ref_id = NA_character_, warning = NULL))
  }
  arxiv_id_bare <- .litxr_bare_arxiv_id(arxiv_id)

  if (isTRUE(prefer_linked_doi)) {
    paired_doi <- NA_character_
    if (!is.null(link_maps) && !is.null(link_maps$arxiv_to_doi) && length(link_maps$arxiv_to_doi)) {
      hit <- unname(link_maps$arxiv_to_doi[arxiv_id_bare])
      if (length(hit) && !is.na(hit[[1L]]) && nzchar(hit[[1L]])) {
        paired_doi <- as.character(hit[[1L]])
      }
    }

    if (!is.na(paired_doi) && nzchar(paired_doi)) {
      doi_row <- .litxr_bibtex_row_from_scaffold_cache(cfg, scaffold_cache$doi, "doi", paired_doi, prefer_doi_key = TRUE)
      if (!is.null(doi_row) && nrow(doi_row)) {
        doi_row$bib_key__ <- .make_arxiv_citekey(arxiv_id)
        doi_row$prefer_doi_key__ <- FALSE
        return(list(row = doi_row, resolved_ref_id = paired_doi, warning = NULL))
      }
      arxiv_row <- .litxr_bibtex_row_from_scaffold_cache(cfg, scaffold_cache$arxiv, "arxiv_id", arxiv_id, prefer_doi_key = FALSE)
      if (!is.null(arxiv_row) && nrow(arxiv_row)) {
        arxiv_row$bib_key__ <- .make_arxiv_citekey(arxiv_id)
      }
      return(list(
        row = arxiv_row,
        resolved_ref_id = arxiv_id,
        warning = sprintf("Paired DOI %s for %s was not found in ref_doi.fst; using the arXiv record.", paired_doi, arxiv_id)
      ))
    }
  }

  arxiv_row <- .litxr_bibtex_row_from_scaffold_cache(cfg, scaffold_cache$arxiv, "arxiv_id", arxiv_id, prefer_doi_key = FALSE)
  if (!is.null(arxiv_row) && nrow(arxiv_row)) {
    arxiv_row$bib_key__ <- .make_arxiv_citekey(arxiv_id)
  }
  list(
    row = arxiv_row,
    resolved_ref_id = arxiv_id,
    warning = NULL
  )
}

.litxr_bibtex_resolve_doi_row <- function(cfg, ref_id, scaffold_cache = NULL) {
  doi_id <- .litxr_normalize_doi_ref_id(ref_id)
  if (is.na(doi_id) || !nzchar(doi_id)) {
    return(list(row = NULL, resolved_ref_id = NA_character_, warning = NULL))
  }
  if (is.null(scaffold_cache)) {
    scaffold_cache <- .litxr_bibtex_scaffold_cache(cfg, doi_ids = .litxr_bare_doi(doi_id))
  }
  list(
    row = .litxr_bibtex_row_from_scaffold_cache(cfg, scaffold_cache$doi, "doi", doi_id, prefer_doi_key = TRUE),
    resolved_ref_id = doi_id,
    warning = NULL
  )
}

.litxr_bibtex_resolve_isbn_row <- function(cfg, ref_id, scaffold_cache = NULL) {
  isbn_id <- .litxr_normalize_isbn_ref_id(ref_id)
  if (is.na(isbn_id) || !nzchar(isbn_id)) {
    return(list(row = NULL, resolved_ref_id = NA_character_, warning = NULL))
  }
  if (is.null(scaffold_cache)) {
    scaffold_cache <- .litxr_bibtex_scaffold_cache(
      cfg,
      isbn_ids = sub("^isbn:", "", isbn_id, ignore.case = TRUE)
    )
  }
  row <- .litxr_bibtex_row_from_scaffold_cache(cfg, scaffold_cache$isbn, "isbn", isbn_id, prefer_doi_key = TRUE)
  list(row = row, resolved_ref_id = isbn_id, warning = NULL)
}

.litxr_bibtex_resolve_row <- function(cfg, ref_id, scaffold_cache = NULL, link_maps = NULL, prefer_linked_doi = TRUE) {
  route <- .litxr_route_ref_id(ref_id)
  key_type <- route$key_type
  key_value <- route$key_value

  if (identical(key_type, "doi")) {
    return(.litxr_bibtex_resolve_doi_row(cfg, key_value, scaffold_cache = scaffold_cache))
  }
  if (identical(key_type, "arxiv_id")) {
    return(.litxr_bibtex_resolve_arxiv_row(cfg, key_value, scaffold_cache = scaffold_cache, link_maps = link_maps, prefer_linked_doi = prefer_linked_doi))
  }
  if (identical(key_type, "isbn")) {
    isbn_row <- .litxr_bibtex_resolve_isbn_row(cfg, key_value, scaffold_cache = scaffold_cache)
    if (!is.null(isbn_row$row) && nrow(isbn_row$row)) {
      return(isbn_row)
    }
  }

  list(row = NULL, resolved_ref_id = NA_character_, warning = NULL)
}

.litxr_bibtex_rows_for_write <- function(cfg, ref_ids, scaffold_cache = NULL, link_maps = NULL, prefer_linked_doi = TRUE) {
  ref_ids <- as.character(ref_ids)
  ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(trimws(ref_ids))]
  if (!length(ref_ids)) {
    return(list(
      rows = data.table::data.table(),
      resolved_ref_ids = character(),
      unresolved_ref_ids = character(),
      warnings = character()
    ))
  }

  resolved <- vector("list", length(ref_ids))
  resolved_ref_ids <- rep(NA_character_, length(ref_ids))
  unresolved <- rep(NA_character_, length(ref_ids))
  warnings <- vector("list", length(ref_ids))
  resolved_n <- 0L
  unresolved_n <- 0L
  warning_n <- 0L
  for (ref_id in ref_ids) {
    result <- .litxr_bibtex_resolve_row(
      cfg,
      ref_id,
      scaffold_cache = scaffold_cache,
      link_maps = link_maps,
      prefer_linked_doi = prefer_linked_doi
    )
    if (!is.null(result$warning) && nzchar(result$warning)) {
      warning_n <- warning_n + 1L
      warnings[[warning_n]] <- result$warning
    }
    if (is.null(result$row) || !nrow(result$row)) {
      unresolved_n <- unresolved_n + 1L
      unresolved[[unresolved_n]] <- ref_id
      next
    }
    resolved_n <- resolved_n + 1L
    resolved[[resolved_n]] <- result$row[1L, ]
    resolved_ref_ids[[resolved_n]] <- as.character(result$resolved_ref_id)
  }

  if (unresolved_n > 0L) {
    warning_n <- warning_n + 1L
    warnings[[warning_n]] <-
      paste(
        "The following ref_id(s) could not be resolved for BibTeX export:",
        paste(unresolved[seq_len(unresolved_n)], collapse = ", ")
      )
  }

  rows <- if (resolved_n > 0L) data.table::rbindlist(resolved[seq_len(resolved_n)], fill = TRUE) else data.table::data.table()
  list(
    rows = rows,
    resolved_ref_ids = if (resolved_n > 0L) resolved_ref_ids[seq_len(resolved_n)] else character(),
    unresolved_ref_ids = if (unresolved_n > 0L) unresolved[seq_len(unresolved_n)] else character(),
    warnings = if (warning_n > 0L) unlist(warnings[seq_len(warning_n)], use.names = FALSE) else character()
  )
}

.litxr_row_to_bibtex <- function(row) {
  if (data.table::is.data.table(row)) {
    row <- stats::setNames(lapply(names(row), function(name) row[[name]]), names(row))
  }
  scalar <- .litxr_scalar_chr

  entry_type <- scalar(row[["entry_type"]])
  if (is.null(entry_type) || is.na(entry_type) || !nzchar(entry_type)) {
    entry_type <- .litxr_entry_type_from_source(row[["source"]])
  }

  key_override <- scalar(row[["bib_key__"]])
  prefer_doi_key <- isTRUE(row[["prefer_doi_key__"]])
  key <- if (!is.null(key_override) && !is.na(key_override) && nzchar(key_override)) {
    key_override
  } else if (prefer_doi_key) {
    .make_citekey(scalar(row[["doi"]]), scalar(row[["source_id"]]), scalar(row[["ref_id"]]))
  } else {
    .make_citekey(NA_character_, scalar(row[["source_id"]]), scalar(row[["ref_id"]]))
  }
  title <- .bibtex_escape(scalar(row[["title"]]))
  authors_list <- row[["authors_list"]]
  if (is.null(authors_list) || !length(authors_list) || !length(authors_list[[1]])) {
    authors_chr <- scalar(row[["authors"]])
    if (!is.null(authors_chr) && !is.na(authors_chr) && nzchar(authors_chr)) {
      authors_list <- list(trimws(strsplit(as.character(authors_chr), ";", fixed = TRUE)[[1]]))
    } else {
      authors_list <- list(character())
    }
  }
  auth <- .bibtex_escape(.format_authors_bib(authors_list[[1]]))
  year <- as.character(scalar(row[["year"]]))

  journal <- scalar(row[["journal"]])
  container <- scalar(row[["container_title"]])
  publisher <- scalar(row[["publisher"]])
  volume <- scalar(row[["volume"]])
  number <- scalar(row[["issue"]])
  pages <- scalar(row[["pages"]])
  doi <- scalar(row[["doi"]])
  isbn <- scalar(row[["isbn"]])
  issn <- scalar(row[["issn"]])
  note <- scalar(row[["note"]])

  url <- scalar(row[["url"]])
  if (is.null(url) || is.na(url) || !nzchar(url)) url <- scalar(row[["url_landing"]])
  if (is.na(url) || !nzchar(url)) url <- scalar(row[["url_pdf"]])
  if (is.na(journal) || !nzchar(journal)) journal <- container

  fields <- c(
    title = title,
    author = auth,
    year = year,
    volume = volume,
    number = number,
    pages = pages,
    doi = doi,
    isbn = isbn,
    issn = issn,
    url = url,
    note = note
  )

  if (entry_type == "article") fields["journal"] <- journal
  if (entry_type %in% c("inproceedings", "incollection")) fields["booktitle"] <- container
  if (entry_type == "book") fields["publisher"] <- publisher
  if (entry_type %in% c("inproceedings", "incollection")) fields["publisher"] <- publisher
  if (entry_type == "techreport") fields["institution"] <- publisher
  if (entry_type == "phdthesis") fields["school"] <- publisher

  fields <- fields[!is.na(fields) & nzchar(fields)]

  lines <- sprintf("  %s = {%s},", names(fields), fields)
  title_idx <- match("title", names(fields))
  if (!is.na(title_idx)) {
    lines[[title_idx]] <- sprintf("@%s{%s, title = {%s},", entry_type, key, fields[["title"]])
  } else {
    lines <- c(sprintf("@%s{%s,", entry_type, key), lines)
  }

  lines[length(lines)] <- sub(",$", "", lines[length(lines)])
  c(lines, "}")
}

.litxr_format_bib_file_lines <- function(entries) {
  if (!length(entries)) {
    return(character())
  }
  unlist(lapply(entries, function(entry) c(entry, "")), use.names = FALSE)
}

#' Write BibTeX entries for canonical reference ids
#'
#' @param path Output `.bib` file path.
#' @param ref_ids Character vector of canonical reference ids.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param prefer_linked_doi If `TRUE` (default), arXiv ref_ids are exported
#'   through a linked DOI row when one is present in the identity map and DOI
#'   payload store. If `FALSE`, arXiv ids are exported as arXiv rows without
#'   DOI promotion.
#'
#' @return Named list describing the write status.
#' @export
write_bibtex_entries <- function(path, ref_ids, config = NULL, prefer_linked_doi = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  candidates <- .litxr_bibtex_candidate_sets(ref_ids)
  link_maps <- if (isTRUE(prefer_linked_doi)) .litxr_bibtex_link_maps(cfg, arxiv_ids = candidates$arxiv_ids, doi_ids = candidates$doi_ids) else NULL
  linked_doi_ids <- if (isTRUE(prefer_linked_doi) && length(candidates$arxiv_ids) && !is.null(link_maps$arxiv_to_doi) && length(link_maps$arxiv_to_doi)) {
    hits <- unname(link_maps$arxiv_to_doi[candidates$arxiv_ids])
    hits <- as.character(hits[!is.na(hits) & nzchar(hits)])
    unique(tolower(hits))
  } else {
    character()
  }
  scaffold_cache <- .litxr_bibtex_scaffold_cache(
    cfg,
    arxiv_ids = candidates$arxiv_ids,
    doi_ids = unique(c(candidates$doi_ids, linked_doi_ids)),
    isbn_ids = candidates$isbn_ids
  )

  resolved <- .litxr_bibtex_rows_for_write(
    cfg,
    ref_ids,
    scaffold_cache = scaffold_cache,
    link_maps = link_maps,
    prefer_linked_doi = prefer_linked_doi
  )
  rows <- resolved$rows
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)

  if (!nrow(rows)) {
    writeLines(character(), path)
    return(invisible(list(
      status = "ok",
      output = normalizePath(path, winslash = "/", mustWork = FALSE),
      written_ref_ids = character(),
      resolved_ref_ids = character(),
      unresolved_ref_ids = as.character(ref_ids),
      warnings = character()
    )))
  }

  bib_entries <- vapply(seq_len(nrow(rows)), function(i) {
    paste(.litxr_row_to_bibtex(rows[i, ]), collapse = "\n")
  }, character(1))
  writeLines(.litxr_format_bib_file_lines(bib_entries), path)

  invisible(list(
    status = "ok",
    output = normalizePath(path, winslash = "/", mustWork = FALSE),
    written_ref_ids = as.character(ref_ids),
    resolved_ref_ids = resolved$resolved_ref_ids,
    unresolved_ref_ids = resolved$unresolved_ref_ids,
    warnings = resolved$warnings
  ))
}

.litxr_replace_bib_with_linked_doi_file <- function(input_bibtex, output_bibtex = input_bibtex, config = NULL) {
  if (!file.exists(input_bibtex)) {
    stop("BibTeX file not found: ", input_bibtex, call. = FALSE)
  }
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  parsed <- .litxr_bibtex_parse_entries(input_bibtex)
  n_entries <- length(parsed$entries)
  if (!n_entries) {
    writeLines(character(), output_bibtex)
    return(list(
      status = "ok",
      converted_arxiv_ids = character(),
      doi_of_converted_arxiv_ids = character(),
      missing_linked_doi_arxiv_ids = character(),
      doi_of_missing_linked_doi_arxiv_ids = character(),
      suspicious_arxiv_ids = character()
    ))
  }
  raw_input_ids <- .litxr_bibtex_entry_ref_ids(parsed, link_maps = NULL, prefer_linked_arxiv = FALSE)
  raw_candidates <- .litxr_bibtex_candidate_sets(raw_input_ids)
  link_maps <- .litxr_bibtex_link_maps(cfg, arxiv_ids = raw_candidates$arxiv_ids, doi_ids = raw_candidates$doi_ids)
  input_ids <- .litxr_bibtex_entry_ref_ids(parsed, link_maps = link_maps, prefer_linked_arxiv = TRUE)
  final_candidates <- .litxr_bibtex_candidate_sets(input_ids)
  linked_doi_ids <- if (length(final_candidates$arxiv_ids) && !is.null(link_maps$arxiv_to_doi) && length(link_maps$arxiv_to_doi)) {
    hits <- unname(link_maps$arxiv_to_doi[final_candidates$arxiv_ids])
    hits <- as.character(hits[!is.na(hits) & nzchar(hits)])
    unique(tolower(hits))
  } else {
    character()
  }
  scaffold_cache <- .litxr_bibtex_scaffold_cache(
    cfg,
    arxiv_ids = final_candidates$arxiv_ids,
    doi_ids = unique(c(final_candidates$doi_ids, linked_doi_ids)),
    isbn_ids = final_candidates$isbn_ids
  )

  result_entries <- parsed$entries
  unresolved_ref_ids <- rep(NA_character_, n_entries)
  converted_arxiv_ids <- rep(NA_character_, n_entries)
  doi_of_converted_arxiv_ids <- rep(NA_character_, n_entries)
  missing_linked_doi_arxiv_ids <- rep(NA_character_, n_entries)
  doi_of_missing_linked_doi_arxiv_ids <- rep(NA_character_, n_entries)
  suspicious_arxiv_ids <- rep(NA_character_, n_entries)

  identity_arxiv_ids <- if (!is.null(link_maps$arxiv_to_doi) && length(link_maps$arxiv_to_doi)) {
    as.character(names(link_maps$arxiv_to_doi))
  } else {
    character()
  }
  available_doi_ids <- if (!is.null(scaffold_cache$doi) && nrow(scaffold_cache$doi) && "doi" %in% names(scaffold_cache$doi)) {
    as.character(scaffold_cache$doi$doi)
  } else {
    character()
  }

  for (i in seq_len(n_entries)) {
    input_id <- input_ids[[i]]
    input_type <- if (length(parsed$types) >= i) as.character(parsed$types[[i]]) else NA_character_
    if (is.na(input_id) || !nzchar(input_id)) {
      next
    }

    result <- .litxr_bibtex_resolve_row(
      cfg,
      input_id,
      scaffold_cache = scaffold_cache,
      link_maps = link_maps,
      prefer_linked_doi = TRUE
    )
    if (is.null(result$row) || !nrow(result$row)) {
      unresolved_ref_ids[[i]] <- input_id
    } else {
      row <- result$row[1L, ]
      row$bib_key__ <- parsed$keys[[i]]
      result_entries[[i]] <- paste(.litxr_row_to_bibtex(row), collapse = "\n")
    }

    is_arxiv_input <- grepl("^arxiv:", input_id, ignore.case = TRUE)
    if (!is_arxiv_input) {
      next
    }
    input_arxiv_bare <- .litxr_bare_arxiv_id(input_id)
    linked_doi <- if (!is.null(link_maps$arxiv_to_doi) && length(link_maps$arxiv_to_doi)) {
      unname(link_maps$arxiv_to_doi[input_arxiv_bare])
    } else {
      NA_character_
    }
    linked_doi <- if (length(linked_doi) && !is.na(linked_doi[[1L]]) && nzchar(linked_doi[[1L]])) as.character(linked_doi[[1L]]) else NA_character_
    linked_doi_bare <- if (!is.na(linked_doi) && nzchar(linked_doi)) .litxr_bare_doi(doi = linked_doi) else NA_character_
    linked_doi_present <- !is.na(linked_doi_bare) && nzchar(linked_doi_bare) && linked_doi_bare %in% available_doi_ids
    linked_doi_known <- !is.na(linked_doi) && nzchar(linked_doi)
    is_article_arxiv <- !is.na(input_type) && identical(input_type, "article")

    if (linked_doi_present) {
      converted_arxiv_ids[[i]] <- input_arxiv_bare
      doi_of_converted_arxiv_ids[[i]] <- linked_doi_bare
    } else if (linked_doi_known) {
      missing_linked_doi_arxiv_ids[[i]] <- input_arxiv_bare
      doi_of_missing_linked_doi_arxiv_ids[[i]] <- linked_doi_bare
    } else if (is_article_arxiv && !(input_arxiv_bare %in% identity_arxiv_ids)) {
      suspicious_arxiv_ids[[i]] <- input_arxiv_bare
    }
  }

  writeLines(.litxr_format_bib_file_lines(result_entries), output_bibtex)

  converted_keep <- !is.na(converted_arxiv_ids) & nzchar(converted_arxiv_ids)
  missing_keep <- !is.na(missing_linked_doi_arxiv_ids) & nzchar(missing_linked_doi_arxiv_ids)
  suspicious_keep <- !is.na(suspicious_arxiv_ids) & nzchar(suspicious_arxiv_ids)
  unresolved_keep <- !is.na(unresolved_ref_ids) & nzchar(unresolved_ref_ids)

  list(
    status = "ok",
    input_bibtex = normalizePath(input_bibtex, winslash = "/", mustWork = FALSE),
    output_bibtex = normalizePath(output_bibtex, winslash = "/", mustWork = FALSE),
    converted_arxiv_ids = unique(converted_arxiv_ids[converted_keep]),
    doi_of_converted_arxiv_ids = doi_of_converted_arxiv_ids[converted_keep][!duplicated(converted_arxiv_ids[converted_keep])],
    missing_linked_doi_arxiv_ids = unique(missing_linked_doi_arxiv_ids[missing_keep]),
    doi_of_missing_linked_doi_arxiv_ids = doi_of_missing_linked_doi_arxiv_ids[missing_keep][!duplicated(missing_linked_doi_arxiv_ids[missing_keep])],
    suspicious_arxiv_ids = unique(suspicious_arxiv_ids[suspicious_keep]),
    unresolved_ref_ids = unique(unresolved_ref_ids[unresolved_keep])
  )
}
