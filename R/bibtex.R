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
  link_maps <- if (isTRUE(prefer_linked_arxiv)) .litxr_bibtex_link_maps(cfg) else NULL

  parsed <- .litxr_bibtex_parse_entries(path)
  if (!length(parsed$entries)) {
    return(character())
  }

  ref_ids <- character()
  for (i in seq_along(parsed$entries)) {
    ref_id <- .litxr_bibtex_entry_ref_id(
      parsed$entries[[i]],
      parsed$keys[[i]],
      link_maps = link_maps,
      prefer_linked_arxiv = prefer_linked_arxiv
    )
    if (!is.na(ref_id) && nzchar(ref_id)) {
      ref_ids <- c(ref_ids, ref_id)
    }
  }

  ref_ids
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

.format_authors_bib <- function(authors_list) {
  if (length(authors_list) == 0L || all(is.na(authors_list))) return("")
  paste(authors_list, collapse = " and ")
}

.litxr_bibtex_parse_entries <- function(path) {
  lines <- readLines(path, warn = FALSE)
  if (!length(lines)) {
    return(list(entries = character(), keys = character()))
  }

  entries <- character()
  keys <- character()
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

    chunk <- character()
    depth <- 0L
    repeat {
      if (i > length(lines)) {
        stop("Unterminated BibTeX entry for key: ", key, call. = FALSE)
      }
      current <- lines[[i]]
      chunk <- c(chunk, current)
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

    entries <- c(entries, paste(chunk, collapse = "\n"))
    keys <- c(keys, key)
  }

  list(entries = entries, keys = keys)
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

.litxr_bibtex_link_maps <- function(cfg) {
  identity_map <- tryCatch(data.table::as.data.table(litxr_read_ref_identity_map(cfg)), error = function(e) data.table::data.table())
  if (!nrow(identity_map) || !all(c("arxiv_id", "doi") %in% names(identity_map))) {
    return(list(doi_to_arxiv = character(), arxiv_to_doi = character()))
  }

  doi_ref_id <- vapply(identity_map$doi, .litxr_normalize_doi_ref_id, character(1))
  arxiv_ref_id <- vapply(identity_map$arxiv_id, .litxr_normalize_arxiv_ref_id, character(1))
  keep <- !is.na(doi_ref_id) & nzchar(doi_ref_id) & !is.na(arxiv_ref_id) & nzchar(arxiv_ref_id)
  if (!any(keep)) {
    return(list(doi_to_arxiv = character(), arxiv_to_doi = character()))
  }

  doi_to_arxiv <- arxiv_ref_id[keep]
  names(doi_to_arxiv) <- doi_ref_id[keep]
  doi_to_arxiv <- doi_to_arxiv[!duplicated(names(doi_to_arxiv))]

  arxiv_to_doi <- doi_ref_id[keep]
  names(arxiv_to_doi) <- arxiv_ref_id[keep]
  arxiv_to_doi <- arxiv_to_doi[!duplicated(names(arxiv_to_doi))]

  list(doi_to_arxiv = doi_to_arxiv, arxiv_to_doi = arxiv_to_doi)
}

.litxr_bibtex_linked_arxiv_ref_id <- function(link_maps, doi_ref_id) {
  doi_ref_id <- .litxr_normalize_doi_ref_id(doi_ref_id)
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
  arxiv_id <- as.character(hit[[1L]])
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

.litxr_bibtex_scaffold_table_cached <- function(cfg, path) {
  table <- .litxr_read_scaffold_table_safe(path)
  if (!nrow(table)) {
    return(table)
  }
  if (!("collection_index" %in% names(table)) || !("json_filename" %in% names(table))) {
    table$json_path <- NA_character_
    return(table)
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
  table
}

.litxr_bibtex_scaffold_cache <- function(cfg) {
  arxiv <- .litxr_bibtex_scaffold_table_cached(cfg, .litxr_ref_arxiv_path(cfg))
  doi <- .litxr_bibtex_scaffold_table_cached(cfg, .litxr_ref_doi_path(cfg))
  isbn <- .litxr_bibtex_scaffold_table_cached(cfg, .litxr_ref_isbn_path(cfg))
  if (nrow(arxiv) && "arxiv_id" %in% names(arxiv)) {
    arxiv <- arxiv[!is.na(arxiv$arxiv_id) & nzchar(as.character(arxiv$arxiv_id)), ]
    if (nrow(arxiv)) data.table::setkey(arxiv, arxiv_id)
  }
  if (nrow(doi) && "doi" %in% names(doi)) {
    doi <- doi[!is.na(doi$doi) & nzchar(as.character(doi$doi)), ]
    if (nrow(doi)) data.table::setkey(doi, doi)
  }
  if (nrow(isbn) && "isbn" %in% names(isbn)) {
    isbn <- isbn[!is.na(isbn$isbn) & nzchar(as.character(isbn$isbn)), ]
    if (nrow(isbn)) data.table::setkey(isbn, isbn)
  }
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
  payload <- .litxr_storage_payload_as_list(json_path)
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
  if (!data.table::haskey(cache_table) || !identical(data.table::key(cache_table), key_col)) {
    data.table::setkeyv(cache_table, key_col)
  }
  if (identical(key_col, "doi")) {
    hit <- cache_table[tolower(as.character(cache_table[[key_col]])) == cache_key, ]
  } else {
    hit <- cache_table[as.character(cache_table[[key_col]]) == cache_key, ]
  }
  if (!nrow(hit)) {
    return(NULL)
  }
  json_path <- as.character(hit$json_path[[1L]])
  row <- .litxr_bibtex_row_from_json_path(cfg, json_path, prefer_doi_key = prefer_doi_key)
  if (!is.null(row) && nrow(row)) {
    return(row)
  }

  fallback <- tryCatch(litxr::litxr_read_references(cfg), error = function(e) data.table::data.table())
  if (!nrow(fallback)) {
    return(NULL)
  }
  normalized_key <- cache_key
  if (key_col %in% names(fallback)) {
    if (identical(key_col, "doi")) {
      fallback_key <- tolower(sub("^doi:", "", vapply(fallback[[key_col]], .litxr_normalize_doi_ref_id, character(1)), ignore.case = TRUE))
    } else if (identical(key_col, "arxiv_id")) {
      fallback_key <- sub("^arxiv:", "", vapply(fallback[[key_col]], .litxr_normalize_arxiv_ref_id, character(1)), ignore.case = TRUE)
    } else if (identical(key_col, "isbn")) {
      fallback_key <- sub("^isbn:", "", vapply(fallback[[key_col]], .litxr_normalize_isbn_ref_id, character(1)), ignore.case = TRUE)
    } else {
      fallback_key <- as.character(fallback[[key_col]])
    }
    keep <- !is.na(fallback_key) & nzchar(fallback_key) & fallback_key == normalized_key
    fallback <- fallback[keep, ]
  } else if ("ref_id" %in% names(fallback)) {
    fallback_ref_id <- as.character(fallback$ref_id)
    fallback_ref_id_norm <- if (identical(key_col, "doi")) {
      tolower(sub("^doi:", "", vapply(fallback_ref_id, .litxr_normalize_doi_ref_id, character(1)), ignore.case = TRUE))
    } else if (identical(key_col, "arxiv_id")) {
      sub("^arxiv:", "", vapply(fallback_ref_id, .litxr_normalize_arxiv_ref_id, character(1)), ignore.case = TRUE)
    } else if (identical(key_col, "isbn")) {
      sub("^isbn:", "", vapply(fallback_ref_id, .litxr_normalize_isbn_ref_id, character(1)), ignore.case = TRUE)
    } else {
      fallback_ref_id
    }
    keep <- !is.na(fallback_ref_id_norm) & nzchar(fallback_ref_id_norm) & fallback_ref_id_norm == normalized_key
    fallback <- fallback[keep, ]
  }
  if (!nrow(fallback)) {
    return(NULL)
  }
  fallback <- fallback[1L, ]
  fallback$prefer_doi_key__ <- prefer_doi_key
  fallback
}

.litxr_bibtex_resolve_arxiv_row <- function(cfg, ref_id, scaffold_cache = NULL, link_maps = NULL, prefer_linked_doi = TRUE) {
  arxiv_id <- .litxr_normalize_arxiv_ref_id(ref_id)
  if (is.na(arxiv_id) || !nzchar(arxiv_id)) {
    return(list(row = NULL, resolved_ref_id = NA_character_, warning = NULL))
  }

  if (isTRUE(prefer_linked_doi)) {
    paired_doi <- NA_character_
    if (!is.null(link_maps) && !is.null(link_maps$arxiv_to_doi) && length(link_maps$arxiv_to_doi)) {
      hit <- unname(link_maps$arxiv_to_doi[arxiv_id])
      if (length(hit) && !is.na(hit[[1L]]) && nzchar(hit[[1L]])) {
        paired_doi <- as.character(hit[[1L]])
      }
    }

    if (!is.na(paired_doi) && nzchar(paired_doi)) {
      doi_row <- .litxr_bibtex_row_from_scaffold_cache(cfg, scaffold_cache$doi, "doi", paired_doi, prefer_doi_key = TRUE)
      if (!is.null(doi_row) && nrow(doi_row)) {
        return(list(row = doi_row, resolved_ref_id = paired_doi, warning = NULL))
      }
      return(list(
        row = .litxr_bibtex_row_from_scaffold_cache(cfg, scaffold_cache$arxiv, "arxiv_id", arxiv_id, prefer_doi_key = FALSE),
        resolved_ref_id = arxiv_id,
        warning = sprintf("Paired DOI %s for %s was not found in ref_doi.fst; using the arXiv record.", paired_doi, arxiv_id)
      ))
    }
  }

  list(
    row = .litxr_bibtex_row_from_scaffold_cache(cfg, scaffold_cache$arxiv, "arxiv_id", arxiv_id, prefer_doi_key = FALSE),
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
    scaffold_cache <- .litxr_bibtex_scaffold_cache(cfg)
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
    scaffold_cache <- .litxr_bibtex_scaffold_cache(cfg)
  }
  row <- .litxr_bibtex_row_from_scaffold_cache(cfg, scaffold_cache$isbn, "isbn", isbn_id, prefer_doi_key = TRUE)
  if (is.null(row) || !nrow(row)) {
    fallback <- tryCatch(data.table::as.data.table(litxr::litxr_read_references(cfg)), error = function(e) data.table::data.table())
    if (nrow(fallback)) {
      if ("isbn" %in% names(fallback)) {
        fallback <- fallback[as.character(fallback$isbn) == sub("^isbn:", "", isbn_id), ]
      } else if ("ref_id" %in% names(fallback)) {
        fallback <- fallback[as.character(fallback$ref_id) == isbn_id, ]
      }
      if (nrow(fallback)) {
        fallback <- fallback[1L, ]
        fallback$prefer_doi_key__ <- TRUE
        row <- fallback
      }
    }
  }
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

  resolved <- list()
  warnings <- character()
  resolved_ref_ids <- character()
  unresolved <- character()
  for (ref_id in ref_ids) {
    result <- .litxr_bibtex_resolve_row(
      cfg,
      ref_id,
      scaffold_cache = scaffold_cache,
      link_maps = link_maps,
      prefer_linked_doi = prefer_linked_doi
    )
    if (!is.null(result$warning) && nzchar(result$warning)) {
      warnings <- c(warnings, result$warning)
    }
    if (is.null(result$row) || !nrow(result$row)) {
      unresolved <- c(unresolved, ref_id)
      next
    }
    resolved[[length(resolved) + 1L]] <- result$row[1L, ]
    resolved_ref_ids <- c(resolved_ref_ids, as.character(result$resolved_ref_id))
  }

  if (length(unresolved)) {
    warnings <- c(
      warnings,
      paste(
        "The following ref_id(s) could not be resolved for BibTeX export:",
        paste(unresolved, collapse = ", ")
      )
    )
  }

  rows <- if (length(resolved)) data.table::rbindlist(resolved, fill = TRUE) else data.table::data.table()
  list(
    rows = rows,
    resolved_ref_ids = resolved_ref_ids,
    unresolved_ref_ids = unresolved,
    warnings = warnings
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

  prefer_doi_key <- isTRUE(row[["prefer_doi_key__"]])
  key <- if (prefer_doi_key) {
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
  scaffold_cache <- .litxr_bibtex_scaffold_cache(cfg)
  link_maps <- if (isTRUE(prefer_linked_doi)) .litxr_bibtex_link_maps(cfg) else NULL

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
