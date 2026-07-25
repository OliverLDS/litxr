.litxr_empty_arxiv_lexical_scores <- function() {
  data.table::data.table(
    ref_id = character(),
    category_id = character(),
    score_max = integer()
  )
}

.litxr_read_lexical_query_set <- function(path, query_set_id = NULL) {
  path <- normalizePath(path, winslash = "/", mustWork = FALSE)
  if (!file.exists(path)) {
    stop("Query set file not found: ", path, call. = FALSE)
  }
  ext <- tolower(tools::file_ext(path))
  if (identical(ext, "csv")) {
    rows <- data.table::fread(path, sep = ",", header = TRUE, na.strings = c("", "NA"))
    required <- c("category_id", "lexical_keywords")
    if (!all(required %in% names(rows))) {
      stop("CSV lexical query set requires columns: category_id, lexical_keywords", call. = FALSE)
    }
    rows <- rows[
      !is.na(rows$category_id) & nzchar(trimws(rows$category_id)) &
        !is.na(rows$lexical_keywords) & nzchar(trimws(rows$lexical_keywords)),
      ,
      drop = FALSE
    ]
    if (!nrow(rows)) {
      stop("Lexical query set CSV has no usable rows: ", path, call. = FALSE)
    }
    rows$category_id <- as.character(rows$category_id)
    rows$lexical_keywords <- as.character(rows$lexical_keywords)
    grouped <- split(rows$lexical_keywords, rows$category_id)
    categories <- lapply(grouped, function(values) {
      keywords <- trimws(unlist(strsplit(values, "|", fixed = TRUE), use.names = FALSE))
      unique(keywords[!is.na(keywords) & nzchar(keywords)])
    })
    categories <- categories[lengths(categories) > 0L]
    if (!length(categories)) {
      stop("Lexical query set CSV produced no usable categories: ", path, call. = FALSE)
    }
    if (is.null(query_set_id) || !nzchar(as.character(query_set_id))) {
      query_set_id <- tools::file_path_sans_ext(basename(path))
    }
    query_set_id <- as.character(query_set_id)[[1L]]
    return(list(query_set_id = query_set_id, query_sets = stats::setNames(list(categories), query_set_id)))
  }
  object <- if (identical(ext, "json")) {
    jsonlite::fromJSON(path, simplifyVector = FALSE)
  } else if (ext %in% c("yml", "yaml")) {
    yaml::read_yaml(path)
  } else {
    stop("Unsupported lexical query set file extension: ", ext, call. = FALSE)
  }
  if (!is.list(object) || !length(object)) {
    stop("Lexical query set file is empty: ", path, call. = FALSE)
  }
  if (is.null(names(object)) || any(!nzchar(names(object)))) {
    stop("Lexical query set file must be a named list.", call. = FALSE)
  }
  if (length(object) == 1L && is.list(object[[1L]]) && !is.null(names(object[[1L]]))) {
    query_set_id <- names(object)[[1L]]
    query_sets <- object
  } else {
    if (is.null(query_set_id) || !nzchar(as.character(query_set_id))) {
      query_set_id <- tools::file_path_sans_ext(basename(path))
    }
    query_set_id <- as.character(query_set_id)[[1L]]
    query_sets <- stats::setNames(list(object), query_set_id)
  }
  list(query_set_id = as.character(query_set_id)[[1L]], query_sets = query_sets)
}

.litxr_score_arxiv_lexical_category_rows <- function(query_set_id, query_sets, metadata_path, postings_path, min_keywords_per_category) {
  metadata <- fst::read_fst(
    metadata_path,
    as.data.table = TRUE,
    columns = c("doc_int", "arxiv_id")
  )
  data.table::setDT(metadata)
  metadata <- metadata[
    !is.na(metadata$doc_int) & !is.na(metadata$arxiv_id) & nzchar(metadata$arxiv_id),
    ,
    drop = FALSE
  ]
  if (!nrow(metadata)) return(.litxr_empty_arxiv_lexical_scores())

  postings <- fst::read_fst(
    postings_path,
    as.data.table = TRUE,
    columns = c("doc_int", "term")
  )
  data.table::setDT(postings)
  postings <- postings[
    !is.na(postings$doc_int) & !is.na(postings$term) & nzchar(postings$term),
    ,
    drop = FALSE
  ]
  if (!nrow(postings)) return(.litxr_empty_arxiv_lexical_scores())

  flat <- litxr_lexical_flatten_query_sets(query_sets)
  flat <- flat[flat$query_set == query_set_id, ]
  if (!nrow(flat)) return(.litxr_empty_arxiv_lexical_scores())
  flat$keyword_norm <- litxr_lexical_normalize_text(flat$keyword)
  flat <- flat[!is.na(flat$keyword_norm) & nzchar(flat$keyword_norm), ]
  flat <- unique(flat, by = c("query_set", "category", "keyword_norm"))
  if (!nrow(flat)) return(.litxr_empty_arxiv_lexical_scores())

  keyword_tokens <- litxr_lexical_tokenize(flat$keyword_norm, normalize = FALSE)
  token_counts <- lengths(keyword_tokens)
  keep <- token_counts > 0L
  if (!any(keep)) return(.litxr_empty_arxiv_lexical_scores())
  flat <- flat[keep, ]
  keyword_tokens <- keyword_tokens[keep]
  token_counts <- token_counts[keep]

  keyword_terms <- data.table::data.table(
    query_set = flat$query_set[rep.int(seq_len(nrow(flat)), token_counts)],
    category = flat$category[rep.int(seq_len(nrow(flat)), token_counts)],
    keyword_norm = flat$keyword_norm[rep.int(seq_len(nrow(flat)), token_counts)],
    term = unlist(keyword_tokens, use.names = FALSE)
  )
  data.table::setDT(keyword_terms)
  keyword_terms <- unique(keyword_terms)
  keyword_terms[, n_terms := data.table::uniqueN(term), by = .(query_set, category, keyword_norm)]
  postings <- postings[postings$term %in% unique(keyword_terms$term), ]
  if (!nrow(postings)) return(.litxr_empty_arxiv_lexical_scores())

  matches <- postings[keyword_terms, on = "term", nomatch = 0L, allow.cartesian = TRUE]
  matches <- unique(matches[, .(doc_int, category, keyword_norm, term, n_terms)])
  if (!nrow(matches)) return(.litxr_empty_arxiv_lexical_scores())
  phrase_hits <- matches[, .(n_terms_matched = .N), by = .(doc_int, category, keyword_norm, n_terms)]
  phrase_hits <- phrase_hits[n_terms_matched >= n_terms]
  if (!nrow(phrase_hits)) return(.litxr_empty_arxiv_lexical_scores())

  scores <- phrase_hits[, .(score_max = .N), by = .(doc_int, category)]
  scores <- scores[score_max >= min_keywords_per_category]
  if (!nrow(scores)) return(.litxr_empty_arxiv_lexical_scores())
  scores$ref_id <- as.character(metadata$arxiv_id[match(scores$doc_int, metadata$doc_int)])
  scores <- scores[!is.na(scores$ref_id) & nzchar(scores$ref_id), ]
  if (!nrow(scores)) return(.litxr_empty_arxiv_lexical_scores())
  if (any(!grepl("^[0-9]{4}\\.[0-9]{4,5}$", scores$ref_id))) {
    stop("Lexical metadata contains non-bare arXiv ids.", call. = FALSE)
  }
  scores[, category_id := as.character(category)]
  scores[, .(ref_id, category_id, score_max)]
}

#' Score one arXiv collection with tokenized lexical categories
#'
#' Scans one collection's lexical postings and returns only paper-category rows
#' that meet `min_keywords_per_category`. A query phrase matches when all of
#' its normalized tokens occur in a document; token adjacency is not required.
#' Downstream callers own date filtering, aggregation, ranking, and paper
#' hydration.
#'
#' @param collection_id ArXiv collection id.
#' @param query_set_id Query-set id under `queries/lexical`.
#' @param query_set_path Optional lexical query-set file path.
#' @param field Corpus field. Defaults to `"abstract"`.
#' @param min_keywords_per_category Minimum distinct matched query phrases.
#'
#' @return A `data.table` with bare `ref_id`, `category_id`, and `score_max`.
#' @import data.table
#' @export
litxr_score_arxiv_lexical_categories <- function(
  collection_id,
  query_set_id = "ai_category_keywords_v1",
  query_set_path = NULL,
  field = "abstract",
  min_keywords_per_category = 2L
) {
  collection_id <- as.character(collection_id)[[1L]]
  field <- as.character(field)[[1L]]
  min_keywords_per_category <- suppressWarnings(as.integer(min_keywords_per_category[[1L]]))
  if (!nzchar(collection_id) || !nzchar(field)) {
    stop("`collection_id` and `field` must be non-empty.", call. = FALSE)
  }
  if (is.na(min_keywords_per_category) || min_keywords_per_category < 1L) {
    stop("`min_keywords_per_category` must be a positive integer.", call. = FALSE)
  }

  cfg <- litxr_read_config()
  collections <- .litxr_config_collections(cfg)
  collection_pos <- match(collection_id, vapply(collections, function(collection) {
    as.character(collection$collection_id %||% collection$journal_id %||% NA_character_)
  }, character(1L)))
  if (is.na(collection_pos) || !identical(collections[[collection_pos]]$remote_channel, "arxiv")) {
    stop("`collection_id` must identify a configured arXiv collection: ", collection_id, call. = FALSE)
  }

  metadata_path <- .litxr_project_corpus_field_lexical_metadata_path(cfg, collection_id, field)
  postings_path <- .litxr_project_corpus_field_lexical_postings_path(cfg, collection_id, field)
  missing <- c(metadata_path, postings_path)[!file.exists(c(metadata_path, postings_path))]
  if (length(missing)) {
    stop("Lexical cache file(s) not found: ", paste(missing, collapse = ", "), call. = FALSE)
  }
  if (is.null(query_set_path) || !nzchar(as.character(query_set_path)[[1L]])) {
    query_set_path <- file.path(.litxr_project_queries_dir(cfg), "lexical", paste0(query_set_id, ".csv"))
  }
  query_spec <- .litxr_read_lexical_query_set(query_set_path, query_set_id = query_set_id)
  .litxr_score_arxiv_lexical_category_rows(
    query_set_id = query_spec$query_set_id,
    query_sets = query_spec$query_sets,
    metadata_path = metadata_path,
    postings_path = postings_path,
    min_keywords_per_category = min_keywords_per_category
  )
}
