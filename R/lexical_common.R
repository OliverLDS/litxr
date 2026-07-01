.litxr_lexical_default_shard_root <- function(cfg = NULL) {
  if (is.null(cfg)) {
    cfg <- litxr_read_config()
  }
  file.path(
    .litxr_project_corpus_dir(cfg),
    "arxiv_cs_ai",
    "abstract",
    "embeddings",
    .litxr_embedding_slug("nvidia/llama-nemotron-embed-vl-1b-v2:free"),
    "shards"
  )
}

#' Build lexical shard paths
#'
#' Returns the expected yearly metadata shard paths used by the lexical-search
#' modules.
#'
#' @param root Root directory containing yearly shard subdirectories.
#' @param years Integer years to include.
#' @param filename Metadata filename inside each yearly directory.
#' @param must_exist Whether to fail when any shard path is missing.
#'
#' @return Named character vector of shard paths.
#' @export
litxr_lexical_shard_paths <- function(
  root = .litxr_lexical_default_shard_root(),
  years = 2019:2026,
  filename = "metadata.fst",
  must_exist = TRUE
) {
  years <- suppressWarnings(as.integer(years))
  years <- years[!is.na(years)]
  if (!length(years)) {
    stop("`years` must contain at least one valid year.", call. = FALSE)
  }
  root <- path.expand(as.character(root)[[1L]])
  filename <- as.character(filename)[[1L]]
  if (!nzchar(filename)) {
    stop("`filename` must be non-empty.", call. = FALSE)
  }
  out <- file.path(root, as.character(years), filename)
  names(out) <- as.character(years)
  if (isTRUE(must_exist)) {
    missing <- out[!file.exists(out)]
    if (length(missing)) {
      stop(
        "Missing lexical shard file(s): ",
        paste(unname(missing), collapse = ", "),
        call. = FALSE
      )
    }
  }
  out
}

#' Detect lexical shard columns
#'
#' Detects the document id and text columns from one shard.
#'
#' @param path Path to one metadata shard `.fst` file.
#' @param id_col Optional explicit id column.
#' @param text_col Optional explicit text column.
#' @param id_candidates Candidate id column names.
#' @param text_candidates Candidate text column names.
#'
#' @return List with `id_col`, `text_col`, and `columns`.
#' @export
litxr_lexical_detect_columns <- function(
  path,
  id_col = NULL,
  text_col = NULL,
  id_candidates = c("arxiv_id", "id", "paper_id", "ref_id", "doc_id"),
  text_candidates = c("abstract", "text", "document", "content")
) {
  path <- path.expand(as.character(path)[[1L]])
  if (!file.exists(path)) {
    stop("Shard file not found: ", path, call. = FALSE)
  }
  columns <- fst::metadata_fst(path)$columnNames
  if (is.null(columns) || !length(columns)) {
    stop("Could not read shard columns from: ", path, call. = FALSE)
  }
  choose_col <- function(explicit, candidates, label) {
    if (!is.null(explicit)) {
      explicit <- as.character(explicit)[[1L]]
      if (!(explicit %in% columns)) {
        stop("Requested ", label, " column not found in shard: ", explicit, call. = FALSE)
      }
      return(explicit)
    }
    match_col <- candidates[candidates %in% columns]
    if (!length(match_col)) {
      stop(
        "Could not detect ", label, " column in shard. Available columns: ",
        paste(columns, collapse = ", "),
        call. = FALSE
      )
    }
    match_col[[1L]]
  }
  list(
    id_col = choose_col(id_col, as.character(id_candidates), "id"),
    text_col = choose_col(text_col, as.character(text_candidates), "text"),
    columns = columns
  )
}

#' Read one lexical shard
#'
#' Reads only the document id and abstract/text columns from one metadata shard.
#'
#' @param path Path to one shard metadata `.fst` file.
#' @param year Optional explicit year. When missing, inferred from the parent
#'   directory name.
#' @param id_col Optional explicit id column.
#' @param text_col Optional explicit text column.
#'
#' @return `data.table` with standardized `doc_id`, `year`, and `text` columns.
#' @export
litxr_lexical_read_shard <- function(
  path,
  year = NA_integer_,
  id_col = NULL,
  text_col = NULL
) {
  spec <- litxr_lexical_detect_columns(path, id_col = id_col, text_col = text_col)
  dt <- fst::read_fst(
    path.expand(as.character(path)[[1L]]),
    columns = c(spec$id_col, spec$text_col),
    as.data.table = TRUE
  )
  year_value <- suppressWarnings(as.integer(year[[1L]]))
  if (is.na(year_value)) {
    year_value <- suppressWarnings(as.integer(basename(dirname(path.expand(as.character(path)[[1L]])))))
  }
  data.table::data.table(
    doc_id = as.character(dt[[spec$id_col]]),
    year = rep(year_value, nrow(dt)),
    text = as.character(dt[[spec$text_col]])
  )
}

#' Normalize lexical text
#'
#' Applies lightweight Unicode-aware normalization for lexical matching.
#'
#' @param x Character vector.
#' @param lowercase Whether to lowercase text.
#' @param normalize_unicode Whether to normalize Unicode.
#' @param collapse_hyphen Whether to replace hyphens, slashes, and underscores
#'   with spaces.
#' @param keep_symbols Symbols preserved during punctuation cleanup.
#'
#' @return Normalized character vector.
#' @export
litxr_lexical_normalize_text <- function(
  x,
  lowercase = TRUE,
  normalize_unicode = TRUE,
  collapse_hyphen = TRUE,
  keep_symbols = c("+", "#", ".")
) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- enc2utf8(x)
  if (isTRUE(lowercase)) {
    x <- tolower(x)
  }
  if (isTRUE(collapse_hyphen)) {
    x <- gsub("[-/_]+", " ", x, perl = TRUE)
  }
  keep_symbols <- unique(as.character(keep_symbols))
  keep_symbols <- keep_symbols[nzchar(keep_symbols)]
  escaped_keep <- if (length(keep_symbols)) {
    paste(gsub("([][{}()+*^$.|?\\\\-])", "\\\\\\1", keep_symbols, perl = TRUE), collapse = "")
  } else {
    ""
  }
  pattern <- paste0("[^\\p{L}\\p{N}\\s", escaped_keep, "]+")
  x <- gsub(pattern, " ", x, perl = TRUE)
  x <- gsub("\\s+", " ", x, perl = TRUE)
  trimws(x)
}

#' Tokenize lexical text
#'
#' Tokenizes normalized text into Unicode-aware term vectors.
#'
#' @param x Character vector.
#'
#' @return List of character vectors.
#' @export
litxr_lexical_tokenize <- function(x) {
  x <- litxr_lexical_normalize_text(x, collapse_hyphen = FALSE, keep_symbols = c("+", "#", ".", "-"))
  if (!length(x)) {
    return(list())
  }
  lapply(strsplit(x, "\\s+", perl = TRUE), function(tokens) {
    tokens <- as.character(tokens)
    tokens[!is.na(tokens) & nzchar(tokens)]
  })
}

#' Flatten nested lexical query sets
#'
#' Converts nested query-set lists into a long table of category keywords.
#'
#' @param query_sets Named nested list of query sets, categories, and keywords.
#'
#' @return `data.table` with `query_set`, `category`, and `keyword`.
#' @export
litxr_lexical_flatten_query_sets <- function(query_sets) {
  if (!is.list(query_sets) || !length(query_sets) || is.null(names(query_sets))) {
    stop("`query_sets` must be a named nested list.", call. = FALSE)
  }
  rows <- list()
  row_n <- 0L
  for (query_set_id in names(query_sets)) {
    categories <- query_sets[[query_set_id]]
    if (!is.list(categories) || !length(categories) || is.null(names(categories))) {
      next
    }
    for (category_id in names(categories)) {
      keywords <- as.character(unlist(categories[[category_id]], use.names = FALSE))
      keywords <- trimws(keywords)
      keywords <- keywords[!is.na(keywords) & nzchar(keywords)]
      if (!length(keywords)) {
        next
      }
      row_n <- row_n + 1L
      rows[[row_n]] <- data.table::data.table(
        query_set = as.character(query_set_id),
        category = as.character(category_id),
        keyword = keywords
      )
    }
  }
  if (!length(rows)) {
    return(data.table::data.table(
      query_set = character(),
      category = character(),
      keyword = character()
    ))
  }
  out <- data.table::rbindlist(rows, fill = TRUE)
  out <- out[!is.na(out$keyword) & nzchar(out$keyword), ]
  out <- unique(out, by = c("query_set", "category", "keyword"))
  data.table::setorder(out, query_set, category, keyword)
  out
}
