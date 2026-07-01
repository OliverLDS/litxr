.litxr_bm25_empty_docs <- function() {
  data.table::data.table(
    doc_int = integer(),
    doc_id = character(),
    year = integer(),
    dl = integer()
  )
}

.litxr_bm25_empty_vocab <- function() {
  data.table::data.table(
    term = character(),
    df = integer(),
    idf = numeric()
  )
}

.litxr_bm25_empty_postings <- function() {
  data.table::data.table(
    term = character(),
    doc_int = integer(),
    tf = integer()
  )
}

#' Build a local BM25 index
#'
#' Reads lexical shards one by one, tokenizes abstracts, and writes reusable
#' BM25 tables to disk.
#'
#' @param shard_paths Named character vector of shard paths.
#' @param id_col Optional explicit document id column.
#' @param text_col Optional explicit abstract/text column.
#' @param years Optional subset of years to include.
#' @param index_dir Output directory for the BM25 index.
#' @param lowercase Whether normalization lowercases text.
#' @param input_already_normalized Whether shard text is already normalized.
#' @param overwrite Whether an existing index directory may be replaced.
#' @param min_df Minimum document frequency to retain a term.
#' @param max_df_prop Maximum document-frequency proportion to retain a term.
#' @param verbose Whether to emit per-shard progress.
#'
#' @return Invisible list with output file paths.
#' @export
litxr_bm25_build_index <- function(
  shard_paths,
  id_col = NULL,
  text_col = NULL,
  years = NULL,
  index_dir,
  lowercase = TRUE,
  input_already_normalized = FALSE,
  overwrite = FALSE,
  min_df = 1L,
  max_df_prop = 1.0,
  verbose = TRUE
) {
  shard_paths <- stats::setNames(as.character(unname(shard_paths)), names(shard_paths))
  if (is.null(names(shard_paths)) || !length(shard_paths)) {
    stop("`shard_paths` must be a named character vector.", call. = FALSE)
  }
  years <- if (is.null(years)) names(shard_paths) else as.character(as.integer(years))
  shard_paths <- shard_paths[names(shard_paths) %in% years]
  if (!length(shard_paths)) {
    stop("No shard paths remain after year filtering.", call. = FALSE)
  }
  index_dir <- path.expand(as.character(index_dir)[[1L]])
  min_df <- max(1L, suppressWarnings(as.integer(min_df[[1L]])))
  max_df_prop <- as.numeric(max_df_prop[[1L]])
  if (is.na(max_df_prop) || max_df_prop <= 0 || max_df_prop > 1) {
    stop("`max_df_prop` must be within (0, 1].", call. = FALSE)
  }
  if (dir.exists(index_dir) && !isTRUE(overwrite)) {
    existing <- file.path(index_dir, c("bm25_docs.fst", "bm25_vocab.fst", "bm25_postings.fst", "bm25_meta.rds"))
    if (any(file.exists(existing))) {
      stop("BM25 index directory already contains index files: ", index_dir, call. = FALSE)
    }
  }
  dir.create(index_dir, recursive = TRUE, showWarnings = FALSE)

  docs_parts <- list()
  postings_parts <- list()
  doc_offset <- 0L
  for (i in seq_along(shard_paths)) {
    if (isTRUE(verbose)) {
      message("bm25 build shard=", names(shard_paths)[[i]])
    }
    shard <- litxr_lexical_read_shard(
      shard_paths[[i]],
      year = suppressWarnings(as.integer(names(shard_paths)[[i]])),
      id_col = id_col,
      text_col = text_col
    )
    shard <- shard[!is.na(shard$doc_id) & nzchar(shard$doc_id), ]
    if (!nrow(shard)) {
      next
    }
    if (anyDuplicated(shard$doc_id)) {
      stop("Duplicated `doc_id` detected within shard: ", shard_paths[[i]], call. = FALSE)
    }
    tokens <- litxr_lexical_tokenize(shard$text, normalize = !isTRUE(input_already_normalized))
    dl <- lengths(tokens)
    doc_int <- doc_offset + seq_len(nrow(shard))
    docs_parts[[length(docs_parts) + 1L]] <- data.table::data.table(
      doc_int = doc_int,
      doc_id = as.character(shard$doc_id),
      year = as.integer(shard$year),
      dl = as.integer(dl)
    )

    term_rows <- vector("list", length(tokens))
    for (j in seq_along(tokens)) {
      tok <- tokens[[j]]
      if (!length(tok)) {
        term_rows[[j]] <- NULL
        next
      }
      counts <- table(tok)
      term_rows[[j]] <- data.table::data.table(
        term = as.character(names(counts)),
        doc_int = rep(doc_int[[j]], length(counts)),
        tf = as.integer(counts)
      )
    }
    term_rows <- term_rows[!vapply(term_rows, is.null, logical(1))]
    if (length(term_rows)) {
      postings_parts[[length(postings_parts) + 1L]] <- data.table::rbindlist(term_rows, fill = TRUE)
    }
    doc_offset <- doc_offset + nrow(shard)
  }

  docs <- if (length(docs_parts)) data.table::rbindlist(docs_parts, fill = TRUE) else .litxr_bm25_empty_docs()
  if (nrow(docs) && anyDuplicated(docs$doc_id)) {
    stop("Duplicated `doc_id` detected across shards.", call. = FALSE)
  }
  postings <- if (length(postings_parts)) data.table::rbindlist(postings_parts, fill = TRUE) else .litxr_bm25_empty_postings()
  N <- nrow(docs)
  avgdl <- if (N) mean(docs$dl) else NA_real_

  vocab <- if (nrow(postings)) {
    doc_term <- unique(postings[, c("term", "doc_int"), with = FALSE])
    df_tab <- aggregate(doc_int ~ term, data = as.data.frame(doc_term), FUN = length)
    data.table::data.table(
      term = as.character(df_tab$term),
      df = as.integer(df_tab$doc_int)
    )
  } else {
    .litxr_bm25_empty_vocab()[, c("term", "df"), with = FALSE]
  }
  if (nrow(vocab)) {
    vocab <- vocab[vocab$df >= min_df, ]
    if (nrow(vocab)) {
      vocab <- vocab[vocab$df / N <= max_df_prop, ]
    }
    if (nrow(vocab)) {
      vocab$idf <- log(1 + (N - vocab$df + 0.5) / (vocab$df + 0.5))
      postings <- postings[postings$term %in% vocab$term, ]
    } else {
      postings <- .litxr_bm25_empty_postings()
      vocab <- .litxr_bm25_empty_vocab()
    }
  } else {
    vocab <- .litxr_bm25_empty_vocab()
  }

  fst::write_fst(as.data.frame(docs), file.path(index_dir, "bm25_docs.fst"))
  fst::write_fst(as.data.frame(vocab), file.path(index_dir, "bm25_vocab.fst"))
  fst::write_fst(as.data.frame(postings), file.path(index_dir, "bm25_postings.fst"))
  saveRDS(list(
    N = N,
    avgdl = avgdl,
    created_at = Sys.time(),
    shard_paths = shard_paths,
    tokenizer = "litxr_lexical_tokenize",
    normalizer = "litxr_lexical_normalize_text",
    idf_formula = "log(1 + (N - df + 0.5) / (df + 0.5))"
  ), file.path(index_dir, "bm25_meta.rds"))
  invisible(list(
    docs = file.path(index_dir, "bm25_docs.fst"),
    vocab = file.path(index_dir, "bm25_vocab.fst"),
    postings = file.path(index_dir, "bm25_postings.fst"),
    meta = file.path(index_dir, "bm25_meta.rds")
  ))
}

#' Load a BM25 index
#'
#' @param index_dir Directory containing BM25 index files.
#'
#' @return List with `docs`, `vocab`, `postings`, and `meta`.
#' @export
litxr_bm25_load_index <- function(index_dir) {
  index_dir <- path.expand(as.character(index_dir)[[1L]])
  docs_path <- file.path(index_dir, "bm25_docs.fst")
  vocab_path <- file.path(index_dir, "bm25_vocab.fst")
  postings_path <- file.path(index_dir, "bm25_postings.fst")
  meta_path <- file.path(index_dir, "bm25_meta.rds")
  required <- c(docs_path, vocab_path, postings_path, meta_path)
  missing <- required[!file.exists(required)]
  if (length(missing)) {
    stop("Missing BM25 index file(s): ", paste(missing, collapse = ", "), call. = FALSE)
  }
  list(
    docs = fst::read_fst(docs_path, as.data.table = TRUE),
    vocab = fst::read_fst(vocab_path, as.data.table = TRUE),
    postings = fst::read_fst(postings_path, as.data.table = TRUE),
    meta = readRDS(meta_path)
  )
}

#' Search a BM25 index
#'
#' @param query Free-text query string.
#' @param index Loaded BM25 index from `litxr_bm25_load_index()`.
#' @param top_k Number of ranked rows to return.
#' @param k1 BM25 `k1` parameter.
#' @param b BM25 `b` parameter.
#' @param restrict_doc_ids Optional character vector restricting candidate docs.
#' @param return_terms Whether to include `matched_terms`.
#'
#' @return `data.table` of ranked BM25 results.
#' @export
litxr_bm25_search <- function(
  query,
  index,
  top_k = 50L,
  k1 = 1.5,
  b = 0.75,
  restrict_doc_ids = NULL,
  return_terms = TRUE
) {
  if (!is.list(index) || !all(c("docs", "vocab", "postings", "meta") %in% names(index))) {
    stop("`index` must come from `litxr_bm25_load_index()`.", call. = FALSE)
  }
  top_k <- max(0L, suppressWarnings(as.integer(top_k[[1L]])))
  query_terms <- unique(unlist(litxr_lexical_tokenize(query), use.names = FALSE))
  query_terms <- query_terms[!is.na(query_terms) & nzchar(query_terms)]
  if (!length(query_terms)) {
    stop("`query` produced no usable BM25 terms after normalization.", call. = FALSE)
  }
  vocab <- data.table::as.data.table(index$vocab)
  postings <- data.table::as.data.table(index$postings)
  docs <- data.table::as.data.table(index$docs)
  if (!nrow(vocab) || !nrow(postings) || !nrow(docs)) {
    return(data.table::data.table())
  }
  vocab <- vocab[vocab$term %in% query_terms, ]
  if (!nrow(vocab)) {
    return(data.table::data.table())
  }
  postings <- postings[postings$term %in% vocab$term, ]
  if (!nrow(postings)) {
    return(data.table::data.table())
  }
  if (!is.null(restrict_doc_ids)) {
    restrict_doc_ids <- unique(as.character(restrict_doc_ids))
    restrict_doc_ids <- restrict_doc_ids[!is.na(restrict_doc_ids) & nzchar(restrict_doc_ids)]
    if (!length(restrict_doc_ids)) {
      return(data.table::data.table())
    }
    docs <- docs[docs$doc_id %in% restrict_doc_ids, ]
    if (!nrow(docs)) {
      return(data.table::data.table())
    }
    postings <- postings[postings$doc_int %in% docs$doc_int, ]
    if (!nrow(postings)) {
      return(data.table::data.table())
    }
  }
  scored <- merge(postings, vocab, by = "term", all = FALSE, sort = FALSE)
  scored <- merge(scored, docs, by = "doc_int", all = FALSE, sort = FALSE)
  if (!nrow(scored)) {
    return(data.table::data.table())
  }
  avgdl <- as.numeric(index$meta$avgdl %||% NA_real_)
  if (!is.finite(avgdl) || avgdl <= 0) {
    stop("BM25 meta `avgdl` is missing or invalid.", call. = FALSE)
  }
  scored$score_part <- scored$idf * ((scored$tf * (k1 + 1)) / (scored$tf + k1 * (1 - b + b * scored$dl / avgdl)))
  score_groups <- split(seq_len(nrow(scored)), as.character(scored$doc_id))
  out <- data.table::rbindlist(lapply(score_groups, function(idx) {
    data.table::data.table(
      doc_id = as.character(scored$doc_id[[idx[[1L]]]]),
      year = as.integer(scored$year[[idx[[1L]]]]),
      score = sum(scored$score_part[idx]),
      matched_terms = if (isTRUE(return_terms)) paste(sort(unique(as.character(scored$term[idx]))), collapse = " | ") else NA_character_,
      n_query_terms_matched = data.table::uniqueN(scored$term[idx])
    )
  }), fill = TRUE)
  data.table::setorderv(out, c("score", "doc_id"), c(-1L, 1L))
  if (top_k == 0L) {
    return(out[0, ])
  }
  if (nrow(out) > top_k) {
    out <- out[seq_len(top_k), ]
  }
  if (!isTRUE(return_terms)) {
    out$matched_terms <- NULL
  }
  out
}

#' Search a BM25 index from one lexical query set
#'
#' @param query_sets Named nested query-set list.
#' @param query_set Query-set id to use.
#' @param category Optional category id inside the query set.
#' @param index Loaded BM25 index.
#' @param top_k Number of ranked rows to return.
#' @param k1 BM25 `k1` parameter.
#' @param b BM25 `b` parameter.
#' @param restrict_doc_ids Optional candidate doc restriction.
#'
#' @return `data.table` of ranked BM25 results.
#' @export
litxr_bm25_search_query_set <- function(
  query_sets,
  query_set,
  category = NULL,
  index,
  top_k = 50L,
  k1 = 1.5,
  b = 0.75,
  restrict_doc_ids = NULL
) {
  flat <- litxr_lexical_flatten_query_sets(query_sets)
  query_set <- as.character(query_set)[[1L]]
  if (!nzchar(query_set)) {
    stop("`query_set` must be non-empty.", call. = FALSE)
  }
  flat <- flat[flat$query_set == query_set, ]
  if (!is.null(category)) {
    category <- as.character(category)[[1L]]
    flat <- flat[flat$category == category, ]
  }
  if (!nrow(flat)) {
    stop("No keywords found for the requested query set/category.", call. = FALSE)
  }
  litxr_bm25_search(
    query = paste(unique(as.character(flat$keyword)), collapse = " "),
    index = index,
    top_k = top_k,
    k1 = k1,
    b = b,
    restrict_doc_ids = restrict_doc_ids,
    return_terms = TRUE
  )
}
