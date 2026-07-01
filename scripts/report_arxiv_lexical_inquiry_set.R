#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(litxr)
})

args <- commandArgs(trailingOnly = TRUE)

`%||%` <- function(x, y) {
  if (is.null(x) || !length(x)) y else x
}

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

emit_json <- function(x) {
  writeLines(
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE),
    con = stdout()
  )
}

parse_args <- function(args) {
  out <- list(
    show_help = FALSE,
    collection_id = "arxiv_cs_ai",
    field = "abstract",
    corpus_path = NULL,
    query_set_id = "ai_category_keywords_v1",
    query_set_path = NULL,
    mode = "bm25",
    min_keywords_per_category = "2",
    top_n = "3",
    threshold = "0.45",
    output_format = "json"
  )
  i <- 1L

  while (i <= length(args)) {
    key <- args[[i]]
    if (!startsWith(key, "--") && !identical(key, "-h")) {
      stop("Unexpected positional argument: ", key, call. = FALSE)
    }
    if (identical(key, "-h") || identical(key, "--help")) {
      out$show_help <- TRUE
      i <- i + 1L
      next
    }
    if (i == length(args)) {
      stop("Missing value for ", key, call. = FALSE)
    }
    value <- args[[i + 1L]]
    if (identical(key, "--collection-id")) {
      out$collection_id <- value
    } else if (identical(key, "--field")) {
      out$field <- value
    } else if (identical(key, "--corpus-path")) {
      out$corpus_path <- value
    } else if (identical(key, "--query-set-id")) {
      out$query_set_id <- value
    } else if (identical(key, "--query-set-path")) {
      out$query_set_path <- value
    } else if (identical(key, "--mode")) {
      out$mode <- value
    } else if (identical(key, "--min-keywords-per-category") || identical(key, "--min_keywords_per_category")) {
      out$min_keywords_per_category <- value
    } else if (identical(key, "--top_n") || identical(key, "--top-n")) {
      out$top_n <- value
    } else if (identical(key, "--threshold")) {
      out$threshold <- value
    } else if (identical(key, "--output-format")) {
      out$output_format <- value
    } else {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    i <- i + 2L
  }

  out
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/report_arxiv_lexical_inquiry_set.R [--collection-id arxiv_cs_ai] [--field abstract]",
      "    [--corpus-path PATH] [--query-set-id ID] [--query-set-path PATH] [--mode keyword|bm25]",
      "    [--min-keywords-per-category 2] [--top_n 3] [--threshold 0.45] [--output-format json|md]",
      "",
      "Options:",
      "  --collection-id ID   Collection id for the raw corpus. Default: arxiv_cs_ai.",
      "  --field FIELD        Corpus field to search. Default: abstract.",
      "  --corpus-path PATH   Explicit raw corpus metadata .fst file.",
      "  --query-set-id ID    Query-set id under project.data_root/queries/lexical/.",
      "  --query-set-path P   Explicit query-set file path (.csv/.json/.yaml).",
      "  --mode MODE          Search mode: bm25 or keyword. Default: bm25.",
      "  --min-keywords-per-category N  Minimum distinct matched keywords per category. Default: 2.",
      "  --top_n N            Keep only the top N refs per category. Default: 3.",
      "  --threshold X        Keep only rows with score >= X. Default: 0.45.",
      "  --output-format F    Output format: json or md. Default: json.",
      "  -h, --help           Show this help message.",
      "",
      "Query set:",
      "  - CSV query sets must contain category_id and lexical_keywords columns.",
      "  - `lexical_keywords` may contain multiple keyword phrases separated by |.",
      "",
      "Input corpus:",
      "  - Prefers project.data_root/corpus/<collection-id>/<field>/lexical/metadata.fst when present.",
      "  - Falls back to project.data_root/corpus/<collection-id>/<field>/raw/metadata.fst.",
      "  - Raw corpus is expected to contain arxiv_id and abstract columns.",
      "",
      "Output:",
      "  Progress logs are written to stderr; machine-readable JSON is written to stdout.",
      sep = "\n"
    )
  )
}

read_lexical_query_set <- function(path, query_set_id = NULL) {
  path <- normalizePath(path, winslash = "/", mustWork = FALSE)
  if (!file.exists(path)) {
    stop("Query set file not found: ", path, call. = FALSE)
  }
  ext <- tolower(tools::file_ext(path))
  if (identical(ext, "csv")) {
    dt <- data.table::fread(path, sep = ",", header = TRUE, na.strings = c("", "NA"))
    required <- c("category_id", "lexical_keywords")
    if (!all(required %in% names(dt))) {
      stop("CSV lexical query set requires columns: category_id, lexical_keywords", call. = FALSE)
    }
    dt <- dt[!is.na(category_id) & nzchar(trimws(category_id)) & !is.na(lexical_keywords) & nzchar(trimws(lexical_keywords))]
    if (!nrow(dt)) {
      stop("Lexical query set CSV has no usable rows: ", path, call. = FALSE)
    }
    dt[, category_id := as.character(category_id)]
    dt[, lexical_keywords := as.character(lexical_keywords)]
    grouped <- split(dt$lexical_keywords, dt$category_id)
    categories <- lapply(grouped, function(values) {
      keywords <- unlist(strsplit(as.character(values), "|", fixed = TRUE), use.names = FALSE)
      keywords <- trimws(keywords)
      keywords <- keywords[!is.na(keywords) & nzchar(keywords)]
      unique(keywords)
    })
    categories <- categories[lengths(categories) > 0L]
    if (!length(categories)) {
      stop("Lexical query set CSV produced no usable categories: ", path, call. = FALSE)
    }
    if (is.null(query_set_id) || !nzchar(as.character(query_set_id))) {
      query_set_id <- tools::file_path_sans_ext(basename(path))
    }
    return(list(query_set_id = as.character(query_set_id)[[1L]], query_sets = stats::setNames(list(categories), as.character(query_set_id)[[1L]])))
  }
  if (identical(ext, "json")) {
    obj <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  } else if (ext %in% c("yml", "yaml")) {
    obj <- yaml::read_yaml(path)
  } else {
    stop("Unsupported lexical query set file extension: ", ext, call. = FALSE)
  }
  if (!is.list(obj) || !length(obj)) {
    stop("Lexical query set file is empty: ", path, call. = FALSE)
  }
  if (is.null(names(obj)) || any(!nzchar(names(obj)))) {
    stop("Lexical query set file must be a named list.", call. = FALSE)
  }
  if (length(obj) == 1L && is.list(obj[[1L]]) && !is.null(names(obj[[1L]]))) {
    query_set_id <- names(obj)[[1L]]
    query_sets <- obj
  } else {
    if (is.null(query_set_id) || !nzchar(as.character(query_set_id))) {
      query_set_id <- tools::file_path_sans_ext(basename(path))
    }
    query_sets <- stats::setNames(list(obj), as.character(query_set_id)[[1L]])
  }
  list(query_set_id = as.character(query_set_id)[[1L]], query_sets = query_sets)
}

resolve_corpus_cache_path <- function(raw_corpus_path) {
  raw_corpus_path <- normalizePath(raw_corpus_path, winslash = "/", mustWork = FALSE)
  if (basename(raw_corpus_path) == "metadata.fst" && file.exists(file.path(dirname(raw_corpus_path), "postings.fst"))) {
    return(list(
      path = raw_corpus_path,
      field = "abstract_norm",
      source = "lexical"
    ))
  }
  lexical_path <- file.path(dirname(dirname(raw_corpus_path)), "lexical", "metadata.fst")
  if (file.exists(lexical_path)) {
    return(list(
      path = lexical_path,
      field = "abstract_norm",
      source = "lexical"
    ))
  }
  list(
    path = raw_corpus_path,
    field = "abstract",
    source = "raw"
  )
}

load_query_scores_keyword <- function(cfg, collection_id, field, query_set_id, query_sets, corpus_path, top_n, threshold, min_keywords_per_category) {
  corpus_dt <- fst::read_fst(corpus_path, as.data.table = TRUE)
  required <- c("arxiv_id", field)
  if (!all(required %in% names(corpus_dt))) {
    stop(
      "Corpus metadata file must contain columns: ",
      paste(required, collapse = ", "),
      call. = FALSE
    )
  }

  doc_ids <- as.character(corpus_dt[["arxiv_id"]])
  texts <- as.character(corpus_dt[[field]])
  keep <- !is.na(doc_ids) & nzchar(doc_ids) & !is.na(texts) & nzchar(texts)
  if (!any(keep)) {
    return(data.table::data.table())
  }
  doc_ids <- doc_ids[keep]
  texts <- texts[keep]
  if (!identical(field, "abstract_norm")) {
    texts <- litxr::litxr_lexical_normalize_text(texts)
  }
  rm(corpus_dt)

  flat <- litxr::litxr_lexical_flatten_query_sets(query_sets)
  flat <- flat[flat$query_set == query_set_id, ]
  if (!nrow(flat)) {
    return(data.table::data.table())
  }
  flat[, keyword_norm := litxr::litxr_lexical_normalize_text(keyword)]
  flat <- flat[!is.na(keyword_norm) & nzchar(keyword_norm)]
  if (!nrow(flat)) {
    return(data.table::data.table())
  }
  flat <- unique(flat, by = c("query_set", "category", "keyword_norm"))
  pattern_dt <- flat[, .(
    pattern = paste(litxr::litxr_lexical_escape_regex(keyword_norm), collapse = "|")
  ), by = .(query_set, category)]
  if (!nrow(pattern_dt)) {
    return(data.table::data.table())
  }

  scores <- pattern_dt[, {
    pat <- pattern[[1L]]
    if (!nzchar(pat)) {
      return(data.table::data.table())
    }
    hit_lists <- regmatches(texts, gregexpr(pat, texts, perl = TRUE))
    hit_counts <- lengths(hit_lists)
    keep <- which(hit_counts > 0L)
    if (!length(keep)) {
      return(data.table::data.table())
    }
    matched <- unlist(hit_lists[keep], use.names = FALSE)
    doc_idx <- rep.int(keep, hit_counts[keep])
    hit_dt <- data.table::data.table(
      ref_id = doc_ids[doc_idx],
      keyword = matched
    )
    hit_dt <- unique(hit_dt)
    out <- hit_dt[, .(
      score_max = data.table::uniqueN(keyword),
      matched_keywords = paste(sort(unique(keyword)), collapse = " | ")
    ), by = ref_id]
    out[, `:=`(
      category_id = as.character(category[[1L]]),
      query_set = as.character(query_set[[1L]])
    )]
    out
  }, by = .(query_set, category)]

  if (!nrow(scores)) {
    return(data.table::data.table())
  }
  scores <- scores[!is.na(ref_id) & nzchar(ref_id) & !is.na(score_max) & score_max >= min_keywords_per_category]
  if (!nrow(scores)) {
    return(data.table::data.table())
  }
  scores <- scores[score_max >= threshold]
  if (!nrow(scores)) {
    return(data.table::data.table())
  }
  scores <- scores[query_set == query_set_id, ]
  if (!nrow(scores)) {
    return(data.table::data.table())
  }
  data.table::setorderv(scores, c("category_id", "score_max", "ref_id"), order = c(1L, -1L, 1L), na.last = TRUE)
  scores[, rank_in_category := seq_len(.N), by = category_id]
  scores <- scores[rank_in_category <= top_n]
  scores[, c("query_set", "rank_in_category") := NULL]
  scores
}

timed_step <- function(label, expr) {
  start <- proc.time()[["elapsed"]]
  value <- force(expr)
  log_line(sprintf("%s=%.3f", label, proc.time()[["elapsed"]] - start))
  value
}

lexical_cache_paths <- function(corpus_path) {
  corpus_path <- normalizePath(corpus_path, winslash = "/", mustWork = FALSE)
  lexical_dir <- if (basename(corpus_path) == "metadata.fst") {
    parent_dir <- dirname(corpus_path)
    if (basename(parent_dir) == "raw") {
      file.path(dirname(parent_dir), "lexical")
    } else {
      parent_dir
    }
  } else {
    corpus_path
  }
  list(
    metadata = file.path(lexical_dir, "metadata.fst"),
    postings = file.path(lexical_dir, "postings.fst"),
    vocab = file.path(lexical_dir, "vocab.fst")
  )
}

load_query_scores_keyword_tokenized <- function(query_set_id, query_sets, corpus_path, top_n, threshold, min_keywords_per_category) {
  paths <- lexical_cache_paths(corpus_path)
  missing <- c(paths$metadata, paths$postings)[!file.exists(c(paths$metadata, paths$postings))]
  if (length(missing)) {
    stop("Lexical cache file(s) not found: ", paste(missing, collapse = ", "), call. = FALSE)
  }

  cache_meta <- timed_step("keyword_read_metadata", fst::read_fst(paths$metadata, as.data.table = TRUE, columns = c("doc_int", "arxiv_id")))
  cache_meta <- cache_meta[!is.na(doc_int) & !is.na(arxiv_id) & nzchar(arxiv_id)]
  if (!nrow(cache_meta)) {
    return(data.table::data.table())
  }
  cache_meta[, ref_id := as.character(arxiv_id)]
  cache_meta[, arxiv_id := NULL]

  cache_postings <- timed_step("keyword_read_postings", fst::read_fst(paths$postings, as.data.table = TRUE, columns = c("doc_int", "term")))
  cache_postings <- cache_postings[!is.na(doc_int) & !is.na(term) & nzchar(term)]
  if (!nrow(cache_postings)) {
    return(data.table::data.table())
  }

  flat <- timed_step("keyword_flatten_query_sets", litxr::litxr_lexical_flatten_query_sets(query_sets))
  flat <- flat[flat$query_set == query_set_id, ]
  if (!nrow(flat)) {
    return(data.table::data.table())
  }
  flat[, keyword_norm := litxr::litxr_lexical_normalize_text(keyword)]
  flat <- flat[!is.na(keyword_norm) & nzchar(keyword_norm)]
  if (!nrow(flat)) {
    return(data.table::data.table())
  }
  flat <- unique(flat, by = c("query_set", "category", "keyword_norm"))

  keyword_tokens <- litxr::litxr_lexical_tokenize(flat$keyword_norm, normalize = FALSE)
  token_counts <- lengths(keyword_tokens)
  keep <- token_counts > 0L
  if (!any(keep)) {
    return(data.table::data.table())
  }
  flat <- flat[keep]
  keyword_tokens <- keyword_tokens[keep]
  token_counts <- token_counts[keep]
  keyword_parts <- data.table::data.table(
    query_set = flat$query_set[rep.int(seq_len(nrow(flat)), token_counts)],
    category = flat$category[rep.int(seq_len(nrow(flat)), token_counts)],
    keyword_norm = flat$keyword_norm[rep.int(seq_len(nrow(flat)), token_counts)],
    term = unlist(keyword_tokens, use.names = FALSE)
  )
  if (!nrow(keyword_parts)) {
    return(data.table::data.table())
  }
  keyword_terms <- unique(keyword_parts)
  keyword_terms[, n_terms := data.table::uniqueN(term), by = .(query_set, category, keyword_norm)]
  needed_terms <- unique(keyword_terms$term)

  keyword_postings <- timed_step(
    "keyword_filter_postings",
    cache_postings[term %in% needed_terms, .(doc_int, term)]
  )
  if (!nrow(keyword_postings)) {
    return(data.table::data.table())
  }

  matches <- timed_step(
    "keyword_join_postings",
    keyword_postings[keyword_terms, on = "term", nomatch = 0L, allow.cartesian = TRUE]
  )
  if (!nrow(matches)) {
    return(data.table::data.table())
  }
  matches <- unique(matches[, .(doc_int, query_set, category, keyword_norm, term, n_terms)])

  keyword_counts <- timed_step(
    "keyword_count_terms",
    matches[, .(n_terms_matched = .N), by = .(doc_int, query_set, category, keyword_norm, n_terms)]
  )
  keyword_counts <- keyword_counts[n_terms_matched >= n_terms]
  if (!nrow(keyword_counts)) {
    return(data.table::data.table())
  }

  scores <- timed_step(
    "keyword_score_categories",
    keyword_counts[, .(
      score_max = .N,
      matched_keywords = paste(sort(unique(keyword_norm)), collapse = " | ")
    ), by = .(doc_int, query_set, category)]
  )
  scores <- scores[score_max >= min_keywords_per_category]
  if (!nrow(scores)) {
    return(data.table::data.table())
  }
  scores <- scores[score_max >= threshold]
  if (!nrow(scores)) {
    return(data.table::data.table())
  }

  scores[, ref_id := cache_meta$ref_id[match(doc_int, cache_meta$doc_int)]]
  scores <- scores[!is.na(ref_id) & nzchar(ref_id)]
  if (!nrow(scores)) {
    return(data.table::data.table())
  }
  scores[, category_id := as.character(category)]
  scores[, query_set := as.character(query_set)]
  data.table::setorderv(scores, c("category_id", "score_max", "ref_id"), order = c(1L, -1L, 1L), na.last = TRUE)
  scores[, rank_in_category := seq_len(.N), by = category_id]
  scores <- scores[rank_in_category <= top_n]
  scores[, c("query_set", "rank_in_category", "doc_int", "category") := NULL]
  scores[]
}

load_query_scores_bm25 <- function(query_set_id, query_sets, corpus_path, field, top_n, threshold) {
  paths <- lexical_cache_paths(corpus_path)
  missing <- c(paths$metadata, paths$postings, paths$vocab)[!file.exists(c(paths$metadata, paths$postings, paths$vocab))]
  if (length(missing)) {
    stop("Lexical cache file(s) not found: ", paste(missing, collapse = ", "), call. = FALSE)
  }

  cache_meta <- timed_step(
    "bm25_read_metadata",
    fst::read_fst(paths$metadata, as.data.table = TRUE, columns = c("doc_int", "arxiv_id", "dl"))
  )
  cache_meta <- cache_meta[!is.na(doc_int) & !is.na(arxiv_id) & nzchar(arxiv_id) & !is.na(dl)]
  if (!nrow(cache_meta)) {
    return(data.table::data.table())
  }
  cache_meta[, ref_id := as.character(arxiv_id)]
  cache_meta[, arxiv_id := NULL]

  cache_postings <- timed_step(
    "bm25_read_postings",
    fst::read_fst(paths$postings, as.data.table = TRUE, columns = c("doc_int", "term", "tf"))
  )
  cache_postings <- cache_postings[!is.na(doc_int) & !is.na(term) & nzchar(term) & !is.na(tf) & tf > 0L]
  if (!nrow(cache_postings)) {
    return(data.table::data.table())
  }

  cache_vocab <- timed_step(
    "bm25_read_vocab",
    fst::read_fst(paths$vocab, as.data.table = TRUE, columns = c("term", "idf"))
  )
  cache_vocab <- cache_vocab[!is.na(term) & nzchar(term) & !is.na(idf)]
  if (!nrow(cache_vocab)) {
    return(data.table::data.table())
  }

  flat <- timed_step("bm25_flatten_query_sets", litxr::litxr_lexical_flatten_query_sets(query_sets))
  flat <- flat[flat$query_set == query_set_id, ]
  if (!nrow(flat)) {
    return(data.table::data.table())
  }
  flat[, keyword_norm := litxr::litxr_lexical_normalize_text(keyword)]
  flat <- flat[!is.na(keyword_norm) & nzchar(keyword_norm)]
  if (!nrow(flat)) {
    return(data.table::data.table())
  }
  flat <- unique(flat, by = c("query_set", "category", "keyword_norm"))

  keyword_tokens <- litxr::litxr_lexical_tokenize(flat$keyword_norm, normalize = FALSE)
  token_counts <- lengths(keyword_tokens)
  keep <- token_counts > 0L
  if (!any(keep)) {
    return(data.table::data.table())
  }
  flat <- flat[keep]
  keyword_tokens <- keyword_tokens[keep]
  token_counts <- token_counts[keep]
  query_terms <- data.table::data.table(
    query_set = flat$query_set[rep.int(seq_len(nrow(flat)), token_counts)],
    category = flat$category[rep.int(seq_len(nrow(flat)), token_counts)],
    term = unlist(keyword_tokens, use.names = FALSE)
  )
  query_terms <- unique(query_terms[!is.na(term) & nzchar(term)], by = c("query_set", "category", "term"))
  if (!nrow(query_terms)) {
    return(data.table::data.table())
  }

  needed_terms <- unique(query_terms$term)
  query_terms <- query_terms[term %in% needed_terms]
  cache_vocab <- cache_vocab[term %in% needed_terms]
  cache_postings <- timed_step(
    "bm25_filter_postings",
    cache_postings[term %in% needed_terms, .(doc_int, term, tf)]
  )
  if (!nrow(cache_postings)) {
    return(data.table::data.table())
  }

  scored <- timed_step(
    "bm25_join_vocab",
    cache_postings[cache_vocab, on = "term", nomatch = 0L]
  )
  if (!nrow(scored)) {
    return(data.table::data.table())
  }
  scored <- timed_step(
    "bm25_join_metadata",
    scored[cache_meta, on = "doc_int", nomatch = 0L]
  )
  if (!nrow(scored)) {
    return(data.table::data.table())
  }
  scored <- timed_step(
    "bm25_join_query_terms",
    scored[query_terms, on = "term", nomatch = 0L, allow.cartesian = TRUE]
  )
  if (!nrow(scored)) {
    return(data.table::data.table())
  }

  avgdl <- mean(cache_meta$dl)
  if (!is.finite(avgdl) || avgdl <= 0) {
    stop("BM25 cache metadata has invalid average document length.", call. = FALSE)
  }
  scored[, score_part := idf * ((tf * (1.5 + 1)) / (tf + 1.5 * (1 - 0.75 + 0.75 * dl / avgdl)))]
  scores <- timed_step(
    "bm25_score_categories",
    scored[, .(
      score_max = sum(score_part)
    ), by = .(doc_int, query_set, category)]
  )
  scores <- scores[!is.na(score_max) & score_max >= threshold]
  if (!nrow(scores)) {
    return(data.table::data.table())
  }
  scores[, ref_id := cache_meta$ref_id[match(doc_int, cache_meta$doc_int)]]
  scores <- scores[!is.na(ref_id) & nzchar(ref_id)]
  if (!nrow(scores)) {
    return(data.table::data.table())
  }
  scores[, category_id := as.character(category)]
  scores[, query_set := as.character(query_set)]
  data.table::setorderv(scores, c("category_id", "score_max", "ref_id"), order = c(1L, -1L, 1L), na.last = TRUE)
  scores[, rank_in_category := seq_len(.N), by = category_id]
  scores <- scores[rank_in_category <= top_n]
  scores[, c("query_set", "rank_in_category", "doc_int", "category") := NULL]
  scores[]
}

hydrate_titles <- function(cfg, collection_id, selected) {
  if (!nrow(selected)) {
    return(selected)
  }
  if (!("ref_id" %in% names(selected))) {
    return(selected)
  }
  lookup_ids <- as.character(selected$ref_id)
  lookup_ids <- lookup_ids[!is.na(lookup_ids) & nzchar(lookup_ids)]
  if (!length(lookup_ids)) {
    selected$title <- NA_character_
    return(selected)
  }
  json_locations <- litxr:::.litxr_ref_json_locations_from_thin_stores(
    cfg,
    lookup_ids,
    collection_id = collection_id
  )
  if (!nrow(json_locations)) {
    selected$title <- NA_character_
    return(selected)
  }
  if ("ref_id" %in% names(json_locations)) {
    json_locations[, ref_id := sub("^arxiv:", "", as.character(ref_id))]
  }
  payload_rows <- data.table::data.table(ref_id = lookup_ids)
  hydrated <- litxr:::.litxr_hydrate_rows_from_json_paths(
    payload_rows,
    json_locations,
    fields = "title"
  )
  if (!nrow(hydrated) || !("title" %in% names(hydrated))) {
    selected$title <- NA_character_
    return(selected)
  }
  hydrated <- hydrated[!is.na(ref_id) & nzchar(ref_id), .(ref_id, title)]
  hydrated[, ref_id := sub("^arxiv:", "", as.character(ref_id))]
  if (!nrow(hydrated)) {
    selected$title <- NA_character_
    return(selected)
  }
  selected[, title := hydrated$title[match(ref_id, hydrated$ref_id)]]
  if (!("title" %in% names(selected))) {
    selected$title <- NA_character_
  }
  selected
}

cleanup_temp_index <- function(path) {
  if (!is.null(path) && dir.exists(path)) {
    unlink(path, recursive = TRUE, force = TRUE)
  }
  invisible(NULL)
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

parsed <- parse_args(args)
if (isTRUE(parsed$show_help)) {
  usage()
  quit(save = "no", status = 0L)
}

collection_id <- as.character(parsed$collection_id)[[1L]]
field <- as.character(parsed$field)[[1L]]
top_n <- as.integer(parsed$top_n)
threshold <- as.numeric(parsed$threshold)
mode <- tolower(trimws(as.character(parsed$mode)[[1L]]))
min_keywords_per_category <- as.integer(parsed$min_keywords_per_category)
output_format <- tolower(trimws(as.character(parsed$output_format)[[1L]]))

if (!nzchar(collection_id)) {
  stop("`--collection-id` must be non-empty.", call. = FALSE)
}
if (!nzchar(field)) {
  stop("`--field` must be non-empty.", call. = FALSE)
}
if (is.na(top_n) || top_n < 0L) {
  stop("`--top_n` must be a non-negative integer.", call. = FALSE)
}
if (is.na(threshold)) {
  stop("`--threshold` must be numeric.", call. = FALSE)
}
if (!(mode %in% c("bm25", "keyword"))) {
  stop("`--mode` must be either bm25 or keyword.", call. = FALSE)
}
if (is.na(min_keywords_per_category) || min_keywords_per_category < 1L) {
  stop("`--min-keywords-per-category` must be a positive integer.", call. = FALSE)
}
if (!(output_format %in% c("json", "md"))) {
  stop("`--output-format` must be either json or md.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
corpus_path <- parsed$corpus_path
if (is.null(corpus_path) || !nzchar(as.character(corpus_path)[[1L]])) {
  corpus_path <- file.path(
    litxr:::.litxr_project_root(cfg),
    "corpus",
    collection_id,
    field,
    "raw",
    "metadata.fst"
  )
}
corpus_path <- normalizePath(corpus_path, winslash = "/", mustWork = FALSE)
if (!file.exists(corpus_path)) {
  stop("Corpus metadata file not found: ", corpus_path, call. = FALSE)
}
cache_spec <- resolve_corpus_cache_path(corpus_path)
corpus_path <- cache_spec$path
field <- cache_spec$field

query_set_path <- parsed$query_set_path
query_set_id <- parsed$query_set_id
if (!is.null(query_set_path) && nzchar(as.character(query_set_path)[[1L]])) {
  query_set_path <- normalizePath(query_set_path, winslash = "/", mustWork = FALSE)
} else {
  query_set_path <- file.path(
    litxr:::.litxr_project_queries_dir(cfg),
    "lexical",
    paste0(query_set_id, ".csv")
  )
}
query_spec <- read_lexical_query_set(query_set_path, query_set_id = query_set_id)
query_set_id <- query_spec$query_set_id
query_sets <- query_spec$query_sets

log_line(sprintf("collection_id=%s", collection_id))
log_line(sprintf("field=%s", field))
log_line(sprintf("corpus_source=%s", cache_spec$source))
log_line(sprintf("mode=%s", mode))
log_line(sprintf("query_set_id=%s", query_set_id))
log_line(sprintf("query_set_path=%s", query_set_path))
log_line(sprintf("corpus_path=%s", corpus_path))

scores <- if (identical(mode, "keyword")) {
  load_query_scores_keyword_tokenized(
    query_set_id = query_set_id,
    query_sets = query_sets,
    corpus_path = corpus_path,
    top_n = top_n,
    threshold = threshold,
    min_keywords_per_category = min_keywords_per_category
  )
} else {
  load_query_scores_bm25(
    query_set_id = query_set_id,
    query_sets = query_sets,
    corpus_path = corpus_path,
    field = field,
    top_n = top_n,
    threshold = threshold
  )
}

if (!nrow(scores)) {
  if (identical(output_format, "json")) {
    emit_json(list(
      status = "ok",
      output_format = output_format,
      mode = mode,
      collection_id = collection_id,
      field = field,
      query_set_id = query_set_id,
      query_set_path = query_set_path,
      corpus_path = corpus_path,
      meta = list(ref_ids = character()),
      selection_rule = list(
        top_n = top_n,
        threshold = threshold,
        mode = mode,
        query_set_id = query_set_id,
        min_keywords_per_category = min_keywords_per_category
      ),
      categories = list()
    ))
  } else {
    cat("No refs met the selection rule.\n")
  }
  quit(save = "no", status = 0)
}

scores <- data.table::as.data.table(scores)
data.table::setorderv(scores, c("category_id", "score_max", "ref_id"), order = c(1L, -1L, 1L), na.last = TRUE)
scores <- scores[!is.na(ref_id) & nzchar(ref_id)]
if (!nrow(scores)) {
  cleanup_temp_index(NULL)
  stop("Lexical search produced no usable ref ids.", call. = FALSE)
}
scores <- hydrate_titles(cfg, collection_id, scores)
if (!("title" %in% names(scores))) {
  scores$title <- NA_character_
}
data.table::setorderv(scores, c("category_id", "score_max", "title"), order = c(1L, -1L, 1L), na.last = TRUE)

if (identical(output_format, "json")) {
  category_list <- lapply(unique(as.character(scores$category_id)), function(cat_id) {
    chunk <- scores[category_id == cat_id, ]
    list(
      category_id = cat_id,
      items = lapply(seq_len(nrow(chunk)), function(i) {
        list(
          rank = i,
          ref_id = as.character(chunk$ref_id[[i]]),
          score_max = as.numeric(chunk$score_max[[i]]),
          title = as.character(chunk$title[[i]])
        )
      })
    )
  })
  emit_json(list(
    status = "ok",
    output_format = output_format,
    mode = mode,
    collection_id = collection_id,
    field = field,
    query_set_id = query_set_id,
    query_set_path = query_set_path,
    corpus_path = corpus_path,
    meta = list(ref_ids = unique(as.character(scores$ref_id))),
    selection_rule = list(
      top_n = top_n,
      threshold = threshold,
      mode = mode,
      query_set_id = query_set_id,
      min_keywords_per_category = min_keywords_per_category
    ),
    categories = category_list
  ))
} else {
  for (cat_id in unique(as.character(scores$category_id))) {
    chunk <- scores[category_id == cat_id, ]
    cat(cat_id, "\n", sep = "")
    for (i in seq_len(nrow(chunk))) {
      cat(sprintf(
        "%d. %s (%.7f): %s\n",
        i,
        chunk$ref_id[[i]],
        chunk$score_max[[i]],
        chunk$title[[i]]
      ))
    }
    cat("\n")
  }
}
