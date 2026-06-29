.litxr_lexical_empty_keyword_hits <- function() {
  data.table::data.table(
    doc_id = character(),
    year = integer(),
    query_set = character(),
    category = character(),
    keyword = character()
  )
}

#' Escape regex metacharacters for lexical matching
#'
#' @param x Character vector.
#'
#' @return Escaped character vector.
#' @export
litxr_lexical_escape_regex <- function(x) {
  x <- as.character(x)
  gsub("([][{}()+*^$.|?\\\\-])", "\\\\\\1", x, perl = TRUE)
}

#' Build a keyword regex pattern
#'
#' @param keyword Keyword or phrase.
#' @param case_insensitive Whether the pattern should be case-insensitive.
#' @param whole_term Whether to require Unicode word boundaries.
#'
#' @return Regex pattern string.
#' @export
litxr_lexical_keyword_pattern <- function(
  keyword,
  case_insensitive = TRUE,
  whole_term = TRUE
) {
  keyword <- litxr_lexical_normalize_text(as.character(keyword)[[1L]])
  if (!nzchar(keyword)) {
    stop("`keyword` must be non-empty after normalization.", call. = FALSE)
  }
  pattern <- litxr_lexical_escape_regex(keyword)
  pattern <- gsub(" +", "\\\\s+", pattern, perl = TRUE)
  if (isTRUE(whole_term)) {
    pattern <- paste0("(?<![\\p{L}\\p{N}])", pattern, "(?![\\p{L}\\p{N}])")
  }
  if (isTRUE(case_insensitive)) {
    pattern <- paste0("(?i)", pattern)
  }
  pattern
}

.litxr_lexical_compile_keywords <- function(query_sets, case_insensitive = TRUE, whole_term = TRUE) {
  flat <- litxr_lexical_flatten_query_sets(query_sets)
  if (!nrow(flat)) {
    stop("`query_sets` produced no lexical keywords.", call. = FALSE)
  }
  flat$keyword_norm <- litxr_lexical_normalize_text(flat$keyword)
  flat <- flat[!is.na(flat$keyword_norm) & nzchar(flat$keyword_norm), ]
  if (!nrow(flat)) {
    stop("`query_sets` produced no usable normalized lexical keywords.", call. = FALSE)
  }
  flat$pattern <- vapply(
    flat$keyword_norm,
    litxr_lexical_keyword_pattern,
    character(1),
    case_insensitive = case_insensitive,
    whole_term = whole_term
  )
  flat
}

#' Label abstracts with lexical category matches
#'
#' Scans one shard at a time, matches category keywords against normalized
#' abstract text, and returns reusable keyword/category/set hit tables.
#'
#' @param shard_paths Named character vector of shard paths.
#' @param query_sets Named nested list of query sets and categories.
#' @param id_col Optional explicit document id column.
#' @param text_col Optional explicit abstract/text column.
#' @param years Optional subset of years to score.
#' @param min_keywords_per_category Minimum distinct keywords required for a
#'   category pass.
#' @param min_categories_per_set Minimum categories required for a set pass.
#' @param case_insensitive Whether keyword matching is case-insensitive.
#' @param whole_term Whether to require Unicode whole-term boundaries.
#' @param output_dir Optional directory where `.fst` outputs should be written.
#' @param overwrite Whether existing output files may be overwritten.
#' @param chunk_size Reserved for future chunked text scans.
#' @param verbose Whether to emit per-shard progress.
#'
#' @return List with `keyword_hits`, `category_scores`, `set_scores`, and
#'   `set_matches`.
#' @export
litxr_lexical_label_categories <- function(
  shard_paths,
  query_sets,
  id_col = NULL,
  text_col = NULL,
  years = NULL,
  min_keywords_per_category = 1L,
  min_categories_per_set = 1L,
  case_insensitive = TRUE,
  whole_term = TRUE,
  output_dir = NULL,
  overwrite = FALSE,
  chunk_size = 50000L,
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
  min_keywords_per_category <- max(1L, suppressWarnings(as.integer(min_keywords_per_category[[1L]])))
  min_categories_per_set <- max(1L, suppressWarnings(as.integer(min_categories_per_set[[1L]])))
  compiled <- .litxr_lexical_compile_keywords(
    query_sets,
    case_insensitive = case_insensitive,
    whole_term = whole_term
  )
  hit_parts <- vector("list", length(shard_paths))
  part_n <- 0L
  for (i in seq_along(shard_paths)) {
    shard_path <- shard_paths[[i]]
    year_value <- suppressWarnings(as.integer(names(shard_paths)[[i]]))
    if (isTRUE(verbose)) {
      message("lexical label shard=", names(shard_paths)[[i]])
    }
    shard <- litxr_lexical_read_shard(shard_path, year = year_value, id_col = id_col, text_col = text_col)
    if (!nrow(shard)) {
      next
    }
    shard$text <- litxr_lexical_normalize_text(shard$text)
    shard <- shard[!is.na(shard$doc_id) & nzchar(shard$doc_id) & !is.na(shard$text) & nzchar(shard$text), ]
    if (!nrow(shard)) {
      next
    }
    rows <- list()
    row_n <- 0L
    for (k in seq_len(nrow(compiled))) {
      matched <- grepl(compiled$pattern[[k]], shard$text, perl = TRUE)
      if (!any(matched)) {
        next
      }
      row_n <- row_n + 1L
      rows[[row_n]] <- data.table::data.table(
        doc_id = shard$doc_id[matched],
        year = shard$year[matched],
        query_set = compiled$query_set[[k]],
        category = compiled$category[[k]],
        keyword = compiled$keyword_norm[[k]]
      )
    }
    if (!length(rows)) {
      next
    }
    part_n <- part_n + 1L
    hit_parts[[part_n]] <- unique(data.table::rbindlist(rows, fill = TRUE))
  }
  keyword_hits <- if (part_n) {
    data.table::rbindlist(hit_parts[seq_len(part_n)], fill = TRUE)
  } else {
    .litxr_lexical_empty_keyword_hits()
  }
  keyword_hits <- unique(keyword_hits, by = c("doc_id", "year", "query_set", "category", "keyword"))

  if (!nrow(keyword_hits)) {
    category_scores <- data.table::data.table(
      doc_id = character(),
      year = integer(),
      query_set = character(),
      category = character(),
      n_keywords = integer(),
      matched_keywords = character(),
      category_pass = logical()
    )
    set_scores <- data.table::data.table(
      doc_id = character(),
      year = integer(),
      query_set = character(),
      n_categories = integer(),
      total_distinct_keywords = integer(),
      matched_categories = character(),
      set_pass = logical()
    )
    set_matches <- set_scores
  } else {
    category_key <- paste(keyword_hits$doc_id, keyword_hits$year, keyword_hits$query_set, keyword_hits$category, sep = "\r")
    category_groups <- split(seq_len(nrow(keyword_hits)), category_key)
    category_scores <- data.table::rbindlist(lapply(category_groups, function(idx) {
      data.table::data.table(
        doc_id = as.character(keyword_hits$doc_id[[idx[[1L]]]]),
        year = as.integer(keyword_hits$year[[idx[[1L]]]]),
        query_set = as.character(keyword_hits$query_set[[idx[[1L]]]]),
        category = as.character(keyword_hits$category[[idx[[1L]]]]),
        n_keywords = data.table::uniqueN(keyword_hits$keyword[idx]),
        matched_keywords = paste(sort(unique(as.character(keyword_hits$keyword[idx]))), collapse = " | ")
      )
    }), fill = TRUE)
    category_scores$category_pass <- category_scores$n_keywords >= min_keywords_per_category

    passed <- category_scores[category_scores$category_pass %in% TRUE, ]
    set_scores <- if (nrow(passed)) {
      set_key <- paste(passed$doc_id, passed$year, passed$query_set, sep = "\r")
      set_groups <- split(seq_len(nrow(passed)), set_key)
      data.table::rbindlist(lapply(set_groups, function(idx) {
        matched_keywords <- unlist(strsplit(as.character(passed$matched_keywords[idx]), " \\| ", perl = TRUE), use.names = FALSE)
        data.table::data.table(
          doc_id = as.character(passed$doc_id[[idx[[1L]]]]),
          year = as.integer(passed$year[[idx[[1L]]]]),
          query_set = as.character(passed$query_set[[idx[[1L]]]]),
          n_categories = length(idx),
          total_distinct_keywords = data.table::uniqueN(matched_keywords),
          matched_categories = paste(sort(unique(as.character(passed$category[idx]))), collapse = " | ")
        )
      }), fill = TRUE)
    } else {
      data.table::data.table(
        doc_id = character(),
        year = integer(),
        query_set = character(),
        n_categories = integer(),
        total_distinct_keywords = integer(),
        matched_categories = character()
      )
    }
    set_scores$set_pass <- set_scores$n_categories >= min_categories_per_set
    set_matches <- set_scores[set_scores$set_pass %in% TRUE, ]
  }

  if (!is.null(output_dir)) {
    output_dir <- path.expand(as.character(output_dir)[[1L]])
    if (dir.exists(output_dir) && !isTRUE(overwrite)) {
      existing <- file.path(output_dir, c(
        "lexical_keyword_hits.fst",
        "lexical_category_scores.fst",
        "lexical_set_scores.fst",
        "lexical_set_matches.fst",
        "lexical_query_keywords.fst"
      ))
      if (any(file.exists(existing))) {
        stop("Lexical output directory already contains result files: ", output_dir, call. = FALSE)
      }
    }
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    fst::write_fst(as.data.frame(keyword_hits), file.path(output_dir, "lexical_keyword_hits.fst"))
    fst::write_fst(as.data.frame(category_scores), file.path(output_dir, "lexical_category_scores.fst"))
    fst::write_fst(as.data.frame(set_scores), file.path(output_dir, "lexical_set_scores.fst"))
    fst::write_fst(as.data.frame(set_matches), file.path(output_dir, "lexical_set_matches.fst"))
    fst::write_fst(as.data.frame(compiled[, c("query_set", "category", "keyword"), with = FALSE]), file.path(output_dir, "lexical_query_keywords.fst"))
  }

  list(
    keyword_hits = keyword_hits,
    category_scores = category_scores,
    set_scores = set_scores,
    set_matches = set_matches
  )
}
