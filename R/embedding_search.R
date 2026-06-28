#' Build pending embedding delta shards for one collection field
#'
#' Embeds only records that are not already present in the main cache or delta
#' shards, then appends those batches to the delta layer without compacting the
#' main cache.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param field Text field to embed, such as `"abstract"`.
#' @param embed_fun Function taking a character vector and returning embeddings
#'   as a matrix, data frame, or list of numeric vectors.
#' @param model Exact embedding model name. The same value must be used for
#'   search queries.
#' @param provider Optional provider label, such as `"openrouter"` or
#'   `"gemini"`.
#' @param batch_size Number of texts per embedding request.
#' @param overwrite Whether to rebuild the delta target set from scratch.
#' @param limit Optional maximum number of new texts to embed in this run.
#'
#' @return Metadata `data.table` for the merged main-plus-delta embedding state.
#' @export
litxr_embed_collection_delta <- function(
  collection_id,
  config = NULL,
  field = "abstract",
  embed_fun,
  model,
  provider = NA_character_,
  batch_size = 64L,
  overwrite = FALSE,
  limit = NULL
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  prepared <- .litxr_prepare_embedding_targets(
    collection_id = collection_id,
    cfg = cfg,
    field = field,
    model = model,
    overwrite = overwrite,
    limit = limit
  )

  delta_metadata <- .litxr_write_embedding_delta_batches(
    targets = prepared$targets,
    paths = prepared$paths,
    collection_id = collection_id,
    field = field,
    embed_fun = embed_fun,
    model = model,
    provider = provider,
    batch_size = batch_size,
    existing = prepared$existing,
    existing_delta = prepared$delta
  )

  delta_metadata
}

#' Build a cached embedding index for one collection field
#'
#' Embeds one text field from `litxr_read_collection()` in batches and caches the
#' resulting numeric matrix under `project.data_root/embeddings/`. The embedding
#' function is user supplied so callers can use providers such as
#' `inferencer::embed_openrouter()` or `inferencer::embed_gemini()`. Completed
#' batches are written to append-only delta shards and compacted into the main
#' embedding index once at the end.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param field Text field to embed, such as `"abstract"`.
#' @param embed_fun Function taking a character vector and returning embeddings
#'   as a matrix, data frame, or list of numeric vectors.
#' @param model Exact embedding model name. The same value must be used for
#'   search queries.
#' @param provider Optional provider label, such as `"openrouter"` or
#'   `"gemini"`.
#' @param batch_size Number of texts per embedding request.
#' @param overwrite Whether to rebuild the cache from scratch.
#' @param limit Optional maximum number of new texts to embed in this run.
#'
#' @return Metadata `data.table` for cached embeddings.
#' @export
litxr_build_embedding_index <- function(
  collection_id,
  config = NULL,
  field = "abstract",
  embed_fun,
  model,
  provider = NA_character_,
  batch_size = 64L,
  overwrite = FALSE,
  limit = NULL
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (!is.function(embed_fun)) {
    stop("`embed_fun` must be a function that accepts a character vector.", call. = FALSE)
  }
  delta <- litxr_embed_collection_delta(
    collection_id = collection_id,
    config = cfg,
    field = field,
    embed_fun = embed_fun,
    model = model,
    provider = provider,
    batch_size = batch_size,
    overwrite = overwrite,
    limit = limit
  )

  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  if (!nrow(delta) && !length(.litxr_embedding_delta_shard_paths(paths))) {
    return(.litxr_compact_embedding_index(
      paths = paths,
      collection_id = collection_id,
      field = field,
      model = as.character(model),
      provider = as.character(provider),
      overwrite = isTRUE(overwrite)
    ))
  }

  tryCatch(
    litxr_compact_embedding_delta(
      collection_id = collection_id,
      config = cfg,
      field = field,
      model = model,
      provider = provider,
      overwrite = overwrite
    ),
    error = function(e) {
      if (!isTRUE(overwrite) && .litxr_is_embedding_matrix_read_error(e)) {
        warning(
          "Main embedding cache is unreadable, so newly embedded rows were left in delta shards and were not compacted. ",
          "Run `litxr_build_embedding_index(..., overwrite = TRUE)` to rebuild the main cache, or ",
          "`litxr_compact_embedding_delta(..., overwrite = TRUE)` if you want to compact from delta only. ",
          "Original error: ", conditionMessage(e),
          call. = FALSE
        )
        return(.litxr_read_embedding_delta_parts(paths)$metadata)
      }
      stop(e)
    }
  )
}

#' Compact pending embedding delta shards into the main embedding cache
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param field Embedded text field.
#' @param model Exact embedding model name used when building the cache.
#' @param provider Optional provider label used in the resulting manifest.
#' @param overwrite Whether the compacted result should ignore any existing main
#'   embedding cache and rebuild it from delta only.
#'
#' @return Metadata `data.table` for the compacted main embedding index.
#' @export
litxr_compact_embedding_delta <- function(
  collection_id,
  config = NULL,
  field = "abstract",
  model,
  provider = NA_character_,
  overwrite = FALSE
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }
  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  .litxr_compact_embedding_index(
    paths = paths,
    collection_id = collection_id,
    field = field,
    model = as.character(model),
    provider = as.character(provider),
    overwrite = isTRUE(overwrite)
  )
}

#' Search embedding delta shards instead of the compacted main cache
#'
#' Embeds the query with the supplied `embed_fun`, or uses a precomputed
#' `query_vec`, then ranks records from one or more selected delta shard files
#' by cosine similarity. Use `date` to select shard files whose timestamp prefix
#' matches a `YYYY-MM-DD` day, or `shard` to point at one specific shard file
#' directly.
#'
#' @param query Query text. Ignored when `query_vec` is supplied.
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param field Embedded text field.
#' @param embed_fun Function taking a character vector and returning embeddings.
#'   Required only when `query_vec` is not supplied.
#' @param query_vec Optional precomputed numeric query embedding vector.
#' @param model Exact embedding model name. Must match the selected delta shard
#'   model.
#' @param date Optional date used to filter shard filenames. Accepts values
#'   coercible by `as.Date()`.
#' @param shard Optional shard filename or full shard path under the collection's
#'   embedding delta directory.
#' @param top_n Number of matches to return.
#'
#' @return `data.table` of matches with `score`.
#' @export
litxr_search_embedding_delta <- function(
  query,
  collection_id,
  config = NULL,
  field = "abstract",
  embed_fun = NULL,
  query_vec = NULL,
  model,
  date = NULL,
  shard = NULL,
  top_n = 20L
) {
  top_n <- as.integer(top_n)
  if (is.na(top_n) || top_n < 0L) {
    stop("`top_n` must be a non-negative integer.", call. = FALSE)
  }

  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }

  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  shard_paths <- .litxr_select_embedding_delta_shards(paths, shard = shard, date = date)
  if (!length(shard_paths)) {
    return(data.table::data.table())
  }

  delta <- .litxr_read_embedding_delta_shards(shard_paths)
  if (!nrow(delta$metadata) || !nrow(delta$matrix)) {
    return(data.table::data.table())
  }
  .litxr_assert_unit_normalized_matrix(delta$matrix, context = "embedding delta matrix")
  manifest_model <- delta$manifest$model %||% NA_character_
  if (!identical(as.character(model), as.character(manifest_model))) {
    stop("Query model does not match selected delta shard model: ", manifest_model, call. = FALSE)
  }

  query_vector <- .litxr_resolve_query_vector(
    query = query,
    embed_fun = embed_fun,
    query_vec = query_vec
  )
  scores <- litxr_cosine_similarity(query_vector, delta$matrix)
  .litxr_top_n_scored_candidates(delta$metadata, scores, top_n)
}

#' Read a cached embedding index
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param field Embedded text field.
#' @param model Exact embedding model name used when building the cache.
#'
#' @return List with `metadata`, `matrix`, and `manifest`.
#' @export
litxr_read_embedding_index <- function(collection_id, config = NULL, field = "abstract", model) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }
  .litxr_read_embedding_index_parts(.litxr_embedding_index_paths(cfg, collection_id, field, model))
}

#' Search cached collection embeddings
#'
#' Embeds the query with the supplied `embed_fun`, or uses a precomputed
#' `query_vec`, checks that `model` matches the cached corpus model, and ranks
#' cached records by cosine similarity.
#'
#' @param query Query text. Ignored when `query_vec` is supplied.
#' @param collection_id Collection identifier from `config.yaml`.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param field Embedded text field.
#' @param embed_fun Function taking a character vector and returning embeddings.
#'   Required only when `query_vec` is not supplied.
#' @param query_vec Optional precomputed numeric query embedding vector.
#' @param model Exact embedding model name. Must match the cached corpus model.
#' @param top_n Number of matches to return.
#'
#' @return `data.table` of matches with `score`.
#' @export
litxr_search_embeddings <- function(
  query,
  collection_id,
  config = NULL,
  field = "abstract",
  embed_fun = NULL,
  query_vec = NULL,
  model,
  top_n = 20L
) {
  top_n <- as.integer(top_n)
  if (is.na(top_n) || top_n < 0L) {
    stop("`top_n` must be a non-negative integer.", call. = FALSE)
  }
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  manifest <- if (file.exists(paths$manifest)) jsonlite::fromJSON(paths$manifest, simplifyVector = FALSE) else list()
  manifest_model <- manifest$model %||% NA_character_
  if (!is.na(manifest_model) && !identical(as.character(model), as.character(manifest_model))) {
    stop("Query model does not match cached corpus model: ", manifest_model, call. = FALSE)
  }

  query_vector <- .litxr_resolve_query_vector(
    query = query,
    embed_fun = embed_fun,
    query_vec = query_vec
  )
  if (top_n == 0L) {
    out <- data.table::data.table(score = numeric())
    return(out)
  }
  .litxr_search_embeddings_streaming(paths, query_vector, top_n)
}

.litxr_search_embeddings_streaming <- function(paths, query_vector, top_n) {
  if (.litxr_embedding_has_shards(paths)) {
    shard_keys <- .litxr_embedding_shard_keys(paths)
    candidates <- vector("list", length(shard_keys))
    candidate_n <- 0L
    for (key in shard_keys) {
      shard <- .litxr_read_embedding_shard_part(paths, key, read_matrix = TRUE)
      if (!nrow(shard$metadata) || !nrow(shard$matrix)) {
        next
      }
      .litxr_assert_unit_normalized_matrix(shard$matrix, context = paste0("embedding shard ", key))
      scores <- litxr_cosine_similarity(query_vector, shard$matrix)
      shard_candidates <- .litxr_top_n_scored_candidates(shard$metadata, scores, top_n)
      if (nrow(shard_candidates)) {
        candidate_n <- candidate_n + 1L
        candidates[[candidate_n]] <- shard_candidates
      }
    }
    candidates <- candidates[seq_len(candidate_n)]
    if (!length(candidates)) {
      return(data.table::data.table(score = numeric()))
    }
    combined <- data.table::rbindlist(candidates, fill = TRUE)
    .litxr_top_n_scored_candidates(combined, top_n = top_n)
  } else {
    index <- .litxr_read_embedding_index_parts(paths, read_matrix = TRUE)
    if (!nrow(index$metadata) || !nrow(index$matrix)) {
      return(data.table::data.table(score = numeric()))
    }
    .litxr_assert_unit_normalized_matrix(index$matrix, context = "embedding main matrix")
    scores <- litxr_cosine_similarity(query_vector, index$matrix)
    .litxr_top_n_scored_candidates(index$metadata, scores, top_n)
  }
}

#' Build a cached category-query embedding index
#'
#' Builds and caches embeddings for a set of category-defining query sentences.
#' The query set can be supplied as a named R list or a JSON/YAML file path.
#'
#' @param query_set Named list mapping category ids to character vectors of
#'   query sentences, or a path to a JSON/YAML file containing that structure.
#' @param query_set_id Identifier for this category query set.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param embed_fun Function taking a character vector and returning embeddings.
#' @param model Exact embedding model name.
#' @param provider Optional provider label, such as `"openrouter"` or
#'   `"gemini"`.
#' @param batch_size Number of query sentences per embedding request.
#' @param overwrite Whether to rebuild the cached query index from scratch.
#'
#' @return List with `metadata`, `matrix`, and `manifest`.
#' @export
litxr_build_label_query_index <- function(
  query_set,
  query_set_id = "default",
  config = NULL,
  embed_fun,
  model,
  provider = NA_character_,
  batch_size = 64L,
  overwrite = FALSE
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (!is.function(embed_fun)) {
    stop("`embed_fun` must be a function that accepts a character vector.", call. = FALSE)
  }
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }
  batch_size <- as.integer(batch_size)
  if (is.na(batch_size) || batch_size <= 0L) {
    stop("`batch_size` must be a positive integer.", call. = FALSE)
  }

  queries <- .litxr_normalize_label_query_set(query_set)
  paths <- .litxr_label_query_index_paths(cfg, query_set_id, model)
  existing <- .litxr_read_label_query_index_parts(paths, read_matrix = FALSE)
  if (isTRUE(overwrite)) {
    existing <- list(metadata = .litxr_empty_label_query_metadata(), matrix = NULL, manifest = list())
  }

  existing_ids <- if (nrow(existing$metadata)) existing$metadata$query_id else character()
  todo <- if (isTRUE(overwrite)) queries else queries[!(queries$query_id %in% existing_ids), ]

  parts <- if (isTRUE(overwrite)) {
    list(
      metadata = .litxr_empty_label_query_metadata(),
      matrix = matrix(numeric(), nrow = 0L, ncol = 0L),
      manifest = list()
    )
  } else {
    .litxr_read_label_query_index_parts(paths, read_matrix = TRUE)
  }

  if (nrow(todo)) {
    meta_parts <- list()
    matrix_parts <- list()
    if (nrow(parts$metadata)) {
      meta_parts[[length(meta_parts) + 1L]] <- parts$metadata
    }
    if (!is.null(parts$matrix) && nrow(parts$matrix)) {
      matrix_parts[[length(matrix_parts) + 1L]] <- parts$matrix
    }
    for (start in seq(1L, nrow(todo), by = batch_size)) {
      end <- min(start + batch_size - 1L, nrow(todo))
      batch <- todo[start:end, ]
      batch_matrix <- .litxr_as_embedding_matrix(embed_fun(batch$query_text))
      if (nrow(batch_matrix) != nrow(batch)) {
        stop("`embed_fun` returned ", nrow(batch_matrix), " embeddings for ", nrow(batch), " query sentences.", call. = FALSE)
      }

      existing_dimension <- .litxr_label_query_existing_dimension(parts)
      if (!is.na(existing_dimension) && existing_dimension != ncol(batch_matrix)) {
        stop("Label query embedding dimension changed from ", existing_dimension, " to ", ncol(batch_matrix), ".", call. = FALSE)
      }

      batch_meta <- data.table::copy(batch)
      meta_parts[[length(meta_parts) + 1L]] <- batch_meta
      matrix_parts[[length(matrix_parts) + 1L]] <- batch_matrix
    }
    parts$metadata <- data.table::rbindlist(meta_parts, fill = TRUE)
    parts$matrix <- if (length(matrix_parts)) do.call(rbind, matrix_parts) else matrix(numeric(), nrow = 0L, ncol = 0L)
    keep <- !duplicated(parts$metadata$query_id, fromLast = TRUE)
    parts$metadata <- parts$metadata[keep, ]
    parts$matrix <- parts$matrix[keep, , drop = FALSE]
  }

  manifest <- list(
    query_set_id = as.character(query_set_id),
    model = as.character(model),
    provider = as.character(provider),
    dimension = if (nrow(parts$matrix)) as.integer(ncol(parts$matrix)) else as.integer(existing$manifest$dimension %||% 0L),
    records = as.integer(nrow(parts$metadata)),
    updated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )
  .litxr_write_label_query_index(paths, parts$metadata, parts$matrix, manifest)
  .litxr_read_label_query_index_parts(paths, read_matrix = TRUE)
}

#' Score one collection against category-query embeddings
#'
#' Scores each collection record against all query sentences in a cached category
#' query index, then aggregates within category by `max`, `mean`, or
#' `top_k_mean`.
#'
#' @param collection_id Collection identifier from `config.yaml`.
#' @param query_set_id Identifier of a cached category query set.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#' @param field Embedded text field. Must match the collection embedding index.
#' @param model Exact embedding model name. Must match both corpus and query
#'   indexes.
#' @param aggregations Character vector of aggregation names chosen from
#'   `"max"`, `"mean"`, and `"top_k_mean"`.
#' @param top_k Number of top query scores to average when
#'   `aggregations` includes `"top_k_mean"`.
#' @param ref_ids Optional subset of collection `ref_id`s to score.
#' @param date_from Optional inclusive lower publication-date bound. Accepts
#'   values coercible by `as.Date()`.
#' @param date_to Optional inclusive upper publication-date bound. Accepts
#'   values coercible by `as.Date()`.
#' @param chunk_size Positive integer chunk size used when scoring large
#'   corpora. Smaller values reduce peak memory use.
#' @param include_query_scores Whether to include the full per-query score table.
#'
#' @return If `include_query_scores = FALSE`, a `data.table` with one row per
#'   `ref_id` and `category_id`. If `TRUE`, a list with `category_scores` and
#'   `query_scores`.
#' @export
litxr_score_collection_categories <- function(
  collection_id,
  query_set_id,
  config = NULL,
  field = "abstract",
  model,
  aggregations = c("max", "mean"),
  top_k = 3L,
  ref_ids = NULL,
  date_from = NULL,
  date_to = NULL,
  chunk_size = 256L,
  include_query_scores = FALSE
) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }
  aggregations <- unique(as.character(aggregations))
  aggregations <- aggregations[nzchar(aggregations)]
  valid_aggs <- c("max", "mean", "top_k_mean")
  if (!length(aggregations)) {
    aggregations <- "max"
  }
  if (any(!aggregations %in% valid_aggs)) {
    stop("Invalid aggregation(s): ", paste(setdiff(aggregations, valid_aggs), collapse = ", "), call. = FALSE)
  }
  chunk_size <- as.integer(chunk_size)
  if (is.na(chunk_size) || chunk_size <= 0L) {
    stop("`chunk_size` must be a positive integer.", call. = FALSE)
  }
  top_k <- as.integer(top_k)
  if (is.na(top_k) || top_k < 0L) {
    stop("`top_k` must be a non-negative integer.", call. = FALSE)
  }

  query_index <- .litxr_read_label_query_index(query_set_id, cfg, model)
  if (!nrow(query_index$metadata) || !nrow(query_index$matrix)) {
    out <- data.table::data.table()
    if (isTRUE(include_query_scores)) {
      return(list(category_scores = out, query_scores = out))
    }
    return(out)
  }
  query_meta <- query_index$metadata
  query_matrix <- query_index$matrix
  .litxr_assert_unit_normalized_matrix(query_matrix, context = "query embedding matrix")
  if (top_k == 0L && "top_k_mean" %in% aggregations) {
    aggregations <- setdiff(aggregations, "top_k_mean")
  }

  target_ref_ids <- if (!is.null(ref_ids)) {
    ref_ids <- unique(vapply(as.character(ref_ids), .litxr_llm_digest_index_key, character(1)))
    ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(ref_ids)]
    ref_ids
  } else {
    .litxr_select_score_ref_ids(
      collection_id = collection_id,
      cfg = cfg,
      ref_ids = NULL,
      date_from = date_from,
      date_to = date_to
    )
  }
  if (!length(target_ref_ids)) {
    out <- data.table::data.table()
    if (isTRUE(include_query_scores)) {
      return(list(category_scores = out, query_scores = out))
    }
    return(out)
  }

  parts <- .litxr_collect_embedding_score_parts(
    collection_id = collection_id,
    cfg = cfg,
    field = field,
    model = model,
    target_ref_ids = target_ref_ids,
    query_meta = query_meta,
    query_matrix = query_matrix,
    aggregations = aggregations,
    top_k = top_k,
    chunk_size = chunk_size,
    include_query_scores = include_query_scores
  )

  if (!isTRUE(include_query_scores)) {
    return(parts$category_scores)
  }
  list(category_scores = parts$category_scores, query_scores = parts$query_scores)
}

#' Score one collection for delta-backed embeddings
#'
#' Uses the main corpus plus delta shards only.
#'
#' @export
litxr_score_collection_categories_delta <- function(
  collection_id,
  query_set_id,
  config = NULL,
  field = "abstract",
  model,
  aggregations = c("max", "mean"),
  top_k = 3L,
  ref_ids = NULL,
  date_from = NULL,
  date_to = NULL,
  chunk_size = 256L,
  include_query_scores = FALSE
) {
  litxr_score_collection_categories(
    collection_id = collection_id,
    query_set_id = query_set_id,
    config = config,
    field = field,
    model = model,
    aggregations = aggregations,
    top_k = top_k,
    ref_ids = ref_ids,
    date_from = date_from,
    date_to = date_to,
    chunk_size = chunk_size,
    include_query_scores = include_query_scores
  )
}

#' Label one collection by category score threshold
#'
#' @export
litxr_label_collection_by_category <- function(
  collection_id,
  query_set_id,
  config = NULL,
  field = "abstract",
  model,
  threshold = 0.5,
  aggregations = c("max"),
  top_k = 3L,
  ref_ids = NULL,
  date_from = NULL,
  date_to = NULL,
  chunk_size = 256L
) {
  scores <- litxr_score_collection_categories(
    collection_id = collection_id,
    query_set_id = query_set_id,
    config = config,
    field = field,
    model = model,
    aggregations = aggregations,
    top_k = top_k,
    ref_ids = ref_ids,
    date_from = date_from,
    date_to = date_to,
    chunk_size = chunk_size,
    include_query_scores = FALSE
  )
  if (!nrow(scores)) {
    return(scores)
  }
  scores <- data.table::as.data.table(scores)
  if (!"score_max" %in% names(scores)) {
    scores$score_max <- NA_real_
  }
  scores[score_max >= threshold, ]
}

litxr_cosine_similarity <- function(query_vec, embedding_matrix) {
  if (!is.double(query_vec) || !length(query_vec)) {
    stop("Query vector must already be a non-empty double vector.", call. = FALSE)
  }
  if (!is.matrix(embedding_matrix) || !is.double(embedding_matrix)) {
    stop("Embedding matrix must already be a double matrix.", call. = FALSE)
  }
  if (length(query_vec) != ncol(embedding_matrix)) {
    stop("Query vector and embedding matrix have different dimensions.", call. = FALSE)
  }
  .litxr_assert_unit_normalized_query_vector(query_vec, context = "query vector")
  as.numeric(embedding_matrix %*% query_vec)
}

.litxr_resolve_query_vector <- function(query = NULL, embed_fun = NULL, query_vec = NULL) {
  if (!is.null(query_vec)) {
    if (!is.double(query_vec) || !length(query_vec)) {
      stop("`query_vec` must already be a non-empty double vector.", call. = FALSE)
    }
    return(query_vec)
  }
  if (!is.function(embed_fun)) {
    stop("`embed_fun` must be supplied when `query_vec` is missing.", call. = FALSE)
  }
  if (is.null(query) || !nzchar(as.character(query))) {
    stop("`query` must be supplied when `query_vec` is missing.", call. = FALSE)
  }
  query_vec <- .litxr_as_embedding_matrix(embed_fun(as.character(query)))
  if (nrow(query_vec) != 1L) {
    stop("`embed_fun` must return a single query embedding.", call. = FALSE)
  }
  query_vec[1L, ]
}

.litxr_normalize_label_query_set <- function(query_set) {
  if (is.character(query_set) && length(query_set) == 1L && file.exists(query_set)) {
    ext <- tolower(tools::file_ext(query_set))
    query_set <- switch(
      ext,
      json = jsonlite::fromJSON(query_set, simplifyVector = FALSE),
      yml = yaml::read_yaml(query_set),
      yaml = yaml::read_yaml(query_set),
      stop("Unsupported label query set file extension: ", ext, call. = FALSE)
    )
  }

  if (!is.list(query_set) || !length(query_set) || is.null(names(query_set)) || any(!nzchar(names(query_set)))) {
    stop("`query_set` must be a named list or a JSON/YAML file path containing a named list.", call. = FALSE)
  }

  rows <- unlist(lapply(names(query_set), function(category_id) {
    queries <- as.character(unlist(query_set[[category_id]], use.names = FALSE))
    queries <- queries[!is.na(queries) & nzchar(trimws(queries))]
    if (!length(queries)) {
      return(list())
    }
    lapply(seq_along(queries), function(i) {
      data.table::data.table(
        category_id = as.character(category_id),
        query_id = paste0(as.character(category_id), "__", sprintf("%03d", i)),
        query_order = as.integer(i),
        query_text = trimws(queries[[i]])
      )
    })
  }), recursive = FALSE)

  if (!length(rows)) {
    return(.litxr_empty_label_query_metadata()[, c("category_id", "query_id", "query_order", "query_text"), with = FALSE])
  }
  data.table::rbindlist(rows, fill = TRUE)
}

.litxr_label_query_spec_from_metadata <- function(metadata) {
  metadata <- data.table::as.data.table(metadata)
  if (!nrow(metadata)) {
    return(list())
  }
  category_ids <- unique(as.character(metadata$category_id))
  category_ids <- category_ids[nzchar(category_ids)]
  out <- setNames(lapply(category_ids, function(category_id) {
    dt <- metadata[metadata$category_id == category_id, ]
    dt <- dt[order(dt$query_order), ]
    as.character(dt[["query_text"]])
  }), category_ids)
  out
}

.litxr_label_query_root_paths <- function(cfg, query_set_id) {
  root_dir <- file.path(
    .litxr_project_embeddings_dir(cfg),
    "label_queries",
    .litxr_embedding_slug(query_set_id)
  )
  list(
    dir = root_dir,
    query_set = file.path(root_dir, "query_set.yaml")
  )
}

.litxr_label_query_index_paths <- function(cfg, query_set_id, model) {
  root_paths <- .litxr_label_query_root_paths(cfg, query_set_id)
  base_dir <- file.path(
    root_paths$dir,
    .litxr_embedding_slug(model)
  )
  list(
    dir = base_dir,
    metadata = file.path(base_dir, "metadata.fst"),
    matrix = file.path(base_dir, "matrix.rds"),
    matrix_f32 = file.path(base_dir, "matrix.f32"),
    manifest = file.path(base_dir, "manifest.json"),
    query_set = root_paths$query_set
  )
}

.litxr_empty_label_query_metadata <- function() {
  data.table::data.table(
    category_id = character(),
    query_id = character(),
    query_order = integer(),
    query_text = character()
  )
}

.litxr_read_label_query_index_parts <- function(paths, read_matrix = TRUE) {
  metadata <- if (file.exists(paths$metadata)) {
    fst::read_fst(paths$metadata, as.data.table = TRUE)
  } else {
    .litxr_empty_label_query_metadata()
  }
  matrix_data <- if (isTRUE(read_matrix) && file.exists(paths$matrix_f32)) {
    .litxr_read_float32_matrix(
      path = paths$matrix_f32,
      nrow = nrow(metadata),
      ncol = as.integer((jsonlite::fromJSON(paths$manifest, simplifyVector = FALSE)$dimension %||% 0L)),
      context = paths$dir
    )
  } else if (isTRUE(read_matrix) && file.exists(paths$matrix)) {
    tryCatch(
      readRDS(paths$matrix),
      error = function(e) {
        stop("Failed to read label query matrix cache at ", paths$matrix, ": ", conditionMessage(e), call. = FALSE)
      }
    )
  } else if (isTRUE(read_matrix)) {
    matrix(numeric(), nrow = 0L, ncol = 0L)
  } else {
    NULL
  }
  if (!is.null(matrix_data)) {
    matrix_data <- as.matrix(matrix_data)
    storage.mode(matrix_data) <- "double"
    if (nrow(metadata) != nrow(matrix_data)) {
      stop("Label query metadata rows do not match matrix rows in cache: ", paths$dir, call. = FALSE)
    }
  }
  manifest <- if (file.exists(paths$manifest)) {
    jsonlite::fromJSON(paths$manifest, simplifyVector = FALSE)
  } else {
    list()
  }
  list(metadata = metadata, matrix = matrix_data, manifest = manifest)
}

.litxr_write_label_query_index <- function(paths, metadata, matrix, manifest) {
  dir.create(paths$dir, recursive = TRUE, showWarnings = FALSE)
  .litxr_write_fst_atomic(as.data.frame(metadata), paths$metadata)
  .litxr_write_float32_matrix_atomic(matrix, paths$matrix_f32)
  if (file.exists(paths$matrix)) unlink(paths$matrix)
  .litxr_write_json_atomic(manifest, paths$manifest)
  if (!is.null(paths$query_set)) {
    .litxr_write_yaml_atomic(.litxr_label_query_spec_from_metadata(metadata), paths$query_set)
  }
  invisible(paths)
}

.litxr_read_label_query_index <- function(query_set_id, cfg, model) {
  if (missing(model) || !nzchar(as.character(model))) {
    stop("`model` must be supplied and non-empty.", call. = FALSE)
  }
  .litxr_read_label_query_index_parts(
    .litxr_label_query_index_paths(cfg, query_set_id, model),
    read_matrix = TRUE
  )
}

.litxr_label_query_available_models <- function(cfg, query_set_id) {
  query_root <- file.path(
    .litxr_project_embeddings_dir(cfg),
    "label_queries",
    .litxr_embedding_slug(query_set_id)
  )
  if (!dir.exists(query_root)) {
    return(character())
  }
  model_dirs <- list.dirs(query_root, recursive = FALSE, full.names = TRUE)
  if (!length(model_dirs)) {
    return(character())
  }
  models <- vapply(model_dirs, function(path) {
    manifest_path <- file.path(path, "manifest.json")
    if (file.exists(manifest_path)) {
      manifest <- jsonlite::fromJSON(manifest_path, simplifyVector = FALSE)
      return(as.character(manifest$model %||% basename(path)))
    }
    basename(path)
  }, character(1))
  unique(models[nzchar(models)])
}

.litxr_warn_label_query_model_mismatch <- function(cfg, query_set_id, requested_model) {
  available_models <- .litxr_label_query_available_models(cfg, query_set_id)
  available_models <- setdiff(available_models, as.character(requested_model))
  if (!length(available_models)) {
    return(invisible(NULL))
  }
  warning(
    "No cached category query index found for `query_set_id = \"", query_set_id,
    "\"` under model `", requested_model, "`. Available cached models for this query set: ",
    paste(sort(available_models), collapse = ", "),
    ". Rebuild the query cache with the requested model or use a matching model.",
    call. = FALSE
  )
  invisible(NULL)
}

.litxr_parse_optional_date <- function(x, arg_name) {
  if (is.null(x)) {
    return(NULL)
  }
  out <- as.Date(x[[1]])
  if (is.na(out)) {
    stop(arg_name, " must be coercible by `as.Date()`.", call. = FALSE)
  }
  out
}

.litxr_select_score_ref_ids <- function(collection_id, cfg, ref_ids = NULL, date_from = NULL, date_to = NULL) {
  out <- .litxr_embedding_target_rows_from_thin_ref_stores(cfg, collection_id)
  if (!nrow(out)) {
    return(character())
  }
  out <- out[!is.na(out$ref_id) & nzchar(out$ref_id), ]
  if (!nrow(out)) {
    return(character())
  }
  out$ref_key <- vapply(out$ref_id, .litxr_llm_digest_index_key, character(1))
  if (!is.null(ref_ids)) {
    ref_keys <- vapply(as.character(ref_ids), .litxr_llm_digest_index_key, character(1))
    ref_keys <- ref_keys[!is.na(ref_keys) & nzchar(ref_keys)]
    if (!length(ref_keys)) {
      return(character())
    }
    out <- out[out$ref_key %in% ref_keys, ]
  }
  date_from <- .litxr_parse_optional_date(date_from, "`date_from`")
  date_to <- .litxr_parse_optional_date(date_to, "`date_to`")
  if (!is.null(date_from) && !is.null(date_to) && date_to < date_from) {
    stop("`date_to` must be on or after `date_from`.", call. = FALSE)
  }
  if (!is.null(date_from) || !is.null(date_to)) {
    out <- .litxr_hydrate_rows_from_json_paths(
      out[, c("ref_id", "json_path"), with = FALSE],
      out[, c("ref_id", "json_path"), with = FALSE],
      fields = "pub_date"
    )
    if (!nrow(out) || !("pub_date" %in% names(out))) {
      return(character())
    }
    pub_dates <- as.Date(out$pub_date)
    keep <- !is.na(pub_dates)
    if (!is.null(date_from)) keep <- keep & pub_dates >= date_from
    if (!is.null(date_to)) keep <- keep & pub_dates <= date_to
    out <- out[keep, ]
  }
  unique(as.character(out$ref_id))
}

.litxr_read_embedding_shard_part <- function(paths, shard_key, read_matrix = TRUE) {
  shard_paths <- .litxr_embedding_shard_paths(paths, shard_key)
  shard_manifest <- if (file.exists(shard_paths$manifest)) jsonlite::fromJSON(shard_paths$manifest, simplifyVector = FALSE) else list()
  top_manifest <- if (file.exists(paths$manifest)) jsonlite::fromJSON(paths$manifest, simplifyVector = FALSE) else list()
  metadata <- if (file.exists(shard_paths$metadata)) {
    .litxr_normalize_embedding_metadata(fst::read_fst(shard_paths$metadata, as.data.table = TRUE))
  } else {
    .litxr_empty_embedding_metadata()
  }
  matrix_data <- if (!isTRUE(read_matrix)) {
    NULL
  } else if (file.exists(shard_paths$matrix_f32)) {
    .litxr_read_float32_matrix(
      path = shard_paths$matrix_f32,
      nrow = nrow(metadata),
      ncol = as.integer(shard_manifest$dimension %||% top_manifest$dimension %||% 0L),
      context = shard_paths$dir
    )
  } else {
    matrix(numeric(), nrow = 0L, ncol = as.integer(shard_manifest$dimension %||% top_manifest$dimension %||% 0L))
  }
  list(metadata = metadata, matrix = matrix_data, manifest = shard_manifest)
}

.litxr_top_n_scored_candidates <- function(metadata, scores = NULL, top_n) {
  top_n <- as.integer(top_n)
  if (is.na(top_n) || top_n < 0L) {
    stop("`top_n` must be a non-negative integer.", call. = FALSE)
  }

  metadata <- data.table::as.data.table(metadata)
  if (!nrow(metadata)) {
    return(metadata[0, ])
  }
  if (is.null(scores)) {
    if (!"score" %in% names(metadata)) {
      return(metadata[0, ])
    }
    scores <- metadata[["score"]]
  }
  if (!length(scores)) {
    return(metadata[0, ])
  }
  if (length(scores) != nrow(metadata)) {
    stop("Internal score count mismatch in embedding candidate ranking.", call. = FALSE)
  }
  scores <- as.numeric(scores)
  keep <- which(!is.na(scores))
  if (!length(keep) || top_n == 0L) {
    return(metadata[0, ])
  }
  if ("ref_id" %in% names(metadata)) {
    rank_dt <- data.table::data.table(
      row_idx = keep,
      ref_id = as.character(metadata$ref_id[keep]),
      score = scores[keep]
    )
    data.table::setorderv(rank_dt, c("score", "ref_id"), c(-1L, 1L))
    rank_dt <- rank_dt[!duplicated(rank_dt$ref_id), , drop = FALSE]
  } else {
    rank_dt <- data.table::data.table(
      row_idx = keep,
      score = scores[keep]
    )
    data.table::setorderv(rank_dt, "score", -1L)
  }
  if (nrow(rank_dt) > top_n) {
    rank_dt <- rank_dt[seq_len(top_n), , drop = FALSE]
  }
  out <- data.table::copy(metadata[rank_dt$row_idx, , drop = FALSE])
  out[["score"]] <- rank_dt$score
  out
}

.litxr_score_corpus_chunk <- function(corpus_meta, corpus_matrix, query_meta, query_matrix, aggregations, top_k, include_query_scores = FALSE) {
  if (!nrow(corpus_meta) || !nrow(corpus_matrix)) {
    return(list(category_scores = data.table::data.table(), query_scores = data.table::data.table()))
  }
  if (ncol(corpus_matrix) != ncol(query_matrix)) {
    stop("Collection and query embedding dimensions do not match.", call. = FALSE)
  }
  score_matrix <- .litxr_cosine_similarity_matrix(corpus_matrix, query_matrix)
  query_score_dt <- if (isTRUE(include_query_scores)) {
    .litxr_query_score_table(corpus_meta, query_meta, score_matrix)
  } else {
    data.table::data.table()
  }
  category_scores <- .litxr_aggregate_category_scores_from_matrix(
    corpus_meta = corpus_meta,
    query_meta = query_meta,
    score_matrix = score_matrix,
    aggregations = aggregations,
    top_k = top_k
  )
  list(category_scores = category_scores, query_scores = query_score_dt)
}

.litxr_aggregate_category_scores_from_matrix <- function(corpus_meta, query_meta, score_matrix, aggregations = c("max", "mean"), top_k = 3L) {
  if (!nrow(corpus_meta) || !nrow(score_matrix) || !nrow(query_meta)) {
    return(data.table::data.table())
  }
  query_categories <- as.character(query_meta$category_id)
  category_indices <- split(seq_along(query_categories), query_categories)
  category_ids <- names(category_indices)
  rows <- vector("list", length(category_indices))
  for (i in seq_along(category_indices)) {
    category_id <- category_ids[[i]]
    idx <- category_indices[[i]]
    category_scores <- score_matrix[, idx, drop = FALSE]
    values <- list(
      ref_id = as.character(corpus_meta$ref_id),
      category_id = rep(category_id, nrow(corpus_meta)),
      query_n = length(idx)
    )
    if ("title" %in% names(corpus_meta)) values[["title"]] <- as.character(corpus_meta$title)
    if ("year" %in% names(corpus_meta)) values[["year"]] <- as.integer(corpus_meta$year)
    if ("max" %in% aggregations) values[["score_max"]] <- matrixStats::rowMaxs(category_scores, useNames = FALSE)
    if ("mean" %in% aggregations) values[["score_mean"]] <- matrixStats::rowMeans2(category_scores, useNames = FALSE)
    if ("top_k_mean" %in% aggregations) {
      values[["score_top_k_mean"]] <- .litxr_top_k_mean_rows(category_scores, top_k)
    }
    rows[[i]] <- data.table::as.data.table(values)
  }
  data.table::rbindlist(rows, fill = TRUE)
}

.litxr_top_k_mean_rows <- function(x, top_k) {
  if (!nrow(x) || !ncol(x)) {
    return(numeric(nrow(x)))
  }
  top_k <- as.integer(top_k)
  if (is.na(top_k) || top_k <= 0L) {
    return(rep(NA_real_, nrow(x)))
  }
  top_k <- min(top_k, ncol(x))
  if (top_k == 1L) {
    return(matrixStats::rowOrderStats(x, which = ncol(x), useNames = FALSE))
  }
  top_vals <- matrix(NA_real_, nrow = nrow(x), ncol = top_k)
  for (k in seq_len(top_k)) {
    top_vals[, k] <- matrixStats::rowOrderStats(-x, which = k, useNames = FALSE)
  }
  -matrixStats::rowMeans2(top_vals, useNames = FALSE)
}

.litxr_collect_embedding_score_parts <- function(collection_id, cfg, field, model, target_ref_ids, query_meta, query_matrix, aggregations, top_k, chunk_size, include_query_scores) {
  paths <- .litxr_embedding_index_paths(cfg, collection_id, field, model)
  target_ref_keys <- unique(vapply(as.character(target_ref_ids), .litxr_llm_digest_index_key, character(1)))
  target_ref_keys <- target_ref_keys[!is.na(target_ref_keys) & nzchar(target_ref_keys)]
  if (!length(target_ref_keys)) {
    out <- data.table::data.table()
    return(list(category_scores = out, query_scores = out))
  }

  category_parts <- list()
  query_parts <- list()

  append_part <- function(part) {
    if (nrow(part$category_scores)) {
      category_parts[[length(category_parts) + 1L]] <<- part$category_scores
    }
    if (isTRUE(include_query_scores) && nrow(part$query_scores)) {
      query_parts[[length(query_parts) + 1L]] <<- part$query_scores
    }
  }

  if (.litxr_embedding_has_shards(paths)) {
    shard_keys <- .litxr_score_target_shard_keys(target_ref_keys, .litxr_embedding_shard_keys(paths))
    shard_keys <- shard_keys[nzchar(shard_keys)]
    for (key in shard_keys) {
      shard <- .litxr_read_embedding_shard_part(paths, key, read_matrix = TRUE)
      if (!nrow(shard$metadata) || !nrow(shard$matrix)) next
      shard_keys <- vapply(as.character(shard$metadata$ref_id), .litxr_llm_digest_index_key, character(1))
      keep <- shard_keys %in% target_ref_keys
      if (!any(keep)) next
      shard_meta <- shard$metadata[keep, ]
      shard_matrix <- shard$matrix[keep, , drop = FALSE]
      .litxr_assert_unit_normalized_matrix(shard_matrix, context = paste0("embedding score shard ", key))
      for (start in seq(1L, nrow(shard_meta), by = chunk_size)) {
        end <- min(start + chunk_size - 1L, nrow(shard_meta))
        append_part(.litxr_score_corpus_chunk(
          corpus_meta = shard_meta[start:end, ],
          corpus_matrix = shard_matrix[start:end, , drop = FALSE],
          query_meta = query_meta,
          query_matrix = query_matrix,
          aggregations = aggregations,
          top_k = top_k,
          include_query_scores = include_query_scores
        ))
      }
    }
  } else {
    corpus <- litxr_read_embedding_index(collection_id, cfg, field = field, model = model)
    if (nrow(corpus$metadata) && nrow(corpus$matrix)) {
      corpus_keys <- vapply(as.character(corpus$metadata$ref_id), .litxr_llm_digest_index_key, character(1))
      keep <- corpus_keys %in% target_ref_keys
      corpus_meta <- corpus$metadata[keep, ]
      corpus_matrix <- corpus$matrix[keep, , drop = FALSE]
      .litxr_assert_unit_normalized_matrix(corpus_matrix, context = "embedding score corpus")
      for (start in seq(1L, nrow(corpus_meta), by = chunk_size)) {
        end <- min(start + chunk_size - 1L, nrow(corpus_meta))
        append_part(.litxr_score_corpus_chunk(
          corpus_meta = corpus_meta[start:end, ],
          corpus_matrix = corpus_matrix[start:end, , drop = FALSE],
          query_meta = query_meta,
          query_matrix = query_matrix,
          aggregations = aggregations,
          top_k = top_k,
          include_query_scores = include_query_scores
        ))
      }
    }
  }

  list(
    category_scores = if (length(category_parts)) data.table::rbindlist(category_parts, fill = TRUE) else data.table::data.table(),
    query_scores = if (isTRUE(include_query_scores) && length(query_parts)) data.table::rbindlist(query_parts, fill = TRUE) else data.table::data.table()
  )
}

.litxr_score_target_shard_keys <- function(ref_ids, available_shard_keys) {
  available_shard_keys <- unique(as.character(available_shard_keys))
  available_shard_keys <- available_shard_keys[!is.na(available_shard_keys) & nzchar(available_shard_keys)]
  if (!length(available_shard_keys)) {
    return(character())
  }

  ref_ids <- unique(as.character(ref_ids))
  ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(ref_ids)]
  if (!length(ref_ids)) {
    return(character())
  }

  arxiv_like <- grepl("^[0-9]{4}\\.[0-9]{4,5}$", ref_ids)
  if (all(arxiv_like)) {
    yy <- substr(ref_ids, 1L, 2L)
    yy_int <- suppressWarnings(as.integer(yy))
    shard_keys <- ifelse(is.na(yy_int), NA_character_, paste0(ifelse(yy_int >= 90L, "19", "20"), yy))
    shard_keys <- unique(shard_keys[!is.na(shard_keys) & nzchar(shard_keys)])
    return(intersect(shard_keys, available_shard_keys))
  }

  available_shard_keys
}

.litxr_label_query_existing_dimension <- function(parts) {
  if (!is.null(parts$matrix) && nrow(parts$matrix)) {
    return(ncol(parts$matrix))
  }
  manifest_dimension <- suppressWarnings(as.integer(parts$manifest$dimension %||% NA_integer_))
  if (!is.na(manifest_dimension) && manifest_dimension > 0L) {
    return(manifest_dimension)
  }
  NA_integer_
}

.litxr_cosine_similarity_matrix <- function(embedding_matrix, query_matrix) {
  if (!is.matrix(embedding_matrix) || !is.matrix(query_matrix)) {
    stop("Embedding matrix and query matrix must already be matrices.", call. = FALSE)
  }
  if (!is.double(embedding_matrix) || !is.double(query_matrix)) {
    stop("Embedding matrix and query matrix must already be double matrices.", call. = FALSE)
  }
  if (ncol(embedding_matrix) != ncol(query_matrix)) {
    stop("Embedding matrix and query matrix have different dimensions.", call. = FALSE)
  }
  tcrossprod(embedding_matrix, query_matrix)
}

.litxr_assert_unit_normalized_matrix <- function(x, context, tolerance = 1e-3, max_rows = 32L) {
  if (!is.matrix(x) || !is.double(x)) {
    stop("`", context, "` must already be a double matrix.", call. = FALSE)
  }
  n <- nrow(x)
  if (!n) {
    return(invisible(TRUE))
  }
  max_rows <- as.integer(max_rows)
  if (is.na(max_rows) || max_rows <= 0L) {
    max_rows <- 32L
  }
  idx <- seq_len(min(n, max_rows))
  if (n > max_rows) {
    idx <- unique(c(idx, seq.int(max(1L, n - max_rows + 1L), n)))
  }
  sample <- x[idx, , drop = FALSE]
  norms <- sqrt(matrixStats::rowSums2(sample * sample, useNames = FALSE))
  bad <- which(abs(norms - 1) > tolerance)
  if (length(bad)) {
    stop(
      "Embedding matrix in ", context, " is not unit-normalized. ",
      "Row sample indices: ", paste(idx[bad], collapse = ", "),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

.litxr_assert_unit_normalized_query_vector <- function(x, context, tolerance = 1e-3) {
  if (!is.double(x) || !length(x)) {
    stop("`", context, "` must already be a non-empty double vector.", call. = FALSE)
  }
  norm <- sqrt(sum(x * x))
  if (!is.finite(norm) || abs(norm - 1) > tolerance) {
    stop("Query vector in ", context, " is not unit-normalized.", call. = FALSE)
  }
  invisible(TRUE)
}

.litxr_query_score_table <- function(corpus_meta, query_meta, score_matrix) {
  score_dt <- data.table::data.table(
    corpus_row = rep(seq_len(nrow(score_matrix)), times = ncol(score_matrix)),
    query_row = rep(seq_len(ncol(score_matrix)), each = nrow(score_matrix)),
    score = as.numeric(score_matrix)
  )

  corpus_map <- data.table::copy(corpus_meta)
  corpus_map[["corpus_row"]] <- seq_len(nrow(corpus_map))
  query_map <- data.table::copy(query_meta)
  query_map[["query_row"]] <- seq_len(nrow(query_map))

  out <- merge(score_dt, corpus_map, by = "corpus_row", all.x = TRUE, sort = FALSE)
  out <- merge(
    out,
    query_map[, c("query_row", "category_id", "query_id", "query_order", "query_text"), with = FALSE],
    by = "query_row",
    all.x = TRUE,
    sort = FALSE
  )
  keep_cols <- intersect(
    c("ref_id", "title", "year", "category_id", "query_id", "query_order", "query_text", "score"),
    names(out)
  )
  out[, keep_cols, with = FALSE]
}

.litxr_threshold_by_category <- function(category_id, threshold) {
  if (length(threshold) == 1L && is.null(names(threshold))) {
    value <- as.numeric(threshold[[1]])
    if (is.na(value)) stop("`threshold` must be numeric.", call. = FALSE)
    return(rep(value, length(category_id)))
  }
  threshold_names <- names(threshold)
  if (is.null(threshold_names) || any(!nzchar(threshold_names))) {
    stop("`threshold` must be a scalar or a named numeric vector keyed by `category_id`.", call. = FALSE)
  }
  threshold <- stats::setNames(as.numeric(threshold), threshold_names)
  out <- unname(threshold[as.character(category_id)])
  out[is.na(out)] <- 0
  out
}
