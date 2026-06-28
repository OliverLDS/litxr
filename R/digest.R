 .litxr_empty_llm_digest_index <- function() {
  data.table::data.table(
    ref_id = character(),
    json_filename = character(),
    history_dir = character()
  )
}

.litxr_llm_digest_index_row <- function(cfg, ref_id, json_filename = NULL, history_dir = NULL) {
  key <- .litxr_llm_digest_index_key(ref_id)
  if (is.na(key) || !nzchar(key)) {
    return(NULL)
  }
  json_filename <- if (!is.null(json_filename) && length(json_filename)) as.character(json_filename[[1L]]) else NA_character_
  if (is.na(json_filename) || !nzchar(trimws(json_filename))) {
    json_filename <- basename(.litxr_llm_digest_path(cfg, ref_id))
  }
  history_dir <- if (!is.null(history_dir) && length(history_dir)) as.character(history_dir[[1L]]) else NA_character_
  if (is.na(history_dir) || !nzchar(trimws(history_dir))) {
    history_dir <- NA_character_
  }
  data.table::data.table(
    ref_id = key,
    json_filename = json_filename,
    history_dir = history_dir
  )
}

.litxr_bootstrap_llm_digest_index <- function(cfg) {
  llm_dir <- .litxr_project_llm_dir(cfg)
  if (!dir.exists(llm_dir)) {
    return(.litxr_empty_llm_digest_index())
  }
  files <- list.files(llm_dir, pattern = "\\.json$", full.names = TRUE)
  if (!length(files)) {
    return(.litxr_empty_llm_digest_index())
  }
  rows <- lapply(files, function(path) {
    digest <- tryCatch(
      .litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE)),
      error = function(e) NULL
    )
    if (is.null(digest) || is.null(digest$ref_id)) {
      return(NULL)
    }
    ref_id <- .litxr_llm_digest_index_key(digest$ref_id)
    if (is.na(ref_id) || !nzchar(ref_id)) {
      return(NULL)
    }
    history_dir <- .litxr_llm_history_ref_dir(cfg, digest$ref_id)
    data.table::data.table(
      ref_id = ref_id,
      json_filename = basename(path),
      history_dir = if (dir.exists(history_dir)) basename(history_dir) else NA_character_
    )
  })
  rows <- rows[vapply(rows, is.null, logical(1L)) == FALSE]
  if (!length(rows)) {
    return(.litxr_empty_llm_digest_index())
  }
  out <- data.table::rbindlist(rows, fill = TRUE)
  out <- out[!is.na(out$ref_id) & nzchar(out$ref_id), ]
  if (!nrow(out)) {
    return(.litxr_empty_llm_digest_index())
  }
  out <- out[!duplicated(out$ref_id, fromLast = TRUE), ]
  .litxr_write_llm_digest_index(cfg, out)
  out
}

.litxr_read_llm_digest_index <- function(cfg) {
  path <- .litxr_project_llm_digest_index_path(cfg)
  if (!file.exists(path)) {
    return(.litxr_bootstrap_llm_digest_index(cfg))
  }
  rows <- tryCatch(
    fst::read_fst(path, as.data.table = TRUE),
    error = function(e) .litxr_empty_llm_digest_index()
  )
  rows <- data.table::as.data.table(rows)
  required <- c("ref_id", "json_filename", "history_dir")
  if (!nrow(rows) || !all(required %in% names(rows))) {
    return(.litxr_bootstrap_llm_digest_index(cfg))
  }
  rows <- rows[, required, with = FALSE]
  rows$ref_id <- vapply(rows$ref_id, .litxr_llm_digest_index_key, character(1))
  rows$json_filename <- as.character(rows$json_filename)
  rows$history_dir <- as.character(rows$history_dir)
  rows <- rows[!is.na(rows$ref_id) & nzchar(rows$ref_id), ]
  if (!nrow(rows)) {
    return(.litxr_empty_llm_digest_index())
  }
  rows <- rows[!duplicated(rows$ref_id, fromLast = TRUE), ]
  rows
}

.litxr_llm_digest_index_lookup <- function(cfg, ref_id) {
  key <- .litxr_llm_digest_index_key(ref_id)
  if (is.na(key) || !nzchar(key)) {
    return(NULL)
  }
  rows <- .litxr_read_llm_digest_index(cfg)
  if (!nrow(rows)) {
    return(NULL)
  }
  hit <- match(key, rows$ref_id)
  if (is.na(hit)) {
    return(NULL)
  }
  rows[hit, ]
}

.litxr_write_llm_digest_index <- function(cfg, rows) {
  path <- .litxr_project_llm_digest_index_path(cfg)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  rows <- data.table::as.data.table(rows)
  if (!nrow(rows)) {
    fst::write_fst(.litxr_empty_llm_digest_index(), path)
    return(invisible(path))
  }
  required <- c("ref_id", "json_filename", "history_dir")
  missing <- setdiff(required, names(rows))
  if (length(missing)) {
    stop("LLM digest index rows are missing columns: ", paste(missing, collapse = ", "), call. = FALSE)
  }
  rows <- rows[, required, with = FALSE]
  rows$ref_id <- vapply(rows$ref_id, .litxr_llm_digest_index_key, character(1))
  rows$json_filename <- as.character(rows$json_filename)
  rows$history_dir <- as.character(rows$history_dir)
  rows <- rows[!is.na(rows$ref_id) & nzchar(rows$ref_id), ]
  rows <- rows[!duplicated(rows$ref_id, fromLast = TRUE), ]
  fst::write_fst(rows, path)
  invisible(path)
}

.litxr_upsert_llm_digest_index <- function(cfg, ref_id, json_filename = NULL, history_dir = NULL) {
  rows <- .litxr_read_llm_digest_index(cfg)
  row <- .litxr_llm_digest_index_row(cfg, ref_id, json_filename = json_filename, history_dir = history_dir)
  if (is.null(row) || !nrow(row)) {
    return(invisible(rows))
  }
  hit <- match(row$ref_id[[1L]], rows$ref_id)
  if (is.na(hit)) {
    rows <- data.table::rbindlist(list(rows, row), fill = TRUE)
  } else {
    data.table::set(rows, i = hit, j = "json_filename", value = row$json_filename[[1L]])
    if (!is.na(row$history_dir[[1L]]) && nzchar(row$history_dir[[1L]])) {
      data.table::set(rows, i = hit, j = "history_dir", value = row$history_dir[[1L]])
    }
  }
  rows <- rows[!is.na(rows$ref_id) & nzchar(rows$ref_id), ]
  rows <- rows[!duplicated(rows$ref_id, fromLast = TRUE), ]
  .litxr_write_llm_digest_index(cfg, rows)
  invisible(rows)
}

.litxr_llm_digest_current_path <- function(cfg, ref_id) {
  row <- .litxr_llm_digest_index_lookup(cfg, ref_id)
  if (!is.null(row) && nrow(row)) {
    json_filename <- as.character(row$json_filename[[1L]])
    if (!is.na(json_filename) && nzchar(json_filename)) {
      return(file.path(.litxr_project_llm_dir(cfg), basename(json_filename)))
    }
  }
  .litxr_llm_digest_path(cfg, ref_id)
}

litxr_llm_digest_template <- function(ref_id, schema_version = "v2") {
  if (!identical(schema_version, "v2") && !identical(schema_version, "v3") && !identical(schema_version, "v4")) {
    stop("Only `schema_version = \"v2\"`, `\"v3\"`, or `\"v4\"` is supported for new templates in the current package version.", call. = FALSE)
  }
  if (identical(schema_version, "v4")) {
    return(.litxr_llm_digest_template_v4(ref_id))
  }
  if (identical(schema_version, "v3")) {
    return(.litxr_llm_digest_template_v3(ref_id))
  }
  .litxr_llm_digest_template_v2(ref_id)
}

litxr_write_llm_digest <- function(ref_id, digest, config = NULL, keep_history = TRUE, bump_revision = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  refs <- .litxr_read_normalized_reference_rows_by_keys(cfg, .litxr_expand_reference_keys(ref_id))
  if (nrow(refs) && !(ref_id %in% refs$ref_id)) {
    warning("Reference id not found in canonical store: ", ref_id, call. = FALSE)
  }

  existing <- litxr_read_llm_digest(ref_id, cfg)
  payload <- .litxr_normalize_llm_digest_for_write(digest, ref_id = ref_id)
  if (identical(.litxr_llm_digest_schema_version(payload), "v2") || identical(.litxr_llm_digest_schema_version(payload), "v3") || identical(.litxr_llm_digest_schema_version(payload), "v4")) {
    if (is.null(existing)) {
      existing_revision <- 0L
    } else if (!identical(.litxr_llm_digest_schema_version(existing), .litxr_llm_digest_schema_version(payload))) {
      existing_revision <- 1L
    } else {
      existing_revision <- suppressWarnings(as.integer(existing$digest_revision %||% 1L))
      if (!length(existing_revision) || is.na(existing_revision[[1]]) || existing_revision[[1]] < 1L) {
        existing_revision <- 1L
      } else {
        existing_revision <- existing_revision[[1]]
      }
    }
    if (is.null(existing)) {
      payload$digest_revision <- 1L
      payload$derived_from_revision <- NULL
      payload$generated_at <- as.character(.litxr_first_nonnull(payload$generated_at, format(Sys.time(), tz = "UTC", usetz = TRUE)))
    } else if (isTRUE(bump_revision)) {
      payload$digest_revision <- existing_revision + 1L
      payload$derived_from_revision <- existing_revision
      payload$generated_at <- as.character(.litxr_first_nonnull(payload$generated_at, format(Sys.time(), tz = "UTC", usetz = TRUE)))
    } else {
      payload$digest_revision <- existing_revision
      payload$derived_from_revision <- existing$derived_from_revision %||% NULL
      payload$generated_at <- as.character(.litxr_first_nonnull(payload$generated_at, existing$generated_at, format(Sys.time(), tz = "UTC", usetz = TRUE)))
    }
    payload$updated_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  }
  litxr_validate_llm_digest(payload)

  path <- .litxr_llm_digest_path(cfg, ref_id)
  .litxr_ensure_project_llm_dir(cfg)
  if (!is.null(existing) && isTRUE(keep_history)) {
    .litxr_ensure_project_llm_history_dir(cfg)
    history_dir <- .litxr_llm_history_ref_dir(cfg, ref_id)
    dir.create(history_dir, recursive = TRUE, showWarnings = FALSE)
    history_path <- .litxr_llm_digest_history_path(cfg, ref_id, existing)
    .litxr_write_json_atomic(existing, history_path)
  }
  .litxr_write_json_atomic(payload, path)
  .litxr_upsert_llm_digest_index(
    cfg,
    ref_id,
    json_filename = basename(path),
    history_dir = if (isTRUE(keep_history) && !is.null(existing)) basename(.litxr_llm_history_ref_dir(cfg, ref_id)) else NULL
  )
  .litxr_update_enrichment_status_ref(cfg, ref_id)
  invisible(path)
}

litxr_build_llm_digest <- function(ref_id, builder, config = NULL, overwrite = FALSE, write = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  if (!is.function(builder)) {
    stop("`builder` must be a function.", call. = FALSE)
  }

  existing <- litxr_read_llm_digest(ref_id, cfg)
  if (!is.null(existing) && !isTRUE(overwrite)) {
    stop("LLM digest already exists for ref_id: ", ref_id, ". Set `overwrite = TRUE` to replace it.", call. = FALSE)
  }

  refs <- litxr_read_references(cfg)
  ref_match <- refs[refs$ref_id == ref_id, ]
  if (nrow(ref_match)) {
    ref_match <- ref_match[1L, ]
  }
  if (!nrow(ref_match)) {
    stop("Reference id not found in canonical store: ", ref_id, call. = FALSE)
  }

  markdown <- litxr_read_md(ref_id, cfg)
  if (is.null(markdown)) {
    stop("Markdown not found for ref_id: ", ref_id, call. = FALSE)
  }

  template <- litxr_llm_digest_template(ref_id)
  built <- builder(ref = ref_match[1, ], markdown = markdown, template = template)
  if (is.null(built) || !is.list(built)) {
    stop("`builder` must return a named list of digest fields.", call. = FALSE)
  }

  payload <- .litxr_normalize_llm_digest_for_write(built, ref_id = ref_id)
  litxr_validate_llm_digest(payload)

  if (isTRUE(write)) {
    litxr_write_llm_digest(ref_id, payload, cfg)
  }
  payload
}

litxr_build_llm_digests <- function(builder, config = NULL, ref_ids = NULL, overwrite = FALSE, limit = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  targets <- if (!is.null(ref_ids) && length(ref_ids)) {
    unique(as.character(ref_ids))
  } else {
    status <- litxr_read_enrichment_status(cfg)
    status <- status[status$has_md & (overwrite | !status$has_llm_digest), ]
    status$ref_id
  }

  if (!length(targets)) {
    return(list())
  }

  if (!is.null(limit)) {
    targets <- targets[seq_len(min(length(targets), as.integer(limit)))]
  }

  out <- stats::setNames(vector("list", length(targets)), targets)
  for (i in seq_along(targets)) {
    out[[i]] <- litxr_build_llm_digest(
      ref_id = targets[[i]],
      builder = builder,
      config = cfg,
      overwrite = overwrite,
      write = TRUE
    )
  }
  out
}

litxr_read_llm_digest <- function(ref_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  path <- .litxr_llm_digest_current_path(cfg, ref_id)
  if (!file.exists(path)) {
    return(NULL)
  }
  digest <- .litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE))
  litxr_validate_llm_digest(digest)
  digest
}

litxr_read_llm_digests <- function(config = NULL, ref_ids = NULL, columns = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  index <- .litxr_read_llm_digest_index(cfg)
  if (!nrow(index)) {
    return(data.table::data.table())
  }

  if (!is.null(ref_ids) && length(ref_ids)) {
    keys <- vapply(as.character(ref_ids), .litxr_llm_digest_index_key, character(1))
    keys <- keys[!is.na(keys) & nzchar(keys)]
    if (!length(keys)) {
      return(data.table::data.table())
    }
    index <- index[index$ref_id %in% keys, ]
  }
  if (!nrow(index)) {
    return(data.table::data.table())
  }

  rows <- lapply(seq_len(nrow(index)), function(i) {
    path <- file.path(.litxr_project_llm_dir(cfg), basename(index$json_filename[[i]]))
    if (!file.exists(path)) {
      return(NULL)
    }
    x <- .litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE))
    data.table::data.table(
      schema_version = x$schema_version %||% "v1",
      ref_id = x$ref_id %||% NA_character_,
      digest_revision = suppressWarnings(as.integer(x$digest_revision %||% 1L)),
      derived_from_revision = suppressWarnings(as.integer(x$derived_from_revision %||% NA_integer_)),
      extraction_mode = x$extraction_mode %||% NA_character_,
      prompt_version = x$prompt_version %||% NA_character_,
      model_hint = x$model_hint %||% NA_character_,
      paper_type = x$paper_type %||% NA_character_,
      summary = x$summary %||% NA_character_,
      motivation = x$motivation %||% NA_character_,
      research_questions = list(unlist(x$research_questions %||% character(), use.names = FALSE)),
      paper_structure = list(unlist(x$paper_structure %||% character(), use.names = FALSE)),
      methods = list(unlist(x$methods %||% character(), use.names = FALSE)),
      research_data = list(x$research_data %||% NULL),
      identification_strategy = x$identification_strategy %||% NA_character_,
      main_variables = list(x$main_variables %||% NULL),
      key_findings = list(unlist(x$key_findings %||% character(), use.names = FALSE)),
      limitations = list(unlist(x$limitations %||% character(), use.names = FALSE)),
      theoretical_mechanism = x$theoretical_mechanism %||% NA_character_,
      empirical_setting = x$empirical_setting %||% NA_character_,
      descriptive_statistics_summary = x$descriptive_statistics_summary %||% NA_character_,
      standardized_findings_summary = x$standardized_findings_summary %||% NA_character_,
      contribution_type = list(unlist(x$contribution_type %||% character(), use.names = FALSE)),
      ranked_contributions = list(x$ranked_contributions %||% list()),
      likely_reader_misconceptions = list(unlist(x$likely_reader_misconceptions %||% character(), use.names = FALSE)),
      business_relevance_pathway = list(unlist(x$business_relevance_pathway %||% character(), use.names = FALSE)),
      tables = list(x$tables %||% list()),
      research_target_github_links = list(x$research_target_github_links %||% list()),
      evidence_strength = x$evidence_strength %||% NA_character_,
      evidence_shape = list(x$evidence_shape %||% NULL),
      anchor_references = list(x$anchor_references %||% list()),
      citation_logic_nodes = list(x$citation_logic_nodes %||% list()),
      keywords = list(unlist(x$keywords %||% character(), use.names = FALSE)),
      notes = x$notes %||% NA_character_,
      generated_at = x$generated_at %||% NA_character_,
      updated_at = x$updated_at %||% NA_character_
    )
  })
  rows <- rows[vapply(rows, is.null, logical(1L)) == FALSE]
  if (!length(rows)) {
    return(data.table::data.table())
  }

  out <- data.table::rbindlist(rows, fill = TRUE)
  if (!is.null(columns) && length(columns)) {
    keep <- intersect(unique(as.character(columns)), names(out))
    out <- out[, keep, with = FALSE]
  }
  out
}

litxr_find_llm <- function(query = NULL, collection_id = NULL, ref_id = NULL, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  digests <- litxr_read_llm_digests(cfg, ref_ids = ref_id)
  if (!nrow(digests)) {
    return(digests)
  }

  if (!is.null(collection_id) && nzchar(as.character(collection_id))) {
    links <- litxr_read_reference_collections(cfg)
    keep_ref_ids <- unique(links$ref_id[links$collection_id == collection_id])
    digests <- digests[digests$ref_id %in% keep_ref_ids, ]
  }

  if (!is.null(query) && nzchar(as.character(query))) {
    q <- tolower(as.character(query[[1]]))
    flat_text <- vapply(seq_len(nrow(digests)), function(i) {
      row <- digests[i, ]
      flatten_cell <- function(x) {
        if (is.null(x) || length(x) == 0L) return(character())
        if (is.list(x) && length(x) == 1L) x <- x[[1]]
        if (is.null(x) || length(x) == 0L) return(character())
        as.character(unlist(x, use.names = FALSE))
      }
      paste(
        c(
          flatten_cell(row$summary[[1]]),
          flatten_cell(row$motivation[[1]]),
          flatten_cell(row$notes[[1]]),
          flatten_cell(row$research_questions[[1]]),
          flatten_cell(row$paper_structure[[1]]),
          flatten_cell(row$methods[[1]]),
          flatten_cell(row$research_data[[1]]),
          flatten_cell(row$identification_strategy[[1]]),
          flatten_cell(row$main_variables[[1]]),
          flatten_cell(row$key_findings[[1]]),
          flatten_cell(row$limitations[[1]]),
          flatten_cell(row$theoretical_mechanism[[1]]),
          flatten_cell(row$empirical_setting[[1]]),
          flatten_cell(row$descriptive_statistics_summary[[1]]),
          flatten_cell(row$standardized_findings_summary[[1]]),
          flatten_cell(row$anchor_references[[1]]),
          flatten_cell(row$citation_logic_nodes[[1]]),
          flatten_cell(row$contribution_type[[1]]),
          flatten_cell(row$ranked_contributions[[1]]),
          flatten_cell(row$likely_reader_misconceptions[[1]]),
          flatten_cell(row$business_relevance_pathway[[1]]),
          flatten_cell(row$tables[[1]]),
          flatten_cell(row$research_target_github_links[[1]]),
          flatten_cell(row$evidence_strength[[1]]),
          flatten_cell(row$evidence_shape[[1]]),
          flatten_cell(row$keywords[[1]])
        ),
        collapse = " "
      )
    }, character(1))
    keep <- !is.na(flat_text) & grepl(q, tolower(flat_text), fixed = TRUE)
    digests <- digests[keep, ]
  }

  digests
}

litxr_list_llm_digest_revisions <- function(ref_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  rows <- list()
  current_path <- .litxr_llm_digest_current_path(cfg, ref_id)
  if (file.exists(current_path)) {
    current <- litxr_read_llm_digest(ref_id, cfg)
    rows[[length(rows) + 1L]] <- data.table::data.table(
      ref_id = ref_id,
      is_current = TRUE,
      path = current_path,
      schema_version = current$schema_version %||% "v1",
      digest_revision = suppressWarnings(as.integer(current$digest_revision %||% 1L)),
      derived_from_revision = suppressWarnings(as.integer(current$derived_from_revision %||% NA_integer_)),
      extraction_mode = current$extraction_mode %||% NA_character_,
      prompt_version = current$prompt_version %||% NA_character_,
      model_hint = current$model_hint %||% NA_character_,
      generated_at = current$generated_at %||% NA_character_,
      updated_at = current$updated_at %||% NA_character_
    )
  }

  history_dir <- .litxr_llm_history_ref_dir(cfg, ref_id)
  if (dir.exists(history_dir)) {
    history_files <- list.files(history_dir, pattern = "\\.json$", full.names = TRUE)
    history_rows <- lapply(history_files, function(path) {
      digest <- .litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE))
      litxr_validate_llm_digest(digest)
      data.table::data.table(
        ref_id = ref_id,
        is_current = FALSE,
        path = path,
        schema_version = digest$schema_version %||% "v1",
        digest_revision = suppressWarnings(as.integer(digest$digest_revision %||% 1L)),
        derived_from_revision = suppressWarnings(as.integer(digest$derived_from_revision %||% NA_integer_)),
        extraction_mode = digest$extraction_mode %||% NA_character_,
        prompt_version = digest$prompt_version %||% NA_character_,
        model_hint = digest$model_hint %||% NA_character_,
        generated_at = digest$generated_at %||% NA_character_,
        updated_at = digest$updated_at %||% NA_character_
      )
    })
    rows <- c(rows, history_rows)
  }

  if (!length(rows)) {
    return(data.table::data.table(
      ref_id = character(),
      is_current = logical(),
      path = character(),
      schema_version = character(),
      digest_revision = integer(),
      derived_from_revision = integer(),
      extraction_mode = character(),
      prompt_version = character(),
      model_hint = character(),
      generated_at = character(),
      updated_at = character()
    ))
  }

  out <- data.table::rbindlist(rows, fill = TRUE)
  out[order(-out$is_current, -out$digest_revision, out$path), ]
}

litxr_read_llm_digest_history <- function(ref_id, config = NULL, include_current = TRUE) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  revisions <- litxr_list_llm_digest_revisions(ref_id, cfg)
  if (!nrow(revisions)) {
    revisions$digest <- list()
    return(revisions)
  }
  if (!isTRUE(include_current)) {
    revisions <- revisions[!revisions$is_current, ]
  }
  if (!nrow(revisions)) {
    revisions$digest <- list()
    return(revisions)
  }

  digests <- lapply(seq_len(nrow(revisions)), function(i) {
    path <- revisions$path[[i]]
    .litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE))
  })
  revisions$digest <- digests
  revisions
}

litxr_write_md <- function(ref_id, text, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_ensure_project_md_dir(cfg)
  path <- .litxr_md_path(cfg, ref_id)
  writeLines(as.character(text), path, useBytes = TRUE)
  .litxr_update_enrichment_status_ref(cfg, ref_id)
  invisible(path)
}

litxr_read_md <- function(ref_id, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  path <- .litxr_md_path(cfg, ref_id)
  if (!file.exists(path)) {
    return(NULL)
  }
  paste(readLines(path, warn = FALSE), collapse = "\n")
}

litxr_validate_llm_digest <- function(digest) {
  schema_version <- .litxr_llm_digest_schema_version(digest)
  required <- .litxr_llm_digest_required_fields(schema_version)
  missing <- setdiff(required, names(digest))
  if (length(missing)) {
    stop("LLM digest is missing required fields: ", paste(missing, collapse = ", "), call. = FALSE)
  }

  if (identical(schema_version, "v2") || identical(schema_version, "v3") || identical(schema_version, "v4")) {
    litxr_validate_paper_type(digest$paper_type)
    digest_revision <- suppressWarnings(as.integer(digest$digest_revision[[1]] %||% digest$digest_revision))
    if (length(digest_revision) != 1L || is.na(digest_revision) || digest_revision < 1L) {
      stop("LLM digest field `digest_revision` must be a positive integer.", call. = FALSE)
    }
    if (!is.null(digest$derived_from_revision) && length(digest$derived_from_revision)) {
      derived <- suppressWarnings(as.integer(digest$derived_from_revision[[1]]))
      if (length(derived) != 1L || is.na(derived) || derived < 1L) {
        stop("LLM digest field `derived_from_revision` must be NULL or a positive integer.", call. = FALSE)
      }
    }
    extraction_mode <- as.character(digest$extraction_mode[[1]] %||% digest$extraction_mode)
    if (!length(extraction_mode) || is.na(extraction_mode[[1]]) || !nzchar(extraction_mode[[1]])) {
      stop("LLM digest field `extraction_mode` must be a non-empty character scalar.", call. = FALSE)
    }
    .litxr_validate_list_fields(
      digest,
      c(
        "research_questions", "paper_structure", "methods", "key_findings",
        "limitations", "contribution_type", "keywords"
      )
    )
    .litxr_validate_named_list_fields(
      digest$research_data,
      required_fields = c("data_sources", "sample_period", "sample_region", "unit_of_observation"),
      field_name = "research_data",
      character_vector_fields = "data_sources"
    )
    sample_size <- digest$research_data$sample_size %||% NULL
    if (is.list(sample_size) && !length(sample_size)) {
      sample_size <- NULL
    }
    if (is.list(sample_size)) {
      sample_size <- unlist(sample_size, use.names = FALSE)
    }
    if (!(is.null(sample_size) || is.numeric(sample_size) || is.integer(sample_size) || is.character(sample_size))) {
      stop("LLM digest field `research_data$sample_size` must be numeric, character, or NA.", call. = FALSE)
    }
    if ("sample_size_note" %in% names(digest$research_data)) {
      sample_size_note <- digest$research_data$sample_size_note
      if (is.list(sample_size_note)) {
        sample_size_note <- unlist(sample_size_note, use.names = FALSE)
      }
      if (!(is.character(sample_size_note) || is.null(sample_size_note))) {
        stop("LLM digest field `research_data$sample_size_note` must be a character vector.", call. = FALSE)
      }
    }
    .litxr_validate_named_list_fields(
      digest$main_variables,
      required_fields = c(
        "dependent_variables", "independent_variables",
        "control_variables", "mechanism_variables"
      ),
      field_name = "main_variables",
      character_vector_fields = c(
        "dependent_variables", "independent_variables",
        "control_variables", "mechanism_variables"
      )
    )
    if (identical(schema_version, "v3") || identical(schema_version, "v4")) {
      .litxr_warn_v3_missing_empirical_fields(digest)
    }
    if (identical(schema_version, "v4")) {
      .litxr_validate_list_fields(
        digest,
        c("likely_reader_misconceptions", "business_relevance_pathway")
      )
      .litxr_validate_ranked_contributions(digest$ranked_contributions)
      .litxr_validate_digest_tables(digest$tables)
      .litxr_validate_research_target_github_links(digest$research_target_github_links)
      .litxr_validate_evidence_shape(digest$evidence_shape)
    }
    if ("anchor_references" %in% names(digest)) {
      .litxr_validate_inline_llm_table_field(
        digest$anchor_references,
        "anchor_references",
        litxr_validate_anchor_references,
        ref_id = digest$ref_id %||% NULL
      )
    }
    if ("citation_logic_nodes" %in% names(digest)) {
      .litxr_validate_inline_llm_table_field(
        digest$citation_logic_nodes,
        "citation_logic_nodes",
        litxr_validate_citation_logic_nodes,
        ref_id = digest$ref_id %||% NULL
      )
    }
  } else {
    if (!is.list(digest$sample)) {
      stop("LLM digest field `sample` must be a named list.", call. = FALSE)
    }
    .litxr_validate_list_fields(
      digest,
      c("research_questions", "methods", "key_findings", "limitations", "keywords")
    )
  }
  invisible(TRUE)
}
