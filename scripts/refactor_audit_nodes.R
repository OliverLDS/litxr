#!/usr/bin/env Rscript

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

emit_json <- function(x) {
  writeLines(
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE),
    con = stdout()
  )
}

`%||%` <- function(x, y) {
  if (is.null(x) || !length(x)) y else x
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/refactor_audit_nodes.R [--report MODE] [--oversized-mb N] [--sample-n N]",
      "",
      "Options:",
      "  --report MODE      Report node to emit. Default: all.",
      "                     One of: all, derived_append_shard_state, legacy_delta_presence,",
      "                     entity_refresh_timing, runtime_compatibility_projection,",
      "                     embedding_search_shard_timing, projection_size_reduction,",
      "                     identity_conflict_audit, unresolved_local_pending_audit.",
      "  --oversized-mb N   Threshold used by refactor diagnostics. Default: 25.",
      "  --sample-n N       Number of refs to use for entity-refresh timing. Default: 10.",
      "  -h, --help         Show this help message.",
      "",
      "Behavior:",
      "  - Emits compact JSON on stdout.",
      "  - Progress and timing logs go to stderr.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    report = "all",
    oversized_mb = 25,
    sample_n = 10L
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(key, "--report")) {
      if (i == length(args)) stop("Missing value for --report", call. = FALSE)
      out$report <- as.character(args[[i + 1L]])
      i <- i + 2L
      next
    }
    if (identical(key, "--oversized-mb")) {
      if (i == length(args)) stop("Missing value for --oversized-mb", call. = FALSE)
      out$oversized_mb <- as.numeric(args[[i + 1L]])
      i <- i + 2L
      next
    }
    if (identical(key, "--sample-n")) {
      if (i == length(args)) stop("Missing value for --sample-n", call. = FALSE)
      out$sample_n <- as.integer(args[[i + 1L]])
      i <- i + 2L
      next
    }
    stop("Unknown argument: ", key, call. = FALSE)
  }
  out
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

find_first_embedding_spec <- function(cfg) {
  root <- file.path(litxr:::.litxr_project_root(cfg), "embeddings")
  if (!dir.exists(root)) {
    return(NULL)
  }
  manifests <- list.files(root, pattern = "manifest\\.json$", recursive = TRUE, full.names = TRUE)
  if (!length(manifests)) {
    return(NULL)
  }
  for (manifest_path in manifests) {
    manifest <- tryCatch(
      jsonlite::fromJSON(manifest_path, simplifyVector = FALSE),
      error = function(e) NULL
    )
    if (is.null(manifest)) {
      next
    }
    return(list(
      manifest_path = manifest_path,
      base_dir = dirname(manifest_path),
      collection_id = as.character(manifest$collection_id %||% NA_character_),
      field = as.character(manifest$field %||% NA_character_),
      model = as.character(manifest$model %||% NA_character_),
      dimension = suppressWarnings(as.integer(manifest$dimension %||% NA_integer_)),
      manifest = manifest
    ))
  }
  NULL
}

file_size_bytes <- function(path) {
  if (!is.character(path) || !length(path) || is.na(path[[1]]) || !file.exists(path[[1]])) {
    return(0)
  }
  as.numeric(file.info(path[[1]])$size %||% 0)
}

temp_fst_size <- function(records) {
  path <- tempfile(pattern = "litxr_audit_projection_", fileext = ".fst")
  on.exit(unlink(path), add = TRUE)
  fst::write_fst(as.data.frame(records), path)
  file_size_bytes(path)
}

measure_projection_reduction <- function(cfg, scope = c("project", "collection"), collection_ids = NULL) {
  scope <- match.arg(scope)
  reference_projection_columns <- function() {
    c(
      "ref_id",
      "source",
      "source_id",
      "entry_type",
      "title",
      "authors",
      "pub_date",
      "year",
      "month",
      "day",
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
      "note",
      "subject_primary",
      "arxiv_version",
      "arxiv_primary_category",
      "arxiv_categories_raw",
      "arxiv_journal_ref",
      "linked_doi_ref_id",
      "linked_arxiv_ref_id"
    )
  }
  reference_projection <- function(records) {
    records <- data.table::as.data.table(records)
    if (!nrow(records)) {
      return(data.table::data.table())
    }
    keep <- intersect(reference_projection_columns(), names(records))
    data.table::copy(records)[, keep, with = FALSE]
  }
  summarize_size_reduction <- function(scope, scope_id, authoritative_records, main_path, delta_path, extra = list()) {
    authoritative_records <- data.table::as.data.table(authoritative_records)
    if (!nrow(authoritative_records)) {
      return(c(
        list(
          scope = scope,
          scope_id = scope_id,
          row_count = 0L,
          current_main_bytes = file_size_bytes(main_path),
          current_delta_bytes = file_size_bytes(delta_path),
          simulated_full_bytes = 0,
          simulated_thin_bytes = 0,
          reduction_bytes = 0,
          reduction_pct = 0,
          full_column_count = 0L,
          thin_column_count = 0L,
          removed_columns = character()
        ),
        extra
      ))
    }
    full_encoded <- litxr:::.litxr_index_encode(authoritative_records)
    thin_encoded <- litxr:::.litxr_index_encode(reference_projection(authoritative_records))
    full_bytes <- temp_fst_size(full_encoded)
    thin_bytes <- temp_fst_size(thin_encoded)
    reduction_bytes <- full_bytes - thin_bytes
    reduction_pct <- if (full_bytes > 0) round(100 * reduction_bytes / full_bytes, 2) else 0
    c(
      list(
        scope = scope,
        scope_id = scope_id,
        row_count = nrow(authoritative_records),
        current_main_bytes = file_size_bytes(main_path),
        current_delta_bytes = file_size_bytes(delta_path),
        simulated_full_bytes = full_bytes,
        simulated_thin_bytes = thin_bytes,
        reduction_bytes = reduction_bytes,
        reduction_pct = reduction_pct,
        full_column_count = length(names(full_encoded)),
        thin_column_count = length(names(thin_encoded)),
        removed_columns = setdiff(names(full_encoded), names(thin_encoded))
      ),
      extra
    )
  }

  measure_project <- function(cfg) {
    log_line("measuring project reference projection reduction")
    authoritative_records <- litxr:::.litxr_authoritative_project_records(cfg)
  summarize_size_reduction(
    scope = "project_references",
    scope_id = "project",
    authoritative_records = authoritative_records,
    main_path = NA_character_,
    delta_path = NA_character_
  )
}

  measure_collection <- function(cfg, collection_id) {
    journal <- litxr:::.litxr_get_journal(cfg, collection_id)
    local_path <- litxr:::.litxr_resolve_local_path(cfg, journal$local_path)
    authoritative_records <- tryCatch(
      litxr:::.litxr_read_journal_records_authoritative(local_path),
      error = function(e) data.table::data.table()
    )
  summarize_size_reduction(
    scope = "collection_references",
    scope_id = collection_id,
    authoritative_records = authoritative_records,
    main_path = NA_character_,
    delta_path = NA_character_,
    extra = list(collection_title = journal$title)
  )
}

  project_report <- measure_project(cfg)
  collection_report <- NULL
  if (!is.null(collection_ids) && length(collection_ids)) {
    collection_report <- data.table::rbindlist(lapply(collection_ids, function(collection_id) {
      tryCatch(
        data.table::as.data.table(as.list(measure_collection(cfg, collection_id))),
        error = function(e) data.table::data.table(
          scope = "collection_references",
          scope_id = collection_id,
          error = conditionMessage(e)
        )
      )
    }), fill = TRUE)
  }

  list(project = project_report, collections = collection_report)
}

report_derived_append_shard_state <- function(cfg) {
  spec <- find_first_embedding_spec(cfg)
  findings_dir <- file.path(litxr:::.litxr_project_root(cfg), "findings")
  derived_delta_files <- data.table::data.table(
    file = c(
      "standardized_findings_delta.fst",
      "descriptive_stats_delta.fst",
      "anchor_references_delta.fst",
      "citation_logic_nodes_delta.fst"
    ),
    path = file.path(findings_dir, c(
      "standardized_findings_delta.fst",
      "descriptive_stats_delta.fst",
      "anchor_references_delta.fst",
      "citation_logic_nodes_delta.fst"
    )),
    exists = c(
      file.exists(file.path(findings_dir, "standardized_findings_delta.fst")),
      file.exists(file.path(findings_dir, "descriptive_stats_delta.fst")),
      file.exists(file.path(findings_dir, "anchor_references_delta.fst")),
      file.exists(file.path(findings_dir, "citation_logic_nodes_delta.fst"))
    )
  )
  out <- list(
    derived_delta_files = derived_delta_files,
    derived_delta_count = as.integer(sum(derived_delta_files$exists))
  )
  if (is.null(spec)) {
    out$embedding_cache <- list(status = "missing")
    return(out)
  }
  diag <- tryCatch(
    litxr::litxr_diagnose_embedding_cache(
      collection_id = spec$collection_id,
      config = cfg,
      field = spec$field %||% "abstract",
      model = spec$model
    ),
    error = function(e) list(summary = data.table::data.table(error = conditionMessage(e)), files = data.table::data.table())
  )
  out$embedding_cache <- diag
  out$append_shard_count <- as.integer((diag$summary$delta_shard_count[[1]] %||% 0L) + out$derived_delta_count)
  out
}

report_legacy_delta_presence <- function(cfg) {
  cache <- litxr::litxr_audit_reference_cache_state(cfg)
  project_cache <- cache$project_reference_cache
  project_cache <- project_cache[project_cache$scope %in% c("project_references", "project_reference_collections"), ]
  list(
    project_reference_cache = project_cache,
    collection_reference_cache = cache$collection_reference_cache
  )
}

report_entity_refresh_timing <- function(cfg, sample_n = 10L) {
  refs <- tryCatch(litxr:::.litxr_authoritative_project_records(cfg), error = function(e) data.table::data.table())
  sample_ref_ids <- if (nrow(refs) && "ref_id" %in% names(refs)) {
    unique(as.character(refs$ref_id))
  } else {
    character()
  }
  sample_ref_ids <- sample_ref_ids[!is.na(sample_ref_ids) & nzchar(sample_ref_ids)]
  sample_ref_ids <- utils::head(sample_ref_ids, sample_n)

  read_started <- Sys.time()
  entity_status <- litxr::litxr_read_entity_status(cfg)
  read_elapsed_sec <- as.numeric(difftime(Sys.time(), read_started, units = "secs"))

  audit_elapsed_sec <- NA_real_
  audited_rows <- 0L
  if (length(sample_ref_ids)) {
    audit_started <- Sys.time()
    entity_audit <- litxr::litxr_audit_entity_status_state(cfg)
    audit_elapsed_sec <- as.numeric(difftime(Sys.time(), audit_started, units = "secs"))
    audited_rows <- length(sample_ref_ids)
    if (is.list(entity_audit) && "missing_entity_status" %in% names(entity_audit)) {
      audited_rows <- max(audited_rows, nrow(entity_audit$missing_entity_status))
    }
  }

  list(
    sample_ref_count = as.integer(length(sample_ref_ids)),
    read_elapsed_sec = read_elapsed_sec,
    audit_elapsed_sec = audit_elapsed_sec,
    entity_status_rows = as.integer(nrow(entity_status)),
    audited_rows = as.integer(audited_rows)
  )
}

report_runtime_compatibility_projection <- function(cfg, oversized_mb = 25) {
  diagnostics <- litxr::litxr_refactor_diagnostics(cfg, oversized_mb = oversized_mb)
  normalized <- litxr::litxr_audit_normalized_authoritative_state(cfg)
  list(
    summary = diagnostics$summary,
    duplicate_identity_conflicts = normalized$duplicate_identity_conflicts,
    orphan_arxiv_payload_rows = normalized$orphan_arxiv_payload_rows,
    orphan_doi_payload_rows = normalized$orphan_doi_payload_rows,
    unresolved_local_pending_rows = normalized$unresolved_local_pending_rows,
    compatibility_runtime_output_stale = normalized$compatibility_runtime_output_stale
  )
}

report_embedding_search_shard_timing <- function(cfg) {
  spec <- find_first_embedding_spec(cfg)
  if (is.null(spec)) {
    return(list(status = "missing"))
  }
  if (is.na(spec$dimension) || spec$dimension <= 0L) {
    return(list(
      status = "error",
      error = "Embedding manifest did not expose a valid dimension."
    ))
  }
  query_vec <- rep(1, spec$dimension)
  started <- Sys.time()
  result <- litxr::litxr_search_embeddings(
    query = NULL,
    collection_id = spec$collection_id,
    config = cfg,
    field = spec$field %||% "abstract",
    query_vec = query_vec,
    model = spec$model,
    top_n = 3L
  )
  elapsed_sec <- as.numeric(difftime(Sys.time(), started, units = "secs"))
  list(
    collection_id = spec$collection_id,
    field = spec$field,
    model = spec$model,
    manifest_path = spec$manifest_path,
    shard_dir = file.path(spec$base_dir, "shards"),
    top_n = 3L,
    result_n = as.integer(nrow(result)),
    elapsed_sec = elapsed_sec
  )
}

report_identity_conflict_audit <- function(cfg) {
  normalized <- litxr::litxr_audit_normalized_authoritative_state(cfg)
  list(
    duplicate_identity_conflicts = normalized$duplicate_identity_conflicts
  )
}

report_unresolved_local_pending_audit <- function(cfg) {
  normalized <- litxr::litxr_audit_normalized_authoritative_state(cfg)
  list(
    unresolved_local_pending_rows = normalized$unresolved_local_pending_rows
  )
}

cfg <- litxr::litxr_read_config()
if (!identical(Sys.getenv("LITXR_REFRACTOR_AUDIT_NODES_SOURCE_ONLY"), "1")) {
  parsed <- parse_args(commandArgs(trailingOnly = TRUE))
  if (isTRUE(parsed$help)) {
    usage()
    quit(save = "no", status = 0L)
  }

  report_mode <- tolower(as.character(parsed$report[[1]]))
  valid_reports <- c(
    "all",
    "derived_append_shard_state",
    "legacy_delta_presence",
    "entity_refresh_timing",
    "runtime_compatibility_projection",
    "embedding_search_shard_timing",
    "projection_size_reduction",
    "identity_conflict_audit",
    "unresolved_local_pending_audit"
  )
  if (!report_mode %in% valid_reports) {
    stop("Unknown report mode: ", report_mode, call. = FALSE)
  }

  result <- switch(
    report_mode,
    all = list(
      derived_append_shard_state = report_derived_append_shard_state(cfg),
      legacy_delta_presence = report_legacy_delta_presence(cfg),
      entity_refresh_timing = report_entity_refresh_timing(cfg, sample_n = parsed$sample_n),
      runtime_compatibility_projection = report_runtime_compatibility_projection(cfg, oversized_mb = parsed$oversized_mb),
      embedding_search_shard_timing = report_embedding_search_shard_timing(cfg),
      projection_size_reduction = measure_projection_reduction(cfg),
      identity_conflict_audit = report_identity_conflict_audit(cfg),
      unresolved_local_pending_audit = report_unresolved_local_pending_audit(cfg)
    ),
    derived_append_shard_state = report_derived_append_shard_state(cfg),
    legacy_delta_presence = report_legacy_delta_presence(cfg),
    entity_refresh_timing = report_entity_refresh_timing(cfg, sample_n = parsed$sample_n),
    runtime_compatibility_projection = report_runtime_compatibility_projection(cfg, oversized_mb = parsed$oversized_mb),
    embedding_search_shard_timing = report_embedding_search_shard_timing(cfg),
    projection_size_reduction = measure_projection_reduction(cfg),
    identity_conflict_audit = report_identity_conflict_audit(cfg),
    unresolved_local_pending_audit = report_unresolved_local_pending_audit(cfg)
  )

  emit_json(c(list(status = "ok", report_mode = report_mode), if (identical(report_mode, "all")) list(reports = result) else list(report = result)))
}
