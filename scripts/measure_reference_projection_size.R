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

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/measure_reference_projection_size.R [--scope project|collection|both] [--collection-id ID1,ID2,...]",
      "",
      "Options:",
      "  --scope VALUE        Measurement scope. Default: project.",
      "  --collection-id IDS  Comma-separated collection ids for collection scope.",
      "                       Default: all configured collections.",
      "  -h, --help           Show this help message.",
      "",
      "Behavior:",
      "  - Reads the configured local litxr store.",
      "  - Compares simulated legacy full reference-index payload size against",
      "    the current thin projection size.",
      "  - Reports current on-disk main/delta sizes and simulated full/thin sizes.",
      "  - Writes progress logs to stderr and compact JSON to stdout.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    scope = "project",
    collection_ids = NULL
  )
  i <- 1L

  while (i <= length(args)) {
    key <- args[[i]]
    if (!startsWith(key, "--") && !identical(key, "-h")) {
      stop("Unexpected positional argument: ", key, call. = FALSE)
    }

    if (identical(key, "-h") || identical(key, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }

    if (i == length(args)) {
      stop("Missing value for argument: ", key, call. = FALSE)
    }

    value <- args[[i + 1L]]
    if (identical(key, "--scope")) {
      out$scope <- value
    } else if (identical(key, "--collection-id")) {
      split_ids <- trimws(strsplit(value, ",", fixed = TRUE)[[1]])
      split_ids <- split_ids[nzchar(split_ids)]
      out$collection_ids <- unique(split_ids)
    } else {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    i <- i + 2L
  }

  out
}

file_size_bytes <- function(path) {
  if (!is.character(path) || !length(path) || is.na(path[[1]]) || !file.exists(path[[1]])) {
    return(0)
  }
  as.numeric(file.info(path[[1]])$size %||% 0)
}

`%||%` <- function(x, y) {
  if (is.null(x) || !length(x)) y else x
}

temp_fst_size <- function(records) {
  path <- tempfile(pattern = "litxr_projection_size_", fileext = ".fst")
  on.exit(unlink(path), add = TRUE)
  fst::write_fst(as.data.frame(records), path)
  file_size_bytes(path)
}

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

projection_stats <- function(records) {
  if (!nrow(records)) {
    thin <- reference_projection(records)
    return(list(
      row_count = 0L,
      full_columns = names(records),
      thin_columns = names(thin),
      removed_columns = setdiff(names(records), names(thin)),
      simulated_full_bytes = temp_fst_size(litxr:::.litxr_index_encode(records)),
      simulated_thin_bytes = temp_fst_size(litxr:::.litxr_index_encode(thin))
    ))
  }

  full_encoded <- litxr:::.litxr_index_encode(records)
  thin_encoded <- litxr:::.litxr_index_encode(reference_projection(records))

  list(
    row_count = nrow(records),
    full_columns = names(full_encoded),
    thin_columns = names(thin_encoded),
    removed_columns = setdiff(names(full_encoded), names(thin_encoded)),
    simulated_full_bytes = temp_fst_size(full_encoded),
    simulated_thin_bytes = temp_fst_size(thin_encoded)
  )
}

summarize_size_reduction <- function(scope, scope_id, authoritative_records, main_path, delta_path, extra = list()) {
  stats <- projection_stats(authoritative_records)
  reduction_bytes <- stats$simulated_full_bytes - stats$simulated_thin_bytes
  reduction_pct <- if (stats$simulated_full_bytes > 0) {
    round(100 * reduction_bytes / stats$simulated_full_bytes, 2)
  } else {
    0
  }

  out <- c(
    list(
      scope = scope,
      scope_id = scope_id,
      row_count = stats$row_count,
      current_main_bytes = file_size_bytes(main_path),
      current_delta_bytes = file_size_bytes(delta_path),
      simulated_full_bytes = stats$simulated_full_bytes,
      simulated_thin_bytes = stats$simulated_thin_bytes,
      reduction_bytes = reduction_bytes,
      reduction_pct = reduction_pct,
      full_column_count = length(stats$full_columns),
      thin_column_count = length(stats$thin_columns),
      removed_columns = stats$removed_columns
    ),
    extra
  )

  out
}

measure_project <- function(cfg) {
  log_line("measuring project reference projection size")
  collections <- litxr:::.litxr_config_collections(cfg)
  refs_list <- lapply(collections, function(collection) {
    records <- litxr::litxr_read_collection(collection$collection_id, cfg)
    litxr:::.litxr_project_references_from_collection_records(records)
  })
  refs_list <- refs_list[vapply(refs_list, nrow, integer(1)) > 0L]
  records <- if (length(refs_list)) {
    merged <- data.table::rbindlist(refs_list, fill = TRUE)
    litxr:::.litxr_upsert_records(merged[0, ], merged)
  } else {
    data.table::data.table()
  }
  summarize_size_reduction(
    scope = "project_references",
    scope_id = "project",
    authoritative_records = records,
    main_path = litxr:::.litxr_project_references_index_path(cfg),
    delta_path = litxr:::.litxr_project_references_delta_index_path(cfg)
  )
}

measure_collection <- function(cfg, collection_id) {
  journal <- litxr:::.litxr_get_journal(cfg, collection_id)
  local_path <- litxr:::.litxr_resolve_local_path(cfg, journal$local_path)
  log_line(sprintf("measuring collection reference projection size: %s", collection_id))
  records <- litxr::litxr_read_collection(collection_id, cfg)
  summarize_size_reduction(
    scope = "collection_references",
    scope_id = collection_id,
    authoritative_records = records,
    main_path = litxr:::.litxr_index_path(local_path),
    delta_path = litxr:::.litxr_delta_index_path(local_path),
    extra = list(
      collection_title = journal$title,
      local_path = local_path
    )
  )
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

parsed <- parse_args(commandArgs(trailingOnly = TRUE))

if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}

scope <- trimws(tolower(parsed$scope %||% "project"))
if (!scope %in% c("project", "collection", "both")) {
  usage()
  stop("`--scope` must be one of: project, collection, both.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
collections_cfg <- litxr:::.litxr_config_collections(cfg)
collection_ids <- parsed$collection_ids
if (is.null(collection_ids) || !length(collection_ids)) {
  collection_ids <- vapply(collections_cfg, function(x) as.character(x$collection_id), character(1))
}

out <- list(status = "ok")

if (scope %in% c("project", "both")) {
  out$project <- measure_project(cfg)
}

if (scope %in% c("collection", "both")) {
  collections <- lapply(collection_ids, function(collection_id) measure_collection(cfg, collection_id))
  out$collections <- collections
}

emit_json(out)
