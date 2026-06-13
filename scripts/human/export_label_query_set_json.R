#!/usr/bin/env Rscript

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/human/export_label_query_set_json.R --cache-dir PATH [--output PATH]",
      "",
      "Options:",
      "  --cache-dir PATH   Existing label-query cache directory containing metadata.fst.",
      "  --output PATH      Output query_set.json path. Default: <cache-dir>/query_set.json",
      "  -h, --help         Show this help message.",
      "",
      "Behavior:",
      "  - Reconstructs the category->inquiry sentence mapping from metadata.fst.",
      "  - Writes a query_set.json file alongside the cache.",
      "  - Progress logs are written to stderr.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(help = FALSE, cache_dir = NULL, output = NULL)
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(key, "--cache-dir")) {
      if (i == length(args)) stop("Missing value for --cache-dir", call. = FALSE)
      out$cache_dir <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(key, "--output")) {
      if (i == length(args)) stop("Missing value for --output", call. = FALSE)
      out$output <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    stop("Unknown argument: ", key, call. = FALSE)
  }
  out
}

query_set_from_metadata <- function(metadata) {
  metadata <- data.table::as.data.table(metadata)
  required <- c("category_id", "query_order", "query_text")
  missing <- setdiff(required, names(metadata))
  if (length(missing)) {
    stop("Metadata is missing required columns: ", paste(missing, collapse = ", "), call. = FALSE)
  }
  metadata <- metadata[!is.na(category_id) & nzchar(trimws(as.character(category_id)))]
  if (!nrow(metadata)) {
    return(list())
  }
  category_ids <- unique(as.character(metadata$category_id))
  category_ids <- category_ids[nzchar(category_ids)]
  setNames(lapply(category_ids, function(category_id) {
    dt <- metadata[metadata$category_id == category_id, ]
    dt <- dt[order(dt$query_order), ]
    as.character(dt[["query_text"]])
  }), category_ids)
}

emit_json <- function(x) {
  jsonlite::write_json(x, stdout(), auto_unbox = TRUE, pretty = TRUE, null = "null")
}

args <- commandArgs(trailingOnly = TRUE)
parsed <- parse_args(args)

if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}

if (is.null(parsed$cache_dir) || !nzchar(trimws(parsed$cache_dir))) {
  usage()
  stop("Missing --cache-dir.", call. = FALSE)
}

cache_dir <- normalizePath(path.expand(parsed$cache_dir), winslash = "/", mustWork = FALSE)
metadata_path <- file.path(cache_dir, "metadata.fst")
if (!file.exists(metadata_path)) {
  stop("metadata.fst not found in cache dir: ", cache_dir, call. = FALSE)
}

output_path <- if (is.null(parsed$output) || !nzchar(trimws(parsed$output))) {
  file.path(cache_dir, "query_set.json")
} else {
  path.expand(parsed$output)
}

metadata <- fst::read_fst(metadata_path, as.data.table = TRUE)
query_set <- query_set_from_metadata(metadata)

dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
jsonlite::write_json(query_set, output_path, auto_unbox = TRUE, pretty = TRUE, null = "null")

log_line(sprintf("cache_dir=%s", cache_dir))
log_line(sprintf("metadata_path=%s", metadata_path))
log_line(sprintf("output_path=%s", normalizePath(output_path, winslash = "/", mustWork = FALSE)))
log_line(sprintf("categories=%d", length(query_set)))

emit_json(list(
  status = "ok",
  cache_dir = cache_dir,
  metadata_path = metadata_path,
  output_path = normalizePath(output_path, winslash = "/", mustWork = FALSE),
  categories = length(query_set)
))
