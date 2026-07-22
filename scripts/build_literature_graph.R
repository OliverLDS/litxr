#!/usr/bin/env Rscript

suppressPackageStartupMessages(library(litxr))

usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript scripts/build_literature_graph.R --ref-ids ID1,ID2 [--max-depth 2] [--max-nodes 100] [--output graph.json]",
    "",
    "Options:",
    "  --ref-ids LIST    Bare root reference ids, separated by commas or spaces.",
    "  --max-depth N     Maximum anchor hops. Default: 2.",
    "  --max-nodes N     Maximum returned graph nodes. Default: 100.",
    "  --output PATH     Write the graph JSON to PATH. Without it, emit graph JSON to stdout.",
    "  --config PATH     Optional config.yaml path. Default: LITXR_DATA_ROOT/config.yaml.",
    "  -h, --help        Show this help message.",
    "",
    "Behavior:",
    "  - Reads index/llm_digest.fst once and hydrates only graph frontier digests.",
    "  - Traverses cached digests only; unresolved anchors remain visible external nodes.",
    sep = "\n"
  ))
}

parse_args <- function(args) {
  out <- list(help = FALSE, ref_ids = NULL, max_depth = "2", max_nodes = "100", output = NULL, config = NULL)
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (i == length(args)) stop("Missing value for ", key, call. = FALSE)
    value <- args[[i + 1L]]
    if (identical(key, "--ref-ids")) out$ref_ids <- value else if (identical(key, "--max-depth")) out$max_depth <- value else if (identical(key, "--max-nodes")) out$max_nodes <- value else if (identical(key, "--output")) out$output <- value else if (identical(key, "--config")) out$config <- value else stop("Unknown argument: ", key, call. = FALSE)
    i <- i + 2L
  }
  out
}

parse_ref_ids <- function(value) {
  if (is.null(value) || !length(value)) return(character())
  ids <- unlist(strsplit(as.character(value[[1L]]), "[,;[:space:]]+", perl = TRUE), use.names = FALSE)
  ids <- trimws(ids)
  unique(ids[nzchar(ids)])
}

emit_json <- function(value) {
  writeLines(jsonlite::toJSON(value, auto_unbox = TRUE, null = "null", dataframe = "rows"), con = stdout())
}

parsed <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(parsed$help)) {
  usage()
  quit(status = 0L)
}

tryCatch({
  ref_ids <- parse_ref_ids(parsed$ref_ids)
  if (!length(ref_ids)) stop("--ref-ids is required.", call. = FALSE)
  graph <- litxr_build_literature_graph(
    ref_ids = ref_ids,
    config = parsed$config,
    max_depth = parsed$max_depth,
    max_nodes = parsed$max_nodes
  )
  if (is.null(parsed$output) || !nzchar(parsed$output)) {
    emit_json(graph)
  } else {
    dir.create(dirname(parsed$output), recursive = TRUE, showWarnings = FALSE)
    jsonlite::write_json(graph, parsed$output, auto_unbox = TRUE, null = "null", dataframe = "rows", pretty = TRUE)
    emit_json(list(status = "ok", output = normalizePath(parsed$output, winslash = "/", mustWork = FALSE), meta = graph$meta))
  }
}, error = function(e) {
  emit_json(list(status = "error", error = conditionMessage(e)))
  quit(status = 1L)
})
