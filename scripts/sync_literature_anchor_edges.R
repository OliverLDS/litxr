#!/usr/bin/env Rscript

emit_json <- function(x) {
  writeLines(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"), con = stdout())
}

parse_args <- function(args) {
  out <- list(help = FALSE, full = FALSE, ref_ids_file = NULL)
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(key, "--full")) {
      out$full <- TRUE
      i <- i + 1L
      next
    }
    if (!identical(key, "--ref-ids-file") || i == length(args)) {
      stop("Unknown argument or missing value: ", key, call. = FALSE)
    }
    out$ref_ids_file <- args[[i + 1L]]
    i <- i + 2L
  }
  out
}

usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript scripts/sync_literature_anchor_edges.R --full",
    "  Rscript scripts/sync_literature_anchor_edges.R --ref-ids-file changed_digest_ids.txt",
    "",
    "Builds index/literature_anchor_edges.fst from current LLM digest anchors.",
    sep = "\n"
  ))
}

options(error = function() {
  emit_json(list(status = "error", error = trimws(geterrmessage())))
  quit(save = "no", status = 1L)
})

parsed <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}
if (isTRUE(parsed$full) == !is.null(parsed$ref_ids_file)) {
  stop("Supply exactly one of --full or --ref-ids-file.", call. = FALSE)
}
ref_ids <- if (isTRUE(parsed$full)) {
  NULL
} else {
  if (!file.exists(parsed$ref_ids_file)) stop("Ref-id file not found: ", parsed$ref_ids_file, call. = FALSE)
  scan(parsed$ref_ids_file, what = character(), quiet = TRUE)
}
cfg <- litxr::litxr_read_config()
result <- litxr:::.litxr_sync_literature_anchor_edges(
  cfg,
  mode = if (isTRUE(parsed$full)) "full" else "incremental",
  ref_ids = ref_ids
)
emit_json(c(list(status = "ok"), result))
