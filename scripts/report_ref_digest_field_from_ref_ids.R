#!/usr/bin/env Rscript

suppressPackageStartupMessages(library(litxr))

`%||%` <- function(x, y) {
  if (is.null(x) || !length(x)) y else x
}

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/report_ref_digest_field_from_ref_ids.R --fields FIELD1,FIELD2 --ref-ids ID1,ID2 --output PATH",
      "",
      "Options:",
      "  --fields LIST    Comma-separated digest fields: summary, key_findings, citation_logic_nodes.",
      "  --field FIELD    Backward-compatible single-field form.",
      "  --ref-ids LIST   Comma/space-separated bare or canonical ref_ids.",
      "  --output PATH    Required Markdown output path.",
      "  -h, --help       Show this help message.",
      "",
      "Behavior:",
      "  - Reads the selected digest index rows once, then renders all requested fields.",
      "  - Writes one Markdown report in the supplied reference-id order.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(help = FALSE, field = NULL, fields = NULL, ref_ids = NULL, output = NULL)
  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (identical(arg, "-h") || identical(arg, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (!arg %in% c("--field", "--fields", "--ref-ids", "--output")) {
      stop("Unknown argument: ", arg, call. = FALSE)
    }
    if (i == length(args)) stop("Missing value for ", arg, call. = FALSE)
    out[[gsub("-", "_", sub("^--", "", arg), fixed = TRUE)]] <- args[[i + 1L]]
    i <- i + 2L
  }
  out
}

parse_ref_ids <- function(value) {
  ids <- unlist(strsplit(as.character(value %||% ""), "[,;[:space:]]+", perl = TRUE), use.names = FALSE)
  ids <- trimws(ids)
  unique(ids[nzchar(ids)])
}

parse_fields <- function(value) {
  fields <- tolower(trimws(unlist(strsplit(as.character(value %||% ""), ",", fixed = TRUE), use.names = FALSE)))
  fields <- unique(fields[nzchar(fields)])
  allowed <- c("summary", "key_findings", "citation_logic_nodes")
  if (!length(fields) || any(!fields %in% allowed)) {
    stop("--fields must contain only: ", paste(allowed, collapse = ", "), call. = FALSE)
  }
  fields
}

scalar_lines <- function(value) {
  values <- as.character(unlist(value, use.names = FALSE))
  unique(values[!is.na(values) & nzchar(trimws(values))])
}

field_label <- function(field) {
  switch(field, summary = "Summary", key_findings = "Key Findings", citation_logic_nodes = "Citation Logic Nodes")
}

render_field_lines <- function(digest, field) {
  switch(
    field,
    summary = {
      value <- scalar_lines(digest$summary)
      if (length(value)) paste0("- ", value[[1L]]) else "- [missing]"
    },
    key_findings = {
      values <- scalar_lines(digest$key_findings)
      if (length(values)) paste0("- ", values) else "- [missing]"
    },
    citation_logic_nodes = {
      nodes <- digest$citation_logic_nodes
      if (is.list(nodes) && length(nodes) == 1L && is.list(nodes[[1L]])) nodes <- nodes[[1L]]
      if (inherits(nodes, "data.frame")) nodes <- split(nodes, seq_len(nrow(nodes)))
      if (!is.list(nodes) || !length(nodes)) return("- [missing]")
      lines <- character()
      for (node in nodes) {
        if (!is.list(node) || !length(node)) next
        claim <- scalar_lines(node$claim_sentence)
        citation_use <- scalar_lines(node$citation_use)
        text <- if (length(claim)) claim[[1L]] else "[missing claim]"
        if (length(citation_use)) text <- paste0(text, " - ", citation_use[[1L]])
        lines <- c(lines, paste0("- ", text))
      }
      if (length(lines)) lines else "- [missing]"
    }
  )
}

args <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(args$help)) {
  usage()
  quit(save = "no", status = 0L)
}

if (!is.null(args$field) && !is.null(args$fields)) {
  stop("Use either --field or --fields, not both.", call. = FALSE)
}
fields <- parse_fields(if (!is.null(args$fields)) args$fields else args$field %||% "summary")
ref_ids <- parse_ref_ids(args$ref_ids)
if (!length(ref_ids)) stop("Missing --ref-ids values.", call. = FALSE)
if (is.null(args$output) || !nzchar(trimws(args$output))) stop("--output is required.", call. = FALSE)

cfg <- litxr_read_config()
digest_ref_ids <- vapply(ref_ids, function(ref_id) {
  litxr:::.litxr_task_ref_id(cfg, ref_id, task = "digest")
}, character(1L))
digest_ref_ids[is.na(digest_ref_ids)] <- ""
needed_ids <- unique(digest_ref_ids[nzchar(digest_ref_ids)])
needed_columns <- unique(c("ref_id", fields))
digests <- litxr:::litxr_read_llm_digests(cfg, ref_ids = needed_ids, columns = needed_columns)
if (nrow(digests)) {
  digests$lookup_ref_id <- sub("^(arxiv|doi|isbn):", "", as.character(digests$ref_id), ignore.case = TRUE)
}

output_path <- path.expand(args$output)
report_lines <- c(
  "# Project Digest Report",
  "",
  sprintf("- Fields: %s", paste(vapply(fields, field_label, character(1L)), collapse = ", ")),
  sprintf("- Source ref_ids: %s", paste(ref_ids, collapse = ", ")),
  sprintf("- Generated at: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z", tz = "UTC")),
  ""
)

missing <- character()
for (i in seq_along(ref_ids)) {
  ref_id <- ref_ids[[i]]
  digest_ref_id <- digest_ref_ids[[i]]
  hit <- if (nzchar(digest_ref_id)) match(digest_ref_id, digests$lookup_ref_id) else NA_integer_
  report_lines <- c(report_lines, sprintf("## %d. %s", i, ref_id), "")
  if (is.na(hit)) {
    missing <- c(missing, ref_id)
    report_lines <- c(report_lines, "- Digest: [missing]", "")
    next
  }
  digest <- as.list(digests[hit, ])
  for (field in fields) {
    report_lines <- c(report_lines, sprintf("### %s", field_label(field)), render_field_lines(digest, field), "")
  }
}

dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
writeLines(report_lines, output_path)
log_line(sprintf("written=%s", normalizePath(output_path, winslash = "/", mustWork = FALSE)))
if (length(missing)) log_line(sprintf("unresolved_ref_ids=%s", paste(unique(missing), collapse = ", ")))
