#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  if (requireNamespace("pkgload", quietly = TRUE)) {
    pkgload::load_all(quiet = TRUE)
  } else {
    library(litxr)
  }
})

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
      "  Rscript scripts/report_ref_digest_field_from_ref_ids.R --field FIELD --ref-ids ID1,ID2 [--output PATH]",
      "",
      "Options:",
      "  --field FIELD   Digest field to render: summary, methods, identification_strategy, citation_use.",
      "  --ref-ids LIST   Comma/space separated ref_ids.",
      "  --output PATH    Output markdown file path. Default: report_ref_<field>.md.",
      "  -h, --help       Show this help message.",
      "",
      "Behavior:",
      "  - Reads the requested digest field for each provided ref_id.",
      "  - Does not parse BibTeX; callers must provide the ref_ids directly.",
      "  - Writes a markdown report to the requested output path.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    field = "summary",
    ref_ids = NULL,
    output = NULL
  )
  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (identical(arg, "-h") || identical(arg, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(arg, "--field")) {
      if (i == length(args)) stop("Missing value for --field", call. = FALSE)
      out$field <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(arg, "--ref-ids")) {
      if (i == length(args)) stop("Missing value for --ref-ids", call. = FALSE)
      out$ref_ids <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(arg, "--output")) {
      if (i == length(args)) stop("Missing value for --output", call. = FALSE)
      out$output <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    stop("Unknown argument: ", arg, call. = FALSE)
  }
  out
}

parse_ref_ids <- function(x) {
  if (is.null(x) || !length(x)) {
    return(character())
  }
  ids <- unlist(strsplit(as.character(x[[1L]]), "[,;[:space:]]+", perl = TRUE), use.names = FALSE)
  ids <- trimws(ids)
  ids <- ids[nzchar(ids)]
  unique(ids)
}

scalar_text <- function(x) {
  if (is.null(x) || !length(x)) {
    return(NA_character_)
  }
  vals <- as.character(unlist(x, use.names = FALSE))
  vals <- vals[!is.na(vals) & nzchar(trimws(vals))]
  if (!length(vals)) {
    return(NA_character_)
  }
  vals[[1L]]
}

scalar_lines <- function(x) {
  vals <- as.character(unlist(x, use.names = FALSE))
  vals <- vals[!is.na(vals) & nzchar(trimws(vals))]
  unique(vals)
}

read_digests_by_ref_ids <- function(ref_ids, cfg, field) {
  ref_ids <- unique(as.character(ref_ids))
  ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(ref_ids)]
  if (!length(ref_ids)) {
    return(list())
  }
  out <- list()
  for (ref_id in ref_ids) {
    digest_ref_id <- litxr:::.litxr_task_ref_id(cfg, ref_id, task = "digest")
    if (is.na(digest_ref_id) || !nzchar(digest_ref_id)) {
      next
    }
    digest <- tryCatch(litxr:::litxr_read_llm_digest(digest_ref_id, cfg), error = function(e) NULL)
    if (!is.null(digest)) {
      out[[digest_ref_id]] <- digest
    }
  }
  out
}

render_digest_field_lines <- function(digest, field) {
  switch(
    field,
    summary = {
      summary_text <- scalar_text(digest$summary)
      mech_text <- scalar_text(digest$theoretical_mechanism)
      c(
        sprintf("- Summary: %s", if (!is.na(summary_text) && nzchar(summary_text)) summary_text else "[missing]"),
        sprintf("- Theoretical Mechanism: %s", if (!is.na(mech_text) && nzchar(mech_text)) mech_text else "[missing]")
      )
    },
    methods = {
      methods <- scalar_lines(digest$methods)
      if (length(methods)) {
        c("- Methods:", paste0("  - ", methods))
      } else {
        "- Methods: [missing]"
      }
    },
    identification_strategy = {
      ident <- scalar_text(digest$identification_strategy)
      sprintf("- Identification Strategy: %s", if (!is.na(ident) && nzchar(ident)) ident else "[missing]")
    },
    citation_use = {
      nodes <- digest$citation_logic_nodes[[1L]]
      if (inherits(nodes, "data.frame")) {
        nodes <- split(nodes, seq_len(nrow(nodes)))
      }
      if (!is.list(nodes) || !length(nodes)) {
        return("- Citation Use: [missing]")
      }
      vals <- vapply(nodes, function(node) {
        if (!is.list(node) || !length(node)) {
          return(NA_character_)
        }
        if ("citation_use" %in% names(node)) {
          return(scalar_text(node[["citation_use"]]))
        }
        NA_character_
      }, character(1))
      vals <- vals[!is.na(vals) & nzchar(trimws(vals))]
      if (length(vals)) {
        c("- Citation Use:", paste0("  - ", unique(vals)))
      } else {
        "- Citation Use: [missing]"
      }
    },
    stop("Unsupported field: ", field, call. = FALSE)
  )
}

args <- commandArgs(trailingOnly = TRUE)
parsed <- parse_args(args)
if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}

field <- tolower(trimws(parsed$field))
if (!field %in% c("summary", "methods", "identification_strategy", "citation_use")) {
  usage()
  stop("Unsupported field: ", field, call. = FALSE)
}

ref_ids <- parse_ref_ids(parsed$ref_ids)
if (!length(ref_ids)) {
  usage()
  stop("Missing --ref-ids values.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
field_label <- switch(
  field,
  summary = "Summary",
  methods = "Methods",
  identification_strategy = "Identification Strategy",
  citation_use = "Citation Use"
)
if (is.null(parsed$output) || !nzchar(trimws(parsed$output))) {
  output_path <- paste0("report_ref_", field, ".md")
} else {
  output_path <- path.expand(parsed$output)
}

digests <- read_digests_by_ref_ids(ref_ids, cfg, field)
report_lines <- c(
  sprintf("# Reference %s", field_label),
  "",
  sprintf("- Source ref_ids: `%s`", paste(ref_ids, collapse = ", ")),
  sprintf("- Generated at: `%s`", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z", tz = "UTC")),
  sprintf("- Entry count: `%d`", length(ref_ids)),
  "",
  sprintf("This report renders the `%s` digest field for each requested ref_id.", field_label),
  ""
)

missing <- character()
resolved <- character()
for (i in seq_along(ref_ids)) {
  ref_id <- ref_ids[[i]]
  digest_ref_id <- litxr:::.litxr_task_ref_id(cfg, ref_id, task = "digest")
  digest <- digests[[digest_ref_id]]
  if (is.null(digest)) {
    missing <- c(missing, ref_id)
    report_lines <- c(
      report_lines,
      sprintf("## %d. %s", i, if (nzchar(digest_ref_id %||% "")) digest_ref_id else ref_id),
      "",
      sprintf("- Ref ID: `%s`", ref_id),
      sprintf("- Digest Ref ID: `%s`", if (nzchar(digest_ref_id %||% "")) digest_ref_id else "[missing]"),
      sprintf("- %s: [missing]", field_label),
      ""
    )
    next
  }

  resolved <- c(resolved, digest_ref_id)
  report_lines <- c(
    report_lines,
    sprintf("## %d. %s", i, digest_ref_id),
    "",
    sprintf("- Ref ID: `%s`", ref_id),
    sprintf("- Digest Ref ID: `%s`", digest_ref_id),
    render_digest_field_lines(digest, field),
    ""
  )
}

dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
writeLines(report_lines, output_path)
log_line(sprintf("written=%s", normalizePath(output_path, winslash = "/", mustWork = FALSE)))
if (length(missing)) {
  log_line(sprintf("unresolved_ref_ids=%s", paste(unique(missing), collapse = ", ")))
}
