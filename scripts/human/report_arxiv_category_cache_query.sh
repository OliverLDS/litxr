#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
})

script_arg <- commandArgs(trailingOnly = FALSE)
script_file <- sub("^--file=", "", script_arg[grep("^--file=", script_arg)][1])
script_dir <- dirname(script_file)
source(file.path(script_dir, "_diagnostics.R"), local = TRUE)

`%||%` <- function(x, y) {
  if (is.null(x) || !length(x)) y else x
}

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(args) {
  out <- list(show_help = FALSE, diagnose = FALSE)
  i <- 1L

  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$show_help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(key, "--diagnose")) {
      out$diagnose <- TRUE
      i <- i + 1L
      next
    }
    stop("Unexpected argument: ", key, call. = FALSE)
  }

  out
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/human/report_arxiv_category_cache_query.sh < input.json",
      "",
      "Purpose:",
      "  Convert the compact JSON output of scripts/report_arxiv_category_labels.R",
      "  into human-readable markdown for a cached category query.",
      "",
      "Input:",
      "  Reads JSON from stdin.",
      "",
      "Output:",
      "  Emits markdown to stdout.",
      "",
      "Notes:",
      "  - This node is intended to be used in a pipe:",
      "    scripts/report_arxiv_category_labels.R --output-format json | Rscript scripts/human/report_arxiv_category_cache_query.sh",
      "  - When the JSON contains no selected refs, it prints a short no-results message.",
      "  - --diagnose emits step timings and I/O metadata to stderr.",
      "  - -h, --help shows this message and exits.",
      sep = "\n"
    )
  )
}

read_stdin_text <- function() {
  con <- file("stdin", open = "r")
  on.exit(close(con), add = TRUE)
  lines <- readLines(con, warn = FALSE)
  if (!length(lines)) {
    stop("No JSON was provided on stdin.", call. = FALSE)
  }
  paste(lines, collapse = "\n")
}

render_md <- function(payload) {
  if (!is.list(payload) || is.null(payload$status)) {
    stop("Invalid category JSON payload.", call. = FALSE)
  }

  if (!identical(as.character(payload$status[[1]] %||% payload$status), "ok")) {
    stop("Category JSON payload did not report status = ok.", call. = FALSE)
  }

  categories <- payload$categories
  if (is.null(categories) || !length(categories)) {
    return("No refs met the selection rule.\n")
  }

  lines <- character()
  for (category in categories) {
    category_id <- as.character(category$category_id[[1]] %||% category$category_id)
    items <- category$items
    if (is.null(items) || !length(items)) {
      next
    }
    lines <- c(lines, category_id)
    for (item in items) {
      rank <- as.integer(item$rank[[1]] %||% item$rank)
      ref_id <- as.character(item$ref_id[[1]] %||% item$ref_id)
      score_max <- as.numeric(item$score_max[[1]] %||% item$score_max)
      title <- as.character(item$title[[1]] %||% item$title)
      lines <- c(
        lines,
        sprintf("%d. %s (%.7f): %s", rank, ref_id, score_max, title)
      )
    }
    lines <- c(lines, "")
  }

  if (!length(lines)) {
    return("No refs met the selection rule.\n")
  }

  paste0(paste(lines, collapse = "\n"), "\n")
}

parsed <- parse_args(args)
diag_state <- litxr_diag_init(
  "scripts/human/report_arxiv_category_cache_query.sh",
  enabled = parsed$diagnose
)
if (isTRUE(parsed$show_help)) {
  usage()
  quit(save = "no", status = 0L)
}

result <- tryCatch(
  {
    read_started <- Sys.time()
    stdin_text <- read_stdin_text()
    diag_state <<- litxr_diag_step(
      diag_state,
      "read_stdin",
      read_started,
      inputs = list(list(stream = "stdin", bytes = nchar(stdin_text, type = "bytes")))
    )
    parse_started <- Sys.time()
    payload <- jsonlite::fromJSON(stdin_text, simplifyVector = FALSE)
    diag_state <<- litxr_diag_step(
      diag_state,
      "parse_json",
      parse_started,
      inputs = list(list(bytes = nchar(stdin_text, type = "bytes"))),
      outputs = list(list(status = as.character(payload$status[[1]] %||% payload$status)))
    )
    render_started <- Sys.time()
    rendered <- render_md(payload)
    diag_state <<- litxr_diag_step(
      diag_state,
      "render_markdown",
      render_started,
      outputs = list(list(bytes = nchar(rendered, type = "bytes")))
    )
    rendered
  },
  error = function(e) {
    litxr_diag_emit(litxr_diag_finish(diag_state, status = "error", error = conditionMessage(e)))
    message(conditionMessage(e))
    quit(save = "no", status = 1L)
  }
)

litxr_diag_emit(
  litxr_diag_finish(
    diag_state,
    status = "ok",
    details = list(output_bytes = nchar(result, type = "bytes"))
  )
)
cat(result)
