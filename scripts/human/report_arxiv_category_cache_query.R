#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
})

`%||%` <- function(x, y) {
  if (is.null(x) || !length(x)) y else x
}

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(args) {
  out <- list(show_help = FALSE)
  i <- 1L

  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$show_help <- TRUE
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
      "  Rscript scripts/human/report_arxiv_category_cache_query.R < input.json",
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
      "    scripts/report_arxiv_category_labels.R --output-format json | Rscript scripts/human/report_arxiv_category_cache_query.R",
      "  - When the JSON contains no selected refs, it prints a short no-results message.",
      "  - -h, --help shows this message and exits.",
      sep = "\n"
    )
  )
}

read_stdin_text <- function() {
  if (isatty(stdin())) {
    stop("No JSON was provided on stdin. Pipe the JSON output into this script.", call. = FALSE)
  }
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
if (isTRUE(parsed$show_help)) {
  usage()
  quit(save = "no", status = 0L)
}

result <- tryCatch(
  {
    stdin_text <- read_stdin_text()
    payload <- jsonlite::fromJSON(stdin_text, simplifyVector = FALSE)
    render_md(payload)
  },
  error = function(e) {
    message(conditionMessage(e))
    quit(save = "no", status = 1L)
  }
)

cat(result)
