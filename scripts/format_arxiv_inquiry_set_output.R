#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
})

`%||%` <- function(x, y) {
  if (is.null(x) || !length(x)) y else x
}

parse_args <- function(args) {
  out <- list(show_help = FALSE, format = "md")
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$show_help <- TRUE
      i <- i + 1L
      next
    }
    if (i == length(args)) {
      stop("Missing value for ", key, call. = FALSE)
    }
    value <- args[[i + 1L]]
    if (identical(key, "--format")) {
      out$format <- value
    } else {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    i <- i + 2L
  }
  out
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/format_arxiv_inquiry_set_output.R [--format md|ids] < input.json",
      "",
      "Purpose:",
      "  Convert the JSON output of scripts/report_arxiv_embedding_inquiry_set.R",
      "  into human-readable markdown or a flat JSON array of arXiv ids.",
      "",
      "Input:",
      "  Reads JSON from stdin.",
      "",
      "Output:",
      "  Emits markdown or a JSON array to stdout.",
      "",
      "Notes:",
      "  - --format md prints grouped markdown by category.",
      "  - --format ids prints a JSON array of selected arXiv ids.",
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

render_markdown <- function(payload) {
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
      lines <- c(lines, sprintf("%d. %s (%.7f): %s", rank, ref_id, score_max, title))
    }
    lines <- c(lines, "")
  }

  if (!length(lines)) {
    return("No refs met the selection rule.\n")
  }

  paste0(paste(lines, collapse = "\n"), "\n")
}

render_ids <- function(payload) {
  if (!is.list(payload) || is.null(payload$status)) {
    stop("Invalid category JSON payload.", call. = FALSE)
  }
  if (!identical(as.character(payload$status[[1]] %||% payload$status), "ok")) {
    stop("Category JSON payload did not report status = ok.", call. = FALSE)
  }
  ids <- character()
  if (!is.null(payload$meta) && !is.null(payload$meta$ref_ids)) {
    ids <- as.character(payload$meta$ref_ids)
  }
  ids <- ids[!is.na(ids) & nzchar(ids)]
  toJSON(unique(ids), auto_unbox = TRUE, null = "null", pretty = FALSE)
}

parsed <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(parsed$show_help)) {
  usage()
  quit(save = "no", status = 0L)
}

result <- tryCatch(
  {
    payload <- fromJSON(read_stdin_text(), simplifyVector = FALSE)
    fmt <- tolower(trimws(as.character(parsed$format)))
    if (identical(fmt, "ids")) {
      render_ids(payload)
    } else if (identical(fmt, "md")) {
      render_markdown(payload)
    } else {
      stop("`--format` must be either md or ids.", call. = FALSE)
    }
  },
  error = function(e) {
    message(conditionMessage(e))
    quit(save = "no", status = 1L)
  }
)

cat(result)
