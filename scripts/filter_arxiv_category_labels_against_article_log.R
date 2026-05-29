#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(jsonlite)
})

article_log_path <- "/Users/oliver/Documents/2025/_2025-06-17_Zelina/zl_agentr/tasks/write_new_blog_article/state/article_record_log.tsv"

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

`%||%` <- function(x, y) if (is.null(x) || !length(x)) y else x

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/filter_arxiv_category_labels_against_article_log.R < input.json",
      "",
      "Purpose:",
      "  Filter the JSON output of scripts/report_arxiv_category_labels.R by",
      "  removing refs already recorded in the article log.",
      "",
      "Input:",
      "  Reads compact JSON from stdin.",
      "",
      "Output:",
      "  Emits the same JSON shape as input, with matching ref_ids removed from",
      "  categories and meta.ref_ids.",
      "",
      "Notes:",
      "  - The article log path is fixed to the zl_agentr state file.",
      "  - If a row contains multiple comma-separated arXiv ids, all are treated",
      "    as already recorded.",
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

split_logged_arxiv_ids <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(trimws(x))]
  if (!length(x)) {
    return(character())
  }
  pieces <- unlist(strsplit(x, ",", fixed = TRUE), use.names = FALSE)
  trimws(pieces)
}

normalize_arxiv_ref_id <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x <- x[!is.na(x) & nzchar(x)]
  x <- sub("^arxiv:", "", x, ignore.case = TRUE)
  paste0("arxiv:", x)
}

read_logged_ref_ids <- function(path) {
  if (!file.exists(path)) {
    return(character())
  }
  article_log <- data.table::fread(path, sep = "\t", header = TRUE, na.strings = c("", "NA"))
  if (!all(c("arxiv_id", "blog_article_filename") %in% names(article_log))) {
    return(character())
  }
  article_log <- article_log[!is.na(arxiv_id) & nzchar(trimws(arxiv_id))]
  if (!nrow(article_log)) {
    return(character())
  }
  ids <- unique(unlist(lapply(article_log$arxiv_id, split_logged_arxiv_ids), use.names = FALSE))
  normalize_arxiv_ref_id(ids)
}

filter_payload <- function(payload, excluded_ref_ids) {
  if (!is.list(payload) || is.null(payload$status)) {
    stop("Invalid category JSON payload.", call. = FALSE)
  }

  if (!identical(as.character(payload$status[[1]] %||% payload$status), "ok")) {
    return(payload)
  }

  excluded_ref_ids <- unique(normalize_arxiv_ref_id(excluded_ref_ids))

  if (!is.null(payload$meta) && !is.null(payload$meta$ref_ids)) {
    ref_ids <- as.character(payload$meta$ref_ids)
    payload$meta$ref_ids <- ref_ids[!(normalize_arxiv_ref_id(ref_ids) %in% excluded_ref_ids)]
  }

  categories <- payload$categories
  if (is.null(categories) || !length(categories)) {
    return(payload)
  }

  filtered_categories <- lapply(categories, function(category) {
    items <- category$items
    if (is.null(items) || !length(items)) {
      category$items <- list()
      return(category)
    }
    keep <- !normalize_arxiv_ref_id(vapply(items, function(item) as.character(item$ref_id[[1]] %||% item$ref_id), character(1))) %in% excluded_ref_ids
    category$items <- items[keep]
    if (length(category$items)) {
      for (i in seq_along(category$items)) {
        category$items[[i]]$rank <- as.integer(i)
      }
    }
    category
  })

  filtered_categories <- Filter(function(category) length(category$items), filtered_categories)
  payload$categories <- filtered_categories
  payload
}

parsed <- parse_args(args)
if (isTRUE(parsed$show_help)) {
  usage()
  quit(save = "no", status = 0L)
}

result <- tryCatch(
  {
    payload <- jsonlite::fromJSON(read_stdin_text(), simplifyVector = FALSE)
    excluded_ref_ids <- read_logged_ref_ids(article_log_path)
    filter_payload(payload, excluded_ref_ids)
  },
  error = function(e) {
    message(conditionMessage(e))
    quit(save = "no", status = 1L)
  }
)

cat(jsonlite::toJSON(result, auto_unbox = TRUE, null = "null", pretty = FALSE))
