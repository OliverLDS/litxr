#!/usr/bin/env Rscript

normalize_arxiv_input <- function(x) {
  x <- as.character(x)
  if (!length(x) || is.na(x[[1]])) return(NA_character_)
  x <- trimws(x[[1]])
  if (!nzchar(x)) return(NA_character_)
  if (!grepl("^arxiv:", x, ignore.case = TRUE)) {
    x <- paste0("arxiv:", x)
  }
  base <- sub("^arxiv:", "", x, ignore.case = TRUE)
  base <- sub("v[0-9]+$", "", base)
  paste0("arxiv:", base)
}

normalize_doi_input <- function(x) {
  x <- as.character(x)
  if (!length(x) || is.na(x[[1]])) return(NA_character_)
  x <- trimws(x[[1]])
  if (!nzchar(x)) return(NA_character_)
  if (grepl("^https?://(dx\\.)?doi\\.org/", x, ignore.case = TRUE)) {
    x <- sub("^https?://(dx\\.)?doi\\.org/", "", x, ignore.case = TRUE)
  }
  x <- sub("^doi:", "", x, ignore.case = TRUE)
  if (!nzchar(x)) return(NA_character_)
  paste0("doi:", x)
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/human/enrich_arxiv_with_doi.R --arxiv-ref-id ARXIV_REF_ID --doi DOI",
      "",
      "Options:",
      "  --arxiv-ref-id ID  Existing local arXiv ref_id to enrich.",
      "                     Bare arXiv ids like 2505.10468 are accepted.",
      "  --doi DOI          Published DOI to link to the arXiv paper.",
      "                     Bare DOIs, doi:... values, and doi.org URLs are accepted.",
      "  -h, --help         Show this help message.",
      "",
      "Behavior:",
      "  - Keeps canonical ref_id values unchanged.",
      "  - Ensures the DOI-backed published record exists locally.",
      "  - Writes `doi` and `linked_doi_ref_id` onto the arXiv row.",
      "  - Writes `linked_arxiv_ref_id` onto the DOI row.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    arxiv_ref_id = NA_character_,
    doi = NA_character_
  )

  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (identical(arg, "-h") || identical(arg, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(arg, "--arxiv-ref-id")) {
      if (i == length(args)) stop("Missing value for --arxiv-ref-id", call. = FALSE)
      out$arxiv_ref_id <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(arg, "--doi")) {
      if (i == length(args)) stop("Missing value for --doi", call. = FALSE)
      out$doi <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    if (startsWith(arg, "--")) {
      stop("Unknown argument: ", arg, call. = FALSE)
    }
    stop("Unexpected positional argument: ", arg, call. = FALSE)
  }

  out
}

args <- commandArgs(trailingOnly = TRUE)
parsed <- parse_args(args)

if (isTRUE(parsed$help)) {
  usage()
  quit(status = 0L)
}

arxiv_ref_id <- normalize_arxiv_input(parsed$arxiv_ref_id)
doi_ref_id <- normalize_doi_input(parsed$doi)
if (is.na(arxiv_ref_id) || !nzchar(arxiv_ref_id)) {
  usage()
  stop("`--arxiv-ref-id` is required.", call. = FALSE)
}
if (is.na(doi_ref_id) || !nzchar(doi_ref_id)) {
  usage()
  stop("`--doi` is required.", call. = FALSE)
}

cat(sprintf("Linking %s -> %s\n", arxiv_ref_id, doi_ref_id))
result <- litxr::litxr_enrich_arxiv_with_doi(
  arxiv_ref_id = arxiv_ref_id,
  doi = doi_ref_id
)

cat("status: ok\n")
cat(sprintf("arxiv_ref_id: %s\n", result$arxiv_ref_id))
cat(sprintf("doi_ref_id: %s\n", result$doi_ref_id))
cat(sprintf("arxiv_title: %s\n", result$arxiv_title))
cat(sprintf("doi_title: %s\n", result$doi_title))
