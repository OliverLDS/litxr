#!/usr/bin/env Rscript

normalize_ref_input <- function(ref_id) {
  ref_id <- as.character(ref_id)
  if (!length(ref_id) || is.na(ref_id[[1]])) return(NA_character_)

  ref_id <- trimws(ref_id[[1]])
  if (!nzchar(ref_id)) return(NA_character_)

  if (grepl("^https?://(dx\\.)?doi\\.org/", ref_id, ignore.case = TRUE)) {
    ref_id <- sub("^https?://(dx\\.)?doi\\.org/", "", ref_id, ignore.case = TRUE)
  }

  if (grepl("^[0-9]{4}\\.[0-9]{4,5}(v[0-9]+)?$", ref_id)) {
    return(paste0("arxiv:", ref_id))
  }

  if (grepl("^doi:", ref_id, ignore.case = TRUE)) {
    return(paste0("doi:", sub("^doi:", "", ref_id, ignore.case = TRUE)))
  }

  if (grepl("^10\\.", ref_id)) {
    return(paste0("doi:", ref_id))
  }

  ref_id
}

is_missing_scalar <- function(x) {
  is.null(x) || !length(x) || is.na(x[[1]]) || !nzchar(as.character(x[[1]]))
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/human/add_ref_record.R --ref-id REF_ID",
      "",
      "Options:",
      "  --ref-id REF_ID   DOI or arXiv id to add to the local cache.",
      "                    Bare arXiv ids like 2510.22085 are accepted.",
      "                    DOI strings may be bare or prefixed with doi:.",
      "                    arXiv ids from cs.AI are rejected by this helper;",
      "                    use the arXiv cs.AI repair/sync workflow instead.",
      "  -h, --help        Show this help message.",
      "",
      "Behavior:",
      "  - DOI inputs are ingested through the Crossref-backed DOI path.",
      "  - Non-cs.AI arXiv inputs are fetched once from the arXiv API and",
      "    stored in a local manual collection if they are not already present.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  if (!length(args)) {
    return(list(ref_id = NA_character_))
  }

  parsed <- list(ref_id = NA_character_)
  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (identical(arg, "-h") || identical(arg, "--help")) {
      parsed$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(arg, "--ref-id")) {
      if (i == length(args)) {
        stop("Missing value for --ref-id", call. = FALSE)
      }
      parsed$ref_id <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    if (startsWith(arg, "--")) {
      stop("Unknown argument: ", arg, call. = FALSE)
    }
    stop("Unexpected positional argument: ", arg, call. = FALSE)
  }

  parsed
}

is_doi_input <- function(ref_id) {
  !is.na(ref_id) && (grepl("^doi:", ref_id, ignore.case = TRUE) || grepl("^10\\.", ref_id))
}

doi_value <- function(ref_id) {
  if (is.na(ref_id)) return(NA_character_)
  ref_id <- sub("^doi:", "", ref_id, ignore.case = TRUE)
  if (grepl("^https?://(dx\\.)?doi\\.org/", ref_id, ignore.case = TRUE)) {
    ref_id <- sub("^https?://(dx\\.)?doi\\.org/", "", ref_id, ignore.case = TRUE)
  }
  trimws(ref_id)
}

is_arxiv_input <- function(ref_id) {
  !is.na(ref_id) && grepl("^arxiv:", ref_id, ignore.case = TRUE)
}

arxiv_value <- function(ref_id) {
  if (is.na(ref_id)) return(NA_character_)
  ref_id <- sub("^arxiv:", "", ref_id, ignore.case = TRUE)
  trimws(ref_id)
}

args <- commandArgs(trailingOnly = TRUE)
parsed <- parse_args(args)
if (isTRUE(parsed$help)) {
  usage()
  quit(status = 0L)
}

if (is_missing_scalar(parsed$ref_id)) {
  usage()
  stop("Missing --ref-id", call. = FALSE)
}

ref_id <- normalize_ref_input(parsed$ref_id)
if (is.na(ref_id) || !nzchar(ref_id)) {
  stop("Invalid --ref-id value.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()

if (is_doi_input(ref_id)) {
  doi <- doi_value(ref_id)
  if (!nzchar(doi)) {
    stop("Invalid DOI value: ", ref_id, call. = FALSE)
  }

  cat(sprintf("Adding DOI reference via Crossref: %s\n", doi))
  before <- tryCatch(litxr::litxr_find_refs(ref_id = paste0("doi:", doi), config = cfg), error = function(e) data.frame())
  if (inherits(before, "data.frame") && nrow(before)) {
    cat(sprintf("Local match before ingest: %d\n", nrow(before)))
  }

  added <- litxr::litxr_add_dois(doi, config = cfg, auto_register = TRUE)
  if (!nrow(added)) {
    cat("status: no_records\n")
    cat(sprintf("ref_id: doi:%s\n", doi))
    quit(status = 0L)
  }

  added_ref_id <- if ("ref_id" %in% names(added)) as.character(added$ref_id[[1]]) else paste0("doi:", doi)
  added_title <- if ("title" %in% names(added)) as.character(added$title[[1]]) else added_ref_id
  added_collection <- if ("collection_id" %in% names(added)) as.character(added$collection_id[[1]]) else NA_character_

  cat("status: ok\n")
  cat(sprintf("ref_id: %s\n", added_ref_id))
  cat(sprintf("title: %s\n", added_title))
  if (!is.na(added_collection) && nzchar(added_collection)) {
    cat(sprintf("collection_id: %s\n", added_collection))
  }
  quit(status = 0L)
}

if (!is_arxiv_input(ref_id)) {
  stop("This helper accepts DOI or arXiv ref ids only.", call. = FALSE)
}

arxiv_id <- arxiv_value(ref_id)
if (!nzchar(arxiv_id)) {
  stop("Invalid arXiv ref id: ", ref_id, call. = FALSE)
}

existing <- litxr::litxr_find_refs(ref_id = ref_id, config = cfg)
if (nrow(existing)) {
  cat(sprintf("Ref already exists locally: %s\n", ref_id))
  cat(sprintf("matches: %d\n", nrow(existing)))
  cat("status: already_present\n")
  quit(status = 0L)
}

cat(sprintf("Fetching arXiv record: %s\n", arxiv_id))
fetch_id <- sub("v[0-9]+$", "", arxiv_id)
feed <- litxr::fetch_arxiv_xml(id_vec = fetch_id)
entries <- xml2::xml_find_all(feed, ".//*[local-name()='entry']")
if (!length(entries)) {
  stop("No arXiv record found for ", fetch_id, call. = FALSE)
}
entry <- entries[[1]]
record <- litxr::parse_arxiv_entry_unified(entry)

primary_category <- if ("arxiv_primary_category" %in% names(record)) as.character(record$arxiv_primary_category[[1]]) else NA_character_
all_categories <- if ("arxiv_categories_raw" %in% names(record)) as.character(record$arxiv_categories_raw[[1]]) else NA_character_
if ((nzchar(primary_category) && identical(primary_category, "cs.AI")) ||
    (!is.na(all_categories) && grepl("\\bcs\\.AI\\b", all_categories))) {
  stop(
    "This helper refuses cs.AI arXiv records. Use the cs.AI repair/sync workflow instead: ",
    ref_id,
    call. = FALSE
  )
}

record$collection_id <- "manual_arxiv_refs"
record$collection_title <- "Manual arXiv References"

added <- litxr::litxr_add_refs(
  record,
  collection_id = "manual_arxiv_refs",
  config = cfg,
  auto_register = TRUE,
  collection_title = "Manual arXiv References"
)

added_ref_id <- if ("ref_id" %in% names(added)) as.character(added$ref_id[[1]]) else ref_id
added_title <- if ("title" %in% names(added)) as.character(added$title[[1]]) else added_ref_id
added_collection <- if ("collection_id" %in% names(added)) as.character(added$collection_id[[1]]) else "manual_arxiv_refs"

cat("status: ok\n")
cat(sprintf("ref_id: %s\n", added_ref_id))
cat(sprintf("title: %s\n", added_title))
cat(sprintf("collection_id: %s\n", added_collection))
