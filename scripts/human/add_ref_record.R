#!/usr/bin/env Rscript

script_arg <- commandArgs(trailingOnly = FALSE)
script_file <- sub("^--file=", "", script_arg[grep("^--file=", script_arg)][1])
script_dir <- dirname(script_file)
source(file.path(script_dir, "_diagnostics.R"), local = TRUE)

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
      "  --diagnose        Emit step timings and I/O metadata to stderr.",
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
    return(list(ref_id = NA_character_, diagnose = FALSE))
  }

  parsed <- list(ref_id = NA_character_, diagnose = FALSE)
  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (identical(arg, "-h") || identical(arg, "--help")) {
      parsed$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(arg, "--diagnose")) {
      parsed$diagnose <- TRUE
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

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  if (exists("diag_state", inherits = FALSE)) {
    litxr_diag_emit(litxr_diag_finish(diag_state, status = "error", error = err))
  }
  cat(jsonlite::toJSON(list(status = "error", error = err), auto_unbox = TRUE, null = "null", pretty = FALSE), "\n", sep = "")
  quit(save = "no", status = 1L)
})

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
diag_state <- litxr_diag_init(
  "scripts/human/add_ref_record.R",
  enabled = parsed$diagnose,
  args = list(ref_id = parsed$ref_id)
)
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
diag_state <- litxr_diag_step(diag_state, "read_config", Sys.time(), outputs = list(list(
  config_path = attr(cfg, "config_path", exact = TRUE),
  config_root = attr(cfg, "config_root", exact = TRUE)
)))

if (is_doi_input(ref_id)) {
  doi <- doi_value(ref_id)
  if (!nzchar(doi)) {
    stop("Invalid DOI value: ", ref_id, call. = FALSE)
  }

  cat(sprintf("Adding DOI reference via Crossref: %s\n", doi))
  doi_step <- Sys.time()
  before <- tryCatch(
    as.data.frame(litxr:::.litxr_task_ref_row_for_keys(cfg, paste0("doi:", doi), task = "citation")),
    error = function(e) data.frame()
  )
  if (inherits(before, "data.frame") && nrow(before)) {
    cat(sprintf("Local match before ingest: %d\n", nrow(before)))
  }

  added <- litxr::litxr_add_dois(doi, config = cfg, auto_register = TRUE)
  diag_state <- litxr_diag_step(
    diag_state,
    "add_doi",
    doi_step,
    inputs = list(list(ref_id = paste0("doi:", doi)))
  )
  if (!nrow(added)) {
    cat("status: no_records\n")
    cat(sprintf("ref_id: doi:%s\n", doi))
    litxr_diag_emit(litxr_diag_finish(diag_state, status = "ok", details = list(ref_id = paste0("doi:", doi), mode = "doi", result = "no_records")))
    quit(status = 0L)
  }

  added_ref_id <- if (nrow(added) && "ref_id" %in% names(added)) as.character(added$ref_id[[1]]) else paste0("doi:", doi)
  added_title <- if (nrow(added) && "title" %in% names(added)) as.character(added$title[[1]]) else added_ref_id
  added_collection <- if (nrow(added) && "collection_id" %in% names(added)) as.character(added$collection_id[[1]]) else NA_character_

  cat("status: ok\n")
  cat(sprintf("ref_id: %s\n", added_ref_id))
  cat(sprintf("title: %s\n", added_title))
  if (!is.na(added_collection) && nzchar(added_collection)) {
    cat(sprintf("collection_id: %s\n", added_collection))
  }
  litxr_diag_emit(litxr_diag_finish(diag_state, status = "ok", details = list(ref_id = added_ref_id, mode = "doi", collection_id = added_collection)))
  quit(status = 0L)
}

if (!is_arxiv_input(ref_id)) {
  stop("This helper accepts DOI or arXiv ref ids only.", call. = FALSE)
}

arxiv_id <- arxiv_value(ref_id)
if (!nzchar(arxiv_id)) {
  stop("Invalid arXiv ref id: ", ref_id, call. = FALSE)
}

existing <- as.data.frame(litxr:::.litxr_task_ref_row_for_keys(cfg, ref_id, task = "citation"))
if (nrow(existing)) {
  cat(sprintf("Ref already exists locally: %s\n", ref_id))
  cat(sprintf("matches: %d\n", nrow(existing)))
  cat("status: already_present\n")
  litxr_diag_emit(litxr_diag_finish(diag_state, status = "ok", details = list(ref_id = ref_id, mode = "arxiv", result = "already_present")))
  quit(status = 0L)
}

cat(sprintf("Fetching arXiv record: %s\n", arxiv_id))
fetch_id <- sub("v[0-9]+$", "", arxiv_id)
fetch_step <- Sys.time()
feed <- litxr::fetch_arxiv_xml(id_vec = fetch_id)
entries <- xml2::xml_find_all(feed, ".//*[local-name()='entry']")
if (!length(entries)) {
  stop("No arXiv record found for ", fetch_id, call. = FALSE)
}
entry <- entries[[1]]
record <- litxr::parse_arxiv_entry_unified(entry)
diag_state <- litxr_diag_step(
  diag_state,
  "fetch_and_parse_arxiv",
  fetch_step,
  inputs = list(list(ref_id = arxiv_id)),
  outputs = list(list(entries = length(entries)))
)

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
diag_state <- litxr_diag_step(
  diag_state,
  "add_ref_record",
  Sys.time(),
  inputs = list(list(ref_id = arxiv_id, collection_id = "manual_arxiv_refs")),
  outputs = list(list(ref_id = if (nrow(added) && "ref_id" %in% names(added)) as.character(added$ref_id[[1]]) else ref_id))
)

added_ref_id <- if (nrow(added) && "ref_id" %in% names(added)) as.character(added$ref_id[[1]]) else ref_id
added_title <- if (nrow(added) && "title" %in% names(added)) as.character(added$title[[1]]) else added_ref_id
added_collection <- if (nrow(added) && "collection_id" %in% names(added)) as.character(added$collection_id[[1]]) else "manual_arxiv_refs"

cat("status: ok\n")
cat(sprintf("ref_id: %s\n", added_ref_id))
cat(sprintf("title: %s\n", added_title))
cat(sprintf("collection_id: %s\n", added_collection))
litxr_diag_emit(litxr_diag_finish(diag_state, status = "ok", details = list(ref_id = added_ref_id, mode = "arxiv", collection_id = added_collection)))
