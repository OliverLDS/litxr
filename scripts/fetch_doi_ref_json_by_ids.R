#!/usr/bin/env Rscript

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

emit_json <- function(x) {
  writeLines(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE), con = stdout())
}

parse_ids <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) {
    return(character())
  }
  ids <- unique(trimws(unlist(strsplit(x, ",", fixed = TRUE), use.names = FALSE)))
  ids <- ids[nzchar(ids)]
  ids <- vapply(ids, function(id) litxr:::.litxr_bare_doi(doi = id), character(1))
  ids <- ids[!is.na(ids) & nzchar(ids)]
  unique(ids)
}

parse_args <- function(args) {
  out <- list(help = FALSE, dois = character())
  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (identical(arg, "-h") || identical(arg, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(arg, "--doi") || identical(arg, "--dois") || identical(arg, "--doi-id") || identical(arg, "--doi-ids")) {
      if (i == length(args)) stop("Missing value for DOI argument", call. = FALSE)
      out$dois <- c(out$dois, args[[i + 1L]])
      i <- i + 2L
      next
    }
    stop("Unknown argument: ", arg, call. = FALSE)
  }
  out
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/fetch_doi_ref_json_by_ids.R --doi DOI1,DOI2 [... ]",
      "",
      "Options:",
      "  --doi IDS     Comma-separated DOIs or canonical DOI ref_ids.",
      "  --dois IDS    Alias for --doi.",
      "  --doi-id IDS  Alias for --doi.",
      "  --doi-ids IDS Alias for --doi.",
      "  -h, --help    Show this help message.",
      "",
      "Behavior:",
      "  - Fetches each DOI from Crossref.",
      "  - Skips DOIs already present in ref_doi.fst.",
      "  - Auto-registers Crossref collections as needed.",
      "  - Writes JSON files directly under the collection's ref directory.",
      "  - Progress logs go to stderr; compact JSON goes to stdout.",
      sep = "\n"
    )
  )
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

parsed <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}

requested_dois <- parse_ids(parsed$dois)
if (!length(requested_dois)) {
  stop("At least one DOI is required.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
existing_doi_store <- litxr:::.litxr_read_scaffold_table_safe(litxr:::.litxr_ref_doi_path(cfg))
existing_dois <- if (nrow(existing_doi_store) && "doi" %in% names(existing_doi_store)) unique(trimws(as.character(existing_doi_store$doi))) else character()
existing_dois <- existing_dois[nzchar(existing_dois)]

fetch_dois <- setdiff(requested_dois, existing_dois)
skipped_existing <- setdiff(requested_dois, fetch_dois)

log_line("syncing DOI ref JSON by id")
log_line("requested=", length(requested_dois))
log_line("skipped_existing=", length(skipped_existing))
log_line("fetch_dois=", length(fetch_dois))

if (!length(fetch_dois)) {
  emit_json(list(
    status = "ok",
    requested = requested_dois,
    fetched = 0L,
    written = 0L,
    skipped_existing = skipped_existing,
    created_collection_ids = character(),
    collection_ids = character()
  ))
  quit(save = "no", status = 0L)
}

before_cfg <- cfg
records <- litxr::litxr_add_dois(fetch_dois, config = cfg, auto_register = TRUE)
cfg_after <- litxr::litxr_read_config()
created_collection_ids <- setdiff(
  litxr:::.litxr_collection_ids_by_remote_channel(cfg_after, "crossref"),
  litxr:::.litxr_collection_ids_by_remote_channel(before_cfg, "crossref")
)

collection_ids <- if (nrow(records) && "collection_id" %in% names(records)) unique(as.character(records$collection_id)) else character()

emit_json(list(
  status = "ok",
  requested = requested_dois,
  fetched = length(fetch_dois),
  written = if (data.table::is.data.table(records)) nrow(records) else length(records),
  skipped_existing = skipped_existing,
  created_collection_ids = created_collection_ids,
  collection_ids = collection_ids
))
