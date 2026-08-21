#!/usr/bin/env Rscript

log_line <- function(...) cat(..., "\n", file = stderr(), sep = "")
emit_json <- function(x) writeLines(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE), stdout())

parse_ids <- function(values) {
  values <- as.character(values)
  values <- values[!is.na(values) & nzchar(values)]
  if (!length(values)) return(character())
  ids <- trimws(unlist(strsplit(values, ",", fixed = TRUE), use.names = FALSE))
  ids <- vapply(ids[nzchar(ids)], litxr:::.litxr_bare_openreview_id, character(1))
  unique(ids[!is.na(ids) & nzchar(ids)])
}

usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript scripts/fetch_openreview_ref_json_by_ids.R --openreview-id ID1,ID2",
    "",
    "Options:",
    "  --openreview-id IDS   Comma-separated OpenReview Note ids or canonical openreview: ref ids.",
    "  --openreview-ids IDS  Alias for --openreview-id.",
    "  -h, --help            Show this help message.",
    "",
    "Behavior:",
    "  - Fetches public Note metadata from OpenReview API v2.",
    "  - Set OPENREVIEW_ACCESS_TOKEN when OpenReview requires authenticated API access.",
    "  - Writes JSON under ref/openreview/ and incrementally updates ref_openreview.fst.",
    sep = "\n"))
}

args <- commandArgs(trailingOnly = TRUE)
if (any(args %in% c("-h", "--help"))) { usage(); quit(save = "no", status = 0L) }
values <- character()
i <- 1L
while (i <= length(args)) {
  if (args[[i]] %in% c("--openreview-id", "--openreview-ids")) {
    if (i == length(args)) stop("Missing value for ", args[[i]], call. = FALSE)
    values <- c(values, args[[i + 1L]])
    i <- i + 2L
  } else stop("Unknown argument: ", args[[i]], call. = FALSE)
}
ids <- parse_ids(values)
if (!length(ids)) stop("At least one OpenReview Note id is required.", call. = FALSE)

options(error = function() {
  err <- trimws(geterrmessage())
  message(err)
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

cfg <- litxr::litxr_read_config()
existing <- litxr:::.litxr_read_scaffold_table_safe(litxr:::.litxr_ref_openreview_path(cfg))
existing_ids <- if (nrow(existing) && "openreview_id" %in% names(existing)) as.character(existing$openreview_id) else character()
fetch_ids <- setdiff(ids, existing_ids)
log_line("fetching OpenReview ref JSON by id")
log_line("requested=", length(ids))
log_line("skipped_existing=", length(ids) - length(fetch_ids))
log_line("fetch_ids=", length(fetch_ids))
if (!length(fetch_ids)) {
  emit_json(list(status = "ok", requested = ids, fetched = 0L, written = 0L, skipped_existing = ids))
  quit(save = "no", status = 0L)
}
records <- litxr::litxr_add_openreview_notes(fetch_ids, config = cfg)
emit_json(list(status = "ok", requested = ids, fetched = nrow(records), written = nrow(records), skipped_existing = setdiff(ids, fetch_ids)))
