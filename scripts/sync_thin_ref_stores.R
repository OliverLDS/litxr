#!/usr/bin/env Rscript

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

emit_json <- function(x) {
  writeLines(
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE),
    con = stdout()
  )
}

read_stdin_line <- function(prompt) {
  cat(prompt, file = stdout())
  flush(stdout())
  stdin_con <- file("stdin", open = "r")
  on.exit(close(stdin_con), add = TRUE)
  lines <- readLines(stdin_con, n = 1L, warn = FALSE)
  if (!length(lines)) {
    return("")
  }
  trimws(lines[[1L]])
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/sync_thin_ref_stores.R [--collection COLLECTION_ID] [--collection-id ID1,ID2,...] [--json-mtime-after TIMESTAMP] [--cuttime TIMESTAMP]",
      "",
      "Options:",
      "  --collection ID       Sync one collection only.",
      "  --collection-id IDS   Optional comma-separated collection ids to sync.",
      "                        Default: all configured collections.",
      "  --json-mtime-after T  Optional cutoff timestamp. When supplied, only",
      "                        JSON files modified after this time are used.",
      "  --cuttime T           Alias for --json-mtime-after.",
      "  -h, --help            Show this help message.",
      "",
      "Behavior:",
      "  - Incremental mode is used when --collection, --collection-id, --json-mtime-after, or --cuttime is supplied.",
      "  - Full mode rewrites ref_identity_map.fst, ref_arxiv.fst, and ref_doi.fst from all local JSON.",
      "  - Full mode requires a double confirmation in the terminal before any write occurs.",
      "  - Progress logs are written to stderr; compact JSON is written to stdout.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    collection = NULL,
    collection_ids = NULL,
    json_mtime_after = NULL,
    cuttime = NULL
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(key, "--collection")) {
      if (i == length(args)) stop("Missing value for --collection", call. = FALSE)
      out$collection <- trimws(args[[i + 1L]])
      i <- i + 2L
      next
    }
    if (identical(key, "--collection-id")) {
      if (i == length(args)) stop("Missing value for --collection-id", call. = FALSE)
      ids <- trimws(strsplit(args[[i + 1L]], ",", fixed = TRUE)[[1]])
      ids <- ids[nzchar(ids)]
      out$collection_ids <- unique(ids)
      i <- i + 2L
      next
    }
    if (identical(key, "--json-mtime-after")) {
      if (i == length(args)) stop("Missing value for --json-mtime-after", call. = FALSE)
      out$json_mtime_after <- trimws(args[[i + 1L]])
      i <- i + 2L
      next
    }
    if (identical(key, "--cuttime")) {
      if (i == length(args)) stop("Missing value for --cuttime", call. = FALSE)
      out$cuttime <- trimws(args[[i + 1L]])
      i <- i + 2L
      next
    }
    stop("Unknown argument: ", key, call. = FALSE)
  }
  out
}

confirm_full_mode <- function() {
  if (!interactive() && !isatty(stdin())) {
    stop(
      "Full mode requires an interactive terminal confirmation. Re-run with --collection or --json-mtime-after for incremental mode.",
      call. = FALSE
    )
  }
  first <- read_stdin_line("Full mode will rewrite ref_identity_map.fst, ref_arxiv.fst, and ref_doi.fst. Type FULL to continue: ")
  if (!identical(first, "FULL")) {
    stop("Full mode confirmation failed.", call. = FALSE)
  }
  second <- read_stdin_line("Repeat FULL to confirm full rewrite: ")
  if (!identical(second, "FULL")) {
    stop("Full mode confirmation failed.", call. = FALSE)
  }
  TRUE
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

cfg <- litxr::litxr_read_config()
if (!is.null(parsed$collection) && nzchar(parsed$collection)) {
  parsed$collection_ids <- unique(c(parsed$collection_ids, parsed$collection))
}
if (!is.null(parsed$cuttime) && nzchar(parsed$cuttime)) {
  parsed$json_mtime_after <- parsed$cuttime
}

full_mode <- is.null(parsed$collection_ids) && (is.null(parsed$json_mtime_after) || !nzchar(trimws(parsed$json_mtime_after)))
if (isTRUE(full_mode)) {
  confirm_full_mode()
}

if (length(parsed$collection_ids) == 1L && (is.null(parsed$json_mtime_after) || !nzchar(trimws(parsed$json_mtime_after)))) {
  parsed$json_mtime_after <- litxr:::.litxr_latest_collection_sync_timestamp(cfg, parsed$collection_ids[[1L]])
}

if (isTRUE(full_mode)) {
  log_line("syncing thin ref stores from json")
  log_line("mode=full")
} else {
  log_line("syncing thin ref stores from json")
  log_line("mode=incremental")
}

report <- litxr::litxr_sync_thin_ref_stores_from_json(
  cfg,
  collection_ids = parsed$collection_ids,
  json_mtime_after = parsed$json_mtime_after
)
emit_json(c(list(status = "ok"), report))
