#!/usr/bin/env Rscript

emit_json <- function(x) {
  writeLines(
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE),
    con = stdout()
  )
}

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

has_text <- function(x) {
  isTRUE(length(x) == 1L && !is.na(x[[1L]]) && nzchar(trimws(as.character(x[[1L]]))))
}

parse_args <- function(args) {
  out <- list(help = FALSE, full = FALSE, json_mtime_after = NA_character_)
  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (identical(arg, "-h") || identical(arg, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(arg, "--full")) {
      out$full <- TRUE
      i <- i + 1L
      next
    }
    if (identical(arg, "--json-mtime-after")) {
      if (i == length(args)) {
        stop("Missing value for --json-mtime-after", call. = FALSE)
      }
      out$json_mtime_after <- args[[i + 1L]]
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
      "  Rscript scripts/sync_thin_digest_stores.R [--full | --json-mtime-after TIMESTAMP]",
      "",
      "Options:",
      "  --full                  Rebuild index/llm_digest.fst from all digest JSON files.",
      "  --json-mtime-after TS   Incrementally upsert digest JSON files modified after TS.",
      "  -h, --help              Show this help message.",
      "",
      "Behavior:",
      "  - Reads digest JSON files from digest/llm/.",
      "  - Writes the digest index to index/llm_digest.fst.",
      "  - Stores bare normalized ref_id, JSON filename, and history directory name.",
      "  - Never bootstraps the index from reader code; this script is the explicit sync entry point.",
      sep = "\n"
    )
  )
}

normalize_json_mtime_after <- function(x) {
  if (!has_text(x)) {
    return(NA_character_)
  }
  tz <- Sys.timezone()
  if (is.null(tz) || !nzchar(tz)) {
    tz <- ""
  }
  parsed <- suppressWarnings(as.POSIXct(as.character(x[[1L]]), tz = tz))
  if (is.na(parsed)) {
    stop("Invalid timestamp for --json-mtime-after: ", as.character(x[[1L]]), call. = FALSE)
  }
  format(parsed, tz = tz, usetz = TRUE)
}

collect_digest_rows <- function(cfg, files) {
  if (!length(files)) {
    return(litxr:::.litxr_empty_llm_digest_index())
  }
  rows <- lapply(files, function(path) {
    digest <- tryCatch(
      jsonlite::fromJSON(path, simplifyVector = FALSE),
      error = function(e) NULL
    )
    if (is.null(digest) || is.null(digest$ref_id)) {
      return(NULL)
    }
    ref_id <- litxr:::.litxr_llm_digest_index_key(digest$ref_id)
    if (is.na(ref_id) || !nzchar(ref_id)) {
      return(NULL)
    }
    history_dir <- litxr:::.litxr_llm_history_ref_dir(cfg, digest$ref_id)
    data.table::data.table(
      ref_id = ref_id,
      json_filename = basename(path),
      history_dir = if (dir.exists(history_dir)) basename(history_dir) else NA_character_
    )
  })
  rows <- rows[!vapply(rows, is.null, logical(1L))]
  if (!length(rows)) {
    return(litxr:::.litxr_empty_llm_digest_index())
  }
  out <- data.table::rbindlist(rows, fill = TRUE)
  out <- out[!is.na(out$ref_id) & nzchar(out$ref_id), ]
  if (!nrow(out)) {
    return(litxr:::.litxr_empty_llm_digest_index())
  }
  out[!duplicated(out$ref_id, fromLast = TRUE), ]
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
if (isTRUE(parsed$full) && has_text(parsed$json_mtime_after)) {
  stop("`--full` cannot be combined with `--json-mtime-after`.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
digest_dir <- litxr:::.litxr_project_llm_dir(cfg)
index_path <- litxr:::.litxr_project_llm_digest_index_path(cfg)
dir.create(dirname(index_path), recursive = TRUE, showWarnings = FALSE)

files <- if (dir.exists(digest_dir)) {
  sort(list.files(digest_dir, pattern = "\\.json$", full.names = TRUE))
} else {
  character()
}

mode <- "full"
if (!isTRUE(parsed$full)) {
  mode <- "incremental"
}

json_mtime_after <- normalize_json_mtime_after(parsed$json_mtime_after)
if (identical(mode, "incremental") && has_text(json_mtime_after)) {
  tz <- Sys.timezone()
  if (is.null(tz) || !nzchar(tz)) {
    tz <- ""
  }
  cutoff <- suppressWarnings(as.POSIXct(json_mtime_after, tz = tz))
  if (!is.na(cutoff)) {
    info <- file.info(files)
    keep <- !is.na(info$mtime) & as.POSIXct(info$mtime, tz = tz) > cutoff
    files <- files[keep]
  }
}

rows <- collect_digest_rows(cfg, files)
existing <- if (file.exists(index_path)) {
  tryCatch(litxr:::.litxr_read_llm_digest_index(cfg), error = function(e) litxr:::.litxr_empty_llm_digest_index())
} else {
  litxr:::.litxr_empty_llm_digest_index()
}

if (identical(mode, "full")) {
  litxr:::.litxr_write_llm_digest_index(cfg, rows)
} else if (nrow(rows)) {
  touched <- unique(rows$ref_id)
  if (!nrow(existing)) {
    merged <- rows
  } else {
    existing <- existing[!existing$ref_id %in% touched, ]
    merged <- data.table::rbindlist(list(existing, rows), fill = TRUE)
    merged <- merged[!duplicated(merged$ref_id, fromLast = TRUE), ]
  }
  litxr:::.litxr_write_llm_digest_index(cfg, merged)
}

log_line("syncing thin digest stores")
log_line("mode=", mode)
log_line("json_dir=", if (dir.exists(digest_dir)) digest_dir else "[missing]")
log_line("selected_json_files=", length(files))
log_line("selected_rows=", nrow(rows))

emit_json(list(
  status = "ok",
  mode = mode,
  json_dir = if (dir.exists(digest_dir)) digest_dir else NA_character_,
  index_path = index_path,
  selected_json_files = length(files),
  selected_rows = nrow(rows)
))
