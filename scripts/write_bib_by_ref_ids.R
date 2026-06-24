#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

args <- commandArgs(trailingOnly = TRUE)

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

emit_json <- function(x) {
  writeLines(
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE),
    con = stdout()
  )
}

parse_args <- function(args) {
  out <- list(
    show_help = FALSE,
    append = FALSE,
    overwrite = FALSE,
    output = NULL,
    ref_ids = NULL
  )
  i <- 1L

  while (i <= length(args)) {
    key <- args[[i]]
    if (!startsWith(key, "--") && !identical(key, "-h")) {
      stop("Unexpected positional argument: ", key, call. = FALSE)
    }
    if (identical(key, "-h") || identical(key, "--help")) {
      out$show_help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(key, "--append")) {
      out$append <- TRUE
      i <- i + 1L
      next
    }
    if (identical(key, "--overwrite")) {
      out$overwrite <- TRUE
      i <- i + 1L
      next
    }
    if (i == length(args)) {
      stop("Missing value for ", key, call. = FALSE)
    }
    value <- args[[i + 1L]]
    if (identical(key, "--output")) {
      out$output <- value
    } else if (identical(key, "--ref-ids") || identical(key, "--ref-id")) {
      out$ref_ids <- value
    } else {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    i <- i + 2L
  }

  out
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/write_bib_by_ref_ids.R --output PATH --ref-ids REF1,REF2 [--append] [--overwrite]",
      "",
      "Options:",
      "  --output PATH       Target BibTeX file path.",
      "  --ref-ids IDS       Comma-separated ref_id values to export.",
      "  --ref-id IDS        Equivalent to --ref-ids.",
      "  --append            Append into an existing BibTeX file instead of creating a new one.",
      "  --overwrite         With --append, replace existing matching entries instead of skipping them.",
      "  -h, --help          Show this help message.",
      "",
      "Notes:",
      "  - All supplied ref_id values must already exist in the local litxr reference cache.",
      "  - Resolution uses the entity identity layer and stops if any requested ref_id cannot be resolved.",
      "  - Duplicate detection in the target .bib file is based on the BibTeX citekey generated",
      "    from each reference row.",
      "  - Progress logs are written to stderr; compact JSON is written to stdout.",
      sep = "\n"
    )
  )
}

parse_ref_ids <- function(x) {
  if (is.null(x) || !nzchar(x)) {
    return(character())
  }
  ids <- trimws(strsplit(x, ",", fixed = TRUE)[[1]])
  ids <- ids[nzchar(ids)]
  if (!length(ids)) {
    return(character())
  }
  ids[!duplicated(ids)]
}

read_bib_entries <- function(path) {
  if (!file.exists(path)) {
    return(list(entries = character(), keys = character()))
  }

  lines <- readLines(path, warn = FALSE)
  if (!length(lines)) {
    return(list(entries = character(), keys = character()))
  }

  entries <- character()
  keys <- character()
  i <- 1L
  while (i <= length(lines)) {
    line <- lines[[i]]
    if (!grepl("^\\s*@", line)) {
      i <- i + 1L
      next
    }

    key <- sub("^\\s*@\\w+\\s*\\{\\s*([^,]+),.*$", "\\1", line)
    if (!nzchar(key) || identical(key, line)) {
      stop("Unable to parse BibTeX entry key from line: ", line, call. = FALSE)
    }

    chunk <- character()
    depth <- 0L
    repeat {
      if (i > length(lines)) {
        stop("Unterminated BibTeX entry for key: ", key, call. = FALSE)
      }
      current <- lines[[i]]
      chunk <- c(chunk, current)
      opens <- lengths(regmatches(current, gregexpr("\\{", current, perl = TRUE)))
      closes <- lengths(regmatches(current, gregexpr("\\}", current, perl = TRUE)))
      opens[opens < 0L] <- 0L
      closes[closes < 0L] <- 0L
      depth <- depth + opens - closes
      i <- i + 1L
      if (depth <= 0L) {
        break
      }
    }

    entries <- c(entries, paste(chunk, collapse = "\n"))
    keys <- c(keys, key)
  }

  list(entries = entries, keys = keys)
}

find_rows_by_ref_ids <- function(ref_ids, cfg) {
  missing <- character()
  resolved_rows <- list()

  for (ref in ref_ids) {
    rows <- as.data.table(litxr:::.litxr_task_ref_row_for_keys(cfg, ref, task = "citation"))
    if (!nrow(rows)) {
      missing <- c(missing, ref)
      next
    }
    resolved_rows[[length(resolved_rows) + 1L]] <- rows[1L, ]
  }

  if (length(missing)) {
    stop(
      "The following ref_id(s) were not found in the local reference cache: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  rows <- data.table::rbindlist(resolved_rows, fill = TRUE)
  rows
}

make_bib_entry <- function(row) {
  key <- make_citekey(row)
  lines <- litxr::row_to_bibtex(row)
  lines[[1]] <- sub("^(@[^\\{]+\\{)[^,]+,", paste0("\\1", key, ","), lines[[1]])
  paste(lines, collapse = "\n")
}

make_citekey <- function(row) {
  prefer_doi_key <- if ("prefer_doi_key__" %in% names(row)) isTRUE(row[["prefer_doi_key__"]]) else TRUE
  litxr:::.make_citekey(
    doi = if (prefer_doi_key) row[["doi"]] else NA_character_,
    source_id = row[["source_id"]],
    ref_id = row[["ref_id"]]
  )
}

format_bib_file_lines <- function(entries) {
  if (!length(entries)) {
    return(character())
  }
  unlist(lapply(entries, function(entry) c(entry, "")), use.names = FALSE)
}

parsed <- parse_args(args)

if (isTRUE(parsed$show_help)) {
  usage()
  quit(save = "no", status = 0L)
}

output_path <- parsed$output
ref_ids <- parse_ref_ids(parsed$ref_ids)

if (is.null(output_path) || !nzchar(output_path)) {
  usage()
  stop("`--output` is required.", call. = FALSE)
}
if (!length(ref_ids)) {
  usage()
  stop("`--ref-ids` is required and must contain at least one ref_id.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
config_path <- attr(cfg, "config_path", exact = TRUE)
config_root <- attr(cfg, "config_root", exact = TRUE)
log_line(sprintf(
  "litxr_config_path=%s",
  if (!is.null(config_path) && nzchar(config_path)) config_path else "[unknown]"
))
log_line(sprintf(
  "litxr_project_root=%s",
  if (!is.null(config_root) && nzchar(config_root)) config_root else "[unknown]"
))
rows <- find_rows_by_ref_ids(ref_ids, cfg)
if (!nrow(rows)) {
  stop("No matching references were found for the supplied ref_id(s).", call. = FALSE)
}

new_entries <- vapply(seq_len(nrow(rows)), function(i) make_bib_entry(rows[i, ]), character(1))
new_keys <- vapply(seq_len(nrow(rows)), function(i) make_citekey(rows[i, ]), character(1))
new_ref_ids <- as.character(rows$ref_id)

if (!isTRUE(parsed$append)) {
  if (file.exists(output_path)) {
    stop(
      "Target BibTeX file already exists: ",
      normalizePath(output_path, winslash = "/", mustWork = FALSE),
      ". Use `--append` to add entries to an existing file.",
      call. = FALSE
    )
  }
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  writeLines(format_bib_file_lines(new_entries), output_path)
  log_line(sprintf("created=%s", normalizePath(output_path, winslash = "/", mustWork = FALSE)))
  log_line(sprintf("written_ref_ids=%s", paste(new_ref_ids, collapse = ", ")))
  emit_json(list(
    status = "ok",
    action = "create",
    output = normalizePath(output_path, winslash = "/", mustWork = FALSE),
    written_ref_ids = new_ref_ids
  ))
  quit(save = "no", status = 0L)
}

existing <- read_bib_entries(output_path)
existing_keys <- existing$keys
existing_entries <- existing$entries
initial_existing_keys <- existing_keys

duplicate_idx <- which(new_keys %in% existing_keys)
duplicate_ref_ids <- new_ref_ids[duplicate_idx]

if (length(duplicate_ref_ids)) {
  log_line(sprintf("existing_ref_ids=%s", paste(duplicate_ref_ids, collapse = ", ")))
}

if (isTRUE(parsed$overwrite)) {
  overwritten_ref_ids <- character()
  for (i in seq_along(new_keys)) {
    key <- new_keys[[i]]
    hit <- match(key, existing_keys)
    if (is.na(hit)) {
      existing_keys <- c(existing_keys, key)
      existing_entries <- c(existing_entries, new_entries[[i]])
    } else {
      existing_entries[[hit]] <- new_entries[[i]]
      overwritten_ref_ids <- c(overwritten_ref_ids, new_ref_ids[[i]])
    }
  }
  if (length(overwritten_ref_ids)) {
    log_line(sprintf("overwritten_ref_ids=%s", paste(overwritten_ref_ids, collapse = ", ")))
  }
} else {
  skipped_ref_ids <- character()
  for (i in seq_along(new_keys)) {
    key <- new_keys[[i]]
    if (key %in% existing_keys) {
      skipped_ref_ids <- c(skipped_ref_ids, new_ref_ids[[i]])
    } else {
      existing_keys <- c(existing_keys, key)
      existing_entries <- c(existing_entries, new_entries[[i]])
    }
  }
  if (length(skipped_ref_ids)) {
    log_line(sprintf("skipped_ref_ids=%s", paste(skipped_ref_ids, collapse = ", ")))
  }
}

dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
writeLines(format_bib_file_lines(existing_entries), output_path)
log_line(sprintf("updated=%s", normalizePath(output_path, winslash = "/", mustWork = FALSE)))

emit_json(list(
  status = "ok",
  action = if (isTRUE(parsed$overwrite)) "append_overwrite" else "append",
  output = normalizePath(output_path, winslash = "/", mustWork = FALSE),
  written_ref_ids = if (isTRUE(parsed$overwrite)) new_ref_ids else new_ref_ids[!new_keys %in% initial_existing_keys],
  existing_ref_ids = duplicate_ref_ids,
  skipped_ref_ids = if (isTRUE(parsed$overwrite)) character() else skipped_ref_ids,
  overwritten_ref_ids = if (isTRUE(parsed$overwrite)) overwritten_ref_ids else character()
))
