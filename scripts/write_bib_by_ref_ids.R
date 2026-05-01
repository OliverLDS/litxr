#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

args <- commandArgs(trailingOnly = TRUE)

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

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/write_bib_by_ref_ids.R --output PATH --ref-ids REF1,REF2 [--append] [--overwrite]",
      "",
      "Options:",
      "  --output PATH       Target BibTeX file path.",
      "  --ref-ids IDS       Comma-separated ref_id values to export.",
      "  --ref-id IDS        Alias of --ref-ids.",
      "  --append            Append into an existing BibTeX file instead of creating a new one.",
      "  --overwrite         With --append, replace existing matching entries instead of skipping them.",
      "  -h, --help          Show this help message.",
      "",
      "Notes:",
      "  - All supplied ref_id values must already exist in the local litxr reference cache.",
      "  - Validation uses exact ref_id lookup only and stops if any requested ref_id is missing.",
      "  - Duplicate detection in the target .bib file is based on the BibTeX citekey generated",
      "    from each reference row.",
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
  rows <- litxr::litxr_find_refs(ref_id = ref_ids, config = cfg)
  rows <- as.data.table(rows)
  if (!nrow(rows)) {
    return(rows)
  }

  rows[, source_id_chr__ := if ("source_id" %in% names(rows)) as.character(source_id) else NA_character_]
  ordered_idx <- integer()
  missing <- character()

  for (ref in ref_ids) {
    expanded <- unique(as.character(litxr:::.litxr_expand_reference_keys(ref)))
    hit <- which(rows$ref_id %in% expanded | rows$source_id_chr__ %in% expanded)
    if (!length(hit)) {
      missing <- c(missing, ref)
      next
    }
    ordered_idx <- c(ordered_idx, hit[[1]])
  }

  if (length(missing)) {
    stop(
      "The following ref_id(s) were not found in the local reference cache: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  rows <- rows[ordered_idx, ]
  rows[, source_id_chr__ := NULL]
  rows <- prefer_published_rows(rows, cfg)
  rows
}

has_value <- function(x) {
  !is.null(x) && length(x) == 1L && !is.na(x) && nzchar(as.character(x))
}

published_metadata_score <- function(row) {
  fields <- c("journal", "container_title", "publisher", "volume", "issue", "pages")
  sum(vapply(fields, function(name) has_value(row[[name]]), logical(1)))
}

prefer_published_rows <- function(rows, cfg) {
  if (!nrow(rows) || !"source" %in% names(rows) || !"doi" %in% names(rows)) {
    rows[, prefer_doi_key__ := TRUE]
    return(rows)
  }

  replace_row <- function(row) {
    doi_value <- as.character(row[["doi"]])
    if (!identical(as.character(row[["source"]]), "arxiv") || is.na(doi_value) || !nzchar(doi_value)) {
      row[["prefer_doi_key__"]] <- TRUE
      return(row)
    }

    published <- as.data.table(litxr::litxr_find_refs(ref_id = paste0("doi:", doi_value), config = cfg))
    if (!nrow(published)) {
      published <- as.data.table(litxr::litxr_find_refs(doi = doi_value, config = cfg))
      if (nrow(published) && "source" %in% names(published)) {
        published <- published[published$source != "arxiv", ]
      }
    }
    if (!nrow(published)) {
      row[["prefer_doi_key__"]] <- FALSE
      return(row)
    }

    scores <- vapply(seq_len(nrow(published)), function(i) published_metadata_score(published[i, ]), integer(1))
    published <- published[order(-scores)]
    best <- published[1, ]
    if (published_metadata_score(best) <= 0L) {
      row[["prefer_doi_key__"]] <- FALSE
      return(row)
    }

    best[["prefer_doi_key__"]] <- TRUE
    best
  }

  rbindlist(lapply(seq_len(nrow(rows)), function(i) replace_row(rows[i, ])), fill = TRUE, use.names = TRUE)
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
  cat(sprintf("created=%s\n", normalizePath(output_path, winslash = "/", mustWork = FALSE)))
  cat(sprintf("written_ref_ids=%s\n", paste(new_ref_ids, collapse = ", ")))
  quit(save = "no", status = 0L)
}

existing <- read_bib_entries(output_path)
existing_keys <- existing$keys
existing_entries <- existing$entries

duplicate_idx <- which(new_keys %in% existing_keys)
duplicate_ref_ids <- new_ref_ids[duplicate_idx]

if (length(duplicate_ref_ids)) {
  cat(sprintf("existing_ref_ids=%s\n", paste(duplicate_ref_ids, collapse = ", ")))
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
    cat(sprintf("overwritten_ref_ids=%s\n", paste(overwritten_ref_ids, collapse = ", ")))
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
    cat(sprintf("skipped_ref_ids=%s\n", paste(skipped_ref_ids, collapse = ", ")))
  }
}

dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
writeLines(format_bib_file_lines(existing_entries), output_path)
cat(sprintf("updated=%s\n", normalizePath(output_path, winslash = "/", mustWork = FALSE)))
