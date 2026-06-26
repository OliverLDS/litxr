#!/usr/bin/env Rscript

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

emit_json <- function(x) {
  writeLines(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE), con = stdout())
}

has_text <- function(x) {
  isTRUE(length(x) == 1L && !is.na(x[[1L]]) && nzchar(trimws(as.character(x[[1L]]))))
}

parse_ids <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) {
    return(character())
  }
  ids <- unique(trimws(unlist(strsplit(x, ",", fixed = TRUE), use.names = FALSE)))
  ids <- ids[nzchar(ids)]
  ids <- vapply(ids, function(id) {
    .litxr <- litxr:::.litxr_bare_arxiv_id(ref_id = id)
    .litxr
  }, character(1))
  ids <- ids[!is.na(ids) & nzchar(ids)]
  unique(ids)
}

parse_args <- function(args) {
  out <- list(help = FALSE, arxiv_ids = character(), batch_size = 50L, force = FALSE)
  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (identical(arg, "-h") || identical(arg, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(arg, "--arxiv-id") || identical(arg, "--arxiv-ids")) {
      if (i == length(args)) stop("Missing value for --arxiv-id", call. = FALSE)
      out$arxiv_ids <- c(out$arxiv_ids, args[[i + 1L]])
      i <- i + 2L
      next
    }
    if (identical(arg, "--batch-size")) {
      if (i == length(args)) stop("Missing value for --batch-size", call. = FALSE)
      out$batch_size <- suppressWarnings(as.integer(args[[i + 1L]]))
      i <- i + 2L
      next
    }
    if (identical(arg, "--force")) {
      out$force <- TRUE
      i <- i + 1L
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
      "  Rscript scripts/fetch_arxiv_ref_json_by_ids.R --arxiv-id ID1,ID2 [... ]",
      "",
      "Options:",
      "  --arxiv-id IDS    Comma-separated arXiv ids or canonical arXiv ref_ids.",
      "  --arxiv-ids IDS   Alias for --arxiv-id.",
      "  --batch-size N    Number of arXiv ids per API request. Default: 50.",
      "  --force           Overwrite existing JSON files.",
      "  -h, --help        Show this help message.",
      "",
      "Behavior:",
      "  - Fetches each arXiv id from the arXiv API.",
      "  - Routes each record by its primary subject.",
      "  - Reuses an existing arXiv collection if it is already registered.",
      "  - Auto-registers a new arXiv collection and folder when needed.",
      "  - Writes JSON files directly under ref/<collection_id>.",
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

arxiv_ids <- parse_ids(parsed$arxiv_ids)
if (!length(arxiv_ids)) {
  stop("At least one arXiv id is required.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
existing_arxiv <- litxr:::.litxr_read_scaffold_table_safe(litxr:::.litxr_ref_arxiv_path(cfg))
existing_ids <- if (nrow(existing_arxiv) && "arxiv_id" %in% names(existing_arxiv)) unique(trimws(as.character(existing_arxiv$arxiv_id))) else character()
existing_ids <- existing_ids[nzchar(existing_ids)]

requested_ids <- unique(arxiv_ids)
fetch_ids <- setdiff(requested_ids, existing_ids)
skipped_existing <- setdiff(requested_ids, fetch_ids)

log_line("fetching arXiv ref JSON by id")
log_line("requested=", length(requested_ids))
log_line("skipped_existing=", length(skipped_existing))
log_line("fetch_ids=", length(fetch_ids))

if (!length(fetch_ids)) {
  emit_json(list(
    status = "ok",
    requested = requested_ids,
    fetched = 0L,
    written = 0L,
    skipped_existing = skipped_existing,
    created_collection_ids = character(),
    written_paths = character()
  ))
  quit(save = "no", status = 0L)
}

batch_size <- suppressWarnings(as.integer(parsed$batch_size[[1L]]))
if (is.na(batch_size) || batch_size < 1L) {
  batch_size <- length(fetch_ids)
}

written_paths <- character()
created_collection_ids <- character()
fetched_rows <- list()

for (start in seq.int(1L, length(fetch_ids), by = batch_size)) {
  batch_ids <- fetch_ids[start:min(start + batch_size - 1L, length(fetch_ids))]
  feed <- litxr::fetch_arxiv_xml(id_vec = batch_ids)
  entries <- xml2::xml_find_all(feed, ".//*[local-name()='entry']")
  if (!length(entries)) {
    next
  }

  batch_rows <- vector("list", length(entries))
  for (entry_idx in seq_along(entries)) {
    entry <- entries[[entry_idx]]
    row <- litxr::parse_arxiv_entry_unified(entry)
    subject <- if (has_text(row$subject_primary)) {
      as.character(row$subject_primary[[1L]])
    } else if (has_text(row$arxiv_primary_category)) {
      as.character(row$arxiv_primary_category[[1L]])
    } else {
      NA_character_
    }
    if (is.na(subject) || !nzchar(subject)) {
      stop("Unable to determine primary subject for arXiv record: ", as.character(row$ref_id[[1L]]), call. = FALSE)
    }

    collection <- litxr:::.litxr_find_arxiv_collection(cfg, subject)
    if (is.null(collection)) {
      registered <- litxr:::.litxr_register_arxiv_collection(cfg, subject)
      cfg <- registered$cfg
      collection <- registered$collection
      created_collection_ids <- unique(c(created_collection_ids, as.character(collection$collection_id)))
    }

    litxr:::.litxr_ensure_collection_ref_dir(cfg, collection$collection_id)

    row$collection_id <- collection$collection_id
    row$collection_title <- collection$title
    batch_rows[[entry_idx]] <- row
  }

  batch_rows <- data.table::rbindlist(batch_rows, fill = TRUE)
  if (!nrow(batch_rows)) {
    next
  }

  if (nrow(batch_rows)) {
    batch_rows <- batch_rows[!duplicated(batch_rows$ref_id), ]
  }

  for (i in seq_len(nrow(batch_rows))) {
    row <- batch_rows[i, ]
    collection <- litxr:::.litxr_find_arxiv_collection(cfg, row$subject_primary[[1L]])
    if (is.null(collection)) {
      collection <- litxr:::.litxr_find_arxiv_collection(cfg, row$arxiv_primary_category[[1L]])
    }
    if (is.null(collection)) {
      stop("Unable to resolve arXiv collection for: ", as.character(row$ref_id[[1L]]), call. = FALSE)
    }

    collection_dir <- litxr:::.litxr_resolve_local_path(cfg, collection$local_path)
    litxr:::.litxr_ensure_collection_ref_dir(cfg, collection$collection_id)
    json_path <- file.path(collection_dir, paste0(litxr:::.litxr_record_slug(data.table::data.table(ref_id = row$ref_id[[1L]], doi = NA_character_)), ".json"))
    if (!isTRUE(parsed$force) && file.exists(json_path)) {
      next
    }

    payload <- litxr:::.litxr_row_to_storage_payload(row, collection)
    jsonlite::write_json(payload, json_path, auto_unbox = TRUE, pretty = TRUE, null = "null")
    written_paths <- c(written_paths, json_path)
    fetched_rows[[length(fetched_rows) + 1L]] <- row
  }
}

emit_json(list(
  status = "ok",
  requested = requested_ids,
  fetched = length(fetched_rows),
  written = length(written_paths),
  skipped_existing = skipped_existing,
  created_collection_ids = unique(created_collection_ids),
  written_paths = written_paths
))
