#!/usr/bin/env Rscript

emit_json <- function(x) {
  writeLines(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE), stdout())
}

parse_args <- function(args) {
  out <- list(pairs = character(), apply = FALSE, help = FALSE)
  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (arg %in% c("-h", "--help")) {
      out$help <- TRUE
      i <- i + 1L
    } else if (identical(arg, "--pair")) {
      if (i == length(args)) stop("Missing value for --pair", call. = FALSE)
      out$pairs <- c(out$pairs, args[[i + 1L]])
      i <- i + 2L
    } else if (identical(arg, "--apply")) {
      out$apply <- TRUE
      i <- i + 1L
    } else {
      stop("Unknown argument: ", arg, call. = FALSE)
    }
  }
  out
}

usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript scripts/migrate_arxiv_datacite_doi_records.R --pair ARXIV_ID,ARXIV_DATACITE_DOI [--pair ...] [--apply]",
    "",
    "Behavior:",
    "  - Validates an existing arXiv JSON and a duplicate unclassified DOI JSON.",
    "  - Adds the arXiv/DOI identity pair and records it in the manual identity log.",
    "  - Rekeys any DOI digest to the bare arXiv id, preserving prior digests in history.",
    "  - Removes the duplicate DOI JSON and its ref_doi.fst row.",
    "  - Defaults to a dry run; pass --apply to write changes.",
    sep = "\n"
  ))
}

normalize_pair <- function(x) {
  pieces <- trimws(strsplit(as.character(x), ",", fixed = TRUE)[[1L]])
  if (length(pieces) != 2L || any(!nzchar(pieces))) {
    stop("Each --pair must be ARXIV_ID,ARXIV_DATACITE_DOI.", call. = FALSE)
  }
  arxiv_id <- litxr:::.litxr_bare_arxiv_id(ref_id = pieces[[1L]])
  doi <- litxr:::.litxr_bare_doi(doi = pieces[[2L]])
  if (is.na(arxiv_id) || is.na(doi) || !startsWith(tolower(doi), "10.48550/arxiv.")) {
    stop("Each pair must contain a bare arXiv id and its 10.48550/arXiv.* DOI.", call. = FALSE)
  }
  list(arxiv_id = arxiv_id, doi = tolower(doi))
}

digest_time <- function(digest) {
  value <- digest$updated_at
  if (is.null(value) || !length(value)) value <- digest$generated_at
  if (is.null(value) || !length(value)) value <- NA_character_
  value <- as.character(value)
  parsed <- suppressWarnings(as.POSIXct(value[[1L]], tz = "UTC"))
  if (is.na(parsed)) as.POSIXct("1970-01-01", tz = "UTC") else parsed
}

append_manual_identity_pair <- function(cfg, arxiv_id, doi) {
  path <- file.path(litxr:::.litxr_project_log_dir(cfg), "manual_ref_identity_pairs.tsv")
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  existing <- if (file.exists(path)) {
    data.table::fread(path, sep = "\t", showProgress = FALSE)
  } else {
    data.table::data.table(arxiv_id = character(), doi = character())
  }
  if (!all(c("arxiv_id", "doi") %in% names(existing))) {
    stop("Manual identity log has an invalid schema: ", path, call. = FALSE)
  }
  exists <- as.character(existing$arxiv_id) == arxiv_id & tolower(as.character(existing$doi)) == doi
  if (!any(exists)) {
    data.table::fwrite(
      data.table::data.table(arxiv_id = arxiv_id, doi = doi),
      path,
      sep = "\t",
      append = file.exists(path),
      col.names = !file.exists(path)
    )
  }
  invisible(path)
}

archive_digest <- function(cfg, arxiv_id, digest) {
  history_dir <- litxr:::.litxr_llm_history_ref_dir(cfg, arxiv_id)
  dir.create(history_dir, recursive = TRUE, showWarnings = FALSE)
  path <- litxr:::.litxr_llm_digest_history_path(cfg, arxiv_id, digest)
  if (file.exists(path)) {
    stop("Digest history collision at ", path, call. = FALSE)
  }
  litxr:::.litxr_write_json_atomic(digest, path)
  path
}

migrate_pair <- function(cfg, pair, apply) {
  arxiv_path_rows <- litxr:::.litxr_ref_json_locations_from_thin_stores(cfg, pair$arxiv_id)
  arxiv_path_rows <- arxiv_path_rows[as.character(arxiv_path_rows$ref_id) == pair$arxiv_id, ]
  if (nrow(arxiv_path_rows) != 1L || !file.exists(arxiv_path_rows$json_path[[1L]])) {
    stop("Expected exactly one local arXiv JSON for ", pair$arxiv_id, call. = FALSE)
  }

  doi_index_path <- litxr:::.litxr_ref_doi_path(cfg)
  doi_rows <- data.table::as.data.table(fst::read_fst(doi_index_path, as.data.table = TRUE))
  doi_hit <- tolower(as.character(doi_rows$doi)) == pair$doi
  if (sum(doi_hit) != 1L) {
    stop("Expected exactly one ref_doi.fst row for ", pair$doi, call. = FALSE)
  }
  doi_row <- doi_rows[doi_hit, ]
  collections <- litxr:::.litxr_config_collections(cfg)
  doi_collection <- as.character(collections[[as.integer(doi_row$collection_index[[1L]])]]$collection_id)
  doi_json_path <- file.path(litxr:::.litxr_collection_ref_dir(cfg, doi_collection), doi_row$json_filename[[1L]])
  if (!identical(doi_collection, "unclassified_doi") || !file.exists(doi_json_path)) {
    stop("DOI record is not a removable unclassified duplicate: ", pair$doi, call. = FALSE)
  }

  digest_index <- litxr:::.litxr_read_llm_digest_index(cfg)
  doi_digest_hit <- digest_index$ref_id == pair$doi
  doi_digest_path <- if (sum(doi_digest_hit) == 1L) {
    file.path(litxr:::.litxr_project_llm_dir(cfg), digest_index$json_filename[[which(doi_digest_hit)]])
  } else {
    NA_character_
  }
  doi_digest <- if (!is.na(doi_digest_path) && file.exists(doi_digest_path)) {
    jsonlite::read_json(doi_digest_path, simplifyVector = FALSE)
  } else {
    NULL
  }
  arxiv_digest <- litxr::litxr_read_llm_digest(pair$arxiv_id, cfg)
  selected <- if (is.null(doi_digest)) arxiv_digest else if (is.null(arxiv_digest) || digest_time(doi_digest) >= digest_time(arxiv_digest)) doi_digest else arxiv_digest
  if (is.null(selected)) {
    stop("No digest is available to preserve for ", pair$arxiv_id, call. = FALSE)
  }

  identity <- data.table::as.data.table(litxr::litxr_read_ref_identity_map(cfg))
  exact_identity <- nrow(identity) && any(as.character(identity$arxiv_id) == pair$arxiv_id & tolower(as.character(identity$doi)) == pair$doi)
  conflicting_identity <- nrow(identity) && any(
    (as.character(identity$arxiv_id) == pair$arxiv_id | tolower(as.character(identity$doi)) == pair$doi) &
      !(as.character(identity$arxiv_id) == pair$arxiv_id & tolower(as.character(identity$doi)) == pair$doi)
  )
  if (conflicting_identity) {
    stop("Identity map contains a conflicting pair for ", pair$arxiv_id, " or ", pair$doi, call. = FALSE)
  }

  result <- list(
    arxiv_id = pair$arxiv_id,
    doi = pair$doi,
    arxiv_json = arxiv_path_rows$json_path[[1L]],
    duplicate_doi_json = doi_json_path,
    digest_source = if (identical(selected, doi_digest)) "doi" else "arxiv",
    had_arxiv_digest = !is.null(arxiv_digest),
    had_doi_digest = !is.null(doi_digest),
    identity_added = !exact_identity,
    applied = isTRUE(apply)
  )
  if (!isTRUE(apply)) return(result)

  if (!exact_identity) {
    litxr::litxr_add_ref_identity_pair(pair$arxiv_id, pair$doi, cfg)
  }
  append_manual_identity_pair(cfg, pair$arxiv_id, pair$doi)

  if (!is.null(doi_digest)) {
    archive_digest(cfg, pair$arxiv_id, doi_digest)
  }
  litxr::litxr_write_llm_digest(
    pair$arxiv_id,
    selected,
    cfg,
    keep_history = !is.null(arxiv_digest),
    bump_revision = !is.null(arxiv_digest)
  )
  if (!is.null(doi_digest)) {
    unlink(doi_digest_path)
    digest_index <- litxr:::.litxr_read_llm_digest_index(cfg)
    digest_index <- digest_index[digest_index$ref_id != pair$doi, ]
    replacement_row <- litxr:::.litxr_llm_digest_index_row(
      cfg,
      pair$arxiv_id,
      json_filename = basename(litxr:::.litxr_llm_digest_path(cfg, pair$arxiv_id)),
      history_dir = basename(litxr:::.litxr_llm_history_ref_dir(cfg, pair$arxiv_id))
    )
    digest_index <- data.table::rbindlist(list(digest_index, replacement_row), fill = TRUE)
    digest_index <- digest_index[!duplicated(ref_id, fromLast = TRUE), ]
    litxr:::.litxr_write_llm_digest_index(cfg, digest_index)
  }

  doi_rows <- doi_rows[!doi_hit, ]
  litxr:::.litxr_write_fst_atomic(as.data.frame(doi_rows), doi_index_path)
  unlink(doi_json_path)
  result
}

parsed <- parse_args(commandArgs(trailingOnly = TRUE))
if (parsed$help) {
  usage()
  quit(save = "no", status = 0L)
}
if (!length(parsed$pairs)) {
  usage()
  stop("At least one --pair is required.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
result <- lapply(parsed$pairs, function(x) migrate_pair(cfg, normalize_pair(x), parsed$apply))
emit_json(list(status = "ok", applied = parsed$apply, migrations = result))
