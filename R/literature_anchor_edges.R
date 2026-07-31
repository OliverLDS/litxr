.litxr_empty_literature_anchor_edges <- function() {
  data.table::data.table(
    source_ref_id = character(),
    target_ref_id = character(),
    anchor_ref_id = character(),
    anchor_rank = integer(),
    anchor_role = character(),
    relationship = character(),
    confidence = character()
  )
}

.litxr_read_literature_anchor_edges <- function(cfg, include_manual = TRUE) {
  path <- .litxr_project_literature_anchor_edges_path(cfg)
  rows <- if (file.exists(path)) {
    tryCatch(
      fst::read_fst(path, as.data.table = TRUE),
      error = function(e) .litxr_empty_literature_anchor_edges()
    )
  } else {
    .litxr_empty_literature_anchor_edges()
  }
  rows <- data.table::as.data.table(rows)
  required <- names(.litxr_empty_literature_anchor_edges())
  if (!nrow(rows) || !all(required %in% names(rows))) {
    rows <- .litxr_empty_literature_anchor_edges()
  }
  rows <- rows[, required, with = FALSE]
  char_cols <- setdiff(required, "anchor_rank")
  rows[, (char_cols) := lapply(.SD, as.character), .SDcols = char_cols]
  rows$anchor_rank <- suppressWarnings(as.integer(rows$anchor_rank))
  rows <- rows[
    !is.na(source_ref_id) & nzchar(source_ref_id) &
      !is.na(target_ref_id) & nzchar(target_ref_id),
    
  ]
  if (isTRUE(include_manual)) {
    manual_path <- file.path(.litxr_project_log_dir(cfg), "manual_literature_anchor_edges.tsv")
    if (file.exists(manual_path)) {
      manual <- data.table::fread(
        manual_path,
        sep = "\t",
        header = TRUE,
        colClasses = "character",
        showProgress = FALSE
      )
      manual_columns <- c("source_ref_id", "target_ref_id", "anchor_role", "relationship")
      if (!identical(names(manual), manual_columns)) {
        stop(
          "Manual literature anchor edge log must contain exactly: ",
          paste(manual_columns, collapse = ", "),
          call. = FALSE
        )
      }
      if (nrow(manual)) {
        manual[, row_order__ := seq_len(.N)]
        manual[, source_ref_id := tolower(trimws(as.character(source_ref_id)))]
        manual[, target_ref_id := tolower(trimws(as.character(target_ref_id)))]
        manual[, anchor_role := trimws(as.character(anchor_role))]
        manual[, relationship := trimws(as.character(relationship))]
        id_ok <- grepl("^[0-9]{4}\\.[0-9]{4,5}$", manual$source_ref_id) |
          grepl("^10\\.[^[:space:]/]+/.+$", manual$source_ref_id)
        target_ok <- grepl("^[0-9]{4}\\.[0-9]{4,5}$", manual$target_ref_id) |
          grepl("^10\\.[^[:space:]/]+/.+$", manual$target_ref_id)
        if (!all(id_ok & target_ok)) {
          stop("Manual literature anchor edge log requires bare arXiv or DOI ids.", call. = FALSE)
        }
        role_ok <- manual$anchor_role %in% .litxr_anchor_reference_levels("v5")
        relationship_ok <- manual$relationship %in% c(
          "builds_on", "extends", "tests", "applies", "compares_with", "contradicts",
          "critiques", "replicates", "generalizes", "narrows", "uses_as_context", "unknown"
        )
        if (!all(role_ok)) stop("Manual literature anchor edge log contains an invalid anchor_role.", call. = FALSE)
        if (!all(relationship_ok)) stop("Manual literature anchor edge log contains an invalid relationship.", call. = FALSE)
        manual <- manual[!duplicated(paste(source_ref_id, target_ref_id, sep = "\r"), fromLast = TRUE), ]
        data.table::setorder(manual, row_order__)
        manual[, anchor_rank := seq_len(.N), by = source_ref_id]
        manual <- manual[, .(
          source_ref_id,
          target_ref_id,
          anchor_ref_id = target_ref_id,
          anchor_rank,
          anchor_role,
          relationship,
          confidence = NA_character_
        )]
        if (nrow(manual)) {
          manual_key <- paste(manual$source_ref_id, manual$target_ref_id, sep = "\r")
          auto_key <- paste(rows$source_ref_id, rows$target_ref_id, sep = "\r")
          rows <- data.table::rbindlist(list(rows[!auto_key %in% manual_key, ], manual), use.names = TRUE)
        }
      }
    }
  }
  data.table::setorder(rows, source_ref_id, anchor_rank, target_ref_id)
  rows[]
}

.litxr_write_literature_anchor_edges <- function(cfg, rows) {
  path <- .litxr_project_literature_anchor_edges_path(cfg)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  rows <- data.table::as.data.table(rows)
  template <- .litxr_empty_literature_anchor_edges()
  required <- names(template)
  if (!all(required %in% names(rows))) {
    stop("Literature anchor edge rows are missing columns: ", paste(setdiff(required, names(rows)), collapse = ", "), call. = FALSE)
  }
  rows <- rows[, required, with = FALSE]
  char_cols <- setdiff(required, "anchor_rank")
  rows[, (char_cols) := lapply(.SD, as.character), .SDcols = char_cols]
  rows$anchor_rank <- suppressWarnings(as.integer(rows$anchor_rank))
  rows <- rows[
    !is.na(source_ref_id) & nzchar(source_ref_id) &
      !is.na(target_ref_id) & nzchar(target_ref_id),
    
  ]
  if (nrow(rows)) {
    data.table::setorder(rows, source_ref_id, anchor_rank, target_ref_id)
    edge_key <- paste(rows$source_ref_id, rows$target_ref_id, rows$anchor_rank, sep = "\r")
    rows <- rows[!duplicated(edge_key, fromLast = TRUE), ]
  } else {
    rows <- template
  }
  .litxr_write_fst_atomic(rows, path)
  invisible(path)
}

.litxr_literature_anchor_edges_from_digests <- function(cfg, ref_ids = NULL) {
  digest_index <- .litxr_read_llm_digest_index(cfg)
  if (!nrow(digest_index)) return(.litxr_empty_literature_anchor_edges())
  if (!is.null(ref_ids)) {
    ref_ids <- unique(vapply(as.character(ref_ids), .litxr_llm_digest_index_key, character(1L)))
    ref_ids <- ref_ids[!is.na(ref_ids) & nzchar(ref_ids)]
    digest_index <- digest_index[digest_index$ref_id %in% ref_ids, ]
  }
  digest_index <- digest_index[!is.na(digest_index$json_filename) & nzchar(digest_index$json_filename), ]
  if (!nrow(digest_index)) return(.litxr_empty_literature_anchor_edges())

  identity_map <- .litxr_read_project_ref_identity_index(cfg, columns = c("arxiv_id", "doi"))
  cached_ids <- digest_index$ref_id
  identity_map <- identity_map[
    !is.na(arxiv_id) & nzchar(arxiv_id) & arxiv_id %in% cached_ids &
      !is.na(doi) & nzchar(doi),
    c("arxiv_id", "doi"),
    with = FALSE
  ]
  doi_to_cached_arxiv <- if (nrow(identity_map)) {
    identity_map <- identity_map[!duplicated(identity_map$doi), ]
    stats::setNames(as.character(identity_map$arxiv_id), as.character(identity_map$doi))
  } else {
    character()
  }

  paths <- file.path(.litxr_project_llm_dir(cfg), basename(digest_index$json_filename))
  parsed <- lapply(paths, function(path) {
    if (!file.exists(path)) return(NULL)
    tryCatch(.litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE)), error = function(e) NULL)
  })
  rows <- Map(function(digest, source_ref_id) {
    if (is.null(digest) || is.null(digest$anchor_references) || !length(digest$anchor_references)) return(NULL)
    anchors <- data.table::as.data.table(digest$anchor_references)
    if (!nrow(anchors) || !"anchor_ref_id" %in% names(anchors)) return(NULL)
    anchor_ref_id <- vapply(as.character(anchors$anchor_ref_id), .litxr_llm_digest_index_key, character(1L))
    target_ref_id <- anchor_ref_id
    cached_arxiv <- unname(doi_to_cached_arxiv[anchor_ref_id])
    use_cached_arxiv <- !is.na(cached_arxiv) & nzchar(cached_arxiv)
    target_ref_id[use_cached_arxiv] <- cached_arxiv[use_cached_arxiv]
    keep <- !is.na(target_ref_id) & nzchar(target_ref_id)
    if (!any(keep)) return(NULL)
    column <- function(name) if (name %in% names(anchors)) as.character(anchors[[name]]) else rep(NA_character_, nrow(anchors))
    data.table::data.table(
      source_ref_id = source_ref_id,
      target_ref_id = target_ref_id[keep],
      anchor_ref_id = anchor_ref_id[keep],
      anchor_rank = suppressWarnings(as.integer(column("anchor_rank")[keep])),
      anchor_role = column("anchor_role")[keep],
      relationship = column("relationship_to_current_paper")[keep],
      confidence = column("confidence")[keep]
    )
  }, parsed, digest_index$ref_id)
  rows <- rows[!vapply(rows, is.null, logical(1L))]
  if (!length(rows)) return(.litxr_empty_literature_anchor_edges())
  data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)
}

.litxr_sync_literature_anchor_edges <- function(cfg, mode = c("incremental", "full"), ref_ids = NULL) {
  mode <- match.arg(mode)
  incoming <- .litxr_literature_anchor_edges_from_digests(cfg, if (identical(mode, "full")) NULL else ref_ids)
  if (identical(mode, "full")) {
    .litxr_write_literature_anchor_edges(cfg, incoming)
    digest_index <- .litxr_read_llm_digest_index(cfg)
    digests_scanned <- sum(!is.na(digest_index$json_filename) & nzchar(digest_index$json_filename))
    return(list(
      path = .litxr_project_literature_anchor_edges_path(cfg),
      mode = mode,
      digests_scanned = as.integer(digests_scanned),
      edges_removed = NA_integer_,
      edges_added = nrow(incoming),
      edges_total = nrow(incoming)
    ))
  }

  touched <- unique(vapply(as.character(ref_ids %||% character()), .litxr_llm_digest_index_key, character(1L)))
  touched <- touched[!is.na(touched) & nzchar(touched)]
  if (!length(touched)) {
    existing <- .litxr_read_literature_anchor_edges(cfg, include_manual = FALSE)
    return(list(
      path = .litxr_project_literature_anchor_edges_path(cfg),
      mode = mode,
      digests_scanned = 0L,
      edges_removed = 0L,
      edges_added = 0L,
      edges_total = nrow(existing)
    ))
  }
  existing <- .litxr_read_literature_anchor_edges(cfg, include_manual = FALSE)
  edges_removed <- sum(existing$source_ref_id %in% touched)
  retained <- existing[!existing$source_ref_id %in% touched, ]
  merged <- data.table::rbindlist(list(retained, incoming), use.names = TRUE, fill = TRUE)
  .litxr_write_literature_anchor_edges(cfg, merged)
  list(
    path = .litxr_project_literature_anchor_edges_path(cfg),
    mode = mode,
    digests_scanned = length(touched),
    edges_removed = as.integer(edges_removed),
    edges_added = nrow(incoming),
    edges_total = nrow(merged)
  )
}
