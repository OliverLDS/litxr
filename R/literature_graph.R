#' Build a bounded literature relationship graph from digest anchors
#'
#' Builds a directed graph where each edge points from a paper to one of its
#' anchored references. Traversal expands only through digests recorded in the
#' thin LLM digest index; unresolved anchors remain visible external nodes.
#'
#' @param ref_ids Bare reference ids used as graph roots.
#' @param config Optional parsed config list or config path.
#' @param max_depth Maximum number of anchor hops from a root. Default: `2`.
#' @param max_nodes Maximum number of returned nodes. Default: `100`.
#'
#' @return A list with `meta`, `nodes`, and `edges` data tables.
#' @export
litxr_build_literature_graph <- function(ref_ids, config = NULL, max_depth = 2L, max_nodes = 100L) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  root_ids <- vapply(as.character(ref_ids), .litxr_llm_digest_index_key, character(1))
  root_ids <- unique(root_ids[!is.na(root_ids) & nzchar(root_ids)])
  if (!length(root_ids)) stop("`ref_ids` must contain at least one bare reference id.", call. = FALSE)

  max_depth <- suppressWarnings(as.integer(max_depth[[1L]]))
  max_nodes <- suppressWarnings(as.integer(max_nodes[[1L]]))
  if (is.na(max_depth) || max_depth < 0L) stop("`max_depth` must be a non-negative integer.", call. = FALSE)
  if (is.na(max_nodes) || max_nodes < length(root_ids)) stop("`max_nodes` must be at least the number of root ids.", call. = FALSE)

  index <- .litxr_read_llm_digest_index(cfg)
  index <- index[!is.na(index$json_filename) & nzchar(index$json_filename), ]
  index_hit <- match(root_ids, index$ref_id)
  root_cached <- !is.na(index_hit)
  nodes <- data.table::data.table(
    id = root_ids,
    ref_id = root_ids,
    node_type = ifelse(root_cached, "cached", "external"),
    title = NA_character_,
    summary = NA_character_,
    theoretical_mechanism = NA_character_,
    github_urls = NA_character_,
    depth = 0L,
    is_root = TRUE,
    traversable = root_cached & max_depth > 0L
  )
  root_titles <- .litxr_literature_graph_root_titles(cfg, root_ids)
  title_hit <- match(nodes$ref_id, root_titles$ref_id)
  nodes$title[!is.na(title_hit)] <- root_titles$title[title_hit[!is.na(title_hit)]]
  edges <- data.table::data.table(
    id = character(), source = character(), target = character(),
    anchor_title = character(), anchor_role = character(), relationship = character(), confidence = character(), reason = character()
  )
  frontier <- root_ids[root_cached]
  seen_cached <- frontier
  truncated_nodes <- 0L

  if (nrow(index) && length(frontier) && max_depth > 0L) {
    for (depth in seq_len(max_depth)) {
      if (!length(frontier)) break
      frontier_hit <- match(frontier, index$ref_id)
      frontier_rows <- index[frontier_hit[!is.na(frontier_hit)], ]
      paths <- file.path(.litxr_project_llm_dir(cfg), basename(frontier_rows$json_filename))
      digests <- lapply(paths, function(path) {
        if (!file.exists(path)) return(NULL)
        tryCatch(.litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE)), error = function(e) NULL)
      })
      details <- .litxr_literature_graph_digest_details(digests, frontier_rows$ref_id)
      if (nrow(details)) {
        detail_hit <- match(nodes$ref_id, details$ref_id)
        use_details <- !is.na(detail_hit)
        nodes$summary[use_details] <- details$summary[detail_hit[use_details]]
        nodes$theoretical_mechanism[use_details] <- details$theoretical_mechanism[detail_hit[use_details]]
        nodes$github_urls[use_details] <- details$github_urls[detail_hit[use_details]]
      }
      anchors <- Map(.litxr_literature_graph_anchor_rows, digests, frontier_rows$ref_id)
      anchors <- anchors[vapply(anchors, nrow, integer(1L)) > 0L]
      if (!length(anchors)) {
        frontier <- character()
        next
      }
      candidates <- data.table::rbindlist(anchors, use.names = TRUE)
      candidate_ids <- unique(candidates$target)
      existing <- match(candidate_ids, nodes$ref_id)
      add_ids <- candidate_ids[is.na(existing)]
      available <- max_nodes - nrow(nodes)
      if (length(add_ids) > available) {
        truncated_nodes <- truncated_nodes + length(add_ids) - available
        add_ids <- head(add_ids, available)
      }
      if (length(add_ids)) {
        add_hit <- match(add_ids, index$ref_id)
        add_cached <- !is.na(add_hit)
        title_hint <- candidates$anchor_title[match(add_ids, candidates$target)]
        additions <- data.table::data.table(
          id = add_ids,
          ref_id = add_ids,
          node_type = ifelse(add_cached, "cached", "external"),
          title = title_hint,
          summary = NA_character_,
          theoretical_mechanism = NA_character_,
          github_urls = NA_character_,
          depth = as.integer(depth),
          is_root = FALSE,
          traversable = add_cached & depth < max_depth
        )
        nodes <- data.table::rbindlist(list(nodes, additions), use.names = TRUE)
      }
      keep_edges <- candidates$target %in% nodes$ref_id
      candidates <- candidates[keep_edges, ]
      if (nrow(candidates)) {
        candidates$id <- paste(candidates$source, candidates$target, seq_len(nrow(candidates)), sep = "->")
        edges <- data.table::rbindlist(list(edges, candidates[, names(edges), with = FALSE]), use.names = TRUE)
      }
      next_cached <- add_ids[!is.na(match(add_ids, index$ref_id))]
      frontier <- setdiff(next_cached, seen_cached)
      seen_cached <- c(seen_cached, frontier)
      if (nrow(nodes) >= max_nodes) break
    }
  }

  if (nrow(edges)) {
    title_hit <- match(nodes$ref_id, edges$target)
    use_hint <- is.na(nodes$title) | !nzchar(nodes$title)
    use_hint <- use_hint & !is.na(title_hit)
    nodes$title[use_hint] <- edges$anchor_title[title_hit[use_hint]]
  }
  data.table::setorder(nodes, depth, ref_id)
  data.table::setorder(edges, source, target, id)
  list(
    meta = list(
      root_ref_ids = root_ids,
      missing_root_ref_ids = root_ids[!root_cached],
      max_depth = max_depth,
      max_nodes = max_nodes,
      returned_nodes = nrow(nodes),
      returned_edges = nrow(edges),
      external_nodes = sum(nodes$node_type == "external"),
      truncated_nodes = truncated_nodes
    ),
    nodes = nodes,
    edges = edges
  )
}

.litxr_literature_graph_digest_details <- function(digests, ref_ids) {
  rows <- lapply(seq_along(digests), function(i) {
    digest <- digests[[i]]
    if (is.null(digest)) return(NULL)
    links <- digest$research_target_github_links
    urls <- if (is.data.frame(links) && "url" %in% names(links)) {
      as.character(links$url)
    } else if (is.list(links)) {
      vapply(links, function(link) as.character(link$url %||% NA_character_)[[1L]], character(1L))
    } else {
      character()
    }
    urls <- unique(urls[!is.na(urls) & nzchar(urls)])
    data.table::data.table(
      ref_id = ref_ids[[i]],
      summary = as.character(digest$summary %||% NA_character_)[[1L]],
      theoretical_mechanism = as.character(digest$theoretical_mechanism %||% NA_character_)[[1L]],
      github_urls = if (length(urls)) paste(urls, collapse = "\n") else NA_character_
    )
  })
  rows <- rows[vapply(rows, is.null, logical(1L)) == FALSE]
  if (!length(rows)) return(data.table::data.table(ref_id = character(), summary = character(), theoretical_mechanism = character(), github_urls = character()))
  data.table::rbindlist(rows, use.names = TRUE)
}

.litxr_literature_graph_root_titles <- function(cfg, ref_ids) {
  collections <- .litxr_config_collections(cfg)
  ref_dirs <- vapply(collections, function(collection) {
    as.character(.litxr_collection_ref_dir(cfg, collection$collection_id %||% collection$journal_id))
  }, character(1L))
  specs <- list(
    list(path = .litxr_ref_arxiv_path(cfg), key = "arxiv_id"),
    list(path = .litxr_ref_doi_path(cfg), key = "doi"),
    list(path = .litxr_ref_isbn_path(cfg), key = "isbn")
  )
  locations <- lapply(specs, function(spec) {
    rows <- .litxr_read_fst_table_safe(spec$path, columns = c(spec$key, "collection_index", "json_filename"))
    if (!nrow(rows) || !all(c(spec$key, "collection_index", "json_filename") %in% names(rows))) return(NULL)
    keys <- as.character(rows[[spec$key]])
    keep <- !is.na(keys) & nzchar(keys) & keys %in% ref_ids
    if (!any(keep)) return(NULL)
    rows <- rows[keep, ]
    collection_index <- suppressWarnings(as.integer(rows$collection_index))
    valid <- !is.na(collection_index) & collection_index >= 1L & collection_index <= length(ref_dirs)
    if (!any(valid)) return(NULL)
    data.table::data.table(
      ref_id = as.character(rows[[spec$key]][valid]),
      json_path = file.path(ref_dirs[collection_index[valid]], as.character(rows$json_filename[valid]))
    )
  })
  locations <- locations[vapply(locations, is.null, logical(1L)) == FALSE]
  if (!length(locations)) return(data.table::data.table(ref_id = character(), title = character()))
  locations <- data.table::rbindlist(locations, use.names = TRUE)
  locations <- locations[!duplicated(locations$ref_id) & file.exists(locations$json_path), ]
  if (!nrow(locations)) return(data.table::data.table(ref_id = character(), title = character()))
  titles <- vapply(locations$json_path, function(path) {
    payload <- tryCatch(jsonlite::fromJSON(path, simplifyVector = FALSE), error = function(e) NULL)
    as.character(payload$title %||% NA_character_)[[1L]]
  }, character(1L))
  data.table::data.table(ref_id = locations$ref_id, title = titles)
}

.litxr_literature_graph_anchor_rows <- function(digest, source_id) {
  empty <- data.table::data.table(
    source = character(), target = character(), anchor_title = character(),
    anchor_role = character(), relationship = character(), confidence = character(), reason = character()
  )
  if (is.null(digest) || is.null(digest$anchor_references) || !length(digest$anchor_references)) return(empty)
  anchors <- digest$anchor_references
  if (!is.data.frame(anchors)) anchors <- data.table::as.data.table(anchors)
  if (!("anchor_ref_id" %in% names(anchors)) || !nrow(anchors)) return(empty)
  target <- vapply(as.character(anchors$anchor_ref_id), .litxr_llm_digest_index_key, character(1))
  keep <- !is.na(target) & nzchar(target)
  if (!any(keep)) return(empty)
  column <- function(name) {
    if (!(name %in% names(anchors))) return(rep(NA_character_, nrow(anchors)))
    as.character(anchors[[name]])
  }
  data.table::data.table(
    source = source_id,
    target = target[keep],
    anchor_title = column("anchor_title")[keep],
    anchor_role = column("anchor_role")[keep],
    relationship = column("relationship_to_current_paper")[keep],
    confidence = column("confidence")[keep],
    reason = column("reason")[keep]
  )
}
