#' Build a bounded literature relationship graph from digest anchors
#'
#' Builds a directed graph where each edge points from a paper to one of its
#' anchored references. Traversal reads the thin LLM digest and literature
#' anchor-edge indexes once. Upward roots follow anchored references; downward
#' roots follow locally cached papers that cite them. DOI anchors linked through
#' the thin identity map use a cached arXiv digest when one is available;
#' unresolved upward anchors remain visible external nodes.
#'
#' @param ref_ids Deprecated alias for `upward_ref_ids`.
#' @param upward_ref_ids Bare reference ids whose anchored references are traced.
#' @param downward_ref_ids Bare reference ids whose local citing papers are traced.
#' @param config Optional parsed config list or config path.
#' @param max_depth Maximum number of hops in each direction from a root.
#'   Default: `2`.
#' @param max_nodes Maximum number of returned nodes. Default: `100`.
#'
#' @return A list with `meta`, `nodes`, and `edges` data tables.
#' @export
litxr_build_literature_graph <- function(ref_ids = NULL, config = NULL, max_depth = 2L, max_nodes = 100L, upward_ref_ids = NULL, downward_ref_ids = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  normalize_ids <- function(x) {
    if (is.null(x) || !length(x)) return(character())
    ids <- vapply(as.character(x), .litxr_llm_digest_index_key, character(1L))
    unique(ids[!is.na(ids) & nzchar(ids)])
  }
  upward_ids <- unique(c(normalize_ids(ref_ids), normalize_ids(upward_ref_ids)))
  downward_ids <- normalize_ids(downward_ref_ids)
  root_ids <- unique(c(upward_ids, downward_ids))
  if (!length(root_ids)) stop("Supply at least one `upward_ref_ids` or `downward_ref_ids` value.", call. = FALSE)

  max_depth <- suppressWarnings(as.integer(max_depth[[1L]]))
  max_nodes <- suppressWarnings(as.integer(max_nodes[[1L]]))
  if (is.na(max_depth) || max_depth < 0L) stop("`max_depth` must be a non-negative integer.", call. = FALSE)
  if (is.na(max_nodes) || max_nodes < length(root_ids)) stop("`max_nodes` must be at least the number of root ids.", call. = FALSE)

  index <- .litxr_read_llm_digest_index(cfg)
  index <- index[!is.na(index$json_filename) & nzchar(index$json_filename), ]
  anchor_edges <- .litxr_read_literature_anchor_edges(cfg)
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
    anchor_ref_id = character(), anchor_role = character(), relationship = character(),
    confidence = character()
  )
  upward_frontier <- upward_ids[upward_ids %in% index$ref_id]
  downward_frontier <- downward_ids[downward_ids %in% index$ref_id]
  frontier_parts <- list()
  if (length(upward_frontier)) {
    frontier_parts[[length(frontier_parts) + 1L]] <- data.table::data.table(ref_id = upward_frontier, direction = "upward")
  }
  if (length(downward_frontier)) {
    frontier_parts[[length(frontier_parts) + 1L]] <- data.table::data.table(ref_id = downward_frontier, direction = "downward")
  }
  frontier <- if (length(frontier_parts)) {
    data.table::rbindlist(frontier_parts, use.names = TRUE)
  } else {
    data.table::data.table(ref_id = character(), direction = character())
  }
  visited_upward <- frontier$ref_id[frontier$direction == "upward"]
  visited_downward <- frontier$ref_id[frontier$direction == "downward"]
  truncated_nodes <- 0L

  if (nrow(index) && nrow(anchor_edges) && nrow(frontier) && max_depth > 0L) {
    for (depth in seq_len(max_depth)) {
      if (!nrow(frontier)) break
      next_frontier <- list()
      directions <- if (depth %% 2L) c("upward", "downward") else c("downward", "upward")
      for (direction in directions) {
        current <- frontier$ref_id[frontier$direction == direction]
        if (!length(current)) next
        candidates <- if (identical(direction, "upward")) {
          anchor_edges[anchor_edges$source_ref_id %in% current, ]
        } else {
          anchor_edges[anchor_edges$target_ref_id %in% current, ]
        }
        if (!nrow(candidates)) next
        candidates$next_ref_id <- if (identical(direction, "upward")) candidates$target_ref_id else candidates$source_ref_id
        data.table::setorder(candidates, anchor_rank, source_ref_id, target_ref_id)
        add_ids <- unique(candidates$next_ref_id[!candidates$next_ref_id %in% nodes$ref_id])
        available <- max_nodes - nrow(nodes)
        if (length(add_ids) > available) {
          truncated_nodes <- truncated_nodes + length(add_ids) - available
          add_ids <- head(add_ids, available)
        }
        if (length(add_ids)) {
          add_hit <- match(add_ids, index$ref_id)
          nodes <- data.table::rbindlist(list(nodes, data.table::data.table(
            id = add_ids,
            ref_id = add_ids,
            node_type = ifelse(!is.na(add_hit), "cached", "external"),
            title = NA_character_,
            summary = NA_character_,
            theoretical_mechanism = NA_character_,
            github_urls = NA_character_,
            depth = as.integer(depth),
            is_root = FALSE,
            traversable = !is.na(add_hit) & depth < max_depth
          )), use.names = TRUE)
        }
        candidates <- candidates[candidates$source_ref_id %in% nodes$ref_id & candidates$target_ref_id %in% nodes$ref_id, ]
        if (nrow(candidates)) {
          candidates$id <- paste(candidates$source_ref_id, candidates$target_ref_id, candidates$anchor_rank, seq_len(nrow(candidates)), sep = "->")
          edges <- data.table::rbindlist(list(edges, data.table::data.table(
            id = candidates$id,
            source = candidates$source_ref_id,
            target = candidates$target_ref_id,
            anchor_ref_id = candidates$anchor_ref_id,
            anchor_role = candidates$anchor_role,
            relationship = candidates$relationship,
            confidence = candidates$confidence
          )), use.names = TRUE)
        }
        next_cached <- add_ids[add_ids %in% index$ref_id]
        if (identical(direction, "upward")) {
          next_cached <- setdiff(next_cached, visited_upward)
          visited_upward <- unique(c(visited_upward, next_cached))
        } else {
          next_cached <- setdiff(next_cached, visited_downward)
          visited_downward <- unique(c(visited_downward, next_cached))
        }
        if (length(next_cached) && depth < max_depth) {
          next_frontier[[length(next_frontier) + 1L]] <- data.table::data.table(ref_id = next_cached, direction = direction)
        }
      }
      frontier <- if (length(next_frontier)) data.table::rbindlist(next_frontier, use.names = TRUE) else data.table::data.table(ref_id = character(), direction = character())
      if (nrow(nodes) >= max_nodes) break
    }
  }

  if (nrow(edges)) {
    edge_key <- paste(edges$source, edges$target, edges$anchor_ref_id, sep = "\r")
    edges <- edges[!duplicated(edge_key), ]
  }
  detail_ids <- nodes$ref_id[nodes$node_type == "cached"]
  if (length(detail_ids)) {
    detail_index <- match(detail_ids, index$ref_id)
    detail_rows <- index[detail_index[!is.na(detail_index)], ]
    detail_paths <- file.path(.litxr_project_llm_dir(cfg), basename(detail_rows$json_filename))
    detail_digests <- lapply(detail_paths, function(path) {
      if (!file.exists(path)) return(NULL)
      tryCatch(.litxr_postprocess_llm_digest_read(jsonlite::fromJSON(path, simplifyVector = FALSE)), error = function(e) NULL)
    })
    details <- .litxr_literature_graph_digest_details(detail_digests, detail_rows$ref_id)
    if (nrow(details)) {
      detail_hit <- match(nodes$ref_id, details$ref_id)
      use_details <- !is.na(detail_hit)
      nodes$summary[use_details] <- details$summary[detail_hit[use_details]]
      nodes$theoretical_mechanism[use_details] <- details$theoretical_mechanism[detail_hit[use_details]]
      nodes$github_urls[use_details] <- details$github_urls[detail_hit[use_details]]
    }
  }
  data.table::setorder(nodes, depth, ref_id)
  data.table::setorder(edges, source, target, id)
  list(
    meta = list(
      root_ref_ids = root_ids,
      upward_root_ref_ids = upward_ids,
      downward_root_ref_ids = downward_ids,
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
