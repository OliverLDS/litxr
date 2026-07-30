td <- tempfile("litxr-literature-graph-")
dir.create(td)
old_litxr_data_root <- Sys.getenv("LITXR_DATA_ROOT", unset = NA_character_)
Sys.setenv(LITXR_DATA_ROOT = td)
on.exit({
  if (is.na(old_litxr_data_root)) Sys.unsetenv("LITXR_DATA_ROOT") else Sys.setenv(LITXR_DATA_ROOT = old_litxr_data_root)
}, add = TRUE)

litxr::litxr_init()
cfg <- litxr::litxr_read_config()

write_digest <- function(ref_id, anchors = list()) {
  digest <- litxr::litxr_llm_digest_template(ref_id, schema_version = "v5")
  digest$anchor_references <- anchors
  if (identical(ref_id, "2501.00001")) {
    digest$summary <- "Root-paper summary."
    digest$theoretical_mechanism <- "Root-paper mechanism."
    digest$research_target_github_links <- list(list(
      url = "https://github.com/example/root-paper",
      category_tags = c("implementation"),
      research_role = "official implementation",
      description = "Root-paper code.",
      evidence_context = "Linked by the paper."
    ))
  }
  if (identical(ref_id, "2309.14556")) {
    digest$summary <- "Linked arXiv digest summary."
    digest$theoretical_mechanism <- "Linked arXiv digest mechanism."
  }
  litxr::litxr_write_llm_digest(ref_id, digest, cfg, keep_history = FALSE, bump_revision = FALSE)
}

write_digest("2501.00001", list(
  list(anchor_rank = 1L, anchor_ref_id = "2501.00002", anchor_title = "Cached parent", anchor_role = "methodological_foundation", relationship_to_current_paper = "builds_on", confidence = "high", reason = "Uses the parent method."),
  list(anchor_rank = 2L, anchor_ref_id = "10.1000/external", anchor_title = "External work", anchor_role = "conceptual_foundation", relationship_to_current_paper = "extends", confidence = "medium", reason = "Frames the problem.")
))
write_digest("2501.00002", list(
  list(anchor_rank = 1L, anchor_ref_id = "2501.00003", anchor_title = "Grandparent", anchor_role = "technical_foundation", relationship_to_current_paper = "builds_on", confidence = "high", reason = "Supplies the algorithm.")
))
write_digest("2501.00003")
litxr:::.litxr_sync_literature_anchor_edges(cfg, mode = "full")

arxiv_collection_index <- litxr:::.litxr_collection_index_for_id(cfg, "arxiv_cs_ai")
arxiv_ref_dir <- litxr:::.litxr_collection_ref_dir(cfg, "arxiv_cs_ai")
dir.create(arxiv_ref_dir, recursive = TRUE, showWarnings = FALSE)
jsonlite::write_json(
  list(title = "Root paper title"),
  file.path(arxiv_ref_dir, "arxiv_2501_00001.json"),
  auto_unbox = TRUE,
  pretty = TRUE
)
jsonlite::write_json(
  list(title = "Cached parent title"),
  file.path(arxiv_ref_dir, "arxiv_2501_00002.json"),
  auto_unbox = TRUE,
  pretty = TRUE
)
dir.create(dirname(litxr:::.litxr_ref_arxiv_path(cfg)), recursive = TRUE, showWarnings = FALSE)
fst::write_fst(
  data.table::data.table(
    arxiv_id = c("2501.00001", "2501.00002"),
    collection_index = arxiv_collection_index,
    json_filename = c("arxiv_2501_00001.json", "arxiv_2501_00002.json")
  ),
  litxr:::.litxr_ref_arxiv_path(cfg)
)

graph <- litxr::litxr_build_literature_graph(upward_ref_ids = "2501.00001", config = cfg, max_depth = 2L, max_nodes = 10L)
stopifnot(identical(graph$meta$root_ref_ids, "2501.00001"))
stopifnot(identical(graph$meta$external_nodes, 1L))
stopifnot(nrow(graph$nodes) == 4L)
stopifnot(nrow(graph$edges) == 3L)
stopifnot(identical(graph$nodes[node_type == "external", ref_id], "10.1000/external"))
stopifnot(identical(graph$nodes[ref_id == "2501.00003", depth], 2L))
stopifnot(identical(graph$nodes[ref_id == "2501.00001", title], "Root paper title"))
stopifnot(identical(graph$nodes[ref_id == "2501.00002", title], "Cached parent title"))
stopifnot(identical(graph$nodes[ref_id == "10.1000/external", title], "External work"))
stopifnot(identical(graph$nodes[ref_id == "10.1000/external", title], "External work"))
stopifnot(identical(graph$nodes[ref_id == "2501.00001", summary], "Root-paper summary."))
stopifnot(identical(graph$nodes[ref_id == "2501.00001", theoretical_mechanism], "Root-paper mechanism."))
stopifnot(identical(graph$nodes[ref_id == "2501.00001", github_urls], "https://github.com/example/root-paper"))

shallow <- litxr::litxr_build_literature_graph(upward_ref_ids = "2501.00001", config = cfg, max_depth = 1L, max_nodes = 10L)
stopifnot(nrow(shallow$nodes) == 3L)
stopifnot(nrow(shallow$edges) == 2L)

limited <- litxr::litxr_build_literature_graph(upward_ref_ids = "2501.00001", config = cfg, max_depth = 2L, max_nodes = 2L)
stopifnot(identical(limited$meta$returned_nodes, 2L))
stopifnot(identical(limited$meta$truncated_nodes, 1L))

write_digest("2501.00004", list(
  list(anchor_rank = 1L, anchor_ref_id = "doi:10.1145/3613904.3642731", anchor_title = "Linked DOI anchor", anchor_role = "methodological_foundation", relationship_to_current_paper = "builds_on", confidence = "high", reason = "Available through the linked arXiv digest.")
))
write_digest("2309.14556")
fst::write_fst(
  data.table::data.table(
    arxiv_id = "2309.14556",
    doi = "10.1145/3613904.3642731"
  ),
  litxr:::.litxr_project_ref_identity_index_path(cfg)
)
litxr:::.litxr_sync_literature_anchor_edges(cfg, mode = "incremental", ref_ids = c("2501.00004", "2309.14556"))

linked <- litxr::litxr_build_literature_graph(upward_ref_ids = "2501.00004", config = cfg, max_depth = 2L, max_nodes = 10L)
stopifnot(nrow(linked$nodes) == 2L)
stopifnot(identical(linked$meta$external_nodes, 0L))
stopifnot(identical(linked$nodes[ref_id == "2309.14556", node_type], "cached"))
stopifnot(isTRUE(linked$nodes[ref_id == "2309.14556", traversable]))
stopifnot(identical(linked$nodes[ref_id == "2309.14556", summary], "Linked arXiv digest summary."))
stopifnot(identical(linked$nodes[ref_id == "2309.14556", theoretical_mechanism], "Linked arXiv digest mechanism."))
stopifnot(!any(linked$nodes$ref_id == "10.1145/3613904.3642731"))
stopifnot(identical(linked$edges$target, "2309.14556"))
stopifnot(identical(linked$edges$anchor_ref_id, "10.1145/3613904.3642731"))

write_digest("2501.00005", list(
  list(anchor_rank = 1L, anchor_ref_id = "2501.00001", anchor_title = "Root paper", anchor_role = "methodological_foundation", relationship_to_current_paper = "builds_on", confidence = "high", reason = "Cites the root paper.")
))
litxr:::.litxr_sync_literature_anchor_edges(cfg, mode = "incremental", ref_ids = "2501.00005")
downward <- litxr::litxr_build_literature_graph(
  downward_ref_ids = "2501.00001",
  config = cfg,
  max_depth = 1L,
  max_nodes = 10L
)
stopifnot(identical(downward$meta$downward_root_ref_ids, "2501.00001"))
stopifnot(identical(downward$nodes[ref_id == "2501.00005", node_type], "cached"))
stopifnot(identical(downward$edges$source, "2501.00005"))
stopifnot(identical(downward$edges$target, "2501.00001"))

bidirectional <- litxr::litxr_build_literature_graph(
  ref_ids = "2501.00001",
  config = cfg,
  max_depth = 1L,
  max_nodes = 10L
)
stopifnot(any(bidirectional$nodes$ref_id == "2501.00005"))
stopifnot(any(bidirectional$edges$source == "2501.00005" & bidirectional$edges$target == "2501.00001"))

write_digest("2501.00101", list(
  list(anchor_rank = 1L, anchor_ref_id = "2501.00102", anchor_role = "methodological_foundation", relationship_to_current_paper = "builds_on", confidence = "high")
))
write_digest("2501.00102", list(
  list(anchor_rank = 1L, anchor_ref_id = "2501.00103", anchor_role = "methodological_foundation", relationship_to_current_paper = "builds_on", confidence = "high")
))
write_digest("2501.00103", list(
  list(anchor_rank = 1L, anchor_ref_id = "2501.00104", anchor_role = "comparison", relationship_to_current_paper = "compares_with", confidence = "medium")
))
write_digest("2501.00104", list(
  list(anchor_rank = 1L, anchor_ref_id = "2501.00101", anchor_role = "methodological_foundation", relationship_to_current_paper = "builds_on", confidence = "high")
))
litxr:::.litxr_sync_literature_anchor_edges(
  cfg,
  mode = "incremental",
  ref_ids = c("2501.00101", "2501.00102", "2501.00103", "2501.00104")
)

cycle_graph <- litxr::litxr_build_literature_graph(upward_ref_ids = "2501.00101", config = cfg, max_depth = 4L, max_nodes = 10L)
stopifnot(all(c("2501.00101", "2501.00102", "2501.00103", "2501.00104") %in% cycle_graph$nodes$ref_id))
stopifnot(any(cycle_graph$edges$source == "2501.00104" & cycle_graph$edges$target == "2501.00101"))
stopifnot(identical(cycle_graph$edges[relationship == "compares_with", edge_type], "comparison"))

foundation_only <- litxr::litxr_build_literature_graph(
  upward_ref_ids = "2501.00101",
  config = cfg,
  max_depth = 4L,
  max_nodes = 10L,
  include_edge_types = "foundation"
)
stopifnot(!any(foundation_only$nodes$ref_id == "2501.00104"))
stopifnot(all(foundation_only$edges$edge_type == "foundation"))
