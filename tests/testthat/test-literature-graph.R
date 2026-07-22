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

arxiv_collection_index <- litxr:::.litxr_collection_index_for_id(cfg, "arxiv_cs_ai")
arxiv_ref_dir <- litxr:::.litxr_collection_ref_dir(cfg, "arxiv_cs_ai")
dir.create(arxiv_ref_dir, recursive = TRUE, showWarnings = FALSE)
jsonlite::write_json(
  list(title = "Root paper title"),
  file.path(arxiv_ref_dir, "arxiv_2501_00001.json"),
  auto_unbox = TRUE,
  pretty = TRUE
)
dir.create(dirname(litxr:::.litxr_ref_arxiv_path(cfg)), recursive = TRUE, showWarnings = FALSE)
fst::write_fst(
  data.table::data.table(
    arxiv_id = "2501.00001",
    collection_index = arxiv_collection_index,
    json_filename = "arxiv_2501_00001.json"
  ),
  litxr:::.litxr_ref_arxiv_path(cfg)
)

graph <- litxr::litxr_build_literature_graph("2501.00001", cfg, max_depth = 2L, max_nodes = 10L)
stopifnot(identical(graph$meta$root_ref_ids, "2501.00001"))
stopifnot(identical(graph$meta$external_nodes, 1L))
stopifnot(nrow(graph$nodes) == 4L)
stopifnot(nrow(graph$edges) == 3L)
stopifnot(identical(graph$nodes[node_type == "external", ref_id], "10.1000/external"))
stopifnot(identical(graph$nodes[ref_id == "2501.00003", depth], 2L))
stopifnot(identical(graph$nodes[ref_id == "2501.00001", title], "Root paper title"))
stopifnot(identical(graph$nodes[ref_id == "2501.00001", summary], "Root-paper summary."))
stopifnot(identical(graph$nodes[ref_id == "2501.00001", theoretical_mechanism], "Root-paper mechanism."))
stopifnot(identical(graph$nodes[ref_id == "2501.00001", github_urls], "https://github.com/example/root-paper"))

shallow <- litxr::litxr_build_literature_graph("2501.00001", cfg, max_depth = 1L, max_nodes = 10L)
stopifnot(nrow(shallow$nodes) == 3L)
stopifnot(nrow(shallow$edges) == 2L)

limited <- litxr::litxr_build_literature_graph("2501.00001", cfg, max_depth = 2L, max_nodes = 2L)
stopifnot(identical(limited$meta$returned_nodes, 2L))
stopifnot(identical(limited$meta$truncated_nodes, 1L))
