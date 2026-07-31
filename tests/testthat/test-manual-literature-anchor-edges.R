td <- tempfile("litxr-manual-literature-edges-")
dir.create(td)
old_litxr_data_root <- Sys.getenv("LITXR_DATA_ROOT", unset = NA_character_)
Sys.setenv(LITXR_DATA_ROOT = td)
on.exit({
  if (is.na(old_litxr_data_root)) Sys.unsetenv("LITXR_DATA_ROOT") else Sys.setenv(LITXR_DATA_ROOT = old_litxr_data_root)
}, add = TRUE)

litxr::litxr_init()
cfg <- litxr::litxr_read_config()
edge_path <- litxr:::.litxr_project_literature_anchor_edges_path(cfg)
dir.create(dirname(edge_path), recursive = TRUE, showWarnings = FALSE)
fst::write_fst(data.table::data.table(
  source_ref_id = "1409.0473",
  target_ref_id = "1409.0474",
  anchor_ref_id = "1409.0474",
  anchor_rank = 1L,
  anchor_role = "methodological_foundation",
  relationship = "builds_on",
  confidence = "high"
), edge_path)

log_dir <- litxr:::.litxr_ensure_project_log_dir(cfg)
data.table::fwrite(data.table::data.table(
  source_ref_id = "1409.0473",
  target_ref_id = "1409.0474",
  anchor_role = "main_comparison",
  relationship = "compares_with"
), file.path(log_dir, "manual_literature_anchor_edges.tsv"), sep = "\t")

generated_only <- litxr:::.litxr_read_literature_anchor_edges(cfg, include_manual = FALSE)
overlay <- litxr:::.litxr_read_literature_anchor_edges(cfg)
stopifnot(identical(generated_only$relationship, "builds_on"))
stopifnot(nrow(overlay) == 1L)
stopifnot(identical(overlay$anchor_role, "main_comparison"))
stopifnot(identical(overlay$relationship, "compares_with"))
stopifnot(identical(overlay$target_ref_id, "1409.0474"))

litxr::litxr_write_llm_digest(
  "1409.0473",
  litxr::litxr_llm_digest_template("1409.0473", schema_version = "v5"),
  cfg,
  keep_history = FALSE,
  bump_revision = FALSE
)
litxr::litxr_write_llm_digest(
  "1409.0474",
  litxr::litxr_llm_digest_template("1409.0474", schema_version = "v5"),
  cfg,
  keep_history = FALSE,
  bump_revision = FALSE
)
graph <- litxr::litxr_build_literature_graph(
  upward_ref_ids = "1409.0473",
  config = cfg,
  max_depth = 1L,
  max_nodes = 2L
)
stopifnot(identical(graph$edges$relationship, "compares_with"))
