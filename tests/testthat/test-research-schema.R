td <- tempfile("litxr-research-schema-")
dir.create(td)
config_path <- file.path(td, ".litxr", "config.yaml")

old_litxr_config <- Sys.getenv("LITXR_CONFIG", unset = NA_character_)
Sys.setenv(LITXR_CONFIG = config_path)
on.exit({
  if (is.na(old_litxr_config)) {
    Sys.unsetenv("LITXR_CONFIG")
  } else {
    Sys.setenv(LITXR_CONFIG = old_litxr_config)
  }
}, add = TRUE)

litxr::litxr_init()
cfg <- litxr::litxr_read_config()

normalized_types <- litxr::litxr_normalize_paper_type(c(
  " Theoretical ",
  "empirical archival",
  "meta-analysis",
  "narrative review",
  "qualitative",
  "",
  NA_character_,
  "not mapped"
))
stopifnot(identical(
  normalized_types,
  c(
    "theoretical",
    "empirical_archival",
    "meta_analysis",
    "review_narrative",
    "empirical_qualitative",
    "unknown",
    "unknown",
    "unknown"
  )
))
stopifnot(identical(
  litxr::litxr_paper_type_levels(),
  c(
    "theoretical",
    "conceptual",
    "empirical_archival",
    "empirical_experimental",
    "empirical_survey",
    "empirical_qualitative",
    "empirical_case_study",
    "empirical_mixed_methods",
    "methodological",
    "computational",
    "simulation",
    "benchmark",
    "system_design",
    "review_narrative",
    "review_systematic",
    "review_scoping",
    "meta_analysis",
    "replication",
    "registered_report",
    "study_protocol",
    "policy_analysis",
    "perspective",
    "commentary",
    "review",
    "dataset",
    "policy_report",
    "book",
    "unknown"
  )
))
stopifnot(isTRUE(invisible(litxr::litxr_validate_paper_type(c("empirical archival", "narrative review", NA_character_, "")))))
stopifnot(inherits(try(litxr::litxr_validate_paper_type("bad_type_value"), silent = TRUE), "try-error"))

digest_template <- litxr::litxr_llm_digest_template("doi:10.1000/v2")
stopifnot(identical(digest_template$schema_version, "v2"))
stopifnot(identical(digest_template$digest_revision, 1L))
stopifnot(identical(digest_template$extraction_mode, "unknown"))
stopifnot(identical(digest_template$paper_type, "unknown"))
stopifnot(all(c("paper_structure", "research_data", "main_variables", "contribution_type") %in% names(digest_template)))
stopifnot(isTRUE(invisible(litxr::litxr_validate_llm_digest(digest_template))))

digest_template_v3 <- litxr::litxr_llm_digest_template("doi:10.1000/v3", schema_version = "v3")
stopifnot(identical(digest_template_v3$schema_version, "v3"))
stopifnot(all(c("anchor_references", "citation_logic_nodes") %in% names(digest_template_v3)))
stopifnot(isTRUE(invisible(litxr::litxr_validate_llm_digest(digest_template_v3))))

legacy_digest <- list(
  ref_id = "doi:10.1000/legacy",
  summary = "Legacy summary",
  motivation = "Legacy motivation",
  research_questions = "Question 1",
  methods = "Method 1",
  sample = list(description = "Sample", size = "100", period = "2000-2010"),
  key_findings = "Finding 1",
  limitations = "Limitation 1",
  keywords = "finance",
  notes = "legacy",
  generated_at = "2026-01-01 00:00:00 UTC"
)
stopifnot(isTRUE(invisible(litxr::litxr_validate_llm_digest(legacy_digest))))
legacy_path <- file.path(
  litxr:::.litxr_ensure_project_llm_dir(cfg),
  paste0("doi_10_1000_legacy.json")
)
jsonlite::write_json(legacy_digest, legacy_path, auto_unbox = TRUE, pretty = TRUE, null = "null")
legacy_read <- litxr::litxr_read_llm_digest("doi:10.1000/legacy", cfg)
stopifnot(identical(legacy_read$schema_version, "v1"))
stopifnot(identical(legacy_read$summary, "Legacy summary"))
litxr::litxr_upgrade_llm_digests(ref_ids = "doi:10.1000/legacy", config = cfg)
legacy_upgraded <- litxr::litxr_read_llm_digest("doi:10.1000/legacy", cfg)
stopifnot(identical(legacy_upgraded$schema_version, "v2"))
stopifnot(identical(legacy_upgraded$paper_type, "unknown"))
stopifnot(identical(legacy_upgraded$digest_revision, 1L))
stopifnot(identical(as.character(legacy_upgraded$extraction_mode[[1]]), "legacy"))

empty_findings <- litxr::litxr_read_standardized_findings(cfg)
stopifnot(inherits(empty_findings, "data.table"))
stopifnot(nrow(empty_findings) == 0L)
stopifnot(identical(
  names(empty_findings),
  names(litxr::litxr_standardized_findings_template())
))

standardized_rows <- data.table::data.table(
  ref_id = c("doi:10.1000/a", "doi:10.1000/a"),
  finding_id = c("f1", "f2"),
  paper_type = c("empirical archival", "theoretical"),
  research_question = c("RQ1", "RQ2"),
  finding_text = c("Positive relation", "Mechanism discussion"),
  effect_direction = c("positive", NA_character_),
  extraction_method = c("manual", "manual"),
  extracted_at = c("2026-04-28 00:00:00 UTC", "2026-04-28 00:00:00 UTC")
)
litxr::litxr_write_standardized_findings(standardized_rows, cfg)
standardized_read <- litxr::litxr_read_standardized_findings(cfg)
stopifnot(nrow(standardized_read) == 2L)
stopifnot(identical(standardized_read$paper_type[[1]], "empirical_archival"))

standardized_update <- data.table::data.table(
  ref_id = "doi:10.1000/a",
  finding_id = "f1",
  paper_type = "empirical archival",
  research_question = "RQ1 updated",
  finding_text = "Updated finding text",
  extraction_method = "manual",
  extracted_at = "2026-04-29 00:00:00 UTC"
)
litxr::litxr_write_standardized_findings(standardized_update, cfg)
updated_findings <- litxr::litxr_read_standardized_findings(cfg)
stopifnot(updated_findings[updated_findings$finding_id == "f1", ]$research_question[[1]] == "RQ1 updated")
litxr::litxr_compact_standardized_findings(cfg)
compacted_findings <- litxr::litxr_read_standardized_findings(cfg)
stopifnot(nrow(compacted_findings) == 2L)
stopifnot(!file.exists(file.path(litxr:::.litxr_project_findings_dir(cfg), "standardized_findings_delta.fst")))
stopifnot(inherits(try(litxr::litxr_validate_standardized_findings(data.table::data.table(ref_id = "", finding_id = "f1")), silent = TRUE), "try-error"))

empty_stats <- litxr::litxr_read_descriptive_stats(cfg)
stopifnot(inherits(empty_stats, "data.table"))
stopifnot(nrow(empty_stats) == 0L)
stopifnot(identical(
  names(empty_stats),
  names(litxr::litxr_descriptive_stats_template())
))

descriptive_rows <- data.table::data.table(
  ref_id = c("doi:10.1000/a", "doi:10.1000/a"),
  table_id = c("t1", "t1"),
  variable = c("x", "y"),
  label = c("Variable X", "Variable Y"),
  mean = c(1.2, 2.3),
  sd = c(0.5, 0.7),
  extraction_method = c("manual", "manual"),
  extracted_at = c("2026-04-28 00:00:00 UTC", "2026-04-28 00:00:00 UTC")
)
litxr::litxr_write_descriptive_stats(descriptive_rows, cfg)
descriptive_read <- litxr::litxr_read_descriptive_stats(cfg)
stopifnot(nrow(descriptive_read) == 2L)
stopifnot(descriptive_read[descriptive_read$variable == "x", ]$mean[[1]] == 1.2)

descriptive_update <- data.table::data.table(
  ref_id = "doi:10.1000/a",
  table_id = "t1",
  variable = "x",
  label = "Variable X",
  mean = 9.9,
  extraction_method = "manual",
  extracted_at = "2026-04-29 00:00:00 UTC"
)
litxr::litxr_write_descriptive_stats(descriptive_update, cfg)
updated_stats <- litxr::litxr_read_descriptive_stats(cfg)
stopifnot(updated_stats[updated_stats$variable == "x", ]$mean[[1]] == 9.9)
litxr::litxr_compact_descriptive_stats(cfg)
compacted_stats <- litxr::litxr_read_descriptive_stats(cfg)
stopifnot(nrow(compacted_stats) == 2L)
stopifnot(!file.exists(file.path(litxr:::.litxr_project_findings_dir(cfg), "descriptive_statistics_delta.fst")))
stopifnot(inherits(try(litxr::litxr_validate_descriptive_stats(data.table::data.table(ref_id = "doi:1", table_id = "", variable = "x")), silent = TRUE), "try-error"))

empty_anchors <- litxr::litxr_read_anchor_references(cfg)
stopifnot(inherits(empty_anchors, "data.table"))
stopifnot(nrow(empty_anchors) == 0L)
stopifnot(identical(
  names(empty_anchors),
  names(litxr::litxr_anchor_references_template())
))

anchor_rows <- data.table::data.table(
  ref_id = c("doi:10.1000/a", "doi:10.1000/a"),
  anchor_rank = c(1L, 2L),
  citation_key = c("smith_2024", "jones_2023"),
  anchor_title = c("Anchor One", "Anchor Two"),
  anchor_authors = c("A Author; B Author", "C Author"),
  anchor_year = c(2024L, 2023L),
  anchor_role = c("theoretical", "review"),
  relationship_to_current_paper = c("build_on", "uses_as_context"),
  confidence = c("high", "medium")
)
litxr::litxr_write_anchor_references(anchor_rows, cfg)
anchor_read <- litxr::litxr_read_anchor_references(cfg)
stopifnot(nrow(anchor_read) == 2L)
stopifnot(identical(anchor_read$anchor_role[[1]], "theoretical_foundation"))
stopifnot(identical(anchor_read$relationship_to_current_paper[[1]], "builds_on"))
stopifnot(inherits(try(litxr::litxr_validate_anchor_references(data.table::data.table(ref_id = "", anchor_rank = 1L)), silent = TRUE), "try-error"))

anchor_update <- data.table::data.table(
  ref_id = "doi:10.1000/a",
  anchor_rank = 1L,
  citation_key = "smith_2024",
  anchor_title = "Anchor One Updated",
  anchor_role = "theoretical",
  relationship_to_current_paper = "build_on",
  confidence = "high"
)
litxr::litxr_write_anchor_references(anchor_update, cfg)
anchor_updated <- litxr::litxr_read_anchor_references(cfg)
stopifnot(anchor_updated[anchor_updated$anchor_rank == 1L, ]$anchor_title[[1]] == "Anchor One Updated")
litxr::litxr_compact_anchor_references(cfg)
anchor_compacted <- litxr::litxr_read_anchor_references(cfg)
stopifnot(nrow(anchor_compacted) == 2L)
stopifnot(!file.exists(file.path(litxr:::.litxr_project_findings_dir(cfg), "anchor_references_delta.fst")))

empty_nodes <- litxr::litxr_read_citation_logic_nodes(cfg)
stopifnot(inherits(empty_nodes, "data.table"))
stopifnot(nrow(empty_nodes) == 0L)
stopifnot(identical(
  names(empty_nodes),
  names(litxr::litxr_citation_logic_nodes_template())
))

logic_rows <- data.table::data.table(
  ref_id = c("doi:10.1000/a", "doi:10.1000/a"),
  node_id = c("node_1", "node_2"),
  claim_sentence = c("A improves B.", "A constrains B."),
  logic_type = c("improves", "constrains"),
  subject_text = c("A", "A"),
  object_text = c("B", "B"),
  evidence_role = c("main_finding", "limitation"),
  confidence = c("high", "medium"),
  tags = c("tag1; tag2", "tag3")
)
litxr::litxr_write_citation_logic_nodes(logic_rows, cfg)
logic_read <- litxr::litxr_read_citation_logic_nodes(cfg)
stopifnot(nrow(logic_read) == 2L)
stopifnot(identical(logic_read$logic_type[[1]], "A_improves_B"))
stopifnot(identical(logic_read$evidence_role[[2]], "limitation_note"))
stopifnot(inherits(try(litxr::litxr_validate_citation_logic_nodes(data.table::data.table(ref_id = "doi:1", node_id = "", claim_sentence = "x")), silent = TRUE), "try-error"))

logic_update <- data.table::data.table(
  ref_id = "doi:10.1000/a",
  node_id = "node_1",
  claim_sentence = "A improves B and C.",
  logic_type = "improves",
  subject_text = "A",
  object_text = "B and C",
  evidence_role = "main_finding",
  confidence = "high"
)
litxr::litxr_write_citation_logic_nodes(logic_update, cfg)
logic_updated <- litxr::litxr_read_citation_logic_nodes(cfg)
stopifnot(logic_updated[logic_updated$node_id == "node_1", ]$claim_sentence[[1]] == "A improves B and C.")
litxr::litxr_compact_citation_logic_nodes(cfg)
logic_compacted <- litxr::litxr_read_citation_logic_nodes(cfg)
stopifnot(nrow(logic_compacted) == 2L)
stopifnot(!file.exists(file.path(litxr:::.litxr_project_findings_dir(cfg), "citation_logic_nodes_delta.fst")))

litxr::litxr_add_refs(
  data.frame(
    source = c("manual", "manual", "manual", "manual"),
    source_id = c("doi:10.1000/a", "doi:10.1000/legacy", "doi:10.1000/nodigest", "doi:10.1000/v3"),
    ref_id = c("doi:10.1000/a", "doi:10.1000/legacy", "doi:10.1000/nodigest", "doi:10.1000/v3"),
    entry_type = c("article", "article", "article", "article"),
    title = c("Paper A", "Paper Legacy", "Paper No Digest", "Paper V3"),
    authors = c("A Author", "B Author", "C Author", "D Author"),
    year = c(2024L, 2023L, 2022L, 2025L),
    stringsAsFactors = FALSE
  ),
  collection_id = "manual_research",
  config = cfg
)

litxr::litxr_write_md("doi:10.1000/a", "Example markdown", cfg)
litxr::litxr_write_llm_digest(
  "doi:10.1000/a",
  list(
    summary = "Summary",
    motivation = "Motivation"
  ),
  cfg
)
schema_status <- litxr::litxr_read_research_schema_status(cfg)
row_a <- schema_status[schema_status$ref_id == "doi:10.1000/a", ]
stopifnot(nrow(row_a) == 1L)
stopifnot(isTRUE(row_a$has_md[[1]]))
stopifnot(isTRUE(row_a$has_llm_digest[[1]]))
stopifnot(identical(row_a$llm_schema_version[[1]], "v2"))
stopifnot(identical(row_a$llm_paper_type[[1]], "unknown"))
stopifnot(isTRUE(row_a$has_standardized_findings[[1]]))
stopifnot(identical(row_a$n_standardized_findings[[1]], 2L))
stopifnot(isTRUE(row_a$has_descriptive_stats[[1]]))
stopifnot(identical(row_a$n_descriptive_stats[[1]], 2L))
stopifnot(isTRUE(row_a$has_anchor_references[[1]]))
stopifnot(identical(row_a$n_anchor_references[[1]], 2L))
stopifnot(isTRUE(row_a$has_citation_logic_nodes[[1]]))
stopifnot(identical(row_a$n_citation_logic_nodes[[1]], 2L))

digest_v3 <- litxr::litxr_llm_digest_template("doi:10.1000/v3", schema_version = "v3")
digest_v3[["summary"]] <- "Summary V3"
digest_v3[["motivation"]] <- "Motivation V3"
digest_v3[["anchor_references"]] <- data.table::data.table(
  anchor_rank = 1L,
  citation_key = "smith_2024",
  anchor_title = "Anchor One",
  anchor_authors = "A Author; B Author",
  anchor_year = 2024L,
  anchor_role = "theoretical",
  relationship_to_current_paper = "build_on",
  confidence = "high"
)
digest_v3[["citation_logic_nodes"]] <- data.table::data.table(
  node_id = "node_1",
  claim_sentence = "A improves B.",
  logic_type = "improves",
  subject_text = "A",
  object_text = "B",
  evidence_role = "main_finding",
  confidence = "high"
)
litxr::litxr_write_llm_digest("doi:10.1000/v3", digest_v3, cfg, keep_history = FALSE, bump_revision = FALSE)
digest_v3_read <- litxr::litxr_read_llm_digest("doi:10.1000/v3", cfg)
stopifnot(identical(digest_v3_read$schema_version, "v3"))
stopifnot(!is.null(digest_v3_read$anchor_references))
stopifnot(!is.null(digest_v3_read$citation_logic_nodes))

missing_llm <- litxr::litxr_find_refs_missing_llm_digest(cfg)
stopifnot(any(missing_llm$ref_id == "doi:10.1000/nodigest"))
missing_findings <- litxr::litxr_find_refs_missing_standardized_findings(cfg)
stopifnot(any(missing_findings$ref_id == "doi:10.1000/legacy"))
missing_desc <- litxr::litxr_find_refs_missing_descriptive_stats(cfg)
stopifnot(any(missing_desc$ref_id == "doi:10.1000/legacy"))
missing_anchors <- litxr::litxr_find_refs_missing_anchor_references(cfg)
stopifnot(any(missing_anchors$ref_id == "doi:10.1000/legacy"))
missing_nodes <- litxr::litxr_find_refs_missing_citation_logic_nodes(cfg)
stopifnot(any(missing_nodes$ref_id == "doi:10.1000/legacy"))

litxr::litxr_rebuild_standardized_findings(cfg)
litxr::litxr_rebuild_descriptive_stats(cfg)
litxr::litxr_rebuild_anchor_references(cfg)
litxr::litxr_rebuild_citation_logic_nodes(cfg)
stopifnot(file.exists(file.path(litxr:::.litxr_project_findings_dir(cfg), "standardized_findings.fst")))
stopifnot(file.exists(file.path(litxr:::.litxr_project_findings_dir(cfg), "descriptive_statistics.fst")))
stopifnot(file.exists(file.path(litxr:::.litxr_project_findings_dir(cfg), "anchor_references.fst")))
stopifnot(file.exists(file.path(litxr:::.litxr_project_findings_dir(cfg), "citation_logic_nodes.fst")))
