td <- tempfile("litxr-research-schema-")
dir.create(td)
config_path <- file.path(td, "config.yaml")

old_litxr_config <- Sys.getenv("LITXR_DATA_ROOT", unset = NA_character_)
Sys.setenv(LITXR_DATA_ROOT = dirname(config_path))
on.exit({
  if (is.na(old_litxr_config)) {
    Sys.unsetenv("LITXR_DATA_ROOT")
  } else {
    Sys.setenv(LITXR_DATA_ROOT = old_litxr_config)
  }
}, add = TRUE)

litxr::litxr_init()
cfg <- litxr::litxr_read_config()
journal <- cfg$journals[[1]]

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

schema_release_info <- litxr::litxr_llm_schema_release_info()
stopifnot(identical(
  names(schema_release_info),
  c(
    "schema_version",
    "prompt_version",
    "schema_release_date",
    "schema_release_tag",
    "schema_release_notes"
  )
))
stopifnot(identical(schema_release_info$schema_version, "v5"))
stopifnot(identical(schema_release_info$prompt_version, "v5.0"))
stopifnot(identical(schema_release_info$schema_release_date, "2026-07-20"))
stopifnot(identical(schema_release_info$schema_release_tag, "v0.1.8.6"))
stopifnot(grepl("source-grounded detail", schema_release_info$schema_release_notes, fixed = TRUE))

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

digest_template_v4 <- litxr::litxr_llm_digest_template("doi:10.1000/v4", schema_version = "v4")
stopifnot(identical(digest_template_v4$schema_version, "v4"))
stopifnot(all(c(
  "ranked_contributions",
  "likely_reader_misconceptions",
  "business_relevance_pathway",
  "tables",
  "research_target_github_links",
  "evidence_shape"
) %in% names(digest_template_v4)))
digest_template_v4$ranked_contributions <- list(list(
  rank = 1L,
  contribution_type = "method",
  contribution = "Provides a test contribution.",
  reason = "It is the central contribution in this fixture."
))
digest_template_v4$likely_reader_misconceptions <- c("A reader might overgeneralize the result.")
digest_template_v4$business_relevance_pathway <- c("The method can inform workflow design.")
digest_template_v4$tables <- list(list(
  table_id = "table_1",
  title = "Fixture table",
  source_location = "Table 1",
  columns = c("variable", "value"),
  rows = list(list(variable = "accuracy", value = "0.9")),
  notes = "Fixture structured table."
))
digest_template_v4$research_target_github_links <- list(list(
  url = "https://gist.github.com/example/research-artifact",
  category_tags = c("framework", "implementation"),
  research_role = "artifact introduced by the paper",
  description = "Fixture repository.",
  evidence_context = "Mentioned in the artifact section."
))
digest_template_v4$evidence_shape <- list(
  evidence_mode = "conceptual_argument",
  evidence_basis = c("The fixture uses conceptual support."),
  inference_type = "interpretive",
  strength_level = "medium",
  limitations = c("Fixture-only evidence.")
)
stopifnot(isTRUE(invisible(litxr::litxr_validate_llm_digest(digest_template_v4))))
gatech_github_v4 <- digest_template_v4
gatech_github_v4$research_target_github_links[[1]]$url <- "https://github.gatech.edu/jfaile3/dl-7643-finalproject-sentinel-stocks"
stopifnot(isTRUE(invisible(litxr::litxr_validate_llm_digest(gatech_github_v4))))
bad_v4 <- digest_template_v4
bad_v4$evidence_shape$evidence_mode <- "bad_mode"
stopifnot(inherits(try(litxr::litxr_validate_llm_digest(bad_v4), silent = TRUE), "try-error"))

digest_template_v5_theory <- litxr::litxr_llm_digest_template("doi:10.1000/v5-theory", schema_version = "v5")
stopifnot(identical(digest_template_v5_theory$schema_version, "v5"))
stopifnot(all(names(digest_template_v4) %in% names(digest_template_v5_theory)))
digest_template_v5_theory$key_findings <- "The objective has a bounded optimum under the stated assumptions."
digest_template_v5_theory$theoretical_mechanism <- "The objective penalizes deviations from the constrained estimator."
digest_template_v5_theory$source_detail$coverage$equations <- "complete"
digest_template_v5_theory$source_detail$evidence_items <- list(list(
  evidence_id = "evidence_1",
  claim = "The objective has a bounded optimum.",
  evidence_type = "theorem",
  source_locator = list(section = "3.2", page = "5", label = "Theorem 1"),
  conditions = c("Convex objective", "Bounded feasible set"),
  limitation = "The result depends on the stated regularity assumptions.",
  supports_v4 = list(list(field = "key_findings", item_index = 1L))
))
digest_template_v5_theory$source_detail$equation_cards <- list(list(
  equation_id = "eq_1",
  latex = "\\\\min_\\\\theta L(\\\\theta)",
  display_name = "Constrained objective",
  source_locator = list(section = "3.2", page = "5", label = "(4)"),
  symbols = list(list(symbol = "theta", meaning = "model parameter", domain_or_shape = "R^p")),
  assumptions = c("The feasible set is bounded."),
  role_in_argument = "Defines the estimator used in the theorem.",
  plain_language_interpretation = "The estimator minimizes loss subject to the stated constraints.",
  supports_v4 = list(list(field = "theoretical_mechanism")),
  drafting_guidance = list(safe_to_explain_without_rendering = TRUE, required_caveat = "State the regularity assumptions.")
))
stopifnot(isTRUE(invisible(litxr::litxr_validate_llm_digest(digest_template_v5_theory))))

digest_template_v5_experiment <- litxr::litxr_llm_digest_template("doi:10.1000/v5-experiment", schema_version = "v5")
digest_template_v5_experiment$key_findings <- "The method improves accuracy on the reported benchmark."
digest_template_v5_experiment$source_detail$coverage$benchmark_tables <- "complete"
digest_template_v5_experiment$source_detail$evidence_items <- list(list(
  evidence_id = "evidence_1",
  claim = "The method improves accuracy from 72.4 to 74.1 on the reported workload.",
  evidence_type = "benchmark",
  source_locator = list(section = "4.1", page = "7", label = "Table 2"),
  conditions = c("Reported hardware", "Listed test split"),
  limitation = "The result is limited to the listed workload and hardware.",
  supports_v4 = list(list(field = "key_findings", item_index = 1L))
))
digest_template_v5_experiment$source_detail$benchmark_tables <- list(list(
  table_id = "table_2",
  title = "Primary benchmark",
  source_locator = list(section = "4.1", page = "7", label = "Table 2"),
  task_or_dataset = "Fixture benchmark",
  metric_definitions = list(list(metric = "accuracy", direction = "higher_is_better")),
  experimental_conditions = list(model = "Fixture model", hardware = "Fixture hardware", data_split = "test", inference_or_training_budget = "reported budget"),
  columns = c("model", "accuracy"),
  rows = list(list(row_id = "table_2_r1", values = list(model = "Baseline", accuracy = 72.4)), list(row_id = "table_2_r2", values = list(model = "Method", accuracy = 74.1))),
  author_reported_takeaway = "The method improves reported accuracy.",
  digest_interpretation_boundary = "Do not generalize beyond the listed workload.",
  supports_v4 = list(list(field = "key_findings", item_index = 1L))
))
stopifnot(isTRUE(invisible(litxr::litxr_validate_llm_digest(digest_template_v5_experiment))))
bad_v5_duplicate_evidence <- digest_template_v5_experiment
bad_v5_duplicate_evidence$source_detail$evidence_items <- rep(bad_v5_duplicate_evidence$source_detail$evidence_items, 2L)
stopifnot(inherits(try(litxr::litxr_validate_llm_digest(bad_v5_duplicate_evidence), silent = TRUE), "try-error"))
legacy_v5_without_source_detail <- litxr::litxr_llm_digest_template("doi:10.1000/v5-optional", schema_version = "v5")
legacy_v5_without_source_detail$source_detail <- NULL
stopifnot(isTRUE(invisible(litxr::litxr_validate_llm_digest(legacy_v5_without_source_detail))))

legacy_v4 <- litxr::litxr_llm_digest_template("doi:10.1000/v4-legacy", schema_version = "v4")
legacy_v4$tables <- NULL
legacy_v4$research_target_github_links <- NULL
stopifnot(isTRUE(invisible(litxr::litxr_validate_llm_digest(legacy_v4))))
legacy_v4_path <- litxr::litxr_write_llm_digest(
  "doi:10.1000/v4-legacy",
  legacy_v4,
  cfg,
  keep_history = FALSE,
  bump_revision = FALSE
)
stopifnot(file.exists(legacy_v4_path))
legacy_v4_read <- litxr::litxr_read_llm_digest("doi:10.1000/v4-legacy", cfg)
stopifnot(isTRUE(invisible(litxr::litxr_validate_llm_digest(legacy_v4_read))))

prompt_ref <- data.table::data.table(
  ref_id = "arxiv:2501.00001",
  source = "arxiv",
  source_id = "2501.00001",
  title = "Prompt Builder Test Paper",
  abstract = "Prompt builder abstract.",
  linked_doi_ref_id = "doi:10.1000/prompt-doi",
  doi = NA_character_,
  year = 2025L
)
litxr:::.litxr_refresh_normalized_reference_scaffold(cfg, records = prompt_ref, refresh_entity_indexes = TRUE)
prompt_create <- litxr::litxr_llm_digest_prompt("arxiv:2501.00001", cfg, mode = "create")
stopifnot(grepl("https://arxiv.org/html/2501.00001", prompt_create, fixed = TRUE))
stopifnot(grepl("https://arxiv.org/pdf/2501.00001", prompt_create, fixed = TRUE))
stopifnot(grepl("anchor_ref_id", prompt_create, fixed = TRUE))
stopifnot(grepl("Do not fabricate anchor_ref_id", prompt_create, fixed = TRUE))
stopifnot(grepl("A_mitigates_effect_of_B_on_C", prompt_create, fixed = TRUE))
stopifnot(grepl("schema-v5", prompt_create, fixed = TRUE))
stopifnot(grepl("identification_strategy: how the paper supports its empirical or causal claim", prompt_create, fixed = TRUE))
stopifnot(grepl("likely_reader_misconceptions", prompt_create, fixed = TRUE))
stopifnot(grepl("business_relevance_pathway", prompt_create, fixed = TRUE))
stopifnot(grepl("ranked_contributions", prompt_create, fixed = TRUE))
stopifnot(grepl("evidence_shape", prompt_create, fixed = TRUE))
stopifnot(grepl("tables", prompt_create, fixed = TRUE))
stopifnot(grepl("research_target_github_links", prompt_create, fixed = TRUE))
stopifnot(grepl("source_detail", prompt_create, fixed = TRUE))
stopifnot(grepl("supports_v4", prompt_create, fixed = TRUE))
stopifnot(grepl("research target or artifact", prompt_create, fixed = TRUE))
stopifnot(!grepl("{{", prompt_create, fixed = TRUE))

prompt_ref_doi <- data.table::data.table(
  ref_id = "doi:10.1000/prompt-doi",
  source = "crossref",
  source_id = "10.1000/prompt-doi",
  title = "Prompt Builder DOI Test Paper",
  abstract = "Prompt builder DOI abstract.",
  doi = "10.1000/prompt-doi",
  linked_arxiv_ref_id = "arxiv:2501.00001",
  year = 2025L
)
litxr:::.litxr_refresh_normalized_reference_scaffold(cfg, records = data.table::rbindlist(list(prompt_ref, prompt_ref_doi), fill = TRUE), refresh_entity_indexes = TRUE)
prompt_create_doi <- litxr::litxr_llm_digest_prompt("doi:10.1000/prompt-doi", cfg, mode = "create")
stopifnot(grepl("linked_arxiv_ref_id: arxiv:2501.00001", prompt_create_doi, fixed = TRUE))
stopifnot(grepl("https://arxiv.org/html/2501.00001", prompt_create_doi, fixed = TRUE))
stopifnot(grepl("https://doi.org/10.1000/prompt-doi", prompt_create_doi, fixed = TRUE))

prompt_digest <- litxr::litxr_llm_digest_template("arxiv:2501.00001", schema_version = "v3")
prompt_digest$summary <- "Existing summary"
prompt_digest$motivation <- "Existing motivation"
litxr::litxr_write_llm_digest("arxiv:2501.00001", prompt_digest, cfg, keep_history = FALSE, bump_revision = FALSE)
prompt_revise <- litxr::litxr_llm_digest_prompt("arxiv:2501.00001", cfg, mode = "revise")
stopifnot(grepl("Existing local digest to improve:", prompt_revise, fixed = TRUE))
stopifnot(grepl("Existing summary", prompt_revise, fixed = TRUE))
stopifnot(inherits(try(litxr::litxr_llm_digest_prompt("arxiv:2501.00001", cfg, mode = "create"), silent = TRUE), "try-error"))

digest_template_v3_emp <- litxr::litxr_llm_digest_template("doi:10.1000/v3-emp", schema_version = "v3")
digest_template_v3_emp$paper_type <- "empirical archival"
digest_template_v3_emp$summary <- "Empirical summary"
digest_template_v3_emp$motivation <- "Empirical motivation"
warning_seen <- FALSE
withCallingHandlers(
  litxr::litxr_validate_llm_digest(digest_template_v3_emp),
  warning = function(w) {
    if (grepl("identification_strategy|empirical_setting|descriptive_statistics_summary|standardized_findings_summary", conditionMessage(w))) {
      warning_seen <<- TRUE
    }
    invokeRestart("muffleWarning")
  }
)
stopifnot(isTRUE(warning_seen))

digest_template_v3_case <- litxr::litxr_llm_digest_template("doi:10.1000/v3-case", schema_version = "v3")
digest_template_v3_case$paper_type <- "empirical case study"
digest_template_v3_case$summary <- "Case summary"
digest_template_v3_case$motivation <- "Case motivation"
case_warning <- NULL
withCallingHandlers(
  litxr::litxr_validate_llm_digest(digest_template_v3_case),
  warning = function(w) {
    case_warning <<- conditionMessage(w)
    invokeRestart("muffleWarning")
  }
)
stopifnot(is.character(case_warning), length(case_warning) == 1L)
stopifnot(grepl("empirical_setting", case_warning, fixed = TRUE))
stopifnot(grepl("standardized_findings_summary", case_warning, fixed = TRUE))
stopifnot(!grepl("identification_strategy", case_warning, fixed = TRUE))
stopifnot(!grepl("descriptive_statistics_summary", case_warning, fixed = TRUE))

digest_template_v3_sample <- litxr::litxr_llm_digest_template("doi:10.1000/v3-sample", schema_version = "v3")
digest_template_v3_sample$summary <- "Sample size narrative summary"
digest_template_v3_sample$motivation <- "Sample size narrative motivation"
digest_template_v3_sample$paper_type <- "empirical archival"
digest_template_v3_sample$research_data$sample_size <- "VM scheduling: training scenario with VM size 50 and test scenarios with VM sizes 30, 100, 150, and 200."
stopifnot(isTRUE(invisible(litxr::litxr_validate_llm_digest(digest_template_v3_sample))))
litxr::litxr_write_llm_digest("doi:10.1000/v3-sample", digest_template_v3_sample, cfg, keep_history = FALSE, bump_revision = FALSE)
sample_read <- litxr::litxr_read_llm_digest("doi:10.1000/v3-sample", cfg)
stopifnot(identical(
  as.character(sample_read$research_data[["sample_size_note"]][[1]]),
  "VM scheduling: training scenario with VM size 50 and test scenarios with VM sizes 30, 100, 150, and 200."
))
stopifnot(is.null(sample_read$research_data[["sample_size"]]))

legacy_inline_v3 <- litxr::litxr_llm_digest_template("doi:10.1000/v3-legacy", schema_version = "v3")
legacy_inline_v3$summary <- "Legacy inline summary"
legacy_inline_v3$motivation <- "Legacy inline motivation"
legacy_inline_v3$anchor_references <- list(
  list(ref_id = "doi:10.1000/v3-legacy", V1 = "cite-1", V2 = "cite-2"),
  list(ref_id = "doi:10.1000/v3-legacy", V1 = "Title 1", V2 = "Title 2"),
  list(ref_id = "doi:10.1000/v3-legacy", V1 = "Reason 1", V2 = "Reason 2"),
  list(ref_id = "doi:10.1000/v3-legacy", V1 = "builds_on", V2 = "compares_with")
)
legacy_inline_v3$citation_logic_nodes <- list(
  list(ref_id = "doi:10.1000/v3-legacy", V1 = "improves", V2 = "contradicts"),
  list(ref_id = "doi:10.1000/v3-legacy", V1 = "A improves B.", V2 = "A contradicts C.")
)
litxr::litxr_write_llm_digest("doi:10.1000/v3-legacy", legacy_inline_v3, cfg, keep_history = FALSE, bump_revision = FALSE)
legacy_inline_read <- litxr::litxr_read_llm_digest("doi:10.1000/v3-legacy", cfg)
stopifnot(identical(legacy_inline_read$schema_version, "v3"))
stopifnot(inherits(legacy_inline_read$anchor_references, "data.table"))
stopifnot(inherits(legacy_inline_read$citation_logic_nodes, "data.table"))
stopifnot(all(c("anchor_rank", "citation_key", "anchor_title", "reason", "relationship_to_current_paper") %in% names(legacy_inline_read$anchor_references)))
stopifnot(all(c("node_id", "logic_type", "claim_sentence") %in% names(legacy_inline_read$citation_logic_nodes)))

partial_inline_v3 <- litxr::litxr_llm_digest_template("doi:10.1000/v3-partial", schema_version = "v3")
partial_inline_v3$summary <- "Partial inline summary"
partial_inline_v3$motivation <- "Partial inline motivation"
partial_inline_v3$anchor_references <- list(
  list(ref_id = "doi:10.1000/v3-partial", V1 = "cite-1"),
  list(ref_id = "doi:10.1000/v3-partial", V1 = "Title 1"),
  list(ref_id = "doi:10.1000/v3-partial", V1 = "Reason 1")
)
partial_inline_v3$citation_logic_nodes <- list(
  list(ref_id = "doi:10.1000/v3-partial", V1 = "improves"),
  list(ref_id = "doi:10.1000/v3-partial", V1 = "A improves B.")
)
litxr::litxr_write_llm_digest("doi:10.1000/v3-partial", partial_inline_v3, cfg, keep_history = FALSE, bump_revision = FALSE)
partial_inline_read <- litxr::litxr_read_llm_digest("doi:10.1000/v3-partial", cfg)
stopifnot(nrow(partial_inline_read$anchor_references) == 1L)
stopifnot(nrow(partial_inline_read$citation_logic_nodes) == 1L)

history_path_safe <- litxr:::.litxr_llm_digest_history_path(
  cfg,
  "doi:10.1000/v3-partial",
  list(
    ref_id = "doi:10.1000/v3-partial",
    schema_version = "v3",
    updated_at = "2026-05-02 00:00:00 UTC"
  )
)
stopifnot(is.character(history_path_safe), length(history_path_safe) == 1L, nzchar(history_path_safe))

identity_inline_v3 <- litxr::litxr_llm_digest_template("doi:10.1000/v3-identity", schema_version = "v3")
identity_inline_v3$summary <- "Identity inline summary"
identity_inline_v3$motivation <- "Identity inline motivation"
identity_inline_v3$anchor_references <- list(
  list(reference = "Boyd et al. (2011), Distributed optimization and statistical learning via the alternating direction method of multipliers", role = "technical foundation", reason = "Provides the ADMM background")
)
identity_inline_v3$citation_logic_nodes <- list(
  list(logic_type = "classification", sentence = "Optimization problem solving can be represented as search over a composite space.", relation = "A can be classified into B, C, D, and E", reuse_context = "Use when defining an optimization-space ontology.")
)
litxr::litxr_write_llm_digest("doi:10.1000/v3-identity", identity_inline_v3, cfg, keep_history = FALSE, bump_revision = FALSE)
identity_inline_read <- litxr::litxr_read_llm_digest("doi:10.1000/v3-identity", cfg)
stopifnot(identical(as.character(identity_inline_read$anchor_references$reference[[1]]), "Boyd et al. (2011), Distributed optimization and statistical learning via the alternating direction method of multipliers"))
stopifnot(identical(as.character(identity_inline_read$anchor_references$role[[1]]), "technical foundation"))
stopifnot(identical(as.character(identity_inline_read$anchor_references$reason[[1]]), "Provides the ADMM background"))
stopifnot(identical(as.character(identity_inline_read$citation_logic_nodes$claim_sentence[[1]]), "Optimization problem solving can be represented as search over a composite space."))
stopifnot(identical(as.character(identity_inline_read$citation_logic_nodes$modifier_text[[1]]), "A can be classified into B, C, D, and E"))
stopifnot(identical(as.character(identity_inline_read$citation_logic_nodes$citation_use[[1]]), "Use when defining an optimization-space ontology."))

duplicate_tag_v3 <- litxr::litxr_llm_digest_template("doi:10.1000/v3-dup-tags", schema_version = "v3")
duplicate_tag_v3$summary <- "Duplicate tag summary"
duplicate_tag_v3$motivation <- "Duplicate tag motivation"
duplicate_tag_v3$citation_logic_nodes <- data.table::data.table(
  ref_id = "doi:10.1000/v3-dup-tags",
  node_id = c("node_1", "node_1"),
  claim_sentence = c(
    "Agentic workflows can reduce coordination burdens.",
    "Agentic workflows can reduce coordination burdens."
  ),
  logic_type = c("A_reduces_B", "A_reduces_B"),
  subject_text = c("agentic workflows", "agentic workflows"),
  object_text = c("coordination burdens", "coordination burdens"),
  modifier_text = c("through specialization", "through specialization"),
  evidence_role = c("conceptual_argument", "conceptual_argument"),
  confidence = c("high", "high"),
  page_or_section = c("Section 3", "Section 3"),
  quote_support = c("", ""),
  citation_use = c("Use for workflow design arguments.", "Use for workflow design arguments."),
  tags = c("coordination", "specialization"),
  created_at = NA_character_,
  updated_at = NA_character_
)
litxr::litxr_write_llm_digest("doi:10.1000/v3-dup-tags", duplicate_tag_v3, cfg, keep_history = FALSE, bump_revision = FALSE)
duplicate_tag_read <- litxr::litxr_read_llm_digest("doi:10.1000/v3-dup-tags", cfg)
stopifnot(nrow(duplicate_tag_read$citation_logic_nodes) == 1L)
stopifnot(identical(as.character(duplicate_tag_read$citation_logic_nodes$tags[[1]]), "coordination; specialization"))

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
stopifnot(isTRUE(invisible(litxr::litxr_validate_citation_logic_nodes(data.table::data.table(ref_id = "doi:1", node_id = "", claim_sentence = "x")))))

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
  collection_id = journal$journal_id,
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
litxr::litxr_build_entity_indexes(cfg)
schema_status <- litxr::litxr_read_research_schema_status(cfg)
row_a <- schema_status[schema_status$ref_id == "doi:10.1000/a", ]
stopifnot(nrow(row_a) == 1L)

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
