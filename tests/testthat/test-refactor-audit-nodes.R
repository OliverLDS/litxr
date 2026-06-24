make_record <- function(ref_id, source, source_id, title, collection_id, collection_title) {
  data.table::data.table(
    ref_id = ref_id,
    source = source,
    source_id = source_id,
    title = title,
    abstract = sprintf("Abstract for %s", ref_id),
    authors = "Jane Doe; John Smith",
    authors_list = list(c("Jane Doe", "John Smith")),
    pub_date = as.POSIXct("2025-01-03", tz = "UTC"),
    year = 2025L,
    month = 1L,
    day = 3L,
    journal = "Example Journal",
    container_title = "Example Journal",
    publisher = "Example Publisher",
    volume = "12",
    issue = "3",
    pages = "100-110",
    doi = NA_character_,
    note = NA_character_,
    subject_primary = "AI",
    subject_all = "AI",
    url_landing = NA_character_,
    url_pdf = NA_character_,
    arxiv_version = NA_integer_,
    arxiv_primary_category = NA_character_,
    arxiv_categories_raw = "",
    arxiv_comment = NA_character_,
    arxiv_journal_ref = NA_character_,
    raw_entry = list(NULL),
    collection_id = collection_id,
    collection_title = collection_title
  )
}

test_that("refactor audit node script emits all report sections", {
  td <- tempfile("litxr-refactor-audit-")
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
  journal <- cfg$journals[[1]]
  arxiv_collection <- Filter(function(collection) identical(collection$remote_channel, "arxiv"), cfg$collections)[[1]]
  journal_local_path <- litxr:::.litxr_resolve_local_path(cfg, journal$local_path)
  arxiv_local_path <- litxr:::.litxr_resolve_local_path(cfg, arxiv_collection$local_path)

  doi_record <- make_record(
    "doi:10.1000/audit-example",
    "crossref",
    "10.1000/audit-example",
    "Audit Example Published Paper",
    journal$journal_id,
    journal$title
  )
  doi_record$doi[[1]] <- "10.1000/audit-example"
  doi_record$url_landing[[1]] <- "https://doi.org/10.1000/audit-example"

  arxiv_record <- make_record(
    "arxiv:2501.00001",
    "arxiv",
    "2501.00001",
    "Audit Example ArXiv Paper",
    arxiv_collection$collection_id,
    arxiv_collection$title
  )
  arxiv_record$url_landing[[1]] <- "https://arxiv.org/abs/2501.00001v1"
  arxiv_record$url_pdf[[1]] <- "https://arxiv.org/pdf/2501.00001v1"
  arxiv_record$arxiv_version[[1]] <- 1L
  arxiv_record$arxiv_primary_category[[1]] <- "cs.AI"
  arxiv_record$arxiv_categories_raw[[1]] <- "cs.AI"

  litxr:::.litxr_write_journal_records(doi_record, journal_local_path, journal, cfg = cfg)
  litxr:::.litxr_write_journal_records(arxiv_record, arxiv_local_path, arxiv_collection, cfg = cfg)

  mock_embed <- function(texts) {
    cbind(
      neural = as.numeric(grepl("neural", tolower(texts), fixed = TRUE)),
      finance = as.numeric(grepl("finance", tolower(texts), fixed = TRUE)),
      length = nchar(texts) / 100
    )
  }
  litxr::litxr_build_embedding_index(
    arxiv_collection$collection_id,
    cfg,
    field = "abstract",
    embed_fun = mock_embed,
    model = "mock-audit-embedding-v1",
    provider = "mock",
    batch_size = 1L,
    overwrite = TRUE
  )
  arxiv_record2 <- data.table::copy(arxiv_record)
  arxiv_record2$ref_id[[1]] <- "arxiv:2501.00002"
  arxiv_record2$source_id[[1]] <- "2501.00002"
  arxiv_record2$title[[1]] <- "Audit Example ArXiv Paper Two"
  arxiv_record2$abstract[[1]] <- "A second example arXiv paper."
  arxiv_record2$url_landing[[1]] <- "https://arxiv.org/abs/2501.00002v1"
  arxiv_record2$url_pdf[[1]] <- "https://arxiv.org/pdf/2501.00002v1"
  litxr:::.litxr_write_journal_records(arxiv_record2, arxiv_local_path, arxiv_collection, cfg = cfg)
  litxr::litxr_embed_collection_delta(
    arxiv_collection$collection_id,
    cfg,
    field = "abstract",
    embed_fun = mock_embed,
    model = "mock-audit-embedding-v1",
    provider = "mock",
    batch_size = 1L,
    overwrite = FALSE,
    limit = 1L
  )

  litxr::litxr_write_standardized_findings(data.table::data.table(
    ref_id = "doi:10.1000/audit-example",
    finding_id = "finding_1",
    paper_type = "empirical_archival",
    research_question = "What happens?",
    finding_text = "An audit finding.",
    notes = "audit"
  ), config = cfg)
  litxr::litxr_write_descriptive_stats(data.table::data.table(
    ref_id = "doi:10.1000/audit-example",
    table_id = "table_1",
    variable = "n",
    n = 1L
  ), config = cfg)
  litxr::litxr_write_anchor_references(data.table::data.table(
    ref_id = "doi:10.1000/audit-example",
    anchor_rank = 1L,
    citation_key = "audit_anchor",
    anchor_title = "Anchor Title",
    anchor_year = 2024L,
    anchor_role = "main_comparison",
    reason = "audit",
    relationship_to_current_paper = "uses_as_context",
    confidence = "high"
  ), config = cfg)
  litxr::litxr_write_citation_logic_nodes(data.table::data.table(
    ref_id = "doi:10.1000/audit-example",
    node_id = "node_1",
    claim_sentence = "Audit nodes support reporting.",
    logic_type = "A_supports_B",
    subject_text = "audit nodes",
    object_text = "reporting",
    evidence_role = "background_context",
    confidence = "high",
    tags = "audit"
  ), config = cfg)

  pending_path <- litxr:::.litxr_ref_local_pending_path(cfg)
  fst::write_fst(as.data.frame(data.table::data.table(
    ref_id = "local:pending-audit",
    key_type = "local_pending",
    key_value = "local:pending-audit"
  )), pending_path)

  script_path <- normalizePath(file.path("..", "..", "scripts", "refactor_audit_nodes.R"), mustWork = TRUE)
  source_env <- new.env(parent = baseenv())
  Sys.setenv(LITXR_REFRACTOR_AUDIT_NODES_SOURCE_ONLY = "1")
  on.exit(Sys.unsetenv("LITXR_REFRACTOR_AUDIT_NODES_SOURCE_ONLY"), add = TRUE)
  source(script_path, local = source_env)
  derived <- source_env$report_derived_append_shard_state(cfg)
  expect_true(is.list(derived))
  expect_true(derived$derived_delta_count >= 1L)
  expect_true(derived$append_shard_count >= 1L)
  legacy <- source_env$report_legacy_delta_presence(cfg)
  expect_true(all(c("project_reference_cache", "collection_reference_cache") %in% names(legacy)))
  refresh_timing <- source_env$report_entity_refresh_timing(cfg, sample_n = 1L)
  expect_true(is.list(refresh_timing))
  expect_true(refresh_timing$sample_ref_count >= 1L)
  runtime_projection <- source_env$report_runtime_compatibility_projection(cfg, oversized_mb = 0.0001)
  expect_true(is.list(runtime_projection))
  expect_true(all(c("summary", "duplicate_identity_conflicts", "orphan_arxiv_payload_rows", "orphan_doi_payload_rows", "unresolved_local_pending_rows", "compatibility_runtime_output_stale") %in% names(runtime_projection)))
  embedding_timing <- source_env$report_embedding_search_shard_timing(cfg)
  expect_true(is.list(embedding_timing))
  expect_true(all(c("collection_id", "field", "model", "manifest_path", "shard_dir", "top_n", "result_n", "elapsed_sec") %in% names(embedding_timing)))
  projection_reduction <- source_env$measure_projection_reduction(cfg)
  expect_true(is.list(projection_reduction))
  expect_true(all(c("project", "collections") %in% names(projection_reduction)))
  identity_audit <- source_env$report_identity_conflict_audit(cfg)
  expect_true(is.list(identity_audit))
  expect_true("duplicate_identity_conflicts" %in% names(identity_audit))
  pending_audit <- source_env$report_unresolved_local_pending_audit(cfg)
  expect_true(is.list(pending_audit))
  expect_true("unresolved_local_pending_rows" %in% names(pending_audit))
})
