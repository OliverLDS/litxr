# Research Schema

`litxr` keeps literature as local data keyed by canonical `ref_id`. The
research-analysis layer extends that same architecture instead of introducing a
new object system.

## Why No R6 Reference Objects

`litxr` does not use R6 classes for individual references.

Reasons:

- local storage and batch workflows are the core use case
- `data.table` plus `fst` is a better fit for large project-level scans,
  compaction, and filtering
- canonical `ref_id` keys make joins simpler across references, embeddings,
  digests, findings, and descriptive statistics
- external AI agents can read and write the stored data directly without being
  hosted inside the package runtime

## Paper Type Vocabulary

Use:

- `litxr_paper_type_levels()`
- `litxr_normalize_paper_type()`
- `litxr_validate_paper_type()`

Canonical levels:

- `theoretical`
- `conceptual`
- `empirical_archival`
- `empirical_experimental`
- `empirical_survey`
- `empirical_qualitative`
- `empirical_case_study`
- `empirical_mixed_methods`
- `methodological`
- `computational`
- `simulation`
- `benchmark`
- `system_design`
- `review_narrative`
- `review_systematic`
- `review_scoping`
- `meta_analysis`
- `replication`
- `registered_report`
- `study_protocol`
- `policy_analysis`
- `perspective`
- `commentary`
- `review`
- `dataset`
- `policy_report`
- `book`
- `unknown`

Normalization lowercases, trims, replaces spaces and hyphens with underscores,
maps common aliases, and falls back to `unknown` for missing or empty values.
Legacy labels such as `review`, `dataset`, `policy_report`, and `book` remain
accepted for backward compatibility.

## Digest Schema V4

Schema `v4` is the current schema for new interactive LLM digest prompts. It
extends `v3` without breaking `v1`, `v2`, or `v3` reads.

Required top-level fields:

- `schema_version`: must be `v4`.
- `ref_id`: canonical litxr reference id.
- `digest_revision`: positive integer revision counter.
- `derived_from_revision`: optional previous revision.
- `extraction_mode`: extraction workflow label.
- `prompt_version`: prompt-template version.
- `model_hint`: optional external model label.
- `paper_type`: one value from `litxr_paper_type_levels()`.
- `summary`: concise plain-language summary.
- `motivation`: gap, puzzle, debate, problem, or need motivating the paper.
- `research_questions`: explicit or inferred questions.
- `paper_structure`: section-level structure and purpose.
- `methods`: methods, research design, analytical procedure, or review strategy.
- `research_data`: data-source and sample context.
- `main_variables`: dependent, independent, control, and mechanism variables.
- `key_findings`: main results, conclusions, propositions, or claims.
- `limitations`: author-stated or design-implied limitations.
- `theoretical_mechanism`: conceptual, causal, formal, or process mechanism.
- `contribution_type`: unordered contribution labels. Prefer
  `theory_building`, `theory_testing`, `conceptual_framework`,
  `empirical_evidence`, `causal_evidence`, `measurement`, `method`,
  `algorithm`, `benchmark`, `system_architecture`, `replication`,
  `literature_synthesis`, `policy_implication`, `business_implication`,
  `research_agenda`, or `other`.
- `ranked_contributions`: ranked contribution objects with `rank`,
  `contribution_type`, `contribution`, and `reason`.
- `likely_reader_misconceptions`: ways an average reader might misunderstand,
  overgeneralize, or misuse the paper.
- `business_relevance_pathway`: concrete ways the paper can matter for business
  decisions, operations, governance, products, strategy, risk, or workflows.
- `tables`: recognized structured tables from the paper as an array of table
  objects. Use an empty array when no table can be represented reliably. Each
  object should include `table_id`, `title`, `source_location`, `columns`,
  `rows`, and `notes`.
- `research_target_github_links`: GitHub repositories that are the paper's
  research target or artifact, such as a model, framework, system, dataset,
  benchmark, mechanism, package, or implementation. Incidental GitHub citations
  should not be included. Each object should include `url`, `category_tags`,
  `research_role`, `description`, and `evidence_context`.
- `evidence_strength`: legacy short evidence-strength field. Prefer
  `very_low`, `low`, `medium`, `high`, `very_high`, `not_applicable`, or
  `unknown`; a short explanatory string remains accepted for backward
  compatibility.
- `evidence_shape`: general evidence structure for any paper type.
- `anchor_references`: optional inline anchor-reference rows.
- `citation_logic_nodes`: optional inline citation-logic rows.
- `keywords`: retrieval keywords.
- `notes`: free-form notes.
- `generated_at`: creation timestamp.
- `updated_at`: write/update timestamp.

`identification_strategy` remains an optional empirical field. It means how the
paper supports its empirical or causal claim through research design. Examples
include randomized assignment, quasi-experimental design, panel or
observational identification, comparison logic, triangulation, qualitative
process tracing, benchmark protocol, or a clear statement that no causal
identification is claimed.

`evidence_shape` generalizes evidence beyond empirical papers:

- `evidence_mode`: one of `empirical_quantitative`,
  `empirical_qualitative`, `experimental`, `simulation`, `benchmark`,
  `theoretical_model`, `conceptual_argument`,
  `methodological_demonstration`, `review_synthesis`, `policy_analysis`,
  `descriptive`, `none`, `unknown`.
- `evidence_basis`: short points describing what the paper uses as support.
- `inference_type`: one of `causal`, `associational`, `predictive`,
  `descriptive`, `mechanistic`, `formal`, `interpretive`, `comparative`,
  `normative`, `synthetic`, `not_applicable`, `unknown`.
- `strength_level`: one of `very_low`, `low`, `medium`, `high`,
  `very_high`, `not_applicable`, `unknown`.
- `limitations`: short evidence limitations.

## Digest Schema V2/V3

`litxr_llm_digest_template()` now defaults to schema `v2`.

Main fields:

- `schema_version`
- `ref_id`
- `paper_type`
- `summary`
- `motivation`
- `research_questions`
- `paper_structure`
- `methods`
- `research_data`
  - `sample_size` stays numeric when a real count exists
  - `sample_size_note` carries narrative or ambiguous sample-size context
- `identification_strategy`
- `main_variables`
- `key_findings`
- `limitations`
- `theoretical_mechanism`
- `empirical_setting`
- `descriptive_statistics_summary`
- `standardized_findings_summary`
- `contribution_type`
- `evidence_strength`
- `keywords`
- `notes`
- `generated_at`

Backward compatibility:

- legacy digests without `schema_version` still read and validate
- validation remains version-aware instead of forcing old files into the new
  shape
- new writes use `paper_type` normalization automatically

Schema `v3` adds optional inline fields:

- `anchor_references`
- `citation_logic_nodes`

These are carried through when a digest is written with schema `v3`, and the
same project-level tables can also be maintained independently under
`project.data_root/findings/`.

## Standardized Findings Table

Project-level paths:

- `project.data_root/findings/standardized_findings.fst`
- `project.data_root/findings/standardized_findings_delta.fst`

Helpers:

- `litxr_standardized_findings_template()`
- `litxr_validate_standardized_findings()`
- `litxr_write_standardized_findings()`
- `litxr_read_standardized_findings()`
- `litxr_find_standardized_findings()`
- `litxr_compact_standardized_findings()`
- `litxr_rebuild_standardized_findings()`

Design:

- write new or updated rows into the delta store
- read as `main + delta` merged by `ref_id + finding_id`
- compact explicitly to rewrite the main store and clear delta

## Descriptive Statistics Table

Project-level paths:

- `project.data_root/findings/descriptive_statistics.fst`
- `project.data_root/findings/descriptive_statistics_delta.fst`

Helpers:

- `litxr_descriptive_stats_template()`
- `litxr_validate_descriptive_stats()`
- `litxr_write_descriptive_stats()`
- `litxr_read_descriptive_stats()`
- `litxr_find_descriptive_stats()`
- `litxr_compact_descriptive_stats()`
- `litxr_rebuild_descriptive_stats()`

Design:

- write into delta by default
- read as `main + delta` merged by `ref_id + table_id + variable`
- allow incomplete numeric statistics with `NA`

## Anchor References Table

Project-level paths:

- `project.data_root/findings/anchor_references.fst`
- `project.data_root/findings/anchor_references_delta.fst`

Helpers:

- `litxr_anchor_references_template()`
- `litxr_validate_anchor_references()`
- `litxr_write_anchor_references()`
- `litxr_read_anchor_references()`
- `litxr_find_anchor_references()`
- `litxr_compact_anchor_references()`
- `litxr_rebuild_anchor_references()`

Design:

- write new or updated rows into the delta store
- read as `main + delta` merged by `ref_id + anchor_rank`
- compact explicitly to rewrite the main store and clear delta

## Citation Logic Nodes Table

Project-level paths:

- `project.data_root/findings/citation_logic_nodes.fst`
- `project.data_root/findings/citation_logic_nodes_delta.fst`

Helpers:

- `litxr_citation_logic_nodes_template()`
- `litxr_validate_citation_logic_nodes()`
- `litxr_write_citation_logic_nodes()`
- `litxr_read_citation_logic_nodes()`
- `litxr_find_citation_logic_nodes()`
- `litxr_compact_citation_logic_nodes()`
- `litxr_rebuild_citation_logic_nodes()`

Design:

- write new or updated rows into the delta store
- read as `main + delta` merged by `ref_id + node_id`
- compact explicitly to rewrite the main store and clear delta

## Main / Delta / Compact Pattern

The new research-analysis tables reuse the package’s existing local-data
pattern:

1. append or upsert into a delta fst file
2. read as merged main plus delta
3. compact explicitly when you want a clean main file

This keeps iterative extraction cheap and avoids repeatedly rewriting the full
project-level main table for every small update.

## Coverage And Maintenance

Use:

- `litxr_read_research_schema_status()`
- `litxr_find_refs_missing_llm_digest()`
- `litxr_find_refs_missing_standardized_findings()`
- `litxr_find_refs_missing_descriptive_stats()`
- `litxr_find_refs_missing_anchor_references()`
- `litxr_find_refs_missing_citation_logic_nodes()`

For digest maintenance, use:

- `litxr_upgrade_llm_digests()`

This rewrites legacy digest JSON through the current v2 write path so old
project-level digests become explicit schema-v2 payloads.

## External AI Agents

External AI agents can use these layers by:

- reading canonical references by `ref_id`
- reading markdown from `project.data_root/md/`
- reading and writing digests under `project.data_root/llm/`
- reading and writing findings/statistics under `project.data_root/findings/`

`litxr` provides the storage, validation, indexing, and compaction layer. It
does not host essay-writing agents, meta-analysis agents, or a Shiny interface
inside the package.
