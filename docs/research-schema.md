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
- `empirical_archival`
- `empirical_experimental`
- `empirical_survey`
- `empirical_case_study`
- `methodological`
- `review`
- `meta_analysis`
- `dataset`
- `policy_report`
- `book`
- `unknown`

Normalization lowercases, trims, replaces spaces and hyphens with underscores,
maps common aliases, and falls back to `unknown` for missing or empty values.

## Digest Schema V2

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
