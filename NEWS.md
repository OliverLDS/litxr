# litxr 0.0.5

- Fixed collection `local_path` resolution so relative paths are resolved under
  `project.data_root`, while absolute paths continue to work as absolute paths.
- Added `litxr_rebuild_sync_state()` to infer a first-pass sync ledger from
  existing local collection data created before sync-state recording existed.
- Extended sync-state rows with `fetched_from` and `fetched_to` fields for more
  informative sync and repair history, especially for arXiv workflows.
- Added `litxr_collection_date_stats()` for collection-level publication-date
  counts, ascending date coverage summaries, and missing-day diagnostics.

# litxr 0.0.4

- Generalized the package from journal-first config toward collection-first
  architecture, while keeping backward-compatible journal wrappers.
- Added canonical project-level indexes for references, collection memberships,
  enrichment status, and sync state.
- Added manual reference ingest, richer identifier support, and `entry_type`
  storage for broader BibTeX coverage beyond DOI-centric journal articles.
- Added project-level markdown and structured LLM digest storage, search, batch
  digest-building helpers, and shell scripts for enrichment workflows.
- Added sync history recording and arXiv day-range repair skipping through a
  project-level sync ledger.
- Split longer package guidance into focused docs under `docs/` and trimmed the
  README into a shorter landing page.

# litxr 0.0.3

- Added `litxr_add_dois()` for DOI-batch ingestion across journals, with
  automatic Crossref journal registration into `config.yaml` when needed.
- Extended `litxr_export_bib()` to export by record keys such as DOI, `ref_id`,
  or `source_id`, with warnings for missing keys.
- Made config loading tolerant of tab-indented YAML files.
- Added `scripts/repair_arxiv_range.R` for day-by-day arXiv repair with
  per-day pagination and sleep intervals.
- Updated the arXiv repair scripts to load the local repo code when run via
  `Rscript`, avoiding stale installed-package behavior.

# litxr 0.0.2

- Added explicit journal upsert behavior with conflict logging for unusual
  metadata replacements.
- Added `fst`-backed journal indexes and `litxr_rebuild_journal_index()` for
  faster local reads.
- Improved Crossref and arXiv parsing robustness across payload shape
  variations.
- Added arXiv request pacing, retry handling, and `litxr_repair_journal()` for
  bounded repair and backfill runs.
- Added `scripts/repair_arxiv.R` as a small CLI wrapper for shell-driven arXiv
  repair jobs.
- Clarified local storage layout, including future `pdf/`, `md/`, and `llm/`
  directions.

# litxr 0.0.1

- Added project initialization with local `.litxr/config.yaml` support.
- Added journal-centric local storage and BibTeX export helpers.
- Added arXiv and Crossref parsing helpers for a unified reference schema.
- Added cursor-paginated Crossref journal sync support.
- Added README guidance, package documentation, and initial test coverage.
