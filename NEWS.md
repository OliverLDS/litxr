# litxr 0.0.6.3

- Added embedding-cache inspection and reset helpers:
  `litxr_diagnose_embedding_cache()` and `litxr_reset_embedding_cache()`.
- Added embedding coverage helpers:
  `litxr_read_embedding_state()` and `litxr_embedding_date_stats()`.
- Added `litxr_score_collection_categories_delta()` for category scoring over
  delta shards only.
- Made `litxr_build_embedding_index()` preserve delta shards when the legacy
  main matrix cache is unreadable instead of failing destructively.
- Added explicit warnings when a category query set exists only under other
  cached embedding models.

# litxr 0.0.6.2

- Added embedding-based category labeling helpers:
  `litxr_build_label_query_index()`,
  `litxr_score_collection_categories()`, and
  `litxr_label_collection_by_category()`.
- Added support for precomputed query vectors in
  `litxr_search_embeddings()` and `litxr_search_embedding_delta()` so routine
  searches do not need a fresh embedding API call.
- Added explicit delta-only embedding helpers:
  `litxr_embed_collection_delta()`, `litxr_compact_embedding_delta()`, and
  `litxr_search_embedding_delta()`.
- Reduced compacted corpus embedding metadata to `ref_id`, `title`, and `year`,
  and reduced label-query metadata to category/query fields only.
- Changed newly compacted corpus embedding caches to float32 year-sharded
  storage, with atomic writes for the compacted cache files.
- Added a checked-in AI category query set under `inst/extdata/` and a sample
  labeling workflow script under `scripts/label_arxiv_categories.R`.

# litxr 0.0.6.1

- Made `litxr_build_embedding_index()` write completed batches to append-only
  delta shards and compact them into the main embedding index once at the end.
- Kept interrupted embedding builds resumable by retaining delta shards until a
  successful final compaction writes the main metadata and matrix files.
- Made exact `litxr_find_refs(ref_id = ...)` searches faster by using row-level
  FST reads and falling back to collection indexes when the project index is
  stale.
- Accepted bare arXiv ids such as `"2405.03710"` in `litxr_find_refs()` in
  addition to canonical ids such as `"arxiv:2405.03710"`.

# litxr 0.0.6

- Added provider-neutral cached embedding indexes for collection text fields,
  starting with abstract embeddings from `litxr_read_collection()`.
- Added `litxr_build_embedding_index()`, `litxr_read_embedding_index()`,
  `litxr_search_embeddings()`, and `litxr_cosine_similarity()`.
- Store embedding metadata, numeric matrices, and manifests under
  `project.data_root/embeddings/`, keyed by collection, field, and exact model.
- Documented how to use external embedding functions such as
  `inferencer::embed_openrouter()` or `inferencer::embed_gemini()` while keeping
  the package independent of a specific embedding provider.

# litxr 0.0.5.8

- Added `litxr_next_arxiv_repair_range()` to compute the next arXiv repair
  window from either the sync ledger or the collection index.
- Added `scripts/repair_arxiv_latest.R`, a shell-friendly wrapper that computes
  the next repair window and calls `scripts/repair_arxiv_range.R` through an
  absolute script path.
- Documented collection-index versus sync-state date bases for latest arXiv
  repair planning.

# litxr 0.0.5.7

- Added a collection-local delta FST index for arXiv range repair so fetched
  pages are appended to `index/references_delta.fst` during a run and compacted
  into `index/references.fst` once at the end or on exit/error.
- Added `litxr_compact_collection_index()` for explicit delta compaction and
  made `litxr_read_collection()` include pending delta rows before compaction.
- Removed the extra post-day sleep in `scripts/repair_arxiv_range.R`; request
  pacing remains before each active day and between paginated API requests.

# litxr 0.0.5.6

- Made project-level index refresh opt-in for `scripts/repair_arxiv_range.R`
  through `--refresh-project-index`.
- Kept collection-level index refresh enabled by default so
  `litxr_read_collection()` and `litxr_collection_date_stats()` see repaired
  records without paying the full project-index refresh cost.

# litxr 0.0.5.5

- Restored the faster default behavior in `scripts/repair_arxiv_range.R` by
  flushing indexes at the end and on exit/error instead of after every day.
- Added `--flush-each-day` for workflows that explicitly need day-by-day index
  visibility during long arXiv repair runs.

# litxr 0.0.5.4

- Added `litxr_refresh_collection_index()` for fast mtime-based index refresh
  from recently changed JSON record files.
- Kept `litxr_rebuild_collection_index()` as the full correctness-first rebuild
  path for schema migrations, legacy cleanup, and suspected index corruption.
- Updated arXiv range repair to flush indexes after completed days and on
  script exit/error when records were written.

# litxr 0.0.5.3

- Reduced repeated full-collection JSON rewrites in small-update paths such as
  arXiv range repair, bounded collection repair, DOI ingest, and manual
  reference ingest.
- Updated arXiv range repair to keep the working collection in memory, write
  only touched JSON record files during the loop, and refresh local/project
  indexes once at the end.
- Made collection-index rebuild refresh project-level indexes for consistency
  with the canonical project store.

# litxr 0.0.5.2

- Fixed arXiv deduplication so base arXiv ids remain the canonical key even
  when DOI metadata is present for only some versions of the same paper.
- Fixed collection-index rebuilds to rewrite stored `ref_id` and `source_id`
  values to the normalized base arXiv id instead of only normalizing the
  internal deduplication key.

# litxr 0.0.5.1

- Canonicalized arXiv reference identity to the base arXiv id so versioned
  records such as `v1` and `v2` now merge into one paper while preserving
  version metadata fields.
- Extended local identity normalization so legacy versioned arXiv rows are
  collapsed to the base id during rebuild and upsert.
- Strengthened arXiv rate-limit handling with larger retry backoff and support
  for honoring `Retry-After` when returned by the API.
- Updated `scripts/repair_arxiv_range.R` to sleep before the first live request
  of each active day, reducing immediate rerun pressure after skipped days.

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
