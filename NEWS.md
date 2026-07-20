# litxr 0.1.8.6

- Added schema-v5 LLM digests as a backward-compatible expansion of the
  complete schema-v4 surface.
- Added the optional `source_detail` layer for source-grounded evidence,
  equations, benchmark tables, precise wording, methodological disputes,
  coverage declarations, and drafting-safety status.
- Added V5 validation for unique evidence ids, source locators, V4 support
  links, bounded wording excerpts, and evidence references from disputes and
  result collections.
- Moved new interactive digest prompts and ingest defaults to `v5.0` while
  retaining V3/V4 prompt and digest compatibility.
- Exported the digest template, read, write, and validation APIs for
  installed-package callers, and added theory-heavy and experiment-heavy V5
  fixtures.

# litxr 0.1.8.5

- Expanded the thin-index and cached-digest workflow so schema refreshes and
  digest ingestion stay on the thin `llm_digest` index instead of old bootstrap
  readers.
- Narrowed BibTeX linked-DOI resolution to batch-local ids and kept the
  replacement/export path key-stable while preloading the needed thin lookup
  caches once per run.
- Refined arXiv/DOI collection sync and thin-store refresh helpers to follow
  the shared `ref/` and `log/` layout with incremental cutoff logs.
- Continued the embedding and lexical inquiry cleanup by trimming wide
  metadata hydration, tightening raw-abstract repair, and keeping keyword/BM25
  search on tokenized lexical caches.
- Added and reorganized wrapper scripts and supporting docs around the thin
  digest, BibTeX, embedding, and lexical workflows.

# litxr 0.1.8.4

- Added an explicit thin digest-store sync script for `index/llm_digest.fst`
  instead of bootstrapping that index from reader paths.
- Kept digest summary and ingest wrappers on the internal digest reader so the
  report path no longer depends on an exported status helper.

# litxr 0.1.8.3

- Fixed the digest summary wrapper so it resolves the current on-disk digest
  file before falling back to the digest index bootstrap path.
- Renamed the human summary wrapper to `scripts/report_ref_summary.sh` and
  kept the digest template call namespace-local.

# litxr 0.1.8.2

- Continued the embedding compaction hardening work by hoisting disk-space
  guards before shard metadata writes and staging shard-tree swaps.
- Renamed the category inquiry cache/report scripts to reflect inquiry-set
  semantics and removed obsolete wrapper scripts.
- Added narrow raw-abstract repair tooling for the arXiv abstract corpus.

# litxr 0.1.8.1

- Fixed the linked-DOI BibTeX replacement path so arXiv-keyed entries keep
  their original BibTeX keys while being rewritten with DOI-backed fields.
- Narrowed BibTeX link and scaffold resolution to the ids present in the
  current `.bib` batch instead of preloading broad identity/scaffold tables.
- Hardened DOI fallback ingestion and added focused DOI fallback regression
  coverage.

# litxr 0.1.8

- Added scaffolded lexical search modules for shard discovery, keyword/category
  labeling, and BM25 indexing/search.
- Added focused lexical regression tests for normalization, category label
  matching, and BM25 ranking.
- Aligned package metadata with the lexical module dependencies.

# litxr 0.1.7.9

- Reused a single preloaded BibTeX scaffold cache across each export batch so
  DOI and ISBN resolution no longer reopens scaffold tables per entry.

# litxr 0.1.7.8

- Fixed the BibTeX linked-DOI wrapper so linked DOI substitutions and
  unresolved-entry reporting stay in one wrapper result.

# litxr 0.1.7.7

- Updated the thin arXiv reference update path so newly fetched arXiv JSON can
  be added to `ref_arxiv.fst` before embedding metadata migration.
  - Kept the embedding metadata migration aligned with the narrow `ref_id` and
  `abstract` shard shape.

# litxr 0.1.7.6

- Added the DOI collection sync log as a shared project-level TSV under
  `log/`, keyed by `collection_id`.
- Added single-collection incremental support to the thin ref-store sync
  wrapper while keeping the default cutoff lookup collection-specific.

# litxr 0.1.7.5

- Refined the thin-store source metadata path so DOI collection sync can use
  collection-specific fetch cutoffs from the shared log.

# litxr 0.1.7.4

- Fixed the thin-store sync pipeline so incremental refreshes no longer
  remove rows or emit removal TSVs, and full rebuilds write the exact
  arXiv/DOI identity shape instead of carrying historical compatibility rows.
- Narrowed the arXiv identity-map write path to exclude blank DOI links and
  kept `ref_arxiv.fst` to the single `arxiv_id` column.

# litxr 0.1.7.3

- Removed the merge-on-write bottleneck from the normalized scaffold writer.
- Narrowed local JSON hydration for rebuild paths so projection rows are built
  without per-row `data.table` materialization.
- Kept the direct arXiv/DOI identity pairing path strict and graph-free.

# litxr 0.1.7.2

- Removed the remaining file-backed entity-status and collection-projection
  writers from the normalized runtime path.
- Kept status derivation runtime-only from `ref_identity_map`, `ref_arxiv`,
  `ref_doi`, and schema caches.
- Continued the strict no-projection policy for collection-local `index/*.fst`
  writes.

# litxr 0.1.7.1

- Made normalized authoritative stores the runtime source for compatibility
  projections instead of persisting the wide project-reference view as normal
  truth.
- Kept DOI/arXiv linkage link-first while hardening the normalized identity
  audit surface.
- Removed a remaining dead collection/DOI projection field from the narrow
  normalized payload projection path and added regression coverage for the
  audit/report nodes and normalized payload shape.

# litxr 0.1.7

- Introduced the normalized scaffold rewrite and lookup refactor that moved
  the package toward thin canonical identity/payload storage.
- Updated the refactor hardening tests and documentation to reflect the
  normalized scaffold shape and lookup policy.

# litxr 0.1.6

- Made normal embedding search shard-aware so the common path scores one shard
  at a time and only keeps shard-local top-N candidates before the final merge.
- Replaced row-wise full sorting in `top_k_mean` aggregation with partial
  top-k selection and kept query-score materialization opt-in.
- Added regression coverage for `top_n = 0`, ties, `NA` scores, score-column
  presence, and parity with the old small-corpus search behavior.

# litxr 0.1.5

- Reworked project lookup and hydration to use keyed batch reads instead of
  row-wise file probing in the common path.
- Added keyed hydration coverage for missing JSON, partial JSON, duplicate
  slug files, duplicate canonical ids, multi-collection rows, and runtime
  wide-projection guardrails.
- Kept list-column hydration stable at the API/export boundary while limiting
  wide materialization to the final matched result set.

# litxr 0.1.4

- Removed the live identity-table dependency from the entity/status read path and
  kept canonical resolution on the thin `entity_id` layer.
- Reworked the entity-index builders to use keyed joins and grouped
  aggregation in the main path, while keeping compatibility-only helpers out
  of the hot path.
- Made entity-status freshness updates incremental and narrowed the refresh
  path to touched rows instead of full rebuilds.
- Added smoke coverage for the narrow-first enrichment path and the entity
  status/report readers.

# litxr 0.1.3

- Rewrote the common upsert core around keyed `data.table` joins so the normal
  path no longer iterates key-by-key.
- Split upsert handling into existing-only, incoming-only, and matched rows,
  with vectorized fast paths for identical, coalescing, and local-priority
  cases.
- Kept arXiv version precedence and conflict logging semantics intact while
  restricting the row-wise merge helper to true conflict rows only.
- Added regression coverage for key-only, conflict, local-priority, version
  precedence, and duplicate-incoming-key cases.

# litxr 0.1.2.1

- Human-facing release label: `v0.1.2a`.
- Narrowed project-level lookup so wide projection materialization happens only
  after canonical filtering and only for the final matched set.
- Added a runtime wide-projection guardrail for project lookup and a focused
  regression test for narrow-first resolution.

# litxr 0.1.2

- Introduced the normalized identity/payload scaffold for `ref_entities`,
  `ref_arxiv`, `ref_doi`, and `ref_local_pending`.
- Retired normal bibliographic delta-cache ingest in favor of direct keyed
  upsert into authoritative payload tables.
- Added narrow derived-artifact append-shard helpers and compaction behavior
  for expensive stores such as embeddings and research-schema digests.
- Kept the earlier entity/status and refactor-compatibility machinery while
  tightening downstream canonical-only lookup policy.

# litxr 0.1.1

- Thinned the core exact-key lookup and research-schema/status read paths so
  they read only the columns they need from the project fst layers.
- Added a reusable fst subset reader plus column-projected project index
  helpers to reduce I/O and memory use in refactor hot paths.
- Kept the public return shapes stable while moving export, lookup, and status
  computation onto narrower internal reads.

# litxr 0.1.0

- Declared the v0.1.0 identity-first refactor complete.
- Finalized the internal architecture around thin `entity_id`-based indexes:
  `ref_identity_map.fst`, `entities.fst`, `entity_collections.fst`, and
  `entity_status.fst`.
- Kept `ref_id` as the stable user-facing identity while making task-policy
  resolution explicit:
  - citation export prefers linked published identities
  - digest/full-text prompting can prefer linked arXiv identities
- Demoted project `references.fst` and `reference_collections.fst` to
  compatibility projections rebuilt from authoritative collection state rather
  than primary mutable stores.
- Confirmed JSON remains the owner of rich payload while `fst` is used for thin
  relationships, search projections, and status.
- Split the old monolithic `R/sync.R` surface into focused internal modules and
  added architecture-focused regression coverage for projection rebuild and
  authoritative-JSON semantics.

# litxr 0.0.8.23

- Split the large refactor/read-projection surface out of `R/sync.R` into
  focused internal modules for lookup/read policy and projection/index storage.
- Added architecture-focused regression coverage for authoritative JSON
  ownership and compatibility projection rebuild semantics.
- Updated storage and maintenance docs to describe project reference caches as
  compatibility projections rather than primary semantics.

# litxr 0.0.8.22

- Demoted project `index/references.fst` and
  `index/reference_collections.fst` to compatibility projections.
- Made thin identity/entity rebuilds independent of healthy project compatibility
  caches by rebuilding from authoritative collection-backed state directly.
- Expanded index audits to distinguish thin-index health from compatibility
  projection health explicitly.

# litxr 0.0.8.21

- Moved `litxr_find_refs()` candidate resolution onto `ref_identity_map.fst` and
  `entity_collections.fst`.
- Added entity-first exact-key and identity expansion semantics while preserving
  the existing row-facing return shape.
- Added focused lookup regressions for canonical arXiv ids, bare arXiv ids,
  DOI-linked lookups, and collection-scoped identity selection.

# litxr 0.0.8.20

- Added release-grade migration and diagnostic helpers for the staged refactor:
  `litxr_migrate_refactor_indexes()` and `litxr_refactor_diagnostics()`.
- Added machine-facing maintenance nodes
  `scripts/migrate_refactor_indexes.R` and
  `scripts/diagnose_refactor_store.R`.
- Documented stable identity policies for citation export, prompting, and
  DOI/arXiv linking in the operator guide.

# litxr 0.0.8.19

- Moved research-schema coverage/status views onto `entity_status.fst`.
- Expanded entity status to include digest schema version, latest digest
  revision, latest paper type, and counts for findings, descriptive stats,
  anchor references, and citation logic nodes.
- Added explicit stale/orphan/revision-mismatch audits for entity status and
  wired normal research-schema writes to incremental entity-status updates.

# litxr 0.0.8.18

- Shifted ingest/update behavior further toward JSON plus thin-index-first
  writes rather than broad reference-table rewrites.
- Added an explicit collection-to-project refresh path that rebuilds project
  compatibility projections from authoritative collection state.
- Hardened damaged project reference-cache rebuilds and cleared project deltas
  after explicit refreshes.

# litxr 0.0.8.17

- Defined thinner collection/project reference projection schemas and removed
  repeated heavy payload columns from those `fst` projections.
- Added `scripts/measure_reference_projection_size.R` to quantify projection
  size reduction on real local stores.
- Established the refactor rule that rich payload stays in JSON while
  projections carry only the minimal fields needed for lookup and compatibility.

# litxr 0.0.8.16

- Finished the `0.0.8.16` read-policy migration slice from the
  `v0.1.0` refactor plan by moving the remaining human lookup/report script
  paths onto the shared identity/entity resolver.
- Extracted shared lookup normalization helpers so BibTeX-adjacent human
  scripts no longer duplicate DOI/arXiv identity-candidate logic.
- Added explicit task-policy identity helpers for citation, digest, and full-text
  resolution, and wired prompt building, summary rendering, and BibTeX-adjacent
  readers onto that shared policy layer.
- Updated the refactor design note with a versioned roadmap and marked the
  `0.0.8.16` checklist complete.

# litxr 0.0.8.15

- Added the first thin entity-index layer for the v0.1.0 refactor, including
  machine-readable identity, entity, entity-collection, and entity-status stores
  plus audit helpers for cache health and entity-status freshness.
- Moved canonical reference resolution and BibTeX export policy onto the new
  entity identity layer so linked arXiv and DOI identities now resolve to one
  preferred citation row.
- Updated DOI/arXiv linking and prompt-building logic to use entity-aware
  resolution, including better linked-arXiv source hints for digest prompts.
- Moved the human BibTeX-adjacent report scripts and `get_ref_summary.sh` onto
  the shared entity resolver and removed obsolete row-selection fallback code.

# litxr 0.0.8.14

- Added machine-readable LLM schema release metadata via
  `litxr_llm_schema_release_info()` backed by
  `inst/extdata/llm_schema_release.yaml`.
- Added link-first arXiv publication enrichment via
  `litxr_enrich_arxiv_with_doi()`, plus a human-facing helper script for
  manually linking an existing arXiv record to a published DOI record.
- Updated DOI digest prompting to prefer linked arXiv full-text hints when a
  DOI record is explicitly linked to an arXiv preprint.

# litxr 0.0.8.13

- Fixed the human-facing identification-strategy reporter so missing values
  no longer crash markdown rendering.

# litxr 0.0.8.12

- Added human-facing BibTeX/ref-id reporters for the `Methods` and
  `identification_strategy` fields in the latest research schema.

# litxr 0.0.8.11

- Made the v4 LLM digest validator backward-compatible with older stored
  digests that do not yet have `tables` or `research_target_github_links`.

# litxr 0.0.8.10

- Added a legacy exporter for cached category-query sets and now write
  `query_set.yaml` at the query-set root so the inquiry sentences are visible
  from disk and shared across embedding-model subdirectories.
- Extended schema-v4 LLM digests and prompts with `tables` for recognized
  structured table data and `research_target_github_links` for GitHub
  repositories that are the paper's research target or artifact.
- Split the interactive schema-generation workflow into a shell wrapper plus
  separate prompt-build, status-check, and JSON-ingest helpers.
- Added key, complete, JSON, and ref-id-only output modes to the category
  label reporter, plus a faster year-filter path that avoids loading the full
  collection.
- Extended `scripts/get_ref_summary.sh` to render schema-v4 research-analysis
  fields in complete mode.

# litxr 0.0.8.9

- Added a legacy exporter for cached category-query sets and now write
  `query_set.json` alongside new label-query caches so the inquiry sentences
  are visible from disk.

# litxr 0.0.8.8

- Extended schema-v4 structured extraction with `tables` for recognized paper
  tables and `research_target_github_links` for GitHub artifacts or research
  targets referenced by the paper.
- Updated the schema-v4 prompt contract and digest rendering to surface those
  richer extracted structures.

# litxr 0.0.8.7

- Replaced the interactive digest workflow with a shell wrapper plus separate
  prompt-build, status-check, and JSON-ingest nodes.
- Added support for multiple LLM digest return formats, including inline raw
  JSON and file-oriented workflows.
- Improved human-facing summary output and the machine-facing digest ingest
  flow used by the CLI wrapper.

# litxr 0.0.8.6

- Added schema-v4 LLM digest support with `ranked_contributions`,
  `likely_reader_misconceptions`, `business_relevance_pathway`, and the
  general-purpose `evidence_shape` block.
- Moved the interactive digest prompt default to schema-v4 and documented a
  fuller schema contract, including the meaning of `identification_strategy`.
- Extended LLM digest reads/search summaries to carry schema-v4 fields while
  preserving v2/v3 compatibility.

# litxr 0.0.8.5

- Added key versus complete modes to `scripts/get_ref_summary.sh`; key mode is
  now the default and reports only the most useful research-schema sections for
  quick reading.
- Fixed duplicated markdown heading prefixes in the complete
  `get_ref_summary.sh` output.
- Hardened schema-v3 citation logic node normalization so duplicate nodes with
  different tags are merged into one node with combined tags.
- Expanded the interactive digest prompt with a full citation-logic
  `logic_type` vocabulary, explicit tag-deduplication guidance, and
  `anchor_ref_id` guidance for detected arXiv ids or DOIs.
- Updated `scripts/report_arxiv_category_inquiry_set.R` to exclude arXiv ids already
  present in the blog article record log, including comma-separated grouped ids.

# litxr 0.0.8.4

- Made schema-v3 research-data handling more permissive by accepting
  narrative `sample_size` input at ingest time and normalizing it into
  `sample_size_note` on write.
- Fixed revision-history edge cases in LLM digest writes, including safer
  revision archiving when older digests have incomplete revision metadata.
- Hardened schema-v3 inline `anchor_references` and `citation_logic_nodes`
  ingestion so partial or identity-shaped ChatGPT payloads are preserved instead
  of collapsing into placeholder `unknown` rows.
- Improved `scripts/build_llm_digest_interactive.R` with explicit field-level
  examples for anchor references and citation logic nodes, and updated
  `scripts/get_ref_summary.sh` to render those richer inline fields.
- Made empirical-field validator warnings more paper-type-sensitive so case
  studies and qualitative papers no longer warn about low-value missing fields
  such as `identification_strategy`.

# litxr 0.0.8.3

- Normalized schema-v3 inline `anchor_references` and
  `citation_logic_nodes` into rowwise digest fields on write and read, while
  still supporting the earlier transposed legacy shape during revision writes.
- Updated `scripts/get_ref_summary.sh` so the digest summary output now renders
  the inline v3 anchor and citation-logic blocks.
- Kept the project-level anchor and citation-logic tables unchanged as the
  searchable, compactable canonical storage layer.

# litxr 0.0.8.2

- Wired the project-level `anchor_references` and `citation_logic_nodes`
  tables into optional schema-v3 inline digest fields.
- Updated `scripts/build_llm_digest_interactive.R` to prompt for schema-v3 and
  explicitly request inline anchor and citation-logic blocks.
- Kept the standalone project-level anchor and citation-logic tables separate
  from the inline digest shape so they remain searchable and compactable.

# litxr 0.0.8.1

- Added revision-aware LLM digest writes with `digest_revision`,
  `derived_from_revision`, `extraction_mode`, `prompt_version`, `model_hint`,
  and `updated_at` metadata on schema-v2 digests.
- Added append-only digest history storage under `project.data_root/llm_history/`
  plus the helpers `litxr_list_llm_digest_revisions()` and
  `litxr_read_llm_digest_history()`.
- Updated `scripts/build_llm_digest_interactive.R` to support `create` versus
  `revise` modes and to stamp ingested digests with manual ChatGPT extraction
  metadata.

# litxr 0.0.8

- Tightened `scripts/write_bib_by_ref_ids.sh` so create mode now stops if the
  target `.bib` file already exists and `--append` was not requested.
- Added `docs/llm-digest-revision-design.md` to document a future revision-aware
  digest workflow with explicit `schema_version` versus `digest_revision`
  semantics and append-only digest history.

# litxr 0.0.7.5

- Changed BibTeX formatting so the `title` field is written on the same first
  line as the BibTeX entry key, which makes outline-style browsing easier in
  editors such as TextMate.
- Fixed `litxr_find_refs(ref_id = ...)` to handle vector `ref_id` inputs
  without scalar-coercion warnings.
- Tightened `scripts/write_bib_by_ref_ids.sh` so it keeps CLI startup light,
  validates multi-id local lookups cleanly, and only prefers DOI-backed local
  metadata over arXiv records when the DOI-backed record is actually richer.
- Added `docs/doi-enrichment-design.md` to document the current DOI-enrichment
  limitation and a future package-level design for linked arXiv / DOI records.

# litxr 0.0.7.4

- Replaced the old builder-file digest workflow scripts with a single
  interactive CLI, `scripts/build_llm_digest_interactive.R`, for schema-v2
  ChatGPT handoff and local JSON ingestion.
- Added `scripts/get_ref_summary.sh` to print a local abstract plus any
  existing research schema in markdown-style form.
- Added `scripts/cache_arxiv_category_inquiry_set.R` to build persistent local inquiry
  embedding caches from YAML files.
- Added `scripts/write_bib_by_ref_ids.sh` for ref-id-based BibTeX file creation
  and append/overwrite workflows.
- Removed the obsolete scripts `scripts/build_llm_digests.R`,
  `scripts/example_digest_builder.R`, and `scripts/get_arxiv_abstract.sh`.
- Made single-record markdown and digest writes faster by avoiding a full
  canonical-reference read and using an incremental enrichment-status update.
- Fixed the interactive digest CLI to copy the prompt with `pbcopy`, verify the
  downloaded JSON `ref_id`, and use a proper stdin read path.

# litxr 0.0.7.3

- Renamed the category-label reporting script to
  `scripts/report_arxiv_category_inquiry_set.R` and fixed its per-category filtering
  and reporting order.
- Made the category-label reporting script reject unknown CLI flags, use cached
  local inquiry embeddings by default, and support `--local-inq` plus
  one-off YAML inquiry files via `--inquiry`.
- Made corpus embedding repair in the category-label reporting workflow opt-in
  through `--embed-missing` instead of always running first.
- Added `inst/extdata/example_category_inquiry.yaml` as a minimal YAML example
  for one-off category inquiry input.
- Tightened other script CLIs so unsupported flags now fail explicitly instead
  of being ignored.

# litxr 0.0.7.2

- Added project-level research-schema coverage helpers:
  `litxr_read_research_schema_status()`,
  `litxr_find_refs_missing_llm_digest()`,
  `litxr_find_refs_missing_standardized_findings()`, and
  `litxr_find_refs_missing_descriptive_stats()`.
- Added explicit rebuild helpers for the project-level research tables:
  `litxr_rebuild_standardized_findings()` and
  `litxr_rebuild_descriptive_stats()`.
- Added `litxr_upgrade_llm_digests()` to rewrite legacy digest JSON through the
  current schema-v2 write path.

# litxr 0.0.7.1

- Changed collection-local folder naming to `ref_json/`, `fulltxt_md/`, and
  `llm_json/`, with backward-compatible fallback reads from legacy `json/`.
- Updated config messaging, tests, and storage docs to reflect the renamed
  collection-local folders and the non-durable role of raw PDF / HTML files.
- Applied the on-disk folder rename to the existing local data root.
- Kept project-level `index/`, `md/`, and `llm/` paths unchanged.

# litxr 0.0.7

- Added controlled paper-type helpers:
  `litxr_paper_type_levels()`,
  `litxr_validate_paper_type()`, and
  `litxr_normalize_paper_type()`.
- Extended the LLM digest layer with a schema-versioned v2 template while
  keeping legacy digest files readable and valid.
- Added project-level research-analysis tables under `project.data_root/findings/`:
  standardized findings and descriptive statistics, each with template,
  validation, read, write, find, and compact helpers.
- Added `docs/research-schema.md` and updated README plus enrichment/storage
  docs for the new research-analysis layers.
- Removed outdated remaining docs references to `scripts/repair_arxiv.R`.

# litxr 0.0.6.5

- Added `scripts/compact_arxiv_embedding_delta.R` as a small CLI wrapper for
  compacting embedding delta into the main cache with before/after coverage
  reporting.
- Added `date_from`, `date_to`, and `chunk_size` support to
  `litxr_score_collection_categories()`, with date filtering before scoring and
  chunked processing for large corpora.
- Improved `litxr_label_collection_by_category()` by replacing the previous
  split/lapply path with a more efficient grouped implementation.

# litxr 0.0.6.4

- Added `--year-from` and `--year-to` filters to
  `scripts/label_arxiv_categories.R`.
- Normalized legacy embedding metadata on read so old wide embedding caches now
  expose the current minimal schema (`ref_id`, `title`, `year`).
- Removed the redundant `scripts/repair_arxiv.R` wrapper.
- Improved local sample scripts:
  `scripts/get_arxiv_abstract.sh` now validates arXiv ids and errors clearly on
  zero/multiple/missing-abstract matches, and
  `scripts/sync_arxiv_abstract_embedding.R` now uses CLI args plus delta-only embedding
  with before/after coverage output.

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
- Added `write_bibtex_entries()` for BibTeX export from canonical `ref_id`
  values, plus a thin shell wrapper for command-line use.
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

- Added project initialization with shared `config.yaml` support under
  `LITXR_DATA_ROOT`.
- Added journal-centric local storage and BibTeX export helpers.
- Added arXiv and Crossref parsing helpers for a unified reference schema.
- Added cursor-paginated Crossref journal sync support.
- Added README guidance, package documentation, and initial test coverage.
