# Sync And Storage

`litxr` separates collection-local storage from project-level canonical indexes.

## Collection-Local Storage

Each collection uses its `local_path` with these folders:

- `ref_json/`: one normalized metadata JSON per reference from the reference source
- `index/`: fast collection-level `fst` indexes
- `fulltxt_md/`: full article text converted to markdown
- `llm_json/`: collection-local LLM JSON outputs when needed

Today, `ref_json/` and `index/` are the core local-storage layers. Full-text
markdown can live under `fulltxt_md/`. Raw PDF or HTML files are not treated as
durable collection-local storage.

## Project-Level Storage

Under `project.data_root/`:

- `ref_arxiv.fst`: authoritative arXiv payload table
- `ref_doi.fst`: authoritative DOI payload table
- `ref_local_pending.fst`: unresolved local bibliographic input
- `index/ref_identity_map.fst`: narrow identity routing output used only when a
  runtime identity view is needed
- `log/<collection>_collection_fetch_history.tsv`: collection-level fetch history
- `embeddings/`: cached embedding metadata, matrices, and manifests
- `llm/`: project-level structured digests keyed by `ref_id`
- `md/`: project-level markdown keyed by `ref_id`

The authoritative bibliographic stores are `ref_arxiv.fst`, `ref_doi.fst`, and
`ref_local_pending.fst`. Any wide compatibility view over entities or
collections is runtime-only and should not be treated as a durable project file.
The old `references.fst`, `reference_collections.fst`, `entities.fst`,
`entity_collections.fst`, and `enrichment_status.fst` durable projection files
are not part of the target storage model.

## Sync APIs

Main collection-level functions:

- `litxr_sync_collection()`
- `litxr_sync_all()`
- `litxr_repair_collection()`
- `litxr_read_collection()`
- `litxr_collection_date_stats()`

Journal convenience wrappers still exist:

- `litxr_sync_journal()`
- `litxr_repair_journal()`
- `litxr_read_journal()`

## Crossref

Crossref sync for journal-type collections is ISSN-based and cursor-paginated.

Recommended model:

- one full collection harvest from Crossref for the registered journal
- parse DOI and metadata from the same payload
- upsert into local storage
- export BibTeX from the local store

Crossref sync history is recorded in `log/doi_collection_fetch_latest.tsv`.
Incremental DOI sync uses the latest-update log as its cutoff when present.

## arXiv

arXiv sync is query-based.

Typical config:

```yaml
sync:
  search_query: cat:cs.AI
  rows: 100
  delay_seconds: 3
```

Practical notes:

- arXiv rate limits matter
- large categories should use bounded repair windows
- local upsert makes reruns idempotent

Helpers:

- `litxr_repair_collection(..., submitted_from = ..., submitted_to = ...)`
- `scripts/fetch_arxiv_by_collection.R`

`scripts/fetch_arxiv_by_collection.R` records successful day-level fetches in
`log/<collection>_collection_fetch_history.tsv` and skips already completed days
unless `--force` is used. It writes fetched pages directly into the collection
JSON store. There is no durable `references_delta.fst` or project-wide
compatibility projection in the target model.

Use `litxr_sync_thin_ref_stores_from_json()` when the thin project stores need
to be rebuilt or incrementally updated from the normalized collection JSON
store.

For local coverage diagnostics, use:

```r
stats_day <- litxr_collection_date_stats("arxiv_cs_ai", cfg, by = "day")
attr(stats_day, "date_min")
attr(stats_day, "date_max")
attr(stats_day, "missing_dates")
```

To plan the next arXiv sync window through today, use the fetch history log:

```r
litxr:::.litxr_latest_collection_fetch_completed_date(cfg, "arxiv_cs_ai")
```

For shell-driven updates from the next collection-index date through today:

```sh
Rscript scripts/fetch_arxiv_by_collection.R --collection arxiv_cs_ai
```

## DOI And Manual Ingest

Batch DOI ingest:

```r
litxr_add_dois(c("10.1111/jofi.12921"), cfg)
```

Manual reference ingest:

```r
refs <- data.frame(
  source = "book",
  entry_type = "book",
  title = "Example Book",
  authors = "Jane Doe",
  year = 2024,
  isbn = "9780262046305"
)

litxr_add_refs(refs, collection_id = "manual_books", config = cfg)
```

Known limitation:

- arXiv sync stores DOI strings when present, but it does not yet run a
  second DOI-enrichment pass to fetch richer publication metadata. See
  [doi-enrichment-design.md](./doi-enrichment-design.md).

## BibTeX Export

Use:

- `write_bibtex_entries(output, ref_ids = ..., config = ...)`
- `scripts/write_bib_by_ref_ids.sh`

The BibTeX writer now accepts canonical `ref_id` values directly. Use the
`scripts/read_bibtex_entries.sh` helper to extract canonical ids from a `.bib`
file when needed.

## Stable Identity Policies

During the staged `v0.1.0` refactor, operator-facing behavior is:

- citation export prefers the published DOI-backed identity when one is linked
- digest/full-text prompting may prefer a linked arXiv identity for source hints
- DOI/arXiv linking preserves existing surface `ref_id` values and records the
  equivalence through the narrow identity routing outputs

See [refactor-operator-guide.md](./refactor-operator-guide.md) for the stable
policy details.

## Migration And Diagnostics

For mixed or older stores:

- `litxr_migrate_refactor_indexes()`
- `Rscript scripts/migrate_refactor_indexes.R`

For bundled diagnostics:

- `litxr_refactor_diagnostics()`
- `Rscript scripts/diagnose_refactor_store.R`

Low-level audits remain available:

- `litxr_audit_reference_cache_state()`
- `litxr_audit_entity_indexes()`
- `litxr_audit_entity_status_state()`
