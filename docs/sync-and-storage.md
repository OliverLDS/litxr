# Sync And Storage

`litxr` separates collection-local storage from project-level canonical indexes.

## Collection-Local Storage

Each collection uses its `local_path` with these folders:

- `json/`: one normalized metadata JSON per reference
- `index/`: fast collection-level `fst` indexes
- `pdf/`: reserved for downloaded PDFs
- `md/`: reserved for collection-local markdown derivatives
- `llm/`: reserved for collection-local LLM outputs when needed

Today, `json/` and `index/` are active. `pdf/` download and HTML-to-Markdown
conversion are still future work.

## Project-Level Storage

Under `project.data_root/`:

- `index/references.fst`: canonical reference table
- `index/reference_collections.fst`: reference-to-collection memberships
- `index/enrichment_status.fst`: markdown/digest coverage
- `index/sync_state.fst`: sync and repair history
- `llm/`: project-level structured digests keyed by `ref_id`
- `md/`: project-level markdown keyed by `ref_id`

## Sync APIs

Main collection-level functions:

- `litxr_sync_collection()`
- `litxr_sync_all()`
- `litxr_repair_collection()`
- `litxr_read_collection()`
- `litxr_collection_date_stats()`
- `litxr_rebuild_collection_index()`

Backward-compatible journal wrappers still exist:

- `litxr_sync_journal()`
- `litxr_repair_journal()`
- `litxr_read_journal()`
- `litxr_rebuild_journal_index()`

## Crossref

Crossref sync for journal-type collections is ISSN-based and cursor-paginated.

Recommended model:

- one full collection harvest from Crossref for the registered journal
- parse DOI and metadata from the same payload
- upsert into local storage
- export BibTeX from the local store

Crossref sync history is recorded in `sync_state.fst`, but automatic incremental
`from-update-date` sync is not implemented yet.

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
- `scripts/repair_arxiv.R`
- `scripts/repair_arxiv_range.R`

`scripts/repair_arxiv_range.R` now records successful day-level repair windows
in `sync_state.fst` and skips already completed days unless `--force` is used.

For local coverage diagnostics, use:

```r
stats_day <- litxr_collection_date_stats("arxiv_cs_ai", cfg, by = "day")
attr(stats_day, "date_min")
attr(stats_day, "date_max")
attr(stats_day, "missing_dates")
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

## BibTeX Export

Use:

- `litxr_export_bib(output, journal_ids = ..., config = ...)`
- `litxr_export_bib(output, keys = ..., config = ...)`

The `journal_ids` argument name is kept for backward compatibility, but it
accepts collection ids.

`keys` can match:

- `doi`
- `ref_id`
- `source_id`

Missing keys now produce a warning instead of being ignored silently.
