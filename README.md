# litxr

`litxr` is a lightweight R package for managing literature as local data.

It is built around:

- collection-level sync from sources such as Crossref and arXiv
- a canonical project-level reference store
- BibTeX export for writing workflows such as Quarto
- project-level markdown and structured LLM digests keyed by `ref_id`

## Docs

Focused guides live under [`docs/`](docs):

- [Configuration](docs/configuration.md)
- [Sync And Storage](docs/sync-and-storage.md)
- [Enrichment And Search](docs/enrichment-and-search.md)

## Quick Start

Set `LITXR_CONFIG` in `.Renviron` and restart R:

```sh
LITXR_CONFIG=/absolute/path/to/project/config.yaml
```

Then initialize and inspect the project config:

```r
library(litxr)

litxr_init()
cfg <- litxr_read_config()
litxr_list_collections(cfg)
```

Sync one collection and export BibTeX:

```r
records <- litxr_sync_collection("journal_of_finance", cfg)
litxr_export_bib("references.bib", journal_ids = "journal_of_finance", config = cfg)
```

Read from the canonical project-level store:

```r
refs <- litxr_read_references(cfg)
litxr_find_refs(query = "asset pricing", config = cfg)
```

## Main APIs

Configuration:

- `litxr_init()`
- `litxr_read_config()`
- `litxr_list_collections()`

Sync and ingest:

- `litxr_sync_collection()`
- `litxr_sync_all()`
- `litxr_repair_collection()`
- `litxr_add_dois()`
- `litxr_add_refs()`

Read and export:

- `litxr_read_collection()`
- `litxr_refresh_collection_index()`
- `litxr_collection_date_stats()`
- `litxr_read_references()`
- `litxr_find_refs()`
- `litxr_export_bib()`

Enrichment:

- `litxr_write_md()`
- `litxr_build_llm_digest()`
- `litxr_build_llm_digests()`
- `litxr_read_enrichment_status()`
- `litxr_list_enrichment_candidates()`

## Project Layout

Collection-local storage uses:

- `json/`
- `index/`
- `pdf/`
- `md/`
- `llm/`

Project-level canonical data lives under `project.data_root/`, including:

- `index/references.fst`
- `index/reference_collections.fst`
- `index/enrichment_status.fst`
- `index/sync_state.fst`
- `md/`
- `llm/`

## Scripts

Included shell helpers:

- `scripts/repair_arxiv.R`
- `scripts/repair_arxiv_range.R`
- `scripts/build_llm_digests.R`
- `scripts/example_digest_builder.R`

The arXiv repair scripts now prefer `--collection-id`; `--journal-id` still
works as a compatibility alias.

## Status

Implemented today:

- collection-first config
- Crossref and arXiv sync
- DOI batch ingest
- manual reference ingest
- canonical project-level indexes
- BibTeX export
- markdown and structured digest storage

Still future work:

- full PDF download
- HTML-to-Markdown conversion
- richer automated incremental sync policies
