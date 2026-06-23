# litxr

`litxr` is a lightweight R package for managing literature as local data.

It is built around:

- collection-level sync from sources such as Crossref and arXiv
- thin project-level alias/entity indexes plus compatibility reference projections
- BibTeX export for writing workflows such as Quarto
- project-level markdown and structured LLM digests keyed by `ref_id`

## Docs

Focused guides live under [`docs/`](docs):

- [Configuration](docs/configuration.md)
- [Sync And Storage](docs/sync-and-storage.md)
- [Enrichment And Search](docs/enrichment-and-search.md)
- [Research Schema](docs/research-schema.md)

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
- `litxr_next_arxiv_repair_range()`
- `litxr_add_dois()`
- `litxr_add_refs()`

Read and export:

- `litxr_read_collection()`
- `litxr_refresh_collection_index()`
- `litxr_compact_collection_index()`
- `litxr_collection_date_stats()`
- `litxr_build_embedding_index()`
- `litxr_search_embeddings()`
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

- `ref_json/`
- `index/`
- `fulltxt_md/`
- `llm_json/`

Project-level canonical data lives under `project.data_root/`, including:

- `index/ref_aliases.fst`
- `index/entities.fst`
- `index/entity_collections.fst`
- `index/entity_status.fst`
- `index/references.fst`
- `index/reference_collections.fst`
- `index/enrichment_status.fst`
- `index/sync_state.fst`
- `embeddings/`
- `md/`
- `llm/`

## Scripts

Included shell helpers:

- `scripts/repair_arxiv_range.R`
- `scripts/repair_arxiv_latest.R`
- `scripts/human/build_llm_digest_interactive.sh`
- `scripts/human/get_ref_summary.sh`
- `scripts/report_arxiv_category_labels.R`
- `scripts/cache_category_inquiries.R`
- `scripts/write_bib_by_ref_ids.R`

The arXiv repair scripts now prefer `--collection-id`; `--journal-id` still
works as a compatibility alias.
Range repair writes fetched pages to a small delta index during the run and
compacts that delta into the full collection index once at the end by default.
`scripts/human/get_ref_summary.sh` prints a key research-schema summary by default;
use `--complete` for the full digest rendering.
The interactive digest prompt is built by `litxr_llm_digest_prompt()` from
package fragments under `inst/prompts/`. New interactive prompts use schema
`v4`, which adds ranked contributions, reader-misconception checks, business
relevance pathways, and a general `evidence_shape` field.

The v0.1.0 refactor makes `entity_id` the internal identity key while keeping
`ref_id` as the surface-facing alias used by users and scripts. Rich payload
stays in JSON; `fst` is used for thin indexes, compatibility projections, and
status.

The 0.1.7 release makes compatibility projections runtime-only over the
normalized authoritative stores. The 0.1.6 release makes normal embedding search shard-aware and streaming,
with partial top-k selection for category aggregation. The 0.1.5 release makes
project lookup and hydration keyed and batch-oriented, including narrow first
reads and runtime wide-projection guardrails. The 0.1.4
release removes the live alias-table dependency from the entity builder/read
path, keeps entity-status refresh incremental, and preserves the keyed upsert
rewrite from 0.1.3. The 0.1.2.1
release adds narrow-first project lookup so wide projection materialization
happens only after canonical filtering, while the 0.1.2 release adds the
normalized `ref_entities` / `ref_arxiv` / `ref_doi` scaffold and narrow
append-shard handling for expensive derived artifacts such as embeddings and
research-schema digests.

## Status

Implemented today:

- collection-first config
- Crossref and arXiv sync
- DOI batch ingest
- manual reference ingest
- canonical project-level indexes
- BibTeX export
- markdown and structured digest storage

## Research Analysis Schemas

`litxr` now includes local research-analysis layers keyed by canonical `ref_id`:

- a controlled `paper_type` vocabulary
- schema-versioned LLM digests
- project-level standardized findings tables
- project-level descriptive statistics tables
- project-level coverage helpers for missing digests, findings, and descriptive statistics

These layers stay within the existing local-data architecture: JSON digests
under `project.data_root/llm/`, revision history under
`project.data_root/llm_history/`, and `fst` main-plus-delta tables under
`project.data_root/findings/`. External AI agents can read and write these
stores, but `litxr` does not host essay-writing or meta-analysis agents inside
the package.

Still future work:

- full PDF download
- HTML-to-Markdown conversion
- richer automated incremental sync policies
