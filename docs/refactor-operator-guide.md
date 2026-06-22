# Refactor Operator Guide

This note documents the stable operator-facing behavior during the staged
`v0.1.0` refactor.

## Alias Policies

`litxr` now resolves `ref_id` inputs through the alias/entity layer before
choosing a task-specific working alias.

### Citation export

For citation-oriented tasks such as `litxr_export_bib()` and
`scripts/write_bib_by_ref_ids.R`:

- prefer `preferred_citation_ref_id`
- if a linked DOI-backed published alias exists, prefer that row
- otherwise fall back to the primary alias

Practical effect:

- a linked arXiv paper will usually export the richer DOI-backed published
  record
- users can still request the arXiv id at the CLI surface

### Digest and full-text prompting

For digest/full-text tasks such as `litxr_llm_digest_prompt()` and
`scripts/build_llm_digest_prompt.R`:

- start from the requested `ref_id`
- if a linked arXiv alias exists, prefer that alias for full-text discovery
  hints
- preserve the requested `ref_id` as the canonical digest key

Practical effect:

- DOI requests can still benefit from arXiv HTML/PDF hints when a linked arXiv
  alias exists
- digest files remain keyed by canonical `ref_id`

### arXiv and DOI linking

For linking flows such as `litxr_enrich_arxiv_with_doi()` and
`scripts/human/enrich_arxiv_with_doi.R`:

- do not rewrite canonical surface `ref_id` values
- write `linked_doi_ref_id` onto the arXiv row
- write `linked_arxiv_ref_id` onto the DOI row
- let entity indexes express equivalence and task preference

Practical effect:

- old scripts can keep using the same `ref_id`
- citation tasks can still prefer the published alias

### Collection membership

Collection membership remains an alias-facing concept at the surface, but
internally is summarized at the entity layer:

- one entity may belong to multiple collections
- `litxr_read_reference_collections()` remains `ref_id`-facing
- `litxr_read_entity_collections()` is the thin entity-level membership view

## Migration Helpers

For mixed or older stores, use:

```r
litxr_migrate_refactor_indexes()
```

This helper:

- optionally rebuilds selected collection projection indexes from `ref_json/`
- refreshes project projection caches from collection state
- rebuilds `ref_aliases.fst`, `entities.fst`, `entity_collections.fst`, and
  `entity_status.fst`

Machine-facing CLI wrapper:

```sh
Rscript scripts/migrate_refactor_indexes.R
```

Useful options:

- `--collection-id ID1,ID2,...`
- `--no-rebuild-collection-indexes`

## Diagnostics

For a single bundled maintenance report, use:

```r
litxr_refactor_diagnostics()
```

This bundles:

- reference-cache audit
- entity-index audit
- entity-status audit
- one-row summary of mismatch counts

Machine-facing CLI wrapper:

```sh
Rscript scripts/diagnose_refactor_store.R
```

Low-level helpers remain available when you need more detail:

- `litxr_audit_reference_cache_state()`
- `litxr_audit_entity_indexes()`
- `litxr_audit_entity_status_state()`

## Recommended Maintenance Order

When a local store looks mixed, stale, or partially migrated:

1. Run `litxr_refactor_diagnostics()`.
2. If collection or project projection caches are stale, run
   `litxr_migrate_refactor_indexes()`.
3. Re-run `litxr_refactor_diagnostics()` to confirm remaining issues.
4. Only inspect individual low-level tables if the bundled report still shows
   problems.
