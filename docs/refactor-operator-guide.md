# Refactor Operator Guide

This note documents the stable operator-facing behavior after the completed
`v0.1.0` identity-first refactor.

## Identity Policies

`litxr` now resolves `ref_id` inputs through the identity/entity layer before
choosing a task-specific working identity.

### Citation export

For citation-oriented tasks such as `write_bibtex_entries()` and
`scripts/write_bib_by_ref_ids.sh`:

- pass canonical `ref_id` values directly
- if a linked DOI-backed published identity exists for an arXiv id, prefer that
  DOI-backed record during BibTeX export
- otherwise fall back to the requested arXiv identity

### Digest and full-text prompting

For digest/full-text tasks such as `litxr_llm_digest_prompt()` and
`scripts/build_llm_digest_prompt.R`:

- start from the requested `ref_id`
- if a linked arXiv identity exists, prefer that identity for full-text discovery
  hints
- preserve the requested `ref_id` as the canonical digest key

Practical effect:

- DOI requests can still benefit from arXiv HTML/PDF hints when a linked arXiv
  identity exists
- digest files remain keyed by canonical `ref_id`

### arXiv and DOI linking

For linking flows such as `litxr_add_ref_identity_pair()` and
`scripts/add_ref_identity_pair.sh`:

- do not rewrite canonical surface `ref_id` values
- append only the arXiv/DOI pair to `ref_identity_map.fst`
- reject any pair whose arXiv id or DOI already exists in the identity map
- keep the script node strict and human-facing

Practical effect:

- identity linking stays a narrow surface action
- downstream tasks can resolve the published identity from the identity map

### Collection membership

Collection membership remains an identity-facing concept at the surface, but
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
- rebuilds `ref_identity_map.fst`, `entities.fst`, `entity_collections.fst`, and
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
