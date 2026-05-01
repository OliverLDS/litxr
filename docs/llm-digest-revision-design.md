# LLM Digest Revision Design

This note describes a future design for revision-aware LLM digests in `litxr`.

It is based on the current practical workflow:

1. a local CLI script prepares a prompt for one `ref_id`
2. a human pastes that prompt into ChatGPT
3. ChatGPT returns a structured JSON digest
4. the JSON is downloaded locally
5. `litxr` validates and stores the digest

The current package already supports the human-in-the-loop extraction model
through:

- `scripts/build_llm_digest_interactive.R`

This design note extends that workflow so the same paper can be extracted
multiple times over time, with explicit revision tracking.

## Why Revision Support Is Needed

Current digest storage is effectively "latest JSON wins".

That is too weak for the real extraction workflow because:

- schema versions will evolve
- prompts will evolve
- ChatGPT extraction quality will improve
- users may want to refine earlier digests
- the same paper may need multiple passes before the digest is satisfactory

So `litxr` should treat a digest as an evolving local data object rather than a
single final artifact.

## Current Limitation

Today, `litxr` tracks:

- `schema_version`

but it does not track:

- digest revision number
- prompt version
- extraction workflow mode
- model hint or extractor provenance
- parent-child relationship between revisions
- retained history of prior digest versions

That means a later improvement pass is not first-class. It is just a blind
overwrite.

## Core Design Principle

Separate:

- **schema version**
- **digest revision**

These are different concepts.

### `schema_version`

Describes the structure of the JSON.

Examples:

- `v2`
- `v3`

### `digest_revision`

Describes the iteration count for the same paper under a given workflow.

Examples:

- first extraction of the paper
- later refinement after better prompt design
- later refinement after better model performance
- later correction after human review

## Proposed Metadata Fields

Future digests should include metadata such as:

```json
{
  "schema_version": "v3",
  "digest_revision": 3,
  "derived_from_revision": 2,
  "extraction_mode": "chatgpt_manual",
  "prompt_version": "v3.1",
  "model_hint": "gpt-5",
  "generated_at": "2026-05-01T00:00:00Z",
  "updated_at": "2026-05-01T00:00:00Z"
}
```

### Field meanings

- `schema_version`
  - the structural schema of the digest
- `digest_revision`
  - current revision number for this `ref_id`
- `derived_from_revision`
  - optional parent revision number
- `extraction_mode`
  - how this digest was produced
- `prompt_version`
  - version of the extraction prompt template
- `model_hint`
  - optional note about the external model used
- `generated_at`
  - creation timestamp
- `updated_at`
  - last write timestamp

## Storage Design

### Keep one current digest

Continue to support a fast current-digest path:

```text
project.data_root/llm/<slug>.json
```

This remains the default read target for:

- `litxr_read_llm_digest()`
- `litxr_find_llm()`
- status helpers

### Add digest history

Add a parallel history store:

```text
project.data_root/llm_history/<ref_id>/<timestamp>__rev003__v3.json
```

Example:

```text
project.data_root/llm_history/arxiv_2505_07087/2026-05-01T00-00-00Z__rev003__v3.json
```

This gives:

- a stable current digest for normal workflows
- an append-only historical record for audit and revision tracking

## Recommended Write Semantics

Future `litxr_write_llm_digest()` behavior should support:

1. write current digest
2. optionally archive prior current digest into history
3. record revision metadata
4. keep backward compatibility with old digest files

Possible API direction:

```r
litxr_write_llm_digest(
  ref_id,
  digest,
  config = NULL,
  keep_history = TRUE,
  bump_revision = TRUE
)
```

### Suggested behavior

- if no current digest exists:
  - write revision `1`
- if current digest exists and `bump_revision = TRUE`:
  - archive old current digest
  - increment revision
  - write new current digest
- if current digest exists and `bump_revision = FALSE`:
  - allow metadata-preserving overwrite only if explicitly requested

## Read Helpers

Future helper ideas:

- `litxr_read_llm_digest_history(ref_id, config = NULL)`
- `litxr_list_llm_digest_revisions(ref_id, config = NULL)`
- `litxr_diff_llm_digests(ref_id, revision_a, revision_b, config = NULL)`
- `litxr_promote_llm_digest_revision(ref_id, revision, config = NULL)`

These helpers would let users:

- inspect prior revisions
- compare revisions
- restore an earlier digest as current

## CLI Evolution Plan

The current manual extraction CLI is:

- `scripts/build_llm_digest_interactive.R`

It should eventually become revision-aware.

### Current mode

Current mode is effectively:

- create or replace one digest from a downloaded JSON file

### Future mode split

The script should support:

- `create`
- `revise`

#### `create`

Used when:

- no digest exists
- or the user wants a fresh start

#### `revise`

Used when:

- a digest already exists
- the user wants ChatGPT to improve the existing digest

In `revise` mode, the prompt should include:

- current digest JSON
- instruction to preserve correct fields
- instruction to improve incomplete or weak fields
- instruction to correct mistakes instead of rewriting blindly

## Prompt Versioning

The prompt itself should become versioned.

Example CLI direction:

```sh
Rscript scripts/build_llm_digest_interactive.R \
  --ref-id arxiv:2505.07087 \
  --schema-version v3 \
  --prompt-version v3.1 \
  --mode revise
```

### Why prompt versioning matters

Because digest quality depends on:

- prompt shape
- schema example quality
- extraction instructions

So revision history should record not only what changed, but under which prompt
template it changed.

## Why Granular Extraction Is Acceptable Here

For package-side automatic inference, a very granular schema can be brittle.

That concern is weaker under the current `litxr` workflow because:

- extraction is delegated to ChatGPT
- the prompt can provide an explicit JSON example
- the output is validated locally
- users can rerun extraction as prompts improve
- later revisions can improve earlier weak outputs

So a richer schema is acceptable when:

- the package does not pretend extraction is deterministic
- digest evolution is first-class
- revision history is retained

## Backward Compatibility

Revision-aware design should remain compatible with current digests.

Rules:

1. old digests without `digest_revision` should still read
2. missing revision metadata should be inferred conservatively
3. current digest location should remain supported
4. history storage should be additive, not required for old digests

### Conservative inference for legacy digests

For an old digest:

- `digest_revision = 1`
- `derived_from_revision = null`
- `extraction_mode = "legacy"`
- `prompt_version = null`

This lets older files remain valid without forced rewrite.

## Suggested Phased Implementation

### Phase 1

- add revision metadata fields
- update digest template
- make write path optionally archive prior digest

### Phase 2

- add history read/list helpers
- add current vs history path helpers

### Phase 3

- update `scripts/build_llm_digest_interactive.R`
- support `create` and `revise`
- support prompt version metadata

### Phase 4

- add diff and restore helpers
- optionally expose digest-history coverage in project-level status

## Non-Goals

This design should not:

- host essay-writing agents inside `litxr`
- turn `litxr` into an agent runtime
- replace local JSON storage with heavy database machinery
- introduce R6 reference objects
- require Shiny or UI layers

## Summary

The right future model for LLM digests in `litxr` is:

- one current digest per `ref_id`
- optional append-only digest history
- explicit separation between `schema_version` and `digest_revision`
- revision-aware ChatGPT extraction prompts

In short:

> `litxr` should treat digests as evolving local research data, not one-shot
> summaries.
