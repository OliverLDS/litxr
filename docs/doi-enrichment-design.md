# DOI Enrichment Design

This note records a known limitation in current `litxr` reference ingest and a
concrete plan for a future improvement.

## Current Issue

Today, an arXiv-synced reference can carry a non-empty `doi` field while still
remaining an arXiv-shaped local record.

That means the local row may still look like:

- `source = "arxiv"`
- `url = https://arxiv.org/...`
- missing `journal`, `container_title`, `publisher`, `volume`, `issue`, `pages`

even when the DOI resolves to richer publication metadata.

Example failure mode:

- arXiv provides `doi = 10.5281/zenodo.18008872`
- `litxr` stores that DOI string on the arXiv row
- but `litxr` does not automatically fetch DOI-backed metadata
- BibTeX export then has a DOI but still uses mostly arXiv metadata

So the current limitation is not that `litxr` intentionally discards DOI
metadata when arXiv ids are present. The limitation is that arXiv sync stops
after parsing the arXiv feed and does not run a second DOI-enrichment step.

## Why This Matters

This affects practical output quality for:

- BibTeX export
- venue-aware filtering
- journal / conference attribution
- downstream research-schema work that relies on publication context

It matters especially for arXiv papers that later receive a journal,
conference, or repository DOI with richer metadata than the original arXiv
entry.

## Current Behavior

Current ingest paths:

- arXiv sync:
  - parses arXiv XML
  - stores arXiv metadata plus DOI string if present
  - does not follow the DOI automatically
- DOI ingest:
  - `litxr_add_dois()` currently uses Crossref-oriented DOI fetch
  - this is not yet a general DOI-registry enrichment layer

So a DOI-bearing arXiv paper only becomes a richer DOI-backed local record if a
separate DOI ingest step is run and succeeds.

## Design Goal

Add an optional DOI-enrichment step that can improve local metadata quality for
DOI-bearing references without breaking the current `ref_id`-centric storage
model.

The goal is not to replace arXiv sync. The goal is to let arXiv sync remain the
source of discovery while DOI enrichment becomes a second metadata-improvement
pass.

## Proposed Approach

### 1. Keep arXiv sync as discovery

Do not change the core arXiv repair / sync workflow.

It should continue to:

- fetch by arXiv search query
- parse arXiv metadata
- store canonical arXiv rows keyed by `ref_id`

### 2. Add optional DOI-enrichment pass

New future helper idea:

- `litxr_enrich_doi_metadata(...)`

Possible interface:

```r
litxr_enrich_doi_metadata(
  ref_ids = NULL,
  collection_id = NULL,
  config = NULL,
  overwrite_fields = FALSE,
  prefer_published = TRUE
)
```

Selection modes:

- explicit `ref_ids`
- all DOI-bearing refs in a collection
- all DOI-bearing refs in the project index

### 3. Fetch DOI-backed metadata explicitly

The enrichment step should fetch metadata from DOI-oriented sources instead of
assuming arXiv metadata is sufficient.

Planned priority:

1. Crossref when DOI is available there
2. other DOI registries if needed in future versions

This keeps the first implementation narrow while allowing later expansion.

### 4. Store DOI-backed rows separately

Preferred design:

- keep the original arXiv row
- add or update a separate DOI-backed canonical row such as `doi:...`
- preserve both records in the project index

Rationale:

- avoids silently mutating source provenance
- keeps `ref_id` canonical and stable
- lets users choose whether they want arXiv or DOI-backed export
- makes debugging easier

This is cleaner than overwriting the arXiv row in place.

### 5. Add a lightweight relationship layer

Future project-level link table idea:

- `index/reference_equivalences.fst`

Suggested columns:

- `ref_id_left`
- `ref_id_right`
- `link_type`
- `confidence`
- `created_at`
- `notes`

For the DOI-enrichment use case:

- `ref_id_left = "arxiv:..."`
- `ref_id_right = "doi:..."`
- `link_type = "same_work_doi"`

This would make it possible to:

- prefer DOI-backed BibTeX when available
- keep arXiv discovery and publication metadata connected
- avoid heuristic row replacement inside export scripts

### 6. Add export preference controls

Future BibTeX export should support an explicit policy such as:

- `prefer = "canonical"`
- `prefer = "doi_if_richer"`
- `prefer = "arxiv"`

This decision should live in package logic, not in one-off script heuristics.

### 7. Define “richer metadata” explicitly

If export or search needs to prefer one linked record over another, define a
stable scoring rule rather than relying on ad hoc substitution.

Possible metadata richness fields:

- `journal`
- `container_title`
- `publisher`
- `volume`
- `issue`
- `pages`
- `published-print` / equivalent date detail

## Non-Goals

This future improvement should not:

- replace `ref_id` with mutable publication identifiers
- collapse all source records into one lossy merged row
- make arXiv sync depend on a mandatory second network pass
- introduce R6 objects or UI workflows

## Interim Guidance

Until DOI enrichment exists at the package level:

- treat arXiv rows with a DOI as still primarily arXiv-shaped records
- only prefer DOI-backed export when a separate DOI-backed local row is already
  present and clearly richer
- avoid assuming that a DOI alone implies usable publication metadata in the
  local cache

## Suggested Future Milestones

1. Add a DOI-enrichment helper for existing DOI-bearing refs.
2. Add a reference-equivalence index linking arXiv and DOI records.
3. Add export-level preference controls for linked records.
4. Expand DOI enrichment beyond Crossref as needed.
