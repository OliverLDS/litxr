# Enrichment And Search

The enrichment layer is keyed by canonical `ref_id`, not by collection path.

## Canonical References

Project-level readers:

- `litxr_read_references()`
- `litxr_read_reference_collections()`
- `litxr_find_refs()`

`litxr_find_refs()` supports:

- free-text query over common bibliographic fields
- exact filters like `entry_type`, `year`, `collection_id`, `doi`, `ref_id`, `isbn`

## Embedding Search

Embedding indexes are cached under
`project.data_root/corpus/<collection_id>/<field>/embeddings/` and keyed by
collection, source text field, and exact embedding model. Query-set embeddings
live under `project.data_root/queries/embeddings/`. The package does not choose
a provider; pass an embedding function so corpus and query embeddings use the
same model.

Example:

```r
embed_fun <- function(texts) {
  inferencer::embed_openrouter(texts, model = "your-embedding-model")
}

litxr_build_embedding_index(
  "arxiv_cs_ai",
  cfg,
  field = "abstract",
  embed_fun = embed_fun,
  model = "your-embedding-model",
  provider = "openrouter",
  batch_size = 64
)

litxr_search_embeddings(
  "graph neural networks for planning",
  "arxiv_cs_ai",
  cfg,
  field = "abstract",
  embed_fun = embed_fun,
  model = "your-embedding-model",
  top_n = 20
)
```

Use the same `model` value for corpus build and query search. If provider
payloads are rejected, rerun with a smaller `batch_size`. Completed batches are
written to append-only delta shards as the build proceeds, then compacted into
the main embedding index once at the end. If the process is interrupted, the
delta shards are left on disk and reruns without `overwrite = TRUE` continue
from missing references instead of recomputing the whole corpus.

Newly compacted corpus embedding caches use float32 storage and are sharded by
`year`. This reduces compacted cache size and avoids rewriting one monolithic
matrix file for the whole corpus.

If you want to separate those two stages explicitly, use:

- `litxr_embed_collection_delta()` to write only the missing embeddings into
  delta shards
- `litxr_compact_embedding_delta()` to merge pending delta shards into the main
  embedding cache
- `litxr_search_embedding_delta()` to search selected delta shard files before
  compaction, including filtering by shard date

For repeated searches, both `litxr_search_embeddings()` and
`litxr_search_embedding_delta()` also accept a precomputed numeric
`query_vec`. In that mode, they do not call `embed_fun` or any external
embedding API.

## Category Labeling

You can also build a small embedding-based labeling workflow for one corpus.
The intended pattern is:

1. Define a named list of category query sentences.
2. Build a cached query embedding index with `litxr_build_label_query_index()`.
3. Score one collection against those category queries with
   `litxr_score_collection_categories()`.
4. Apply threshold rules with `litxr_label_collection_by_category()`.

Cached category-query embedding roots now include a `query_set.yaml` file at
`project.data_root/queries/embeddings/<query_set_id>/query_set.yaml`. It shows
the category ids and inquiry sentences used to build the cache, so the semantic
meaning of the inquiries is visible without opening the original source YAML.
For an older cache that only has `metadata.fst`, you can backfill the
descriptor from a model subdirectory with:

```bash
Rscript scripts/cache_arxiv_embedding_inquiry_set.R \
  --cache-dir /path/to/ai_category_query_set_v1/nvidia_llama_nemotron_embed_vl_1b_v2_free
```

Example:

```r
query_set <- list(
  reasoning = c(
    "This paper studies reasoning in language models.",
    "This paper focuses on planning and multi-step inference."
  ),
  retrieval = c(
    "This paper studies retrieval-augmented generation.",
    "This paper focuses on dense retrieval and semantic search."
  )
)

litxr_build_label_query_index(
  query_set = query_set,
  query_set_id = "ai-topics-v1",
  config = cfg,
  embed_fun = embed_fun,
  model = "your-embedding-model"
)

scores <- litxr_score_collection_categories(
  "arxiv_cs_ai",
  query_set_id = "ai-topics-v1",
  config = cfg,
  field = "abstract",
  model = "your-embedding-model",
  aggregations = c("max", "mean")
)

labels <- litxr_label_collection_by_category(
  scores,
  score_col = "score_max",
  threshold = 0.35
)
```

For repeated category searches, keep the collection embedding index and the
query-set embedding index on the same `model`.

## Markdown

Project-level markdown helpers:

- `litxr_write_md(ref_id, text, config = ...)`
- `litxr_read_md(ref_id, config = ...)`

This is the current place to store extracted or manually prepared article text
before digest generation.

## LLM Digest Contract

Each digest is one JSON file under `project.data_root/digest/llm/`.

Interactive prompt default: schema `v4`.

Package template default: schema `v2`, retained for backward compatibility in
programmatic code.

Legacy `v1` digests without `schema_version` still read and validate
successfully.

Core latest fields:

- `schema_version`
- `ref_id`
- `digest_revision`
- `extraction_mode`
- `prompt_version`
- `model_hint`
- `paper_type`
- `summary`
- `motivation`
- `research_questions`
- `paper_structure`
- `methods`
- `research_data`
  - `sample_size` stays numeric when possible, while narrative sample-size context belongs in `sample_size_note`
- `identification_strategy`
- `main_variables`
- `key_findings`
- `limitations`
- `theoretical_mechanism`
- `empirical_setting`
- `descriptive_statistics_summary`
- `standardized_findings_summary`
- `contribution_type`
- `ranked_contributions`
- `likely_reader_misconceptions`
- `business_relevance_pathway`
- `tables`
- `research_target_github_links`
- `evidence_strength`
- `evidence_shape`
- `anchor_references`
- `citation_logic_nodes`
- `keywords`
- `notes`
- `generated_at`
- `updated_at`

`identification_strategy` means how the paper supports its empirical or causal
claim through research design, such as randomized assignment,
quasi-experimental design, panel/observational identification, qualitative
process tracing, benchmark protocol, triangulation, or a statement that the
paper does not make a causal claim.

Helpers:

- `litxr_llm_digest_template()`
- `litxr_validate_llm_digest()`
- `litxr_write_llm_digest()`
- `litxr_read_llm_digest()`
- `litxr_read_llm_digests()`
- `litxr_list_llm_digest_revisions()`
- `litxr_read_llm_digest_history()`
- `litxr_find_llm()`

The `paper_type` field uses the canonical vocabulary returned by
`litxr_paper_type_levels()`.

Schema `v3` digests may carry optional inline `anchor_references` and
`citation_logic_nodes` fields. Schema `v4` keeps those blocks and adds
`ranked_contributions`, `likely_reader_misconceptions`,
`business_relevance_pathway`, `tables`, `research_target_github_links`, and
`evidence_shape`. The same anchor and citation-node information can also be
stored as separate project-level tables under `project.data_root/findings/`.

## Building Digests

Single-reference build:

```r
builder_fun <- function(ref, markdown, template) {
  template$summary <- paste("Draft summary for", ref$title[[1]])
  template$motivation <- "Replace with your extraction logic."
  template$research_questions <- c("Question 1")
  template$methods <- c("Method 1")
  template$key_findings <- c("Finding 1")
  template$limitations <- c("Limitation 1")
  template$keywords <- c("keyword")
  template
}

litxr_build_llm_digest(ref_id, builder = builder_fun, config = cfg)
```

## Candidate Inspection

Use `litxr_list_enrichment_candidates()` to see what is ready and why other
references are excluded.

Example:

```r
litxr_list_enrichment_candidates(cfg, collection_id = "journal_of_finance")
```

Output flags:

- `has_md`
- `has_llm_digest`
- `eligible`
- `reason`

Current reasons:

- `ready`
- `missing_md`
- `digest_exists`

## Interactive Digest Workflow

For manual schema-v5 extraction with ChatGPT, use:

- `scripts/run_llm_digest_interactive.sh`

Example:

```sh
scripts/run_llm_digest_interactive.sh \
  --ref-id arxiv:2505.07087
```

The shell wrapper checks whether the digest already exists, generates the
appropriate create or revise prompt, copies it with `pbcopy`, then hands ingest
off to `scripts/ingest_llm_digest_json.R` after you confirm the download.
On macOS, the prompt is copied to the clipboard with `pbcopy` instead of being
dumped directly to the terminal.

Prompt generation itself is package-owned through `litxr_llm_digest_prompt()`.
The prompt text is assembled from Markdown fragments under
`inst/prompts/llm_digest_v5/fragments/`. V5 inherits unchanged V4 prompt
fragments and overrides the task and schema contract, while the shell wrapper remains a thin
orchestration layer.

To inspect a reference quickly, use `scripts/report_ref_summary.sh`. By default it
prints only the key schema sections. Pass `--complete` or `--report complete`
to render the full digest.

Use `--mode revise` when a local digest already exists and you want ChatGPT to
improve that digest rather than start from scratch. Revised digests bump
`digest_revision` and archive the prior current digest under
`project.data_root/digest/llm_history/`.

For the revision-aware digest design and future extension points, see
[llm-digest-revision-design.md](./llm-digest-revision-design.md).
