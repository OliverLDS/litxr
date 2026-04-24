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

Embedding indexes are cached under `project.data_root/embeddings/` and keyed by
collection, source text field, and exact embedding model. The package does not
choose a provider; pass an embedding function so corpus and query embeddings use
the same model.

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

Each digest is one JSON file under `project.data_root/llm/`.

Core fields:

- `ref_id`
- `summary`
- `motivation`
- `research_questions`
- `methods`
- `sample`
- `key_findings`
- `limitations`
- `keywords`
- `notes`
- `generated_at`

Helpers:

- `litxr_llm_digest_template()`
- `litxr_validate_llm_digest()`
- `litxr_write_llm_digest()`
- `litxr_read_llm_digest()`
- `litxr_read_llm_digests()`
- `litxr_find_llm()`

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

Batch build:

```r
litxr_build_llm_digests(builder = builder_fun, config = cfg, limit = 20)
```

By default, batch build targets references with markdown and without an existing
digest.

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

## Shell Workflow

The package includes:

- `scripts/build_llm_digests.R`
- `scripts/example_digest_builder.R`

Example:

```sh
Rscript scripts/build_llm_digests.R \
  --builder-file scripts/example_digest_builder.R \
  --collection-id journal_of_finance \
  --limit 20
```

Use the example builder as a scaffold and replace its placeholder logic with
your actual local LLM or rule-based extractor.
