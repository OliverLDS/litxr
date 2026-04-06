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
