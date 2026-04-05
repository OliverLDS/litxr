# litxr

`litxr` is a lightweight R package for managing academic literature as local data.
It is designed around journal-level sync, local reference stores, and downstream
export to BibTeX for Quarto writing.

## Current Architecture

The package centers on a local project config at `.litxr/config.yaml`.
Each configured journal declares:

- a `journal_id`
- a `remote_channel`
- journal metadata such as title and ISSN
- a `local_path` for local storage
- sync settings

The current storage model for each journal is:

- `json/` for parsed metadata records
- `pdf/` for article PDFs
- `md/` for article notes, narratives, or future derived outputs

The package already exposes:

- `litxr_init()`
- `litxr_read_config()`
- `litxr_list_journals()`
- `litxr_sync_journal()`
- `litxr_sync_all()`
- `litxr_read_journal()`
- `litxr_export_bib()`

## End-To-End Example

The minimal project workflow is:

```r
library(litxr)

# 1. Create the local project config.
litxr_init(".")

# 2. Edit .litxr/config.yaml and confirm the journal metadata and ISSNs.
cfg <- litxr_read_config(".")
litxr_list_journals(cfg)

# 3. Sync one configured journal into the local JSON store.
records <- litxr_sync_journal("journal_of_finance", cfg)

# 4. Export the locally stored records to BibTeX for Quarto.
litxr_export_bib("references.bib", journal_ids = "journal_of_finance", config = cfg)
```

After sync, the journal data lives under the configured `local_path`, with
metadata in `json/` and room for future `pdf/` and `md/` assets.

## Journal Sync Design

For journals such as *Journal of Finance*, Crossref is the correct primary sync
source.

The important design choice is:

- do one full paginated journal sync from Crossref
- do not first fetch a DOI list and then re-fetch each DOI

That second step is unnecessary because Crossref already returns the DOI inside
each work record.

So the correct sync pipeline is:

```text
Crossref journal endpoint by ISSN
-> paginated work records
-> parse DOI + title + authors + dates + journal metadata + links
-> upsert into local JSON store
-> export BibTeX from local store
```

## Why One Hit Is Enough

When syncing a journal from Crossref, each returned work payload already
contains:

- `DOI`
- title
- author list
- publication dates
- container title
- volume, issue, pages
- publisher
- URLs and other metadata when available

That means the DOI should be extracted from the same payload used for the rest of
the reference metadata.

Use DOI as the canonical identifier when present, but do not make DOI the only
possible internal key. Some literature objects may not have a DOI yet. `litxr`
should keep an internal `ref_id`, for example:

- `doi:10.1111/jofi.12345`
- `arxiv:2501.12345v1`

## How To Implement Full Paginated Journal Sync

### Recommended Crossref endpoint

For a known journal, use the journal ISSN endpoint:

```text
https://api.crossref.org/journals/{issn}/works
```

For example, Journal of Finance print ISSN:

```text
https://api.crossref.org/journals/0022-1082/works
```

If a journal has both print and electronic ISSNs, sync both and deduplicate on
DOI.

### Required pagination strategy

Do not use offset-based pagination for full journal harvests.
Use Crossref cursor pagination:

- first request: `cursor=*`
- set `rows` to a large value such as `1000`
- read `message$items`
- read `message$next-cursor`
- keep requesting until the returned item count is less than `rows`

This is the correct pattern for large result sets.

### Minimal request shape

```text
/journals/{issn}/works?rows=1000&cursor=*
```

If you only need a small subset of fields, Crossref supports `select=...`.
However, for `litxr`, full-record retrieval is usually better because:

- parsing logic is simpler
- later enrichment fields are already present locally
- journal sync is easier to evolve without changing fetch shape often

So for `litxr`, prefer retrieving full records during sync and pruning locally if
needed.

## Crossref Pagination Implementation

`litxr` now implements a dedicated fetch helper for paginated journal sync:

```r
fetch_crossref_journal_works <- function(issn, limit = Inf, rows = 1000L) {
  cursor <- "*"
  out <- list()
  total <- 0L

  repeat {
    resp <- httr2::request(sprintf("https://api.crossref.org/journals/%s/works", issn)) |>
      httr2::req_url_query(rows = rows, cursor = cursor) |>
      httr2::req_perform()

    payload <- httr2::resp_body_json(resp, simplifyVector = FALSE)
    items <- payload$message$items

    if (!length(items)) break

    out <- c(out, items)
    total <- total + length(items)

    if (is.finite(limit) && total >= limit) {
      out <- out[seq_len(limit)]
      break
    }

    if (length(items) < rows) break

    cursor <- payload$message$`next-cursor`
    if (is.null(cursor) || !nzchar(cursor)) break
  }

  out
}
```

## How This Fits Into `litxr_sync_journal()`

For a Crossref-configured journal, `litxr_sync_journal()` should:

1. read the journal entry from `.litxr/config.yaml`
2. get one or more ISSNs from `metadata` or `sync.filters`
3. call the paginated Crossref journal fetch helper
4. parse every returned item with `parse_crossref_entry_unified()`
5. add collection metadata such as `collection_id` and `collection_title`
6. upsert each record into the local JSON store

`litxr_sync_journal()` now uses journal-specific paginated Crossref sync when
`remote_channel: crossref`.

## Upsert Rules

For local storage, use:

- DOI as the canonical match key when present
- otherwise use `ref_id`

When a synced record already exists:

- overwrite metadata fields from the newest Crossref payload
- preserve local user-authored files such as notes in `md/`
- do not delete PDFs or notes just because the latest metadata payload omits a field

## Incremental Sync After Initial Backfill

For the first sync of a journal, do a full paginated harvest.

After that, incremental sync should use a date filter such as:

- `from-update-date`
- or `from-created-date` if needed for a narrower workflow

The incremental pipeline is then:

```text
last successful sync timestamp
-> Crossref query with from-update-date
-> fetch changed records only
-> upsert locally
```

This avoids re-harvesting the entire journal every time.

## Configuration Recommendation

For journal configs, prefer storing ISSNs explicitly in YAML rather than relying
on journal title matching.

Example:

```yaml
journals:
  - journal_id: journal_of_finance
    title: Journal of Finance
    remote_channel: crossref
    local_path: data/literature/journal_of_finance
    metadata:
      publisher: Wiley
      issn_print: 0022-1082
      issn_electronic: 1540-6261
    sync:
      limit: 1000
```

Using ISSN-based sync is more stable and precise than title-based search.

## Summary

For a journal like *Journal of Finance*, the best implementation is:

- one paginated Crossref journal sync
- DOI extracted from each work payload during that same sync
- local upsert keyed by DOI when available
- future incremental updates using Crossref update-date filters

That is the architecture `litxr` should follow.
