# litxr

`litxr` is a lightweight R package for managing academic literature as local data.
It is designed around journal-level sync, local reference stores, and downstream
export to BibTeX for Quarto writing.

## Current Architecture

The package centers on a local project config file whose path is stored in
`LITXR_CONFIG` in `.Renviron`.
The recommended pattern is to point `LITXR_CONFIG` at a project-local
`.litxr/config.yaml`.
Each configured journal declares:

- a `journal_id`
- a `remote_channel`
- journal metadata such as title and ISSN
- a `local_path` for local storage
- sync settings

The current storage model for each journal is:

- `json/` for parsed raw reference records
- `index/` for fast local tables such as `references.fst`
- `pdf/` for downloaded article PDFs
- `md/` for article-level markdown derived from full text or notes
- `llm/` for LLM-digested structured outputs such as motivations, questions,
  logic, sample features, and key findings

The package already exposes:

- `litxr_init()`
- `litxr_read_config()`
- `litxr_list_journals()`
- `litxr_sync_journal()`
- `litxr_sync_all()`
- `litxr_repair_journal()`
- `litxr_read_journal()`
- `litxr_export_bib()`

## End-To-End Example

The minimal project workflow is:

```r
library(litxr)

# 1. Add this to .Renviron, then restart R:
#    LITXR_CONFIG=/absolute/path/to/project/.litxr/config.yaml

# 2. Create the config file if it does not exist yet.
litxr_init()

# 3. Edit the config file before syncing.
#    - project.data_root: root folder for your literature data store
#    - journals[[i]]$local_path: per-journal folder for json/pdf/md files
#    - omit sync.limit if you want a full journal harvest
#
# 4. Confirm the journal metadata and ISSNs.
cfg <- litxr_read_config()
litxr_list_journals(cfg)

# 5. Sync one configured journal into the local JSON store.
records <- litxr_sync_journal("journal_of_finance", cfg)

# 6. Export the locally stored records to BibTeX for Quarto.
litxr_export_bib("references.bib", journal_ids = "journal_of_finance", config = cfg)
```

After sync, the journal data lives under the configured `local_path`, with
metadata in `json/`, a fast materialized index in `index/`, and room for
future `pdf/`, `md/`, and `llm/` assets.

Basic summary after reading a journal:

```r
DT <- litxr_read_journal("journal_of_finance", cfg)

nrow(DT)
sum(!is.na(DT$doi) & nzchar(DT$doi))
range(DT$year, na.rm = TRUE)
table(DT$year)[order(as.integer(names(table(DT$year))))]

volume_num <- sort(unique(as.integer(DT$volume[grepl("^[0-9]+$", DT$volume)])))
volume_gap <- setdiff(seq(min(volume_num), max(volume_num)), volume_num)
if (length(volume_gap) == 0) {
  "No gap in numeric volume sequence."
} else {
  paste("Missing numeric volumes:", paste(volume_gap, collapse = ", "))
}

DT[order(-pub_date)][1:30, c("title", "pub_date", "year", "volume", "issue", "doi")]
```

Path note:

- set `project.data_root: .` if you want it to mean the folder containing `config.yaml`
- set `project.data_root: ~` only if you want to use your home directory

Current implementation note:

- `json/` and `index/` are active today
- `pdf/`, `md/`, and `llm/` are part of the intended journal storage contract,
  but full PDF download, HTML-to-Markdown conversion, and LLM digestion are not
  yet implemented end to end

## arXiv Rate Limits And Repair

arXiv is not like an exchange candle API with a simple authoritative full date
grid. For a query such as `cat:cs.AI`, there is no local-vs-remote gap check as
clean as "expected timestamps from metadata, then fill the missing intervals".

So the practical arXiv strategy in `litxr` is:

- use a request delay between arXiv API calls
- retry transient failures such as HTTP 429
- repair or backfill in bounded submitted-date windows
- make each repair run idempotent through local upsert by DOI or source id

Current arXiv sync behavior:

- `sync.delay_seconds` controls the pause between paginated arXiv requests
- `sync.rows` controls arXiv batch size per request
- `sync.limit` controls the final record count only when you explicitly set it
- if `sync.limit` is omitted, `litxr` paginates until the query is exhausted

Example arXiv config:

```yaml
- journal_id: arxiv_cs_ai
  title: arXiv cs.AI
  remote_channel: arxiv
  local_path: data/literature/arxiv_cs_ai
  metadata:
    archive: arXiv
    category: cs.AI
  sync:
    search_query: cat:cs.AI
    rows: 100
    delay_seconds: 3
```

If a full arXiv sync hits rate limits, use bounded repair windows instead of one
large run:

```r
cfg <- litxr_read_config()

litxr_repair_journal(
  "arxiv_cs_ai",
  cfg,
  submitted_from = "2026-01-01",
  submitted_to = "2026-01-31",
  limit = 500
)
```

That produces a query like:

```text
(cat:cs.AI) AND submittedDate:[202601010000 TO 202601312359]
```

You can call the repair helper repeatedly from an external shell or cron job to
gradually complete the local store without hammering arXiv.

Example shell pattern:

```sh
Rscript -e 'library(litxr); cfg <- litxr_read_config(); litxr_repair_journal("arxiv_cs_ai", cfg, submitted_from = "2026-01-01", submitted_to = "2026-01-31", limit = 500)'
```

There is also a small CLI wrapper:

```sh
Rscript scripts/repair_arxiv.R --journal-id arxiv_cs_ai --submitted-from 2026-01-01 --submitted-to 2026-01-31 --limit 500
```

For old local stores, you can rebuild the fast local index after repair:

```r
litxr_rebuild_journal_index("arxiv_cs_ai", cfg)
```

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

By default, a full journal sync should omit `sync.limit`. If `sync.limit` is
set, `litxr` will truncate the final result to at most that many records.

When a synced record matches an existing local record by DOI or source id,
`litxr` performs an explicit upsert:

- remote bibliographic metadata replaces old values
- local user-authored fields such as `note` are preserved
- unusual field changes are recorded in `json/_upsert_conflicts.jsonl`

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
      # omit limit for a full harvest
  - journal_id: arxiv_cs_ai
    title: arXiv cs.AI
    remote_channel: arxiv
    local_path: data/literature/arxiv_cs_ai
    metadata:
      archive: arXiv
      category: cs.AI
    sync:
      search_query: cat:cs.AI
```

Using ISSN-based sync is more stable and precise than title-based search.

## Summary

For a journal like *Journal of Finance*, the best implementation is:

- one paginated Crossref journal sync
- DOI extracted from each work payload during that same sync
- local upsert keyed by DOI when available
- future incremental updates using Crossref update-date filters

That is the architecture `litxr` should follow.
