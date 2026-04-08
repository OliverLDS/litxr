td <- tempfile("litxr-test-")
dir.create(td)
config_path <- file.path(td, ".litxr", "config.yaml")

old_litxr_config <- Sys.getenv("LITXR_CONFIG", unset = NA_character_)
Sys.unsetenv("LITXR_CONFIG")
on.exit({
  if (is.na(old_litxr_config)) {
    Sys.unsetenv("LITXR_CONFIG")
  } else {
    Sys.setenv(LITXR_CONFIG = old_litxr_config)
  }
}, add = TRUE)

stopifnot(inherits(try(litxr::litxr_init(), silent = TRUE), "try-error"))

Sys.setenv(LITXR_CONFIG = config_path)
config_path <- litxr::litxr_init()
stopifnot(file.exists(config_path))
stopifnot(identical(
  normalizePath(config_path, winslash = "/", mustWork = FALSE),
  litxr::litxr_config_path()
))

Sys.setenv(LITXR_CONFIG = config_path)
cfg <- litxr::litxr_read_config()
stopifnot(identical(cfg$project$name, basename(td)))
stopifnot(length(cfg$collections) == 3L)
journals <- litxr::litxr_list_journals(cfg)
stopifnot(inherits(journals, "data.table"))
stopifnot(nrow(journals) == 3L)
collections <- litxr::litxr_list_collections(cfg)
stopifnot(inherits(collections, "data.table"))
stopifnot(nrow(collections) == 3L)
stopifnot(identical(collections$collection_type[[1]], "journal"))
stopifnot(inherits(try(litxr::litxr_init(), silent = TRUE), "try-error"))

tab_cfg_path <- file.path(td, ".litxr", "tab-config.yaml")
writeLines(c(
  "version: 1",
  "project:",
  "\tname: bad_tabs",
  "\tdata_root: data/literature",
  "journals:",
  "\t- journal_id: journal_of_finance",
  "\t  title: Journal of Finance",
  "\t  remote_channel: crossref",
  "\t  local_path: journal_of_finance"
), tab_cfg_path)
tab_cfg <- litxr::litxr_read_config(tab_cfg_path)
stopifnot(identical(tab_cfg$project$name, "bad_tabs"))
stopifnot(identical(tab_cfg$journals[[1]]$journal_id, "journal_of_finance"))
stopifnot(identical(tab_cfg$collections[[1]]$collection_id, "journal_of_finance"))

td_export <- tempfile("litxr-test-")
dir.create(td_export)
cfg_path <- file.path(td_export, ".litxr", "config.yaml")

Sys.setenv(LITXR_CONFIG = cfg_path)
cfg_path <- litxr::litxr_init()
cfg_export <- litxr::litxr_read_config()
journal <- cfg_export$journals[[1]]
local_path <- litxr:::.litxr_resolve_local_path(cfg_export, journal$local_path)

record <- data.table::data.table(
  ref_id = "doi:10.1000/example",
  source = "crossref",
  source_id = "10.1000/example",
  title = "Example Paper",
  abstract = NA_character_,
  authors = "Jane Doe; John Smith",
  authors_list = list(c("Jane Doe", "John Smith")),
  pub_date = as.POSIXct("2024-01-15", tz = "UTC"),
  year = 2024L,
  month = 1L,
  day = 15L,
  journal = "Journal of Finance",
  container_title = "Journal of Finance",
  publisher = "Wiley",
  volume = "10",
  issue = "2",
  pages = "1-20",
  doi = "10.1000/example",
  note = NA_character_,
  subject_primary = "Finance",
  subject_all = "Finance",
  url_landing = "https://doi.org/10.1000/example",
  url_pdf = NA_character_,
  arxiv_version = NA_integer_,
  arxiv_primary_category = NA_character_,
  arxiv_categories_raw = "",
  arxiv_comment = NA_character_,
  arxiv_journal_ref = NA_character_,
  raw_entry = list(NULL),
  collection_id = journal$journal_id,
  collection_title = journal$title
)

litxr:::.litxr_write_journal_records(record, local_path, journal, cfg = cfg_export)
stopifnot(file.exists(file.path(local_path, "index", "references.fst")))
stopifnot(dir.exists(file.path(local_path, "llm")))
stopifnot(file.exists(file.path(litxr:::.litxr_project_root(cfg_export), "index", "references.fst")))
stopifnot(file.exists(file.path(litxr:::.litxr_project_root(cfg_export), "index", "reference_collections.fst")))

read_back <- litxr::litxr_read_journal(journal$journal_id, cfg_export)
stopifnot(nrow(read_back) == 1L)
stopifnot(identical(read_back$doi[[1]], "10.1000/example"))
stopifnot(identical(read_back$authors_list[[1]], c("Jane Doe", "John Smith")))

date_stats <- litxr::litxr_collection_date_stats(journal$journal_id, cfg_export, by = "day")
stopifnot(nrow(date_stats) == 1L)
stopifnot(as.character(date_stats$date[[1]]) == "2024-01-15")
stopifnot(date_stats$n[[1]] == 1L)
stopifnot(identical(attr(date_stats, "date_min"), "2024-01-15"))
stopifnot(identical(attr(date_stats, "date_max"), "2024-01-15"))
stopifnot(length(attr(date_stats, "missing_dates")) == 0L)

project_refs <- litxr::litxr_read_references(cfg_export)
stopifnot(nrow(project_refs) >= 1L)
stopifnot(any(project_refs$ref_id == "doi:10.1000/example"))
project_links <- litxr::litxr_read_reference_collections(cfg_export)
stopifnot(any(project_links$ref_id == "doi:10.1000/example" & project_links$collection_id == journal$journal_id))

found_jof <- litxr::litxr_find_refs(query = "example paper", config = cfg_export)
stopifnot(any(found_jof$ref_id == "doi:10.1000/example"))
found_jof_collection <- litxr::litxr_find_refs(collection_id = journal$journal_id, config = cfg_export)
stopifnot(any(found_jof_collection$ref_id == "doi:10.1000/example"))

index_path <- file.path(local_path, "index", "references.fst")
file.remove(index_path)
stopifnot(!file.exists(index_path))
rebuilt_path <- litxr::litxr_rebuild_journal_index(journal$journal_id, cfg_export)
stopifnot(file.exists(rebuilt_path))

out <- file.path(td_export, "references.bib")
litxr::litxr_export_bib(out, journal_ids = journal$journal_id, config = cfg_export)

stopifnot(file.exists(out))
bib <- paste(readLines(out, warn = FALSE), collapse = "\n")
stopifnot(grepl("@article\\{example,", bib))
stopifnot(grepl("journal = \\{Journal of Finance\\}", bib))
stopifnot(grepl("doi = \\{10.1000/example\\}", bib))

out_keys <- file.path(td_export, "references_by_keys.bib")
warn_msg <- NULL
withCallingHandlers(
  litxr::litxr_export_bib(
    out_keys,
    keys = c("10.1000/example", "doi:10.1000/example", "arxiv:missing-id"),
    config = cfg_export
  ),
  warning = function(w) {
    warn_msg <<- conditionMessage(w)
    invokeRestart("muffleWarning")
  }
)
stopifnot(file.exists(out_keys))
bib_keys <- paste(readLines(out_keys, warn = FALSE), collapse = "\n")
stopifnot(grepl("@article\\{example,", bib_keys))
stopifnot(grepl("doi = \\{10.1000/example\\}", bib_keys))
stopifnot(grepl("arxiv:missing-id", warn_msg, fixed = TRUE))

existing <- data.table::copy(record)
existing[["title"]] <- "Old Title"
existing[["note"]] <- "keep me"

incoming <- data.table::copy(record)
incoming[["title"]] <- "New Title"
incoming[["note"]] <- NA_character_

merged <- litxr:::.litxr_upsert_journal_records(existing, incoming, local_path)
stopifnot(nrow(merged) == 1L)
stopifnot(identical(merged$title[[1]], "New Title"))
stopifnot(identical(merged$note[[1]], "keep me"))
stopifnot(file.exists(file.path(local_path, "json", "_upsert_conflicts.jsonl")))

legacy_truncated <- data.table::copy(record)
legacy_truncated[["doi"]] <- NA_character_
legacy_truncated[["journal"]] <- NA_character_
legacy_truncated[["container_title"]] <- NA_character_
legacy_truncated[["publisher"]] <- NA_character_
legacy_truncated[["volume"]] <- NA_character_
legacy_truncated[["issue"]] <- NA_character_
legacy_truncated[["pages"]] <- NA_character_
legacy_truncated[["collection_id"]] <- NA_character_
legacy_truncated[["collection_title"]] <- NA_character_
legacy_truncated[["authors"]] <- ""
legacy_truncated[["authors_list"]] <- list(character())
legacy_truncated[["pub_date"]] <- as.POSIXct(NA)
legacy_truncated[["year"]] <- NA_integer_
legacy_truncated[["month"]] <- NA_integer_
legacy_truncated[["day"]] <- NA_integer_
legacy_truncated[["url_landing"]] <- NA_character_
legacy_truncated[["url_pdf"]] <- NA_character_

legacy_path <- file.path(local_path, "json", "doi_10_1000_example.json")
full_path <- file.path(local_path, "json", "10_1000_example.json")
jsonlite::write_json(litxr:::.litxr_row_to_storage_payload(legacy_truncated, journal), legacy_path, auto_unbox = TRUE, pretty = TRUE, null = "null")
jsonlite::write_json(litxr:::.litxr_row_to_storage_payload(record, journal), full_path, auto_unbox = TRUE, pretty = TRUE, null = "null")
file.remove(index_path)
rebuilt_path <- litxr::litxr_rebuild_journal_index(journal$journal_id, cfg_export)
rebuilt <- litxr::litxr_read_journal(journal$journal_id, cfg_export)
rebuilt_one <- rebuilt[rebuilt$ref_id == "doi:10.1000/example", ]
stopifnot(nrow(rebuilt_one) == 1L)
stopifnot(identical(rebuilt_one$doi[[1]], "10.1000/example"))
stopifnot(identical(rebuilt_one$journal[[1]], "Journal of Finance"))

new_cr_message <- list(
  title = "New DOI Paper",
  author = list(list(given = "Jane", family = "Doe")),
  issued = list(`date-parts` = list(c(2025, 1, 15))),
  `container-title` = "Review of New Finance",
  publisher = "Test Publisher",
  volume = "1",
  issue = "1",
  page = "1-10",
  DOI = "10.2000/newdoi",
  URL = "https://doi.org/10.2000/newdoi",
  ISSN = c("1234-5678", "8765-4321"),
  `issn-type` = list(
    list(type = "print", value = "1234-5678"),
    list(type = "electronic", value = "8765-4321")
  )
)

registered <- litxr:::.litxr_register_crossref_journal(cfg_export, new_cr_message)
cfg_registered <- registered$cfg
new_journal <- registered$journal
stopifnot(identical(new_journal$title, "Review of New Finance"))
stopifnot(identical(new_journal$remote_channel, "crossref"))
stopifnot(identical(new_journal$metadata$issn_print, "1234-5678"))
stopifnot(file.exists(attr(cfg_registered, "config_path", exact = TRUE)))

cfg_reloaded <- litxr::litxr_read_config(attr(cfg_registered, "config_path", exact = TRUE))
stopifnot(any(vapply(cfg_reloaded$collections, `[[`, character(1), "collection_id") == new_journal$collection_id))

manual_refs <- data.table::data.table(
  source = "book",
  entry_type = "book",
  title = "Manual Book",
  authors = "Jane Doe",
  year = 2024L,
  publisher = "Example Press",
  isbn = "9780262046305",
  url = "https://example.org/manual-book"
)

manual_added <- litxr::litxr_add_refs(
  manual_refs,
  collection_id = "manual_books",
  config = cfg_export,
  auto_register = TRUE,
  collection_title = "Manual Books"
)
stopifnot(nrow(manual_added) == 1L)
stopifnot(identical(manual_added$ref_id[[1]], "isbn:9780262046305"))

cfg_manual <- litxr::litxr_read_config(attr(cfg_export, "config_path", exact = TRUE))
stopifnot(any(vapply(cfg_manual$collections, `[[`, character(1), "collection_id") == "manual_books"))

manual_read <- litxr::litxr_read_collection("manual_books", cfg_manual)
stopifnot(nrow(manual_read) == 1L)
stopifnot(identical(manual_read$entry_type[[1]], "book"))
stopifnot(identical(manual_read$isbn[[1]], "9780262046305"))

project_refs_manual <- litxr::litxr_read_references(cfg_manual)
stopifnot(any(project_refs_manual$ref_id == "isbn:9780262046305"))
project_links_manual <- litxr::litxr_read_reference_collections(cfg_manual)
stopifnot(any(project_links_manual$ref_id == "isbn:9780262046305" & project_links_manual$collection_id == "manual_books"))

found_manual_book <- litxr::litxr_find_refs(entry_type = "book", collection_id = "manual_books", config = cfg_manual)
stopifnot(any(found_manual_book$ref_id == "isbn:9780262046305"))

manual_bib_path <- file.path(td_export, "manual_books.bib")
litxr::litxr_export_bib(manual_bib_path, journal_ids = "manual_books", config = cfg_manual)
manual_bib <- paste(readLines(manual_bib_path, warn = FALSE), collapse = "\n")
stopifnot(grepl("@book\\{9780262046305,", manual_bib))

llm_path <- litxr::litxr_write_llm_digest(
  "isbn:9780262046305",
  list(
    summary = "A concise summary of the manual book.",
    motivation = "Understand manual reference ingestion.",
    key_findings = c("Manual books can be stored.", "Digests are searchable."),
    keywords = c("manual", "book", "ingestion")
  ),
  config = cfg_manual
)
stopifnot(file.exists(llm_path))

llm_one <- litxr::litxr_read_llm_digest("isbn:9780262046305", cfg_manual)
stopifnot(identical(llm_one$ref_id, "isbn:9780262046305"))
stopifnot(identical(llm_one$motivation, "Understand manual reference ingestion."))

llm_all <- litxr::litxr_read_llm_digests(cfg_manual)
stopifnot(any(llm_all$ref_id == "isbn:9780262046305"))

llm_found <- litxr::litxr_find_llm(query = "searchable", collection_id = "manual_books", config = cfg_manual)
stopifnot(any(llm_found$ref_id == "isbn:9780262046305"))

md_path <- litxr::litxr_write_md(
  "isbn:9780262046305",
  "# Manual Book\n\nThis markdown captures the full-text derivative.",
  config = cfg_manual
)
stopifnot(file.exists(md_path))
md_text <- litxr::litxr_read_md("isbn:9780262046305", cfg_manual)
stopifnot(grepl("full-text derivative", md_text, fixed = TRUE))

stopifnot(isTRUE(litxr::litxr_validate_llm_digest(llm_one)))

status <- litxr::litxr_read_enrichment_status(cfg_manual)
status_one <- status[status$ref_id == "isbn:9780262046305", ]
stopifnot(nrow(status_one) == 1L)
stopifnot(isTRUE(status_one$has_md[[1]]))
stopifnot(isTRUE(status_one$has_llm_digest[[1]]))

candidates_manual <- litxr::litxr_list_enrichment_candidates(config = cfg_manual, collection_id = "manual_books")
candidate_one <- candidates_manual[candidates_manual$ref_id == "isbn:9780262046305", ]
stopifnot(nrow(candidate_one) == 1L)
stopifnot(isTRUE(candidate_one$eligible[[1]]) == FALSE)
stopifnot(identical(candidate_one$reason[[1]], "digest_exists"))

manual_refs2 <- data.table::data.table(
  source = "report",
  entry_type = "techreport",
  title = "Manual Report",
  authors = "Jane Analyst",
  year = 2025L,
  url = "https://example.org/manual-report"
)
litxr::litxr_add_refs(
  manual_refs2,
  collection_id = "manual_books",
  config = cfg_manual,
  auto_register = FALSE
)
candidates_after_report <- litxr::litxr_list_enrichment_candidates(config = cfg_manual, collection_id = "manual_books")
report_row <- candidates_after_report[candidates_after_report$title == "Manual Report", ]
stopifnot(nrow(report_row) == 1L)
stopifnot(isFALSE(report_row$has_md[[1]]))
stopifnot(isFALSE(report_row$eligible[[1]]))
stopifnot(identical(report_row$reason[[1]], "missing_md"))

builder_fun <- function(ref, markdown, template) {
  list(
    summary = paste("Built from", ref$title[[1]]),
    motivation = "Test builder path.",
    research_questions = c("What is the manual workflow?"),
    methods = c("Structured parsing"),
    key_findings = c("Builder writes a digest."),
    limitations = c("Mock builder only."),
    keywords = c("builder", "test"),
    notes = markdown
  )
}

built_one <- litxr::litxr_build_llm_digest(
  "isbn:9780262046305",
  builder = builder_fun,
  config = cfg_manual,
  overwrite = TRUE
)
stopifnot(identical(built_one$motivation, "Test builder path."))

built_batch <- litxr::litxr_build_llm_digests(
  builder = builder_fun,
  config = cfg_manual,
  ref_ids = "isbn:9780262046305",
  overwrite = TRUE
)
stopifnot(identical(names(built_batch), "isbn:9780262046305"))

sync_row <- litxr:::.litxr_make_sync_state_row(
  collection_id = "manual_books",
  remote_channel = "manual",
  sync_type = "manual_add",
  status = "success",
  started_at = "2026-01-01 00:00:00 UTC",
  completed_at = "2026-01-01 00:00:10 UTC",
  query = NA_character_,
  range_from = NA_character_,
  range_to = NA_character_,
  fetched_from = "2026-01-01",
  fetched_to = "2026-01-01",
  page_start = NA_integer_,
  page_size = NA_integer_,
  records_fetched = 1L,
  records_after = 2L,
  notes = "test"
)
litxr:::.litxr_append_sync_state(cfg_manual, sync_row)
sync_state <- litxr::litxr_read_sync_state(cfg_manual, collection_id = "manual_books")
stopifnot(nrow(sync_state) >= 1L)
stopifnot(any(sync_state$sync_type == "manual_add"))
stopifnot(any(sync_state$fetched_from == "2026-01-01"))
stopifnot(any(sync_state$fetched_to == "2026-01-01"))

cfg_sync_rebuild <- litxr::litxr_read_config(attr(cfg_export, "config_path", exact = TRUE))
empty_sync_path <- file.path(litxr:::.litxr_project_root(cfg_sync_rebuild), "index", "sync_state.fst")
if (file.exists(empty_sync_path)) file.remove(empty_sync_path)
sync_before_rebuild <- litxr::litxr_read_sync_state(cfg_sync_rebuild)
stopifnot(nrow(sync_before_rebuild) == 0L)
rebuilt_sync <- litxr::litxr_rebuild_sync_state(cfg_sync_rebuild, overwrite = TRUE)
rebuilt_sync_one <- rebuilt_sync[rebuilt_sync$collection_id == journal$journal_id, ]
stopifnot(nrow(rebuilt_sync_one) == 1L)
stopifnot(identical(rebuilt_sync_one$sync_type[[1]], "inferred_rebuild"))
stopifnot(identical(rebuilt_sync_one$status[[1]], "inferred"))
stopifnot(rebuilt_sync_one$records_after[[1]] >= 1L)
