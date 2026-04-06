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
journals <- litxr::litxr_list_journals(cfg)
stopifnot(inherits(journals, "data.table"))
stopifnot(nrow(journals) == 3L)
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
tab_err <- try(litxr::litxr_read_config(tab_cfg_path), silent = TRUE)
stopifnot(inherits(tab_err, "try-error"))
stopifnot(grepl("spaces, not tabs", as.character(tab_err), fixed = TRUE))

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

litxr:::.litxr_write_journal_records(record, local_path, journal)
stopifnot(file.exists(file.path(local_path, "index", "references.fst")))
stopifnot(dir.exists(file.path(local_path, "llm")))

read_back <- litxr::litxr_read_journal(journal$journal_id, cfg_export)
stopifnot(nrow(read_back) == 1L)
stopifnot(identical(read_back$doi[[1]], "10.1000/example"))
stopifnot(identical(read_back$authors_list[[1]], c("Jane Doe", "John Smith")))

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
