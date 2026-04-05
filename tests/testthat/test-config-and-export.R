td <- tempfile("litxr-test-")
dir.create(td)

config_path <- litxr::litxr_init(td)
stopifnot(file.exists(config_path))
stopifnot(file.exists(file.path(td, ".gitignore")))

gitignore <- readLines(file.path(td, ".gitignore"), warn = FALSE)
stopifnot(".litxr/config.yaml" %in% gitignore)

cfg <- litxr::litxr_read_config(td)
journals <- litxr::litxr_list_journals(cfg)
stopifnot(inherits(journals, "data.table"))
stopifnot(nrow(journals) == 2L)

td_export <- tempfile("litxr-test-")
dir.create(td_export)

cfg_path <- litxr::litxr_init(td_export)
cfg_export <- litxr::litxr_read_config(cfg_path)
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

out <- file.path(td_export, "references.bib")
litxr::litxr_export_bib(out, journal_ids = journal$journal_id, config = cfg_export)

stopifnot(file.exists(out))
bib <- paste(readLines(out, warn = FALSE), collapse = "\n")
stopifnot(grepl("@article\\{example,", bib))
stopifnot(grepl("journal = \\{Journal of Finance\\}", bib))
stopifnot(grepl("doi = \\{10.1000/example\\}", bib))
