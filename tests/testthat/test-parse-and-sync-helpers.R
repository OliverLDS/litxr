cr_message <- list(
  title = "A Test Article",
  author = data.frame(
    given = c("Jane", "John"),
    family = c("Doe", "Smith"),
    stringsAsFactors = FALSE
  ),
  `published-print` = list(`date-parts` = matrix(c(2024, 2, 15), nrow = 1)),
  `container-title` = "Journal of Finance",
  publisher = "Wiley",
  volume = "12",
  issue = "3",
  page = "101-120",
  DOI = "10.1000/testdoi",
  URL = "https://doi.org/10.1000/testdoi",
  ISBN = "9780262046305",
  ISSN = c("0022-1082", "1540-6261"),
  subject = c("Finance", "Asset Pricing")
)

row <- litxr::parse_crossref_entry_unified(cr_message)
stopifnot(identical(row$ref_id[[1]], "doi:10.1000/testdoi"))
stopifnot(identical(row$title[[1]], "A Test Article"))
stopifnot(identical(row$authors[[1]], "Jane Doe; John Smith"))
stopifnot(identical(row$journal[[1]], "Journal of Finance"))
stopifnot(identical(row$publisher[[1]], "Wiley"))
stopifnot(identical(row$year[[1]], 2024L))
stopifnot(identical(row$entry_type[[1]], "article"))
stopifnot(identical(row$isbn[[1]], "9780262046305"))
stopifnot(identical(row$issn[[1]], "0022-1082; 1540-6261"))
stopifnot(identical(row$url[[1]], "https://doi.org/10.1000/testdoi"))

records <- data.table::data.table(
  ref_id = c("doi:10.1000/a", "doi:10.1000/a", "arxiv:1234.5678v1"),
  doi = c("10.1000/a", "10.1000/a", NA_character_),
  title = c("A", "A duplicate", "B")
)

deduped <- litxr:::.litxr_deduplicate_records(records)
stopifnot(nrow(deduped) == 2L)
stopifnot(identical(deduped$title[[1]], "A"))
stopifnot(identical(deduped$title[[2]], "B"))

cr_message_list_date <- list(
  title = "List Date Article",
  author = data.frame(
    given = "Jane",
    family = "Doe",
    stringsAsFactors = FALSE
  ),
  issued = list(`date-parts` = list(c(2024, 3, 7))),
  `container-title` = "Journal of Finance",
  DOI = "10.1000/listdate"
)

row_list_date <- litxr::parse_crossref_entry_unified(cr_message_list_date)
stopifnot(identical(row_list_date$year[[1]], 2024L))
stopifnot(identical(row_list_date$month[[1]], 3L))
stopifnot(identical(row_list_date$day[[1]], 7L))

cr_message_list_link <- list(
  title = "List Link Article",
  author = data.frame(
    given = "Jane",
    family = "Doe",
    stringsAsFactors = FALSE
  ),
  issued = list(`date-parts` = list(c(2024, 1, 1))),
  `container-title` = "Journal of Finance",
  DOI = "10.1000/listlink",
  link = list(
    list(URL = "https://example.org/paper.pdf", `content-type` = "application/pdf")
  )
)

row_list_link <- litxr::parse_crossref_entry_unified(cr_message_list_link)
stopifnot(identical(row_list_link$url_pdf[[1]], "https://example.org/paper.pdf"))

cr_message_list_author <- list(
  title = "List Author Article",
  author = list(
    list(given = "Jane", family = "Doe"),
    list(given = "John", family = "Smith")
  ),
  issued = list(`date-parts` = list(c(2024, 1, 1))),
  `container-title` = "Journal of Finance",
  DOI = "10.1000/listauthor"
)

row_list_author <- litxr::parse_crossref_entry_unified(cr_message_list_author)
stopifnot(identical(row_list_author$authors[[1]], "Jane Doe; John Smith"))

arxiv_xml <- xml2::read_xml(
'<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/2501.12345v2</id>
    <title>Example arXiv Paper</title>
    <summary>Example abstract.</summary>
    <published>2025-01-15T00:00:00Z</published>
    <author><name>Jane Doe</name></author>
    <author><name>John Smith</name></author>
    <link rel="alternate" href="http://arxiv.org/abs/2501.12345v2"/>
    <link title="pdf" href="http://arxiv.org/pdf/2501.12345v2"/>
    <category term="cs.AI"/>
    <category term="cs.LG"/>
    <arxiv:doi>10.1000/arxiv-example</arxiv:doi>
  </entry>
</feed>')

arxiv_entry <- xml2::xml_find_first(arxiv_xml, ".//*[local-name() = 'entry']")
arxiv_row <- litxr::parse_arxiv_entry_unified(arxiv_entry)
stopifnot(identical(arxiv_row$ref_id[[1]], "arxiv:2501.12345"))
stopifnot(identical(arxiv_row$source_id[[1]], "2501.12345"))
stopifnot(identical(arxiv_row$arxiv_id_versioned[[1]], "2501.12345v2"))
stopifnot(identical(arxiv_row$arxiv_id_base[[1]], "2501.12345"))
stopifnot(identical(arxiv_row$title[[1]], "Example arXiv Paper"))
stopifnot(identical(arxiv_row$authors[[1]], "Jane Doe; John Smith"))
stopifnot(identical(arxiv_row$arxiv_primary_category[[1]], "cs.AI"))
stopifnot(identical(arxiv_row$doi[[1]], "10.1000/arxiv-example"))
stopifnot(identical(arxiv_row$entry_type[[1]], "unpublished"))
stopifnot(identical(arxiv_row$url[[1]], "http://arxiv.org/abs/2501.12345v2"))

arxiv_row_v1 <- data.table::copy(arxiv_row)
arxiv_row_v1$raw_entry <- list(NULL)
arxiv_row_v1$title[[1]] <- "Example arXiv Paper v1"
arxiv_row_v1$arxiv_version[[1]] <- 1L
arxiv_row_v1$arxiv_id_versioned[[1]] <- "2501.12345v1"
arxiv_row_v1$url[[1]] <- "http://arxiv.org/abs/2501.12345v1"
arxiv_row_v1$url_landing[[1]] <- "http://arxiv.org/abs/2501.12345v1"

merged_arxiv_versions <- litxr:::.litxr_upsert_records(
  existing = arxiv_row,
  incoming = arxiv_row_v1,
  conflict_path = tempfile("litxr-arxiv-conflicts-", fileext = ".jsonl")
)
stopifnot(nrow(merged_arxiv_versions) == 1L)
stopifnot(identical(merged_arxiv_versions$arxiv_version[[1]], 2L))
stopifnot(identical(merged_arxiv_versions$arxiv_id_versioned[[1]], "2501.12345v2"))

legacy_arxiv_row <- data.table::copy(arxiv_row_v1)
legacy_arxiv_row$source_id[[1]] <- "2501.12345v1"
legacy_arxiv_row$ref_id[[1]] <- "arxiv:2501.12345v1"
legacy_arxiv_row$arxiv_id_base[[1]] <- NA_character_

merged_arxiv_legacy <- litxr:::.litxr_upsert_records(
  existing = legacy_arxiv_row,
  incoming = arxiv_row,
  conflict_path = tempfile("litxr-arxiv-conflicts-", fileext = ".jsonl")
)
stopifnot(nrow(merged_arxiv_legacy) == 1L)
stopifnot(identical(merged_arxiv_legacy$ref_id[[1]], "arxiv:2501.12345"))
stopifnot(identical(merged_arxiv_legacy$source_id[[1]], "2501.12345"))
stopifnot(identical(merged_arxiv_legacy$arxiv_version[[1]], 2L))

td_arxiv <- tempfile("litxr-arxiv-")
dir.create(td_arxiv)
journal_arxiv <- list(
  journal_id = "arxiv_cs_ai",
  title = "arXiv cs.AI",
  remote_channel = "arxiv"
)
litxr:::.litxr_write_journal_records(arxiv_row, td_arxiv, journal_arxiv)
stopifnot(length(list.files(file.path(td_arxiv, "json"), pattern = "\\.json$")) == 1L)

body_text <- paste(deparse(body(get("fetch_arxiv_xml", envir = asNamespace("litxr")))), collapse = "\n")
stopifnot(grepl("start = as.integer\\(start\\)", body_text))

arxiv_query <- litxr:::.litxr_build_arxiv_search_query(
  "cat:cs.AI",
  submitted_from = "2026-01-01",
  submitted_to = "2026-01-31"
)
stopifnot(identical(
  arxiv_query,
  "(cat:cs.AI) AND submittedDate:[202601010000 TO 202601312359]"
))

bib_book <- litxr::row_to_bibtex(data.table::data.table(
  ref_id = "book:test",
  source = "book",
  source_id = "booktest",
  entry_type = "book",
  title = "Example Book",
  authors = "Jane Doe",
  authors_list = list("Jane Doe"),
  year = 2024L,
  month = 1L,
  day = 1L,
  journal = NA_character_,
  container_title = NA_character_,
  publisher = "Example Press",
  volume = NA_character_,
  issue = NA_character_,
  pages = NA_character_,
  doi = NA_character_,
  isbn = "9780262046305",
  issn = NA_character_,
  url = "https://example.org/book",
  note = NA_character_,
  url_landing = NA_character_,
  url_pdf = NA_character_
))
stopifnot(identical(bib_book[[1]], "@book{booktest,"))
stopifnot(any(grepl("isbn = \\{9780262046305\\}", bib_book)))
