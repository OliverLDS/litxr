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
  subject = c("Finance", "Asset Pricing")
)

row <- litxr::parse_crossref_entry_unified(cr_message)
stopifnot(identical(row$ref_id[[1]], "doi:10.1000/testdoi"))
stopifnot(identical(row$title[[1]], "A Test Article"))
stopifnot(identical(row$authors[[1]], "Jane Doe; John Smith"))
stopifnot(identical(row$journal[[1]], "Journal of Finance"))
stopifnot(identical(row$publisher[[1]], "Wiley"))
stopifnot(identical(row$year[[1]], 2024L))

records <- data.table::data.table(
  ref_id = c("doi:10.1000/a", "doi:10.1000/a", "arxiv:1234.5678v1"),
  doi = c("10.1000/a", "10.1000/a", NA_character_),
  title = c("A", "A duplicate", "B")
)

deduped <- litxr:::.litxr_deduplicate_records(records)
stopifnot(nrow(deduped) == 2L)
stopifnot(identical(deduped$title[[1]], "A"))
stopifnot(identical(deduped$title[[2]], "B"))
