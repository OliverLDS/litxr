test_that("read_bibtex_entries extracts canonical ref_ids in priority order", {
  bib_path <- tempfile(fileext = ".bib")
  writeLines(
    c(
      "@article{arxiv_entry,",
      "  title = {ArXiv First},",
      "  eprint = {2507.22064v2},",
      "  archivePrefix = {arXiv},",
      "  primaryClass = {cs.AI}",
      "}",
      "",
      "@article{doi_entry,",
      "  title = {DOI Second},",
      "  doi = {10.1016/j.jfineco.2026.104308}",
      "}",
      "",
      "@book{isbn_entry,",
      "  title = {ISBN Third},",
      "  isbn = {978-1-4028-9462-6}",
      "}",
      "",
      "@misc{skip_entry,",
      "  title = {Nothing useful here}",
      "}"
    ),
    bib_path
  )

  expect_identical(
    litxr::read_bibtex_entries(bib_path),
    c(
      "arxiv:2507.22064",
      "doi:10.1016/j.jfineco.2026.104308",
      "isbn:9781402894626"
    )
  )
})

test_that("read_bibtex_entries recognizes arXiv DOI references as arXiv ids", {
  bib_path <- tempfile(fileext = ".bib")
  writeLines(
    c(
      "@article{arxiv_doi_entry,",
      "  title = {ArXiv DOI},",
      "  doi = {10.48550/arXiv.2303.09664}",
      "}"
    ),
    bib_path
  )

  expect_identical(
    litxr::read_bibtex_entries(bib_path),
    "arxiv:2303.09664"
  )
})

test_that("read_bibtex_entries recognizes arXiv URLs in url fields as arXiv ids", {
  bib_path <- tempfile(fileext = ".bib")
  writeLines(
    c(
      "@article{arxiv_url_entry,",
      "  title = {ArXiv URL},",
      "  url = {https://arxiv.org/abs/2507.22064v1}",
      "}"
    ),
    bib_path
  )

  expect_identical(
    litxr::read_bibtex_entries(bib_path),
    "arxiv:2507.22064"
  )
})

test_that("read_bibtex_entries prefers linked arXiv ids for DOI entries by default", {
  cfg <- litxr::litxr_read_config()
  imap <- data.table::as.data.table(litxr::litxr_read_ref_identity_map(cfg))
  imap <- imap[!is.na(arxiv_id) & nzchar(arxiv_id) & !is.na(doi) & nzchar(doi), ]
  skip_if_not(nrow(imap) > 0L)
  pair <- imap[1L, ]

  bib_path <- tempfile(fileext = ".bib")
  writeLines(
    c(
      "@article{linked_doi_entry,",
      "  title = {Linked DOI},",
      sprintf("  doi = {%s}", pair$doi[[1L]]),
      "}"
    ),
    bib_path
  )

  expect_identical(
    litxr::read_bibtex_entries(bib_path, config = cfg),
    paste0("arxiv:", as.character(pair$arxiv_id[[1L]]))
  )

  expect_identical(
    litxr::read_bibtex_entries(bib_path, config = cfg, prefer_linked_arxiv = FALSE),
    paste0("doi:", pair$doi[[1L]])
  )
})
