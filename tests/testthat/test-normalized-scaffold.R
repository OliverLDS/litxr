test_that("normalized scaffold helpers route ids and validate canonical keys", {
  route_arxiv <- litxr:::.litxr_route_ref_id(" 2505.07087v3 ")
  expect_identical(route_arxiv$key_type, "arxiv_id")
  expect_identical(route_arxiv$key_value, "arxiv:2505.07087")
  expect_identical(route_arxiv$ref_id, "arxiv:2505.07087")

  route_doi <- litxr:::.litxr_route_ref_id("https://doi.org/10.1016/j.inffus.2025.103599")
  expect_identical(route_doi$key_type, "doi")
  expect_identical(route_doi$key_value, "doi:10.1016/j.inffus.2025.103599")

  route_pending <- litxr:::.litxr_route_ref_id("unresolved-local-note")
  expect_identical(route_pending$key_type, "local_pending")
  expect_identical(route_pending$key_value, "unresolved-local-note")

  expect_silent(litxr:::.litxr_validate_arxiv_id("arxiv:2505.07087"))
  expect_silent(litxr:::.litxr_validate_doi("doi:10.1016/j.inffus.2025.103599"))
  expect_silent(litxr:::.litxr_validate_entity_id("ent:example"))
  expect_error(litxr:::.litxr_validate_arxiv_id("not-an-arxiv"), "Invalid arXiv id")
  expect_error(litxr:::.litxr_validate_doi("not-a-doi"), "Invalid DOI")
  expect_error(litxr:::.litxr_validate_entity_id(""), "non-empty")
})

test_that("normalized payload projection strips collection metadata and keeps only routed payload rows", {
  records <- data.table::data.table(
    ref_id = c("arxiv:2505.07087", "doi:10.1016/j.inffus.2025.103599", "local-note"),
    title = c("Arxiv Title", "DOI Title", "Pending Note"),
    collection_id = c("arxiv_cs_ai", "information_fusion", "manual"),
    collection_title = c("arXiv cs.AI", "Information Fusion", "Manual"),
    journal = c("arXiv", "Information Fusion", NA_character_)
  )

  arxiv_rows <- litxr:::.litxr_normalized_payload_projection(records, key_type = "arxiv_id")
  expect_true("ref_id" %in% names(arxiv_rows))
  expect_true("key_type" %in% names(arxiv_rows))
  expect_true("key_value" %in% names(arxiv_rows))
  expect_false("collection_id" %in% names(arxiv_rows))
  expect_false("collection_title" %in% names(arxiv_rows))
  expect_identical(arxiv_rows$ref_id, "arxiv:2505.07087")
  expect_identical(arxiv_rows$key_type, "arxiv_id")

  doi_rows <- litxr:::.litxr_normalized_payload_projection(records, key_type = "doi")
  expect_identical(doi_rows$ref_id, "doi:10.1016/j.inffus.2025.103599")
  expect_identical(doi_rows$key_type, "doi")
})
