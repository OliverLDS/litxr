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
