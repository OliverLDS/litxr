test_that("latest arXiv metadata sync updates only higher remote versions", {
  local_version <- function(version, versioned_id) {
    litxr:::.litxr_arxiv_version_value(
      version = version,
      versioned_id = versioned_id
    )
  }

  expect_equal(local_version(1L, "2501.12345v1"), 1L)
  expect_equal(local_version(3L, "2501.12345v3"), 3L)
  expect_true(3L > local_version(1L, "2501.12345v1"))
  expect_false(1L > local_version(3L, "2501.12345v3"))
  expect_false(3L > local_version(3L, "2501.12345v3"))
})
