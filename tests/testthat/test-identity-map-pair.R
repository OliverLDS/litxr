test_that("litxr_add_ref_identity_pair appends strict unique pairs", {
  td <- tempfile("litxr-identity-pair-")
  dir.create(td)

  old_litxr_config <- Sys.getenv("LITXR_DATA_ROOT", unset = NA_character_)
  Sys.setenv(LITXR_DATA_ROOT = td)
  on.exit({
    if (is.na(old_litxr_config)) {
      Sys.unsetenv("LITXR_DATA_ROOT")
    } else {
      Sys.setenv(LITXR_DATA_ROOT = old_litxr_config)
    }
  }, add = TRUE)

  cfg_path <- litxr::litxr_init()
  expect_true(file.exists(cfg_path))

  result <- litxr::litxr_add_ref_identity_pair(
    arxiv_ref_id = "2501.00001",
    doi = "10.1000/example",
    config = cfg_path
  )
  expect_identical(result$status, "ok")

  identity_map <- data.table::as.data.table(litxr::litxr_read_ref_identity_map(cfg_path))
  expect_true(any(identity_map$arxiv_id == "2501.00001" & identity_map$doi == "10.1000/example"))

  expect_error(
    litxr::litxr_add_ref_identity_pair(
      arxiv_ref_id = "2501.00001",
      doi = "10.1000/another",
      config = cfg_path
    ),
    "arXiv id already exists"
  )
  expect_error(
    litxr::litxr_add_ref_identity_pair(
      arxiv_ref_id = "2501.00002",
      doi = "10.1000/example",
      config = cfg_path
    ),
    "DOI already exists"
  )
})
