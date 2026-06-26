test_that("exact-path hydration handles missing, partial, and duplicate paths", {
  td <- tempfile("litxr-json-hydration-")
  dir.create(td)

  path_a <- file.path(td, "arxiv_2501_00001.json")
  path_b <- file.path(td, "arxiv_2501_00002.json")
  jsonlite::write_json(
    list(
      ref_id = "arxiv:2501.00001",
      doi = NULL,
      abstract = "Complete abstract",
      authors_list = list(c("Jane Doe", "John Smith"))
    ),
    path_a,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )
  jsonlite::write_json(
    list(
      ref_id = "arxiv:2501.00002",
      doi = NULL,
      abstract = "Partial abstract"
    ),
    path_b,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )

  rows <- data.table::data.table(
    ref_id = c("arxiv:2501.00001", "arxiv:2501.00001", "arxiv:2501.00002", "arxiv:2501.00003")
  )
  json_paths <- data.table::data.table(
    ref_id = c("arxiv:2501.00001", "arxiv:2501.00001", "arxiv:2501.00002"),
    json_path = c(path_a, path_a, path_b)
  )
  hydrated <- litxr:::.litxr_hydrate_rows_from_json_paths(rows, json_paths)

  expect_equal(nrow(hydrated), 4L)
  expect_true("abstract" %in% names(hydrated))
  expect_true("authors_list" %in% names(hydrated))
  expect_identical(hydrated$abstract[[1]], "Complete abstract")
  expect_identical(hydrated$abstract[[2]], "Complete abstract")
  expect_identical(hydrated$abstract[[3]], "Partial abstract")
  expect_true(is.na(hydrated$abstract[[4]]))
  expect_identical(hydrated$authors_list[[1]], c("Jane Doe", "John Smith"))
  expect_identical(hydrated$authors_list[[2]], c("Jane Doe", "John Smith"))
  expect_true(is.null(hydrated$authors_list[[3]]) || length(hydrated$authors_list[[3]]) == 0L)
})

test_that("exact-path hydration prefers the first duplicate mapping", {
  td <- tempfile("litxr-dup-path-")
  dir.create(td)

  path_a <- file.path(td, "arxiv_2501_00001_a.json")
  path_b <- file.path(td, "arxiv_2501_00001_b.json")
  jsonlite::write_json(
    list(ref_id = "arxiv:2501.00001", abstract = "First exact path"),
    path_a,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )
  jsonlite::write_json(
    list(ref_id = "arxiv:2501.00001", abstract = "Second exact path"),
    path_b,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )

  rows <- data.table::data.table(ref_id = "arxiv:2501.00001")
  json_paths <- data.table::data.table(
    ref_id = c("arxiv:2501.00001", "arxiv:2501.00001"),
    json_path = c(path_a, path_b)
  )
  hydrated <- litxr:::.litxr_hydrate_rows_from_json_paths(rows, json_paths, fields = "abstract")

  expect_equal(nrow(hydrated), 1L)
  expect_identical(hydrated$abstract[[1]], "First exact path")
})

test_that("wide projection guardrails still apply", {
  td <- tempfile("litxr-multi-collection-")
  dir.create(td)
  config_path <- file.path(td, "config.yaml")

  old_litxr_config <- Sys.getenv("LITXR_DATA_ROOT", unset = NA_character_)
  Sys.setenv(LITXR_DATA_ROOT = dirname(config_path))
  on.exit({
    if (is.na(old_litxr_config)) {
      Sys.unsetenv("LITXR_DATA_ROOT")
    } else {
      Sys.setenv(LITXR_DATA_ROOT = old_litxr_config)
    }
  }, add = TRUE)

  litxr::litxr_init()

  expect_error(
    litxr:::.litxr_hydrate_project_projection_rows(
      litxr::litxr_read_config(),
      data.table::data.table(ref_id = c("arxiv:2501.99998", "arxiv:2501.99999")),
      wide_projection_limit = 1L
    ),
    "wide-projection limit"
  )
})
