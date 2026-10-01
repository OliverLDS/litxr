test_that("Retry-After accepts numeric seconds and HTTP dates", {
  now <- as.POSIXct("2026-09-15 00:00:00", tz = "UTC")
  expect_equal(litxr:::.litxr_parse_retry_after("120", now = now), 120)
  expect_equal(
    litxr:::.litxr_parse_retry_after("Tue, 15 Sep 2026 00:02:00 GMT", now = now),
    120
  )
})

test_that("429 and 503 use conservative exponential retry waits", {
  no_jitter <- function(n, min, max) rep(0, n)
  expect_true(litxr:::.litxr_arxiv_retryable_status(429L))
  expect_true(litxr:::.litxr_arxiv_retryable_status(503L))
  expect_false(litxr:::.litxr_arxiv_retryable_status(400L))
  expect_equal(
    litxr:::.litxr_arxiv_retry_wait_seconds(429L, 1L, jitter = no_jitter),
    60
  )
  expect_equal(
    litxr:::.litxr_arxiv_retry_wait_seconds(429L, 1L, fallback_seconds = 5, jitter = no_jitter),
    60
  )
  expect_equal(
    litxr:::.litxr_arxiv_retry_wait_seconds(503L, 2L, jitter = no_jitter),
    120
  )
  expect_equal(
    litxr:::.litxr_arxiv_retry_wait_seconds(429L, 1L, retry_after = 17, jitter = no_jitter),
    17
  )
})

test_that("a mocked 503 follows the bounded arXiv retry path", {
  httr2::with_mocked_responses(
    list(httr2::response(status_code = 503L)),
    expect_error(
      litxr::fetch_arxiv_xml(search_query = "cat:cs.AI", retry_max = 1L),
      "HTTP 503 after 1 attempts"
    )
  )
})

test_that("persisted arXiv cooldown blocks until expiry", {
  cooldown_path <- file.path(tempfile("litxr-arxiv-cooldown-"), "cooldown.json")
  now <- as.POSIXct("2026-09-15 00:00:00", tz = "UTC")
  litxr:::.litxr_write_arxiv_api_cooldown(cooldown_path, 429L, 120, now = now)

  expect_error(
    litxr:::.litxr_assert_arxiv_api_ready(cooldown_path, now = now + 60),
    "HTTP 429.*next_retry_at"
  )
  expect_silent(
    litxr:::.litxr_assert_arxiv_api_ready(cooldown_path, now = now + 121)
  )
})

test_that("ChatGPT content-reference fragments are removed before JSON parsing", {
  raw_json <- paste0(
    '{"notes":"Digest extracted from HTML. ',
    ':chatgpt-content-reference{index="0"} Continue.","ref_id":"2609.12132"}'
  )
  cleaned <- litxr:::.litxr_strip_chatgpt_content_references(raw_json)

  expect_false(grepl(":chatgpt-content-reference", cleaned, fixed = TRUE))
  parsed <- jsonlite::fromJSON(cleaned, simplifyVector = FALSE)
  expect_identical(parsed$notes, "Digest extracted from HTML.  Continue.")
})
