#' @noRd
.litxr_parse_retry_after <- function(value, now = Sys.time()) {
  if (is.null(value) || !length(value)) return(NA_real_)
  value <- trimws(as.character(value[[1]]))
  if (!nzchar(value)) return(NA_real_)

  numeric_value <- suppressWarnings(as.numeric(value))
  if (!is.na(numeric_value) && is.finite(numeric_value) && numeric_value >= 0) {
    return(numeric_value)
  }

  parsed_time <- suppressWarnings(as.POSIXct(value, tz = "UTC", format = "%a, %d %b %Y %H:%M:%S GMT"))
  if (is.na(parsed_time)) return(NA_real_)

  wait_seconds <- as.numeric(difftime(parsed_time, now, units = "secs"))
  if (!is.finite(wait_seconds)) return(NA_real_)
  max(wait_seconds, 0)
}

.litxr_arxiv_api_cooldown_path <- function(cfg) {
  file.path(.litxr_project_log_dir(cfg), "arxiv_api_cooldown.json")
}

.litxr_arxiv_retryable_status <- function(status) {
  as.integer(status) %in% c(429L, 502L, 503L, 504L)
}

.litxr_arxiv_retry_wait_seconds <- function(
  status,
  attempt,
  retry_after = NA_real_,
  fallback_seconds = 60,
  max_wait_seconds = 900,
  jitter = stats::runif
) {
  if (!is.na(retry_after)) return(as.numeric(retry_after))

  attempt <- max(1L, as.integer(attempt))
  base_wait <- min(as.numeric(max_wait_seconds), max(60, as.numeric(fallback_seconds)) * 2 ^ (attempt - 1L))
  jitter_ceiling <- min(60, max(1, base_wait * 0.1))
  min(as.numeric(max_wait_seconds), base_wait + jitter(1L, min = 0, max = jitter_ceiling))
}

.litxr_read_arxiv_api_cooldown <- function(path, now = Sys.time()) {
  if (is.null(path) || !nzchar(path) || !file.exists(path)) return(NULL)
  state <- tryCatch(jsonlite::fromJSON(path, simplifyVector = FALSE), error = function(e) NULL)
  if (!is.list(state) || is.null(state$next_retry_at) || is.null(state$status)) return(NULL)
  next_retry_at <- suppressWarnings(as.POSIXct(as.character(state$next_retry_at[[1L]]), tz = "UTC"))
  if (is.na(next_retry_at) || next_retry_at <= now) return(NULL)
  list(
    status = as.integer(state$status[[1L]]),
    next_retry_at = next_retry_at,
    seconds_remaining = as.numeric(difftime(next_retry_at, now, units = "secs"))
  )
}

.litxr_assert_arxiv_api_ready <- function(cooldown_path, now = Sys.time()) {
  cooldown <- .litxr_read_arxiv_api_cooldown(cooldown_path, now = now)
  if (is.null(cooldown)) return(invisible(NULL))
  stop(
    "arXiv API cooldown is active after HTTP ", cooldown$status,
    "; next_retry_at=", format(cooldown$next_retry_at, tz = "UTC", usetz = TRUE),
    ". Retry after that time.",
    call. = FALSE
  )
}

.litxr_write_arxiv_api_cooldown <- function(cooldown_path, status, wait_seconds, now = Sys.time()) {
  if (is.null(cooldown_path) || !nzchar(cooldown_path)) return(invisible(NULL))
  next_retry_at <- now + as.numeric(wait_seconds)
  .litxr_write_json_atomic(
    list(
      status = as.integer(status),
      next_retry_at = format(next_retry_at, tz = "UTC", usetz = TRUE),
      recorded_at = format(now, tz = "UTC", usetz = TRUE)
    ),
    cooldown_path
  )
  invisible(next_retry_at)
}

#' Fetch arXiv API XML
#'
#' @param id_vec Optional arXiv ids.
#' @param search_query Optional arXiv search query.
#' @param start Result offset.
#' @param max_results Maximum number of records to request.
#' @param sort_by Optional stable arXiv sort field for search queries.
#' @param sort_order Optional sort order for `sort_by`.
#' @param retry_max Maximum number of retries after rate limiting or transient
#'   request failure.
#' @param retry_backoff_seconds Base backoff in seconds for arXiv rate limiting.
#' @param cooldown_path Optional persistent endpoint-cooldown state path.
#'
#' @return XML document.
#' @export
fetch_arxiv_xml <- function(
  id_vec = NULL,
  search_query = NULL,
  start = NULL,
  max_results = 100L,
  sort_by = NULL,
  sort_order = NULL,
  retry_max = 6L,
  retry_backoff_seconds = 60,
  cooldown_path = NULL
) {
  retry_max <- max(1L, as.integer(retry_max))
  .litxr_assert_arxiv_api_ready(cooldown_path)
  req <- httr2::request("https://export.arxiv.org/api/query")
  req <- httr2::req_error(req, is_error = function(resp) FALSE)

  if (!is.null(id_vec) && length(id_vec)) {
    req <- httr2::req_url_query(req, id_list = paste(id_vec, collapse = ","))
  }

  if (!is.null(start)) {
    req <- httr2::req_url_query(req, start = as.integer(start))
  }

  if (!is.null(search_query) && nzchar(search_query)) {
    req <- httr2::req_url_query(req, search_query = search_query, max_results = as.integer(max_results))
    if (!is.null(sort_by) && length(sort_by) && nzchar(sort_by[[1L]])) {
      req <- httr2::req_url_query(req, sortBy = as.character(sort_by[[1L]]))
    }
    if (!is.null(sort_order) && length(sort_order) && nzchar(sort_order[[1L]])) {
      req <- httr2::req_url_query(req, sortOrder = as.character(sort_order[[1L]]))
    }
  }

  resp <- NULL
  attempt <- 1L

  repeat {
    resp <- tryCatch(
      httr2::req_perform(req),
      error = function(e) e
    )

    if (!inherits(resp, "error")) {
      status <- httr2::resp_status(resp)
      if (status >= 200L && status < 300L) {
        break
      }

      if (!.litxr_arxiv_retryable_status(status)) {
        stop(sprintf("arXiv API request failed with HTTP %s.", status), call. = FALSE)
      }

      retry_after <- .litxr_parse_retry_after(httr2::resp_header(resp, "retry-after"))
      wait_seconds <- .litxr_arxiv_retry_wait_seconds(
        status = status,
        attempt = attempt,
        retry_after = retry_after,
        fallback_seconds = retry_backoff_seconds
      )
      cooldown_next_retry_at <- NULL
      if (identical(status, 429L)) {
        cooldown_next_retry_at <- .litxr_write_arxiv_api_cooldown(cooldown_path, status, wait_seconds)
      }
      if (attempt >= retry_max) {
        cooldown_suffix <- if (is.null(cooldown_next_retry_at)) "" else paste0(
          "; next_retry_at=", format(cooldown_next_retry_at, tz = "UTC", usetz = TRUE)
        )
        stop(sprintf("arXiv API request failed with HTTP %s after %s attempts%s.", status, attempt, cooldown_suffix), call. = FALSE)
      }
      Sys.sleep(wait_seconds)
      attempt <- attempt + 1L
      next
    }

    if (attempt >= retry_max) {
      stop(resp)
    }

    Sys.sleep(.litxr_arxiv_retry_wait_seconds(
      status = NA_integer_,
      attempt = attempt,
      fallback_seconds = retry_backoff_seconds
    ))
    attempt <- attempt + 1L
  }

  httr2::resp_body_xml(resp)
}
