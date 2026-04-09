#' @noRd
.litxr_parse_retry_after <- function(value) {
  if (is.null(value) || !length(value)) return(NA_real_)
  value <- trimws(as.character(value[[1]]))
  if (!nzchar(value)) return(NA_real_)

  numeric_value <- suppressWarnings(as.numeric(value))
  if (!is.na(numeric_value) && is.finite(numeric_value) && numeric_value >= 0) {
    return(numeric_value)
  }

  parsed_time <- suppressWarnings(as.POSIXct(value, tz = "UTC", format = "%a, %d %b %Y %H:%M:%S GMT"))
  if (is.na(parsed_time)) return(NA_real_)

  wait_seconds <- as.numeric(difftime(parsed_time, Sys.time(), units = "secs"))
  if (!is.finite(wait_seconds)) return(NA_real_)
  max(wait_seconds, 0)
}

#' Fetch arXiv API XML
#'
#' @param id_vec Optional arXiv ids.
#' @param search_query Optional arXiv search query.
#' @param start Result offset.
#' @param max_results Maximum number of records to request.
#' @param retry_max Maximum number of retries after rate limiting or transient
#'   request failure.
#' @param retry_backoff_seconds Base backoff in seconds for arXiv rate limiting.
#'
#' @return XML document.
#' @export
fetch_arxiv_xml <- function(
  id_vec = NULL,
  search_query = NULL,
  start = NULL,
  max_results = 100L,
  retry_max = 6L,
  retry_backoff_seconds = 30
) {
  req <- httr2::request("http://export.arxiv.org/api/query")
  req <- httr2::req_error(req, is_error = function(resp) FALSE)

  if (!is.null(id_vec) && length(id_vec)) {
    req <- httr2::req_url_query(req, id_list = paste(id_vec, collapse = ","))
  }

  if (!is.null(start)) {
    req <- httr2::req_url_query(req, start = as.integer(start))
  }

  if (!is.null(search_query) && nzchar(search_query)) {
    req <- httr2::req_url_query(req, search_query = search_query, max_results = as.integer(max_results))
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

      if (attempt >= retry_max) {
        stop(sprintf("arXiv API request failed with HTTP %s after %s attempts.", status, attempt), call. = FALSE)
      }

      retry_after <- .litxr_parse_retry_after(httr2::resp_header(resp, "retry-after"))
      wait_seconds <- if (identical(status, 429L)) {
        if (!is.na(retry_after)) retry_after else retry_backoff_seconds * attempt
      } else {
        max(5, 5 * attempt)
      }

      Sys.sleep(wait_seconds)
      attempt <- attempt + 1L
      next
    }

    if (attempt >= retry_max) {
      stop(resp)
    }

    msg <- conditionMessage(resp)
    if (grepl("429", msg, fixed = TRUE)) {
      Sys.sleep(retry_backoff_seconds * attempt)
    } else {
      Sys.sleep(max(5, 5 * attempt))
    }
    attempt <- attempt + 1L
  }

  httr2::resp_body_xml(resp)
}
