#' Fetch arXiv API XML
#'
#' @param id_vec Optional arXiv ids.
#' @param search_query Optional arXiv search query.
#' @param start Result offset.
#' @param max_results Maximum number of records to request.
#' @param retry_max Maximum number of retries after rate limiting or transient
#'   request failure.
#'
#' @return XML document.
#' @export
fetch_arxiv_xml <- function(id_vec = NULL, search_query = NULL, start = NULL, max_results = 100L, retry_max = 3L) {
  req <- httr2::request("http://export.arxiv.org/api/query")

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
      break
    }

    if (attempt >= retry_max) {
      stop(resp)
    }

    msg <- conditionMessage(resp)
    if (grepl("429", msg, fixed = TRUE)) {
      Sys.sleep(5 * attempt)
    } else {
      Sys.sleep(2 * attempt)
    }
    attempt <- attempt + 1L
  }

  httr2::resp_body_xml(resp)
}
