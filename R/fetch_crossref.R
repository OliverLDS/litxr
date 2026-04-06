#' Fetch Crossref records by DOI
#'
#' @param dois Character vector of DOIs.
#'
#' @return List of Crossref `message` objects.
#' @export
fetch_crossref_messages <- function(dois) {
  dois <- unique(as.character(dois))
  stats::setNames(lapply(dois, function(doi) {
    req <- httr2::request(paste0("https://api.crossref.org/works/", utils::URLencode(doi, reserved = TRUE))) |>
      httr2::req_error(is_error = function(resp) FALSE)

    resp <- httr2::req_perform(req)
    if (httr2::resp_status(resp) >= 400) {
      return(NULL)
    }

    payload <- httr2::resp_body_json(resp, simplifyVector = FALSE)
    payload$message
  }), dois)
}

#' Fetch Crossref works
#'
#' @param filters Named list of Crossref filters.
#' @param query Optional free-text query.
#' @param limit Maximum number of items to return.
#'
#' @return List of Crossref items.
#' @export
fetch_crossref_works <- function(filters = NULL, query = NULL, limit = 100L) {
  req <- httr2::request("https://api.crossref.org/works")
  filter_string <- .compact_filter_string(filters)

  if (!is.null(filter_string) && nzchar(filter_string)) {
    req <- httr2::req_url_query(req, filter = filter_string)
  }

  if (!is.null(query) && nzchar(query)) {
    req <- httr2::req_url_query(req, query = query)
  }

  resp <- req |>
    httr2::req_url_query(rows = as.integer(limit)) |>
    httr2::req_perform()

  payload <- httr2::resp_body_json(resp, simplifyVector = FALSE)
  payload$message$items
}

#' Fetch all works for one Crossref journal using cursor pagination
#'
#' @param issn Journal ISSN.
#' @param limit Maximum number of items to return. Use `Inf` for a full harvest.
#' @param rows Page size per API request.
#'
#' @return List of Crossref items.
#' @export
fetch_crossref_journal_works <- function(issn, limit = Inf, rows = 1000L) {
  cursor <- "*"
  out <- list()
  total <- 0L

  repeat {
    resp <- httr2::request(sprintf("https://api.crossref.org/journals/%s/works", issn)) |>
      httr2::req_url_query(rows = as.integer(rows), cursor = cursor) |>
      httr2::req_perform()

    payload <- httr2::resp_body_json(resp, simplifyVector = FALSE)
    items <- payload$message$items

    if (!length(items)) {
      break
    }

    out <- c(out, items)
    total <- total + length(items)

    if (is.finite(limit) && total >= limit) {
      out <- out[seq_len(limit)]
      break
    }

    if (length(items) < rows) {
      break
    }

    cursor <- payload$message$`next-cursor`
    if (is.null(cursor) || !nzchar(cursor)) {
      break
    }
  }

  out
}
