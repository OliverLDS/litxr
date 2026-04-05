#' Fetch arXiv API XML
#'
#' @param id_vec Optional arXiv ids.
#' @param search_query Optional arXiv search query.
#' @param max_results Maximum number of records to request.
#'
#' @return XML document.
#' @export
fetch_arxiv_xml <- function(id_vec = NULL, search_query = NULL, max_results = 100L) {
  req <- httr2::request("http://export.arxiv.org/api/query")

  if (!is.null(id_vec) && length(id_vec)) {
    req <- httr2::req_url_query(req, id_list = paste(id_vec, collapse = ","))
  }

  if (!is.null(search_query) && nzchar(search_query)) {
    req <- httr2::req_url_query(req, search_query = search_query, max_results = as.integer(max_results))
  }

  resp <- httr2::req_perform(req)
  httr2::resp_body_xml(resp)
}
