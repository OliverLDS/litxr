#' @export
fetch_arxiv_xml <- function(id_vec) {
  id_str <- paste(id_vec, collapse = ",")
  
  resp <- httr2::request("http://export.arxiv.org/api/query") |>
    httr2::req_url_query(id_list = id_str) |>
    httr2::req_perform()
  
  httr2::resp_body_xml(resp)
}