#' @export
bibtex_escape <- function(x) {
  if (is.na(x) || x == "") return("")
  x <- gsub("\\\\", "\\\\textbackslash{}", x)
  x <- gsub("\\{", "\\\\{", x)
  x <- gsub("\\}", "\\\\}", x)
  x <- gsub("\"", "''", x)
  x
}

#' @export
make_citekey <- function(row) {
  doi <- row[["doi"]]
  if (!is.na(doi) && nzchar(doi)) {
    core <- sub(".*/", "", doi)           # last part of DOI
  } else if (!is.null(row[["source_id"]]) && nzchar(row[["source_id"]])) {
    core <- row[["source_id"]]
  } else {
    core <- row[["ref_id"]]
  }
  core <- gsub("[^A-Za-z0-9]+", "_", core)
  core <- gsub("^_+|_+$", "", core)
  if (!nzchar(core)) core <- "ref"
  core
}

#' @export
format_authors_bib <- function(row) {
  a <- row[["authors_list"]][[1]]
  if (length(a) == 0L || all(is.na(a))) return("")
  paste(a, collapse = " and ")
}

#' @export
row_to_bibtex <- function(row) {
  # decide type
  has_journal <- !is.na(row[["journal"]]) && nzchar(row[["journal"]])
  is_arxiv    <- identical(row[["source"]], "arxiv")
  
  entry_type <- if (has_journal) "article" else "article"
  
  key   <- make_citekey(row)
  title <- bibtex_escape(row[["title"]])
  year  <- if (!is.na(row[["year"]])) as.character(row[["year"]]) else ""
  auth  <- bibtex_escape(format_authors_bib(row))
  
  journal <- if (has_journal) bibtex_escape(row[["journal"]]) else ""
  volume  <- if (!is.na(row[["volume"]])) as.character(row[["volume"]]) else ""
  number  <- if (!is.na(row[["issue"]]))  as.character(row[["issue"]])  else ""
  pages   <- if (!is.na(row[["pages"]]))  as.character(row[["pages"]])  else ""
  doi     <- if (!is.na(row[["doi"]]))    row[["doi"]]                 else ""
  
  url <- row[["url_landing"]]
  if (is.na(url) || !nzchar(url)) {
    url <- row[["url_pdf"]]
  }
  url <- if (!is.na(url)) url else ""
  
  lines <- c(
    sprintf("@%s{%s,", entry_type, key),
    if (nzchar(title))  sprintf("  title   = {%s},", title)  else NULL,
    if (nzchar(auth))   sprintf("  author  = {%s},", auth)   else NULL,
    if (nzchar(journal))sprintf("  journal = {%s},", journal)else NULL,
    if (nzchar(year))   sprintf("  year    = {%s},", year)   else NULL,
    if (nzchar(volume)) sprintf("  volume  = {%s},", volume) else NULL,
    if (nzchar(number)) sprintf("  number  = {%s},", number) else NULL,
    if (nzchar(pages))  sprintf("  pages   = {%s},", pages)  else NULL,
    if (nzchar(doi))    sprintf("  doi     = {%s},", doi)    else NULL,
    if (nzchar(url))    sprintf("  url     = {%s},", url)    else NULL
  )
  
  # Add arXiv-specific minimal fields if applicable
  if (is_arxiv) {
    # strip version for eprint
    sid <- row[["source_id"]]
    eprint <- sub("v[0-9]+$", "", sid)
    primary <- row[["arxiv_primary_category"]]
    if (is.na(primary) || !nzchar(primary)) {
      primary <- row[["subject_primary"]]
    }
    if (nzchar(eprint))  lines <- c(lines, sprintf("  eprint        = {%s},", eprint))
    lines <- c(lines, "  archivePrefix = {arXiv},")
    if (!is.na(primary) && nzchar(primary)) {
      lines <- c(lines, sprintf("  primaryClass  = {%s},", primary))
    }
  }
  
  # close entry, trim trailing comma if you want to be strict (not strictly required)
  if (length(lines) > 1) {
    lines[length(lines)] <- sub(",$", "", lines[length(lines)])
  }
  c(lines, "}")
}