#' Extract text from an XML node or return `NA`
#'
#' @param node XML node.
#' @param xpath XPath expression.
#' @param ns Optional XML namespace mapping.
#'
#' @return Character scalar.
#' @export
xml_text_or_na <- function(node, xpath, ns = NULL) {
  x <- xml2::xml_find_first(node, xpath, ns = ns)
  if (is.na(x)) NA_character_ else xml2::xml_text(x)
}

.litxr_parse_arxiv_posixct <- function(text) {
  if (inherits(text, "Date")) {
    return(as.POSIXct(text[[1L]], tz = "UTC"))
  }
  if (inherits(text, c("POSIXct", "POSIXt"))) {
    return(as.POSIXct(text[[1L]], tz = "UTC"))
  }
  if (is.numeric(text) && length(text) == 1L && is.finite(text[[1L]])) {
    maybe_date <- suppressWarnings(as.Date(text[[1L]], origin = "1970-01-01"))
    if (!is.na(maybe_date)) {
      return(as.POSIXct(maybe_date, tz = "UTC"))
    }
  }
  text <- as.character(text)[[1L]]
  text <- trimws(text)
  if (!nzchar(text) || is.na(text)) {
    return(as.POSIXct(NA, tz = "UTC"))
  }
  if (grepl("^[0-9]+$", text) && nchar(text) <= 7L) {
    maybe_date <- suppressWarnings(as.Date(as.integer(text), origin = "1970-01-01"))
    if (!is.na(maybe_date)) {
      return(as.POSIXct(maybe_date, tz = "UTC"))
    }
  }

  safe_parse <- function(value, format = NULL) {
    tryCatch(
      suppressWarnings(if (is.null(format)) {
        as.POSIXct(value, tz = "UTC")
      } else {
        as.POSIXct(value, format = format, tz = "UTC")
      }),
      error = function(e) as.POSIXct(NA, tz = "UTC")
    )
  }

  normalized <- text
  normalized <- sub("([+-][0-9]{2}):([0-9]{2})$", "\\1\\2", normalized)
  normalized <- sub("\\.([0-9]+)Z$", "+0000", normalized)
  normalized <- sub("Z$", "+0000", normalized)

  candidates <- c(
    normalized,
    text
  )

  for (candidate in unique(candidates)) {
    parsed <- safe_parse(candidate, "%Y-%m-%dT%H:%M:%S%z")
    if (!is.na(parsed)) {
      return(parsed)
    }

    parsed <- safe_parse(candidate, "%Y-%m-%dT%H:%M:%OS%z")
    if (!is.na(parsed)) {
      return(parsed)
    }

    parsed <- safe_parse(candidate, "%Y-%m-%dT%H:%M:%S")
    if (!is.na(parsed)) {
      return(parsed)
    }
  }

  parsed <- safe_parse(text, "%Y-%m-%d")
  if (!is.na(parsed)) {
    return(parsed)
  }

  parsed <- safe_parse(text)
  if (!is.na(parsed)) {
    return(parsed)
  }

  as.POSIXct(NA, tz = "UTC")
}

#' Parse one arXiv entry into the unified litxr schema
#'
#' @param entry One arXiv Atom entry node.
#'
#' @return One-row `data.table`.
#' @export
parse_arxiv_entry_unified <- function(entry) {
  title    <- xml_text_or_na(entry, ".//*[local-name()='title']")
  abstract <- xml_text_or_na(entry, ".//*[local-name()='summary']")
  
  # authors
  auth_nodes <- xml2::xml_find_all(entry, ".//*[local-name()='author']/*[local-name()='name']")
  auth_vec   <- xml2::xml_text(auth_nodes)
  authors_str <- if (length(auth_vec)) paste(auth_vec, collapse = "; ") else ""
  
  # categories
  cat_nodes <- xml2::xml_find_all(entry, ".//*[local-name()='category']")
  cats      <- xml2::xml_attr(cat_nodes, "term")
  arxiv_primary_category <- if (length(cats)) cats[1] else NA_character_
  arxiv_categories_raw   <- if (length(cats)) paste(cats, collapse = " ") else ""
  
  # dates
  published <- xml_text_or_na(entry, ".//*[local-name()='published']")
  pub_date  <- if (is.na(published)) as.POSIXct(NA, tz = "UTC") else .litxr_parse_arxiv_posixct(published)
  year      <- if (is.na(pub_date)) NA_integer_ else as.integer(format(pub_date, "%Y"))
  month     <- if (is.na(pub_date)) NA_integer_ else as.integer(format(pub_date, "%m"))
  day       <- if (is.na(pub_date)) NA_integer_ else as.integer(format(pub_date, "%d"))
  
  # id + links (these work even without ns, but we can keep them consistent)
  id_str   <- xml_text_or_na(entry, ".//*[local-name()='id']")
  source_id_versioned <- sub("^.*/", "", id_str)
  
  m   <- regexec("^([^v]+)v(\\d+)$", source_id_versioned)
  reg <- regmatches(source_id_versioned, m)[[1]]
  source_id <- if (length(reg) == 3) reg[2] else source_id_versioned
  ver <- if (length(reg) == 3) as.integer(reg[3]) else NA_integer_
  
  link_nodes <- xml2::xml_find_all(entry, ".//*[local-name()='link']")
  rels <- xml2::xml_attr(link_nodes, "rel")
  href <- xml2::xml_attr(link_nodes, "href")
  
  url_pdf <- href[rels == "related" | grepl("pdf", href)]
  url_pdf <- if (!length(url_pdf)) NA_character_ else url_pdf[1]
  
  url_landing <- href[rels == "alternate"]
  url_landing <- if (!length(url_landing)) NA_character_ else url_landing[1]
  url <- url_landing
  
  # arXiv-specific
  doi           <- xml_text_or_na(entry, ".//*[local-name()='doi']")
  isbn          <- NA_character_
  issn          <- NA_character_
  arxiv_comment <- xml_text_or_na(entry, ".//*[local-name()='comment']")
  journal_ref   <- xml_text_or_na(entry, ".//*[local-name()='journal_ref']")
  entry_type <- if (!is.na(journal_ref) && nzchar(journal_ref)) "article" else "unpublished"
  
  subject_primary <- arxiv_primary_category
  subject_all     <- arxiv_categories_raw
  
  data.table::data.table(
    ref_id        = paste0("arxiv:", source_id),
    source        = "arxiv",
    source_id     = source_id,
    entry_type    = entry_type,
    
    title         = title,
    abstract      = abstract,
    authors       = authors_str,
    authors_list  = list(auth_vec),
    
    pub_date      = pub_date,
    year          = year,
    month         = month,
    day           = day,
    
    journal       = NA_character_,
    container_title = NA_character_,
    publisher     = NA_character_,
    volume        = NA_character_,
    issue         = NA_character_,
    pages         = NA_character_,
    doi           = doi,
    isbn          = isbn,
    issn          = issn,
    url           = url,
    note          = NA_character_,
    
    subject_primary = subject_primary,
    subject_all     = subject_all,
    
    url_landing   = url_landing,
    url_pdf       = url_pdf,
    
    arxiv_id_versioned      = source_id_versioned,
    arxiv_id_base           = source_id,
    arxiv_version          = ver,
    arxiv_primary_category = arxiv_primary_category,
    arxiv_categories_raw   = arxiv_categories_raw,
    arxiv_comment          = arxiv_comment,
    arxiv_journal_ref      = journal_ref,
    linked_doi_ref_id      = if (!is.na(doi) && nzchar(doi)) paste0("doi:", doi) else NA_character_,
    linked_arxiv_ref_id    = NA_character_,
    
    raw_entry = list(entry)
  )
}
