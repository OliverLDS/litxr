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
  pub_date  <- if (is.na(published)) NA else as.POSIXct(published, tz = "UTC")
  year      <- if (is.na(pub_date)) NA_integer_ else as.integer(format(pub_date, "%Y"))
  month     <- if (is.na(pub_date)) NA_integer_ else as.integer(format(pub_date, "%m"))
  day       <- if (is.na(pub_date)) NA_integer_ else as.integer(format(pub_date, "%d"))
  
  # id + links (these work even without ns, but we can keep them consistent)
  id_str   <- xml_text_or_na(entry, ".//*[local-name()='id']")
  source_id <- sub("^.*/", "", id_str)
  
  m   <- regexec("^([^v]+)v(\\d+)$", source_id)
  reg <- regmatches(source_id, m)[[1]]
  ver <- if (length(reg) == 3) as.integer(reg[3]) else NA_integer_
  
  link_nodes <- xml2::xml_find_all(entry, ".//*[local-name()='link']")
  rels <- xml2::xml_attr(link_nodes, "rel")
  href <- xml2::xml_attr(link_nodes, "href")
  
  url_pdf <- href[rels == "related" | grepl("pdf", href)]
  url_pdf <- if (!length(url_pdf)) NA_character_ else url_pdf[1]
  
  url_landing <- href[rels == "alternate"]
  url_landing <- if (!length(url_landing)) NA_character_ else url_landing[1]
  
  # arXiv-specific
  doi           <- xml_text_or_na(entry, ".//*[local-name()='doi']")
  arxiv_comment <- xml_text_or_na(entry, ".//*[local-name()='comment']")
  journal_ref   <- xml_text_or_na(entry, ".//*[local-name()='journal_ref']")
  
  subject_primary <- arxiv_primary_category
  subject_all     <- arxiv_categories_raw
  
  data.table::data.table(
    ref_id        = paste0("arxiv:", source_id),
    source        = "arxiv",
    source_id     = source_id,
    
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
    note          = NA_character_,
    
    subject_primary = subject_primary,
    subject_all     = subject_all,
    
    url_landing   = url_landing,
    url_pdf       = url_pdf,
    
    arxiv_version          = ver,
    arxiv_primary_category = arxiv_primary_category,
    arxiv_categories_raw   = arxiv_categories_raw,
    arxiv_comment          = arxiv_comment,
    arxiv_journal_ref      = journal_ref,
    
    raw_entry = list(entry)
  )
}
