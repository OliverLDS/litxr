#' Parse one Crossref work into the unified litxr schema
#'
#' @param cr_message One Crossref `message` object.
#'
#' @return One-row `data.table`.
#' @export
parse_crossref_entry_unified <- function(cr_message) {
  # Safe scalar getter for character-ish things
  getf_chr <- function(x) {
    if (is.null(x) || length(x) == 0) return(NA_character_)
    x1 <- x[1]
    if (is.na(x1) || identical(x1, "")) NA_character_ else as.character(x1)
  }

  # --- Title ---
  title <- getf_chr(cr_message$title)

  # --- Authors (cr_message$author is a data.frame) ---
  authors_vec <- character(0)
  if (!is.null(cr_message$author) && nrow(cr_message$author) > 0) {
    adf <- cr_message$author
    authors_vec <- vapply(
      seq_len(nrow(adf)),
      function(i) {
        given  <- getf_chr(adf$given[i])
        family <- getf_chr(adf$family[i])
        parts  <- c(given, family)
        parts  <- parts[!is.na(parts) & nzchar(parts)]
        if (!length(parts)) "" else paste(parts, collapse = " ")
      },
      FUN.VALUE = character(1)
    )
    # remove empty
    authors_vec <- authors_vec[nzchar(authors_vec)]
  }
  authors_str <- if (length(authors_vec)) paste(authors_vec, collapse = "; ") else ""

  # --- Publication date selection ---
  extract_date <- function(x) {
    if (is.null(x) || is.null(x$`date-parts`)) return(NA)
    dp <- x$`date-parts`
    if (length(dp) < 1) return(NA)
    year  <- dp[1,1]
    month <- if (length(dp) >= 2) dp[1,2] else 1
    day   <- if (length(dp) >= 3) dp[1,3] else 1
    as.POSIXct(sprintf("%04d-%02d-%02d", year, month, day), tz = "UTC")
  }

  pub_date <- extract_date(cr_message$`published-print`)
  if (is.na(pub_date)) pub_date <- extract_date(cr_message$`published-online`)
  if (is.na(pub_date)) pub_date <- extract_date(cr_message$issued)
  if (is.na(pub_date)) pub_date <- extract_date(cr_message$created)

  year  <- if (!is.na(pub_date)) as.integer(format(pub_date, "%Y")) else NA_integer_
  month <- if (!is.na(pub_date)) as.integer(format(pub_date, "%m")) else NA_integer_
  day   <- if (!is.na(pub_date)) as.integer(format(pub_date, "%d")) else NA_integer_

  # --- Journal + biblio ---
  journal <- getf_chr(cr_message$`container-title`)
  publisher <- getf_chr(cr_message$publisher)
  volume  <- getf_chr(cr_message$volume)
  issue   <- getf_chr(cr_message[['issue']])
  pages   <- getf_chr(cr_message$page)

  # --- DOI + URLs ---
  doi <- getf_chr(cr_message$DOI)

  # landing: prefer CrossRef's URL, then DOI resolver
  url_landing <- getf_chr(cr_message$URL)
  if (is.na(url_landing) && !is.na(doi)) {
    url_landing <- paste0("https://doi.org/", doi)
  }

  # pdf: look at link$URL if present
  url_pdf <- NA_character_
  if (!is.null(cr_message$link) && nrow(cr_message$link) > 0) {
    url_pdf <- getf_chr(cr_message$link$URL)
  }

  # --- Abstract ---
  abstract <- getf_chr(cr_message$abstract)

  # --- Subjects (if any) ---
  subject_primary <- NA_character_
  subject_all     <- ""
  if (!is.null(cr_message$subject) && length(cr_message$subject) > 0) {
    subject_primary <- getf_chr(cr_message$subject)
    subject_all     <- paste(unique(cr_message$subject), collapse = "; ")
  }

  # --- Arxiv-only fields: NA here ---
  arxiv_version          <- NA_integer_
  arxiv_primary_category <- NA_character_
  arxiv_categories_raw   <- ""
  arxiv_comment          <- NA_character_
  arxiv_journal_ref      <- NA_character_

  data.table::data.table(
    ref_id      = paste0("doi:", doi),
    source      = "crossref",
    source_id   = doi,
    title       = title,
    abstract    = abstract,
    authors     = authors_str,
    authors_list = list(authors_vec),
    pub_date    = pub_date,
    year        = year,
    month       = month,
    day         = day,
    journal     = journal,
    container_title = journal,
    publisher   = publisher,
    volume      = volume,
    issue       = issue,
    pages       = pages,
    doi         = doi,
    note        = NA_character_,
    subject_primary = subject_primary,
    subject_all     = subject_all,
    url_landing     = url_landing,
    url_pdf         = url_pdf,
    arxiv_version          = arxiv_version,
    arxiv_primary_category = arxiv_primary_category,
    arxiv_categories_raw   = arxiv_categories_raw,
    arxiv_comment          = arxiv_comment,
    arxiv_journal_ref      = arxiv_journal_ref,
    raw_entry              = list(cr_message)
  )
}
