.bibtex_escape <- function(x) {
  if (is.na(x) || x == "") return("")
  x <- gsub("\\\\", "\\\\textbackslash{}", x)
  x <- gsub("\\{", "\\\\{", x)
  x <- gsub("\\}", "\\\\}", x)
  x <- gsub("\"", "''", x)
  x
}

.make_citekey <- function(doi, source_id, ref_id) {
  if (!is.na(doi) && nzchar(doi)) {
    core <- sub(".*/", "", doi)           # last part of DOI
  } else if (!is.null(source_id) && nzchar(source_id)) {
    core <- source_id
  } else {
    core <- ref_id
  }
  core <- gsub("[^A-Za-z0-9]+", "_", core)
  core <- gsub("^_+|_+$", "", core)
  if (!nzchar(core)) core <- "ref"
  core
}

.format_authors_bib <- function(authors_list) {
  if (length(authors_list) == 0L || all(is.na(authors_list))) return("")
  paste(authors_list, collapse = " and ")
}

.make_manual_ref_row <- function(
  title,
  authors,
  year,
  month                 = NA_integer_,
  day                   = NA_integer_,
  journal               = NA_character_,
  container_title       = NA_character_,
  publisher             = NA_character_,
  volume                = NA_character_,
  issue                 = NA_character_,
  pages                 = NA_character_,
  doi                   = NA_character_,
  url_landing           = NA_character_,
  url_pdf               = NA_character_,
  note                  = NA_character_,
  subject_primary       = NA_character_,
  subject_all           = NA_character_,
  kind                  = c("blog post","web page","report","thesis","magazine article","newspaper article","book","book section","dataset","manuscript","conference paper","encyclopedia article"),
  abstract              = NA_character_,
  raw_entry             = NULL
) {

  kind <- match.arg(kind)

  if (length(authors) == 1L) {
    authors_vec <- trimws(strsplit(authors, ";")[[1]])
  } else {
    authors_vec <- trimws(authors)
  }

  authors_chr  <- paste(authors_vec, collapse="; ")
  authors_list <- list(authors_vec)

  if (is.na(year)) {
    pub_date <- as.POSIXct(NA)
  } else {
    m <- if (is.na(month)) 1L else month
    d <- if (is.na(day))   1L else day
    pub_date <- as.POSIXct(sprintf("%04d-%02d-%02d",year,m,d), tz="UTC")
  }

  source <- switch(kind,
    "blog post"="blog",
    "web page"="web",
    "report"="report",
    "thesis"="thesis",
    "magazine article"="magazine",
    "newspaper article"="newspaper",
    "book"="book",
    "book section"="booksection",
    "dataset"="dataset",
    "manuscript"="manuscript",
    "conference paper"="conference",
    "encyclopedia article"="encyclopedia",
    "misc"
  )

  first_author <- sub(" .*","",authors_vec[1])
  slug_title   <- gsub("[^A-Za-z0-9]+","",substr(title,1,20))
  source_id    <- paste0(first_author,year,slug_title)
  ref_id       <- paste0(source,":",source_id)

  if (is.na(subject_all) || !nzchar(subject_all)) {
    subject_all <- subject_primary
  }

  data.table::data.table(
    ref_id           = ref_id,
    source           = source,
    source_id        = source_id,
    title            = title,
    abstract         = abstract,
    authors          = authors_chr,
    authors_list     = list(authors_vec),
    pub_date         = pub_date,
    year             = as.integer(year),
    month            = as.integer(month),
    day              = as.integer(day),
    journal          = journal,
    container_title  = container_title,
    publisher        = publisher,
    volume           = volume,
    issue            = issue,
    pages            = pages,
    doi              = doi,
    subject_primary  = subject_primary,
    subject_all      = subject_all,
    url_landing      = url_landing,
    url_pdf          = url_pdf,
    note             = note,
    raw_entry        = list(raw_entry)
  )
}

#' Convert one unified reference row to a BibTeX entry
#'
#' @param row One unified reference row, as a list or one-row `data.table`.
#'
#' @return Character vector containing one BibTeX entry.
#' @export
row_to_bibtex <- function(row) {
  if (data.table::is.data.table(row)) {
    row <- stats::setNames(lapply(names(row), function(name) row[[name]]), names(row))
  }

  entry_type <- switch(row[["source"]],
    blog="misc", web="misc", report="techreport", thesis="phdthesis",
    magazine="article", newspaper="article", book="book",
    booksection="incollection", dataset="misc", manuscript="unpublished",
    conference="inproceedings", encyclopedia="incollection",
    "article"
  )

  key   <- .make_citekey(row[["doi"]], row[["source_id"]], row[["ref_id"]])
  title <- .bibtex_escape(row[["title"]])
  authors_list <- row[["authors_list"]]
  if (is.null(authors_list)) authors_list <- list(character())
  auth  <- .bibtex_escape(.format_authors_bib(authors_list[[1]]))
  year  <- as.character(row[["year"]])

  journal   <- row[["journal"]]
  container <- row[["container_title"]]
  publisher <- row[["publisher"]]
  volume <- row[["volume"]]
  number <- row[["issue"]]
  pages  <- row[["pages"]]
  doi    <- row[["doi"]]
  note   <- row[["note"]]

  url <- row[["url_landing"]]
  if (is.na(url) || !nzchar(url)) url <- row[["url_pdf"]]

  fields <- c(
    title=title, author=auth, year=year,
    volume=volume, number=number, pages=pages,
    doi=doi, url=url, note=note
  )

  if (entry_type=="article") fields["journal"] <- journal
  if (entry_type %in% c("inproceedings","incollection")) fields["booktitle"] <- container
  if (entry_type=="book") fields["publisher"] <- publisher
  if (entry_type %in% c("inproceedings","incollection")) fields["publisher"] <- publisher
  if (entry_type=="techreport") fields["institution"] <- publisher
  if (entry_type=="phdthesis") fields["school"] <- publisher

  fields <- fields[!is.na(fields) & nzchar(fields)]

  lines <- c(sprintf("@%s{%s,", entry_type, key),
             sprintf("  %s = {%s},", names(fields), fields))

  lines[length(lines)] <- sub(",$","",lines[length(lines)])

  c(lines,"}")
}
