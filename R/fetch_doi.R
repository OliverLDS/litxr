.litxr_first_nonempty_chr <- function(x) {
  if (is.null(x) || !length(x)) {
    return(NA_character_)
  }
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(trimws(x))]
  if (!length(x)) {
    return(NA_character_)
  }
  x[[1L]]
}

.litxr_nonempty_chr <- function(x) {
  if (is.null(x) || !length(x)) {
    return(character())
  }
  x <- as.character(unlist(x, use.names = FALSE))
  x <- x[!is.na(x) & nzchar(trimws(x))]
  unique(x)
}

.litxr_message_field_missing <- function(x) {
  if (is.null(x) || !length(x)) {
    return(TRUE)
  }
  if (is.character(x)) {
    vals <- x[!is.na(x) & nzchar(trimws(x))]
    return(!length(vals))
  }
  if (is.list(x) && length(x) == 1L && is.character(x[[1L]])) {
    vals <- x[[1L]]
    vals <- vals[!is.na(vals) & nzchar(trimws(vals))]
    return(!length(vals))
  }
  FALSE
}

.litxr_doi_source_rank <- function(source) {
  source <- tolower(as.character(source)[[1L]])
  switch(
    source,
    crossref = 5L,
    doi_org_html = 4L,
    doi_org_csl = 3L,
    datacite = 2L,
    zenodo = 1L,
    0L
  )
}

.litxr_merge_doi_messages <- function(primary, fallback) {
  if (is.null(primary)) return(fallback)
  if (is.null(fallback)) return(primary)
  out <- primary
  preferred_source <- if (.litxr_doi_source_rank(primary$source) >= .litxr_doi_source_rank(fallback$source)) {
    primary$source
  } else {
    fallback$source
  }
  preferred_url <- .litxr_first_nonempty_chr(c(primary$URL, fallback$URL))
  fields <- c(
    "title", "author", "issued", "container-title", "publisher",
    "volume", "issue", "page", "abstract", "URL", "ISBN", "ISSN", "subject"
  )
  for (field in fields) {
    if (.litxr_message_field_missing(out[[field]]) && !.litxr_message_field_missing(fallback[[field]])) {
      out[[field]] <- fallback[[field]]
    }
  }
  out$DOI <- .litxr_first_nonempty_chr(c(primary$DOI, fallback$DOI))
  out$URL <- preferred_url
  out$source <- preferred_source
  out
}

.litxr_date_parts_from_components <- function(year = NA_integer_, month = NA_integer_, day = NA_integer_) {
  year <- suppressWarnings(as.integer(year[[1L]]))
  if (is.na(year)) {
    return(NULL)
  }
  month <- suppressWarnings(as.integer(month[[1L]]))
  day <- suppressWarnings(as.integer(day[[1L]]))
  if (is.na(month) || month < 1L) month <- 1L
  if (is.na(day) || day < 1L) day <- 1L
  list(`date-parts` = list(c(year, month, day)))
}

.litxr_people_to_crossref_author <- function(people) {
  if (is.null(people) || !length(people)) {
    return(NULL)
  }

  if (is.data.frame(people)) {
    people <- data.table::as.data.table(people)
    if (!nrow(people)) {
      return(NULL)
    }
    return(lapply(seq_len(nrow(people)), function(i) {
      given <- .litxr_first_nonempty_chr(people$given[[i]])
      family <- .litxr_first_nonempty_chr(people$family[[i]])
      name <- .litxr_first_nonempty_chr(people$name[[i]])
      if (is.na(given) && is.na(family) && !is.na(name)) {
        return(list(name = name))
      }
      list(
        given = if (is.na(given)) NA_character_ else given,
        family = if (is.na(family)) NA_character_ else family
      )
    }))
  }

  if (is.list(people)) {
    return(lapply(people, function(person) {
      if (is.null(person) || !length(person)) {
        return(NULL)
      }
      given <- .litxr_first_nonempty_chr(person$given %||% person$givenName)
      family <- .litxr_first_nonempty_chr(person$family %||% person$familyName)
      name <- .litxr_first_nonempty_chr(person$name)
      if (is.na(given) && is.na(family) && !is.na(name)) {
        return(list(name = name))
      }
      list(
        given = if (is.na(given)) NA_character_ else given,
        family = if (is.na(family)) NA_character_ else family
      )
    }))
  }

  NULL
}

.litxr_doi_message_from_datacite <- function(payload, doi) {
  attrs <- payload$data$attributes %||% payload$attributes %||% list()
  creators <- attrs$creators %||% attrs$author %||% attrs$authors
  authors <- .litxr_people_to_crossref_author(creators)
  title <- .litxr_first_nonempty_chr(
    attrs$titles[[1]]$title %||%
      attrs$title %||%
      attrs$name %||%
      payload$data$attributes$title
  )
  if (is.na(title) && !is.null(attrs$titles) && length(attrs$titles)) {
    title <- .litxr_first_nonempty_chr(vapply(attrs$titles, function(x) x$title %||% NA_character_, character(1)))
  }

  publication_year <- attrs$publicationYear %||% attrs$publication_year
  issued <- .litxr_date_parts_from_components(publication_year, 1L, 1L)
  if (is.null(issued) && !is.null(attrs$dates) && length(attrs$dates)) {
    date_entry <- attrs$dates[[1L]]
    if (!is.null(date_entry$date)) {
      parsed <- suppressWarnings(as.Date(.litxr_first_nonempty_chr(date_entry$date)))
      if (!is.na(parsed)) {
        issued <- list(`date-parts` = list(c(
          as.integer(format(parsed, "%Y")),
          as.integer(format(parsed, "%m")),
          as.integer(format(parsed, "%d"))
        )))
      }
    }
  }

  container_title <- .litxr_first_nonempty_chr(attrs$container_title %||% attrs$containerTitle %||% attrs$container)
  publisher <- .litxr_first_nonempty_chr(attrs$publisher)
  volume <- .litxr_first_nonempty_chr(attrs$volume)
  issue <- .litxr_first_nonempty_chr(attrs$issue)
  pages <- .litxr_first_nonempty_chr(attrs$page %||% attrs$pages)
  abstract <- .litxr_first_nonempty_chr(attrs$descriptions[[1]]$description %||% attrs$description)
  url <- .litxr_first_nonempty_chr(attrs$url %||% attrs$landingPage %||% attrs$landing_page)
  isbn <- if (!is.null(attrs$isbn) && length(attrs$isbn)) paste(unique(as.character(unlist(attrs$isbn, use.names = FALSE))), collapse = "; ") else NA_character_
  issn <- if (!is.null(attrs$issn) && length(attrs$issn)) paste(unique(as.character(unlist(attrs$issn, use.names = FALSE))), collapse = "; ") else NA_character_
  subject <- if (!is.null(attrs$subjects) && length(attrs$subjects)) {
    unique(vapply(attrs$subjects, function(x) .litxr_first_nonempty_chr(x$subject %||% x), character(1)))
  } else {
    character()
  }
  if (!length(subject)) {
    subject <- character()
  }
  if (is.na(url) && !is.na(doi)) {
    url <- paste0("https://doi.org/", doi)
  }

  list(
    source = "datacite",
    DOI = doi,
    title = if (is.na(title)) character() else title,
    author = authors,
    issued = issued,
    `container-title` = if (is.na(container_title)) character() else container_title,
    publisher = publisher,
    volume = volume,
    issue = issue,
    page = pages,
    abstract = abstract,
    URL = url,
    ISBN = if (is.na(isbn)) character() else isbn,
    ISSN = if (is.na(issn)) character() else issn,
    subject = subject
  )
}

.litxr_doi_message_from_zenodo <- function(payload, doi) {
  metadata <- payload$metadata %||% payload$data$metadata %||% list()
  title <- .litxr_first_nonempty_chr(metadata$title)
  creators <- metadata$creators %||% metadata$authors
  authors <- .litxr_people_to_crossref_author(creators)

  pub_date <- .litxr_first_nonempty_chr(metadata$publication_date %||% metadata$publicationDate)
  issued <- NULL
  if (!is.na(pub_date)) {
    parsed <- suppressWarnings(as.Date(pub_date))
    if (!is.na(parsed)) {
      issued <- list(`date-parts` = list(c(
        as.integer(format(parsed, "%Y")),
        as.integer(format(parsed, "%m")),
        as.integer(format(parsed, "%d"))
      )))
    }
  }
  if (is.null(issued) && !is.null(metadata$year)) {
    issued <- .litxr_date_parts_from_components(metadata$year, 1L, 1L)
  }

  container_title <- .litxr_first_nonempty_chr(metadata$journal_title %||% metadata$journal)
  publisher <- .litxr_first_nonempty_chr(payload$metadata$publisher %||% payload$publisher %||% "Zenodo")
  volume <- .litxr_first_nonempty_chr(metadata$volume)
  issue <- .litxr_first_nonempty_chr(metadata$issue)
  pages <- .litxr_first_nonempty_chr(metadata$page %||% metadata$pages)
  abstract <- .litxr_first_nonempty_chr(metadata$description)
  url <- .litxr_first_nonempty_chr(
    payload$links$html %||%
      payload$links$self %||%
      metadata$prereserve_doi %||%
      metadata$url
  )
  subject <- if (!is.null(metadata$keywords) && length(metadata$keywords)) {
    unique(as.character(unlist(metadata$keywords, use.names = FALSE)))
  } else {
    character()
  }
  isbn <- if (!is.null(metadata$isbn) && length(metadata$isbn)) paste(unique(as.character(unlist(metadata$isbn, use.names = FALSE))), collapse = "; ") else NA_character_
  issn <- if (!is.null(metadata$issn) && length(metadata$issn)) paste(unique(as.character(unlist(metadata$issn, use.names = FALSE))), collapse = "; ") else NA_character_
  if (is.na(url) && !is.na(doi)) {
    url <- paste0("https://doi.org/", doi)
  }

  list(
    source = "zenodo",
    DOI = doi,
    title = if (is.na(title)) character() else title,
    author = authors,
    issued = issued,
    `container-title` = if (is.na(container_title)) character() else container_title,
    publisher = publisher,
    volume = volume,
    issue = issue,
    page = pages,
    abstract = abstract,
    URL = url,
    ISBN = if (is.na(isbn)) character() else isbn,
    ISSN = if (is.na(issn)) character() else issn,
    subject = subject
  )
}

.litxr_fetch_json_or_null <- function(url, accept = NULL) {
  req <- httr2::request(url) |>
    httr2::req_error(is_error = function(resp) FALSE)
  if (!is.null(accept) && nzchar(accept)) {
    req <- httr2::req_headers(req, Accept = accept)
  }
  resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
  if (is.null(resp) || httr2::resp_status(resp) >= 400) {
    return(NULL)
  }
  tryCatch(httr2::resp_body_json(resp, simplifyVector = FALSE), error = function(e) NULL)
}

.litxr_fetch_html_or_null <- function(url) {
  req <- httr2::request(url) |>
    httr2::req_error(is_error = function(resp) FALSE)
  resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
  if (is.null(resp) || httr2::resp_status(resp) >= 400) {
    return(NULL)
  }
  tryCatch(httr2::resp_body_string(resp), error = function(e) NULL)
}

.litxr_html_meta_map <- function(doc) {
  nodes <- xml2::xml_find_all(doc, ".//meta")
  if (!length(nodes)) {
    return(list())
  }

  names <- trimws(as.character(xml2::xml_attr(nodes, "name")))
  props <- trimws(as.character(xml2::xml_attr(nodes, "property")))
  keys <- ifelse(!is.na(names) & nzchar(names), names, props)
  contents <- trimws(as.character(xml2::xml_attr(nodes, "content")))
  keep <- !is.na(keys) & nzchar(keys) & !is.na(contents) & nzchar(contents)
  if (!any(keep)) {
    return(list())
  }

  split(as.character(contents[keep]), as.character(keys[keep]))
}

.litxr_html_meta_first <- function(meta, keys) {
  keys <- as.character(keys)
  for (key in keys) {
    if (!length(key) || is.na(key) || !nzchar(key)) next
    value <- meta[[key]]
    if (!is.null(value) && length(value)) {
      value <- as.character(value)
      value <- value[!is.na(value) & nzchar(trimws(value))]
      if (length(value)) {
        return(value[[1L]])
      }
    }
  }
  NA_character_
}

.litxr_html_meta_author_list <- function(meta) {
  authors <- meta[["citation_author"]]
  if (is.null(authors) || !length(authors)) {
    return(NULL)
  }
  authors <- as.character(authors)
  authors <- authors[!is.na(authors) & nzchar(trimws(authors))]
  if (!length(authors)) {
    return(NULL)
  }
  lapply(authors, function(name) list(name = name))
}

.litxr_html_meta_date_parts <- function(meta) {
  raw_date <- .litxr_html_meta_first(meta, c("citation_publication_date", "citation_online_date", "citation_date"))
  if (is.na(raw_date) || !nzchar(raw_date)) {
    return(NULL)
  }
  raw_date <- gsub("/", "-", raw_date, fixed = TRUE)
  parsed <- suppressWarnings(as.Date(raw_date))
  if (is.na(parsed)) {
    return(NULL)
  }
  list(`date-parts` = list(c(
    as.integer(format(parsed, "%Y")),
    as.integer(format(parsed, "%m")),
    as.integer(format(parsed, "%d"))
  )))
}

.litxr_doi_message_from_html <- function(html, doi, source_url = NULL) {
  meta <- .litxr_html_meta_map(html)

  title <- .litxr_html_meta_first(meta, c("citation_title", "og:title", "twitter:title", "dc.title"))
  authors <- .litxr_html_meta_author_list(meta)
  issued <- .litxr_html_meta_date_parts(meta)

  container_title <- .litxr_html_meta_first(meta, c("citation_journal_title", "citation_conference_title", "citation_venue", "dc.source"))
  publisher <- .litxr_html_meta_first(meta, c("citation_publisher", "dc.publisher", "og:site_name"))
  volume <- .litxr_html_meta_first(meta, "citation_volume")
  issue <- .litxr_html_meta_first(meta, "citation_issue")
  pages <- .litxr_html_meta_first(meta, c("citation_firstpage", "citation_lastpage"))
  if (!is.na(pages) && nzchar(pages)) {
    last_page <- .litxr_html_meta_first(meta, "citation_lastpage")
    first_page <- .litxr_html_meta_first(meta, "citation_firstpage")
    if (!is.na(first_page) && !is.na(last_page) && nzchar(first_page) && nzchar(last_page)) {
      pages <- paste0(first_page, "-", last_page)
    } else if (!is.na(first_page) && nzchar(first_page)) {
      pages <- first_page
    }
  }
  abstract <- .litxr_html_meta_first(meta, c("citation_abstract", "description", "dc.description"))
  url <- .litxr_html_meta_first(meta, c("citation_url", "citation_pdf_url", "og:url"))
  if (is.na(url) || !nzchar(url)) {
    url <- source_url
  }
  isbn <- meta[["citation_isbn"]]
  isbn <- if (!is.null(isbn) && length(isbn)) paste(unique(as.character(isbn)), collapse = "; ") else NA_character_
  issn <- meta[["citation_issn"]]
  issn <- if (!is.null(issn) && length(issn)) paste(unique(as.character(issn)), collapse = "; ") else NA_character_
  subject <- meta[["citation_keywords"]]
  subject <- if (!is.null(subject) && length(subject)) unique(as.character(subject)) else character()

  list(
    source = "doi_org_html",
    DOI = doi,
    title = if (is.na(title)) character() else title,
    author = authors,
    issued = issued,
    `container-title` = if (is.na(container_title)) character() else container_title,
    publisher = if (is.na(publisher)) NA_character_ else publisher,
    volume = if (is.na(volume)) NA_character_ else volume,
    issue = if (is.na(issue)) NA_character_ else issue,
    page = if (is.na(pages)) NA_character_ else pages,
    abstract = if (is.na(abstract)) NA_character_ else abstract,
    URL = if (is.na(url)) NA_character_ else url,
    ISBN = if (is.na(isbn)) character() else isbn,
    ISSN = if (is.na(issn)) character() else issn,
    subject = subject
  )
}

.litxr_fetch_doi_message_from_html <- function(doi, source_url = NULL) {
  doi <- .litxr_normalize_doi_ref_id(doi)
  if (is.na(doi) || !nzchar(doi)) {
    return(NULL)
  }
  bare_doi <- sub("^doi:", "", doi, ignore.case = TRUE)
  html_url <- if (!is.null(source_url) && nzchar(source_url)) source_url else sprintf("https://doi.org/%s", utils::URLencode(bare_doi, reserved = TRUE))
  html <- .litxr_fetch_html_or_null(html_url)
  if (is.null(html)) {
    return(NULL)
  }
  doc <- tryCatch(xml2::read_html(html), error = function(e) NULL)
  if (is.null(doc)) {
    return(NULL)
  }
  message <- .litxr_doi_message_from_html(doc, bare_doi, source_url = html_url)
  if (is.null(message) || !length(message)) {
    return(NULL)
  }
  message
}

.litxr_fetch_doi_message_one <- function(doi) {
  doi <- .litxr_normalize_doi_ref_id(doi)
  if (is.na(doi) || !nzchar(doi)) {
    return(NULL)
  }
  bare_doi <- sub("^doi:", "", doi, ignore.case = TRUE)

  crossref <- tryCatch(fetch_crossref_messages(doi), error = function(e) list(setNames(list(NULL), doi)))
  if (!is.null(crossref[[1L]])) {
    crossref[[1L]]$source <- "crossref"
    return(crossref[[1L]])
  }

  html_message <- .litxr_fetch_doi_message_from_html(bare_doi)

  zenodo_message <- NULL
  if (grepl("zenodo", bare_doi, ignore.case = TRUE)) {
    zenodo_id <- sub("^10\\.5281/zenodo\\.", "", bare_doi, ignore.case = TRUE)
    zenodo_payload <- .litxr_fetch_json_or_null(sprintf("https://zenodo.org/api/records/%s", zenodo_id))
    if (!is.null(zenodo_payload)) {
      zenodo_message <- .litxr_doi_message_from_zenodo(zenodo_payload, bare_doi)
    }
    if (is.null(zenodo_message)) {
      zenodo_payload <- .litxr_fetch_json_or_null(sprintf(
        "https://zenodo.org/api/records?q=%s",
        utils::URLencode(sprintf('"%s"', bare_doi), reserved = TRUE)
      ))
      if (!is.null(zenodo_payload)) {
        hit <- zenodo_payload$hits$hits[[1L]] %||% zenodo_payload$hits[[1L]]
        if (!is.null(hit)) {
          zenodo_message <- .litxr_doi_message_from_zenodo(hit, bare_doi)
        }
      }
    }
  }

  datacite_message <- NULL
  datacite_payload <- .litxr_fetch_json_or_null(sprintf("https://api.datacite.org/dois/%s", utils::URLencode(bare_doi, reserved = TRUE)))
  if (!is.null(datacite_payload)) {
    datacite_message <- .litxr_doi_message_from_datacite(datacite_payload, bare_doi)
  }

  doi_org_message <- NULL
  doi_org_payload <- .litxr_fetch_json_or_null(
    sprintf("https://doi.org/%s", utils::URLencode(bare_doi, reserved = TRUE)),
    accept = "application/vnd.citationstyles.csl+json"
  )
  if (!is.null(doi_org_payload)) {
    doi_org_payload$source <- "doi_org_csl"
    doi_org_message <- doi_org_payload
  }

  merged <- NULL
  merged <- .litxr_merge_doi_messages(merged, html_message)
  merged <- .litxr_merge_doi_messages(merged, doi_org_message)
  merged <- .litxr_merge_doi_messages(merged, datacite_message)
  merged <- .litxr_merge_doi_messages(merged, zenodo_message)
  merged
}

.litxr_fetch_doi_messages <- function(dois) {
  dois <- unique(as.character(dois))
  dois <- dois[!is.na(dois) & nzchar(trimws(dois))]
  if (!length(dois)) {
    return(setNames(vector("list", 0L), character()))
  }
  stats::setNames(lapply(dois, .litxr_fetch_doi_message_one), dois)
}
