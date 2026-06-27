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

.litxr_fetch_doi_message_one <- function(doi) {
  doi <- .litxr_normalize_doi_ref_id(doi)
  if (is.na(doi) || !nzchar(doi)) {
    return(NULL)
  }
  bare_doi <- sub("^doi:", "", doi, ignore.case = TRUE)

  crossref <- tryCatch(fetch_crossref_messages(doi), error = function(e) list(setNames(list(NULL), doi)))
  if (!is.null(crossref[[1L]])) {
    return(crossref[[1L]])
  }

  if (grepl("zenodo", bare_doi, ignore.case = TRUE)) {
    zenodo_id <- sub("^10\\.5281/zenodo\\.", "", bare_doi, ignore.case = TRUE)
    zenodo_payload <- .litxr_fetch_json_or_null(sprintf("https://zenodo.org/api/records/%s", zenodo_id))
    if (!is.null(zenodo_payload)) {
      return(.litxr_doi_message_from_zenodo(zenodo_payload, bare_doi))
    }
    zenodo_payload <- .litxr_fetch_json_or_null(sprintf(
      "https://zenodo.org/api/records?q=%s",
      utils::URLencode(sprintf('"%s"', bare_doi), reserved = TRUE)
    ))
    if (!is.null(zenodo_payload)) {
      hit <- zenodo_payload$hits$hits[[1L]] %||% zenodo_payload$hits[[1L]]
      if (!is.null(hit)) {
        return(.litxr_doi_message_from_zenodo(hit, bare_doi))
      }
    }
  }

  datacite_payload <- .litxr_fetch_json_or_null(sprintf("https://api.datacite.org/dois/%s", utils::URLencode(bare_doi, reserved = TRUE)))
  if (!is.null(datacite_payload)) {
    return(.litxr_doi_message_from_datacite(datacite_payload, bare_doi))
  }

  doi_org_payload <- .litxr_fetch_json_or_null(
    sprintf("https://doi.org/%s", utils::URLencode(bare_doi, reserved = TRUE)),
    accept = "application/vnd.citationstyles.csl+json"
  )
  if (!is.null(doi_org_payload)) {
    return(doi_org_payload)
  }

  NULL
}

.litxr_fetch_doi_messages <- function(dois) {
  dois <- unique(as.character(dois))
  dois <- dois[!is.na(dois) & nzchar(trimws(dois))]
  if (!length(dois)) {
    return(setNames(vector("list", 0L), character()))
  }
  stats::setNames(lapply(dois, .litxr_fetch_doi_message_one), dois)
}
