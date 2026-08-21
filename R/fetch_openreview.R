.litxr_openreview_content_value <- function(content, name) {
  value <- content[[name]]
  if (is.null(value) || !length(value)) return(NA_character_)
  if (is.list(value) && !is.null(value$value)) value <- value$value
  if (is.list(value) && length(value) == 1L) value <- value[[1L]]
  value <- as.character(unlist(value, use.names = FALSE))
  value <- value[!is.na(value) & nzchar(trimws(value))]
  if (!length(value)) NA_character_ else paste(value, collapse = "; ")
}

.litxr_openreview_authors <- function(content) {
  value <- content$authors
  if (is.list(value) && !is.null(value$value)) value <- value$value
  authors <- as.character(unlist(value, use.names = FALSE))
  authors <- authors[!is.na(authors) & nzchar(trimws(authors))]
  unique(authors)
}

.litxr_openreview_note_to_record <- function(note) {
  note_id <- .litxr_bare_openreview_id(ref_id = note$id)
  if (is.na(note_id) || !nzchar(note_id)) {
    stop("OpenReview response is missing a valid Note id.", call. = FALSE)
  }
  content <- note$content %||% list()
  date_ms <- .litxr_first_nonempty_chr(note$pdate %||% note$tcdate %||% note$cdate)
  date_ms <- suppressWarnings(as.numeric(date_ms))
  pub_date <- if (is.na(date_ms)) as.POSIXct(NA, tz = "UTC") else as.POSIXct(date_ms / 1000, origin = "1970-01-01", tz = "UTC")
  authors <- .litxr_openreview_authors(content)
  venue <- .litxr_openreview_content_value(content, "venue")
  data.table::data.table(
    ref_id = paste0("openreview:", note_id),
    source = "openreview",
    source_id = note_id,
    entry_type = "inproceedings",
    title = .litxr_openreview_content_value(content, "title"),
    abstract = .litxr_openreview_content_value(content, "abstract"),
    authors = if (length(authors)) paste(authors, collapse = "; ") else NA_character_,
    authors_list = list(authors),
    pub_date = pub_date,
    year = if (is.na(pub_date)) NA_integer_ else as.integer(format(pub_date, "%Y")),
    month = if (is.na(pub_date)) NA_integer_ else as.integer(format(pub_date, "%m")),
    day = if (is.na(pub_date)) NA_integer_ else as.integer(format(pub_date, "%d")),
    journal = NA_character_,
    container_title = venue,
    publisher = "OpenReview",
    volume = NA_character_,
    issue = NA_character_,
    pages = NA_character_,
    doi = NA_character_,
    isbn = NA_character_,
    issn = NA_character_,
    url = paste0("https://openreview.net/forum?id=", note_id),
    url_landing = paste0("https://openreview.net/forum?id=", note_id),
    url_pdf = paste0("https://openreview.net/pdf?id=", note_id),
    note = NA_character_,
    subject_primary = NA_character_,
    subject_all = NA_character_,
    arxiv_id_versioned = NA_character_,
    arxiv_id_base = NA_character_,
    arxiv_version = NA_integer_,
    arxiv_primary_category = NA_character_,
    arxiv_categories_raw = NA_character_,
    arxiv_comment = NA_character_,
    arxiv_journal_ref = NA_character_,
    linked_doi_ref_id = NA_character_,
    linked_arxiv_ref_id = NA_character_,
    raw_entry = list(note)
  )
}

.litxr_fetch_openreview_notes <- function(openreview_ids) {
  openreview_ids <- unique(vapply(openreview_ids, .litxr_bare_openreview_id, character(1)))
  openreview_ids <- openreview_ids[!is.na(openreview_ids) & nzchar(openreview_ids)]
  if (!length(openreview_ids)) return(list())

  notes <- lapply(openreview_ids, function(openreview_id) {
    request <- httr2::request("https://api2.openreview.net/notes")
    request <- httr2::req_url_query(request, id = openreview_id)
    request <- httr2::req_headers(request, Accept = "application/json")
    request <- httr2::req_user_agent(request, "litxr/0.1")
    token <- Sys.getenv("OPENREVIEW_ACCESS_TOKEN", unset = "")
    if (nzchar(token)) {
      request <- httr2::req_headers(request, Authorization = paste("Bearer", sub("^Bearer\\s+", "", token, ignore.case = TRUE)))
    }
    request <- httr2::req_error(request, is_error = function(response) FALSE)
    response <- httr2::req_perform(request)
    status <- httr2::resp_status(response)
    if (status < 200L || status >= 300L) {
      stop(
        "OpenReview API request failed with HTTP ", status,
        ". Set OPENREVIEW_ACCESS_TOKEN when OpenReview requires an authenticated request.",
        call. = FALSE
      )
    }
    payload <- httr2::resp_body_json(response, simplifyVector = FALSE)
    rows <- payload$notes %||% list()
    if (!length(rows)) return(NULL)
    rows[[1L]]
  })
  names(notes) <- openreview_ids
  notes
}

#' Add OpenReview submissions by Note id
#'
#' Fetches public OpenReview Note metadata, writes each reference JSON under
#' `ref/openreview/`, and registers that collection if required.
#'
#' @param openreview_ids Character vector of bare OpenReview Note ids or
#'   canonical `openreview:` ref ids.
#' @param config Optional parsed config list or a config path.
#' @return A `data.table` of fetched reference rows.
#' @export
litxr_add_openreview_notes <- function(openreview_ids, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  note_ids <- unique(vapply(openreview_ids, .litxr_bare_openreview_id, character(1)))
  note_ids <- note_ids[!is.na(note_ids) & nzchar(note_ids)]
  if (!length(note_ids)) return(data.table::data.table())

  collection <- .litxr_collection_entry_by_id(cfg, "openreview")
  if (is.null(collection)) {
    registered <- .litxr_register_manual_collection(
      cfg,
      "openreview",
      collection_title = "OpenReview",
      remote_channel = "openreview",
      collection_type = "openreview"
    )
    cfg <- registered$cfg
  } else if (!identical(as.character(collection$remote_channel), "openreview")) {
    stop("Collection `openreview` must use remote_channel `openreview`.", call. = FALSE)
  }

  notes <- .litxr_fetch_openreview_notes(note_ids)
  notes <- notes[!vapply(notes, is.null, logical(1L))]
  if (!length(notes)) return(data.table::data.table())
  records <- data.table::rbindlist(lapply(notes, .litxr_openreview_note_to_record), fill = TRUE)
  litxr_add_refs(records, collection_id = "openreview", config = cfg, auto_register = FALSE)
}
