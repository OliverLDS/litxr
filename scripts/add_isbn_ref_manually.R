#!/usr/bin/env Rscript

emit_json <- function(x) {
  writeLines(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"), con = stdout())
}

usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript scripts/add_isbn_ref_manually.R --isbn ISBN --title TITLE [options]",
    "",
    "Required:",
    "  --isbn ISBN              ISBN-10 or ISBN-13.",
    "  --title TITLE             Book title.",
    "",
    "Optional:",
    "  --authors NAMES           Semicolon-separated author names.",
    "  --year YEAR               Publication year.",
    "  --publisher NAME          Publisher.",
    "  --edition TEXT            Edition, stored as a note.",
    "  --url URL                 Landing page URL.",
    "  --abstract TEXT           Abstract or description.",
    "  --collection-id ID        Target ISBN collection. Default: manual_books.",
    "  --collection-title TITLE  Collection title when it is first created.",
    "  -h, --help                Show this help message.",
    "",
    "Behavior:",
    "  - Writes or replaces one manually supplied book JSON by ISBN.",
    "  - Creates the target collection as an ISBN collection when needed.",
    "  - Runs incremental thin-store sync for that collection only.",
    sep = "\n"
  ))
}

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    isbn = NULL,
    title = NULL,
    authors = NULL,
    year = NULL,
    publisher = NULL,
    edition = NULL,
    url = NULL,
    abstract = NULL,
    collection_id = "manual_books",
    collection_title = "Manual Books"
  )
  names_by_flag <- c(
    "--isbn" = "isbn",
    "--title" = "title",
    "--authors" = "authors",
    "--year" = "year",
    "--publisher" = "publisher",
    "--edition" = "edition",
    "--url" = "url",
    "--abstract" = "abstract",
    "--collection-id" = "collection_id",
    "--collection-title" = "collection_title"
  )
  i <- 1L
  while (i <= length(args)) {
    flag <- args[[i]]
    if (flag %in% c("-h", "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (!(flag %in% names(names_by_flag)) || i == length(args)) {
      stop("Unknown argument or missing value: ", flag, call. = FALSE)
    }
    out[[names_by_flag[[flag]]]] <- args[[i + 1L]]
    i <- i + 2L
  }
  out
}

normalize_isbn <- function(x) {
  isbn <- toupper(gsub("[^0-9Xx]", "", trimws(as.character(x))))
  if (nchar(isbn) == 10L && grepl("^[0-9]{9}[0-9X]$", isbn)) {
    values <- c(as.integer(strsplit(substr(isbn, 1L, 9L), "", fixed = TRUE)[[1L]]), if (substr(isbn, 10L, 10L) == "X") 10L else as.integer(substr(isbn, 10L, 10L)))
    if (sum(values * 10:1) %% 11L == 0L) return(isbn)
  }
  if (nchar(isbn) == 13L && grepl("^[0-9]{13}$", isbn)) {
    values <- as.integer(strsplit(isbn, "", fixed = TRUE)[[1L]])
    if (sum(values * rep(c(1L, 3L), length.out = 13L)) %% 10L == 0L) return(isbn)
  }
  NA_character_
}

options(error = function() {
  emit_json(list(status = "error", error = trimws(geterrmessage())))
  quit(save = "no", status = 1L)
})

args <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(args$help)) {
  usage()
  quit(save = "no", status = 0L)
}
if (is.null(args$isbn) || is.null(args$title) || !nzchar(trimws(args$title))) {
  stop("Both --isbn and --title are required.", call. = FALSE)
}

isbn <- normalize_isbn(args$isbn)
if (is.na(isbn)) {
  stop("`--isbn` must be a valid ISBN-10 or ISBN-13.", call. = FALSE)
}
year <- if (is.null(args$year)) NA_integer_ else suppressWarnings(as.integer(args$year))
if (!is.null(args$year) && is.na(year)) {
  stop("`--year` must be an integer.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
collection <- tryCatch(litxr:::.litxr_get_journal(cfg, args$collection_id), error = function(e) NULL)
if (is.null(collection)) {
  registered <- litxr:::.litxr_register_manual_collection(
    cfg,
    args$collection_id,
    collection_title = args$collection_title,
    remote_channel = "isbn",
    collection_type = "manual_isbn"
  )
  cfg <- registered$cfg
} else if (!identical(as.character(collection$remote_channel), "isbn")) {
  stop(
    "Collection `", args$collection_id,
    "` is not an ISBN collection (remote_channel must be `isbn`).",
    call. = FALSE
  )
}

records <- litxr::litxr_add_refs(
  data.frame(
    source = "manual",
    entry_type = "book",
    title = trimws(args$title),
    authors = if (is.null(args$authors)) NA_character_ else trimws(args$authors),
    year = year,
    publisher = if (is.null(args$publisher)) NA_character_ else trimws(args$publisher),
    isbn = isbn,
    url = if (is.null(args$url)) NA_character_ else trimws(args$url),
    abstract = if (is.null(args$abstract)) NA_character_ else trimws(args$abstract),
    note = if (is.null(args$edition)) NA_character_ else paste0("Edition: ", trimws(args$edition)),
    stringsAsFactors = FALSE
  ),
  collection_id = args$collection_id,
  config = cfg,
  auto_register = FALSE
)
sync <- litxr::litxr_sync_thin_ref_stores_from_json(cfg, collection_ids = args$collection_id)

emit_json(list(
  status = "ok",
  collection_id = args$collection_id,
  ref_id = records$ref_id[[1L]],
  isbn = isbn,
  json_written = paste0("isbn_", isbn, ".json"),
  thin_rows = sync$row_counts$ref_isbn
))
