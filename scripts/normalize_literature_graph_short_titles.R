#!/usr/bin/env Rscript

usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript scripts/normalize_literature_graph_short_titles.R --input PATH --output PATH",
    "",
    "Normalizes literature graph short titles into unique BibTeX-safe keys.",
    "The output retains the {schema_version, short_titles} sidecar shape.",
    "It suffixes normalized duplicate keys with their reference id.",
    sep = "\n"
  ))
}

parse_args <- function(args) {
  out <- list(help = FALSE, input = NULL, output = NULL)
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (key %in% c("-h", "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (i == length(args)) stop("Missing value for ", key, call. = FALSE)
    if (key == "--input") out$input <- args[[i + 1L]] else if (key == "--output") out$output <- args[[i + 1L]] else stop("Unknown argument: ", key, call. = FALSE)
    i <- i + 2L
  }
  out
}

args <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(args$help)) {
  usage()
  quit(save = "no", status = 0L)
}
if (is.null(args$input) || is.null(args$output)) stop("Both --input and --output are required.", call. = FALSE)
if (!file.exists(args$input)) stop("Short-title sidecar not found: ", args$input, call. = FALSE)

sidecar <- jsonlite::read_json(args$input, simplifyVector = FALSE)
titles <- as.character(unlist(sidecar$short_titles, use.names = FALSE))
names(titles) <- names(sidecar$short_titles)
if (!is.character(titles) || is.null(names(titles)) || !length(titles) || length(titles) != length(names(sidecar$short_titles))) {
  stop("Short-title sidecar must contain a named `short_titles` object.", call. = FALSE)
}

normalized <- trimws(as.character(titles))
normalized <- iconv(normalized, from = "UTF-8", to = "ASCII//TRANSLIT")
normalized <- gsub("[^A-Za-z0-9]+", "_", normalized)
normalized <- gsub("^_+|_+$", "", normalized)
names(normalized) <- names(titles)
normalized[!nzchar(normalized) | !grepl("^[A-Za-z0-9][A-Za-z0-9_]*$", normalized)] <- NA_character_
if (anyNA(normalized)) {
  bad <- names(normalized)[is.na(normalized)]
  stop("Unable to normalize short title(s): ", paste(bad, collapse = ", "), call. = FALSE)
}
duplicate_keys <- duplicated(tolower(normalized)) | duplicated(tolower(normalized), fromLast = TRUE)
if (any(duplicate_keys)) {
  suffixes <- gsub("[^A-Za-z0-9]+", "_", names(normalized)[duplicate_keys])
  normalized[duplicate_keys] <- paste0(normalized[duplicate_keys], "_", suffixes)
}
if (anyDuplicated(tolower(normalized))) {
  pairs <- paste(names(normalized), normalized, sep = " -> ")
  stop("Unable to make short-title keys unique: ", paste(pairs, collapse = "; "), call. = FALSE)
}

out <- list(schema_version = 1L, short_titles = as.list(normalized[order(names(normalized))]))
dir.create(dirname(args$output), recursive = TRUE, showWarnings = FALSE)
jsonlite::write_json(out, args$output, auto_unbox = TRUE, pretty = TRUE)
writeLines(jsonlite::toJSON(list(status = "ok", input = normalizePath(args$input, winslash = "/", mustWork = TRUE), output = normalizePath(args$output, winslash = "/", mustWork = FALSE), converted = length(normalized)), auto_unbox = TRUE), con = stdout())
