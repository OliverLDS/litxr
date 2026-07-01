#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(litxr)
})

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

emit_json <- function(x) {
  writeLines(
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE),
    con = stdout()
  )
}

parse_args <- function(args) {
  out <- list(
    show_help = FALSE,
    input = NULL,
    output_dir = NULL,
    chunk_size = "25000",
    overwrite = FALSE
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (!startsWith(key, "--") && !identical(key, "-h")) {
      stop("Unexpected positional argument: ", key, call. = FALSE)
    }
    if (identical(key, "-h") || identical(key, "--help")) {
      out$show_help <- TRUE
      i <- i + 1L
      next
    }
    if (i == length(args)) {
      stop("Missing value for ", key, call. = FALSE)
    }
    value <- args[[i + 1L]]
    if (identical(key, "--input")) {
      out$input <- value
    } else if (identical(key, "--output-dir")) {
      out$output_dir <- value
    } else if (identical(key, "--chunk-size")) {
      out$chunk_size <- value
    } else if (identical(key, "--overwrite")) {
      out$overwrite <- value
    } else {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    i <- i + 2L
  }
  out
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/build_arxiv_lexical_index.R --input PATH [--output-dir PATH]",
      "    [--chunk-size 25000] [--overwrite TRUE|FALSE]",
      "",
      "Options:",
      "  --input PATH        Raw metadata fst with bare arxiv_id and raw abstract.",
      "  --output-dir PATH   Output lexical cache directory.",
      "  --chunk-size N      Number of rows to process per chunk. Default: 25000.",
      "  --overwrite BOOL    Overwrite lexical cache files if they already exist.",
      "  -h, --help          Show this help message.",
      "",
      "Output layout:",
      "  lexical/metadata.fst   doc_int, arxiv_id, abstract_norm, dl",
      "  lexical/postings.fst    doc_int, term, tf",
      "  lexical/vocab.fst       term, df, idf",
      "  lexical/manifest.json   build summary",
      sep = "\n"
    )
  )
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

parsed <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(parsed$show_help)) {
  usage()
  quit(save = "no", status = 0L)
}

if (is.null(parsed$input) || !nzchar(as.character(parsed$input)[[1L]])) {
  stop("`--input` must be supplied.", call. = FALSE)
}
raw_path <- normalizePath(parsed$input, winslash = "/", mustWork = FALSE)
if (!file.exists(raw_path)) {
  stop("Raw metadata file not found: ", raw_path, call. = FALSE)
}

raw_columns <- tryCatch(
  fst::metadata_fst(raw_path)$columnNames,
  error = function(e) character()
)
required <- c("arxiv_id", "abstract")
if (!all(required %in% raw_columns)) {
  stop(
    "Raw metadata fst must contain columns: ",
    paste(required, collapse = ", "),
    call. = FALSE
  )
}

if (is.null(parsed$output_dir) || !nzchar(as.character(parsed$output_dir)[[1L]])) {
  output_dir <- file.path(dirname(dirname(raw_path)), "lexical")
} else {
  output_dir <- normalizePath(parsed$output_dir, winslash = "/", mustWork = FALSE)
}
chunk_size <- suppressWarnings(as.integer(parsed$chunk_size[[1L]]))
if (is.na(chunk_size) || chunk_size < 1L) {
  stop("`--chunk-size` must be a positive integer.", call. = FALSE)
}
overwrite <- tolower(trimws(as.character(parsed$overwrite[[1L]]))) %in% c("true", "t", "1", "yes", "y")

file.exists_check <- file.exists(file.path(output_dir, c("metadata.fst", "postings.fst", "vocab.fst", "manifest.json")))
if (dir.exists(output_dir) && !isTRUE(overwrite) && any(file.exists_check)) {
  stop("Lexical output directory already contains cache files: ", output_dir, call. = FALSE)
}
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

raw_dt <- fst::read_fst(raw_path, as.data.table = TRUE, columns = c("arxiv_id", "abstract"))
raw_dt <- raw_dt[!is.na(arxiv_id) & nzchar(arxiv_id) & !is.na(abstract) & nzchar(abstract)]
if (!nrow(raw_dt)) {
  stop("Raw metadata file has no usable rows: ", raw_path, call. = FALSE)
}
if (anyDuplicated(raw_dt$arxiv_id)) {
  stop("Duplicate bare arXiv ids found in raw metadata: ", raw_path, call. = FALSE)
}

total_rows <- nrow(raw_dt)
chunk_starts <- seq.int(1L, total_rows, by = chunk_size)
docs_parts <- vector("list", length(chunk_starts))
postings_parts <- vector("list", length(chunk_starts))
part_n <- 0L
doc_offset <- 0L

for (i in seq_along(chunk_starts)) {
  start <- chunk_starts[[i]]
  end <- min(total_rows, start + chunk_size - 1L)
  ids <- as.character(raw_dt$arxiv_id[start:end])
  abstracts <- as.character(raw_dt$abstract[start:end])
  norm <- litxr::litxr_lexical_normalize_text(abstracts)
  if (!length(ids)) {
    next
  }
  if (anyDuplicated(ids)) {
    stop("Duplicate bare arXiv ids detected within lexical build chunk.", call. = FALSE)
  }

  local_n <- length(ids)
  doc_int <- seq.int(doc_offset + 1L, doc_offset + local_n)
  doc_offset <- doc_offset + local_n
  tokens <- litxr::litxr_lexical_tokenize(norm, normalize = FALSE)
  dl <- lengths(tokens)
  docs_parts[[i]] <- data.table(
    doc_int = as.integer(doc_int),
    arxiv_id = ids,
    abstract_norm = norm,
    dl = as.integer(dl)
  )

  token_vec <- unlist(tokens, use.names = FALSE)
  if (length(token_vec)) {
    token_doc <- rep.int(doc_int, dl)
    posting_dt <- data.table(
      doc_int = as.integer(token_doc),
      term = as.character(token_vec)
    )
    posting_dt <- posting_dt[!is.na(term) & nzchar(term)]
    if (nrow(posting_dt)) {
      part_n <- part_n + 1L
      postings_parts[[part_n]] <- posting_dt[, .(tf = .N), by = .(doc_int, term)]
    }
  }
}

docs_parts <- docs_parts[!vapply(docs_parts, is.null, logical(1))]
postings_parts <- postings_parts[!vapply(postings_parts, is.null, logical(1))]
docs <- if (length(docs_parts)) rbindlist(docs_parts, fill = TRUE) else data.table(doc_int = integer(), arxiv_id = character(), abstract_norm = character(), dl = integer())
postings <- if (length(postings_parts)) rbindlist(postings_parts, fill = TRUE) else data.table(doc_int = integer(), term = character(), tf = integer())

if (nrow(docs) && anyDuplicated(docs$arxiv_id)) {
  stop("Duplicate bare arXiv ids detected after lexical build.", call. = FALSE)
}
if (nrow(postings)) {
  setorder(postings, term, doc_int)
}

vocab <- if (nrow(postings)) {
  vocab_dt <- postings[, .(df = .N), by = term]
  vocab_dt[, idf := log(1 + (nrow(docs) - df + 0.5) / (df + 0.5))]
  vocab_dt
} else {
  data.table(term = character(), df = integer(), idf = numeric())
}
if (nrow(vocab)) {
  setorder(vocab, term)
}

metadata_path <- file.path(output_dir, "metadata.fst")
postings_path <- file.path(output_dir, "postings.fst")
vocab_path <- file.path(output_dir, "vocab.fst")
manifest_path <- file.path(output_dir, "manifest.json")

litxr:::.litxr_write_fst_atomic(docs, metadata_path)
litxr:::.litxr_write_fst_atomic(postings, postings_path)
litxr:::.litxr_write_fst_atomic(vocab, vocab_path)
jsonlite::write_json(
  list(
    raw_path = raw_path,
    output_dir = output_dir,
    rows = nrow(docs),
    postings_rows = nrow(postings),
    vocab_terms = nrow(vocab),
    created_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  ),
  path = manifest_path,
  auto_unbox = TRUE,
  pretty = TRUE,
  null = "null"
)

emit_json(list(
  status = "ok",
  input = raw_path,
  output_dir = output_dir,
  metadata_path = metadata_path,
  postings_path = postings_path,
  vocab_path = vocab_path,
  manifest_path = manifest_path,
  rows = nrow(docs),
  postings_rows = nrow(postings),
  vocab_terms = nrow(vocab)
))
