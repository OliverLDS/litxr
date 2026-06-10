#!/usr/bin/env Rscript

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/human/report_ref_citation_use_from_bibtex.R --bibtex PATH [--output PATH]",
      "  Rscript scripts/human/report_ref_citation_use_from_bibtex.R PATH [--output PATH]",
      "",
      "Options:",
      "  --bibtex PATH   Input BibTeX file path.",
      "  --output PATH   Output markdown file path. Default: same path with .md suffix.",
      "  -h, --help      Show this help message.",
      "",
      "Behavior:",
      "  - Reads each BibTeX entry and resolves it to a local litxr reference.",
      "  - Resolution tries citekey/source_id/ref_id first, then DOI if present.",
      "  - Generates a markdown file listing ref_id and citation_use from",
      "    citation_logic_nodes in the latest research schema.",
      "  - Progress logs are written to stderr.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(
    help = FALSE,
    bibtex = NULL,
    output = NULL
  )
  positional <- character()
  i <- 1L

  while (i <= length(args)) {
    arg <- args[[i]]
    if (identical(arg, "-h") || identical(arg, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(arg, "--bibtex")) {
      if (i == length(args)) stop("Missing value for --bibtex", call. = FALSE)
      out$bibtex <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(arg, "--output")) {
      if (i == length(args)) stop("Missing value for --output", call. = FALSE)
      out$output <- args[[i + 1L]]
      i <- i + 2L
      next
    }
    if (startsWith(arg, "--")) {
      stop("Unknown argument: ", arg, call. = FALSE)
    }
    positional <- c(positional, arg)
    i <- i + 1L
  }

  if (!is.null(out$bibtex) && length(positional)) {
    stop("Use either a positional BibTeX path or --bibtex, not both.", call. = FALSE)
  }
  if (is.null(out$bibtex) && length(positional)) {
    out$bibtex <- positional[[1L]]
  }
  if (length(positional) > 1L) {
    stop("Unexpected positional argument: ", positional[[2L]], call. = FALSE)
  }

  out
}

read_bibtex_entries <- function(path) {
  if (!file.exists(path)) {
    stop("BibTeX file not found: ", path, call. = FALSE)
  }

  lines <- readLines(path, warn = FALSE)
  if (!length(lines)) {
    return(list(entries = character(), keys = character()))
  }

  entries <- character()
  keys <- character()
  i <- 1L
  while (i <= length(lines)) {
    line <- lines[[i]]
    if (!grepl("^\\s*@", line)) {
      i <- i + 1L
      next
    }

    key <- sub("^\\s*@\\w+\\s*\\{\\s*([^,]+),.*$", "\\1", line)
    if (!nzchar(key) || identical(key, line)) {
      stop("Unable to parse BibTeX entry key from line: ", line, call. = FALSE)
    }

    chunk <- character()
    depth <- 0L
    repeat {
      if (i > length(lines)) {
        stop("Unterminated BibTeX entry for key: ", key, call. = FALSE)
      }
      current <- lines[[i]]
      chunk <- c(chunk, current)
      opens <- lengths(regmatches(current, gregexpr("\\{", current, perl = TRUE)))
      closes <- lengths(regmatches(current, gregexpr("\\}", current, perl = TRUE)))
      opens[opens < 0L] <- 0L
      closes[closes < 0L] <- 0L
      depth <- depth + opens - closes
      i <- i + 1L
      if (depth <= 0L) {
        break
      }
    }

    entries <- c(entries, paste(chunk, collapse = "\n"))
    keys <- c(keys, key)
  }

  list(entries = entries, keys = keys)
}

extract_bib_field <- function(entry_lines, field) {
  pattern <- sprintf("^\\s*%s\\s*=\\s*([\\{\\\"])(.*)[\\}\\\"],?\\s*$", field)
  hit <- grep(pattern, entry_lines, perl = TRUE)
  if (!length(hit)) {
    return(NA_character_)
  }
  value <- sub(pattern, "\\2", entry_lines[[hit[[1L]]]], perl = TRUE)
  value <- trimws(value)
  if (!nzchar(value)) {
    return(NA_character_)
  }
  value
}

normalize_lookup_value <- function(x) {
  x <- as.character(x)
  if (!length(x) || is.na(x[[1L]])) return(NA_character_)
  x <- trimws(x[[1L]])
  if (!nzchar(x)) return(NA_character_)

  if (grepl("^https?://(dx\\.)?doi\\.org/", x, ignore.case = TRUE)) {
    x <- sub("^https?://(dx\\.)?doi\\.org/", "", x, ignore.case = TRUE)
  }
  if (grepl("^doi:", x, ignore.case = TRUE)) {
    x <- sub("^doi:", "", x, ignore.case = TRUE)
  }
  if (grepl("^[0-9]{4}_[0-9]{4,5}(v[0-9]+)?$", x)) {
    x <- paste0("arxiv:", sub("_", ".", x, fixed = TRUE))
  } else if (grepl("^[0-9]{4}\\.[0-9]{4,5}(v[0-9]+)?$", x)) {
    x <- paste0("arxiv:", x)
  }
  x
}

scalar_text <- function(x) {
  if (is.null(x) || !length(x)) return(NA_character_)
  vals <- as.character(unlist(x, use.names = FALSE))
  vals <- vals[!is.na(vals) & nzchar(trimws(vals))]
  if (!length(vals)) return(NA_character_)
  vals[[1L]]
}

pick_preferred_row <- function(rows, doi = NA_character_) {
  if (!nrow(rows)) {
    return(rows)
  }

  if ("source" %in% names(rows)) {
    non_arxiv <- rows[rows$source != "arxiv", ]
    if (nrow(non_arxiv)) {
      rows <- non_arxiv
    }
  }

  if (!is.na(doi) && nzchar(doi) && "doi" %in% names(rows)) {
    exact <- rows[rows$doi == doi, ]
    if (nrow(exact)) {
      rows <- exact
    }
  }

  rows[1L, ]
}

resolve_reference <- function(entry_lines, bib_key, cfg) {
  doi <- extract_bib_field(entry_lines, "doi")
  doi <- normalize_lookup_value(doi)
  key_candidates <- unique(stats::na.omit(c(
    normalize_lookup_value(bib_key),
    if (!is.na(doi) && nzchar(doi)) paste0("doi:", doi) else NA_character_,
    if (!is.na(doi) && nzchar(doi)) doi else NA_character_
  )))

  for (candidate in key_candidates) {
    rows <- data.table::as.data.table(litxr::litxr_find_refs(ref_id = candidate, config = cfg))
    if (nrow(rows)) {
      rows <- pick_preferred_row(rows, doi = doi)
      return(list(resolved = TRUE, ref = rows, matched_by = candidate))
    }
  }

  if (!is.na(doi) && nzchar(doi)) {
    rows <- data.table::as.data.table(litxr::litxr_find_refs(doi = doi, config = cfg))
    if (nrow(rows)) {
      rows <- pick_preferred_row(rows, doi = doi)
      return(list(resolved = TRUE, ref = rows, matched_by = paste0("doi:", doi)))
    }
  }

  list(resolved = FALSE, ref = data.frame(), matched_by = NA_character_)
}

read_citation_use <- function(ref_id, cfg) {
  digest <- tryCatch(litxr::litxr_read_llm_digest(ref_id, cfg), error = function(e) NULL)
  if (is.null(digest) || !length(digest$citation_logic_nodes)) {
    return(character())
  }

  nodes <- digest$citation_logic_nodes
  if (inherits(nodes, "data.frame")) {
    nodes <- split(nodes, seq_len(nrow(nodes)))
  }
  if (!is.list(nodes)) {
    return(character())
  }

  node_text <- vapply(nodes, function(node) {
    if (!is.list(node) || !length(node)) return(NA_character_)
    if ("citation_use" %in% names(node)) {
      val <- scalar_text(node[["citation_use"]])
      if (!is.na(val) && nzchar(val)) return(val)
    }
    NA_character_
  }, character(1))
  node_text <- node_text[!is.na(node_text) & nzchar(trimws(node_text))]
  unique(node_text)
}

args <- commandArgs(trailingOnly = TRUE)
parsed <- parse_args(args)

if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}

if (is.null(parsed$bibtex) || !nzchar(trimws(parsed$bibtex))) {
  usage()
  stop("Missing BibTeX input path.", call. = FALSE)
}

bibtex_path <- path.expand(parsed$bibtex)
if (!file.exists(bibtex_path)) {
  stop("BibTeX file not found: ", bibtex_path, call. = FALSE)
}

if (is.null(parsed$output) || !nzchar(trimws(parsed$output))) {
  output_path <- paste0(tools::file_path_sans_ext(bibtex_path), ".citation_use.md")
} else {
  output_path <- path.expand(parsed$output)
}

cfg <- litxr::litxr_read_config()
entries <- read_bibtex_entries(bibtex_path)
if (!length(entries$entries)) {
  stop("No BibTeX entries found in: ", bibtex_path, call. = FALSE)
}

report_lines <- c(
  "# BibTeX Citation Use",
  "",
  sprintf("- Source BibTeX: `%s`", bibtex_path),
  sprintf("- Generated at: `%s`", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z", tz = "UTC")),
  sprintf("- Entry count: `%d`", length(entries$entries)),
  "",
  "This report resolves each BibTeX entry to a local `litxr` reference when possible.",
  "BibTeX citekeys may differ from `ref_id`; when that happens, the script falls back to DOI-based lookup.",
  ""
)

missing <- character()
for (i in seq_along(entries$entries)) {
  entry_lines <- strsplit(entries$entries[[i]], "\n", fixed = TRUE)[[1L]]
  bib_key <- entries$keys[[i]]
  resolved <- resolve_reference(entry_lines, bib_key, cfg)
  title <- extract_bib_field(entry_lines, "title")
  title <- if (!is.na(title) && nzchar(title)) title else "[missing]"

  if (isTRUE(resolved$resolved) && nrow(resolved$ref)) {
    ref <- resolved$ref
    ref_id <- scalar_text(ref$ref_id)
    title_local <- if ("title" %in% names(ref)) scalar_text(ref$title) else NA_character_
    if (!is.na(title_local) && nzchar(title_local)) {
      title <- title_local
    }
    citation_use <- read_citation_use(ref_id, cfg)
    matched_by <- resolved$matched_by
  } else {
    ref_id <- NA_character_
    citation_use <- character()
    matched_by <- NA_character_
    missing <- c(missing, bib_key)
  }

  report_lines <- c(
    report_lines,
    sprintf("## %d. %s", i, if (!is.na(ref_id) && nzchar(ref_id)) ref_id else paste0("[unresolved] ", bib_key)),
    "",
    sprintf("- BibTeX key: `%s`", bib_key),
    sprintf("- Resolved by: `%s`", if (!is.na(matched_by) && nzchar(matched_by)) matched_by else "[unresolved]"),
    sprintf("- Ref ID: `%s`", if (!is.na(ref_id) && nzchar(ref_id)) ref_id else "[missing]"),
    sprintf("- Title: %s", title),
    if (length(citation_use)) {
      c("- Citation Use:", paste0("  - ", citation_use))
    } else {
      "- Citation Use: [missing]"
    },
    ""
  )
}

dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
writeLines(report_lines, output_path)

log_line(sprintf("written=%s", normalizePath(output_path, winslash = "/", mustWork = FALSE)))
if (length(missing)) {
  log_line(sprintf("unresolved_bib_keys=%s", paste(unique(missing), collapse = ", ")))
}

