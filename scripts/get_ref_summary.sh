#!/bin/zsh

set -eu

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  cat <<'EOF'
Usage:
  scripts/get_ref_summary.sh <ref-id-or-bare-arxiv-id>

Options:
  -h, --help    Show this help message.

Notes:
  - Accepts a canonical ref_id like arxiv:2405.03710 or doi:10.1000/example.
  - Also accepts a bare arXiv id like 2405.03710.
  - Prints the local abstract first.
  - If a local research schema / LLM digest exists for that ref_id, prints a markdown-style rendering of it.
EOF
  exit 0
fi

if [[ $# -ne 1 ]]; then
  print -u2 "usage: $0 <ref-id-or-bare-arxiv-id>"
  exit 1
fi

if [[ "$1" == --* || "$1" == -?* ]]; then
  print -u2 "Unknown argument: $1"
  exit 1
fi

ref_key="${1}"

Rscript -e '
args <- commandArgs(trailingOnly = TRUE)
ref_key <- args[[1]]
if (!nzchar(ref_key)) {
  stop("One ref id is required.", call. = FALSE)
}
if (!grepl("^[A-Za-z0-9_]+:", ref_key) && grepl("^[0-9]{4}\\\\.[0-9]{4,5}(v[0-9]+)?$", ref_key)) {
  ref_key <- paste0("arxiv:", ref_key)
}

hits <- litxr::litxr_find_refs(ref_id = ref_key)
if (!nrow(hits)) {
  stop("No record found for ", ref_key, ".", call. = FALSE)
}
if (nrow(hits) != 1L) {
  stop("Expected exactly one record for ", ref_key, " but found ", nrow(hits), ".", call. = FALSE)
}

ref_id <- as.character(hits$ref_id[[1]])
title <- if ("title" %in% names(hits)) as.character(hits$title[[1]]) else ref_id
abstract <- if ("abstract" %in% names(hits)) as.character(hits$abstract[[1]]) else NA_character_

cat(sprintf("ref_id: %s\n", ref_id))
cat(sprintf("title: %s\n\n", title))
cat("abstract\n")
if (is.na(abstract) || !nzchar(trimws(abstract))) {
  cat("[missing]\n")
} else {
  cat(abstract, "\n", sep = "")
}

digest <- litxr::litxr_read_llm_digest(ref_id)

to_md_lines <- function(digest) {
  scalar_text <- function(x) {
    if (is.null(x) || !length(x) || is.na(x[[1]]) || !nzchar(trimws(as.character(x[[1]])))) return(NULL)
    as.character(x[[1]])
  }
  vector_lines <- function(title, x) {
    if (is.null(x) || !length(x)) return(character())
    vals <- as.character(unlist(x, use.names = FALSE))
    vals <- vals[!is.na(vals) & nzchar(trimws(vals))]
    if (!length(vals)) return(character())
    c(title, paste0("- ", vals), "")
  }
  inline_lines <- function(title, x) {
    if (is.null(x) || !length(x)) return(character())

    items <- NULL
    if (inherits(x, "data.frame")) {
      items <- lapply(seq_len(nrow(x)), function(i) as.list(x[i, , drop = FALSE]))
    } else if (is.list(x) && length(x) && all(vapply(x, is.list, logical(1L)))) {
      n_items <- max(vapply(x, function(el) sum(grepl("^V\\d+$", names(el))), integer(1L)), 0L)
      if (n_items > 0L) {
        items <- lapply(seq_len(n_items), function(i) {
          vals <- lapply(x, function(el) {
            nm <- paste0("V", i)
            if (nm %in% names(el)) el[[nm]] else NA_character_
          })
          names(vals) <- paste0("field_", seq_along(vals))
          vals
        })
      }
    }

    if (is.null(items) || !length(items)) return(character())
    lines <- vapply(seq_along(items), function(i) {
      vals <- as.character(unlist(items[[i]], use.names = FALSE))
      vals <- vals[!is.na(vals) & nzchar(trimws(vals))]
      if (!length(vals)) {
        sprintf("%d. [missing]", i)
      } else {
        sprintf("%d. %s", i, paste(vals, collapse = " — "))
      }
    }, character(1))
    c(title, lines, "")
  }
  named_scalar_lines <- function(title, x) {
    if (!is.list(x) || !length(x)) return(character())
    vals <- unlist(x, use.names = TRUE)
    vals <- vals[!is.na(vals) & nzchar(trimws(as.character(vals)))]
    if (!length(vals)) return(character())
    c(title, vapply(seq_along(vals), function(i) sprintf("- %s: %s", names(vals)[[i]], as.character(vals[[i]])), character(1)), "")
  }

  digest_ref <- scalar_text(digest$ref_id)
  if (is.null(digest_ref)) digest_ref <- ref_id
  lines <- c(
    sprintf("# %s", digest_ref),
    ""
  )
  if (!is.null(scalar_text(digest$schema_version))) lines <- c(lines, sprintf("- schema_version: %s", scalar_text(digest$schema_version)))
  if (!is.null(scalar_text(digest$paper_type))) lines <- c(lines, sprintf("- paper_type: %s", scalar_text(digest$paper_type)))
  if (!is.null(scalar_text(digest$evidence_strength))) lines <- c(lines, sprintf("- evidence_strength: %s", scalar_text(digest$evidence_strength)))
  if (tail(lines, 1) != "") lines <- c(lines, "")

  section_scalar <- function(title, x) {
    value <- scalar_text(x)
    if (is.null(value)) return(character())
    c(paste0("## ", title), value, "")
  }

  lines <- c(
    lines,
    section_scalar("Summary", digest$summary),
    section_scalar("Motivation", digest$motivation),
    vector_lines("## Research Questions", digest$research_questions),
    vector_lines("## Paper Structure", digest$paper_structure),
    vector_lines("## Methods", digest$methods),
    named_scalar_lines("## Research Data", digest$research_data),
    section_scalar("## Identification Strategy", digest$identification_strategy),
    named_scalar_lines("## Main Variables", digest$main_variables),
    vector_lines("## Key Findings", digest$key_findings),
    vector_lines("## Limitations", digest$limitations),
    section_scalar("## Theoretical Mechanism", digest$theoretical_mechanism),
    section_scalar("## Empirical Setting", digest$empirical_setting),
    section_scalar("## Descriptive Statistics Summary", digest$descriptive_statistics_summary),
    section_scalar("## Standardized Findings Summary", digest$standardized_findings_summary),
    vector_lines("## Contribution Type", digest$contribution_type),
    vector_lines("## Keywords", digest$keywords),
    inline_lines("## Anchor References", digest$anchor_references),
    inline_lines("## Citation Logic Nodes", digest$citation_logic_nodes),
    section_scalar("## Notes", digest$notes)
  )

  lines[nzchar(lines) | c(TRUE, head(nzchar(lines), -1L))]
}

cat("\nresearch_schema\n")
if (is.null(digest)) {
  cat("[missing]\n")
} else {
  cat(paste(to_md_lines(digest), collapse = "\n"), "\n", sep = "")
}
' "$ref_key"
