#!/bin/zsh

set -eu

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  cat <<'EOF'
Usage:
  scripts/human/get_ref_summary.sh <ref-id-or-bare-arxiv-id> [--report key|complete]
  scripts/human/get_ref_summary.sh <ref-id-or-bare-arxiv-id> [--key|--complete]

Options:
  --report MODE  Report mode: key or complete. Default: key.
  --key          Report key research-schema fields only. This is the default.
  --complete     Report the complete research-schema digest.
  -h, --help     Show this help message.

Notes:
  - Accepts a canonical ref_id like arxiv:2405.03710 or doi:10.1000/example.
  - Also accepts a bare arXiv id like 2405.03710.
  - Prints the local abstract first.
  - Key mode reports Summary, Motivation, Theoretical Mechanism,
    Anchor References, and Citation Logic Nodes.
  - Complete mode reports the full markdown-style research schema rendering.
EOF
  exit 0
fi

if [[ $# -lt 1 ]]; then
  print -u2 "usage: $0 <ref-id-or-bare-arxiv-id> [--report key|complete]"
  exit 1
fi

ref_key=""
report_mode="key"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      print -u2 "usage: $0 <ref-id-or-bare-arxiv-id> [--report key|complete]"
      exit 0
      ;;
    --report)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --report"
        exit 1
      fi
      report_mode="$2"
      shift 2
      ;;
    --key)
      report_mode="key"
      shift
      ;;
    --complete)
      report_mode="complete"
      shift
      ;;
    --*)
      print -u2 "Unknown argument: $1"
      exit 1
      ;;
    -*)
      print -u2 "Unknown argument: $1"
      exit 1
      ;;
    *)
      if [[ -n "$ref_key" ]]; then
        print -u2 "Unexpected positional argument: $1"
        exit 1
      fi
      ref_key="$1"
      shift
      ;;
  esac
done

if [[ -z "$ref_key" ]]; then
  print -u2 "Missing <ref-id-or-bare-arxiv-id>"
  exit 1
fi

if [[ "$report_mode" != "key" && "$report_mode" != "complete" ]]; then
  print -u2 "--report must be either key or complete"
  exit 1
fi

Rscript - "$ref_key" "$report_mode" <<'EOF'
args <- commandArgs(trailingOnly = TRUE)
ref_key <- args[[1]]
report_mode <- args[[2]]
if (!nzchar(ref_key)) {
  stop("One ref id is required.", call. = FALSE)
}
if (!grepl("^[A-Za-z0-9_]+:", ref_key) && grepl("^[0-9]{4}\\.[0-9]{4,5}(v[0-9]+)?$", ref_key)) {
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

  to_md_lines <- function(digest, report_mode = "key") {
    scalar_text <- function(x) {
      if (is.null(x) || !length(x) || is.na(x[[1]]) || !nzchar(trimws(as.character(x[[1]])))) return(NULL)
      as.character(x[[1]])
    }
    extract_scalar <- function(value) {
      vals <- as.character(unlist(value, use.names = FALSE))
      vals <- vals[!is.na(vals) & nzchar(trimws(vals))]
      if (!length(vals)) return(NA_character_)
      vals[[1]]
    }
    extract_tags <- function(value) {
      vals <- as.character(unlist(value, use.names = FALSE))
      vals <- vals[!is.na(vals) & nzchar(trimws(vals))]
      if (!length(vals)) return(character())
      vals <- unlist(strsplit(vals, "\\s*,\\s*"))
      vals <- trimws(vals)
      vals <- vals[nzchar(vals)]
      unique(vals)
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
      n_items <- max(vapply(x, function(el) sum(grepl("^V[0-9]+$", names(el))), integer(1L)), 0L)
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
    preferred_fields <- function(title) {
      if (identical(title, "## Anchor References")) {
        return(c(
          "anchor_title", "reference", "citation_key", "anchor_role", "role",
          "reason", "relationship_to_current_paper", "confidence",
          "anchor_ref_id", "anchor_year"
        ))
      }
      if (identical(title, "## Citation Logic Nodes")) {
        return(c(
          "node_id", "logic_type", "claim_sentence", "sentence", "relation",
          "reuse_context", "subject_text", "object_text", "modifier_text",
          "evidence_role", "citation_use", "confidence", "page_or_section"
        ))
      }
      character()
    }
    format_item <- function(item, title) {
      if (is.list(item) && length(item) && !is.null(names(item))) {
        fields <- preferred_fields(title)
        if (length(fields)) {
          vals <- unlist(item[intersect(fields, names(item))], use.names = FALSE)
        } else {
          vals <- unlist(item, use.names = FALSE)
        }
      } else {
        vals <- as.character(unlist(item, use.names = FALSE))
      }
      vals <- vals[!is.na(vals) & nzchar(trimws(vals))]
      vals
    }
    merge_citation_node_items <- function(items) {
      if (!length(items)) return(items)
      key_for <- function(item, fallback_idx) {
        if (is.list(item) && !is.null(names(item)) && "node_id" %in% names(item)) {
          key <- extract_scalar(item[["node_id"]])
          if (!is.na(key) && nzchar(key)) return(key)
        }
        sprintf(".row_%d", fallback_idx)
      }
      keys <- vapply(seq_along(items), function(i) key_for(items[[i]], i), character(1))
      ordered_keys <- unique(keys)
      merged <- lapply(ordered_keys, function(key) {
        idx <- which(keys == key)
        group <- items[idx]
        merged_item <- list()
        merged_item$node_id <- extract_scalar(lapply(group, function(x) x[["node_id"]]))
        if (is.na(merged_item$node_id) || !nzchar(merged_item$node_id)) merged_item$node_id <- key
        scalar_fields <- c(
          "logic_type", "claim_sentence", "sentence", "relation", "reuse_context",
          "subject_text", "object_text", "modifier_text", "evidence_role",
          "citation_use", "confidence", "page_or_section"
        )
        for (field in scalar_fields) {
          merged_item[[field]] <- extract_scalar(lapply(group, function(x) x[[field]]))
        }
        merged_item$tags <- extract_tags(lapply(group, function(x) x[["tags"]]))
        merged_item
      })
      merged
    }
    if (identical(title, "## Citation Logic Nodes")) {
      items <- merge_citation_node_items(items)
    }
    lines <- vapply(seq_along(items), function(i) {
      vals <- format_item(items[[i]], title)
      if (identical(title, "## Citation Logic Nodes") && is.list(items[[i]]) && !is.null(names(items[[i]]))) {
        node_tags <- items[[i]]$tags
        if (length(node_tags)) {
          vals <- c(vals, sprintf("tags: [%s]", paste(sprintf("\"%s\"", node_tags), collapse = ", ")))
        }
      }
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
    structured_lines <- function(title, x, field_order = NULL) {
      if (is.null(x) || !length(x)) return(character())
      items <- NULL
      if (inherits(x, "data.frame")) {
        items <- lapply(seq_len(nrow(x)), function(i) as.list(x[i, , drop = FALSE]))
      } else if (is.list(x) && length(x) && all(vapply(x, is.list, logical(1L)))) {
        items <- x
      }
      if (is.null(items) || !length(items)) return(character())
      fmt <- function(item) {
        if (!is.list(item) || !length(item)) return(character())
        vals <- item
        if (!is.null(field_order)) {
          keep <- intersect(field_order, names(vals))
          if (length(keep)) vals <- vals[keep]
        }
        if (identical(title, "## Ranked Contributions")) {
          rank <- if ("rank" %in% names(vals)) extract_scalar(vals[["rank"]]) else NA_character_
          contrib_type <- if ("contribution_type" %in% names(vals)) extract_scalar(vals[["contribution_type"]]) else NA_character_
          contrib <- if ("contribution" %in% names(vals)) extract_scalar(vals[["contribution"]]) else NA_character_
          reason <- if ("reason" %in% names(vals)) extract_scalar(vals[["reason"]]) else NA_character_
          parts <- c()
          if (!is.na(rank) && nzchar(rank)) parts <- c(parts, paste0("rank=", rank))
          if (!is.na(contrib_type) && nzchar(contrib_type)) parts <- c(parts, paste0("type=", contrib_type))
          if (!is.na(contrib) && nzchar(contrib)) parts <- c(parts, contrib)
          if (!is.na(reason) && nzchar(reason)) parts <- c(parts, paste0("reason=", reason))
          return(parts)
        }
        if (identical(title, "## Evidence Shape")) {
          vals <- vals[intersect(c("evidence_mode", "inference_type", "strength_level", "evidence_basis", "limitations"), names(vals))]
        }
        vals <- unlist(vals, use.names = FALSE)
        vals <- as.character(vals)
        vals <- vals[!is.na(vals) & nzchar(trimws(vals))]
        vals
      }
      lines <- vapply(seq_along(items), function(i) {
        vals <- fmt(items[[i]])
        if (!length(vals)) {
          sprintf("%d. [missing]", i)
        } else {
          sprintf("%d. %s", i, paste(vals, collapse = " — "))
        }
      }, character(1))
      c(title, lines, "")
    }

  digest_ref <- scalar_text(digest$ref_id)
  if (is.null(digest_ref)) digest_ref <- ref_id
  lines <- c(
    sprintf("# %s", digest_ref),
    ""
  )

  section_scalar <- function(title, x) {
    value <- scalar_text(x)
    if (is.null(value)) return(character())
    c(paste0("## ", title), value, "")
  }

  if (identical(report_mode, "key")) {
    lines <- c(
      lines,
      section_scalar("Summary", digest$summary),
      section_scalar("Motivation", digest$motivation),
      section_scalar("Theoretical Mechanism", digest$theoretical_mechanism),
      inline_lines("## Anchor References", digest$anchor_references),
      inline_lines("## Citation Logic Nodes", digest$citation_logic_nodes)
    )
    return(lines[nzchar(lines) | c(TRUE, head(nzchar(lines), -1L))])
  }

  if (!is.null(scalar_text(digest$schema_version))) lines <- c(lines, sprintf("- schema_version: %s", scalar_text(digest$schema_version)))
  if (!is.null(scalar_text(digest$paper_type))) lines <- c(lines, sprintf("- paper_type: %s", scalar_text(digest$paper_type)))
  if (!is.null(scalar_text(digest$evidence_strength))) lines <- c(lines, sprintf("- evidence_strength: %s", scalar_text(digest$evidence_strength)))
  if (tail(lines, 1) != "") lines <- c(lines, "")

    lines <- c(
      lines,
      section_scalar("Summary", digest$summary),
      section_scalar("Motivation", digest$motivation),
      vector_lines("## Research Questions", digest$research_questions),
    vector_lines("## Paper Structure", digest$paper_structure),
    vector_lines("## Methods", digest$methods),
    named_scalar_lines("## Research Data", digest$research_data),
    section_scalar("Identification Strategy", digest$identification_strategy),
    named_scalar_lines("## Main Variables", digest$main_variables),
      vector_lines("## Key Findings", digest$key_findings),
      vector_lines("## Limitations", digest$limitations),
      section_scalar("Theoretical Mechanism", digest$theoretical_mechanism),
      section_scalar("Empirical Setting", digest$empirical_setting),
      section_scalar("Descriptive Statistics Summary", digest$descriptive_statistics_summary),
      section_scalar("Standardized Findings Summary", digest$standardized_findings_summary),
      vector_lines("## Contribution Type", digest$contribution_type),
      structured_lines("## Ranked Contributions", digest$ranked_contributions),
      vector_lines("## Likely Reader Misconceptions", digest$likely_reader_misconceptions),
      vector_lines("## Business Relevance Pathway", digest$business_relevance_pathway),
      structured_lines("## Evidence Shape", digest$evidence_shape),
      vector_lines("## Keywords", digest$keywords),
      inline_lines("## Anchor References", digest$anchor_references),
      inline_lines("## Citation Logic Nodes", digest$citation_logic_nodes),
      section_scalar("Notes", digest$notes)
  )

  lines[nzchar(lines) | c(TRUE, head(nzchar(lines), -1L))]
}

cat("\nresearch_schema\n")
if (is.null(digest)) {
  cat("[missing]\n")
} else {
  cat(paste(to_md_lines(digest, report_mode = report_mode), collapse = "\n"), "\n", sep = "")
}
EOF
