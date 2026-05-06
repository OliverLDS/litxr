#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
})

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(args) {
  out <- list(
    show_help = FALSE,
    ref_id = NULL,
    json_path = "~/Downloads/litxr_schema.json",
    mode = "create",
    prompt_version = "v3.0"
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
    if (identical(key, "--ref-id")) {
      out$ref_id <- value
    } else if (identical(key, "--json-path")) {
      out$json_path <- value
    } else if (identical(key, "--mode")) {
      out$mode <- value
    } else if (identical(key, "--prompt-version")) {
      out$prompt_version <- value
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
      "  Rscript scripts/build_llm_digest_interactive.R --ref-id REF_ID [--json-path ~/Downloads/litxr_schema.json] [--mode create|revise] [--prompt-version v3.0]",
      "",
      "Options:",
      "  --ref-id REF_ID     Canonical litxr ref_id to build a schema-v3 digest for.",
      "  --json-path PATH    Downloaded JSON path to ingest after ChatGPT returns the schema.",
      "                      Default: ~/Downloads/litxr_schema.json",
      "  --mode MODE         Either `create` or `revise`. Default: create",
      "  --prompt-version V  Prompt template version metadata to store on ingest.",
      "                      Default: v3.0",
      "  -h, --help          Show this help message.",
      "",
      "Workflow:",
      "  1. The script copies a prompt into the macOS clipboard with pbcopy.",
      "  2. ChatGPT should read the paper full text and return litxr_schema.json.",
      "  3. Download that file locally, then return to this script and type Y.",
      "  4. The script validates and writes the digest into the local litxr store.",
      sep = "\n"
    )
  )
}

as_scalar_chr <- function(x, default = NA_character_) {
  if (is.null(x) || !length(x) || is.na(x[[1]]) || !nzchar(as.character(x[[1]]))) {
    return(default)
  }
  as.character(x[[1]])
}

parsed <- parse_args(args)

if (isTRUE(parsed$show_help)) {
  usage()
  quit(save = "no", status = 0L)
}

if (is.null(parsed$ref_id) || !nzchar(parsed$ref_id)) {
  usage()
  stop("`--ref-id` is required.", call. = FALSE)
}

ref_id <- as.character(parsed$ref_id)
json_path <- path.expand(as.character(parsed$json_path))

cfg <- litxr::litxr_read_config()
ref <- litxr::litxr_find_refs(ref_id = ref_id, config = cfg)

if (!nrow(ref)) {
  stop("Reference not found in local litxr cache: ", ref_id, call. = FALSE)
}
if (nrow(ref) > 1L) {
  stop("Expected exactly one reference row for ", ref_id, " but found ", nrow(ref), ".", call. = FALSE)
}

mode <- tolower(trimws(as.character(parsed$mode)))
if (!(mode %in% c("create", "revise"))) {
  stop("`--mode` must be either `create` or `revise`.", call. = FALSE)
}

title_value <- as_scalar_chr(ref$title, ref_id)
doi_value <- as_scalar_chr(ref$doi)
source_value <- as_scalar_chr(ref$source)
source_id_value <- as_scalar_chr(ref$source_id)
template <- litxr::litxr_llm_digest_template(ref_id, schema_version = "v3")
existing_digest <- litxr::litxr_read_llm_digest(ref_id, cfg)
if (identical(mode, "create") && !is.null(existing_digest)) {
  stop("A local digest already exists for this ref_id. Use `--mode revise` to improve it.", call. = FALSE)
}
if (identical(mode, "revise") && is.null(existing_digest)) {
  stop("`--mode revise` requires an existing local digest for ref_id: ", ref_id, call. = FALSE)
}
template_json <- jsonlite::toJSON(
  template,
  auto_unbox = TRUE,
  pretty = TRUE,
  null = "null",
  na = "null"
)
paper_type_vocab <- paste(litxr::litxr_paper_type_levels(), collapse = ", ")

id_lines <- c(sprintf("ref_id: %s", ref_id))
if (!is.na(doi_value)) id_lines <- c(id_lines, sprintf("doi: %s", doi_value))
if (!is.na(source_id_value) && !identical(source_id_value, ref_id)) {
  source_label <- if (is.na(source_value) || !nzchar(source_value)) "source" else source_value
  id_lines <- c(id_lines, sprintf("%s_id: %s", source_label, source_id_value))
}
arxiv_hint_lines <- character()
if (startsWith(ref_id, "arxiv:")) {
  arxiv_id <- sub("^arxiv:", "", ref_id)
  arxiv_hint_lines <- c(
    "ArXiv-specific hint:",
    sprintf("  - HTML full text: %s", sprintf("https://arxiv.org/html/%s", arxiv_id)),
    sprintf("  - PDF full text: %s", sprintf("https://arxiv.org/pdf/%s", arxiv_id)),
    "  Use these direct links first before exploring other resources."
  )
}

prompt_lines <- c(
  "You are helping build a structured litxr schema-v3 JSON digest for one academic paper.",
  "",
  "Paper metadata:",
  sprintf("title: %s", title_value),
  id_lines,
  arxiv_hint_lines,
  "",
  "Instructions:",
  "1. Find the full text of this paper.",
  "2. Prefer an HTML full-text version if available.",
  "3. If HTML full text is not available, try to find a PDF version.",
  "4. Make sure you actually read the full text instead of guessing from abstract or metadata only.",
  "5. If you cannot find the full text, say clearly that you cannot find it and do not invent details.",
  "6. After reading the full text, parse the paper into the exact schema-v3 JSON below.",
  "7. Return a downloadable JSON file named litxr_schema.json.",
  "8. Do not add extra keys beyond this schema unless they already exist in the schema.",
  "9. Keep unknown fields explicit with null, empty string, or empty arrays as appropriate; do not guess.",
  sprintf("10. The accepted paper_type vocabulary is: %s.", paper_type_vocab),
  sprintf("11. This extraction is in `%s` mode with prompt_version `%s`.", mode, parsed$prompt_version),
  "12. Populate the optional inline v3 blocks explicitly when supported by the paper:",
  "    - anchor_references",
  "    - citation_logic_nodes",
  "    Use these inline blocks to provide up to three anchor references and reusable citation logic sentences.",
  "    Follow these field rules strictly:",
  "    - Do not collapse multiple meanings into one field.",
  "    - Do not put the whole explanation into citation_key and leave the other anchor fields empty.",
  "    - Do not leave claim_sentence empty or copy node_id into claim_sentence.",
  "    - If a field is unsupported by the paper, use null, empty string, or empty array instead of guessing.",
  "    Anchor reference field guidance:",
  "    - citation_key: short citation key such as boyd_2011_admm",
  "    - anchor_title: the prior-work title or short citation text",
  "    - anchor_role: one role label such as methodological_foundation, conceptual_foundation, review_anchor, main_comparison",
  "    - reason: why this prior work matters for understanding the current paper",
  "    - relationship_to_current_paper: one relationship such as builds_on, extends, compares_with, uses_as_context",
  "    Anchor reference example:",
  '    [{"anchor_rank":1,"citation_key":"boyd_2011_admm","anchor_title":"Distributed Optimization and Statistical Learning via the Alternating Direction Method of Multipliers","anchor_role":"methodological_foundation","reason":"Provides the ADMM background underlying the optimization workflow discussed in the paper.","relationship_to_current_paper":"builds_on","confidence":"high"}]',
  "    Citation logic node field guidance:",
  "    - claim_sentence: a reusable citation-ready sentence",
  "    - logic_type: the controlled relation label such as A_improves_B, A_reduces_B, A_supports_B, A_has_limitation_B",
  "    - subject_text: the main subject of the claim",
  "    - object_text: the main object or outcome of the claim",
  "    - modifier_text: qualifier, condition, mechanism, or relation detail when useful",
  "    - citation_use: when a future writer should cite this node",
  "    Citation logic node example:",
  '    [{"node_id":"node_1","claim_sentence":"Agentic workflows can reduce manual coordination burdens when reasoning, tool use, and validation are decomposed across specialized agents.","logic_type":"A_reduces_B","subject_text":"agentic workflows with specialized agents","object_text":"manual coordination burdens","modifier_text":"when reasoning, tool use, and validation are decomposed across agent roles","evidence_role":"conceptual_argument","confidence":"medium","page_or_section":"Section 3","quote_support":"","citation_use":"Use when arguing that agentic workflow design can reduce coordination-heavy manual work.","tags":["agentic workflow","coordination","specialized agents"]}]',
  "13. For research_data, keep sample_size numeric only when there is a real count.",
  "    If the paper only describes a scenario, benchmark size, application count, or other narrative context, put that text in sample_size_note instead of sample_size.",
  "14. Keep field semantics separate. For example:",
  "    - citation_key is a short key, not the entire prior-work explanation",
  "    - anchor_title is the title or short citation text",
  "    - anchor_role is the role label",
  "    - reason is the explanation",
  "    - claim_sentence is the actual reusable sentence",
  "    - logic_type is the controlled relation label",
  "",
  if (identical(mode, "revise")) "Existing local digest to improve:" else NULL,
  if (identical(mode, "revise")) jsonlite::toJSON(existing_digest, auto_unbox = TRUE, pretty = TRUE, null = "null", na = "null") else NULL,
  if (identical(mode, "revise")) "" else NULL,
  if (identical(mode, "revise")) "Revise the existing digest rather than rewriting blindly. Preserve correct fields, correct mistakes, and improve incomplete fields when the full text supports it. Keep the inline anchor_references and citation_logic_nodes blocks current and explicit." else NULL,
  if (identical(mode, "revise")) "" else NULL,
  "Return JSON matching this schema exactly:",
  template_json
)

prompt_text <- paste(prompt_lines, collapse = "\n")
clipboard <- pipe("pbcopy", open = "w")
writeLines(prompt_text, clipboard, useBytes = TRUE)
close(clipboard)

cat("Prompt copied to clipboard with pbcopy.\n\n")
cat("After downloading the returned file to:\n")
cat(sprintf("  %s\n\n", json_path))

cat("Type Y to ingest the downloaded JSON now, or N to stop: ")
stdin_con <- file("stdin")
answer <- readLines(stdin_con, n = 1L)
close(stdin_con)
answer <- toupper(trimws(answer))

if (!identical(answer, "Y")) {
  cat("Stopped without ingesting a digest.\n")
  quit(save = "no", status = 0L)
}

if (!file.exists(json_path)) {
  stop("Downloaded JSON file not found: ", json_path, call. = FALSE)
}

digest <- jsonlite::fromJSON(json_path, simplifyVector = FALSE)
json_ref_id <- if (!is.null(digest$ref_id) && length(digest$ref_id) && nzchar(as.character(digest$ref_id[[1]]))) {
  as.character(digest$ref_id[[1]])
} else {
  NA_character_
}
if (is.na(json_ref_id) || !identical(json_ref_id, ref_id)) {
  stop(
    "Downloaded JSON ref_id does not match the requested ref_id. Requested: ",
    ref_id,
    "; JSON: ",
    if (is.na(json_ref_id)) "[missing]" else json_ref_id,
    call. = FALSE
  )
}
digest$extraction_mode <- "chatgpt_manual"
digest$prompt_version <- as.character(parsed$prompt_version)
litxr::litxr_validate_llm_digest(digest)
litxr::litxr_write_llm_digest(
  ref_id,
  digest,
  cfg,
  keep_history = TRUE,
  bump_revision = identical(mode, "revise")
)

cat(sprintf("digest_written=%s mode=%s\n", ref_id, mode))
