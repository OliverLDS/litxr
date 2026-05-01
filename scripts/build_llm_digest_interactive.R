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
    prompt_version = "v2.1"
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
      "  Rscript scripts/build_llm_digest_interactive.R --ref-id REF_ID [--json-path ~/Downloads/litxr_schema.json] [--mode create|revise] [--prompt-version v2.1]",
      "",
      "Options:",
      "  --ref-id REF_ID     Canonical litxr ref_id to build a schema-v2 digest for.",
      "  --json-path PATH    Downloaded JSON path to ingest after ChatGPT returns the schema.",
      "                      Default: ~/Downloads/litxr_schema.json",
      "  --mode MODE         Either `create` or `revise`. Default: create",
      "  --prompt-version V  Prompt template version metadata to store on ingest.",
      "                      Default: v2.1",
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
template <- litxr::litxr_llm_digest_template(ref_id)
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

id_lines <- c(sprintf("ref_id: %s", ref_id))
if (!is.na(doi_value)) id_lines <- c(id_lines, sprintf("doi: %s", doi_value))
if (!is.na(source_id_value) && !identical(source_id_value, ref_id)) {
  source_label <- if (is.na(source_value) || !nzchar(source_value)) "source" else source_value
  id_lines <- c(id_lines, sprintf("%s_id: %s", source_label, source_id_value))
}

prompt_lines <- c(
  "You are helping build a structured litxr schema-v2 JSON digest for one academic paper.",
  "",
  "Paper metadata:",
  sprintf("title: %s", title_value),
  id_lines,
  "",
  "Instructions:",
  "1. Find the full text of this paper.",
  "2. Prefer an HTML full-text version if available.",
  "3. If HTML full text is not available, try to find a PDF version.",
  "4. Make sure you actually read the full text instead of guessing from abstract or metadata only.",
  "5. If you cannot find the full text, say clearly that you cannot find it and do not invent details.",
  "6. After reading the full text, parse the paper into the exact JSON schema below.",
  "7. Return a downloadable JSON file named litxr_schema.json.",
  "8. Do not add extra keys beyond this schema unless they already exist in the schema.",
  "9. Keep unknown fields explicit with null, empty string, or empty arrays as appropriate; do not guess.",
  sprintf("10. This extraction is in `%s` mode with prompt_version `%s`.", mode, parsed$prompt_version),
  "",
  if (identical(mode, "revise")) "Existing local digest to improve:" else NULL,
  if (identical(mode, "revise")) jsonlite::toJSON(existing_digest, auto_unbox = TRUE, pretty = TRUE, null = "null", na = "null") else NULL,
  if (identical(mode, "revise")) "" else NULL,
  if (identical(mode, "revise")) "Revise the existing digest rather than rewriting blindly. Preserve correct fields, correct mistakes, and improve incomplete fields when the full text supports it." else NULL,
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
