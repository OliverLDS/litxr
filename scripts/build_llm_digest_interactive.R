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
    prompt_version = "v4.0"
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
      "  Rscript scripts/build_llm_digest_interactive.R --ref-id REF_ID [--json-path ~/Downloads/litxr_schema.json] [--mode create|revise] [--prompt-version v4.0]",
      "",
      "Options:",
      "  --ref-id REF_ID     Canonical litxr ref_id to build a schema-v4 digest for.",
      "  --json-path PATH    Downloaded JSON path to ingest after ChatGPT returns the schema.",
      "                      Default: ~/Downloads/litxr_schema.json",
      "  --mode MODE         Either `create` or `revise`. Default: create",
      "  --prompt-version V  Prompt template version metadata to store on ingest.",
      "                      Default: v4.0",
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

mode <- tolower(trimws(as.character(parsed$mode)))
if (!(mode %in% c("create", "revise"))) {
  stop("`--mode` must be either `create` or `revise`.", call. = FALSE)
}

prompt_text <- litxr::litxr_llm_digest_prompt(
  ref_id = ref_id,
  config = cfg,
  mode = mode,
  schema_version = "v4",
  prompt_version = parsed$prompt_version
)
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
