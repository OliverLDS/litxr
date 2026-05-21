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

.normalize_ref_id_input <- function(ref_id) {
  ref_id <- as.character(ref_id)
  if (!length(ref_id) || is.na(ref_id[[1]])) {
    return(ref_id)
  }
  ref_id <- trimws(ref_id[[1]])
  if (grepl("^[0-9]{4}\\.[0-9]{4,5}(v[0-9]+)?$", ref_id)) {
    return(paste0("arxiv:", ref_id))
  }
  ref_id
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/ingest_llm_digest_json.R --ref-id REF_ID [--json-path ~/Downloads/litxr_schema.json] [--mode create|revise] [--prompt-version v4.0]",
      "",
      "Options:",
      "  --ref-id REF_ID     Canonical litxr ref_id to ingest for.",
      "                      Bare arXiv ids like 2510.22085 are also accepted and normalized to arxiv:2510.22085.",
      "  --json-path PATH    Downloaded JSON file to ingest.",
      "                      Default: ~/Downloads/litxr_schema.json",
      "  --mode MODE         Either `create` or `revise`. Default: create",
      "  --prompt-version V  Prompt template version metadata to store on ingest.",
      "                      Default: v4.0",
      "  -h, --help          Show this help message.",
      "",
      "Output:",
      "  Emits compact JSON with fields `status` and `digest_written`.",
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

ref_id <- .normalize_ref_id_input(parsed$ref_id)
json_path <- path.expand(as.character(parsed$json_path))
mode <- tolower(trimws(as.character(parsed$mode)))
if (!(mode %in% c("create", "revise"))) {
  stop("`--mode` must be either `create` or `revise`.", call. = FALSE)
}

result <- tryCatch(
  {
    cfg <- litxr::litxr_read_config()
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

    list(status = "ok", digest_written = ref_id, mode = mode)
  },
  error = function(e) {
    message(conditionMessage(e))
    list(status = "error", digest_written = NULL, mode = mode)
  }
)

cat(
  jsonlite::toJSON(
    result,
    auto_unbox = TRUE,
    null = "null",
    pretty = FALSE
  )
)

if (!identical(result$status, "ok")) {
  quit(save = "no", status = 1L)
}
