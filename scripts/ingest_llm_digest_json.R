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
    json_path_provided = FALSE,
    json_raw = NULL,
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
      out$json_path_provided <- TRUE
    } else if (identical(key, "--json-raw")) {
      out$json_raw <- value
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
    return(sub("v[0-9]+$", "", ref_id))
  }
  if (grepl("^arxiv:", ref_id, ignore.case = TRUE)) {
    ref_id <- sub("^arxiv:", "", ref_id, ignore.case = TRUE)
    ref_id <- trimws(ref_id)
    ref_id <- sub("v[0-9]+$", "", ref_id)
    return(ref_id)
  }
  ref_id
}

normalize_inline_json <- function(text) {
  text <- as.character(text)
  if (!length(text) || is.na(text[[1]])) {
    return(text)
  }
  text <- paste(text, collapse = "\n")
  text <- trimws(text)
  if (grepl("^```", text) && grepl("```\\s*$", text)) {
    text <- sub("^```json\\s*", "", text)
    text <- sub("^```\\s*", "", text)
    text <- sub("\\s*```\\s*$", "", text)
  }
  text
}

first_or_null <- function(x) {
  if (is.null(x) || !length(x)) {
    return(NULL)
  }
  x[[1]]
}

or_na_character <- function(x) {
  if (is.null(x) || !length(x) || is.na(x[[1]]) || !nzchar(as.character(x[[1]]))) {
    return(NA_character_)
  }
  as.character(x[[1]])
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/ingest_llm_digest_json.R --ref-id REF_ID [--json-path ~/Downloads/litxr_schema.json | --json-raw RAW_JSON] [--mode create|revise] [--prompt-version v4.0]",
      "",
      "Options:",
      "  --ref-id REF_ID     Ref id to ingest for.",
      "                      Bare arXiv ids like 2510.22085 are accepted.",
      "  --json-path PATH    Downloaded JSON file to ingest.",
      "                      Default: ~/Downloads/litxr_schema.json",
      "  --json-raw JSON     Raw JSON text to ingest inline.",
      "                      Use this when the upstream prompt asks for inline",
      "                      raw JSON instead of a downloadable file.",
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

if (!is.null(parsed$json_raw) && nzchar(parsed$json_raw)) {
  parsed$json_path <- NULL
  parsed$json_path_provided <- FALSE
}

if (is.null(parsed$ref_id) || !nzchar(parsed$ref_id)) {
  usage()
  stop("`--ref-id` is required.", call. = FALSE)
}

ref_id <- .normalize_ref_id_input(parsed$ref_id)
json_path <- if (is.null(parsed$json_path)) NULL else path.expand(as.character(parsed$json_path))
json_raw <- parsed$json_raw
mode <- tolower(trimws(as.character(parsed$mode)))
if (!(mode %in% c("create", "revise"))) {
  stop("`--mode` must be either `create` or `revise`.", call. = FALSE)
}
pkg_version <- as.character(utils::packageVersion("litxr"))
if (utils::compareVersion(pkg_version, "0.0.8.6") < 0) {
  stop(
    "Installed litxr version is ", pkg_version,
    ", but this CLI requires litxr >= 0.0.8.6. Reinstall the package from the current workspace first.",
    call. = FALSE
  )
}
if (isTRUE(parsed$json_path_provided) && !is.null(json_raw) && nzchar(json_raw) && !is.null(json_path) && file.exists(json_path)) {
  warning("Both `--json-path` and `--json-raw` were provided; `--json-raw` will be used.", call. = FALSE)
}

result <- tryCatch(
  {
    cfg <- litxr::litxr_read_config()
    existing <- litxr:::litxr_read_llm_digest(ref_id, cfg)
    existing_revision <- if (is.null(existing) || is.null(existing$digest_revision)) {
      NA_integer_
    } else {
      suppressWarnings(as.integer(first_or_null(existing$digest_revision)))
    }

    if (!is.null(json_raw) && nzchar(json_raw)) {
      digest <- jsonlite::fromJSON(normalize_inline_json(json_raw), simplifyVector = FALSE)
    } else {
      if (!file.exists(json_path)) {
        stop("Downloaded JSON file not found: ", json_path, call. = FALSE)
      }
      digest <- jsonlite::fromJSON(json_path, simplifyVector = FALSE)
    }
    json_ref_id <- if (!is.null(digest$ref_id) && length(digest$ref_id) && nzchar(as.character(digest$ref_id[[1]]))) {
      as.character(digest$ref_id[[1]])
    } else {
      NA_character_
    }
    if (!is.na(json_ref_id) && grepl("^arxiv:", json_ref_id, ignore.case = TRUE)) {
      json_ref_id <- sub("^arxiv:", "", json_ref_id, ignore.case = TRUE)
      json_ref_id <- trimws(json_ref_id)
      json_ref_id <- sub("v[0-9]+$", "", json_ref_id)
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
    litxr:::litxr_validate_llm_digest(digest)
    litxr:::litxr_write_llm_digest(
      ref_id,
      digest,
      cfg,
      keep_history = TRUE,
      bump_revision = identical(mode, "revise")
    )
    written <- litxr:::litxr_read_llm_digest(ref_id, cfg)
    written_revision <- if (is.null(written) || is.null(written$digest_revision)) {
      NA_integer_
    } else {
      suppressWarnings(as.integer(first_or_null(written$digest_revision)))
    }
    if (identical(mode, "revise") && !is.na(existing_revision) && !is.na(written_revision) &&
        written_revision <= existing_revision) {
      stop(
        "Revise ingest did not advance digest_revision. Existing revision: ",
        existing_revision,
        "; stored revision after write: ",
        written_revision,
        call. = FALSE
      )
    }

    list(
      status = "ok",
      digest_written = ref_id,
      mode = mode,
      stored_revision = written_revision,
      stored_updated_at = or_na_character(written$updated_at),
      stored_generated_at = or_na_character(written$generated_at)
    )
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
