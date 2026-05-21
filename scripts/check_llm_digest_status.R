#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
})

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(args) {
  out <- list(
    show_help = FALSE,
    ref_id = NULL
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
      "  Rscript scripts/check_llm_digest_status.R --ref-id REF_ID",
      "",
      "Options:",
      "  --ref-id REF_ID     Canonical litxr ref_id to check.",
      "                      Bare arXiv ids like 2510.22085 are also accepted and normalized to arxiv:2510.22085.",
      "  -h, --help          Show this help message.",
      "",
      "Output:",
      "  Emits compact JSON with fields `status`, `ref_id`, and `already_digested`.",
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

result <- tryCatch(
  {
    cfg <- litxr::litxr_read_config()
    digest <- litxr::litxr_read_llm_digest(ref_id, cfg)
    list(
      status = "ok",
      ref_id = ref_id,
      already_digested = !is.null(digest)
    )
  },
  error = function(e) {
    message(conditionMessage(e))
    list(
      status = "error",
      ref_id = ref_id,
      already_digested = NULL
    )
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
