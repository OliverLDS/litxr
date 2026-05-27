#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
})

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(args) {
  out <- list(
    show_help = FALSE,
    ref_id = NULL,
    mode = "create",
    prompt_version = "v4.0",
    return_format = "download_json_file"
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
    } else if (identical(key, "--mode")) {
      out$mode <- value
    } else if (identical(key, "--prompt-version")) {
      out$prompt_version <- value
    } else if (identical(key, "--return-format")) {
      out$return_format <- value
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
      "  Rscript scripts/build_llm_digest_prompt.R --ref-id REF_ID [--mode create|revise] [--prompt-version v4.0] [--return-format download_json_file|inline_raw_json|markdown_fenced_json]",
      "",
      "Options:",
      "  --ref-id REF_ID     Canonical litxr ref_id to build a digest prompt for.",
      "                      Bare arXiv ids like 2510.22085 are also accepted and normalized to arxiv:2510.22085.",
      "  --mode MODE         Either `create` or `revise`. Default: create",
      "  --prompt-version V  Prompt template version metadata to include.",
      "                      Default: v4.0",
      "  --return-format F   Expected returned format from the external LLM.",
      "                      `download_json_file` asks for a downloadable",
      "                      litxr_schema.json file.",
      "                      `inline_raw_json` asks for raw JSON only, with no",
      "                      Markdown fence, no explanation, and no surrounding",
      "                      text.",
      "                      `markdown_fenced_json` asks for JSON inside a fenced",
      "                      Markdown code block with `json` after the opening",
      "                      backticks.",
      "                      Default: download_json_file",
      "  -h, --help          Show this help message.",
      "",
      "Output:",
      "  Emits compact JSON with fields `status` and `prompt`.",
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
mode <- tolower(trimws(as.character(parsed$mode)))
if (!(mode %in% c("create", "revise"))) {
  stop("`--mode` must be either `create` or `revise`.", call. = FALSE)
}
return_format <- trimws(as.character(parsed$return_format))
if (!(return_format %in% c("download_json_file", "inline_raw_json", "markdown_fenced_json"))) {
  stop("`--return-format` must be one of `download_json_file`, `inline_raw_json`, or `markdown_fenced_json`.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()

pkg_version <- as.character(utils::packageVersion("litxr"))
if (utils::compareVersion(pkg_version, "0.0.8.6") < 0) {
  stop(
    "Installed litxr version is ", pkg_version,
    ", but this CLI requires litxr >= 0.0.8.6. Reinstall the package from the current workspace first.",
    call. = FALSE
  )
}

result <- tryCatch(
  {
    prompt <- litxr::litxr_llm_digest_prompt(
      ref_id = ref_id,
      config = cfg,
      mode = mode,
      schema_version = "v4",
      prompt_version = parsed$prompt_version
    )
    prompt <- switch(
      return_format,
      download_json_file = prompt,
      inline_raw_json = gsub(
        "Return a downloadable JSON file named litxr_schema.json.",
        "Return raw JSON only, with no Markdown fence, no explanation, and no surrounding text.",
        prompt,
        fixed = TRUE
      ),
      markdown_fenced_json = gsub(
        "Return a downloadable JSON file named litxr_schema.json.",
        "Return the JSON inside a fenced Markdown code block with json after the opening backticks.",
        prompt,
        fixed = TRUE
      )
    )
    list(status = "ok", prompt = prompt)
  },
  error = function(e) {
    message(conditionMessage(e))
    list(status = "error", prompt = NULL)
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
