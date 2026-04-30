#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(litxr)
})

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(args) {
  out <- list(
    show_help = FALSE,
    inquiry = NULL,
    query_set_id = NULL
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
    if (identical(key, "--inquiry")) {
      out$inquiry <- value
    } else if (identical(key, "--query-set-id")) {
      out$query_set_id <- value
    } else {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    i <- i + 2L
  }

  out
}

parsed <- parse_args(args)

if (isTRUE(parsed$show_help)) {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/cache_category_inquiries.R --inquiry PATH [--query-set-id QUERY_SET_ID]",
      "",
      "Options:",
      "  --inquiry PATH            YAML file defining category ids and inquiry sentences.",
      "  --query-set-id ID         Optional local cache id to create or overwrite.",
      "                            Default: tmp_inquiry_<timestamp>_<pid>",
      "  -h, --help               Show this help message.",
      "",
      "Notes:",
      "  - The script writes a persistent local inquiry embedding cache under",
      "    project.data_root/embeddings/label_queries/.",
      "  - If --query-set-id is supplied, that cache is overwritten.",
      "  - If --query-set-id is omitted, a new tmp_inquiry_* cache id is created.",
      sep = "\n"
    )
  )
  quit(save = "no", status = 0L)
}

inquiry_path <- parsed$inquiry
query_set_id <- parsed$query_set_id

if (is.null(inquiry_path) || !nzchar(inquiry_path)) {
  stop("`--inquiry` is required.", call. = FALSE)
}
inquiry_path <- normalizePath(inquiry_path, winslash = "/", mustWork = FALSE)
if (!file.exists(inquiry_path)) {
  stop("`--inquiry` file not found: ", inquiry_path, call. = FALSE)
}
ext <- tolower(tools::file_ext(inquiry_path))
if (!ext %in% c("yml", "yaml")) {
  stop("`--inquiry` must point to a YAML file.", call. = FALSE)
}

if (is.null(query_set_id) || !nzchar(query_set_id)) {
  query_set_id <- paste0(
    "tmp_inquiry_",
    format(Sys.time(), "%Y%m%d%H%M%S"),
    "_",
    Sys.getpid()
  )
}

if (!requireNamespace("inferencer", quietly = TRUE)) {
  stop("Package `inferencer` is required for this script.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
embed_model <- "nvidia/llama-nemotron-embed-vl-1b-v2:free"
embed_fun <- function(texts) {
  inferencer::embed_openrouter(texts, model = embed_model)
}

index <- litxr::litxr_build_label_query_index(
  query_set = inquiry_path,
  query_set_id = query_set_id,
  config = cfg,
  embed_fun = embed_fun,
  model = embed_model,
  provider = "openrouter",
  batch_size = 32L,
  overwrite = TRUE
)

paths <- litxr:::.litxr_label_query_index_paths(cfg, query_set_id, embed_model)

cat(sprintf("query_set_id=%s\n", query_set_id))
cat(sprintf("records=%s\n", nrow(index$metadata)))
cat(sprintf("model=%s\n", embed_model))
cat(sprintf("cache_dir=%s\n", paths$dir))
