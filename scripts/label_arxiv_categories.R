#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(litxr)
})

args <- commandArgs(trailingOnly = TRUE)

read_flag <- function(flag, default = NULL) {
  idx <- match(flag, args)
  if (is.na(idx)) {
    return(default)
  }
  if (idx == length(args)) {
    stop("Missing value for ", flag, call. = FALSE)
  }
  args[[idx + 1L]]
}

year_from <- read_flag("--year-from", NULL)
year_to <- read_flag("--year-to", NULL)
year_from <- if (is.null(year_from)) NULL else as.integer(year_from)
year_to <- if (is.null(year_to)) NULL else as.integer(year_to)
if (!is.null(year_from) && (is.na(year_from) || year_from < 0L)) {
  stop("`--year-from` must be a non-negative integer.", call. = FALSE)
}
if (!is.null(year_to) && (is.na(year_to) || year_to < 0L)) {
  stop("`--year-to` must be a non-negative integer.", call. = FALSE)
}
if (!is.null(year_from) && !is.null(year_to) && year_to < year_from) {
  stop("`--year-to` must be on or after `--year-from`.", call. = FALSE)
}

script_path <- {
  args_full <- commandArgs(trailingOnly = FALSE)
  file_flag <- "--file="
  hit <- grep(file_flag, args_full, value = TRUE)
  if (length(hit)) sub(file_flag, "", hit[[1]]) else ""
}

repo_root <- if (nzchar(script_path)) {
  normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = FALSE)
} else {
  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

query_set_path <- system.file("extdata", "ai_category_query_set.json", package = "litxr")
if (!nzchar(query_set_path)) {
  query_set_path <- file.path(repo_root, "inst", "extdata", "ai_category_query_set.json")
}
if (!file.exists(query_set_path)) {
  stop("Category query set not found: ", query_set_path, call. = FALSE)
}

if (!requireNamespace("inferencer", quietly = TRUE)) {
  stop("Package `inferencer` is required for this script.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
embed_model <- "nvidia/llama-nemotron-embed-vl-1b-v2:free"
embed_fun <- function(texts) {
  inferencer::embed_openrouter(texts, model = embed_model)
}

delta_meta <- litxr::litxr_embed_collection_delta(
  "arxiv_cs_ai",
  cfg,
  field = "abstract",
  embed_fun = embed_fun,
  model = embed_model,
  provider = "openrouter",
  batch_size = 64,
  limit = 640L
)

query_index <- litxr::litxr_build_label_query_index(
  query_set = query_set_path,
  query_set_id = "ai-category-query-set-v1",
  config = cfg,
  embed_fun = embed_fun,
  model = embed_model,
  provider = "openrouter",
  batch_size = 32L
)

ref_ids <- NULL
if (!is.null(year_from) || !is.null(year_to)) {
  refs <- litxr::litxr_read_collection("arxiv_cs_ai", cfg)
  keep <- rep(TRUE, nrow(refs))
  if (!is.null(year_from)) {
    keep <- keep & !is.na(refs$year) & refs$year >= year_from
  }
  if (!is.null(year_to)) {
    keep <- keep & !is.na(refs$year) & refs$year <= year_to
  }
  ref_ids <- unique(as.character(refs$ref_id[keep & !is.na(refs$ref_id) & nzchar(refs$ref_id)]))
}

scores <- litxr::litxr_score_collection_categories(
  "arxiv_cs_ai",
  query_set_id = "ai-category-query-set-v1",
  config = cfg,
  field = "abstract",
  model = embed_model,
  aggregations = "max",
  ref_ids = ref_ids
)

if (!nrow(scores)) {
  cat("No category scores were produced.\n")
  quit(save = "no", status = 0)
}

top3 <- scores[order(category_id, -score_max, title)][
  ,
  head(.SD, 3L),
  by = category_id
]

top3_out <- top3[, c("category_id", "ref_id", "title", "year", "score_max"), with = FALSE]

cat(sprintf("delta_rows=%d\n", nrow(delta_meta)))
cat(sprintf("query_rows=%d\n", nrow(query_index$metadata)))
if (!is.null(year_from) || !is.null(year_to)) {
  cat(sprintf(
    "year_filter: year_from=%s year_to=%s matched_ref_ids=%d\n",
    if (is.null(year_from)) "NA" else year_from,
    if (is.null(year_to)) "NA" else year_to,
    length(ref_ids)
  ))
}
print(top3_out)
