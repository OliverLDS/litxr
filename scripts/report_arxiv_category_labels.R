#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(litxr)
})

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(args) {
  out <- list(
    show_help = FALSE,
    delta_only = FALSE,
    embed_missing = FALSE,
    year_from = NULL,
    year_to = NULL,
    inquiry = NULL,
    local_inq = "ai_category_query_set_v1",
    top_n = "3",
    threshold = "0.45"
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
    if (identical(key, "--embed-missing")) {
      out$embed_missing <- TRUE
      i <- i + 1L
      next
    }
    if (identical(key, "--delta-only")) {
      out$delta_only <- TRUE
      i <- i + 1L
      next
    }
    if (i == length(args)) {
      stop("Missing value for ", key, call. = FALSE)
    }
    value <- args[[i + 1L]]
    if (identical(key, "--year-from")) {
      out$year_from <- value
    } else if (identical(key, "--year-to")) {
      out$year_to <- value
    } else if (identical(key, "--inquiry")) {
      out$inquiry <- value
    } else if (identical(key, "--local-inq")) {
      out$local_inq <- value
    } else if (identical(key, "--top_n") || identical(key, "--top-n")) {
      out$top_n <- value
    } else if (identical(key, "--threshold")) {
      out$threshold <- value
    } else {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    i <- i + 2L
  }

  out
}

parsed <- parse_args(args)
show_help <- isTRUE(parsed$show_help)
delta_only <- isTRUE(parsed$delta_only)
embed_missing <- isTRUE(parsed$embed_missing)

if (show_help) {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/report_arxiv_category_labels.R [--embed-missing] [--year-from YYYY] [--year-to YYYY] [--delta-only] [--local-inq QUERY_SET_ID] [--inquiry PATH] [--top_n 3] [--threshold 0.45]",
      "",
      "Options:",
      "  --embed-missing    Run litxr_embed_collection_delta() before scoring. Default: off.",
      "  --year-from YYYY   Restrict scoring to refs with year >= YYYY.",
      "  --year-to YYYY     Restrict scoring to refs with year <= YYYY.",
      "  --inquiry PATH     YAML file defining category ids and inquiry sentences for this run.",
      "  --local-inq ID     Cached local inquiry/query-set id to use when --inquiry is not supplied.",
      "  --delta-only       Score only the pending embedding delta shards instead of the full compacted corpus.",
      "  --top_n N          Keep only the top N refs per category by score. Default: 3.",
      "  --top-n N          Alias of --top_n.",
      "  --threshold X      Keep only refs with score >= X. Default: 0.45.",
      "  -h, --help         Show this help message.",
      "",
      "Selection rule:",
      "  - Keep refs that satisfy both conditions:",
      "    rank_in_category <= top_n and score_max >= threshold.",
      "  - Default values are top_n = 3 and threshold = 0.45.",
      "  - By default, the script does not call litxr_embed_collection_delta().",
      "    Use --embed-missing when you want to embed uncovered corpus refs first.",
      "  - Without --inquiry, the script uses the cached query set id",
      "    from --local-inq, defaulting to ai_category_query_set_v1.",
      "  - With --inquiry, the script builds a temporary query embedding cache for this run",
      "    and removes it afterward.",
      "",
      "Output format:",
      "  category_id",
      "  1. ref_id (score_max): title",
      sep = "\n"
    )
  )
  quit(save = "no", status = 0)
}

year_from <- parsed$year_from
year_to <- parsed$year_to
inquiry_path <- parsed$inquiry
local_inq <- as.character(parsed$local_inq)
top_n <- as.integer(parsed$top_n)
threshold <- as.numeric(parsed$threshold)

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
if (is.na(top_n) || top_n < 0L) {
  stop("`--top_n` must be a non-negative integer.", call. = FALSE)
}
if (is.na(threshold)) {
  stop("`--threshold` must be numeric.", call. = FALSE)
}
if (is.na(local_inq) || !nzchar(local_inq)) {
  stop("`--local-inq` must be non-empty.", call. = FALSE)
}
if (!is.null(inquiry_path)) {
  inquiry_path <- normalizePath(inquiry_path, winslash = "/", mustWork = FALSE)
  if (!file.exists(inquiry_path)) {
    stop("`--inquiry` file not found: ", inquiry_path, call. = FALSE)
  }
  ext <- tolower(tools::file_ext(inquiry_path))
  if (!ext %in% c("yml", "yaml")) {
    stop("`--inquiry` must point to a YAML file.", call. = FALSE)
  }
}

script_path <- {
  args_full <- commandArgs(trailingOnly = FALSE)
  file_flag <- "--file="
  hit <- grep(file_flag, args_full, value = TRUE)
  if (length(hit)) sub(file_flag, "", hit[[1]]) else ""
}

if (!requireNamespace("inferencer", quietly = TRUE)) {
  stop("Package `inferencer` is required for this script.", call. = FALSE)
}

cfg <- litxr::litxr_read_config()
embed_model <- "nvidia/llama-nemotron-embed-vl-1b-v2:free"
query_set_id <- local_inq
embed_fun <- function(texts) {
  inferencer::embed_openrouter(texts, model = embed_model)
}

if (!is.null(inquiry_path)) {
  query_set_id <- paste0(
    "tmp_inquiry_",
    format(Sys.time(), "%Y%m%d%H%M%S"),
    "_",
    Sys.getpid()
  )
  temp_paths <- litxr:::.litxr_label_query_index_paths(cfg, query_set_id, embed_model)
  on.exit(unlink(temp_paths$dir, recursive = TRUE, force = TRUE), add = TRUE)
  litxr::litxr_build_label_query_index(
    query_set = inquiry_path,
    query_set_id = query_set_id,
    config = cfg,
    embed_fun = embed_fun,
    model = embed_model,
    provider = "openrouter",
    batch_size = 32L,
    overwrite = TRUE
  )
}

if (isTRUE(embed_missing)) {
  litxr::litxr_embed_collection_delta(
    "arxiv_cs_ai",
    cfg,
    field = "abstract",
    embed_fun = embed_fun,
    model = embed_model,
    provider = "openrouter",
    batch_size = 64,
    limit = 640L
  )
}

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

scores <- if (isTRUE(delta_only)) {
  litxr::litxr_score_collection_categories_delta(
    "arxiv_cs_ai",
    query_set_id = query_set_id,
    config = cfg,
    field = "abstract",
    model = embed_model,
    aggregations = "max",
    ref_ids = ref_ids
  )
} else {
  litxr::litxr_score_collection_categories(
    "arxiv_cs_ai",
    query_set_id = query_set_id,
    config = cfg,
    field = "abstract",
    model = embed_model,
    aggregations = "max",
    ref_ids = ref_ids
  )
}

if (!nrow(scores)) {
  cat("No category scores were produced.\n")
  quit(save = "no", status = 0)
}

scores <- data.table::as.data.table(scores)
data.table::setorderv(scores, c("category_id", "score_max", "title"), order = c(1L, -1L, 1L), na.last = TRUE)
scores[, rank_in_category := seq_len(.N), by = category_id]

selected <- copy(scores)
selected <- selected[rank_in_category <= top_n & !is.na(score_max) & score_max >= threshold]

if (!nrow(selected)) {
  cat("No refs met the selection rule.\n")
  quit(save = "no", status = 0)
}

data.table::setorderv(selected, c("category_id", "score_max", "title"), order = c(1L, -1L, 1L), na.last = TRUE)

for (cat_id in unique(as.character(selected$category_id))) {
  chunk <- selected[category_id == cat_id, ]
  cat(cat_id, "\n", sep = "")
  for (i in seq_len(nrow(chunk))) {
    cat(sprintf(
      "%d. %s (%.7f): %s\n",
      i,
      chunk$ref_id[[i]],
      chunk$score_max[[i]],
      chunk$title[[i]]
    ))
  }
  cat("\n")
}
