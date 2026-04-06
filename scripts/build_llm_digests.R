#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(args) {
  out <- list()
  i <- 1L

  while (i <= length(args)) {
    key <- args[[i]]
    if (!startsWith(key, "--")) {
      stop("Unexpected positional argument: ", key, call. = FALSE)
    }

    name <- sub("^--", "", key)
    if (identical(name, "help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }

    if (name %in% c("overwrite", "dry-run")) {
      out[[name]] <- TRUE
      i <- i + 1L
      next
    }

    if (i == length(args)) {
      stop("Missing value for argument: ", key, call. = FALSE)
    }

    out[[name]] <- args[[i + 1L]]
    i <- i + 2L
  }

  out
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/build_llm_digests.R --builder-file scripts/my_digest_builder.R [--ref-ids doi:10.x,arxiv:2501.12345v1] [--collection-id my_collection] [--limit 50] [--overwrite] [--dry-run]",
      "",
      "Notes:",
      "  - LITXR_CONFIG must be set in the environment.",
      "  - The builder file must define `builder`, `litxr_builder`, or `build_digest`.",
      "  - Without `--ref-ids`, the script targets references with markdown and missing digests.",
      "  - `--collection-id` narrows the target set before batch build.",
      sep = "\n"
    )
  )
}

load_builder <- function(path) {
  env <- new.env(parent = baseenv())
  sys.source(path, envir = env)

  for (name in c("builder", "litxr_builder", "build_digest")) {
    if (exists(name, envir = env, inherits = FALSE) &&
        is.function(get(name, envir = env, inherits = FALSE))) {
      return(get(name, envir = env, inherits = FALSE))
    }
  }

  stop(
    "Builder file must define a function named `builder`, `litxr_builder`, or `build_digest`.",
    call. = FALSE
  )
}

parse_ref_ids <- function(x) {
  if (is.null(x) || !nzchar(x)) {
    return(NULL)
  }
  out <- trimws(strsplit(x, ",", fixed = TRUE)[[1]])
  out <- unique(out[nzchar(out)])
  if (!length(out)) NULL else out
}

parsed <- parse_args(args)

if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}

if (is.null(parsed[["builder-file"]]) || !nzchar(parsed[["builder-file"]])) {
  usage()
  stop("`--builder-file` is required.", call. = FALSE)
}

builder_file <- normalizePath(parsed[["builder-file"]], winslash = "/", mustWork = TRUE)
limit <- if (is.null(parsed$limit)) NULL else as.integer(parsed$limit)
if (!is.null(limit) && (is.na(limit) || limit <= 0L)) {
  stop("`--limit` must be a positive integer.", call. = FALSE)
}

script_arg <- commandArgs(trailingOnly = FALSE)
script_file <- sub("^--file=", "", script_arg[grep("^--file=", script_arg)][1])
repo_root <- normalizePath(file.path(dirname(script_file), ".."), winslash = "/", mustWork = TRUE)

if (requireNamespace("pkgload", quietly = TRUE)) {
  pkgload::load_all(repo_root, quiet = TRUE, export_all = FALSE, helpers = FALSE)
} else {
  suppressPackageStartupMessages(library(litxr))
}

cfg <- litxr_read_config()
builder <- load_builder(builder_file)
ref_ids <- parse_ref_ids(parsed[["ref-ids"]])

if (!is.null(parsed[["collection-id"]]) && nzchar(parsed[["collection-id"]])) {
  status <- litxr_read_enrichment_status(cfg)
  links <- litxr_read_reference_collections(cfg)
  keep <- unique(links$ref_id[links$collection_id == parsed[["collection-id"]]])
  if (is.null(ref_ids)) {
    status <- status[status$ref_id %in% keep & status$has_md, ]
    if (!isTRUE(parsed$overwrite)) {
      status <- status[!status$has_llm_digest, ]
    }
    ref_ids <- status$ref_id
  } else {
    ref_ids <- intersect(ref_ids, keep)
  }
}

if (isTRUE(parsed[["dry-run"]])) {
  targets <- if (is.null(ref_ids)) {
    status <- litxr_read_enrichment_status(cfg)
    status <- status[status$has_md, ]
    if (!isTRUE(parsed$overwrite)) {
      status <- status[!status$has_llm_digest, ]
    }
    status$ref_id
  } else {
    ref_ids
  }
  if (!is.null(limit)) {
    targets <- targets[seq_len(min(length(targets), limit))]
  }
  cat(sprintf("dry run: targets=%s\n", length(targets)))
  if (length(targets)) {
    cat(paste(targets, collapse = "\n"), "\n", sep = "")
  }
  quit(save = "no", status = 0L)
}

built <- litxr_build_llm_digests(
  builder = builder,
  config = cfg,
  ref_ids = ref_ids,
  overwrite = isTRUE(parsed$overwrite),
  limit = limit
)

cat(sprintf("digest build complete: built=%s\n", length(built)))
