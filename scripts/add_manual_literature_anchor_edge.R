#!/usr/bin/env Rscript

emit_json <- function(x) {
  writeLines(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"), con = stdout())
}

usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript scripts/add_manual_literature_anchor_edge.R \\",
    "    --source-ref-id ID --target-ref-id ID \\",
    "    --anchor-role ROLE --relationship RELATIONSHIP",
    "",
    "Appends one human-supplied literature relationship to",
    "log/manual_literature_anchor_edges.tsv.",
    "",
    "Both ids must be bare arXiv ids or bare DOIs.",
    "Accepted anchor roles: theoretical_foundation, conceptual_foundation,",
    "methodological_foundation, empirical_benchmark, main_comparison, review_anchor, unknown.",
    "Accepted relationships: builds_on, extends, tests, applies, compares_with,",
    "contradicts, critiques, replicates, generalizes, narrows, uses_as_context, unknown.",
    sep = "\n"
  ))
}

parse_args <- function(args) {
  out <- list(help = FALSE, source_ref_id = NULL, target_ref_id = NULL, anchor_role = NULL, relationship = NULL)
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (key %in% c("-h", "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (!(key %in% c("--source-ref-id", "--target-ref-id", "--anchor-role", "--relationship")) || i == length(args)) {
      stop("Unknown argument or missing value: ", key, call. = FALSE)
    }
    field <- switch(
      key,
      "--source-ref-id" = "source_ref_id",
      "--target-ref-id" = "target_ref_id",
      "--anchor-role" = "anchor_role",
      "--relationship" = "relationship"
    )
    out[[field]] <- args[[i + 1L]]
    i <- i + 2L
  }
  out
}

options(error = function() {
  emit_json(list(status = "error", error = trimws(geterrmessage())))
  quit(save = "no", status = 1L)
})

args <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(args$help)) {
  usage()
  quit(save = "no", status = 0L)
}
if (is.null(args$source_ref_id) || is.null(args$target_ref_id) ||
    is.null(args$anchor_role) || is.null(args$relationship)) {
  stop("Supply --source-ref-id, --target-ref-id, --anchor-role, and --relationship.", call. = FALSE)
}

args$source_ref_id <- tolower(trimws(args$source_ref_id))
args$target_ref_id <- tolower(trimws(args$target_ref_id))
args$anchor_role <- trimws(args$anchor_role)
args$relationship <- trimws(args$relationship)
is_bare_ref_id <- function(x) {
  grepl("^[0-9]{4}\\.[0-9]{4,5}$", x) || grepl("^10\\.[^[:space:]/]+/.+$", x)
}
if (!is_bare_ref_id(args$source_ref_id) || !is_bare_ref_id(args$target_ref_id)) {
  stop("`source_ref_id` and `target_ref_id` must be bare arXiv ids or bare DOIs.", call. = FALSE)
}
roles <- litxr:::.litxr_anchor_reference_levels("v5")
relationships <- c(
  "builds_on", "extends", "tests", "applies", "compares_with", "contradicts",
  "critiques", "replicates", "generalizes", "narrows", "uses_as_context", "unknown"
)
if (!(args$anchor_role %in% roles)) {
  stop("Invalid --anchor-role. Allowed values: ", paste(roles, collapse = ", "), call. = FALSE)
}
if (!(args$relationship %in% relationships)) {
  stop("Invalid --relationship. Allowed values: ", paste(relationships, collapse = ", "), call. = FALSE)
}

cfg <- litxr::litxr_read_config()
log_dir <- litxr:::.litxr_ensure_project_log_dir(cfg)
path <- file.path(log_dir, "manual_literature_anchor_edges.tsv")
row <- data.table::data.table(
  source_ref_id = args$source_ref_id,
  target_ref_id = args$target_ref_id,
  anchor_role = args$anchor_role,
  relationship = args$relationship
)
data.table::fwrite(row, path, sep = "\t", append = file.exists(path), col.names = !file.exists(path))
emit_json(list(status = "ok", path = path, edge = as.list(row)))
