#!/usr/bin/env Rscript

log_line <- function(...) {
  cat(..., "\n", file = stderr(), sep = "")
}

emit_json <- function(x) {
  writeLines(
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE),
    con = stdout()
  )
}

usage <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript scripts/diagnose_refactor_store.R [--oversized-mb 25]",
      "",
      "Options:",
      "  --oversized-mb N  Threshold in MB for flagging indexes as oversized.",
      "                    Default: 25",
      "  -h, --help        Show this help message.",
      "",
      "Behavior:",
      "  - Runs the bundled v0.1.0 refactor diagnostics.",
      "  - Writes progress logs to stderr and compact JSON to stdout.",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  out <- list(help = FALSE, oversized_mb = 25)
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (identical(key, "--oversized-mb")) {
      if (i == length(args)) stop("Missing value for --oversized-mb", call. = FALSE)
      out$oversized_mb <- as.numeric(args[[i + 1L]])
      i <- i + 2L
      next
    }
    stop("Unknown argument: ", key, call. = FALSE)
  }
  out
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

parsed <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(parsed$help)) {
  usage()
  quit(save = "no", status = 0L)
}

cfg <- litxr::litxr_read_config()
log_line("running refactor diagnostics")
report <- litxr::litxr_refactor_diagnostics(cfg, oversized_mb = parsed$oversized_mb)
emit_json(c(list(status = "ok"), report))
