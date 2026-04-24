#!/bin/zsh

set -eu

if [[ $# -ne 1 ]]; then
  print -u2 "usage: $0 <arxiv-id>"
  exit 1
fi

arxiv_id="${1}"

Rscript -e '
args <- commandArgs(trailingOnly = TRUE)
ref_id <- args[[1]]
if (!nzchar(ref_id)) {
  stop("One arXiv id is required.", call. = FALSE)
}
if (!grepl("^arxiv:", ref_id)) {
  ref_id <- paste0("arxiv:", ref_id)
}
hits <- litxr::litxr_find_refs(ref_id = ref_id)
if (!nrow(hits)) {
  stop("No record found for ", ref_id, ".", call. = FALSE)
}
if (nrow(hits) != 1L) {
  stop("Expected exactly one record for ", ref_id, " but found ", nrow(hits), ".", call. = FALSE)
}
abstract <- as.character(hits$abstract[[1]])
if (is.na(abstract) || !nzchar(trimws(abstract))) {
  stop("Abstract is missing for ", ref_id, ".", call. = FALSE)
}
cat(abstract, "\n", sep = "")
' -- "$arxiv_id"
